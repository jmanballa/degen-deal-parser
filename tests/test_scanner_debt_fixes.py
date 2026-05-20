"""Regression tests for scanner debt fixes (M5, H4, H7)."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import asyncio

from app.inventory import pokemon_scanner as ps


def _reset_caches():
    ps._pending_validations.clear()
    ps._scan_result_cache.clear()
    ps._EXTERNAL_API_SEMAPHORE = None


# ---- M5: get_validation_result must be non-destructive by default ----

def test_m5_validation_result_survives_rereads():
    _reset_caches()
    scan_id = "test-m5-a"
    payload = {"status": "MATCHED", "best_match": {"name": "Pikachu"}}
    ps._pending_validations[scan_id] = (time.monotonic(), payload)

    first = ps.get_validation_result(scan_id)
    second = ps.get_validation_result(scan_id)
    third = ps.get_validation_result(scan_id)

    assert first == payload
    assert second == payload
    assert third == payload, "re-reads must not drop the cached result"
    assert scan_id in ps._pending_validations


def test_m5_pending_status_still_reported():
    _reset_caches()
    scan_id = "test-m5-pending"
    ps._pending_validations[scan_id] = (time.monotonic(), None)

    out = ps.get_validation_result(scan_id)
    assert out == {"validation_status": "pending", "scan_id": scan_id}
    # pending entries stay put (unchanged from prior behavior)
    assert scan_id in ps._pending_validations


def test_m5_explicit_ack_removes_entry():
    _reset_caches()
    scan_id = "test-m5-ack"
    payload = {"status": "MATCHED"}
    ps._pending_validations[scan_id] = (time.monotonic(), payload)

    assert ps.get_validation_result(scan_id, ack=True) == payload
    assert scan_id not in ps._pending_validations
    assert ps.get_validation_result(scan_id) is None


def test_m5_unknown_scan_id_returns_none():
    _reset_caches()
    assert ps.get_validation_result("nope") is None


# ---- H4: image-hash cache short-circuits identical scans ----

def test_h4_identical_image_hits_cache():
    _reset_caches()
    calls = {"n": 0}

    async def fake_balanced(image_b64, category_id):
        calls["n"] += 1
        return {"status": "MATCHED", "best_match": {"name": "Charizard"}, "debug": {}}

    orig = ps._run_balanced_pipeline
    ps._run_balanced_pipeline = fake_balanced  # type: ignore[assignment]
    try:
        r1 = asyncio.run(ps.run_pipeline("imagedata", "3", "balanced"))
        r2 = asyncio.run(ps.run_pipeline("imagedata", "3", "balanced"))
    finally:
        ps._run_balanced_pipeline = orig  # type: ignore[assignment]

    assert calls["n"] == 1, "second call should have hit cache, not re-run pipeline"
    assert r1["status"] == "MATCHED"
    assert r2["status"] == "MATCHED"
    assert r2.get("debug", {}).get("cache_hit") is True
    assert r1.get("debug", {}).get("cache_hit") is not True


def test_h4_different_image_misses_cache():
    _reset_caches()
    calls = {"n": 0}

    async def fake_balanced(image_b64, category_id):
        calls["n"] += 1
        return {"status": "MATCHED", "best_match": {"name": "x"}, "debug": {}}

    orig = ps._run_balanced_pipeline
    ps._run_balanced_pipeline = fake_balanced  # type: ignore[assignment]
    try:
        asyncio.run(ps.run_pipeline("img-a", "3", "balanced"))
        asyncio.run(ps.run_pipeline("img-b", "3", "balanced"))
    finally:
        ps._run_balanced_pipeline = orig  # type: ignore[assignment]

    assert calls["n"] == 2


def test_h4_pending_validation_not_cached():
    # Optimistic returns (validation still in flight) must not be cached —
    # caching would break the /validate/{scan_id} poll for future clients.
    _reset_caches()
    key = ("balanced", "3", "abc")
    ps._scan_cache_put(key, {"status": "MATCHED", "validation_pending": True})
    assert ps._scan_cache_get(key) is None


def test_h4_error_results_not_cached():
    _reset_caches()
    key = ("fast", "3", "abc")
    ps._scan_cache_put(key, {"status": "ERROR", "error": "boom"})
    assert ps._scan_cache_get(key) is None


# ---- H7: external-API semaphore caps fan-out concurrency ----

def test_h7_semaphore_caps_concurrency():
    _reset_caches()

    async def drive():
        sem = ps._get_external_api_semaphore()
        # Semaphore is created lazily — confirm it's a real Semaphore and
        # its initial value matches the configured cap.
        assert isinstance(sem, asyncio.Semaphore)

        in_flight = {"n": 0, "peak": 0}

        async def worker():
            async with sem:
                in_flight["n"] += 1
                in_flight["peak"] = max(in_flight["peak"], in_flight["n"])
                await asyncio.sleep(0.02)
                in_flight["n"] -= 1

        # Launch 4x the cap; peak concurrency must respect the cap.
        await asyncio.gather(*[worker() for _ in range(ps._EXTERNAL_API_CONCURRENCY * 4)])
        return in_flight["peak"]

    peak = asyncio.run(drive())
    assert peak <= ps._EXTERNAL_API_CONCURRENCY
