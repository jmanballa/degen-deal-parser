"""Degen Eye v2 orchestration — pHash-first identification + price.

Flow (happy path):
    base64 image
      -> card_detect.detect_and_crop  (OpenCV perspective rectification)
      -> phash_scanner.lookup          (nearest-neighbor over local index)
      -> price_cache.get_price_for_match  (TCGTracking, warm cache)
      -> ScanResult

Fallback path (pHash confidence LOW or index missing):
    base64 image
      -> _run_ximilar_pipeline         (v1's cloud pipeline)
      -> price_cache.get_price_for_match
      -> ScanResult

Returns the same ScanResult dict shape as ``app.pokemon_scanner.run_pipeline``
so the frontend helpers (``addToBatchFromCard``, ``_pickDisplayPrice``,
``_resolveConditionPrice``) work unchanged. Adds a ``debug.v2`` block with
per-stage timing + the pHash verdict so the debug page can show what
actually happened.
"""
from __future__ import annotations

import asyncio
import base64
import collections
import datetime as _dt
import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from threading import Lock
from typing import Any, AsyncIterator, Optional

from .card_detect import detect_and_crop
from .config import get_settings
from .phash_scanner import (
    MEDIUM_THRESHOLD,
    PhashMatch,
    has_index,
    lookup,
)
from .pokemon_scanner import (
    CandidateCard,
    ScanResult,
    ScoredCandidate,
    _game_for_category,
    _run_ximilar_pipeline,
    _scan_history as _V1_SCAN_HISTORY,
)
from .price_cache import get_price_for_match

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# v2 has its own scan history — kept separate from v1's _scan_history so the
# /degen_eye/debug and /degen_eye/history endpoints stay pure v1, and the new
# /degen_eye/v2/history endpoint returns only v2 scans. Mirrors v1's ring
# buffer semantics (maxlen=25).
# ---------------------------------------------------------------------------
_V2_SCAN_HISTORY: collections.deque[dict] = collections.deque(maxlen=25)
_ROOT = Path(__file__).resolve().parent.parent
_V2_HISTORY_PATH = _ROOT / "data" / "v2_scan_history.jsonl"
_V2_HISTORY_LOCK = Lock()
_V2_HISTORY_MAX_BYTES = 1024 * 1024


def _append_v2_history_file(entry: dict) -> None:
    try:
        _V2_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(entry, default=str, separators=(",", ":"))
        with _V2_HISTORY_LOCK:
            with _V2_HISTORY_PATH.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
            if _V2_HISTORY_PATH.stat().st_size > _V2_HISTORY_MAX_BYTES:
                lines = _V2_HISTORY_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
                _V2_HISTORY_PATH.write_text("\n".join(lines[-200:]) + "\n", encoding="utf-8")
    except OSError:
        logger.debug("[degen_eye_v2] unable to persist v2 history", exc_info=True)


def _save_v2_history(result: dict) -> None:
    """Write a v2 scan result to the v2-only history buffer.

    Shape mirrors v1's ``_save_to_history`` entry layout so the debug page
    can render either history with the same template.
    """
    bm = result.get("best_match") or {}
    ef = result.get("extracted_fields") or {}
    dbg = result.get("debug") or {}
    entry = {
        "timestamp": _dt.datetime.now().isoformat(timespec="seconds"),
        "status": result.get("status"),
        "best_match_name": bm.get("name"),
        "best_match_number": bm.get("number"),
        "best_match_set": bm.get("set_name"),
        "best_match_score": bm.get("score"),
        "best_match_confidence": bm.get("confidence"),
        "best_match_price": bm.get("market_price"),
        "candidates_count": len(result.get("candidates") or []),
        "processing_time_ms": result.get("processing_time_ms"),
        "extracted_name": ef.get("card_name"),
        "extracted_number": ef.get("collector_number"),
        "extracted_set": ef.get("set_name"),
        "ocr_text": dbg.get("ocr_raw_text", ""),
        "ocr_confidence": dbg.get("ocr_confidence"),
        "extraction_method": dbg.get("extraction_method", "phash"),
        "disambiguation": result.get("disambiguation_method"),
        "error": result.get("error"),
        "debug": dbg,
    }
    _V2_SCAN_HISTORY.appendleft(entry)
    _append_v2_history_file(entry)


def get_v2_scan_history() -> list[dict]:
    """Return the recent v2-only scan history (newest first)."""
    try:
        if _V2_HISTORY_PATH.exists():
            lines = _V2_HISTORY_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
            entries: list[dict] = []
            for line in reversed(lines[-100:]):
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
                if len(entries) >= 25:
                    break
            if entries:
                return entries
    except OSError:
        logger.debug("[degen_eye_v2] unable to read v2 history file", exc_info=True)
    return list(_V2_SCAN_HISTORY)


def _b64_to_bytes(image_b64: str) -> Optional[bytes]:
    s = (image_b64 or "").strip()
    if "," in s:
        s = s.split(",", 1)[1]
    try:
        return base64.b64decode(s)
    except Exception as exc:
        logger.warning("[degen_eye_v2] base64 decode failed: %s", exc)
        return None


def _elapsed(t0: float) -> float:
    return round((time.monotonic() - t0) * 1000, 1)


def _phash_match_to_candidate(match: PhashMatch) -> ScoredCandidate:
    """Project a pHash match into the ScanResult candidate shape."""
    score = 100.0 if match.confidence == "HIGH" else (65.0 if match.confidence == "MEDIUM" else 40.0)
    return ScoredCandidate(
        id=match.entry.card_id,
        name=match.entry.name,
        number=match.entry.number,
        set_id=match.entry.set_id,
        set_name=match.entry.set_name,
        image_url=match.entry.image_url or "",
        image_url_small=match.entry.image_url or "",
        source="phash",
        tcgplayer_url=match.entry.tcgplayer_url,
        score=score,
        confidence=match.confidence,
        match_reason=f"pHash d={match.distance}",
    )


def _same_card_core(name_a: str, number_a: str, name_b: str, number_b: str) -> bool:
    return (
        (name_a or "").strip().lower() == (name_b or "").strip().lower()
        and (number_a or "").split("/")[0].strip().lstrip("0")
        == (number_b or "").split("/")[0].strip().lstrip("0")
    )


def _ximilar_best_to_candidate(ximilar_result: Optional[dict]) -> Optional[ScoredCandidate]:
    if not ximilar_result:
        return None
    x_best = ximilar_result.get("best_match") or {}
    x_name = x_best.get("name") or ""
    if not x_name:
        return None
    return ScoredCandidate(
        id=x_best.get("id") or "",
        name=x_name,
        number=x_best.get("number") or "",
        set_id=x_best.get("set_id") or "",
        set_name=x_best.get("set_name") or "",
        image_url=x_best.get("image_url") or "",
        image_url_small=x_best.get("image_url_small") or "",
        source="ximilar",
        score=float(x_best.get("score") or 50.0),
        confidence=x_best.get("confidence") or "MEDIUM",
        match_reason="ximilar fallback",
        tcgplayer_url=x_best.get("tcgplayer_url"),
        available_variants=list(x_best.get("available_variants") or []),
        market_price=x_best.get("market_price"),
    )


def _merge_ximilar_candidate(
    candidates: list[ScoredCandidate],
    ximilar_result: Optional[dict],
    *,
    prefer_ximilar_top: bool,
) -> None:
    x_candidate = _ximilar_best_to_candidate(ximilar_result)
    if x_candidate is None:
        return
    already = any(
        _same_card_core(c.name, c.number, x_candidate.name, x_candidate.number)
        for c in candidates
    )
    if already:
        return
    candidates.insert(0 if prefer_ximilar_top else min(1, len(candidates)), x_candidate)


async def _enrich_top_candidate(
    candidates: list[ScoredCandidate],
    matches: list[PhashMatch],
    category_id: str,
    debug: dict[str, Any],
) -> None:
    """Populate the top candidate's price + variants via the cached
    TCGTracking lookup. Mutates ``candidates[0]`` in place."""
    if not candidates or not matches:
        return
    t_price = time.monotonic()
    try:
        price_info = await get_price_for_match(matches[0], category_id=category_id)
    except Exception as exc:
        logger.warning("[degen_eye_v2] price lookup failed for %s: %s", candidates[0].name, exc)
        debug["price_error"] = str(exc)
        return
    debug["price_elapsed_ms"] = _elapsed(t_price)
    debug["price_source"] = price_info.get("source")

    top = candidates[0]
    if price_info.get("market_price") is not None:
        top.market_price = price_info["market_price"]
    if price_info.get("tcgplayer_url"):
        top.tcgplayer_url = price_info["tcgplayer_url"]
    # Prefer the richer TCGTracking / TCGdex image when available.
    if price_info.get("image_url"):
        top.image_url = price_info["image_url"]
    if price_info.get("image_url_small"):
        top.image_url_small = price_info["image_url_small"]
    if price_info.get("variants"):
        top.available_variants = list(price_info["variants"])


def _ximilar_top_sig(ximilar_result: Optional[dict]) -> Optional[tuple[str, str]]:
    if not ximilar_result:
        return None
    bm = ximilar_result.get("best_match") or {}
    name = (bm.get("name") or "").strip().lower()
    num = (bm.get("number") or "").split("/")[0].strip().lstrip("0")
    return (name, num) if (name or num) else None


def _phash_top_sig(matches: list[PhashMatch]) -> Optional[tuple[str, str]]:
    if not matches:
        return None
    e = matches[0].entry
    name = (e.name or "").strip().lower()
    num = (e.number or "").split("/")[0].strip().lstrip("0")
    return (name, num) if (name or num) else None


async def _run_ximilar_fallback(
    image_b64: str, category_id: str, debug: dict[str, Any],
) -> Optional[dict]:
    """Run v1's Ximilar pipeline when pHash is weak or the index is missing.

    ``_run_ximilar_pipeline`` writes its own entry into v1's ``_scan_history``
    ring buffer. Since this call was triggered by a v2 scan we don't want it
    to appear in v1's debug/history endpoints — the v2 orchestrator will
    record the wrapped result into ``_V2_SCAN_HISTORY`` separately. After the
    call returns, pop the newest v1 entry so v1's history stays "pure v1".
    """
    settings = get_settings()
    if not settings.ximilar_api_token:
        debug["ximilar_fallback"] = "skipped_no_token"
        return None

    t_ximilar = time.monotonic()
    try:
        result = await _run_ximilar_pipeline(
            image_b64, settings.ximilar_api_token, category_id,
        )
    except Exception as exc:
        logger.warning("[degen_eye_v2] Ximilar fallback failed: %s", exc)
        debug["ximilar_fallback"] = f"error: {exc}"
        return None

    # Ximilar pipeline appendleft-ed one history entry. There is no await
    # between that save and the return to this coroutine, so the newest entry
    # is the fallback result even when the deque was already at maxlen.
    try:
        _V1_SCAN_HISTORY.popleft()
    except IndexError:
        pass

    debug["ximilar_fallback_elapsed_ms"] = _elapsed(t_ximilar)
    debug["ximilar_fallback"] = "ran"
    return result


def _stamp(result_dict: dict, category_id: str, v2_debug: dict[str, Any]) -> dict:
    """Stamp canonical v2 metadata onto a ScanResult dict."""
    result_dict.setdefault("debug", {})
    result_dict["debug"]["mode"] = "v2"
    result_dict["debug"]["scanner_version"] = "2.0"
    result_dict["debug"]["v2"] = v2_debug
    result_dict["debug"].setdefault("engines_used", [])
    result_dict["debug"].setdefault("tiebreaker_used", False)
    result_dict["debug"].setdefault("tiebreaker_winner", None)
    if not result_dict.get("game"):
        effective = result_dict["debug"].get("effective_category_id") or category_id
        result_dict["game"] = _game_for_category(effective)
    return result_dict


# ---------------------------------------------------------------------------
# Single-shot (non-streaming) pipeline
# ---------------------------------------------------------------------------

async def run_v2_pipeline(image_b64: str, category_id: str = "3") -> dict[str, Any]:
    """Identify a card via the pHash-first v2 pipeline.

    Returns a ScanResult-shaped dict compatible with the v1 frontend.
    """
    t_start = time.monotonic()
    v2_debug: dict[str, Any] = {"stages_ms": {}}

    raw_bytes = _b64_to_bytes(image_b64)
    if raw_bytes is None:
        result = asdict(ScanResult(
            status="ERROR", error="Invalid base64 image",
            processing_time_ms=_elapsed(t_start),
        ))
        out = _stamp(result, category_id, v2_debug)
        _save_v2_history(out)
        return out

    # Stage 1: card detection + rectification
    t_detect = time.monotonic()
    crop_bytes, detect_debug = await asyncio.to_thread(detect_and_crop, raw_bytes)
    v2_debug["stages_ms"]["detect"] = _elapsed(t_detect)
    v2_debug["detect"] = detect_debug

    # Use the crop if we have it; fall back to the raw upload otherwise.
    image_for_hash = crop_bytes if crop_bytes else raw_bytes

    # Stage 2: pHash lookup
    if not await asyncio.to_thread(has_index):
        # Index isn't built yet — log once, immediately fall back to Ximilar so
        # the server is still useful even before the offline build has run.
        v2_debug["phash"] = {"skipped": "index_not_built"}
        ximilar_result = await _run_ximilar_fallback(image_b64, category_id, v2_debug)
        if ximilar_result:
            ximilar_result["processing_time_ms"] = _elapsed(t_start)
            ximilar_result.setdefault("debug", {}).update({
                "engines_used": ["ximilar"],
                "v2_fallback": "phash_index_missing",
            })
            out = _stamp(ximilar_result, category_id, v2_debug)
            _save_v2_history(out)
            return out
        result = asdict(ScanResult(
            status="ERROR",
            error="pHash index not built and Ximilar unavailable. Run scripts/build_phash_index.py.",
            processing_time_ms=_elapsed(t_start),
        ))
        out = _stamp(result, category_id, v2_debug)
        _save_v2_history(out)
        return out

    t_phash = time.monotonic()
    phash_value, matches = await asyncio.to_thread(lookup, image_for_hash, top_n=5)
    phash_source = "crop" if crop_bytes else "raw"
    # If we warped and the best distance is still weak, also try the raw
    # input. Card detection's perspective transform can introduce small
    # resampling shifts that move the pHash even when the content is right;
    # the raw image may produce a tighter match.
    if crop_bytes and (not matches or matches[0].distance > MEDIUM_THRESHOLD):
        raw_phash_value, raw_matches = await asyncio.to_thread(lookup, raw_bytes, top_n=5)
        if raw_matches and (not matches or raw_matches[0].distance < matches[0].distance):
            v2_debug["raw_image_preferred"] = {
                "crop_distance": matches[0].distance if matches else None,
                "raw_distance": raw_matches[0].distance,
            }
            phash_value = raw_phash_value
            phash_source = "raw"
            matches = raw_matches
    v2_debug["stages_ms"]["phash"] = _elapsed(t_phash)
    v2_debug["phash"] = {
        "value": phash_value,
        "source": phash_source,
        "top": [
            {
                "name": m.entry.name, "number": m.entry.number,
                "set_name": m.entry.set_name, "distance": m.distance,
                "confidence": m.confidence,
            }
            for m in matches[:5]
        ],
    }

    if not matches:
        # Lookup produced nothing (empty index or undecodeable image). Fall back.
        ximilar_result = await _run_ximilar_fallback(image_b64, category_id, v2_debug)
        if ximilar_result:
            ximilar_result["processing_time_ms"] = _elapsed(t_start)
            ximilar_result.setdefault("debug", {}).update({
                "engines_used": ["ximilar"], "v2_fallback": "phash_no_match",
            })
            out = _stamp(ximilar_result, category_id, v2_debug)
            _save_v2_history(out)
            return out
        result = asdict(ScanResult(
            status="NO_MATCH", error="No pHash match and Ximilar unavailable",
            processing_time_ms=_elapsed(t_start),
        ))
        out = _stamp(result, category_id, v2_debug)
        _save_v2_history(out)
        return out

    top_match = matches[0]

    # Stage 3: Ximilar fallback only when pHash confidence is LOW
    ximilar_result: Optional[dict] = None
    if top_match.confidence == "LOW":
        ximilar_result = await _run_ximilar_fallback(image_b64, category_id, v2_debug)

    # If Ximilar ran and disagrees with pHash top (different name OR different
    # number-core), we upgrade to AMBIGUOUS and include both as candidates.
    x_sig = _ximilar_top_sig(ximilar_result)
    p_sig = _phash_top_sig(matches)
    phash_ximilar_agree = (x_sig is not None and p_sig is not None and x_sig == p_sig)
    v2_debug["phash_ximilar_agree"] = phash_ximilar_agree

    # Build candidates. pHash matches come first when confident; when LOW and
    # Ximilar ran, we merge the two top results.
    candidates: list[ScoredCandidate] = [_phash_match_to_candidate(m) for m in matches]

    # Stage 4: price enrichment for the pHash top candidate. Do this before
    # inserting a disagreeing Ximilar candidate so price/image data cannot be
    # applied to the wrong card.
    await _enrich_top_candidate(candidates, matches, category_id, v2_debug)
    if ximilar_result:
        _merge_ximilar_candidate(
            candidates,
            ximilar_result,
            prefer_ximilar_top=(top_match.confidence == "LOW" and not phash_ximilar_agree),
        )

    # Stage 5: decide the final status
    top = candidates[0]
    if top_match.confidence == "HIGH":
        status = "MATCHED"
    elif top_match.confidence == "MEDIUM" and phash_ximilar_agree:
        status = "MATCHED"
    elif top_match.confidence == "MEDIUM":
        status = "MATCHED"  # pHash is usually right even at MEDIUM
    elif top_match.confidence == "LOW" and phash_ximilar_agree:
        status = "MATCHED"
    else:
        status = "AMBIGUOUS"

    result = ScanResult(
        status=status,
        best_match=asdict(top),
        candidates=[asdict(c) for c in candidates[:10]],
        extracted_fields=None,
        disambiguation_method="phash" if not ximilar_result else "phash+ximilar",
        processing_time_ms=_elapsed(t_start),
    )
    engines_used = ["phash"]
    if ximilar_result:
        engines_used.append("ximilar")
    result_dict = asdict(result)
    result_dict.setdefault("debug", {})
    result_dict["debug"]["engines_used"] = engines_used
    out = _stamp(result_dict, category_id, v2_debug)

    logger.info(
        "[degen_eye_v2] status=%s top=%s #%s d=%d (%s) total=%.0fms price=%s",
        status, top.name, top.number,
        top_match.distance, top_match.confidence,
        out["processing_time_ms"], v2_debug.get("price_source"),
    )
    _save_v2_history(out)
    return out


# ---------------------------------------------------------------------------
# Streaming pipeline (Server-Sent Events)
# ---------------------------------------------------------------------------

async def run_v2_pipeline_stream(
    image_b64: str, category_id: str = "3",
) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    """Yield ``(event_name, payload)`` tuples as each stage completes.

    The caller serializes to SSE format. Keeping the generator transport-
    agnostic lets us reuse this from WebSocket or test code without rewriting.

    Events:
        detected   — card located + cropped
        identified — pHash (or Ximilar fallback) produced a best match
        price      — TCGTracking price populated
        variants   — variant + condition pricing available
        done       — final ScanResult dict (shape-compatible with v1)
        error      — unrecoverable failure (details in payload.error)
    """
    t_start = time.monotonic()
    v2_debug: dict[str, Any] = {"stages_ms": {}}

    raw_bytes = _b64_to_bytes(image_b64)
    if raw_bytes is None:
        yield ("error", {"error": "Invalid base64 image"})
        result = asdict(ScanResult(
            status="ERROR",
            error="Invalid base64 image",
            processing_time_ms=_elapsed(t_start),
        ))
        _save_v2_history(_stamp(result, category_id, v2_debug))
        return

    # --- Stage 1: detect + crop ---
    t_detect = time.monotonic()
    crop_bytes, detect_debug = await asyncio.to_thread(detect_and_crop, raw_bytes)
    v2_debug["stages_ms"]["detect"] = _elapsed(t_detect)
    v2_debug["detect"] = detect_debug
    image_for_hash = crop_bytes if crop_bytes else raw_bytes
    yield ("detected", {
        "found": bool(crop_bytes),
        "reason": detect_debug.get("reason"),
        "elapsed_ms": v2_debug["stages_ms"]["detect"],
    })

    # --- Stage 2: pHash lookup (or skip straight to Ximilar) ---
    matches: list[PhashMatch] = []
    ximilar_result: Optional[dict] = None
    phash_source = "crop" if crop_bytes else "raw"
    if await asyncio.to_thread(has_index):
        t_phash = time.monotonic()
        phash_value, matches = await asyncio.to_thread(lookup, image_for_hash, top_n=5)
        if crop_bytes and (not matches or matches[0].distance > MEDIUM_THRESHOLD):
            raw_phash_value, raw_matches = await asyncio.to_thread(lookup, raw_bytes, top_n=5)
            if raw_matches and (not matches or raw_matches[0].distance < matches[0].distance):
                v2_debug["raw_image_preferred"] = {
                    "crop_distance": matches[0].distance if matches else None,
                    "raw_distance": raw_matches[0].distance,
                }
                phash_value = raw_phash_value
                phash_source = "raw"
                matches = raw_matches
        v2_debug["stages_ms"]["phash"] = _elapsed(t_phash)
        v2_debug["phash"] = {
            "value": phash_value,
            "source": phash_source,
            "top": [
                {
                    "name": m.entry.name, "number": m.entry.number,
                    "set_name": m.entry.set_name, "distance": m.distance,
                    "confidence": m.confidence,
                }
                for m in matches[:5]
            ],
        }
    else:
        v2_debug["phash"] = {"skipped": "index_not_built"}

    if matches:
        top_match = matches[0]
        # For LOW confidence, also run Ximilar before we emit identified so the
        # user's first visible identity is the ensemble's best guess.
        if top_match.confidence == "LOW":
            ximilar_result = await _run_ximilar_fallback(image_b64, category_id, v2_debug)
        candidates = [_phash_match_to_candidate(m) for m in matches]
        x_sig = _ximilar_top_sig(ximilar_result)
        p_sig = _phash_top_sig(matches)
        phash_ximilar_agree = (x_sig is not None and p_sig is not None and x_sig == p_sig)
        v2_debug["phash_ximilar_agree"] = phash_ximilar_agree
        if ximilar_result:
            _merge_ximilar_candidate(
                candidates,
                ximilar_result,
                prefer_ximilar_top=(top_match.confidence == "LOW" and not phash_ximilar_agree),
            )
        top = candidates[0]
        yield ("identified", {
            "name": top.name, "number": top.number, "set_name": top.set_name,
            "confidence": top.confidence, "score": top.score,
            "source": top.source,
            "distance": top_match.distance if top.source == "phash" else None,
            "elapsed_ms": v2_debug["stages_ms"].get("phash"),
        })
    else:
        # No pHash match — fall back to Ximilar
        ximilar_result = await _run_ximilar_fallback(image_b64, category_id, v2_debug)
        if ximilar_result:
            bm = ximilar_result.get("best_match") or {}
            yield ("identified", {
                "name": bm.get("name"), "number": bm.get("number"),
                "set_name": bm.get("set_name"), "confidence": bm.get("confidence"),
                "score": bm.get("score"), "source": "ximilar",
                "elapsed_ms": v2_debug.get("ximilar_fallback_elapsed_ms"),
            })
            ximilar_result["processing_time_ms"] = _elapsed(t_start)
            ximilar_result.setdefault("debug", {}).update({
                "engines_used": ["ximilar"], "v2_fallback": "phash_no_match",
            })
            stamped = _stamp(ximilar_result, category_id, v2_debug)
            _save_v2_history(stamped)
            yield ("done", stamped)
            return
        # Nothing worked
        yield ("error", {"error": "No pHash match and Ximilar unavailable"})
        result = asdict(ScanResult(
            status="NO_MATCH",
            error="No pHash match and Ximilar unavailable",
            processing_time_ms=_elapsed(t_start),
        ))
        stamped = _stamp(result, category_id, v2_debug)
        _save_v2_history(stamped)
        yield ("done", stamped)
        return

    # --- Stage 3: price enrichment ---
    if candidates and candidates[0].source == "phash":
        await _enrich_top_candidate(candidates, matches, category_id, v2_debug)
    top = candidates[0]

    yield ("price", {
        "market_price": top.market_price,
        "tcgplayer_url": top.tcgplayer_url,
        "image_url": top.image_url,
        "elapsed_ms": v2_debug.get("price_elapsed_ms"),
        "source": v2_debug.get("price_source") or top.source,
    })

    yield ("variants", {
        "available_variants": list(top.available_variants or []),
    })

    top_match = matches[0]
    phash_ximilar_agree = bool(v2_debug.get("phash_ximilar_agree"))

    if top_match.confidence in ("HIGH", "MEDIUM"):
        status = "MATCHED"
    elif top_match.confidence == "LOW" and phash_ximilar_agree:
        status = "MATCHED"
    else:
        status = "AMBIGUOUS"

    result = ScanResult(
        status=status,
        best_match=asdict(top),
        candidates=[asdict(c) for c in candidates[:10]],
        extracted_fields=None,
        disambiguation_method="phash" if not ximilar_result else "phash+ximilar",
        processing_time_ms=_elapsed(t_start),
    )
    engines_used = ["phash"]
    if ximilar_result:
        engines_used.append("ximilar")
    result_dict = asdict(result)
    result_dict.setdefault("debug", {})
    result_dict["debug"]["engines_used"] = engines_used
    final = _stamp(result_dict, category_id, v2_debug)
    _save_v2_history(final)
    yield ("done", final)
