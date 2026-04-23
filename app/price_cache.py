"""Degen Eye v2 — price fetch with aggressive pre-warming.

Thin wrapper over v1's ``_enrich_price_fast``. The real cache backing
(``_tcgtracking_cache``) already lives in ``app/pokemon_scanner.py``, so we
just drive it on v2's behalf:

- At startup we pre-fetch the top N most-recent Pokemon sets from
  TCGTracking so the in-memory dict is hot before the first scan lands.
- At lookup time we build a ``ScoredCandidate`` from the pHash match and
  hand it to ``_enrich_price_fast``, which reads the cache when it can
  and falls through to a live call if not.
- A background task refreshes the pre-warmed sets every 24h because
  TCGTracking market prices change daily.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict
from typing import Any, Optional

import httpx

from .config import get_settings
from .pokemon_scanner import (
    TCGTRACKING_BASE,
    ScoredCandidate,
    _cache_tcgtracking,
    _enrich_price_fast,
    _tcgtracking_cache,
)
from .phash_scanner import PhashMatch

logger = logging.getLogger(__name__)


# Pokemon (cat 3) and Pokemon Japan (cat 85) — the two categories the v2
# scanner targets at launch. Other TCGs can be added later.
_WARMUP_CATEGORIES = ("3", "85")
_WARMUP_TOP_N = 20
_WARMUP_CONCURRENCY = 4
_REFRESH_INTERVAL_SECONDS = 24 * 60 * 60  # 24 hours

_warm_task: Optional[asyncio.Task] = None
_last_warm_at: float = 0.0


async def get_price_for_match(
    match: PhashMatch, *, category_id: str = "3",
) -> dict[str, Any]:
    """Return price + variants + images for a pHash match.

    Shape:
        {
            "market_price": float | None,
            "tcgplayer_url": str | None,
            "image_url": str | None,
            "image_url_small": str | None,
            "variants": list[dict],   # name + price + low_price + conditions
            "source": "cache" | "live" | "none",
            "elapsed_ms": float,
        }
    """
    settings = get_settings()
    ptcg_key = settings.pokemon_tcg_api_key or ""

    t_start = time.monotonic()
    # Snapshot the cache size before the fetch so we can tell whether
    # _enrich_price_fast served from cache (no new entry) or went live
    # (a new entry appeared).
    cache_before = len(_tcgtracking_cache)

    candidate = ScoredCandidate(
        id=match.entry.card_id,
        name=match.entry.name,
        number=match.entry.number,
        set_id=match.entry.set_id,
        set_name=match.entry.set_name,
        image_url=match.entry.image_url or "",
        image_url_small=match.entry.image_url or "",
        source="phash",
        tcgplayer_url=match.entry.tcgplayer_url,
    )

    try:
        await _enrich_price_fast(candidate, ptcg_key=ptcg_key, category_id=category_id)
    except Exception as exc:
        logger.warning("[price_cache] _enrich_price_fast failed for %s: %s", candidate.name, exc)

    cache_after = len(_tcgtracking_cache)
    cache_hit = cache_after == cache_before

    return {
        "market_price": candidate.market_price,
        "tcgplayer_url": candidate.tcgplayer_url,
        "image_url": candidate.image_url,
        "image_url_small": candidate.image_url_small,
        "variants": list(candidate.available_variants or []),
        "source": "cache" if cache_hit else ("live" if candidate.market_price is not None else "none"),
        "elapsed_ms": round((time.monotonic() - t_start) * 1000, 1),
    }


# ---------------------------------------------------------------------------
# Pre-warm
# ---------------------------------------------------------------------------

async def _fetch_recent_sets(
    client: httpx.AsyncClient, cat_id: str, top_n: int,
) -> list[dict]:
    """Return the N most-recent sets for a TCGTracking category."""
    try:
        resp = await client.get(
            f"{TCGTRACKING_BASE}/{cat_id}/sets/search",
            params={"q": "", "limit": str(max(top_n * 2, 50))},
            timeout=20.0,
        )
        if resp.status_code != 200:
            logger.warning(
                "[price_cache] TCGTracking set listing HTTP %s for category=%s: %s",
                resp.status_code, cat_id, resp.text[:200],
            )
            return []
        sets = resp.json().get("sets") or []
    except Exception as exc:
        logger.warning("[price_cache] set listing failed (cat=%s): %s", cat_id, exc)
        return []

    # Prefer ``released_on`` descending if present, else preserve order.
    def _key(s: dict) -> str:
        return str(s.get("released_on") or s.get("release_date") or s.get("name") or "")

    sets.sort(key=_key, reverse=True)
    return sets[:top_n]


async def _warm_one_set(
    client: httpx.AsyncClient, cat_id: str, set_info: dict, sem: asyncio.Semaphore,
) -> bool:
    async with sem:
        set_id = set_info.get("id")
        set_name = set_info.get("name") or ""
        if not set_id:
            return False
        set_key = f"{cat_id}:{set_name.lower()}"
        if set_key in _tcgtracking_cache:
            return True
        try:
            prod_resp, price_resp, sku_resp = await asyncio.gather(
                client.get(f"{TCGTRACKING_BASE}/{cat_id}/sets/{set_id}", timeout=15.0),
                client.get(f"{TCGTRACKING_BASE}/{cat_id}/sets/{set_id}/pricing", timeout=15.0),
                client.get(f"{TCGTRACKING_BASE}/{cat_id}/sets/{set_id}/skus", timeout=15.0),
                return_exceptions=True,
            )
        except Exception as exc:
            logger.debug("[price_cache] warm gather failed for %s/%s: %s", cat_id, set_id, exc)
            return False

        def _safe_json(r, key: str, default):
            if isinstance(r, Exception) or r.status_code != 200:
                return default
            try:
                return r.json().get(key, default)
            except Exception:
                return default

        products = _safe_json(prod_resp, "products", [])
        pricing = _safe_json(price_resp, "prices", {})
        skus = _safe_json(sku_resp, "products", {})
        if not products:
            logger.debug("[price_cache] warm: %s/%s returned no products", cat_id, set_id)
            return False

        _cache_tcgtracking(set_key, {
            "set_id": set_id, "cat_id": cat_id,
            "products": products, "pricing": pricing, "skus": skus,
        })
        return True


async def warm_price_cache(
    *,
    categories: tuple[str, ...] = _WARMUP_CATEGORIES,
    top_n: int = _WARMUP_TOP_N,
    concurrency: int = _WARMUP_CONCURRENCY,
) -> dict[str, Any]:
    """Populate ``_tcgtracking_cache`` with the top-N most-recent sets per category.

    Safe to call repeatedly — sets already cached are skipped.
    """
    global _last_warm_at
    t_start = time.monotonic()
    stats = {"categories": {}, "total_warmed": 0, "elapsed_ms": 0}
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:
        for cat_id in categories:
            sets = await _fetch_recent_sets(client, cat_id, top_n)
            if not sets:
                stats["categories"][cat_id] = {"fetched_sets": 0, "warmed": 0}
                continue
            results = await asyncio.gather(
                *[_warm_one_set(client, cat_id, s, sem) for s in sets],
                return_exceptions=True,
            )
            warmed = sum(1 for r in results if r is True)
            stats["categories"][cat_id] = {
                "fetched_sets": len(sets),
                "warmed": warmed,
            }
            stats["total_warmed"] += warmed

    _last_warm_at = time.monotonic()
    stats["elapsed_ms"] = round((time.monotonic() - t_start) * 1000, 1)
    logger.info(
        "[price_cache] Pre-warm complete: %d sets across %d categories in %.0fms",
        stats["total_warmed"], len(categories), stats["elapsed_ms"],
    )
    return stats


def start_background_warm_refresh(
    *, initial_delay_seconds: float = 5.0,
    interval_seconds: int = _REFRESH_INTERVAL_SECONDS,
) -> None:
    """Fire-and-forget background task: initial warm + daily refresh.

    Intended to be called once from the FastAPI lifespan hook. Reads from
    the running event loop; exits silently if no loop is active (e.g.
    import-time).
    """
    global _warm_task

    if _warm_task is not None and not _warm_task.done():
        return

    async def _loop() -> None:
        await asyncio.sleep(max(0.0, initial_delay_seconds))
        try:
            await warm_price_cache()
        except Exception as exc:
            logger.warning("[price_cache] initial warm failed: %s", exc)
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                logger.info("[price_cache] scheduled refresh kicking off")
                await warm_price_cache()
            except Exception as exc:
                logger.warning("[price_cache] scheduled refresh failed: %s", exc)

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        logger.debug("[price_cache] no event loop; skipping background warm")
        return

    _warm_task = loop.create_task(_loop())


def get_warm_stats() -> dict[str, Any]:
    """For admin / debug pages."""
    now = time.monotonic()
    return {
        "last_warm_at_monotonic": _last_warm_at,
        "seconds_since_warm": (now - _last_warm_at) if _last_warm_at else None,
        "cache_entries": len(_tcgtracking_cache),
        "sample_keys": list(_tcgtracking_cache.keys())[:20],
    }
