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
import logging
import time
from dataclasses import asdict
from typing import Any, AsyncIterator, Optional

from .card_detect import detect_and_crop
from .config import get_settings
from .phash_scanner import (
    HIGH_THRESHOLD,
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
    _save_to_history,
)
from .price_cache import get_price_for_match

logger = logging.getLogger(__name__)


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
    """Run v1's Ximilar pipeline when pHash is weak or the index is missing."""
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
        return _stamp(result, category_id, v2_debug)

    # Stage 1: card detection + rectification
    t_detect = time.monotonic()
    crop_bytes, detect_debug = detect_and_crop(raw_bytes)
    v2_debug["stages_ms"]["detect"] = _elapsed(t_detect)
    v2_debug["detect"] = detect_debug

    # Use the crop if we have it; fall back to the raw upload otherwise.
    image_for_hash = crop_bytes if crop_bytes else raw_bytes

    # Stage 2: pHash lookup
    if not has_index():
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
            _save_to_history(out)
            return out
        result = asdict(ScanResult(
            status="ERROR",
            error="pHash index not built and Ximilar unavailable. Run scripts/build_phash_index.py.",
            processing_time_ms=_elapsed(t_start),
        ))
        return _stamp(result, category_id, v2_debug)

    t_phash = time.monotonic()
    phash_value, matches = lookup(image_for_hash, top_n=5)
    # If we warped and the best distance is still weak, also try the raw
    # input. Card detection's perspective transform can introduce small
    # resampling shifts that move the pHash even when the content is right;
    # the raw image may produce a tighter match.
    if crop_bytes and (not matches or matches[0].distance > HIGH_THRESHOLD):
        _, raw_matches = lookup(raw_bytes, top_n=5)
        if raw_matches and (not matches or raw_matches[0].distance < matches[0].distance):
            v2_debug["raw_image_preferred"] = {
                "crop_distance": matches[0].distance if matches else None,
                "raw_distance": raw_matches[0].distance,
            }
            matches = raw_matches
    v2_debug["stages_ms"]["phash"] = _elapsed(t_phash)
    v2_debug["phash"] = {
        "value": phash_value,
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
            _save_to_history(out)
            return out
        result = asdict(ScanResult(
            status="NO_MATCH", error="No pHash match and Ximilar unavailable",
            processing_time_ms=_elapsed(t_start),
        ))
        return _stamp(result, category_id, v2_debug)

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

    if ximilar_result:
        x_best = (ximilar_result.get("best_match") or {})
        x_num = x_best.get("number") or ""
        x_name = x_best.get("name") or ""
        already = any(
            (c.name or "").strip().lower() == x_name.strip().lower()
            and (c.number or "").split("/")[0].lstrip("0") == x_num.split("/")[0].lstrip("0")
            for c in candidates
        )
        if x_name and not already:
            candidates.insert(0 if top_match.confidence == "LOW" else 1, ScoredCandidate(
                id=x_best.get("id") or "",
                name=x_name, number=x_num,
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
            ))

    # Stage 4: price enrichment for the top candidate
    await _enrich_top_candidate(candidates, matches, category_id, v2_debug)

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
    _save_to_history(out)
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
        return

    # --- Stage 1: detect + crop ---
    t_detect = time.monotonic()
    crop_bytes, detect_debug = detect_and_crop(raw_bytes)
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
    if has_index():
        t_phash = time.monotonic()
        phash_value, matches = lookup(image_for_hash, top_n=5)
        if crop_bytes and (not matches or matches[0].distance > HIGH_THRESHOLD):
            _, raw_matches = lookup(raw_bytes, top_n=5)
            if raw_matches and (not matches or raw_matches[0].distance < matches[0].distance):
                v2_debug["raw_image_preferred"] = {
                    "crop_distance": matches[0].distance if matches else None,
                    "raw_distance": raw_matches[0].distance,
                }
                matches = raw_matches
        v2_debug["stages_ms"]["phash"] = _elapsed(t_phash)
        v2_debug["phash"] = {
            "value": phash_value,
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
        top = candidates[0]
        yield ("identified", {
            "name": top.name, "number": top.number, "set_name": top.set_name,
            "confidence": top.confidence, "score": top.score,
            "source": "phash",
            "distance": top_match.distance,
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
            _save_to_history(stamped)
            yield ("done", stamped)
            return
        # Nothing worked
        yield ("error", {"error": "No pHash match and Ximilar unavailable"})
        result = asdict(ScanResult(
            status="NO_MATCH",
            error="No pHash match and Ximilar unavailable",
            processing_time_ms=_elapsed(t_start),
        ))
        yield ("done", _stamp(result, category_id, v2_debug))
        return

    # --- Stage 3: price enrichment ---
    candidates = [_phash_match_to_candidate(m) for m in matches]
    await _enrich_top_candidate(candidates, matches, category_id, v2_debug)
    top = candidates[0]

    yield ("price", {
        "market_price": top.market_price,
        "tcgplayer_url": top.tcgplayer_url,
        "image_url": top.image_url,
        "elapsed_ms": v2_debug.get("price_elapsed_ms"),
        "source": v2_debug.get("price_source"),
    })

    yield ("variants", {
        "available_variants": list(top.available_variants or []),
    })

    # Merge Ximilar fallback into candidate list if present
    if ximilar_result:
        x_best = (ximilar_result.get("best_match") or {})
        x_name = x_best.get("name") or ""
        x_num = x_best.get("number") or ""
        already = any(
            (c.name or "").strip().lower() == x_name.strip().lower()
            and (c.number or "").split("/")[0].lstrip("0") == x_num.split("/")[0].lstrip("0")
            for c in candidates
        )
        if x_name and not already:
            candidates.insert(1, ScoredCandidate(
                id=x_best.get("id") or "",
                name=x_name, number=x_num,
                set_id=x_best.get("set_id") or "",
                set_name=x_best.get("set_name") or "",
                image_url=x_best.get("image_url") or "",
                image_url_small=x_best.get("image_url_small") or "",
                source="ximilar",
                score=float(x_best.get("score") or 50.0),
                confidence=x_best.get("confidence") or "MEDIUM",
                match_reason="ximilar fallback",
                tcgplayer_url=x_best.get("tcgplayer_url"),
            ))

    top_match = matches[0]
    x_sig = _ximilar_top_sig(ximilar_result)
    p_sig = _phash_top_sig(matches)
    phash_ximilar_agree = (x_sig is not None and p_sig is not None and x_sig == p_sig)
    v2_debug["phash_ximilar_agree"] = phash_ximilar_agree

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
    _save_to_history(final)
    yield ("done", final)
