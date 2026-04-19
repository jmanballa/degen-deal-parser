"""
Pokemon card scanning pipeline.

Multi-stage pipeline: Capture -> Preprocess (Pillow) -> OCR (Google Vision)
-> Lookup (TCGdex + PokemonTCG) -> Score -> Disambiguate (OpenAI Vision) -> Result.

Designed for Pokemon cards only. Identification works backward from the
collector number, which uniquely identifies a card within a set.
"""
from __future__ import annotations

import asyncio
import base64
import collections
import hashlib
import io
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import httpx
from .ai_client import (
    get_ai_client,
    get_fast_model,
    get_gemini_flash_model,
    get_haiku_model,
    get_model,
    get_tiebreaker_client,
    get_tiebreaker_model,
    has_ai_key,
    has_tiebreaker_key,
)
from .config import get_settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

@dataclass
class ExtractedFields:
    collector_number: Optional[str] = None
    collector_number_raw: Optional[str] = None
    card_name: Optional[str] = None
    set_name: Optional[str] = None
    language: Optional[str] = None
    variant_hints: list[str] = field(default_factory=list)
    hp_value: Optional[str] = None
    ocr_raw_text: str = ""
    ocr_confidence: float = 0.0
    extraction_method: str = "regex"  # "ai" | "regex"
    extraction_warnings: list[str] = field(default_factory=list)


@dataclass
class CandidateCard:
    id: str = ""
    name: str = ""
    number: str = ""
    set_id: str = ""
    set_name: str = ""
    image_url: str = ""
    image_url_small: str = ""
    rarity: Optional[str] = None
    variant: Optional[str] = None
    source: str = ""  # "tcgdex" | "pokemontcg"
    market_price: Optional[float] = None
    tcgplayer_url: Optional[str] = None
    available_variants: list[dict] = field(default_factory=list)


@dataclass
class ScoredCandidate(CandidateCard):
    score: float = 0.0
    confidence: str = "LOW"  # "HIGH" | "MEDIUM" | "LOW"
    score_breakdown: dict[str, float] = field(default_factory=dict)
    match_reason: str = ""


@dataclass
class ScanResult:
    status: str = "ERROR"  # "MATCHED" | "AMBIGUOUS" | "NO_MATCH" | "ERROR"
    best_match: Optional[dict] = None
    candidates: list[dict] = field(default_factory=list)
    extracted_fields: Optional[dict] = None
    disambiguation_method: Optional[str] = None
    processing_time_ms: float = 0.0
    debug: dict = field(default_factory=dict)
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCORING_WEIGHTS = {
    "exact_collector_number": 50,
    "exact_full_number": 60,
    "fuzzy_name_similarity": 25,
    "set_consistency": 15,
    "variant_consistency": 10,
    "ocr_confidence_bonus": 5,
}

XIMILAR_TCG_URL = "https://api.ximilar.com/collectibles/v2/tcg_id"
TCGDEX_BASE = "https://api.tcgdex.net/v2/en"
POKEMONTCG_BASE = "https://api.pokemontcg.io/v2"
SCRYFALL_BASE = "https://api.scryfall.com"
YGOPRODECK_BASE = "https://db.ygoprodeck.com/api/v7"
OPTCG_BASE = "https://optcgapi.com/api"
LORCAST_BASE = "https://api.lorcast.com/v0"

_tcgdex_sets_cache: list[dict] | None = None

# In-memory ring buffer of recent scan results for debugging
_scan_history: collections.deque[dict] = collections.deque(maxlen=25)

# Background OCR validation: scan_id -> (timestamp, updated_result_or_None)
_pending_validations: dict[str, tuple[float, dict | None]] = {}
_VALIDATION_TTL = 300  # expire pending validations after 5 minutes
_VALIDATION_MAX = 200  # hard cap to prevent unbounded growth under rapid scanning

# Image-hash -> (timestamp, result) cache. Keyed by (mode, category_id, sha256)
# so identical re-scans (employee tests, accidental double-taps at the show)
# skip the full Ximilar + AI fan-out. Only completed results are cached — we
# never cache optimistic/"validation_pending" entries, since those still have
# a background ensemble in flight and re-serving them would break the poll.
_scan_result_cache: dict[tuple[str, str, str], tuple[float, dict]] = {}
_SCAN_CACHE_TTL = 300  # 5 minutes
_SCAN_CACHE_MAX = 100

# Bounded concurrency across all external engine calls (Ximilar / Claude /
# Gemini). Protects our API budgets under bursty scanning — rip-and-ship
# streams and show days can issue many scans in parallel, and un-throttled
# fan-out has spiked quota usage before.
_EXTERNAL_API_SEMAPHORE: asyncio.Semaphore | None = None
_EXTERNAL_API_CONCURRENCY = 6


def _get_external_api_semaphore() -> asyncio.Semaphore:
    # Lazily constructed so it binds to the running event loop, not import-time.
    global _EXTERNAL_API_SEMAPHORE
    if _EXTERNAL_API_SEMAPHORE is None:
        _EXTERNAL_API_SEMAPHORE = asyncio.Semaphore(_EXTERNAL_API_CONCURRENCY)
    return _EXTERNAL_API_SEMAPHORE


def _hash_image(image_b64: str) -> str:
    return hashlib.sha256(image_b64.encode("utf-8", errors="ignore")).hexdigest()


def _scan_cache_get(key: tuple[str, str, str]) -> dict | None:
    entry = _scan_result_cache.get(key)
    if entry is None:
        return None
    ts, result = entry
    if time.monotonic() - ts > _SCAN_CACHE_TTL:
        _scan_result_cache.pop(key, None)
        return None
    return result


def _scan_cache_put(key: tuple[str, str, str], result: dict) -> None:
    # Never cache in-flight / optimistic results — the poll endpoint still
    # needs to drive the scan_id through the real _pending_validations map.
    if result.get("validation_pending"):
        return
    if result.get("status") in (None, "ERROR"):
        return
    if len(_scan_result_cache) >= _SCAN_CACHE_MAX:
        try:
            oldest = min(_scan_result_cache, key=lambda k: _scan_result_cache[k][0])
            _scan_result_cache.pop(oldest, None)
        except ValueError:
            pass
    _scan_result_cache[key] = (time.monotonic(), result)


def _insert_pending_validation(scan_id: str, entry: tuple[float, dict | None]) -> None:
    """Insert into _pending_validations with TTL cleanup and hard-cap eviction."""
    _cleanup_stale_validations()
    if len(_pending_validations) >= _VALIDATION_MAX:
        # Evict the oldest entry (smallest timestamp) to stay under cap.
        try:
            oldest_key = min(_pending_validations, key=lambda k: _pending_validations[k][0])
            _pending_validations.pop(oldest_key, None)
            logger.warning(
                "[pokemon_scanner] _pending_validations hit cap=%d; evicted %s",
                _VALIDATION_MAX, oldest_key,
            )
        except ValueError:
            pass
    _pending_validations[scan_id] = entry


def _cleanup_stale_validations() -> None:
    """Remove expired entries to prevent unbounded memory growth."""
    now = time.monotonic()
    stale = [k for k, (ts, _) in _pending_validations.items() if now - ts > _VALIDATION_TTL]
    for k in stale:
        _pending_validations.pop(k, None)

# Confidence thresholds for the tiered pipeline
XIMILAR_CONFIDENCE_HIGH = 0.85   # ≥ this → accept Ximilar, skip OCR
XIMILAR_CONFIDENCE_MEDIUM = 0.60 # ≥ this → optimistic return, background OCR


def _loads_ai_json(raw: str) -> Optional[dict]:
    """Parse a JSON object from an AI response, tolerating markdown fences.

    Used across all ``chat.completions`` call sites because NVIDIA's
    OpenAI-compatible endpoint (which serves Claude) does not support
    ``response_format={"type": "json_object"}``. Returns None on failure.
    """
    if not raw:
        return None
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        parsed = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


# ---------------------------------------------------------------------------
# Stage D: TCGdex + PokemonTCG database lookup (used by text search and the
# vision pipeline's precise identification step).
# ---------------------------------------------------------------------------

async def _fetch_tcgdex_sets() -> list[dict]:
    """Fetch and cache the TCGdex sets list."""
    global _tcgdex_sets_cache
    if _tcgdex_sets_cache is not None:
        return _tcgdex_sets_cache

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{TCGDEX_BASE}/sets")
        if resp.status_code == 200:
            _tcgdex_sets_cache = resp.json()
        else:
            _tcgdex_sets_cache = []
            logger.warning("[pokemon_scanner] TCGdex sets fetch failed: HTTP %s", resp.status_code)

    return _tcgdex_sets_cache


def _infer_set_id(set_name: Optional[str], sets: list[dict]) -> Optional[str]:
    """Try to match extracted set name to a TCGdex set ID.

    Also tries stripping a leading character in case OCR concatenated the
    regulation mark with the set abbreviation (e.g. "JASC" -> try "ASC" too).
    """
    if not set_name:
        return None

    candidates_to_try = [set_name.lower().strip()]
    # If set name is all-caps and >= 3 chars, the first letter may be a
    # regulation mark (D-J) that OCR glued onto the abbreviation.
    stripped = set_name.strip()
    if stripped.isupper() and len(stripped) >= 3:
        candidates_to_try.append(stripped[1:].lower())

    for name_lower in candidates_to_try:
        for s in sets:
            if s.get("name", "").lower() == name_lower:
                return s.get("id")
        for s in sets:
            if name_lower in s.get("name", "").lower():
                return s.get("id")

    return None


async def _tcgdex_lookup_by_set_and_number(
    client: httpx.AsyncClient, set_id: str, local_id: str
) -> Optional[dict]:
    """Tier 1: exact set + collector number."""
    url = f"{TCGDEX_BASE}/sets/{set_id}/{local_id}"
    resp = await client.get(url)
    if resp.status_code == 200:
        return resp.json()
    return None


async def _tcgdex_search_by_name(
    client: httpx.AsyncClient, name: str, limit: int = 10,
    prefer_number: Optional[str] = None,
    prefer_set: Optional[str] = None,
) -> list[dict]:
    """
    Search TCGdex by card name, fetching full details for top results.
    If prefer_number is given, prioritize results whose localId matches.
    If prefer_set is given, prioritize results whose set name is similar.
    """
    resp = await client.get(f"{TCGDEX_BASE}/cards", params={"name": name})
    if resp.status_code != 200:
        return []

    results = resp.json()
    if not isinstance(results, list):
        return []

    # Sort results by relevance: prefer matching set, then matching number
    if prefer_number or prefer_set:
        num_prefix = ""
        if prefer_number:
            num_prefix = prefer_number.split("/")[0].lstrip("0") if "/" in prefer_number else prefer_number.lstrip("0")
        set_lower = (prefer_set or "").lower()

        set_words = [w for w in set_lower.split() if len(w) >= 3] if set_lower else []

        def sort_key(card: dict) -> tuple[int, int]:
            set_score = 1
            num_score = 1
            if set_words:
                card_id = (card.get("id", "") or "").lower()
                if any(w in card_id for w in set_words):
                    set_score = 0
            if num_prefix:
                lid = (card.get("localId") or "").lstrip("0")
                if lid == num_prefix:
                    num_score = 0
            return (set_score, num_score)

        results.sort(key=sort_key)

    # Fetch full card details for up to `limit` results
    detailed: list[dict] = []
    for slim in results[:limit]:
        card_id = slim.get("id", "")
        if not card_id:
            continue
        detail_resp = await client.get(f"{TCGDEX_BASE}/cards/{card_id}")
        if detail_resp.status_code == 200:
            detailed.append(detail_resp.json())
        else:
            slim["_slim"] = True
            detailed.append(slim)

    return detailed


def _tcgdex_to_candidate(card: dict, source: str = "tcgdex") -> CandidateCard:
    """Convert a TCGdex card object to CandidateCard."""
    image = card.get("image", "")
    image_url = f"{image}/high.webp" if image else ""
    image_url_small = f"{image}/low.webp" if image else ""

    set_info = card.get("set") or {}
    local_id = card.get("localId", "") or card.get("number", "")

    # For full card objects, set_info is a dict; for slim results it may be missing
    if isinstance(set_info, dict):
        set_id = set_info.get("id", "")
        set_name = set_info.get("name", "")
        # Build a full number like "228/217" from localId + set card count
        card_count = set_info.get("cardCount", {})
        official_count = card_count.get("official") if isinstance(card_count, dict) else None
        if official_count and local_id and "/" not in local_id:
            full_number = f"{local_id}/{official_count}"
        else:
            full_number = local_id
    else:
        set_id = str(set_info) if set_info else ""
        set_name = ""
        full_number = local_id

    # Extract pricing + available variants from TCGdex's built-in market data
    market_price = None
    tcgplayer_url = None
    available_variants: list[dict] = []
    pricing = card.get("pricing") or {}

    _TCGDEX_VARIANT_LABELS = {
        "normal": "Normal",
        "holo": "Holo",
        "reverse": "Reverse Holo",
    }

    # TCGPlayer USD pricing — extract per-variant prices
    tcgp = pricing.get("tcgplayer")
    if isinstance(tcgp, dict):
        for variant_key, label in _TCGDEX_VARIANT_LABELS.items():
            vdata = tcgp.get(variant_key)
            if isinstance(vdata, dict):
                mp = vdata.get("marketPrice")
                if mp is not None:
                    try:
                        price = round(float(mp), 2)
                        if price > 0:
                            available_variants.append({"name": label, "price": price})
                            if market_price is None:
                                market_price = price
                    except (ValueError, TypeError):
                        pass

    # Cardmarket EUR pricing — extract per-variant prices
    cm = pricing.get("cardmarket")
    if isinstance(cm, dict):
        _CM_VARIANT_MAP = [
            ("trend", "avg", "Normal"),
            ("trend-holo", "avg-holo", "Holo"),
        ]
        for trend_key, avg_key, label in _CM_VARIANT_MAP:
            raw = cm.get(trend_key) or cm.get(avg_key)
            if raw is not None:
                try:
                    eur_price = float(raw)
                    if eur_price > 0:
                        usd_price = round(eur_price * 1.10, 2)
                        # Only add if not already covered by TCGPlayer
                        if not any(v["name"] == label for v in available_variants):
                            available_variants.append({"name": label, "price": usd_price})
                        if market_price is None:
                            market_price = usd_price
                except (ValueError, TypeError):
                    pass

    return CandidateCard(
        id=card.get("id", ""),
        name=card.get("name", ""),
        number=full_number,
        set_id=set_id,
        set_name=set_name,
        image_url=image_url,
        image_url_small=image_url_small,
        rarity=card.get("rarity"),
        variant=card.get("variant"),
        source=source,
        market_price=market_price,
        tcgplayer_url=tcgplayer_url,
        available_variants=available_variants,
    )


async def _pokemontcg_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    number: Optional[str] = None,
    set_name: Optional[str] = None,
    api_key: str = "",
    limit: int = 10,
) -> list[CandidateCard]:
    """Fallback: search PokemonTCG API."""
    headers = {}
    if api_key:
        headers["X-Api-Key"] = api_key

    q_parts = []
    if name:
        q_parts.append(f'name:"{name}"')
    if number:
        clean_num = number.split("/")[0] if "/" in number else number
        q_parts.append(f'number:"{clean_num}"')
    if set_name:
        clean_set = re.sub(r'\s+Set$', '', set_name, flags=re.IGNORECASE).strip()
        q_parts.append(f'set.name:"{clean_set}*"')

    if not q_parts:
        return []

    params = {"q": " ".join(q_parts), "pageSize": str(limit)}

    resp = await client.get(f"{POKEMONTCG_BASE}/cards", params=params, headers=headers)
    if resp.status_code != 200:
        logger.warning(
            "[pokemon_scanner] PokemonTCG HTTP %s for %s (params=%r): %s",
            resp.status_code, f"{POKEMONTCG_BASE}/cards", params, resp.text[:200],
        )
        return []

    data = resp.json()
    cards = data.get("data") or []
    results = []
    for card in cards:
        images = card.get("images") or {}
        set_info = card.get("set") or {}

        prices_wrap = card.get("tcgplayer", {}).get("prices", {})
        market_price = None
        ptcg_variants: list[dict] = []

        _PTCG_VARIANT_LABELS = {
            "normal": "Normal",
            "holofoil": "Holo",
            "reverseHolofoil": "Reverse Holo",
            "1stEditionHolofoil": "1st Edition Holo",
            "1stEditionNormal": "1st Edition",
        }
        for price_type, label in _PTCG_VARIANT_LABELS.items():
            if price_type in prices_wrap:
                mp = prices_wrap[price_type].get("market")
                if mp is not None:
                    try:
                        price = round(float(mp), 2)
                        if price > 0:
                            ptcg_variants.append({"name": label, "price": price})
                            if market_price is None:
                                market_price = price
                    except (ValueError, TypeError):
                        pass

        results.append(CandidateCard(
            id=card.get("id", ""),
            name=card.get("name", ""),
            number=card.get("number", ""),
            set_id=set_info.get("id", ""),
            set_name=set_info.get("name", ""),
            image_url=images.get("large", ""),
            image_url_small=images.get("small", ""),
            rarity=card.get("rarity"),
            source="pokemontcg",
            market_price=market_price,
            tcgplayer_url=card.get("tcgplayer", {}).get("url"),
            available_variants=ptcg_variants,
        ))

    return results


# ---------------------------------------------------------------------------
# Multi-TCG search backends
# ---------------------------------------------------------------------------

async def _scryfall_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    set_name: Optional[str] = None,
    number: Optional[str] = None,
    limit: int = 10,
) -> list[CandidateCard]:
    """Search Scryfall for Magic: The Gathering cards.

    Scryfall's search syntax requires:
      - the card name wrapped in `!"..."` for an exact match (unquoted tokens
        are fulltext-matched across name/type/oracle, which spuriously fails);
      - `e:<code>` or `e:"<full set name>"` for set filters — we try both.

    Strategy (waterfall — stops at the first hit):
      1. exact name + set + number       (tight — pinpoint)
      2. exact name + number             (all prints at that number)
      3. exact name                      (all prints)
      4. fuzzy fallback via `/cards/named?fuzzy=`
    """
    if not name:
        return []

    def _q_name(n: str) -> str:
        # Drop stray double-quotes so we don't break the query ourselves.
        return f'!"{n.replace(chr(34), "")}"'

    def _q_set(s: str) -> str:
        # Use the most permissive form — quoted full name works when Scryfall
        # recognizes it; otherwise this naturally returns 0 and we fall back.
        return f'e:"{s}"' if " " in s else f"e:{s}"

    clean_num = ""
    if number:
        clean_num = number.split("/")[0].strip() if "/" in number else number.strip()

    attempts: list[list[str]] = []
    if set_name and clean_num:
        attempts.append([_q_name(name), _q_set(set_name), f"cn:{clean_num}"])
    if clean_num:
        attempts.append([_q_name(name), f"cn:{clean_num}"])
    if set_name:
        attempts.append([_q_name(name), _q_set(set_name)])
    attempts.append([_q_name(name)])

    data: list[dict] = []
    for parts in attempts:
        params = {
            "q": " ".join(parts),
            "unique": "prints",
            "order": "released",
            "dir": "desc",
        }
        try:
            resp = await client.get(f"{SCRYFALL_BASE}/cards/search", params=params)
        except Exception as exc:
            logger.warning("[pokemon_scanner] Scryfall search failed: %s", exc)
            continue
        if resp.status_code == 200:
            data = resp.json().get("data") or []
            if data:
                break
        elif resp.status_code == 404:
            # Scryfall returns 404 on "no matches" — just try the next attempt.
            continue
        else:
            logger.warning(
                "[pokemon_scanner] Scryfall HTTP %s for %s (params=%r): %s",
                resp.status_code,
                f"{SCRYFALL_BASE}/cards/search",
                params,
                resp.text[:200],
            )

    # Last-ditch: fuzzy name match. Returns a single card (or 404). Helpful
    # when the visual pipeline OCR'd a misspelling of the name.
    if not data:
        try:
            resp = await client.get(
                f"{SCRYFALL_BASE}/cards/named",
                params={"fuzzy": name},
            )
            if resp.status_code == 200:
                data = [resp.json()]
        except Exception as exc:
            logger.debug("[pokemon_scanner] Scryfall fuzzy fallback failed: %s", exc)

    results: list[CandidateCard] = []
    for card in data[:limit]:
        images = card.get("image_uris") or {}
        faces = card.get("card_faces") or []
        if not images and faces:
            images = faces[0].get("image_uris") or {}
        set_info_name = card.get("set_name", "")
        col_num = card.get("collector_number", "")
        price_str = (card.get("prices") or {}).get("usd")
        price = None
        if price_str:
            try:
                price = round(float(price_str), 2)
            except (ValueError, TypeError):
                pass
        results.append(CandidateCard(
            id=card.get("id", ""),
            name=card.get("name", ""),
            number=col_num,
            set_id=card.get("set", ""),
            set_name=set_info_name,
            image_url=images.get("normal", ""),
            image_url_small=images.get("small", ""),
            rarity=card.get("rarity"),
            source="scryfall",
            market_price=price,
            tcgplayer_url=card.get("purchase_uris", {}).get("tcgplayer"),
        ))
    return results


async def _ygoprodeck_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    set_name: Optional[str] = None,
    limit: int = 10,
) -> list[CandidateCard]:
    """Search YGOPRODeck for Yu-Gi-Oh cards."""
    if not name:
        return []
    params: dict[str, str] = {"fname": name, "num": str(limit * 2), "offset": "0"}
    if set_name:
        params["cardset"] = set_name

    try:
        resp = await client.get(f"{YGOPRODECK_BASE}/cardinfo.php", params=params)
        if resp.status_code != 200:
            logger.warning(
                "[pokemon_scanner] YGOPRODeck HTTP %s for %s (params=%r): %s",
                resp.status_code, f"{YGOPRODECK_BASE}/cardinfo.php", params, resp.text[:200],
            )
            return []
        data = resp.json().get("data") or []
    except Exception as exc:
        logger.warning("[pokemon_scanner] YGOPRODeck search failed: %s", exc)
        return []

    results: list[CandidateCard] = []
    seen: set[str] = set()
    for card in data:
        card_images = card.get("card_images") or []
        img = card_images[0].get("image_url", "") if card_images else ""
        img_small = card_images[0].get("image_url_small", "") if card_images else ""
        card_sets = card.get("card_sets") or []
        if not card_sets:
            key = card.get("name", "")
            if key in seen:
                continue
            seen.add(key)
            results.append(CandidateCard(
                id=str(card.get("id", "")),
                name=card.get("name", ""),
                number="",
                set_id="",
                set_name="",
                image_url=img,
                image_url_small=img_small,
                rarity=card.get("race"),
                source="ygoprodeck",
            ))
            continue
        for cs in card_sets:
            key = f"{card.get('name','')}|{cs.get('set_code','')}"
            if key in seen:
                continue
            seen.add(key)
            price = None
            price_str = cs.get("set_price")
            if price_str:
                try:
                    price = round(float(price_str), 2)
                    if price <= 0:
                        price = None
                except (ValueError, TypeError):
                    pass
            results.append(CandidateCard(
                id=f"{card.get('id','')}_{cs.get('set_code','')}",
                name=card.get("name", ""),
                number=cs.get("set_code", ""),
                set_id=cs.get("set_code", ""),
                set_name=cs.get("set_name", ""),
                image_url=img,
                image_url_small=img_small,
                rarity=cs.get("set_rarity"),
                source="ygoprodeck",
                market_price=price,
            ))
            if len(results) >= limit:
                break
        if len(results) >= limit:
            break
    return results[:limit]


async def _optcg_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    set_name: Optional[str] = None,
    number: Optional[str] = None,
    limit: int = 10,
) -> list[CandidateCard]:
    """Search OPTCG API for One Piece Card Game cards.

    The OPTCG API is picky:
      - ``set_name`` must match their canonical set label or you get HTTP 404
        ("Card was not found in the set card name view!"). We try with the
        set filter first, then retry without it.
      - Their card names are stored without spaces around punctuation
        (``Monkey.D.Luffy``, not ``Monkey D. Luffy``). A longer/prettier
        name from OCR or GPT-Vision returns 0 rows, so we try a few
        normalizations (dots collapsed to spaces, last token alone) before
        giving up.
      - Collector numbers are stored as ``card_set_id`` (``OP01-003``), so we
        optionally post-filter by that when available.
    """
    if not name:
        return []

    # Generate name variants from most-specific to fallback.
    name_variants: list[str] = []

    def _add(v: str) -> None:
        v = v.strip()
        if v and v not in name_variants:
            name_variants.append(v)

    _add(name)
    # "Monkey D. Luffy" -> "Monkey.D.Luffy" (how OPTCG stores names).
    _add(re.sub(r"\s+", "", name.replace(" ", "")) if "." in name else name)
    _add(re.sub(r"\s*\.\s*", ".", name))
    # Last token fallback (e.g. "Monkey D. Luffy" -> "Luffy").
    tokens = [t for t in re.split(r"[\s\.]+", name) if len(t) >= 3]
    if tokens:
        _add(tokens[-1])

    # Normalize the collector number to the expected "OPxx-yyy" form so we
    # can post-filter results.
    target_id = ""
    if number:
        n = number.upper().strip()
        n = n.replace(" ", "")
        if re.match(r"^[A-Z]{2,4}\d+-\d+$", n):
            target_id = n
        else:
            # Just digits? We can't reconstruct the set prefix reliably —
            # leave it blank and let name/set filtering carry the match.
            pass

    data: list[dict] = []
    tried: list[tuple[str, Optional[str]]] = []

    for variant in name_variants:
        for use_set in (True, False):
            if use_set and not set_name:
                continue
            params: dict[str, str] = {"card_name": variant}
            if use_set:
                params["set_name"] = set_name  # type: ignore[assignment]
            tried.append((variant, set_name if use_set else None))
            try:
                resp = await client.get(
                    f"{OPTCG_BASE}/sets/filtered/", params=params,
                )
            except Exception as exc:
                logger.warning("[pokemon_scanner] OPTCG search failed: %s", exc)
                continue
            if resp.status_code != 200:
                # 404 just means that particular filter didn't match; move on.
                if resp.status_code != 404:
                    logger.warning(
                        "[pokemon_scanner] OPTCG HTTP %s for %s (params=%r): %s",
                        resp.status_code,
                        f"{OPTCG_BASE}/sets/filtered/",
                        params,
                        resp.text[:200],
                    )
                continue
            try:
                payload = resp.json()
            except Exception:
                continue
            if isinstance(payload, list):
                data = payload
            else:
                data = payload.get("data") or payload.get("results") or []
            if data:
                break
        if data:
            break

    if not data:
        logger.info(
            "[pokemon_scanner] OPTCG no matches for name=%r set=%r (tried %s)",
            name, set_name, tried,
        )
        return []

    # If we know the exact collector number, prefer exact matches first.
    if target_id:
        exact = [c for c in data if (c.get("card_set_id") or "") == target_id]
        if exact:
            data = exact + [c for c in data if c not in exact]

    results: list[CandidateCard] = []
    seen: set[str] = set()
    for card in data:
        card_set_id = card.get("card_set_id") or ""
        card_name = card.get("card_name") or ""
        key = f"{card_name}|{card_set_id}"
        if key in seen:
            continue
        seen.add(key)

        price = None
        mp = card.get("market_price")
        if mp is not None:
            try:
                price = round(float(mp), 2)
                if price <= 0:
                    price = None
            except (ValueError, TypeError):
                pass

        results.append(CandidateCard(
            id=card_set_id,
            name=card_name,
            number=card_set_id,
            set_id=card.get("set_id", ""),
            set_name=card.get("set_name", ""),
            image_url=card.get("card_image", ""),
            image_url_small=card.get("card_image", ""),
            rarity=card.get("rarity"),
            source="optcg",
            market_price=price,
        ))
        if len(results) >= limit:
            break
    return results[:limit]


async def _lorcast_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    set_name: Optional[str] = None,
    number: Optional[str] = None,
    limit: int = 10,
) -> list[CandidateCard]:
    """Search Lorcast for Disney Lorcana cards."""
    if not name:
        return []
    q_parts = [name]
    if set_name:
        q_parts.append(f"set:{set_name}")
    if number:
        q_parts.append(f"number:{number}")

    lorcast_params = {"q": " ".join(q_parts), "unique": "prints"}
    try:
        resp = await client.get(f"{LORCAST_BASE}/cards/search", params=lorcast_params)
        if resp.status_code != 200:
            logger.warning(
                "[pokemon_scanner] Lorcast HTTP %s for %s (params=%r): %s",
                resp.status_code, f"{LORCAST_BASE}/cards/search", lorcast_params, resp.text[:200],
            )
            return []
        data = resp.json().get("results") or []
    except Exception as exc:
        logger.warning("[pokemon_scanner] Lorcast search failed: %s", exc)
        return []

    results: list[CandidateCard] = []
    for card in data[:limit]:
        images = (card.get("image_uris") or {}).get("digital") or {}
        set_info = card.get("set") or {}
        col_num = card.get("collector_number", "")
        version = card.get("version") or ""
        display_name = card.get("name", "")
        if version:
            display_name = f"{display_name} - {version}"

        price = None
        prices = card.get("prices") or {}
        for price_key in ("usd", "usd_foil"):
            val = prices.get(price_key)
            if val is not None:
                try:
                    price = round(float(val), 2)
                    if price > 0:
                        break
                    price = None
                except (ValueError, TypeError):
                    pass

        results.append(CandidateCard(
            id=card.get("id", ""),
            name=display_name,
            number=col_num,
            set_id=set_info.get("code", ""),
            set_name=set_info.get("name", ""),
            image_url=images.get("large") or images.get("normal", ""),
            image_url_small=images.get("small") or images.get("normal", ""),
            rarity=card.get("rarity"),
            source="lorcast",
            market_price=price,
            tcgplayer_url=f"https://www.tcgplayer.com/product/{card['tcgplayer_id']}" if card.get("tcgplayer_id") else None,
        ))
    return results


async def _tcgtracking_product_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    set_name: Optional[str] = None,
    category_id: str = "27",
    limit: int = 10,
) -> list[CandidateCard]:
    """Fallback: search TCGTracking by set, then match products by card name.

    For TCGs without a dedicated card-name API (Dragon Ball, etc.).
    Requires set_name to find the right set first.
    """
    if not set_name:
        return []

    search_url = f"{TCGTRACKING_BASE}/{category_id}/search"
    search_params = {"q": set_name}
    try:
        search_resp = await client.get(search_url, params=search_params)
        if search_resp.status_code != 200:
            logger.warning(
                "[pokemon_scanner] TCGTracking set search HTTP %s for %s (params=%r): %s",
                search_resp.status_code, search_url, search_params, search_resp.text[:200],
            )
            return []
        sets = search_resp.json().get("sets") or []
        if not sets:
            logger.info(
                "[pokemon_scanner] TCGTracking set search returned 0 sets for category=%s q=%r",
                category_id, set_name,
            )
            return []

        set_id = sets[0]["id"]
        prod_url = f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}"
        prod_resp = await client.get(prod_url)
        if prod_resp.status_code != 200:
            logger.warning(
                "[pokemon_scanner] TCGTracking products HTTP %s for %s: %s",
                prod_resp.status_code, prod_url, prod_resp.text[:200],
            )
            return []
        products = prod_resp.json().get("products") or []
    except Exception as exc:
        logger.warning(
            "[pokemon_scanner] TCGTracking product search failed for category=%s set=%r: %s",
            category_id, set_name, exc,
        )
        return []

    name_lower = (name or "").lower()
    results: list[CandidateCard] = []
    for prod in products:
        clean = (prod.get("clean_name") or prod.get("name") or "").lower()
        if name_lower and name_lower not in clean:
            continue
        prod_num = prod.get("number") or ""
        img_raw = prod.get("image_url") or ""
        img_large = img_raw.replace("_200w.jpg", "_400w.jpg") if img_raw else ""

        results.append(CandidateCard(
            id=str(prod.get("id", "")),
            name=prod.get("clean_name") or prod.get("name", ""),
            number=prod_num,
            set_id=str(set_id),
            set_name=sets[0].get("name", ""),
            image_url=img_large,
            image_url_small=img_raw,
            source="tcgtracking",
            tcgplayer_url=prod.get("tcgplayer_url"),
        ))
        if len(results) >= limit:
            break
    return results


async def _riftbound_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    set_name: Optional[str] = None,
    number: Optional[str] = None,
    limit: int = 10,
) -> list[CandidateCard]:
    """Search TCGTracking for Riftbound (Riot's League of Legends TCG).

    Riftbound is TCGTracking category_id 89 (``Riftbound: League of Legends
    Trading Card Game``). There's no first-party public card API yet, but
    TCGTracking ships full set + product data straight from TCGPlayer, which
    gives us cards, images, collector numbers, and live prices in one hop.

    Strategy:
      1. If the caller gave a set hint, search by that (most accurate).
      2. Otherwise crawl every Riftbound set and filter products by name
         (Riftbound has ~5 sets right now, so this is cheap). Useful when
         GPT-Vision guesses ``riftbound`` but can't read the small set
         marker in the corner.

    Also matches on collector number within each set — Riftbound prints
    numbers as ``NNN/TTT`` (``001/024`` in Origins: Proving Grounds), which
    both the card face and TCGTracking use verbatim.
    """
    if not name:
        return []

    category_id = "89"
    name_lower = name.lower().strip()
    target_num = (number or "").split("/")[0].strip().lstrip("0")

    async def _search_set(set_id: int, set_display: str) -> list[CandidateCard]:
        prod_url = f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}"
        try:
            resp = await client.get(prod_url)
            if resp.status_code != 200:
                return []
            products = resp.json().get("products") or []
        except Exception as exc:
            logger.warning(
                "[pokemon_scanner] Riftbound products fetch failed (%s): %s",
                set_id, exc,
            )
            return []

        out: list[CandidateCard] = []
        for prod in products:
            clean = (prod.get("clean_name") or prod.get("name") or "").lower()
            prod_num_raw = (prod.get("number") or "").split("/")[0].strip().lstrip("0")

            name_hit = bool(name_lower) and name_lower in clean
            num_hit = bool(target_num) and target_num == prod_num_raw
            if not (name_hit or num_hit):
                continue

            img_raw = prod.get("image_url") or ""
            img_large = (
                img_raw.replace("_200w.jpg", "_400w.jpg") if img_raw else ""
            )
            out.append(CandidateCard(
                id=str(prod.get("id", "")),
                name=prod.get("clean_name") or prod.get("name", ""),
                number=prod.get("number") or "",
                set_id=str(set_id),
                set_name=set_display,
                image_url=img_large,
                image_url_small=img_raw,
                source="tcgtracking_riftbound",
                tcgplayer_url=prod.get("tcgplayer_url"),
            ))
        return out

    # 1) Named-set lookup
    if set_name:
        search_url = f"{TCGTRACKING_BASE}/{category_id}/search"
        try:
            search_resp = await client.get(search_url, params={"q": set_name})
            if search_resp.status_code == 200:
                sets = search_resp.json().get("sets") or []
                if sets:
                    hits = await _search_set(sets[0]["id"], sets[0].get("name", ""))
                    if hits:
                        # Put exact-number matches first for scoring.
                        if target_num:
                            hits.sort(
                                key=lambda c: (
                                    0
                                    if (c.number or "").split("/")[0].strip().lstrip("0")
                                    == target_num
                                    else 1
                                )
                            )
                        return hits[:limit]
        except Exception as exc:
            logger.warning(
                "[pokemon_scanner] Riftbound set lookup failed for %r: %s",
                set_name, exc,
            )

    # 2) Cross-set fallback — fetch all Riftbound sets + scan each.
    try:
        all_resp = await client.get(
            f"{TCGTRACKING_BASE}/{category_id}/search",
            params={"q": "riftbound"},
        )
        if all_resp.status_code != 200:
            return []
        all_sets = all_resp.json().get("sets") or []
    except Exception as exc:
        logger.warning("[pokemon_scanner] Riftbound all-set list failed: %s", exc)
        return []

    # Newer sets first — that's where the hot cards are at a card show.
    all_sets.sort(key=lambda s: s.get("published_on", ""), reverse=True)

    aggregated: list[CandidateCard] = []
    for s in all_sets:
        hits = await _search_set(s["id"], s.get("name", ""))
        aggregated.extend(hits)
        if len(aggregated) >= limit:
            break

    if target_num:
        aggregated.sort(
            key=lambda c: (
                0
                if (c.number or "").split("/")[0].strip().lstrip("0") == target_num
                else 1
            )
        )
    return aggregated[:limit]


async def _lookup_candidates_by_category(
    fields: ExtractedFields, category_id: str, ptcg_key: str = "",
) -> list[CandidateCard]:
    """Route candidate lookup to the appropriate API based on TCG category."""
    is_pokemon = category_id in ("3", "85")

    if is_pokemon:
        return await lookup_candidates(fields, api_key=ptcg_key)

    async with httpx.AsyncClient(timeout=10.0) as client:
        if category_id == "1":
            return await _scryfall_search(
                client, name=fields.card_name, set_name=fields.set_name,
                number=fields.collector_number,
            )
        if category_id == "2":
            return await _ygoprodeck_search(
                client, name=fields.card_name, set_name=fields.set_name,
            )
        if category_id == "68":
            return await _optcg_search(
                client, name=fields.card_name, set_name=fields.set_name,
                number=fields.collector_number,
            )
        if category_id == "71":
            return await _lorcast_search(
                client, name=fields.card_name, set_name=fields.set_name,
                number=fields.collector_number,
            )
        if category_id == "89":
            return await _riftbound_search(
                client, name=fields.card_name, set_name=fields.set_name,
                number=fields.collector_number,
            )
        return await _tcgtracking_product_search(
            client, name=fields.card_name, set_name=fields.set_name,
            category_id=category_id,
        )


async def lookup_candidates(fields: ExtractedFields, api_key: str = "") -> list[CandidateCard]:
    """
    Waterfall lookup strategy:
    Tier 1: Exact collector number + inferred set (TCGdex)
    Tier 2: Collector number only across all sets (PokemonTCG)
    Tier 3: Fuzzy name + approximate number (PokemonTCG)
    Tier 4: Name only (PokemonTCG)
    Tier 5: No results
    """
    settings = get_settings()
    ptcg_key = settings.pokemon_tcg_api_key
    candidates: list[CandidateCard] = []
    tier_reached = 0

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Tier 1: TCGdex exact set + number
        if fields.collector_number:
            tier_reached = 1
            sets = await _fetch_tcgdex_sets()
            set_id = _infer_set_id(fields.set_name, sets)
            if set_id:
                local_id = fields.collector_number.split("/")[0] if "/" in fields.collector_number else fields.collector_number
                card = await _tcgdex_lookup_by_set_and_number(client, set_id, local_id)
                if card:
                    candidates.append(_tcgdex_to_candidate(card))

        # Tier 1.5: TCGdex name search (has newer sets like Pocket/Ascended Heroes)
        # Always run when we have a name, even if Tier 1 returned results,
        # so that name-matched candidates are in the scoring pool.
        if fields.card_name:
            tier_reached = max(tier_reached, 2)
            tcgdex_by_name = await _tcgdex_search_by_name(
                client, fields.card_name, limit=10,
                prefer_number=fields.collector_number,
                prefer_set=fields.set_name,
            )
            seen_ids = {c.id for c in candidates}
            for tc in tcgdex_by_name:
                cand = _tcgdex_to_candidate(tc)
                if cand.id not in seen_ids:
                    candidates.append(cand)
                    seen_ids.add(cand.id)

        # Tier 1.7: When set_name is available, run a PokemonTCG search filtered
        # by set so the target card is guaranteed in the pool even if TCGdex
        # didn't return it in its first N results.
        if fields.card_name and fields.set_name:
            set_filtered = await _pokemontcg_search(
                client, name=fields.card_name, set_name=fields.set_name,
                api_key=ptcg_key, limit=5,
            )
            seen_ids = {c.id for c in candidates}
            for c in set_filtered:
                if c.id not in seen_ids:
                    candidates.append(c)
                    seen_ids.add(c.id)

        # Tier 1.6: TCGdex word-by-word fallback — OCR may mangle part of the
        # name (e.g. "dive Tangela" instead of "Erika's Tangela").  Try each
        # word individually, longest first, to find the Pokemon name.
        if not candidates and fields.card_name:
            name_words = [w for w in fields.card_name.split() if len(w) >= 4]
            name_words.sort(key=len, reverse=True)
            for word in name_words[:3]:
                tcgdex_by_word = await _tcgdex_search_by_name(
                    client, word, limit=10,
                    prefer_number=fields.collector_number,
                )
                if tcgdex_by_word:
                    for tc in tcgdex_by_word:
                        candidates.append(_tcgdex_to_candidate(tc))
                    break

        # Tier 2: PokemonTCG by name + number (most precise external search)
        if not candidates and fields.card_name and fields.collector_number:
            tier_reached = 3
            candidates = await _pokemontcg_search(
                client,
                name=fields.card_name,
                number=fields.collector_number,
                api_key=ptcg_key,
            )

        # Tier 3: PokemonTCG by name only
        if not candidates and fields.card_name:
            tier_reached = 4
            candidates = await _pokemontcg_search(
                client, name=fields.card_name, api_key=ptcg_key, limit=10,
            )

        # Tier 3.5: PokemonTCG word-by-word fallback
        if not candidates and fields.card_name:
            name_words = [w for w in fields.card_name.split() if len(w) >= 4]
            name_words.sort(key=len, reverse=True)
            for word in name_words[:3]:
                candidates = await _pokemontcg_search(
                    client, name=word, api_key=ptcg_key, limit=10,
                )
                if candidates:
                    break

        # Tier 4: PokemonTCG by number only (broad, may return wrong cards)
        if not candidates and fields.collector_number:
            tier_reached = 5
            candidates = await _pokemontcg_search(
                client, number=fields.collector_number, api_key=ptcg_key,
            )

        # Tier 5: No results
        if not candidates:
            tier_reached = 6

    logger.info(
        "[pokemon_scanner] Lookup tier=%d, candidates=%d, number=%s, name=%s",
        tier_reached, len(candidates), fields.collector_number, fields.card_name,
    )

    return candidates


# ---------------------------------------------------------------------------
# Stage E: Scoring & Ranking
# ---------------------------------------------------------------------------

def _levenshtein(s1: str, s2: str) -> int:
    """Simple Levenshtein distance."""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[-1]


def score_candidates(
    candidates: list[CandidateCard], fields: ExtractedFields
) -> list[ScoredCandidate]:
    """Score and rank candidates against extracted OCR fields."""
    scored: list[ScoredCandidate] = []

    for c in candidates:
        breakdown: dict[str, float] = {}
        total = 0.0

        # Collector number matching (exact, prefix, or proximity)
        if fields.collector_number and c.number:
            extracted_num = fields.collector_number.strip()
            candidate_num = c.number.strip()
            if extracted_num == candidate_num:
                breakdown["exact_full_number"] = SCORING_WEIGHTS["exact_full_number"]
            elif extracted_num.split("/")[0] == candidate_num.split("/")[0]:
                breakdown["exact_collector_number"] = SCORING_WEIGHTS["exact_collector_number"]
            else:
                # Proximity bonus: OCR often misreads a few digits (213 vs 218)
                try:
                    ext_n = int(extracted_num.split("/")[0])
                    cand_n = int(candidate_num.split("/")[0])
                    diff = abs(ext_n - cand_n)
                    if diff <= 10:
                        proximity = 1.0 - (diff / 10.0)
                        breakdown["number_proximity"] = round(
                            proximity * SCORING_WEIGHTS["exact_collector_number"] * 0.6, 1
                        )
                except (ValueError, IndexError):
                    pass

        # Fuzzy name similarity (Levenshtein normalized to 0-25)
        if fields.card_name and c.name:
            ext_lower = fields.card_name.lower()
            cand_lower = c.name.lower()
            dist = _levenshtein(ext_lower, cand_lower)
            max_len = max(len(ext_lower), len(cand_lower), 1)
            similarity = 1.0 - (dist / max_len)

            # Bonus: if any significant word from the extracted name appears
            # in the candidate name (handles OCR mangling half the name)
            ext_words = [w for w in ext_lower.split() if len(w) >= 4]
            word_match = any(w in cand_lower for w in ext_words)
            if word_match:
                similarity = max(similarity, 0.7)

            name_score = similarity * SCORING_WEIGHTS["fuzzy_name_similarity"]
            breakdown["fuzzy_name_similarity"] = round(name_score, 1)

            # Heavy penalty when OCR clearly extracted a name but the candidate
            # is completely different (e.g. "Zweilous" vs "Staraptor").
            # Prevents collector-number-only matches from reaching MEDIUM.
            if similarity < 0.3 and not word_match and len(ext_lower) >= 4:
                breakdown["name_mismatch_penalty"] = -35

        # Set consistency
        if fields.set_name and c.set_name:
            set_dist = _levenshtein(fields.set_name.lower(), c.set_name.lower())
            set_max = max(len(fields.set_name), len(c.set_name), 1)
            set_sim = 1.0 - (set_dist / set_max)
            if set_sim > 0.4:
                breakdown["set_consistency"] = round(set_sim * SCORING_WEIGHTS["set_consistency"], 1)

        # Variant consistency
        if fields.variant_hints and c.rarity:
            rarity_upper = (c.rarity or "").upper()
            for hint in fields.variant_hints:
                if hint.upper() in rarity_upper or rarity_upper in hint.upper():
                    breakdown["variant_consistency"] = SCORING_WEIGHTS["variant_consistency"]
                    break

        # OCR confidence bonus
        if fields.ocr_confidence > 0:
            breakdown["ocr_confidence_bonus"] = round(
                fields.ocr_confidence * SCORING_WEIGHTS["ocr_confidence_bonus"], 1
            )

        total = sum(breakdown.values())

        if total >= 80:
            confidence = "HIGH"
        elif total >= 50:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        reason_parts = []
        if "exact_full_number" in breakdown:
            reason_parts.append(f"Exact match on collector number {c.number}")
        elif "exact_collector_number" in breakdown:
            reason_parts.append(f"Matched collector number {c.number.split('/')[0]}")
        if "fuzzy_name_similarity" in breakdown and breakdown["fuzzy_name_similarity"] > 15:
            reason_parts.append(f"Name match: {c.name}")
        if "set_consistency" in breakdown:
            reason_parts.append(f"in set {c.set_name}")

        sc = ScoredCandidate(
            id=c.id,
            name=c.name,
            number=c.number,
            set_id=c.set_id,
            set_name=c.set_name,
            image_url=c.image_url,
            image_url_small=c.image_url_small,
            rarity=c.rarity,
            variant=c.variant,
            source=c.source,
            market_price=c.market_price,
            tcgplayer_url=c.tcgplayer_url,
            available_variants=c.available_variants,
            score=round(total, 1),
            confidence=confidence,
            score_breakdown=breakdown,
            match_reason=" ".join(reason_parts) if reason_parts else "Low-confidence match",
        )
        scored.append(sc)

    scored.sort(key=lambda x: x.score, reverse=True)
    return scored


TCGTRACKING_BASE = "https://tcgtracking.com/tcgapi/v1"
TCGTRACKING_POKEMON_CATS = ["3", "85"]  # 3 = Pokemon, 85 = Pokemon Japan

# Minimal manual category fallback: if the TCGTracking category list ever
# goes down or drops a game, we still want the scanner UI to offer the
# TCGs our scanner actively supports. Keep this list in sync with
# _lookup_candidates_by_category's dedicated branches.
_MANUAL_CATEGORY_FALLBACK: list[dict] = [
    {"id": "3", "name": "Pokemon"},
    {"id": "85", "name": "Pokemon Japan"},
    {"id": "68", "name": "One Piece Card Game"},
    {"id": "89", "name": "Riftbound: League of Legends Trading Card Game"},
    {"id": "1", "name": "Magic: The Gathering"},
    {"id": "2", "name": "YuGiOh"},
    {"id": "71", "name": "Disney Lorcana"},
]

# Preferred category ordering for the frontend selector
_PREFERRED_CAT_ORDER = [
    "3",   # Pokemon
    "85",  # Pokemon Japan
    "68",  # One Piece Card Game
    "89",  # Riftbound (League of Legends TCG)
    "80",  # Dragon Ball Super Fusion World
    "27",  # Dragon Ball Super CCG
    "23",  # Dragon Ball Z TCG
    "1",   # Magic
    "2",   # YuGiOh
    "71",  # Lorcana TCG
]

# Cache: set_name (lowercase) -> {set_id, products, pricing}
_tcgtracking_cache: dict[str, dict] = {}
_TCGTRACKING_CACHE_MAX = 64


def _cache_tcgtracking(set_key: str, cached: dict) -> None:
    """Insert into _tcgtracking_cache with FIFO eviction when over cap."""
    if set_key not in _tcgtracking_cache and len(_tcgtracking_cache) >= _TCGTRACKING_CACHE_MAX:
        try:
            oldest = next(iter(_tcgtracking_cache))
            _tcgtracking_cache.pop(oldest, None)
        except StopIteration:
            pass
    _tcgtracking_cache[set_key] = cached

# Cache for TCGTracking categories (fetched once per server lifetime)
_tcg_categories_cache: list[dict] | None = None


async def fetch_tcg_categories() -> list[dict]:
    """Fetch and cache TCGTracking categories with preferred ordering."""
    global _tcg_categories_cache
    if _tcg_categories_cache is not None:
        return _tcg_categories_cache

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{TCGTRACKING_BASE}/categories")
            if resp.status_code != 200:
                logger.warning("[pokemon_scanner] Failed to fetch TCGTracking categories: %s", resp.status_code)
                return list(_MANUAL_CATEGORY_FALLBACK)
            raw_cats = resp.json().get("categories") or resp.json().get("data") or []
            if isinstance(resp.json(), list):
                raw_cats = resp.json()
    except Exception as exc:
        logger.error("[pokemon_scanner] Error fetching TCGTracking categories: %s", exc)
        return list(_MANUAL_CATEGORY_FALLBACK)

    cat_map = {}
    for cat in raw_cats:
        cat_id = str(cat.get("id", ""))
        name = cat.get("display_name") or cat.get("name") or ""
        cat_map[cat_id] = {"id": cat_id, "name": name}

    # Belt-and-suspenders: make sure every game we actively support shows up
    # in the dropdown even if TCGTracking ever drops it from their category
    # list. Without this, a Riftbound outage on their side would make
    # Riftbound disappear from the scanner UI despite our code supporting it.
    for fallback in _MANUAL_CATEGORY_FALLBACK:
        cat_map.setdefault(fallback["id"], fallback)

    ordered: list[dict] = []
    seen = set()
    for pref_id in _PREFERRED_CAT_ORDER:
        if pref_id in cat_map:
            ordered.append(cat_map[pref_id])
            seen.add(pref_id)

    remaining = sorted(
        [c for cid, c in cat_map.items() if cid not in seen],
        key=lambda c: c["name"].lower(),
    )
    ordered.extend(remaining)

    _tcg_categories_cache = ordered
    return ordered


async def _enrich_price_fast(
    candidate: ScoredCandidate, ptcg_key: str = "", category_id: str = "3",
) -> None:
    """Fast price lookup using TCGTracking.com (real TCGPlayer prices, no auth).

    Strategy: search for set -> match product by number -> get TCGPlayer price.
    Falls back to PokemonTCG API for Pokemon categories only.
    """
    has_conditions = any(v.get("conditions") for v in (candidate.available_variants or []))
    if candidate.market_price is not None and has_conditions:
        return
    if not candidate.name or not candidate.set_name:
        return

    is_pokemon = category_id in ("3", "85")
    cat_ids_to_try = [category_id]
    if is_pokemon and category_id == "3":
        cat_ids_to_try = ["3", "85"]
    elif is_pokemon and category_id == "85":
        cat_ids_to_try = ["85", "3"]

    async with httpx.AsyncClient(timeout=5.0) as client:
        # --- Try 1: TCGTracking (TCGPlayer prices, free, fast) ---
        try:
            set_key = f"{category_id}:{candidate.set_name.lower()}"
            cached = _tcgtracking_cache.get(set_key)

            if not cached:
                for cat_id in cat_ids_to_try:
                    search_resp = await client.get(
                        f"{TCGTRACKING_BASE}/{cat_id}/search",
                        params={"q": candidate.set_name},
                    )
                    if search_resp.status_code != 200:
                        continue
                    sets = search_resp.json().get("sets") or []
                    if not sets:
                        continue

                    set_info = sets[0]
                    set_id = set_info["id"]

                    prod_resp, price_resp, sku_resp = await asyncio.gather(
                        client.get(f"{TCGTRACKING_BASE}/{cat_id}/sets/{set_id}"),
                        client.get(f"{TCGTRACKING_BASE}/{cat_id}/sets/{set_id}/pricing"),
                        client.get(f"{TCGTRACKING_BASE}/{cat_id}/sets/{set_id}/skus"),
                    )

                    products = prod_resp.json().get("products", []) if prod_resp.status_code == 200 else []
                    pricing = price_resp.json().get("prices", {}) if price_resp.status_code == 200 else {}
                    skus = sku_resp.json().get("products", {}) if sku_resp.status_code == 200 else {}

                    cached = {"set_id": set_id, "cat_id": cat_id, "products": products, "pricing": pricing, "skus": skus}
                    _cache_tcgtracking(set_key, cached)
                    break

            if cached and cached["products"]:
                cand_num_raw = (candidate.number or "").split("/")[0].strip()
                cand_name_lower = candidate.name.lower().replace("'", "").replace("\u2019", "")

                def _nums_match(a: str, b: str) -> bool:
                    """Flexible number comparison for all card games."""
                    if not a or not b:
                        return False
                    if a == b:
                        return True
                    a_stripped = a.lstrip("0")
                    b_stripped = b.lstrip("0")
                    if a_stripped and a_stripped == b_stripped:
                        return True
                    if a_stripped.endswith(b_stripped) or b_stripped.endswith(a_stripped):
                        return True
                    a_digits = re.sub(r"[^0-9]", "", a)
                    b_digits = re.sub(r"[^0-9]", "", b)
                    if a_digits and a_digits == b_digits and len(a_digits) >= 2:
                        return True
                    return False

                matched_product = None
                for prod in cached["products"]:
                    prod_num_raw = (prod.get("number") or "").split("/")[0].strip()
                    prod_clean = (prod.get("clean_name") or "").lower()

                    if cand_num_raw and _nums_match(cand_num_raw, prod_num_raw) and cand_name_lower in prod_clean:
                        matched_product = prod
                        break

                # Fallback 1: match by number only
                if not matched_product and cand_num_raw:
                    for prod in cached["products"]:
                        prod_num_raw = (prod.get("number") or "").split("/")[0].strip()
                        if _nums_match(cand_num_raw, prod_num_raw):
                            matched_product = prod
                            break

                # Fallback 2: match by name only (handles cases where
                # number formats are completely different across sources)
                if not matched_product and cand_name_lower:
                    for prod in cached["products"]:
                        prod_clean = (prod.get("clean_name") or "").lower()
                        if cand_name_lower == prod_clean or (len(cand_name_lower) >= 5 and cand_name_lower in prod_clean):
                            matched_product = prod
                            break

                if matched_product:
                    prod_id = str(matched_product["id"])
                    prod_prices = cached["pricing"].get(prod_id, {}).get("tcg", {})

                    variants = []
                    for subtype_name, subtype_data in prod_prices.items():
                        mp = subtype_data.get("market")
                        lp = subtype_data.get("low")
                        if mp is not None or lp is not None:
                            variants.append({
                                "name": subtype_name,
                                "price": round(float(mp), 2) if mp is not None else None,
                                "low_price": round(float(lp), 2) if lp is not None else None,
                            })

                    prod_skus = cached.get("skus", {}).get(prod_id, {})
                    if prod_skus and variants:
                        _VAR_CODE_MAP = {
                            "N": "Normal", "RH": "Reverse Holofoil", "H": "Holofoil",
                            "1H": "1st Edition Holofoil", "1N": "1st Edition Normal",
                        }
                        cond_by_variant: dict[str, dict[str, dict]] = {}
                        for sku_data in prod_skus.values():
                            var_code = sku_data.get("var", "")
                            cnd = sku_data.get("cnd", "")
                            var_name = _VAR_CODE_MAP.get(var_code, var_code)
                            if not cnd:
                                continue
                            cond_by_variant.setdefault(var_name, {})
                            entry: dict[str, float] = {}
                            if "mkt" in sku_data and sku_data["mkt"] is not None:
                                entry["mkt"] = round(float(sku_data["mkt"]), 2)
                            if "low" in sku_data and sku_data["low"] is not None:
                                entry["low"] = round(float(sku_data["low"]), 2)
                            if entry:
                                cond_by_variant[var_name][cnd] = entry

                        for v in variants:
                            conds = cond_by_variant.get(v["name"])
                            if conds:
                                v["conditions"] = conds

                    if variants:
                        candidate.available_variants = variants
                    for v in variants:
                        if v["price"] is not None:
                            candidate.market_price = v["price"]
                            break

                    if not candidate.tcgplayer_url and matched_product.get("tcgplayer_url"):
                        candidate.tcgplayer_url = matched_product["tcgplayer_url"]

                    if not candidate.image_url and matched_product.get("image_url"):
                        base_img = matched_product["image_url"].replace("_200w.jpg", "")
                        candidate.image_url = base_img + "_400w.jpg"
                        candidate.image_url_small = matched_product["image_url"]

                    if candidate.market_price:
                        logger.info(
                            "[pokemon_scanner] TCGTracking price: %s = $%.2f (%d variants)",
                            candidate.name, candidate.market_price, len(variants),
                        )
                        return

        except Exception as exc:
            logger.warning("[pokemon_scanner] TCGTracking price lookup failed: %s", exc)

        # --- Try 2: PokemonTCG API fallback (Pokemon categories only) ---
        if not is_pokemon:
            return
        try:
            headers = {"X-Api-Key": ptcg_key} if ptcg_key else {}
            parts = [f'name:"{candidate.name}"']
            if candidate.set_name:
                parts.append(f'set.name:"{candidate.set_name}"')
            query = " ".join(parts)

            resp = await client.get(
                f"{POKEMONTCG_BASE}/cards",
                params={"q": query, "pageSize": "3", "orderBy": "-set.releaseDate"},
                headers=headers,
            )
            cards = resp.json().get("data") or [] if resp.status_code == 200 else []

            if cards:
                best_card = cards[0]
                if candidate.number:
                    clean_num = candidate.number.split("/")[0].lstrip("0")
                    for card in cards:
                        if card.get("number", "").lstrip("0") == clean_num:
                            best_card = card
                            break

                _PTCG_FALLBACK_LABELS = {
                    "normal": "Normal",
                    "holofoil": "Holofoil",
                    "reverseHolofoil": "Reverse Holofoil",
                    "1stEditionHolofoil": "1st Edition Holofoil",
                    "1stEditionNormal": "1st Edition Normal",
                }
                prices_wrap = best_card.get("tcgplayer", {}).get("prices", {})
                ptcg_variants = []
                for price_key, label in _PTCG_FALLBACK_LABELS.items():
                    if price_key in prices_wrap:
                        mp = prices_wrap[price_key].get("market")
                        lp = prices_wrap[price_key].get("low")
                        mp_val = None
                        lp_val = None
                        try:
                            if mp is not None:
                                mp_val = round(float(mp), 2)
                        except (ValueError, TypeError):
                            pass
                        try:
                            if lp is not None:
                                lp_val = round(float(lp), 2)
                        except (ValueError, TypeError):
                            pass
                        if mp_val is not None or lp_val is not None:
                            ptcg_variants.append({"name": label, "price": mp_val, "low_price": lp_val})

                if ptcg_variants and not candidate.available_variants:
                    candidate.available_variants = ptcg_variants
                for v in ptcg_variants:
                    if v["price"] is not None and candidate.market_price is None:
                        candidate.market_price = v["price"]
                        break

                tcgp_url = best_card.get("tcgplayer", {}).get("url")
                if tcgp_url and not candidate.tcgplayer_url:
                    candidate.tcgplayer_url = tcgp_url

                if not candidate.image_url:
                    images = best_card.get("images") or {}
                    candidate.image_url = images.get("large", "")
                    candidate.image_url_small = images.get("small", "")
        except Exception as exc:
            logger.debug("[pokemon_scanner] PokemonTCG price lookup failed: %s", exc)


# ---------------------------------------------------------------------------
# Ximilar pipeline (visual recognition — single API call)
# ---------------------------------------------------------------------------

def _ximilar_to_scored(card_data: dict, distance: float, source: str = "ximilar") -> ScoredCandidate:
    """Map a Ximilar identification result to a ScoredCandidate."""
    card_number = card_data.get("card_number", "")
    out_of = card_data.get("out_of", "")
    number_str = f"{card_number}/{out_of}" if card_number and out_of else (card_number or "")

    links = card_data.get("links") or {}
    tcgplayer_url = links.get("tcgplayer.com") or links.get("tcgplayer")

    if distance <= 0.25:
        score, confidence = 95.0, "HIGH"
    elif distance <= 0.35:
        score, confidence = 80.0, "HIGH"
    elif distance <= 0.45:
        score, confidence = 65.0, "MEDIUM"
    elif distance <= 0.55:
        score, confidence = 50.0, "MEDIUM"
    else:
        score, confidence = 30.0, "LOW"

    return ScoredCandidate(
        id=f"ximilar-{card_data.get('set_code', '')}-{card_number}",
        name=card_data.get("name", ""),
        number=number_str,
        set_id=card_data.get("set_code", ""),
        set_name=card_data.get("set", ""),
        image_url="",
        image_url_small="",
        rarity=card_data.get("rarity"),
        variant=None,
        source=source,
        market_price=None,
        tcgplayer_url=tcgplayer_url,
        available_variants=[],
        score=score,
        confidence=confidence,
        score_breakdown={"ximilar_distance": round(distance, 4)},
        match_reason=f"Visual match: {card_data.get('full_name', card_data.get('name', ''))}",
    )


def _detect_variant_from_tags(tags: list[str]) -> str | None:
    """Map Ximilar visual tags to a TCGTracking variant subtype name."""
    tag_set = {t.lower() for t in tags}
    is_1st = any("1st" in t for t in tag_set)
    is_reverse = "reverse holo" in tag_set
    is_foil = "foil/holo" in tag_set or any(
        "rare" in t for t in tag_set if t != "non-foil"
    )

    if is_1st and is_foil:
        return "1st Edition Holofoil"
    if is_1st:
        return "1st Edition Normal"
    if is_reverse:
        return "Reverse Holofoil"
    if is_foil:
        return "Holofoil"
    if "non-foil" in tag_set:
        return "Normal"
    return None


_XIMILAR_TAG_TO_CATEGORY: dict[str, str] = {
    "pokemon": "3",
    "pocket monster": "3",
    "one piece": "68",
    "yu-gi-oh": "2",
    "yugioh": "2",
    "magic: the gathering": "1",
    "magic the gathering": "1",
    "mtg": "1",
    "dragon ball super fusion": "80",
    "dragon ball super": "27",
    "dragon ball z": "23",
    "dragon ball": "27",
    "lorcana": "71",
    "flesh and blood": "62",
    "cardfight vanguard": "16",
    "digimon": "57",
    "weiss schwarz": "19",
    "union arena": "82",
    "riftbound": "89",
    "league of legends tcg": "89",
    "league of legends trading card game": "89",
    "lol tcg": "89",
}


# TCGTracking category_id -> canonical game name stored on inventory items.
# Keep these in sync with _XIMILAR_TAG_TO_CATEGORY.
_CATEGORY_TO_GAME: dict[str, str] = {
    "1": "Magic",
    "2": "Yu-Gi-Oh",
    "3": "Pokemon",
    "16": "Cardfight Vanguard",
    "19": "Weiss Schwarz",
    "23": "Dragon Ball",
    "27": "Dragon Ball",
    "57": "Digimon",
    "62": "Flesh and Blood",
    "68": "One Piece",
    "71": "Lorcana",
    "80": "Dragon Ball",
    "82": "Union Arena",
    "85": "Pokemon",
    "89": "Riftbound",
}


def _game_for_category(cat_id: str) -> str:
    """Return the canonical inventory game name for a TCGTracking category_id."""
    return _CATEGORY_TO_GAME.get(str(cat_id or ""), "Other")


def _detect_category_from_ximilar(tags: list[str], set_name: str = "") -> str | None:
    """Detect the correct TCGTracking category from Ximilar tags or set name."""
    combined = " ".join(tags).lower()
    if set_name:
        combined += " " + set_name.lower()

    for keyword, cat_id in _XIMILAR_TAG_TO_CATEGORY.items():
        if keyword in combined:
            return cat_id

    return None


async def _run_ximilar_pipeline(
    image_b64: str, api_token: str, category_id: str = "3",
) -> dict[str, Any]:
    """Run card identification via Ximilar's visual recognition API."""
    t_start = time.monotonic()
    debug_info: dict[str, Any] = {
        "engine": "ximilar",
        "extraction_method": "ximilar",
        "stage_times_ms": {},
    }

    def _early(r: ScanResult) -> dict:
        d = asdict(r)
        _save_to_history(d)
        return d

    try:
        raw_bytes = base64.b64decode(image_b64)
    except Exception:
        return _early(ScanResult(
            status="ERROR",
            error="Invalid base64 image data",
            processing_time_ms=_elapsed(t_start),
        ))

    # Resize for Ximilar: 640px max dimension is plenty for visual recognition
    # and dramatically reduces upload time from phone -> server -> Ximilar.
    t_stage = time.monotonic()
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(raw_bytes))
        max_dim = max(img.size)
        if max_dim > 640:
            scale = 640 / max_dim
            new_size = (int(img.size[0] * scale), int(img.size[1] * scale))
            img = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80)
        send_b64 = base64.b64encode(buf.getvalue()).decode()
    except Exception:
        send_b64 = image_b64
    debug_info["stage_times_ms"]["resize"] = _elapsed(t_stage)
    debug_info["image_size_kb"] = round(len(send_b64) * 3 / 4 / 1024, 1)

    t_stage = time.monotonic()
    async with _get_external_api_semaphore():
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                XIMILAR_TCG_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Token {api_token}",
                },
                json={"records": [{"_base64": send_b64}]},
            )
    debug_info["stage_times_ms"]["ximilar_api"] = _elapsed(t_stage)
    debug_info["ximilar_http_status"] = resp.status_code

    if resp.status_code != 200:
        logger.error("[pokemon_scanner] Ximilar HTTP %s: %s", resp.status_code, resp.text[:500])
        return _early(ScanResult(
            status="ERROR",
            error=f"Ximilar API returned HTTP {resp.status_code}",
            processing_time_ms=_elapsed(t_start),
            debug=debug_info,
        ))

    data = resp.json()
    records = data.get("records") or []
    if not records:
        return _early(ScanResult(
            status="NO_MATCH",
            error="Ximilar returned no records",
            processing_time_ms=_elapsed(t_start),
            debug=debug_info,
        ))

    record = records[0]
    objects = record.get("_objects") or []

    # Find the Card object (skip Slab Label objects)
    card_obj = None
    for obj in objects:
        if obj.get("name") == "Card":
            card_obj = obj
            break

    if not card_obj:
        return _early(ScanResult(
            status="NO_MATCH",
            error="No card detected in image. Make sure the card is visible and well-lit.",
            processing_time_ms=_elapsed(t_start),
            debug=debug_info,
        ))

    identification = card_obj.get("_identification") or {}
    best_data = identification.get("best_match")
    alternatives = identification.get("alternatives") or []
    distances = identification.get("distances") or []

    # Extract tags for debug
    tags = card_obj.get("_tags_simple") or []
    debug_info["ximilar_tags"] = tags
    debug_info["ximilar_detection_prob"] = card_obj.get("prob", 0)
    debug_info["ximilar_distances"] = distances

    if not best_data:
        return _early(ScanResult(
            status="NO_MATCH",
            error="Card detected but could not be identified.",
            processing_time_ms=_elapsed(t_start),
            debug=debug_info,
        ))

    # Map to ScoredCandidate
    best_distance = distances[0] if distances else 0.5
    best = _ximilar_to_scored(best_data, best_distance)

    # Build extracted_fields for display/debug compatibility
    card_number = best_data.get("card_number", "")
    out_of = best_data.get("out_of", "")
    fields_dict = {
        "card_name": best_data.get("name"),
        "collector_number": f"{card_number}/{out_of}" if card_number and out_of else card_number,
        "set_name": best_data.get("set"),
        "language": None,
        "variant_hints": [t for t in tags if t not in ("front", "Card/Trading Card Game")],
        "hp_value": None,
        "ocr_raw_text": "",
        "ocr_confidence": 0.0,
        "extraction_method": "ximilar",
        "extraction_warnings": [],
        "collector_number_raw": None,
    }
    debug_info["ximilar_full_name"] = best_data.get("full_name", "")
    debug_info["ximilar_best_distance"] = best_distance

    # Build alternatives as candidates
    scored_list = [best]
    for i, alt_data in enumerate(alternatives):
        alt_distance = distances[i + 1] if (i + 1) < len(distances) else 0.6
        scored_list.append(_ximilar_to_scored(alt_data, alt_distance))

    # Auto-detect the correct TCGTracking category from Ximilar tags/set name
    # so pricing works even if the user has the wrong category selected.
    effective_cat = _detect_category_from_ximilar(tags, best.set_name)
    if effective_cat and effective_cat != category_id:
        logger.info(
            "[pokemon_scanner] Auto-detected category %s from Ximilar (user selected %s) — tags=%r set_name=%r",
            effective_cat, category_id, tags, best.set_name,
        )
        category_id = effective_cat
    debug_info["effective_category_id"] = category_id

    # Price + image enrichment for all candidates
    t_stage = time.monotonic()
    settings = get_settings()
    try:
        await _enrich_price_fast(best, settings.pokemon_tcg_api_key, category_id=category_id)
    except Exception as exc:
        logger.debug("[pokemon_scanner] Ximilar price enrichment failed for %s: %s", best.name, exc)
    for alt in scored_list[1:]:
        try:
            await _enrich_price_fast(alt, settings.pokemon_tcg_api_key, category_id=category_id)
        except Exception as exc:
            logger.debug(
                "[pokemon_scanner] Ximilar alternative price enrichment failed for %s: %s",
                alt.name, exc,
            )
    debug_info["stage_times_ms"]["price_enrich"] = _elapsed(t_stage)

    # Determine status
    if best.confidence == "HIGH":
        status = "MATCHED"
    elif len(scored_list) > 1 and best.score - scored_list[1].score < 15:
        status = "AMBIGUOUS"
    else:
        status = "MATCHED" if best.score >= 50 else "AMBIGUOUS"

    result = ScanResult(
        status=status,
        best_match=asdict(best),
        candidates=[asdict(s) for s in scored_list[:10]],
        extracted_fields=fields_dict,
        disambiguation_method=None,
        processing_time_ms=_elapsed(t_start),
        debug=debug_info,
    )

    logger.info(
        "[pokemon_scanner] Ximilar pipeline: status=%s, best=%s (dist=%.3f, score=%.0f), alternatives=%d, time=%.0fms",
        result.status, best.name, best_distance, best.score,
        len(alternatives), result.processing_time_ms,
    )

    result_dict = asdict(result)
    detected_variant = _detect_variant_from_tags(tags)
    if detected_variant:
        result_dict["detected_variant"] = detected_variant
        debug_info["detected_variant"] = detected_variant
    result_dict["game"] = _game_for_category(category_id)
    _save_to_history(result_dict)
    return result_dict


def _ximilar_confidence(result: dict) -> float:
    """Extract a 0-1 confidence value from a Ximilar pipeline result."""
    best = result.get("best_match") or {}
    return best.get("score", 0) / 100.0


# ---------------------------------------------------------------------------
# Stage F2: Direct vision-model identification (replaces the legacy OCR path)
# ---------------------------------------------------------------------------

_VISION_IDENTIFY_PROMPT = (
    "You are identifying a trading card from a photo. "
    "Return JSON with exactly these fields:\n"
    "  game (one of: pokemon, magic, yugioh, onepiece, lorcana, riftbound, dragonball, other)\n"
    "  card_name (the card name exactly as printed, including variant suffixes like \"ex\", \"VMAX\", \"V\")\n"
    "  set_name (the full set name if visible — do not guess)\n"
    "  collector_number (in \"X/Y\" format if both are visible, else just \"X\", else null)\n"
    "  variant_hint (e.g., \"Full Art\", \"Reverse Holo\", \"1st Edition\", or null)\n"
    "  language (e.g., \"English\", \"Japanese\")\n"
    "  confidence (0.0-1.0 — how certain you are)\n"
    "Note: \"riftbound\" is Riot's League of Legends Trading Card Game — "
    "look for the Riftbound logo or League of Legends branding.\n"
    "Return only JSON. No markdown fences. No explanation.\n"
    "If you cannot identify the card, return {\"confidence\": 0.0}."
)

# Ximilar-style game -> TCGTracking category_id (subset of _XIMILAR_TAG_TO_CATEGORY
# constrained to the canonical labels the vision prompt asks for). Used only
# for a debug warning when the user-selected category mismatches the model's
# opinion — the caller's category still wins for the database lookup.
_VISION_GAME_TO_CATEGORY: dict[str, str] = {
    "pokemon": "3",
    "magic": "1",
    "yugioh": "2",
    "onepiece": "68",
    "lorcana": "71",
    "riftbound": "89",
    "dragonball": "27",
    # "other" intentionally absent
}


async def _run_vision_pipeline(
    image_b64: str,
    category_id: str = "3",
    *,
    model_name: Optional[str] = None,
    engine_label: str = "vision",
) -> dict[str, Any]:
    """Identify a card directly via a vision-capable chat model.

    Replaces the OCR + field-extraction + disambiguation waterfall with a
    single call. The model returns structured fields that anchor a precise
    database lookup via ``_lookup_candidates_by_category``; no fuzzy scoring.

    Args:
        model_name: Override the model id. Defaults to ``get_model()`` (Claude
            Opus 4.7 today). Balanced mode passes Haiku / Gemini Flash ids here
            so the same pipeline can run for the ensemble vote.
        engine_label: Tag stamped into ``debug.engine`` and the history entry.
            Lets the debug page distinguish Opus vs Haiku vs Gemini Flash.

    Returns a ScanResult dict shaped the same as ``_run_ximilar_pipeline`` so
    the orchestrator, frontend, and history page stay compatible.
    """
    t_start = time.monotonic()
    debug_info: dict[str, Any] = {
        "engine": engine_label,
        "extraction_method": engine_label,
        "stage_times_ms": {},
        "engines_used": [engine_label],
    }

    def _early(r: ScanResult) -> dict:
        d = asdict(r)
        d["debug"] = {**d.get("debug", {}), **debug_info}
        d["game"] = _game_for_category(category_id)
        _save_to_history(d)
        return d

    if not has_ai_key():
        logger.error("[pokemon_scanner] Vision pipeline requested but no AI key configured")
        return _early(ScanResult(
            status="ERROR",
            error="No AI key configured for vision identification",
            processing_time_ms=_elapsed(t_start),
        ))

    # Resize for vision model: 1024px max dim keeps small-text detail for
    # collector numbers and set symbols while keeping the payload reasonable.
    t_stage = time.monotonic()
    try:
        raw_bytes = base64.b64decode(image_b64)
    except Exception:
        return _early(ScanResult(
            status="ERROR",
            error="Invalid base64 image data",
            processing_time_ms=_elapsed(t_start),
        ))

    try:
        from PIL import Image
        img = Image.open(io.BytesIO(raw_bytes))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        max_dim = max(img.size)
        if max_dim > 1024:
            scale = 1024 / max_dim
            new_size = (int(img.size[0] * scale), int(img.size[1] * scale))
            img = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        send_b64 = base64.b64encode(buf.getvalue()).decode()
    except Exception as exc:
        logger.warning("[pokemon_scanner] Vision pipeline image resize failed, sending raw: %s", exc)
        send_b64 = image_b64
    debug_info["stage_times_ms"]["preprocess"] = _elapsed(t_stage)

    # Vision model call
    t_stage = time.monotonic()
    model_name = model_name or get_model()
    debug_info["model"] = model_name
    try:
        async with _get_external_api_semaphore():
            client = get_ai_client().with_options(timeout=30.0)
            # temperature is deliberately omitted — Claude Opus 4.7 deprecated the
            # parameter and 400s when it's present. Defaults are already near-zero
            # for identification workloads.
            response = client.chat.completions.create(
                model=model_name,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": _VISION_IDENTIFY_PROMPT},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{send_b64}"}},
                    ],
                }],
                max_tokens=400,
            )
            raw = response.choices[0].message.content or ""
    except Exception as exc:
        logger.error("[pokemon_scanner] Vision model call failed (model=%s): %s", model_name, exc)
        debug_info["stage_times_ms"]["vision_call"] = _elapsed(t_stage)
        return _early(ScanResult(
            status="ERROR",
            error=f"Vision model call failed: {exc}",
            processing_time_ms=_elapsed(t_start),
        ))
    debug_info["stage_times_ms"]["vision_call"] = _elapsed(t_stage)

    data = _loads_ai_json(raw)
    if data is None:
        logger.warning(
            "[pokemon_scanner] Vision model returned non-JSON (model=%s): %s",
            model_name, raw[:200],
        )
        debug_info["vision_raw_snippet"] = raw[:300]
        return _early(ScanResult(
            status="NO_MATCH",
            error="Vision model returned unparseable output",
            processing_time_ms=_elapsed(t_start),
        ))

    vision_confidence = float(data.get("confidence") or 0.0)
    debug_info["vision_confidence"] = vision_confidence

    if vision_confidence < 0.3 or (not data.get("card_name") and not data.get("collector_number")):
        logger.info(
            "[pokemon_scanner] Vision model declined to identify (confidence=%.2f, data=%s)",
            vision_confidence, {k: v for k, v in data.items() if k != "raw"},
        )
        fields = ExtractedFields(
            card_name=data.get("card_name") or None,
            set_name=data.get("set_name") or None,
            collector_number=data.get("collector_number") or None,
            language=data.get("language") or None,
            variant_hints=[data.get("variant_hint")] if data.get("variant_hint") else [],
            extraction_method="vision",
        )
        result_dict = asdict(ScanResult(
            status="NO_MATCH",
            error="Vision model could not identify the card",
            extracted_fields=asdict(fields),
            processing_time_ms=_elapsed(t_start),
        ))
        result_dict["debug"] = {**(result_dict.get("debug") or {}), **debug_info}
        result_dict["game"] = _game_for_category(category_id)
        _save_to_history(result_dict)
        return result_dict

    # Warn (don't override) when the model's opinion disagrees with the caller's
    # category selection. Using the caller's category keeps text-search-style
    # consistency; the user's UI dropdown is authoritative.
    model_game = (data.get("game") or "").strip().lower()
    if model_game and _VISION_GAME_TO_CATEGORY.get(model_game) not in (None, category_id):
        logger.info(
            "[pokemon_scanner] Vision model thinks game=%s (cat=%s) but scan category=%s",
            model_game, _VISION_GAME_TO_CATEGORY.get(model_game), category_id,
        )
        debug_info["vision_game_mismatch"] = {"vision": model_game, "scan": category_id}

    # Build ExtractedFields from the vision response
    fields = ExtractedFields(
        card_name=data.get("card_name") or None,
        set_name=data.get("set_name") or None,
        collector_number=data.get("collector_number") or None,
        language=data.get("language") or None,
        variant_hints=[data.get("variant_hint")] if data.get("variant_hint") else [],
        extraction_method="vision",
    )

    logger.info(
        "[pokemon_scanner] Vision extracted: name=%s, number=%s, set=%s, variant=%s, conf=%.2f",
        fields.card_name, fields.collector_number, fields.set_name,
        data.get("variant_hint"), vision_confidence,
    )

    # Database lookup (reuses the same router used by text search)
    t_stage = time.monotonic()
    settings = get_settings()
    ptcg_key = settings.pokemon_tcg_api_key or ""
    candidates = await _lookup_candidates_by_category(fields, category_id, ptcg_key)
    debug_info["stage_times_ms"]["lookup"] = _elapsed(t_stage)

    if not candidates:
        logger.warning(
            "[pokemon_scanner] Vision pipeline: no DB candidates for name=%s number=%s set=%s cat=%s",
            fields.card_name, fields.collector_number, fields.set_name, category_id,
        )
        result_dict = asdict(ScanResult(
            status="NO_MATCH",
            error=f"No database match for '{fields.card_name}' {fields.collector_number or ''}".strip(),
            extracted_fields=asdict(fields),
            processing_time_ms=_elapsed(t_start),
        ))
        result_dict["debug"] = {**(result_dict.get("debug") or {}), **debug_info}
        result_dict["game"] = _game_for_category(category_id)
        _save_to_history(result_dict)
        return result_dict

    # Re-rank so a candidate matching vision's full "X/Y" collector number
    # wins over an unrelated printing that merely shares the numerator.
    # Waterfall lookups sometimes surface an older popular printing (e.g.
    # Ampharos #1/64 Neo Revelation) above the one vision actually saw
    # (Ampharos #1/127 Platinum) when the vision-supplied set_name doesn't
    # resolve cleanly in TCGdex.
    #
    # Set name is included in the ranking so that when two candidates tie on
    # name+number (same Pokemon reprinted across sets), the one whose set
    # matches the vision-extracted set wins. This is what prevents the
    # thumbnail from showing the wrong printing of the right card (e.g. a
    # 2003 EX Aerodactyl ex image when scanning a modern Aerodactyl ex).
    want_full_num = _norm_number(fields.collector_number) if fields.collector_number else ""
    want_name = _norm_name(fields.card_name)
    want_set = _norm_set(fields.set_name)

    def _candidate_rank(c: CandidateCard) -> tuple[int, int, int, int, int]:
        cand_num = _norm_number(c.number)
        cand_name = _norm_name(c.name)
        cand_set = _norm_set(c.set_name)
        # Higher tuple wins when reverse-sorted.
        full_num_match = 1 if (want_full_num and "/" in want_full_num and cand_num == want_full_num) else 0
        num_core_match = 1 if (
            want_full_num and cand_num.split("/")[0] == want_full_num.split("/")[0]
        ) else 0
        name_match = 1 if (want_name and cand_name == want_name) else 0
        # Set signal: 2 = exact normalized match, 1 = substring in either
        # direction (handles "151" <-> "Scarlet & Violet: 151" style drift
        # between vision output and DB set names), 0 = no info / no overlap.
        if want_set and cand_set:
            if cand_set == want_set:
                set_match = 2
            elif want_set in cand_set or cand_set in want_set:
                set_match = 1
            else:
                set_match = 0
        else:
            set_match = 0
        return (full_num_match, name_match, set_match, num_core_match, 0)

    candidates = sorted(candidates, key=_candidate_rank, reverse=True)

    # Promote the top candidate. The vision model already disambiguated; the
    # database lookup confirms it exists. HIGH requires name AND full "X/Y"
    # number (or a number-only string when vision didn't see the denominator)
    # to match. Name-only or numerator-only matches mark MEDIUM.
    top_n = candidates[:10]
    top = top_n[0]
    scored_top_n: list[ScoredCandidate] = []
    for c in top_n:
        sc = ScoredCandidate(
            **{k: getattr(c, k) for k in (
                "id", "name", "number", "set_id", "set_name", "image_url",
                "image_url_small", "rarity", "variant", "source", "market_price",
                "tcgplayer_url", "available_variants",
            )},
        )
        scored_top_n.append(sc)

    top_name_match = bool(
        fields.card_name
        and top.name.strip().lower() == fields.card_name.strip().lower()
    )
    top_norm = _norm_number(top.number)
    want_norm = _norm_number(fields.collector_number)
    # Full-number match when either (a) vision didn't see a number at all, or
    # (b) the normalized strings match exactly (including denominator when both
    # have one). A candidate with "1/64" never satisfies want "1/127" here.
    top_full_number_match = (not want_norm) or (top_norm == want_norm)
    top_numerator_only_match = bool(
        want_norm and top_norm.split("/")[0] == want_norm.split("/")[0]
    )

    if top_name_match and top_full_number_match:
        scored_top_n[0].score = 100.0
        scored_top_n[0].confidence = "HIGH"
        scored_top_n[0].match_reason = "vision+db exact match"
        status = "MATCHED"
    elif top_name_match and top_numerator_only_match:
        # Same card name and numerator, different denominator → most likely
        # the DB lookup landed on a different printing than vision saw.
        scored_top_n[0].score = 55.0
        scored_top_n[0].confidence = "MEDIUM"
        scored_top_n[0].match_reason = (
            f"vision saw #{fields.collector_number}, db closest match is #{top.number}"
        )
        status = "AMBIGUOUS" if len(scored_top_n) > 1 else "MATCHED"
        logger.info(
            "[pokemon_scanner] Vision pipeline: numerator-only match — vision=%s #%s, db=%s #%s (set=%s)",
            fields.card_name, fields.collector_number, top.name, top.number, top.set_name,
        )
    else:
        scored_top_n[0].score = 65.0
        scored_top_n[0].confidence = "MEDIUM"
        scored_top_n[0].match_reason = "vision identified but db closest match differs"
        status = "AMBIGUOUS" if len(scored_top_n) > 1 else "MATCHED"

    for s in scored_top_n[1:]:
        s.score = 40.0
        s.confidence = "LOW"
        s.match_reason = "alternative db candidate"

    # Price enrichment for the top 8 candidates in parallel
    t_stage = time.monotonic()
    await asyncio.gather(
        *[_enrich_price_fast(c, ptcg_key=ptcg_key, category_id=category_id) for c in scored_top_n[:8]],
        return_exceptions=True,
    )
    debug_info["stage_times_ms"]["price_enrich"] = _elapsed(t_stage)

    result_dict = asdict(ScanResult(
        status=status,
        best_match=asdict(scored_top_n[0]),
        candidates=[asdict(c) for c in scored_top_n],
        extracted_fields=asdict(fields),
        processing_time_ms=_elapsed(t_start),
    ))
    result_dict["debug"] = {**(result_dict.get("debug") or {}), **debug_info}
    result_dict["game"] = _game_for_category(category_id)

    logger.info(
        "[pokemon_scanner] Vision pipeline: status=%s, best=%s #%s, candidates=%d, time=%.0fms",
        status, scored_top_n[0].name, scored_top_n[0].number,
        len(scored_top_n), result_dict["processing_time_ms"],
    )

    _save_to_history(result_dict)
    return result_dict


# ---------------------------------------------------------------------------
# Stage F3: Tiebreaker (Gemini 3.1 Pro via NVIDIA)
# ---------------------------------------------------------------------------

def _norm_name(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _norm_set(s: Optional[str]) -> str:
    """Normalize a set name for cross-source comparison.

    Vision, TCGdex, PokemonTCG, TCGTracking, and Scryfall all spell set
    names slightly differently ("Scarlet & Violet: 151" vs "151" vs
    "Scarlet & Violet—151"). Lowercase, drop punctuation and common
    prefixes/noise so substring matching is meaningful.
    """
    s = (s or "").strip().lower()
    if not s:
        return ""
    # Drop common prefixes used by PokemonTCG / TCGdex that visuals don't include
    for prefix in ("scarlet & violet: ", "scarlet & violet—", "scarlet and violet: ",
                   "sword & shield: ", "sword and shield: ",
                   "sun & moon: ", "sun and moon: ",
                   "xy: ", "x y: ",
                   "black & white: ", "black and white: "):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    # Normalize punctuation and whitespace
    import re as _re
    s = _re.sub(r"[^a-z0-9]+", " ", s).strip()
    return " ".join(s.split())


def _norm_number(s: Optional[str]) -> str:
    """Normalize a collector number so ``1/127`` and ``001/127`` compare equal
    but ``1/127`` and ``1/64`` do not.

    Previously this stripped the denominator entirely, which caused false
    positives: two different Pokémon with the same numerator (e.g. Ampharos
    #1/127 Platinum vs #1/64 Neo Revelation) compared equal and got promoted
    to MATCHED/HIGH. Preserving the denominator when both sides have ``X/Y``
    fixes this while still tolerating leading-zero OCR drift.
    """
    s = (s or "").strip()
    if not s:
        return ""
    if "/" in s:
        num, tot = s.split("/", 1)
        return f"{num.strip().lstrip('0') or '0'}/{tot.strip().lstrip('0') or '0'}"
    return s.lstrip("0") or "0"


def _best_match_tuple(result: Optional[dict]) -> tuple[str, str]:
    """Return a normalized (name, number_core) key from a result's best_match."""
    if not result:
        return ("", "")
    bm = result.get("best_match") or {}
    return (_norm_name(bm.get("name")), _norm_number(bm.get("number")))


async def _run_tiebreaker(
    image_b64: str,
    ximilar_result: Optional[dict],
    vision_result: Optional[dict],
) -> Optional[dict]:
    """Ask a third vision model to identify the card when the first two disagree.

    Returns ``{"card_name", "collector_number", "confidence", "raw", "model"}``
    on success, or ``None`` when the tiebreaker is unavailable / failed. The
    caller decides what to do with the verdict (2-of-3 majority, etc.).
    """
    if not has_tiebreaker_key():
        return None

    t_start = time.monotonic()
    model_name = get_tiebreaker_model()

    try:
        # Resize identically to the primary vision pipeline so the two models
        # see the same input and we can trust a majority vote.
        raw_bytes = base64.b64decode(image_b64)
        from PIL import Image
        img = Image.open(io.BytesIO(raw_bytes))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        max_dim = max(img.size)
        if max_dim > 1024:
            scale = 1024 / max_dim
            new_size = (int(img.size[0] * scale), int(img.size[1] * scale))
            img = img.resize(new_size, Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        send_b64 = base64.b64encode(buf.getvalue()).decode()
    except Exception as exc:
        logger.warning("[pokemon_scanner] Tiebreaker image preprocess failed: %s", exc)
        send_b64 = image_b64

    try:
        async with _get_external_api_semaphore():
            client = get_tiebreaker_client().with_options(timeout=30.0)
            # See _run_vision_pipeline: temperature omitted for Claude Opus 4.7
            # compatibility. Other tiebreaker targets (e.g., Gemini) accept
            # temperature but there's no behavioral reason to set it.
            response = client.chat.completions.create(
                model=model_name,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": _VISION_IDENTIFY_PROMPT},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{send_b64}"}},
                    ],
                }],
                max_tokens=400,
            )
            raw = response.choices[0].message.content or ""
    except Exception as exc:
        logger.error("[pokemon_scanner] Tiebreaker call failed (model=%s): %s", model_name, exc)
        return None

    data = _loads_ai_json(raw)
    if data is None:
        logger.warning(
            "[pokemon_scanner] Tiebreaker returned non-JSON (model=%s): %s",
            model_name, raw[:200],
        )
        return None

    verdict = {
        "card_name": data.get("card_name") or None,
        "collector_number": data.get("collector_number") or None,
        "confidence": float(data.get("confidence") or 0.0),
        "raw": data,
        "model": model_name,
        "processing_time_ms": _elapsed(t_start),
    }

    ximilar_tuple = _best_match_tuple(ximilar_result)
    vision_tuple = _best_match_tuple(vision_result)
    gemini_tuple = (_norm_name(verdict["card_name"]), _norm_number(verdict["collector_number"]))
    if gemini_tuple == ximilar_tuple and gemini_tuple != ("", ""):
        winner = "ximilar"
    elif gemini_tuple == vision_tuple and gemini_tuple != ("", ""):
        winner = "vision"
    else:
        winner = "none"
    logger.info(
        "[pokemon_scanner] Tiebreaker fired — ximilar=%s vision=%s gemini=%s → winner=%s (model=%s, conf=%.2f)",
        ximilar_tuple, vision_tuple, gemini_tuple, winner, model_name, verdict["confidence"],
    )
    verdict["winner"] = winner
    return verdict


# ---------------------------------------------------------------------------
# Stage F4: Engine merge (Ximilar vs vision, with optional tiebreaker)
# ---------------------------------------------------------------------------

async def _merge_engine_results(
    ximilar_result: Optional[dict],
    vision_result: Optional[dict],
    image_b64: str,
) -> dict:
    """Reconcile two engine outputs into a single ScanResult.

    - Both agree on (name, number) → Ximilar result, HIGH confidence.
    - Disagree + tiebreaker available → ask tiebreaker, take 2-of-3 majority.
    - Disagree + no tiebreaker + vision confidence > 0.8 → vision wins.
    - Otherwise → AMBIGUOUS with both (or all three) candidates.

    Always stamps ``debug.tiebreaker_used`` and ``debug.tiebreaker_winner`` so
    the debug page can show whether the ensemble fired.
    """

    def _stamp_tb(result: dict, *, used: bool, winner: Optional[str]) -> dict:
        result.setdefault("debug", {})
        result["debug"]["tiebreaker_used"] = used
        result["debug"]["tiebreaker_winner"] = winner
        existing_engines = result["debug"].get("engines_used") or []
        new_engines = ["ximilar", "vision"]
        if used:
            new_engines.append("gemini")
        result["debug"]["engines_used"] = list(dict.fromkeys(existing_engines + new_engines))
        return result

    # Missing engine paths — caller uses whichever single engine is available
    if ximilar_result is None and vision_result is None:
        return _stamp_tb(
            asdict(ScanResult(status="ERROR", error="Both engines failed")),
            used=False, winner=None,
        )
    if ximilar_result is None:
        return _stamp_tb(dict(vision_result or {}), used=False, winner=None)
    if vision_result is None:
        return _stamp_tb(dict(ximilar_result), used=False, winner=None)

    ximilar_tuple = _best_match_tuple(ximilar_result)
    vision_tuple = _best_match_tuple(vision_result)

    # Agreement: trust the Ximilar result (richer variant metadata / tcgplayer
    # links) but promote confidence to HIGH since a second model confirmed.
    if ximilar_tuple == vision_tuple and ximilar_tuple != ("", ""):
        merged = dict(ximilar_result)
        if merged.get("best_match"):
            merged["best_match"] = {**merged["best_match"], "confidence": "HIGH", "score": max(
                float(merged["best_match"].get("score") or 0), 95.0,
            )}
        merged["status"] = "MATCHED"
        logger.info(
            "[pokemon_scanner] Engines agree — name=%s number=%s (no tiebreaker needed)",
            ximilar_tuple[0], ximilar_tuple[1],
        )
        return _stamp_tb(merged, used=False, winner=None)

    # Disagreement — try tiebreaker first
    verdict = await _run_tiebreaker(image_b64, ximilar_result, vision_result)
    if verdict is not None:
        winner = verdict.get("winner")
        if winner == "ximilar":
            merged = dict(ximilar_result)
            if merged.get("best_match"):
                merged["best_match"] = {**merged["best_match"], "confidence": "HIGH"}
            merged["status"] = "MATCHED"
            return _stamp_tb(merged, used=True, winner="ximilar")
        if winner == "vision":
            merged = dict(vision_result)
            if merged.get("best_match"):
                merged["best_match"] = {**merged["best_match"], "confidence": "HIGH"}
            merged["status"] = "MATCHED"
            return _stamp_tb(merged, used=True, winner="vision")
        # Three-way disagreement — present all as candidates and let the user decide
        merged = dict(ximilar_result)
        merged["status"] = "AMBIGUOUS"
        combined: list[dict] = list(merged.get("candidates") or [])
        for vc in (vision_result.get("candidates") or [])[:3]:
            if all(
                (_norm_name(vc.get("name")), _norm_number(vc.get("number"))) !=
                (_norm_name(ec.get("name")), _norm_number(ec.get("number")))
                for ec in combined
            ):
                combined.append(vc)
        merged["candidates"] = combined[:10]
        merged.setdefault("debug", {})["hard_case"] = True
        merged["debug"]["tiebreaker_verdict"] = {
            "card_name": verdict.get("card_name"),
            "collector_number": verdict.get("collector_number"),
            "confidence": verdict.get("confidence"),
        }
        logger.warning(
            "[pokemon_scanner] Hard case: all three engines disagree — ximilar=%s vision=%s gemini=(%s,%s)",
            ximilar_tuple, vision_tuple,
            _norm_name(verdict.get("card_name")), _norm_number(verdict.get("collector_number")),
        )
        return _stamp_tb(merged, used=True, winner="none")

    # No tiebreaker available — fall back to vision-confidence heuristic
    vision_conf = 0.0
    try:
        vision_conf = float((vision_result.get("debug") or {}).get("vision_confidence") or 0.0)
    except (TypeError, ValueError):
        vision_conf = 0.0

    if vision_conf > 0.8:
        merged = dict(vision_result)
        merged["status"] = "MATCHED"
        logger.info(
            "[pokemon_scanner] Disagreement + no tiebreaker — vision wins by confidence=%.2f",
            vision_conf,
        )
        return _stamp_tb(merged, used=False, winner=None)

    # Low-confidence disagreement — surface both as candidates
    merged = dict(ximilar_result)
    merged["status"] = "AMBIGUOUS"
    combined: list[dict] = list(merged.get("candidates") or [])
    for vc in (vision_result.get("candidates") or [])[:3]:
        if all(
            (_norm_name(vc.get("name")), _norm_number(vc.get("number"))) !=
            (_norm_name(ec.get("name")), _norm_number(ec.get("number")))
            for ec in combined
        ):
            combined.append(vc)
    merged["candidates"] = combined[:10]
    logger.info(
        "[pokemon_scanner] Disagreement + no tiebreaker + low vision confidence (%.2f) → AMBIGUOUS",
        vision_conf,
    )
    return _stamp_tb(merged, used=False, winner=None)


async def _background_vision_validate(
    scan_id: str,
    image_b64: str,
    ximilar_result: dict,
    category_id: str,
) -> None:
    """Run the vision pipeline in background and store merged result for polling.

    Fires for MEDIUM-confidence Ximilar scans so the UI can return immediately
    and then upgrade via `/validate/{scan_id}` once the ensemble has agreed
    (or the tiebreaker has arbitrated).
    """
    try:
        vision_result = await _run_vision_pipeline(image_b64, category_id)
        merged = await _merge_engine_results(ximilar_result, vision_result, image_b64)
        merged["scan_id"] = scan_id
        merged["validation_status"] = "validated"
        merged.setdefault("debug", {})["pipeline_tier"] = "medium_confidence_validated"

        _save_to_history(merged)
        _insert_pending_validation(scan_id, (time.monotonic(), merged))

        logger.info(
            "[pokemon_scanner] Background vision validation complete for scan_id=%s: status=%s, tiebreaker_winner=%s",
            scan_id, merged.get("status"),
            merged.get("debug", {}).get("tiebreaker_winner"),
        )
    except Exception as exc:
        logger.error(
            "[pokemon_scanner] Background vision validation failed for scan_id=%s: %s",
            scan_id, exc,
        )
        _insert_pending_validation(
            scan_id,
            (time.monotonic(), {"validation_status": "error", "error": str(exc)}),
        )


# ---------------------------------------------------------------------------
# Stage F5: Balanced-mode ensemble — Ximilar + Haiku + Gemini Flash in parallel
# with 3-way majority voting. Used by run_pipeline when mode="balanced".
# ---------------------------------------------------------------------------

def _vote_three_way(
    ximilar: Optional[dict],
    vision_a: Optional[dict],
    vision_b: Optional[dict],
) -> tuple[str, dict]:
    """Decide the winner of a Ximilar + two-AI ensemble scan.

    Engines with a missing ``best_match`` or non-matched status don't vote.
    Returns ``(winner_label, summary)`` where winner is one of:
        - ``"ximilar"``     — Ximilar's result wins (on full agreement or
          because Ximilar is part of any 2-of-3 majority; Ximilar is
          preferred because it carries richer variant/tcgplayer metadata).
        - ``"vision_a"``    — Haiku's result wins (part of a 2-of-3 majority
          that excluded Ximilar).
        - ``"vision_b"``    — Gemini Flash's result wins (same).
        - ``"none"``        — no majority: all disagree, or only one engine
          voted. Caller surfaces AMBIGUOUS with all candidates.

    Equality uses ``_best_match_tuple`` which now preserves the full ``X/Y``
    collector number (fix from commit ``c368c47``) so different printings of
    the same card name no longer tuple-equal.
    """
    engines = [("ximilar", ximilar), ("vision_a", vision_a), ("vision_b", vision_b)]
    voters: list[tuple[str, tuple[str, str]]] = []
    for label, result in engines:
        if not result or result.get("status") in ("ERROR", "NO_MATCH"):
            continue
        sig = _best_match_tuple(result)
        if sig == ("", ""):
            continue
        voters.append((label, sig))

    summary: dict[str, Any] = {
        "ximilar": _best_match_tuple(ximilar),
        "vision_a": _best_match_tuple(vision_a),
        "vision_b": _best_match_tuple(vision_b),
        "voter_count": len(voters),
    }

    if not voters:
        summary["reason"] = "no engine produced a usable best_match"
        return ("none", summary)
    if len(voters) == 1:
        # A single voter isn't a majority by itself — surface as AMBIGUOUS so
        # the user confirms rather than trusting one engine blindly.
        summary["reason"] = f"only {voters[0][0]} voted"
        return ("none", summary)

    # Tally signatures
    tally: dict[tuple[str, str], list[str]] = {}
    for label, sig in voters:
        tally.setdefault(sig, []).append(label)

    # Full agreement (all voters share one signature)
    if len(tally) == 1:
        summary["reason"] = "all voters agree"
        return ("ximilar" if "ximilar" in voters[0][1:] or any(
            lbl == "ximilar" for lbl, _ in voters
        ) else voters[0][0], summary)

    # 2-of-3: find the signature with the most votes
    best_sig, best_labels = max(tally.items(), key=lambda kv: len(kv[1]))
    if len(best_labels) >= 2:
        # Prefer Ximilar within the winning group for its richer metadata.
        winner = "ximilar" if "ximilar" in best_labels else best_labels[0]
        summary["reason"] = f"{len(best_labels)}-of-{len(voters)} majority"
        return (winner, summary)

    # No pair agreed — all distinct
    summary["reason"] = "all voters disagree"
    return ("none", summary)


def _assemble_balanced_result(
    winner: str,
    vote_summary: dict,
    ximilar_result: Optional[dict],
    haiku_result: Optional[dict],
    gemini_result: Optional[dict],
) -> dict:
    """Build the final ScanResult dict from an ensemble vote outcome.

    - Winner ``"ximilar"`` (full agreement or majority including Ximilar):
      clone Ximilar's result, promote to HIGH, MATCHED.
    - Winner ``"vision_a"`` / ``"vision_b"``: clone that engine's result as
      the best_match (so DB-confirmed name/number/set is authoritative), and
      append Ximilar's top candidate to the candidates list for transparency.
    - Winner ``"none"``: AMBIGUOUS, candidates array contains each engine's
      top match so the reviewer can pick.
    """
    def _ensure_debug(d: dict) -> dict:
        d.setdefault("debug", {})
        return d

    def _clone(result: Optional[dict]) -> dict:
        import copy as _c
        return _c.deepcopy(result) if result else {}

    if winner == "ximilar" and ximilar_result:
        merged = _ensure_debug(_clone(ximilar_result))
        if merged.get("best_match"):
            merged["best_match"]["confidence"] = "HIGH"
            merged["best_match"]["score"] = max(
                float(merged["best_match"].get("score") or 0), 95.0,
            )
        merged["status"] = "MATCHED"
        merged["disambiguation_method"] = "balanced_ensemble"
        return merged

    if winner in ("vision_a", "vision_b"):
        source = haiku_result if winner == "vision_a" else gemini_result
        merged = _ensure_debug(_clone(source))
        if merged.get("best_match"):
            merged["best_match"]["confidence"] = "HIGH"
            merged["best_match"]["score"] = max(
                float(merged["best_match"].get("score") or 0), 95.0,
            )
        merged["status"] = "MATCHED"
        merged["disambiguation_method"] = "balanced_ensemble"
        # Include Ximilar's top as an alternative candidate for transparency,
        # deduped by normalized signature.
        if ximilar_result and (ximilar_result.get("best_match") or {}).get("name"):
            existing = {
                (_norm_name((c or {}).get("name")), _norm_number((c or {}).get("number")))
                for c in (merged.get("candidates") or [])
            }
            x_best = ximilar_result.get("best_match") or {}
            x_sig = (_norm_name(x_best.get("name")), _norm_number(x_best.get("number")))
            if x_sig not in existing and x_sig != ("", ""):
                cands = list(merged.get("candidates") or [])
                cands.append(x_best)
                merged["candidates"] = cands[:10]
        return merged

    # winner == "none" — AMBIGUOUS
    base = ximilar_result or haiku_result or gemini_result or {}
    merged = _ensure_debug(_clone(base))
    merged["status"] = "AMBIGUOUS"
    merged["disambiguation_method"] = "balanced_ensemble"
    combined: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for src in (ximilar_result, haiku_result, gemini_result):
        if not src:
            continue
        for c in ((src.get("candidates") or [])[:3] + [src.get("best_match")]):
            if not c:
                continue
            sig = (_norm_name(c.get("name")), _norm_number(c.get("number")))
            if sig == ("", "") or sig in seen:
                continue
            seen.add(sig)
            combined.append(c)
    if combined:
        merged["candidates"] = combined[:10]
    return merged


async def _run_balanced_pipeline(image_b64: str, category_id: str) -> dict[str, Any]:
    """Ximilar-first ensemble: HIGH → return immediately; otherwise return
    Ximilar optimistically while Haiku + Gemini Flash run in parallel for a
    3-way majority-vote validation in the background.

    The optimistic return uses the same ``scan_id`` +
    ``_insert_pending_validation`` pattern as Accurate-mode MEDIUM tier, so
    the existing frontend polling at ``/degen_eye/validate/{scan_id}`` just
    works. Ensemble votes are stamped onto ``debug.ensemble_votes`` for the
    debug page.
    """
    settings = get_settings()
    has_ximilar = bool(settings.ximilar_api_token)
    has_vision = has_ai_key()

    # No Ximilar → parallel 2-of-2 vote between Haiku and Gemini Flash
    if not has_ximilar:
        if not has_vision:
            return asdict(ScanResult(
                status="ERROR",
                error="Balanced mode needs either XIMILAR_API_TOKEN or an AI provider key",
            ))
        haiku_task = _run_vision_pipeline(
            image_b64, category_id,
            model_name=get_haiku_model(), engine_label="vision_haiku",
        )
        gemini_task = _run_vision_pipeline(
            image_b64, category_id,
            model_name=get_gemini_flash_model(), engine_label="vision_gemini_flash",
        )
        results = await asyncio.gather(haiku_task, gemini_task, return_exceptions=True)
        haiku_result = results[0] if not isinstance(results[0], Exception) else None
        gemini_result = results[1] if not isinstance(results[1], Exception) else None
        winner, summary = _vote_three_way(None, haiku_result, gemini_result)
        merged = _assemble_balanced_result(winner, summary, None, haiku_result, gemini_result)
        merged.setdefault("debug", {})["mode"] = "balanced"
        merged["debug"]["pipeline_tier"] = "balanced_no_ximilar"
        merged["debug"]["ensemble_votes"] = summary
        merged["debug"]["engines_used"] = ["vision_haiku", "vision_gemini_flash"]
        merged["debug"].setdefault("tiebreaker_used", False)
        merged["debug"].setdefault("tiebreaker_winner", None)
        return merged

    # Run Ximilar first (fast: 2-4s)
    try:
        ximilar_result = await _run_ximilar_pipeline(
            image_b64, settings.ximilar_api_token, category_id,
        )
    except Exception as exc:
        logger.error("[pokemon_scanner] Balanced: Ximilar pipeline failed: %s", exc)
        if not has_vision:
            return asdict(ScanResult(
                status="ERROR",
                error=f"Ximilar failed and no AI key available: {exc}",
            ))
        # Fall back to the 2-AI parallel vote
        haiku_task = _run_vision_pipeline(
            image_b64, category_id,
            model_name=get_haiku_model(), engine_label="vision_haiku",
        )
        gemini_task = _run_vision_pipeline(
            image_b64, category_id,
            model_name=get_gemini_flash_model(), engine_label="vision_gemini_flash",
        )
        results = await asyncio.gather(haiku_task, gemini_task, return_exceptions=True)
        haiku_result = results[0] if not isinstance(results[0], Exception) else None
        gemini_result = results[1] if not isinstance(results[1], Exception) else None
        winner, summary = _vote_three_way(None, haiku_result, gemini_result)
        merged = _assemble_balanced_result(winner, summary, None, haiku_result, gemini_result)
        merged.setdefault("debug", {})["mode"] = "balanced"
        merged["debug"]["pipeline_tier"] = "balanced_ximilar_failed"
        merged["debug"]["ensemble_votes"] = summary
        merged["debug"]["engines_used"] = ["vision_haiku", "vision_gemini_flash"]
        merged["debug"].setdefault("tiebreaker_used", False)
        merged["debug"].setdefault("tiebreaker_winner", None)
        return merged

    # No AI → just return Ximilar as-is (effectively Fast mode)
    if not has_vision:
        ximilar_result.setdefault("debug", {})["mode"] = "balanced"
        ximilar_result["debug"]["pipeline_tier"] = "balanced_no_ai_key"
        ximilar_result["debug"]["engines_used"] = ["ximilar"]
        ximilar_result["debug"].setdefault("tiebreaker_used", False)
        ximilar_result["debug"].setdefault("tiebreaker_winner", None)
        return ximilar_result

    confidence = _ximilar_confidence(ximilar_result)
    x_status = ximilar_result.get("status", "ERROR")
    scan_id = str(uuid.uuid4())

    # HIGH → accept immediately, skip AI calls entirely
    if confidence >= XIMILAR_CONFIDENCE_HIGH and x_status not in ("ERROR", "NO_MATCH"):
        logger.info(
            "[pokemon_scanner] Balanced HIGH (%.2f) — accepting Ximilar, skipping ensemble, category=%s",
            confidence, category_id,
        )
        ximilar_result["scan_id"] = scan_id
        ximilar_result.setdefault("debug", {})["mode"] = "balanced"
        ximilar_result["debug"]["pipeline_tier"] = "balanced_high_confidence"
        ximilar_result["debug"]["engines_used"] = ["ximilar"]
        ximilar_result["debug"]["tiebreaker_used"] = False
        ximilar_result["debug"]["tiebreaker_winner"] = None
        return ximilar_result

    # Non-HIGH → return Ximilar optimistically, fire parallel ensemble in bg
    logger.info(
        "[pokemon_scanner] Balanced non-HIGH (%.2f, status=%s) — optimistic return + parallel Haiku+Gemini-Flash, category=%s",
        confidence, x_status, category_id,
    )
    ximilar_result["scan_id"] = scan_id
    ximilar_result["validation_pending"] = True
    ximilar_result.setdefault("debug", {})["mode"] = "balanced"
    ximilar_result["debug"]["pipeline_tier"] = "balanced_parallel"
    ximilar_result["debug"]["engines_used"] = ["ximilar"]
    ximilar_result["debug"].setdefault("tiebreaker_used", False)
    ximilar_result["debug"].setdefault("tiebreaker_winner", None)

    _insert_pending_validation(scan_id, (time.monotonic(), None))

    import copy as _copy
    ximilar_copy = _copy.deepcopy(ximilar_result)
    asyncio.create_task(_background_balanced_validate(
        scan_id, image_b64, ximilar_copy, category_id,
    ))

    return ximilar_result


async def _background_balanced_validate(
    scan_id: str,
    image_b64: str,
    ximilar_result: dict,
    category_id: str,
) -> None:
    """Run Haiku and Gemini Flash in parallel, then vote with Ximilar."""
    try:
        haiku_task = _run_vision_pipeline(
            image_b64, category_id,
            model_name=get_haiku_model(), engine_label="vision_haiku",
        )
        gemini_task = _run_vision_pipeline(
            image_b64, category_id,
            model_name=get_gemini_flash_model(), engine_label="vision_gemini_flash",
        )
        results = await asyncio.gather(haiku_task, gemini_task, return_exceptions=True)
        haiku_result = results[0] if not isinstance(results[0], Exception) else None
        gemini_result = results[1] if not isinstance(results[1], Exception) else None

        if isinstance(results[0], Exception):
            logger.error(
                "[pokemon_scanner] Balanced: Haiku call failed for scan_id=%s: %s",
                scan_id, results[0],
            )
        if isinstance(results[1], Exception):
            logger.error(
                "[pokemon_scanner] Balanced: Gemini Flash call failed for scan_id=%s: %s",
                scan_id, results[1],
            )

        winner, summary = _vote_three_way(ximilar_result, haiku_result, gemini_result)
        merged = _assemble_balanced_result(
            winner, summary, ximilar_result, haiku_result, gemini_result,
        )
        merged["scan_id"] = scan_id
        merged["validation_status"] = "validated"
        merged.setdefault("debug", {})["mode"] = "balanced"
        merged["debug"]["pipeline_tier"] = "balanced_parallel_validated"
        merged["debug"]["ensemble_votes"] = summary
        merged["debug"]["engines_used"] = [
            "ximilar", "vision_haiku", "vision_gemini_flash",
        ]
        merged["debug"].setdefault("tiebreaker_used", False)
        merged["debug"].setdefault("tiebreaker_winner", None)

        logger.info(
            "[pokemon_scanner] Balanced vote for scan_id=%s — winner=%s, votes=%s",
            scan_id, winner, {
                "ximilar": summary.get("ximilar"),
                "vision_a_haiku": summary.get("vision_a"),
                "vision_b_gemini_flash": summary.get("vision_b"),
            },
        )

        _save_to_history(merged)
        _insert_pending_validation(scan_id, (time.monotonic(), merged))
    except Exception as exc:
        logger.error(
            "[pokemon_scanner] Balanced background validate failed for scan_id=%s: %s",
            scan_id, exc,
        )
        _insert_pending_validation(
            scan_id,
            (time.monotonic(), {"validation_status": "error", "error": str(exc)}),
        )


# ---------------------------------------------------------------------------
# Text search — free-text query parsing
#
# The parser feeds _lookup_candidates_by_category, which requires structured
# fields (card_name, set_name, collector_number). Real users type things like
# "charizard 151" or "moonbreon" — not "Charizard from Scarlet & Violet: 151".
# We close that gap with:
#   (a) a small nickname dictionary (app/data/pokemon_nicknames.json) for chase
#       cards that collectors always ask for by nickname,
#   (b) per-game set alias dicts ("151" -> "Scarlet & Violet: 151",
#       "4ed" -> "Fourth Edition"),
#   (c) a regex for set-specific collector numbers (OP01-003 etc.),
#   (d) an AI parser upgraded with few-shot examples.
# The heuristic is the reliable path — AI augments it when the key works.
# ---------------------------------------------------------------------------

_TEXT_SEARCH_PARSE_PROMPT = """You are a TCG card search query parser. Users type short queries for trading-card-game cards (Pokemon, Magic: the Gathering, One Piece, etc.). Extract structured fields.

Return JSON with:
- "card_name": the card/character name (e.g. "Charizard", "Pikachu VMAX", "Lightning Bolt")
- "set_name": the full, canonical set name if mentioned, else null
- "collector_number": the collector number if mentioned (e.g. "4/102", "25", "OP01-003"), else null

Resolve Pokemon set aliases and community nicknames to canonical names:
- "151" -> "Scarlet & Violet: 151"
- "fossil" -> "Fossil"
- "base set" / "base" -> "Base Set"
- "evolving skies" -> "Evolving Skies"
- "lost origin" -> "Lost Origin"
- "silver tempest" -> "Silver Tempest"
- "crown zenith" -> "Crown Zenith"
- "obsidian flames" -> "Obsidian Flames"
- "paldean fates" -> "Paldean Fates"
- "hidden fates" -> "Hidden Fates"
- "brilliant stars" -> "Brilliant Stars"
- "astral radiance" -> "Astral Radiance"
- "paradox rift" -> "Paradox Rift"
- "temporal forces" -> "Temporal Forces"
- "twilight masquerade" -> "Twilight Masquerade"
- "stellar crown" -> "Stellar Crown"
- "shrouded fable" -> "Shrouded Fable"
- "surging sparks" -> "Surging Sparks"
- "prismatic evolutions" / "prismatic" -> "Prismatic Evolutions"
- "darkness ablaze" -> "Darkness Ablaze"
- "vivid voltage" -> "Vivid Voltage"
- "pokemon go" / "go" -> "Pokemon Go"
- "moonbreon" -> card_name "Umbreon VMAX", set_name "Evolving Skies"
- "rainbow rayquaza" -> card_name "Rayquaza VMAX", set_name "Evolving Skies"
- "giratina v alt art" / "giratina alt" -> card_name "Giratina V", set_name "Lost Origin"
- "palkia origin alt" -> card_name "Origin Forme Palkia V", set_name "Astral Radiance"
- "shining charizard" -> card_name "Shining Charizard", set_name "Neo Destiny"

Magic set codes:
- "4ed" / "4th" -> "Fourth Edition"
- "lea" -> "Limited Edition Alpha"
- "leb" -> "Limited Edition Beta"
- "dmu" -> "Dominaria United"

Handle set-first and card-first orderings:
- "charizard 151" -> {"card_name": "Charizard", "set_name": "Scarlet & Violet: 151"}
- "151 zapdos" -> {"card_name": "Zapdos", "set_name": "Scarlet & Violet: 151"}
- "base set charizard" -> {"card_name": "Charizard", "set_name": "Base Set"}

Examples:
Q: "charizard 151"   -> {"card_name":"Charizard","set_name":"Scarlet & Violet: 151","collector_number":null}
Q: "pikachu 25"      -> {"card_name":"Pikachu","set_name":null,"collector_number":"25"}
Q: "dragonite fossil"-> {"card_name":"Dragonite","set_name":"Fossil","collector_number":null}
Q: "base set charizard" -> {"card_name":"Charizard","set_name":"Base Set","collector_number":null}
Q: "mew ex 151"      -> {"card_name":"Mew ex","set_name":"Scarlet & Violet: 151","collector_number":null}
Q: "moonbreon"       -> {"card_name":"Umbreon VMAX","set_name":"Evolving Skies","collector_number":null}
Q: "charizard vmax 20/189" -> {"card_name":"Charizard VMAX","set_name":null,"collector_number":"20/189"}
Q: "lightning bolt 4ed"-> {"card_name":"Lightning Bolt","set_name":"Fourth Edition","collector_number":null}
Q: "luffy op01-001"  -> {"card_name":"Luffy","set_name":null,"collector_number":"OP01-001"}

Respond with ONLY valid JSON. No markdown fences, no explanation."""


# Pokemon set aliases — lowercase alias -> canonical set_name. Order of keys
# doesn't matter; we match longest-first at parse time.
_POKEMON_SET_ALIASES: dict[str, str] = {
    # PokemonTCG / TCGdex both index this set as simply "151" — using the
    # SV-prefixed form breaks set-filtered lookups.
    "151": "151",
    "sv 151": "151",
    "scarlet & violet 151": "151",
    "fossil": "Fossil",
    "base set": "Base Set",
    "jungle": "Jungle",
    "team rocket": "Team Rocket",
    "gym heroes": "Gym Heroes",
    "gym challenge": "Gym Challenge",
    "neo genesis": "Neo Genesis",
    "neo destiny": "Neo Destiny",
    "neo revelation": "Neo Revelation",
    "neo discovery": "Neo Discovery",
    "evolving skies": "Evolving Skies",
    "lost origin": "Lost Origin",
    "silver tempest": "Silver Tempest",
    "crown zenith": "Crown Zenith",
    "brilliant stars": "Brilliant Stars",
    "astral radiance": "Astral Radiance",
    "obsidian flames": "Obsidian Flames",
    "paldea evolved": "Paldea Evolved",
    "paldean fates": "Paldean Fates",
    "hidden fates": "Hidden Fates",
    "paradox rift": "Paradox Rift",
    "temporal forces": "Temporal Forces",
    "twilight masquerade": "Twilight Masquerade",
    "stellar crown": "Stellar Crown",
    "shrouded fable": "Shrouded Fable",
    "surging sparks": "Surging Sparks",
    "prismatic evolutions": "Prismatic Evolutions",
    "prismatic": "Prismatic Evolutions",
    "darkness ablaze": "Darkness Ablaze",
    "vivid voltage": "Vivid Voltage",
    "fusion strike": "Fusion Strike",
    "chilling reign": "Chilling Reign",
    "battle styles": "Battle Styles",
    "rebel clash": "Rebel Clash",
    "champions path": "Champion's Path",
    "champion's path": "Champion's Path",
    "shining fates": "Shining Fates",
    "celebrations": "Celebrations",
    "pokemon go": "Pokemon Go",
    "paradise dragona": "Paradise Dragona",
}

# Magic set aliases (category_id=1)
_MTG_SET_ALIASES: dict[str, str] = {
    "4ed": "Fourth Edition",
    "4th": "Fourth Edition",
    "3ed": "Revised Edition",
    "rev": "Revised Edition",
    "revised": "Revised Edition",
    "lea": "Limited Edition Alpha",
    "alpha": "Limited Edition Alpha",
    "leb": "Limited Edition Beta",
    "beta": "Limited Edition Beta",
    "unl": "Unlimited Edition",
    "unlimited": "Unlimited Edition",
    "dmu": "Dominaria United",
    "mh2": "Modern Horizons 2",
    "mh3": "Modern Horizons 3",
    "neo": "Kamigawa: Neon Dynasty",
    "znr": "Zendikar Rising",
}


_POKEMON_NICKNAMES_CACHE: Optional[list[dict]] = None


def _load_pokemon_nicknames() -> list[dict]:
    """Load community-nickname -> fields map from app/data/pokemon_nicknames.json.

    Returned list is a flat list of {"alias": str, "card_name": str,
    "set_name": str | None, "collector_number": str | None}, pre-lowercased
    on ``alias``. Failures (missing file, bad JSON) are logged once and cached
    as an empty list — the parser degrades gracefully.
    """
    global _POKEMON_NICKNAMES_CACHE
    if _POKEMON_NICKNAMES_CACHE is not None:
        return _POKEMON_NICKNAMES_CACHE
    import os
    path = os.path.join(os.path.dirname(__file__), "data", "pokemon_nicknames.json")
    entries: list[dict] = []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for e in data.get("nicknames", []) or []:
            for alias in e.get("aliases", []) or []:
                entries.append({
                    "alias": alias.strip().lower(),
                    "card_name": e.get("card_name"),
                    "set_name": e.get("set_name"),
                    "collector_number": e.get("collector_number"),
                })
        # Longest-first so "moonbreon alt" beats "moonbreon"
        entries.sort(key=lambda x: len(x["alias"]), reverse=True)
    except FileNotFoundError:
        logger.info("[pokemon_scanner] nicknames file missing at %s", path)
    except Exception as exc:
        logger.warning("[pokemon_scanner] failed to load nicknames: %s", exc)
    _POKEMON_NICKNAMES_CACHE = entries
    return entries


def _heuristic_parse_query(query: str, category_id: str) -> ExtractedFields:
    """Parse query without calling AI. Handles nicknames, set aliases, and
    several collector-number formats. Safe to call alone or as a fallback.
    """
    fields = ExtractedFields()
    q = query.strip()
    if not q:
        return fields
    q_norm = re.sub(r"\s+", " ", q.lower())

    # Nickname dictionary (Pokemon only — the file is Pokemon-specific).
    if category_id in ("3", "85"):
        for entry in _load_pokemon_nicknames():
            alias = entry["alias"]
            if alias and alias in q_norm:
                fields.card_name = entry.get("card_name")
                fields.set_name = entry.get("set_name")
                fields.collector_number = entry.get("collector_number")
                fields.extraction_method = "nickname"
                logger.info(
                    "[pokemon_scanner] Nickname match %r -> name=%s set=%s num=%s",
                    alias, fields.card_name, fields.set_name, fields.collector_number,
                )
                return fields

    # Full "X/Y" collector number
    m = re.search(r"\b(\d{1,4})\s*/\s*(\d{1,4})\b", q)
    if m:
        fields.collector_number = f"{m.group(1)}/{m.group(2)}"
        q = (q[:m.start()] + q[m.end():]).strip()
        q_norm = re.sub(r"\s+", " ", q.lower())

    # Set-specific collector numbers like "OP01-003", "ST01-001"
    if not fields.collector_number:
        m2 = re.search(r"\b([a-z]{2,4})\s*-?\s*(\d{2,3})\s*-\s*(\d{2,4})\b", q, flags=re.I)
        if m2:
            fields.collector_number = f"{m2.group(1).upper()}{m2.group(2)}-{m2.group(3)}"
            q = (q[:m2.start()] + q[m2.end():]).strip()
            q_norm = re.sub(r"\s+", " ", q.lower())

    # Set alias match — longest-first so "neo destiny" beats "neo".
    alias_dict = _POKEMON_SET_ALIASES if category_id in ("3", "85") else (
        _MTG_SET_ALIASES if category_id == "1" else {}
    )
    if alias_dict and not fields.set_name:
        for alias in sorted(alias_dict.keys(), key=len, reverse=True):
            # Word-boundary match so "151" doesn't eat "a1510".
            pat = r"(?:^|\s)" + re.escape(alias) + r"(?=\s|$)"
            m3 = re.search(pat, q_norm)
            if m3:
                fields.set_name = alias_dict[alias]
                # Strip the alias token from the query
                span_start = m3.start()
                # Advance past optional leading whitespace captured by (?:^|\s)
                while span_start < len(q) and q[span_start].isspace():
                    span_start += 1
                q = (q[:span_start] + q[span_start + len(alias):]).strip()
                q = re.sub(r"\s{2,}", " ", q)
                q_norm = q.lower()
                break

    # Bare number (no denominator) — only if we have something left as a name.
    if not fields.collector_number:
        m4 = re.search(r"\b(\d{1,3})\b", q)
        if m4 and re.sub(r"\d+", "", q).strip():
            fields.collector_number = m4.group(1)
            q = (q[:m4.start()] + q[m4.end():]).strip()

    # Whatever remains is the card name.
    q = re.sub(r"\s{2,}", " ", q).strip(" -,:;")
    fields.card_name = q or None
    fields.extraction_method = fields.extraction_method or "heuristic"
    return fields


def _parse_search_query(query: str, category_id: str = "3") -> ExtractedFields:
    """Parse a free-text card search query into structured fields.

    Strategy: run the heuristic first (fast, deterministic, covers nicknames
    and set aliases). Then ask the AI as an augmentation — if the AI parse
    is clearly richer (fills in a set or number the heuristic missed), merge
    those hints in. This makes behavior predictable when the AI is down.
    """
    heur = _heuristic_parse_query(query, category_id)

    if not has_ai_key():
        return heur

    try:
        client = get_ai_client()
        fast_model = get_fast_model()
        response = client.chat.completions.create(
            model=fast_model,
            messages=[
                {"role": "system", "content": _TEXT_SEARCH_PARSE_PROMPT},
                {"role": "user", "content": query},
            ],
            max_tokens=200,
        )
        raw = response.choices[0].message.content or ""
        data = _loads_ai_json(raw)
        if data is None:
            logger.warning("[pokemon_scanner] Text search parse returned non-JSON: %s", raw[:200])
            return heur
        ai_name = data.get("card_name") or None
        ai_set = data.get("set_name") or None
        ai_number = data.get("collector_number") or None
        if not (ai_name or ai_number):
            return heur
        # If heuristic came from the nickname dict, trust it over AI.
        if heur.extraction_method == "nickname":
            return heur
        merged = ExtractedFields(
            card_name=ai_name or heur.card_name,
            set_name=ai_set or heur.set_name,
            collector_number=ai_number or heur.collector_number,
            extraction_method="ai",
        )
        logger.info(
            "[pokemon_scanner] Text search parsed (ai): name=%s, set=%s, number=%s",
            merged.card_name, merged.set_name, merged.collector_number,
        )
        return merged
    except Exception as exc:
        logger.warning("[pokemon_scanner] AI query parse failed, using heuristic: %s", exc)
        return heur


async def text_search_cards(query: str, category_id: str = "3") -> dict[str, Any]:
    """Search for cards by text query. Returns same shape as scan pipeline."""
    t_start = time.monotonic()

    game = _game_for_category(category_id)

    if not query or not query.strip():
        return {**asdict(ScanResult(status="ERROR", error="Empty search query")), "game": game}

    fields = _parse_search_query(query.strip(), category_id=category_id)
    if not fields.card_name and not fields.collector_number:
        return {**asdict(ScanResult(status="NO_MATCH", error="Could not parse search query")), "game": game}

    is_pokemon = category_id in ("3", "85")

    # For Pokemon: try heuristic set extraction from the query against known sets
    if is_pokemon and not fields.set_name and fields.card_name:
        tcgdex_sets = await _fetch_tcgdex_sets()
        set_names = sorted(
            [(s.get("name", ""), s.get("id", "")) for s in tcgdex_sets if s.get("name")],
            key=lambda x: len(x[0]),
            reverse=True,
        )
        query_lower = fields.card_name.lower()
        for sname, _sid in set_names:
            sname_lower = sname.lower()
            if len(sname_lower) < 3:
                continue
            if query_lower.endswith(sname_lower):
                remainder = query_lower[: -len(sname_lower)].strip()
                if remainder:
                    fields.card_name = remainder.strip().title()
                    fields.set_name = sname
                    logger.info(
                        "[pokemon_scanner] Heuristic set extraction: name=%s, set=%s",
                        fields.card_name, fields.set_name,
                    )
                    break

    settings = get_settings()
    ptcg_key = settings.pokemon_tcg_api_key or ""

    candidates = await _lookup_candidates_by_category(fields, category_id, ptcg_key)

    # Pokemon-only: supplement with a broad PokemonTCG name search for image coverage
    if is_pokemon and fields.card_name:
        async with httpx.AsyncClient(timeout=10.0) as client:
            ptcg_extra = await _pokemontcg_search(
                client, name=fields.card_name, api_key=ptcg_key, limit=10,
            )
        existing = {}
        for c in candidates:
            existing[(c.name.lower(), c.number.split("/")[0] if c.number else "")] = c
        for pc in ptcg_extra:
            key = (pc.name.lower(), pc.number.split("/")[0] if pc.number else "")
            existing_card = existing.get(key)
            if existing_card is None:
                candidates.append(pc)
                existing[key] = pc
            elif not existing_card.image_url and pc.image_url:
                existing_card.image_url = pc.image_url
                existing_card.image_url_small = pc.image_url_small

    if not candidates:
        return {**asdict(ScanResult(
            status="NO_MATCH",
            error=f"No cards found for '{query}'",
            processing_time_ms=round((time.monotonic() - t_start) * 1000, 1),
        )), "game": game}

    scored = score_candidates(candidates, fields)

    # Set-aware rerank: score_candidates uses Levenshtein for set similarity,
    # which is noisy when the parsed set is "151" and the DB set is
    # "Scarlet & Violet: 151". Apply _norm_set substring match as a
    # tiebreaker so the correct printing beats a same-named card from a
    # random older set (e.g. Dragonite #4 Fossil over McDonald's promos).
    if fields.set_name:
        want_set = _norm_set(fields.set_name)
        want_name = _norm_name(fields.card_name)

        def _set_rerank_key(c):
            cand_set = _norm_set(c.set_name)
            if want_set and cand_set:
                if cand_set == want_set:
                    set_bonus = 2
                elif want_set in cand_set or cand_set in want_set:
                    set_bonus = 1
                else:
                    set_bonus = 0
            else:
                set_bonus = 0
            name_match = 1 if (want_name and _norm_name(c.name) == want_name) else 0
            return (set_bonus, name_match, c.score)

        scored = sorted(scored, key=_set_rerank_key, reverse=True)

    top_n = scored[:8]
    await asyncio.gather(
        *[_enrich_price_fast(c, ptcg_key=ptcg_key, category_id=category_id) for c in top_n],
    )

    best = top_n[0] if top_n else None
    status = "MATCHED" if best and best.confidence in ("HIGH", "MEDIUM") else "AMBIGUOUS"

    result = ScanResult(
        status=status,
        best_match=asdict(best) if best else None,
        candidates=[asdict(c) for c in top_n],
        extracted_fields=asdict(fields),
        processing_time_ms=round((time.monotonic() - t_start) * 1000, 1),
    )
    result_dict = asdict(result)
    result_dict["debug"] = {"engine": "text_search", "query": query}
    result_dict["game"] = _game_for_category(category_id)
    return result_dict


def _stamp_game_for(result: dict, category_id: str) -> dict:
    """Ensure every orchestrator return path has a canonical ``game`` field."""
    if result.get("game"):
        return result
    effective = (result.get("debug") or {}).get("effective_category_id") or category_id
    result["game"] = _game_for_category(effective)
    return result


_VALID_SCAN_MODES = ("fast", "balanced", "accurate")


async def run_pipeline(
    image_b64: str,
    category_id: str = "3",
    mode: str = "balanced",
) -> dict[str, Any]:
    """Dispatch to the requested scanner mode.

    - ``fast``: Ximilar only, no validation. Lowest cost / latency.
    - ``balanced`` (default): Ximilar first; HIGH short-circuits; otherwise
      return Ximilar optimistically and fire Haiku + Gemini Flash in parallel
      in the background, resolved by a 3-way majority vote.
    - ``accurate``: existing sequential-with-tiebreaker flow — Ximilar first,
      MEDIUM backgrounds a single Opus call, LOW blocks on Opus then falls
      through a Gemini Pro tiebreaker on disagreement.

    ``category_id`` controls price enrichment (which TCGTracking category to
    search).
    """
    mode = (mode or "balanced").strip().lower()
    if mode not in _VALID_SCAN_MODES:
        logger.info("[pokemon_scanner] Unknown scan mode=%r, falling back to balanced", mode)
        mode = "balanced"

    cache_key = (mode, category_id, _hash_image(image_b64))
    cached = _scan_cache_get(cache_key)
    if cached is not None:
        import copy as _copy
        hit = _copy.deepcopy(cached)
        hit.setdefault("debug", {})["cache_hit"] = True
        return hit

    if mode == "fast":
        result = await _run_fast_pipeline(image_b64, category_id)
    elif mode == "balanced":
        result = await _run_balanced_pipeline(image_b64, category_id)
        result.setdefault("debug", {})["mode"] = "balanced"
        result = _stamp_game_for(result, category_id)
    else:
        # mode == "accurate"
        result = await _run_accurate_pipeline(image_b64, category_id)
        result.setdefault("debug", {})["mode"] = "accurate"
        result = _stamp_game_for(result, category_id)

    _scan_cache_put(cache_key, result)
    return result


async def _run_fast_pipeline(image_b64: str, category_id: str) -> dict[str, Any]:
    """Ximilar-only path. Returns Ximilar's result even at LOW confidence."""
    settings = get_settings()
    if not settings.ximilar_api_token:
        # No Ximilar token — Fast mode can't do anything useful
        return _stamp_game_for(asdict(ScanResult(
            status="ERROR",
            error="Fast mode requires XIMILAR_API_TOKEN",
        )), category_id)

    try:
        result = await _run_ximilar_pipeline(
            image_b64, settings.ximilar_api_token, category_id,
        )
    except Exception as exc:
        logger.error("[pokemon_scanner] Fast: Ximilar pipeline failed: %s", exc)
        return _stamp_game_for(asdict(ScanResult(
            status="ERROR",
            error=f"Ximilar pipeline failed: {exc}",
            processing_time_ms=0,
        )), category_id)

    result.setdefault("debug", {})["mode"] = "fast"
    result["debug"]["pipeline_tier"] = "fast_ximilar_only"
    result["debug"]["engines_used"] = ["ximilar"]
    result["debug"].setdefault("tiebreaker_used", False)
    result["debug"].setdefault("tiebreaker_winner", None)
    # Fast mode never enters the validation-polling path.
    result.pop("validation_pending", None)
    return _stamp_game_for(result, category_id)


async def _run_accurate_pipeline(image_b64: str, category_id: str) -> dict[str, Any]:
    """
    Sequential Ximilar + Opus vision + Gemini Pro tiebreaker flow.

    With Ximilar configured:
      - Ximilar confidence >= 0.85: accept immediately, no vision call
      - Ximilar confidence 0.60-0.84: return Ximilar optimistically,
        trigger vision validation in background (poll via scan_id)
      - Ximilar confidence < 0.60: wait for vision pipeline, merge, return

    Falls back to whichever single engine is available.
    """
    settings = get_settings()
    has_ximilar = bool(settings.ximilar_api_token)
    has_vision = has_ai_key()

    def _stamp_game(result: dict) -> dict:
        return _stamp_game_for(result, category_id)

    # No Ximilar → vision only (if available)
    if not has_ximilar:
        if not has_vision:
            return _stamp_game(asdict(ScanResult(
                status="ERROR",
                error="No scanning engine configured. Set XIMILAR_API_TOKEN or an AI provider key.",
            )))
        result = await _run_vision_pipeline(image_b64, category_id)
        result.setdefault("debug", {})["pipeline_tier"] = "vision_only"
        result["debug"]["engines_used"] = ["vision"]
        result["debug"].setdefault("tiebreaker_used", False)
        result["debug"].setdefault("tiebreaker_winner", None)
        return _stamp_game(result)

    # Run Ximilar first (fast: 2-4s)
    try:
        ximilar_result = await _run_ximilar_pipeline(image_b64, settings.ximilar_api_token, category_id)
    except Exception as exc:
        logger.error("[pokemon_scanner] Ximilar pipeline failed: %s", exc)
        if has_vision:
            result = await _run_vision_pipeline(image_b64, category_id)
            result.setdefault("debug", {})["pipeline_tier"] = "ximilar_failed_vision_fallback"
            result["debug"]["engines_used"] = ["vision"]
            result["debug"].setdefault("tiebreaker_used", False)
            result["debug"].setdefault("tiebreaker_winner", None)
            return _stamp_game(result)
        return _stamp_game(asdict(ScanResult(
            status="ERROR",
            error=f"Ximilar pipeline failed: {exc}",
            processing_time_ms=0,
        )))

    # No vision available → return Ximilar regardless of confidence
    if not has_vision:
        ximilar_result.setdefault("debug", {})["pipeline_tier"] = "ximilar_only"
        ximilar_result["debug"]["engines_used"] = ["ximilar"]
        ximilar_result["debug"].setdefault("tiebreaker_used", False)
        ximilar_result["debug"].setdefault("tiebreaker_winner", None)
        return _stamp_game(ximilar_result)

    confidence = _ximilar_confidence(ximilar_result)
    x_status = ximilar_result.get("status", "ERROR")
    scan_id = str(uuid.uuid4())

    if confidence >= XIMILAR_CONFIDENCE_HIGH and x_status not in ("ERROR", "NO_MATCH"):
        # HIGH confidence — accept immediately, no vision call
        logger.info(
            "[pokemon_scanner] HIGH confidence (%.2f) — accepting Ximilar, skipping vision, category=%s",
            confidence, category_id,
        )
        ximilar_result["scan_id"] = scan_id
        ximilar_result.setdefault("debug", {})["pipeline_tier"] = "high_confidence"
        ximilar_result["debug"]["engines_used"] = ["ximilar"]
        ximilar_result["debug"]["tiebreaker_used"] = False
        ximilar_result["debug"]["tiebreaker_winner"] = None
        return _stamp_game(ximilar_result)

    elif confidence >= XIMILAR_CONFIDENCE_MEDIUM and x_status not in ("ERROR", "NO_MATCH"):
        # MEDIUM confidence — return optimistically, fire background vision validation
        logger.info(
            "[pokemon_scanner] MEDIUM confidence (%.2f) — optimistic return + background vision validation, category=%s",
            confidence, category_id,
        )
        ximilar_result["scan_id"] = scan_id
        ximilar_result["validation_pending"] = True
        ximilar_result.setdefault("debug", {})["pipeline_tier"] = "medium_confidence"
        ximilar_result["debug"]["engines_used"] = ["ximilar"]
        ximilar_result["debug"]["tiebreaker_used"] = False
        ximilar_result["debug"]["tiebreaker_winner"] = None

        _insert_pending_validation(scan_id, (time.monotonic(), None))  # sentinel: vision in progress

        import copy
        ximilar_copy = copy.deepcopy(ximilar_result)
        asyncio.create_task(_background_vision_validate(
            scan_id, image_b64, ximilar_copy, category_id,
        ))

        return _stamp_game(ximilar_result)

    else:
        # LOW confidence — wait for vision, merge, return
        logger.info(
            "[pokemon_scanner] LOW confidence (%.2f) — waiting for vision pipeline, category=%s",
            confidence, category_id,
        )
        try:
            vision_result = await _run_vision_pipeline(image_b64, category_id)
        except Exception as exc:
            logger.error("[pokemon_scanner] Vision pipeline failed in LOW tier: %s", exc)
            vision_result = None
        merged = await _merge_engine_results(ximilar_result, vision_result, image_b64)
        merged["scan_id"] = scan_id
        merged.setdefault("debug", {})["pipeline_tier"] = "low_confidence"
        return _stamp_game(merged)


def get_validation_result(scan_id: str, ack: bool = False) -> dict | None:
    """Check if a background OCR validation has completed.

    Non-destructive by default: completed results stay in the cache until TTL
    cleanup or an explicit ack. Previously the first successful poll popped
    the entry, so any legitimate re-read (retry, second consumer, poll-race
    on reconnect) returned 404 and lost the result.

    Args:
      ack: if True, remove the entry after returning a completed result.
    """
    _cleanup_stale_validations()
    if scan_id not in _pending_validations:
        return None
    ts, result = _pending_validations[scan_id]
    if result is None:
        return {"validation_status": "pending", "scan_id": scan_id}
    if ack:
        _pending_validations.pop(scan_id, None)
    return result


def _save_to_history(result: dict) -> None:
    """Save a scan result to the in-memory history ring buffer."""
    import datetime
    entry = {
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
        "status": result.get("status"),
        "best_match_name": (result.get("best_match") or {}).get("name"),
        "best_match_number": (result.get("best_match") or {}).get("number"),
        "best_match_set": (result.get("best_match") or {}).get("set_name"),
        "best_match_score": (result.get("best_match") or {}).get("score"),
        "best_match_confidence": (result.get("best_match") or {}).get("confidence"),
        "best_match_price": (result.get("best_match") or {}).get("market_price"),
        "candidates_count": len(result.get("candidates") or []),
        "processing_time_ms": result.get("processing_time_ms"),
        "extracted_name": (result.get("extracted_fields") or {}).get("card_name"),
        "extracted_number": (result.get("extracted_fields") or {}).get("collector_number"),
        "extracted_set": (result.get("extracted_fields") or {}).get("set_name"),
        "ocr_text": (result.get("debug") or {}).get("ocr_raw_text", ""),
        "ocr_confidence": (result.get("debug") or {}).get("ocr_confidence"),
        "extraction_method": (result.get("debug") or {}).get("extraction_method", "regex"),
        "disambiguation": result.get("disambiguation_method"),
        "error": result.get("error"),
        "debug": result.get("debug"),
    }
    _scan_history.appendleft(entry)


def get_scan_history() -> list[dict]:
    """Return the recent scan history (newest first)."""
    return list(_scan_history)


def _elapsed(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 1)
