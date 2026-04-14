"""
Pokemon card scanning pipeline.

Multi-stage pipeline: Capture -> Preprocess (Pillow) -> OCR (Google Vision)
-> Lookup (TCGdex + PokemonTCG) -> Score -> Disambiguate (OpenAI Vision) -> Result.

Designed for Pokemon cards only. Identification works backward from the
collector number, which uniquely identifies a card within a set.
"""
from __future__ import annotations

import base64
import collections
import io
import logging
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import httpx
from openai import OpenAI

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

DISAMBIGUATION_THRESHOLD = 15
VISION_MODEL = "gpt-4o-mini"

GOOGLE_VISION_URL = "https://vision.googleapis.com/v1/images:annotate"
TCGDEX_BASE = "https://api.tcgdex.net/v2/en"
POKEMONTCG_BASE = "https://api.pokemontcg.io/v2"

COLLECTOR_NUM_PATTERNS = [
    re.compile(r"(TG\d{1,3})\s*/\s*(TG\d{1,3})", re.I), # TG12/TG30
    re.compile(r"(GG\d{1,3})\s*/?\s*(GG\d{1,3})?", re.I),# GG21/GG70
    re.compile(r"(SVP\s*\d{2,3})", re.I),                # SVP001
    re.compile(r"(SWSH\d{2,3})", re.I),                  # SWSH promo
    re.compile(r"(SV\d{2,3})", re.I),                    # SV promo
    re.compile(r"(\d{1,4})\s*/\s*(\d{1,4})"),            # 123/197 (last — most generic)
]

# Broader pattern to grab a collector-number-like region even with OCR noise
# e.g. "228/2ND" -> grab "228" and try to reconstruct total from surrounding chars
_BROAD_NUMBER_RE = re.compile(r"(\d{1,4})\s*/\s*(\S{1,5})")

VARIANT_KEYWORDS = [
    "VMAX", "VSTAR", "V", "EX", "GX", "ex", "FULL ART", "TRAINER GALLERY",
    "ALT ART", "ILLUSTRATION RARE", "SPECIAL ART RARE", "REVERSE HOLO",
    "SECRET RARE", "ULTRA RARE", "RAINBOW RARE", "GOLD",
]

_tcgdex_sets_cache: list[dict] | None = None

# In-memory ring buffer of recent scan results for debugging
_scan_history: collections.deque[dict] = collections.deque(maxlen=25)


# ---------------------------------------------------------------------------
# Stage B: Preprocess
# ---------------------------------------------------------------------------

def preprocess_image(raw_bytes: bytes) -> bytes:
    """
    Light preprocessing for OCR: auto-orient, resize to 1500px, mild sharpen.
    Keeps color — grayscale destroys too much info on Pokemon cards.
    """
    try:
        from PIL import Image, ImageEnhance, ImageFilter, ImageOps
    except ImportError:
        logger.warning("[pokemon_scanner] Pillow not installed, skipping preprocessing")
        return raw_bytes

    try:
        img = Image.open(io.BytesIO(raw_bytes))
        img = ImageOps.exif_transpose(img) or img

        max_dim = max(img.width, img.height)
        if max_dim > 1500:
            scale = 1500 / max_dim
            new_w = int(img.width * scale)
            new_h = int(img.height * scale)
            img = img.resize((new_w, new_h), Image.LANCZOS)

        img = img.filter(ImageFilter.UnsharpMask(radius=0.8, percent=80, threshold=3))
        img = ImageEnhance.Contrast(img).enhance(1.08)

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=92)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("[pokemon_scanner] Preprocessing failed, using raw image: %s", exc)
        return raw_bytes


# ---------------------------------------------------------------------------
# Stage C: Google Vision OCR
# ---------------------------------------------------------------------------

async def run_ocr(image_bytes: bytes, api_key: str) -> ExtractedFields:
    """Send image to Google Cloud Vision TEXT_DETECTION and extract structured fields."""
    b64 = base64.b64encode(image_bytes).decode()

    payload = {
        "requests": [{
            "image": {"content": b64},
            "features": [{"type": "TEXT_DETECTION", "maxResults": 1}],
        }]
    }

    fields = ExtractedFields()

    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            GOOGLE_VISION_URL,
            params={"key": api_key},
            json=payload,
        )

    if resp.status_code != 200:
        fields.extraction_warnings.append(f"vision_api_http_{resp.status_code}")
        logger.error("[pokemon_scanner] Google Vision HTTP %s: %s", resp.status_code, resp.text[:300])
        return fields

    data = resp.json()
    responses = data.get("responses", [])
    if not responses:
        fields.extraction_warnings.append("no_vision_response")
        return fields

    annotations = responses[0].get("textAnnotations", [])
    if not annotations:
        fields.extraction_warnings.append("no_text_detected")
        return fields

    raw_text = annotations[0].get("description", "")
    fields.ocr_raw_text = raw_text

    full_text_annotation = responses[0].get("fullTextAnnotation", {})
    pages = full_text_annotation.get("pages", [])
    if pages:
        conf_values = []
        for page in pages:
            for block in page.get("blocks", []):
                conf_values.append(block.get("confidence", 0))
        if conf_values:
            fields.ocr_confidence = sum(conf_values) / len(conf_values)

    _extract_fields_from_text(raw_text, fields)
    return fields


def _normalize_ocr_number(text: str) -> str:
    """Fix common OCR misreads in number strings."""
    text = text.replace("\\", "/").replace("|", "/").replace(" ", "")

    if "/" in text:
        parts = text.split("/", 1)
        parts = [_fix_digit_string(p) for p in parts]
        return "/".join(parts)

    return _fix_digit_string(text)


def _fix_digit_string(s: str) -> str:
    """Convert a string that should be all digits, fixing OCR misreads."""
    result = []
    for ch in s:
        if ch.isdigit():
            result.append(ch)
        elif ch in ("O", "o", "D", "Q"):
            result.append("0")
        elif ch in ("l", "I", "i", "|", "!"):
            result.append("1")
        elif ch in ("S", "s", "$"):
            result.append("5")
        elif ch in ("B",):
            result.append("8")
        elif ch in ("Z", "z"):
            result.append("2")
        elif ch in ("G",):
            result.append("6")
        elif ch in ("T", "t"):
            result.append("7")
        # Skip letters that are clearly not digit misreads (N, A, E, etc.)
    return "".join(result) if result else s


def _extract_fields_from_text(raw_text: str, fields: ExtractedFields) -> None:
    """Parse OCR raw text into structured ExtractedFields."""
    lines = [ln.strip() for ln in raw_text.strip().split("\n") if ln.strip()]
    text_upper = raw_text.upper()

    # --- Collector number (most reliable identifier) ---
    for pattern in COLLECTOR_NUM_PATTERNS:
        match = pattern.search(raw_text)
        if match:
            fields.collector_number_raw = match.group(0).strip()
            fields.collector_number = _normalize_ocr_number(fields.collector_number_raw)
            break

    # Fallback: broad pattern for OCR-mangled numbers like "228/2ND" -> "228/217"
    if not fields.collector_number:
        broad = _BROAD_NUMBER_RE.search(raw_text)
        if broad:
            fields.collector_number_raw = broad.group(0).strip()
            first = broad.group(1)
            second_raw = broad.group(2)
            second = _fix_digit_string(second_raw)
            if second and second.isdigit():
                fields.collector_number = f"{first}/{second}"
            else:
                fields.collector_number = first

    # --- HP value (extract early — helps identify the name line) ---
    hp_match = re.search(r"(\d{2,3})\s*HP", raw_text, re.I)
    if hp_match:
        fields.hp_value = hp_match.group(1)

    # --- Card name: find the best candidate from top lines ---
    _SKIP_WORDS = {"WEAKNESS", "RESISTANCE", "RETREAT", "TRAINER",
                   "SUPPORTER", "ITEM", "TOOL", "STADIUM", "ENERGY", "POKEMON",
                   "POKÉMON", "ILLUSTRATOR", "REGULATION", "ABILITY"}
    _STAGE_WORDS = {"BASIC", "STAGE"}
    _POKEMON_FRAGMENTS = {"poke", "mon", "pokémon", "pokemon", "poké"}
    for line in lines[:10]:
        cleaned = re.sub(r"[^\w\s\-\'é.':]", "", line).strip()
        cleaned = re.sub(r"\b\d{2,3}\s*HP\b", "", cleaned, flags=re.I).strip()
        if len(cleaned) < 3:
            continue
        upper_words = set(cleaned.upper().split())

        # Lines with game-mechanic words are never card names
        if upper_words & _SKIP_WORDS:
            continue

        # Lines with BASIC/STAGE may contain the name — try to extract it
        if upper_words & _STAGE_WORDS:
            name_part = re.sub(r"\bSTAGE\s*[\d\*]*", "", cleaned, flags=re.I)
            name_part = re.sub(r"\bBASIC\b", "", name_part, flags=re.I)
            name_part = re.sub(r"\bEvolves?\s+from\s+\S+", "", name_part, flags=re.I)
            name_part = re.sub(r"\bSHOP\b", "", name_part, flags=re.I)
            name_part = re.sub(r"\b\d+\b", "", name_part)
            name_part = name_part.strip(" *-.:").strip()
            if len(name_part) >= 3:
                fields.card_name = name_part
                break
            continue

        if re.match(r"^[\d\s/\-]+$", cleaned):
            continue
        if cleaned.lower().strip() in _POKEMON_FRAGMENTS:
            continue
        # Skip single short words (< 5 chars) that aren't real names
        words = cleaned.split()
        if len(words) == 1 and len(words[0]) < 5 and "'" not in words[0]:
            continue
        fields.card_name = cleaned
        break

    # --- Set name: extract from the line containing the collector number ---
    # On Pokemon cards, the set abbreviation (e.g. "ASCEN", "SVI", "PAL")
    # appears on the same line as the collector number.
    _set_extracted = False
    if fields.collector_number_raw:
        for line in lines:
            if fields.collector_number_raw in line or (
                fields.collector_number and fields.collector_number.split("/")[0] in line
            ):
                # Strip the collector number and surrounding punctuation/digits
                remainder = line
                if fields.collector_number_raw:
                    remainder = remainder.replace(fields.collector_number_raw, "")
                remainder = re.sub(r"\d{1,4}\s*/\s*\d{1,4}", "", remainder)
                # Remove standalone single uppercase letters (regulation marks)
                remainder = re.sub(r"\b[A-Z]\b", "", remainder).strip()
                # Remove common stray characters
                remainder = re.sub(r"[^\w\s]", "", remainder).strip()
                if remainder and len(remainder) >= 2:
                    fields.set_name = remainder
                    _set_extracted = True
                break

    # Fallback: scan bottom lines for short set-name-like text
    if not _set_extracted:
        _SET_SKIP_KW = {"WEAKNESS", "RESISTANCE", "RETREAT", "HP", "DAMAGE",
                        "ATTACK", "ILLUSTRATOR", "NINTENDO", "CREATURES",
                        "GAME FREAK", "ABILITY", "FLIP", "COIN", "SEARCH",
                        "YOUR", "THIS", "HIDDEN", "ONCE", "DURING"}
        for line in reversed(lines):
            line_clean = line.strip()
            if len(line_clean) < 3 or len(line_clean) > 30:
                continue
            line_upper = line_clean.upper()
            if any(kw in line_upper for kw in _SET_SKIP_KW):
                continue
            if "©" in line_clean or "0202" in line_clean:
                continue
            if fields.collector_number_raw and fields.collector_number_raw in line_clean:
                continue
            if re.match(r"^[\d\s/\-]+$", line_clean):
                continue
            fields.set_name = line_clean
            break

    # --- Variant hints ---
    for keyword in VARIANT_KEYWORDS:
        if keyword.upper() in text_upper:
            fields.variant_hints.append(keyword)

    # --- Language (default English) ---
    fields.language = "English"

    logger.info(
        "[pokemon_scanner] Extracted: name=%s, number=%s, set=%s, hp=%s, variants=%s",
        fields.card_name, fields.collector_number, fields.set_name,
        fields.hp_value, fields.variant_hints,
    )


# ---------------------------------------------------------------------------
# Stage D: TCGdex + PokemonTCG Waterfall Lookup
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
) -> list[dict]:
    """
    Search TCGdex by card name, fetching full details for top results.
    If prefer_number is given, prioritize results whose localId matches.
    """
    resp = await client.get(f"{TCGDEX_BASE}/cards", params={"name": name})
    if resp.status_code != 200:
        return []

    results = resp.json()
    if not isinstance(results, list):
        return []

    # If we have a collector number, prioritize matching results
    if prefer_number:
        num_prefix = prefer_number.split("/")[0].lstrip("0") if "/" in prefer_number else prefer_number.lstrip("0")
        # Sort: exact number matches first, then the rest
        def sort_key(card: dict) -> int:
            lid = (card.get("localId") or "").lstrip("0")
            if lid == num_prefix:
                return 0
            return 1
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
    )


async def _pokemontcg_search(
    client: httpx.AsyncClient,
    name: Optional[str] = None,
    number: Optional[str] = None,
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

    if not q_parts:
        return []

    params = {"q": " ".join(q_parts), "pageSize": str(limit)}

    resp = await client.get(f"{POKEMONTCG_BASE}/cards", params=params, headers=headers)
    if resp.status_code != 200:
        return []

    data = resp.json()
    cards = data.get("data") or []
    results = []
    for card in cards:
        images = card.get("images") or {}
        set_info = card.get("set") or {}

        prices_wrap = card.get("tcgplayer", {}).get("prices", {})
        market_price = None
        for price_type in ("normal", "holofoil", "reverseHolofoil", "1stEditionHolofoil"):
            if price_type in prices_wrap:
                mp = prices_wrap[price_type].get("market")
                if mp is not None:
                    try:
                        market_price = round(float(mp), 2)
                    except (ValueError, TypeError):
                        pass
                    if market_price:
                        break

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
        ))

    return results


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
        if not candidates and fields.card_name:
            tier_reached = 2
            tcgdex_by_name = await _tcgdex_search_by_name(
                client, fields.card_name, limit=10,
                prefer_number=fields.collector_number,
            )
            for tc in tcgdex_by_name:
                candidates.append(_tcgdex_to_candidate(tc))

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
            score=round(total, 1),
            confidence=confidence,
            score_breakdown=breakdown,
            match_reason=" ".join(reason_parts) if reason_parts else "Low-confidence match",
        )
        scored.append(sc)

    scored.sort(key=lambda x: x.score, reverse=True)
    return scored


# ---------------------------------------------------------------------------
# Price enrichment (fetch from PokemonTCG if missing)
# ---------------------------------------------------------------------------

async def _enrich_price(candidate: ScoredCandidate, ptcg_key: str = "") -> None:
    """If candidate has no market_price (e.g. from TCGdex), fetch from PokemonTCG."""
    if candidate.market_price is not None:
        return

    if not candidate.id and not candidate.name:
        return

    async with httpx.AsyncClient(timeout=10.0) as client:
        headers = {"X-Api-Key": ptcg_key} if ptcg_key else {}
        q_parts = []
        if candidate.name:
            q_parts.append(f'name:"{candidate.name}"')
        if candidate.number:
            clean = candidate.number.split("/")[0]
            q_parts.append(f'number:"{clean}"')
        if candidate.set_id:
            q_parts.append(f"set.id:{candidate.set_id}")

        if not q_parts:
            return

        params = {"q": " ".join(q_parts), "pageSize": "1"}
        resp = await client.get(f"{POKEMONTCG_BASE}/cards", params=params, headers=headers)
        if resp.status_code != 200:
            return

        cards = resp.json().get("data") or []
        if not cards:
            return

        prices_wrap = cards[0].get("tcgplayer", {}).get("prices", {})
        for price_type in ("normal", "holofoil", "reverseHolofoil"):
            if price_type in prices_wrap:
                mp = prices_wrap[price_type].get("market")
                if mp is not None:
                    try:
                        candidate.market_price = round(float(mp), 2)
                    except (ValueError, TypeError):
                        pass
                    if candidate.market_price:
                        break

        # Also grab image if missing
        if not candidate.image_url:
            images = cards[0].get("images") or {}
            candidate.image_url = images.get("large", "")
            candidate.image_url_small = images.get("small", "")


# ---------------------------------------------------------------------------
# Stage F: Visual Disambiguation (OpenAI Vision)
# ---------------------------------------------------------------------------

async def disambiguate_with_vision(
    original_b64: str,
    candidates: list[ScoredCandidate],
    openai_key: str,
) -> Optional[int]:
    """
    Use OpenAI Vision to pick the best match among ambiguous candidates.
    Returns the index of the best match, or None on failure.
    """
    if not openai_key or not candidates:
        return None

    content_parts: list[dict] = [
        {
            "type": "image_url",
            "image_url": {
                "url": f"data:image/jpeg;base64,{original_b64}",
                "detail": "high",
            },
        },
        {
            "type": "text",
            "text": (
                "You are identifying a Pokémon card. "
                "The first image is a photo of a card being scanned. "
                "Below are the candidate matches from the database:\n\n"
            ),
        },
    ]

    for i, c in enumerate(candidates):
        content_parts.append({
            "type": "text",
            "text": f"Candidate {i}: {c.name} — {c.number} — {c.set_name} (Score: {c.score})",
        })
        if c.image_url and c.image_url.startswith("http"):
            content_parts.append({
                "type": "image_url",
                "image_url": {"url": c.image_url, "detail": "low"},
            })

    content_parts.append({
        "type": "text",
        "text": (
            "\nWhich candidate image most closely matches the scanned card? "
            "Consider: card layout, artwork, border color, collector number, set symbol. "
            'Respond with JSON only: {"bestMatchIndex": 0, "confidence": 0.0, "reasoning": "..."}'
        ),
    })

    try:
        client = OpenAI(api_key=openai_key, timeout=30.0)
        response = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{"role": "user", "content": content_parts}],
            response_format={"type": "json_object"},
            max_tokens=300,
        )
        raw = response.choices[0].message.content or "{}"
        result = __import__("json").loads(raw)
        idx = result.get("bestMatchIndex")
        if isinstance(idx, int) and 0 <= idx < len(candidates):
            logger.info(
                "[pokemon_scanner] Vision disambiguation picked index=%d (%s), confidence=%.2f, reason=%s",
                idx, candidates[idx].name, result.get("confidence", 0), result.get("reasoning", ""),
            )
            return idx
    except Exception as exc:
        logger.warning("[pokemon_scanner] Vision disambiguation failed: %s", exc)

    return None


# ---------------------------------------------------------------------------
# Stage G: Orchestrator
# ---------------------------------------------------------------------------

async def run_pipeline(image_b64: str) -> dict[str, Any]:
    """
    Run the full 7-stage Pokemon card scanning pipeline.
    Returns a ScanResult as a dict.
    """
    settings = get_settings()
    t_start = time.monotonic()
    debug_info: dict[str, Any] = {
        "preprocessing_applied": [],
        "ocr_raw_text": "",
        "query_tiers_attempted": 0,
        "candidates_before_scoring": 0,
        "stage_times_ms": {},
    }

    def _early_return(r: ScanResult) -> dict:
        d = asdict(r)
        _save_to_history(d)
        return d

    # --- Validate config ---
    if not settings.google_vision_api_key:
        return _early_return(ScanResult(
            status="ERROR",
            error="Google Vision API key not configured (GOOGLE_VISION_API_KEY)",
            processing_time_ms=_elapsed(t_start),
        ))

    # --- Stage B: Preprocess ---
    t_stage = time.monotonic()
    try:
        raw_bytes = base64.b64decode(image_b64)
    except Exception:
        return _early_return(ScanResult(
            status="ERROR",
            error="Invalid base64 image data",
            processing_time_ms=_elapsed(t_start),
        ))

    processed_bytes = preprocess_image(raw_bytes)
    debug_info["preprocessing_applied"] = [
        "exif_transpose", "resize_1500px", "mild_sharpen", "contrast_1.08", "color"
    ]
    debug_info["stage_times_ms"]["preprocess"] = _elapsed(t_stage)

    # --- Stage C: OCR (try preprocessed first, fallback to raw) ---
    t_stage = time.monotonic()
    fields = await run_ocr(processed_bytes, settings.google_vision_api_key)
    debug_info["ocr_raw_text"] = fields.ocr_raw_text
    debug_info["ocr_confidence"] = fields.ocr_confidence
    debug_info["stage_times_ms"]["ocr"] = _elapsed(t_stage)

    # Fallback: if preprocessed image yielded nothing, try the raw image
    if not fields.collector_number and not fields.card_name:
        logger.info("[pokemon_scanner] Preprocessed OCR empty, retrying with raw image")
        t_stage = time.monotonic()
        fields = await run_ocr(raw_bytes, settings.google_vision_api_key)
        debug_info["ocr_raw_text"] = fields.ocr_raw_text
        debug_info["ocr_confidence"] = fields.ocr_confidence
        debug_info["ocr_fallback"] = "raw_image"
        debug_info["stage_times_ms"]["ocr_fallback"] = _elapsed(t_stage)

    if not fields.collector_number and not fields.card_name:
        return _early_return(ScanResult(
            status="NO_MATCH",
            error="Could not extract card name or collector number from image. Try holding the card closer or improving lighting.",
            extracted_fields=asdict(fields),
            processing_time_ms=_elapsed(t_start),
            debug=debug_info,
        ))

    # --- Stage D: Lookup ---
    t_stage = time.monotonic()
    candidates = await lookup_candidates(fields, settings.pokemon_tcg_api_key)
    debug_info["candidates_before_scoring"] = len(candidates)
    debug_info["stage_times_ms"]["lookup"] = _elapsed(t_stage)

    if not candidates:
        return _early_return(ScanResult(
            status="NO_MATCH",
            error="No matching cards found in database",
            extracted_fields=asdict(fields),
            processing_time_ms=_elapsed(t_start),
            debug=debug_info,
        ))

    # --- Stage E: Score ---
    t_stage = time.monotonic()
    scored = score_candidates(candidates, fields)
    debug_info["stage_times_ms"]["score"] = _elapsed(t_stage)

    # --- Price enrichment for top candidates ---
    t_stage = time.monotonic()
    for sc in scored[:3]:
        try:
            await _enrich_price(sc, settings.pokemon_tcg_api_key)
        except Exception as exc:
            logger.debug("[pokemon_scanner] Price enrichment failed: %s", exc)
    debug_info["stage_times_ms"]["price_enrich"] = _elapsed(t_stage)

    # --- Stage F: Disambiguation (if needed) ---
    disambiguation_method: Optional[str] = None
    top = scored[0]

    if len(scored) >= 2 and (scored[0].score - scored[1].score) < DISAMBIGUATION_THRESHOLD:
        ambiguous_set = [s for s in scored if (scored[0].score - s.score) < DISAMBIGUATION_THRESHOLD][:3]

        if settings.openai_api_key:
            t_stage = time.monotonic()
            best_idx = await disambiguate_with_vision(
                image_b64, ambiguous_set, settings.openai_api_key,
            )
            debug_info["stage_times_ms"]["disambiguate"] = _elapsed(t_stage)

            if best_idx is not None:
                ambiguous_set[best_idx].score += 20
                ambiguous_set[best_idx].confidence = "HIGH" if ambiguous_set[best_idx].score >= 80 else "MEDIUM"
                ambiguous_set[best_idx].match_reason += " (confirmed by visual disambiguation)"
                scored.sort(key=lambda x: x.score, reverse=True)
                top = scored[0]
                disambiguation_method = "vision_api"
            else:
                disambiguation_method = "user_pick"
        else:
            disambiguation_method = "user_pick"

    # --- Build result ---
    if top.confidence == "HIGH":
        status = "MATCHED"
    elif disambiguation_method == "user_pick":
        status = "AMBIGUOUS"
    else:
        status = "MATCHED" if top.score >= 50 else "AMBIGUOUS"

    result = ScanResult(
        status=status,
        best_match=asdict(top),
        candidates=[asdict(s) for s in scored[:10]],
        extracted_fields=asdict(fields),
        disambiguation_method=disambiguation_method,
        processing_time_ms=_elapsed(t_start),
        debug=debug_info,
    )

    logger.info(
        "[pokemon_scanner] Pipeline complete: status=%s, best=%s (%.1f), candidates=%d, time=%.0fms",
        result.status, top.name, top.score, len(scored), result.processing_time_ms,
    )

    result_dict = asdict(result)
    _save_to_history(result_dict)
    return result_dict


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
