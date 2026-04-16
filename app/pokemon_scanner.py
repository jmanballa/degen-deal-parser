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
import io
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import httpx
from .ai_client import get_ai_client, get_fast_model, get_model, has_ai_key
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

DISAMBIGUATION_THRESHOLD = 15
VISION_MODEL = get_model(default="gpt-5-nano")

XIMILAR_TCG_URL = "https://api.ximilar.com/collectibles/v2/tcg_id"
GOOGLE_VISION_URL = "https://vision.googleapis.com/v1/images:annotate"
TCGDEX_BASE = "https://api.tcgdex.net/v2/en"
POKEMONTCG_BASE = "https://api.pokemontcg.io/v2"
SCRYFALL_BASE = "https://api.scryfall.com"
YGOPRODECK_BASE = "https://db.ygoprodeck.com/api/v7"
OPTCG_BASE = "https://optcgapi.com/api"
LORCAST_BASE = "https://api.lorcast.com/v0"

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

# Background OCR validation: scan_id -> (timestamp, updated_result_or_None)
_pending_validations: dict[str, tuple[float, dict | None]] = {}
_VALIDATION_TTL = 300  # expire pending validations after 5 minutes

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

    # Primary: AI-powered extraction (understands Pokemon card structure)
    settings = get_settings()
    ai_fields = _ai_extract_fields(raw_text, settings.openai_api_key)
    if ai_fields:
        ai_fields.ocr_raw_text = raw_text
        ai_fields.ocr_confidence = fields.ocr_confidence
        ai_fields.extraction_method = "ai"
        return ai_fields

    # Fallback: regex-based extraction
    _extract_fields_from_text(raw_text, fields)
    fields.extraction_method = "regex"
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


_AI_EXTRACT_PROMPT = """You are a Pokemon card OCR interpreter. Given raw OCR text from a card photo, extract structured fields.

Rules:
- card_name: The Pokemon's full name as printed (e.g. "Erika's Tangela", "Charizard ex"). Fix obvious OCR errors using your knowledge of real Pokemon card names.
- collector_number: The collector number in "X/Y" format (e.g. "218/217"). This appears near the bottom of the card. Fix OCR digit misreads.
- set_name: The set abbreviation as it appears in the OCR text, near the collector number at the bottom of the card. Return the RAW abbreviation (e.g. "ASCEN", "SVI", "TWI", "DWHTEN") — do NOT guess or expand to a full set name. Only return a full set name if the full name is clearly printed in the OCR text. If unsure, return the abbreviation exactly as seen. Ignore single regulation mark letters (J, H, G, etc.) that appear before the abbreviation.
- hp_value: The HP as a string (e.g. "80", "230") or null if not visible.
- variant_hints: Array of variant/rarity keywords found (e.g. ["FULL ART", "EX", "ILLUSTRATION RARE"]). Empty array if none.

IMPORTANT: For set_name, accuracy matters more than helpfulness. A raw abbreviation like "DWHTEN" is better than a wrong guess like "Darkness Ablaze". The lookup system will match abbreviations to sets.

Respond with JSON only. Do not include any explanation."""


def _ai_extract_fields(raw_text: str, openai_key: str) -> Optional[ExtractedFields]:
    """Use AI to interpret raw OCR text into structured card fields.

    Returns populated ExtractedFields on success, None on failure.
    """
    if not has_ai_key() or not raw_text.strip():
        return None

    try:
        client = get_ai_client(timeout=10.0)
        response = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[
                {"role": "system", "content": _AI_EXTRACT_PROMPT},
                {"role": "user", "content": raw_text},
            ],
            max_tokens=300,
            temperature=0.1,
        )
        raw_json = response.choices[0].message.content or "{}"
        data = _loads_ai_json(raw_json)
        if data is None:
            logger.warning("[pokemon_scanner] AI extraction returned non-JSON: %s", raw_json[:200])
            return None

        fields = ExtractedFields()
        fields.card_name = data.get("card_name") or None
        fields.collector_number = data.get("collector_number") or None
        fields.set_name = data.get("set_name") or None
        fields.hp_value = data.get("hp_value") or None
        fields.variant_hints = data.get("variant_hints") or []
        fields.language = "English"
        fields.ocr_raw_text = raw_text

        if not fields.card_name and not fields.collector_number:
            logger.info("[pokemon_scanner] AI extraction returned empty fields")
            return None

        logger.info(
            "[pokemon_scanner] AI extracted: name=%s, number=%s, set=%s, hp=%s, variants=%s",
            fields.card_name, fields.collector_number, fields.set_name,
            fields.hp_value, fields.variant_hints,
        )
        return fields

    except Exception as exc:
        logger.warning("[pokemon_scanner] AI extraction failed: %s", exc)
        return None


def _extract_fields_from_text(raw_text: str, fields: ExtractedFields) -> None:
    """Parse OCR raw text into structured ExtractedFields (regex fallback)."""
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
    """Search Scryfall for Magic: The Gathering cards."""
    if not name:
        return []
    q_parts = [name]
    if set_name:
        q_parts.append(f"set:{set_name}")
    if number:
        clean_num = number.split("/")[0] if "/" in number else number
        q_parts.append(f"number:{clean_num}")

    params = {"q": " ".join(q_parts), "unique": "prints", "order": "released", "dir": "desc"}
    try:
        resp = await client.get(f"{SCRYFALL_BASE}/cards/search", params=params)
        if resp.status_code != 200:
            return []
        data = resp.json().get("data") or []
    except Exception as exc:
        logger.warning("[pokemon_scanner] Scryfall search failed: %s", exc)
        return []

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
    limit: int = 10,
) -> list[CandidateCard]:
    """Search OPTCG API for One Piece Card Game cards."""
    if not name:
        return []
    params: dict[str, str] = {"card_name": name}
    if set_name:
        params["set_name"] = set_name

    try:
        resp = await client.get(f"{OPTCG_BASE}/sets/filtered/", params=params)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if not isinstance(data, list):
            data = data.get("data") or data.get("results") or []
    except Exception as exc:
        logger.warning("[pokemon_scanner] OPTCG search failed: %s", exc)
        return []

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

    try:
        resp = await client.get(
            f"{LORCAST_BASE}/cards/search",
            params={"q": " ".join(q_parts), "unique": "prints"},
        )
        if resp.status_code != 200:
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

    try:
        search_resp = await client.get(
            f"{TCGTRACKING_BASE}/{category_id}/search",
            params={"q": set_name},
        )
        if search_resp.status_code != 200:
            return []
        sets = search_resp.json().get("sets") or []
        if not sets:
            return []

        set_id = sets[0]["id"]
        prod_resp = await client.get(f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}")
        if prod_resp.status_code != 200:
            return []
        products = prod_resp.json().get("products") or []
    except Exception as exc:
        logger.warning("[pokemon_scanner] TCGTracking product search failed: %s", exc)
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
            )
        if category_id == "71":
            return await _lorcast_search(
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


# ---------------------------------------------------------------------------
# Price enrichment (fetch from PokemonTCG if missing)
# ---------------------------------------------------------------------------

async def _enrich_price(candidate: ScoredCandidate, ptcg_key: str = "") -> None:
    """If candidate has no market_price (e.g. from TCGdex), fetch from PokemonTCG.

    Tries multiple query strategies since TCGdex and PokemonTCG use different
    set IDs (e.g. TCGdex "me02.5" vs PokemonTCG "sv5").
    """
    if candidate.market_price is not None:
        return

    if not candidate.name:
        return

    async with httpx.AsyncClient(timeout=10.0) as client:
        headers = {"X-Api-Key": ptcg_key} if ptcg_key else {}

        # Build queries in order of specificity
        queries_to_try = []

        # Try 1: name + number (skip set_id — TCGdex IDs don't match PokemonTCG)
        if candidate.number:
            clean_num = candidate.number.split("/")[0]
            queries_to_try.append(f'name:"{candidate.name}" number:"{clean_num}"')

        # Try 2: name + set name (fuzzy, via set.name)
        if candidate.set_name:
            queries_to_try.append(f'name:"{candidate.name}" set.name:"{candidate.set_name}"')

        # Try 3: name only (broadest)
        queries_to_try.append(f'name:"{candidate.name}"')

        for query in queries_to_try:
            params = {"q": query, "pageSize": "5", "orderBy": "-set.releaseDate"}
            resp = await client.get(f"{POKEMONTCG_BASE}/cards", params=params, headers=headers)
            if resp.status_code != 200:
                continue

            cards = resp.json().get("data") or []
            if not cards:
                continue

            # Pick the best card — prefer one whose number matches
            best_card = cards[0]
            if candidate.number:
                clean_num = candidate.number.split("/")[0].lstrip("0")
                for card in cards:
                    if card.get("number", "").lstrip("0") == clean_num:
                        best_card = card
                        break

            prices_wrap = best_card.get("tcgplayer", {}).get("prices", {})
            for price_type in ("normal", "holofoil", "reverseHolofoil", "1stEditionHolofoil"):
                if price_type in prices_wrap:
                    mp = prices_wrap[price_type].get("market")
                    if mp is not None:
                        try:
                            candidate.market_price = round(float(mp), 2)
                        except (ValueError, TypeError):
                            pass
                        if candidate.market_price:
                            break

            # Always grab TCGPlayer URL and images
            tcgp_url = best_card.get("tcgplayer", {}).get("url")
            if tcgp_url and not candidate.tcgplayer_url:
                candidate.tcgplayer_url = tcgp_url

            if not candidate.image_url:
                images = best_card.get("images") or {}
                candidate.image_url = images.get("large", "")
                candidate.image_url_small = images.get("small", "")

            if candidate.market_price:
                return
            # No price from this query — try next


TCGTRACKING_BASE = "https://tcgtracking.com/tcgapi/v1"
TCGTRACKING_POKEMON_CATS = ["3", "85"]  # 3 = Pokemon, 85 = Pokemon Japan

# Preferred category ordering for the frontend selector
_PREFERRED_CAT_ORDER = [
    "3",   # Pokemon
    "85",  # Pokemon Japan
    "68",  # One Piece Card Game
    "80",  # Dragon Ball Super Fusion World
    "27",  # Dragon Ball Super CCG
    "23",  # Dragon Ball Z TCG
    "1",   # Magic
    "2",   # YuGiOh
    "71",  # Lorcana TCG
]

# Cache: set_name (lowercase) -> {set_id, products, pricing}
_tcgtracking_cache: dict[str, dict] = {}

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
                return []
            raw_cats = resp.json().get("categories") or resp.json().get("data") or []
            if isinstance(resp.json(), list):
                raw_cats = resp.json()
    except Exception as exc:
        logger.error("[pokemon_scanner] Error fetching TCGTracking categories: %s", exc)
        return []

    cat_map = {}
    for cat in raw_cats:
        cat_id = str(cat.get("id", ""))
        name = cat.get("display_name") or cat.get("name") or ""
        cat_map[cat_id] = {"id": cat_id, "name": name}

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
                    _tcgtracking_cache[set_key] = cached
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
# Stage F: Visual Disambiguation (OpenAI Vision)
# ---------------------------------------------------------------------------

async def disambiguate_with_vision(
    original_b64: str,
    candidates: list[ScoredCandidate],
    openai_key: str,
) -> Optional[int]:
    """
    Use AI Vision to pick the best match among ambiguous candidates.
    Returns the index of the best match, or None on failure.
    """
    if not has_ai_key() or not candidates:
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
        client = get_ai_client(timeout=30.0)
        response = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[{"role": "user", "content": content_parts}],
            max_tokens=300,
        )
        raw = response.choices[0].message.content or "{}"
        result = _loads_ai_json(raw)
        if result is None:
            logger.warning("[pokemon_scanner] Vision disambiguation returned non-JSON: %s", raw[:200])
            return None
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
            "[pokemon_scanner] Auto-detected category %s from Ximilar (user selected %s)",
            effective_cat, category_id,
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


# ---------------------------------------------------------------------------
# Stage G: Orchestrator (dispatches to Ximilar or legacy OCR pipeline)
# ---------------------------------------------------------------------------

async def _safe_run(coro_func, *args) -> dict[str, Any] | None:
    """Run a pipeline function, returning None on any failure."""
    try:
        return await coro_func(*args)
    except Exception as exc:
        logger.error("[pokemon_scanner] Pipeline %s failed: %s", coro_func.__name__, exc)
        return None


def _candidate_key(c: dict) -> tuple:
    return (
        (c.get("name") or "").lower(),
        (c.get("number") or "").split("/")[0].lstrip("0"),
        (c.get("set_name") or "").lower(),
    )


def _merge_results(
    ximilar: dict[str, Any] | None,
    legacy: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge results from parallel Ximilar + legacy OCR pipelines.

    Strategy:
    - Both succeed, same best match  -> boost confidence to HIGH
    - Both succeed, different matches -> merge candidates, mark AMBIGUOUS
    - One fails                       -> return the other
    """
    if ximilar is None and legacy is None:
        return asdict(ScanResult(
            status="ERROR",
            error="Both scanning pipelines failed.",
            processing_time_ms=0,
        ))

    if ximilar is None:
        if legacy:
            legacy.setdefault("debug", {})["engines_used"] = ["legacy_ocr"]
        return legacy
    if legacy is None:
        ximilar.setdefault("debug", {})["engines_used"] = ["ximilar"]
        return ximilar

    # Both succeeded — compare best matches
    xbest = ximilar.get("best_match") or {}
    lbest = legacy.get("best_match") or {}

    # Handle cases where one pipeline returned NO_MATCH / ERROR
    if ximilar.get("status") in ("ERROR", "NO_MATCH") and legacy.get("status") not in ("ERROR", "NO_MATCH"):
        legacy.setdefault("debug", {})["engines_used"] = ["legacy_ocr"]
        legacy["debug"]["ximilar_status"] = ximilar.get("status")
        return legacy
    if legacy.get("status") in ("ERROR", "NO_MATCH") and ximilar.get("status") not in ("ERROR", "NO_MATCH"):
        ximilar.setdefault("debug", {})["engines_used"] = ["ximilar"]
        ximilar["debug"]["legacy_status"] = legacy.get("status")
        return ximilar

    import copy
    merged = copy.deepcopy(ximilar)
    merged.setdefault("debug", {})["engines_used"] = ["ximilar", "legacy_ocr"]
    merged["debug"]["legacy_best_match"] = {
        "name": lbest.get("name"),
        "number": lbest.get("number"),
        "set_name": lbest.get("set_name"),
        "score": lbest.get("score"),
        "confidence": lbest.get("confidence"),
    }
    merged["debug"]["legacy_processing_time_ms"] = legacy.get("processing_time_ms")

    # Compare best matches by name + collector number
    # Normalize names: strip hyphens, spaces, periods, and case for fuzzy comparison
    def _norm_name(n):
        return re.sub(r"[\s\-\.'\u2019]+", "", (n or "")).lower()

    x_name = _norm_name(xbest.get("name"))
    l_name = _norm_name(lbest.get("name"))
    x_num = (xbest.get("number") or "").split("/")[0].lstrip("0")
    l_num = (lbest.get("number") or "").split("/")[0].lstrip("0")

    same_name = x_name == l_name
    same_number = x_num and l_num and x_num == l_num
    same_card = same_name and same_number

    x_score = xbest.get("score", 0)
    x_conf = xbest.get("confidence", "LOW")
    l_score = lbest.get("score", 0)

    if same_card:
        logger.info(
            "[pokemon_scanner] Dual-engine AGREE: %s #%s (ximilar=%.0f, legacy=%.0f)",
            xbest.get("name"), xbest.get("number"), x_score, l_score,
        )
        merged["best_match"]["score"] = merged["best_match"].get("score", 0) + 15
        merged["best_match"]["confidence"] = "HIGH"
        merged["best_match"]["match_reason"] = (
            merged["best_match"].get("match_reason", "") + " (confirmed by OCR pipeline)"
        )
        if merged.get("status") == "AMBIGUOUS":
            merged["status"] = "MATCHED"
        merged["disambiguation_method"] = "dual_engine_agree"
    elif same_name and not same_number:
        # Same card name but different set/number — likely a reprint.
        logger.info(
            "[pokemon_scanner] Dual-engine PARTIAL: same name '%s', different number (x=#%s, l=#%s) xScore=%.0f lScore=%.0f",
            xbest.get("name"), xbest.get("number"), lbest.get("number"), x_score, l_score,
        )
        merged["disambiguation_method"] = "dual_engine_partial"

        if l_score >= 90 and l_score > x_score + 30:
            # OCR read the set/number with very high confidence — trust it
            logger.info(
                "[pokemon_scanner] Trusting legacy OCR for reprint disambiguation: %s #%s (%.0f) over Ximilar %s #%s (%.0f)",
                lbest.get("name"), lbest.get("number"), l_score,
                xbest.get("name"), xbest.get("number"), x_score,
            )
            import copy as _copy
            merged["best_match"] = _copy.deepcopy(lbest)
            merged["extracted_fields"] = legacy.get("extracted_fields") or merged.get("extracted_fields")
            merged["status"] = "MATCHED"
            merged["debug"]["dual_engine_note"] = (
                f"Same card name, different printings — legacy OCR strong ({l_score:.0f}) vs Ximilar ({x_score:.0f}), trusting OCR"
            )
        else:
            # Default: trust Ximilar visual match for reprint disambiguation
            merged["debug"]["dual_engine_note"] = f"Same card name, different printings — trusting Ximilar visual match"
            if merged.get("status") == "AMBIGUOUS" and x_score >= 50:
                merged["status"] = "MATCHED"
    else:
        logger.info(
            "[pokemon_scanner] Dual-engine DISAGREE: ximilar=%s #%s (%.0f) vs legacy=%s #%s (%.0f)",
            xbest.get("name"), xbest.get("number"), x_score,
            lbest.get("name"), lbest.get("number"), l_score,
        )
        merged["disambiguation_method"] = "dual_engine_disagree"

        # Trust Ximilar when it's reasonably confident and legacy is weak
        if x_conf == "HIGH" and l_score < 70:
            merged["debug"]["dual_engine_note"] = "Ximilar HIGH, legacy weak — trusting Ximilar"
        elif x_score >= 50 and l_score < 50:
            merged["debug"]["dual_engine_note"] = "Ximilar decent, legacy unreliable — trusting Ximilar"
        elif l_score >= 70 and x_score < 50:
            # Legacy OCR is strong, Ximilar is weak — swap to legacy's result
            logger.info(
                "[pokemon_scanner] Trusting legacy over weak Ximilar: %s (%.0f) > %s (%.0f)",
                lbest.get("name"), l_score, xbest.get("name"), x_score,
            )
            merged["best_match"] = lbest
            merged["extracted_fields"] = legacy.get("extracted_fields") or merged.get("extracted_fields")
            merged["status"] = "MATCHED" if l_score >= 80 else "AMBIGUOUS"
            merged["debug"]["dual_engine_note"] = (
                f"Legacy strong ({l_score:.0f}), Ximilar weak ({x_score:.0f}) — trusting OCR"
            )
        else:
            merged["status"] = "AMBIGUOUS"

    # Merge candidate lists (dedup by name+number+set)
    existing_keys = set()
    merged_candidates = []
    for c in (merged.get("candidates") or []):
        key = _candidate_key(c)
        if key not in existing_keys:
            existing_keys.add(key)
            merged_candidates.append(c)

    for c in (legacy.get("candidates") or []):
        key = _candidate_key(c)
        if key not in existing_keys:
            existing_keys.add(key)
            c_copy = dict(c)
            c_copy["source"] = c_copy.get("source", "legacy_ocr")
            merged_candidates.append(c_copy)

    merged_candidates.sort(key=lambda c: -(c.get("score") or 0))
    merged["candidates"] = merged_candidates[:12]

    # Use the slower pipeline's total time as the overall time
    merged["processing_time_ms"] = max(
        ximilar.get("processing_time_ms", 0),
        legacy.get("processing_time_ms", 0),
    )

    _save_to_history(merged)
    return merged


def _ximilar_confidence(result: dict) -> float:
    """Extract a 0-1 confidence value from a Ximilar pipeline result."""
    best = result.get("best_match") or {}
    return best.get("score", 0) / 100.0


async def _background_ocr_validate(
    scan_id: str,
    image_b64: str,
    ximilar_result: dict,
    category_id: str,
) -> None:
    """Run legacy OCR pipeline in background and store merged result for polling."""
    try:
        legacy_result = await _run_legacy_pipeline(image_b64, category_id)
        merged = _merge_results(ximilar_result, legacy_result)
        merged["scan_id"] = scan_id
        merged["validation_status"] = "validated"

        _save_to_history(merged)
        _pending_validations[scan_id] = (time.monotonic(), merged)

        logger.info(
            "[pokemon_scanner] Background OCR validation complete for scan_id=%s: status=%s",
            scan_id, merged.get("status"),
        )
    except Exception as exc:
        logger.error("[pokemon_scanner] Background OCR validation failed for scan_id=%s: %s", scan_id, exc)
        _pending_validations[scan_id] = (time.monotonic(), {"validation_status": "error", "error": str(exc)})


_TEXT_SEARCH_PARSE_PROMPT = """You are a TCG card search query parser. The user will give you a free-text search query for a trading card game card. Extract structured fields from it.

Return JSON with:
- "card_name": the card/character name (e.g. "Charizard", "Pikachu VMAX")
- "set_name": the set name if mentioned (e.g. "Base Set", "Evolving Skies"), or null
- "collector_number": the collector number if mentioned (e.g. "4/102", "25"), or null

Only extract what is explicitly stated. Do not guess or infer missing fields.
Respond with ONLY valid JSON. No markdown fences, no explanation."""


def _parse_search_query(query: str) -> ExtractedFields:
    """Parse a free-text card search query into structured fields.

    Uses AI when available, falls back to simple heuristic parsing.
    """
    fields = ExtractedFields()

    if has_ai_key():
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
                temperature=0.0,
            )
            raw = response.choices[0].message.content or ""
            data = _loads_ai_json(raw)
            if data is None:
                logger.warning("[pokemon_scanner] Text search parse returned non-JSON: %s", raw[:200])
                raise ValueError("non-JSON AI response")
            ai_name = data.get("card_name") or None
            ai_set = data.get("set_name") or None
            ai_number = data.get("collector_number") or None
            if ai_name or ai_number:
                fields.card_name = ai_name
                fields.set_name = ai_set
                fields.collector_number = ai_number
                fields.extraction_method = "ai"
                logger.info(
                    "[pokemon_scanner] Text search parsed: name=%s, set=%s, number=%s",
                    fields.card_name, fields.set_name, fields.collector_number,
                )
                return fields
            logger.warning("[pokemon_scanner] AI returned empty fields for query '%s', falling back to heuristic", query)
        except Exception as exc:
            logger.warning("[pokemon_scanner] AI query parse failed, using heuristic: %s", exc)

    # Heuristic fallback: look for number patterns, treat rest as card name
    number_match = re.search(r"(\d{1,4})\s*/\s*(\d{1,4})", query)
    if number_match:
        fields.collector_number = number_match.group(0)
        query = query[:number_match.start()] + query[number_match.end():]

    fields.card_name = query.strip() or None
    fields.extraction_method = "heuristic"
    return fields


async def text_search_cards(query: str, category_id: str = "3") -> dict[str, Any]:
    """Search for cards by text query. Returns same shape as scan pipeline."""
    t_start = time.monotonic()

    game = _game_for_category(category_id)

    if not query or not query.strip():
        return {**asdict(ScanResult(status="ERROR", error="Empty search query")), "game": game}

    fields = _parse_search_query(query.strip())
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


async def run_pipeline(image_b64: str, category_id: str = "3") -> dict[str, Any]:
    """
    Confidence-tiered card scanning pipeline.

    When both Ximilar and Google Vision API keys are configured:
      - Ximilar confidence >= 0.85: accept immediately, skip OCR
      - Ximilar confidence 0.60-0.84: return Ximilar optimistically,
        trigger OCR validation in background (poll via scan_id)
      - Ximilar confidence < 0.60: wait for OCR, merge, return

    Falls back to whichever single pipeline is available.
    category_id controls price enrichment (which TCGTracking category to search).
    """
    settings = get_settings()
    has_ximilar = bool(settings.ximilar_api_token)
    has_ocr = bool(settings.google_vision_api_key)

    def _stamp_game(result: dict) -> dict:
        """Ensure every orchestrator return path has a canonical ``game`` field.

        Uses ``debug.effective_category_id`` when Ximilar auto-detected a
        different category, else the caller's ``category_id``.
        """
        if result.get("game"):
            return result
        effective = (result.get("debug") or {}).get("effective_category_id") or category_id
        result["game"] = _game_for_category(effective)
        return result

    # No Ximilar → legacy only
    if not has_ximilar:
        return _stamp_game(await _run_legacy_pipeline(image_b64, category_id))

    # Run Ximilar first (fast: 2-4s)
    try:
        ximilar_result = await _run_ximilar_pipeline(image_b64, settings.ximilar_api_token, category_id)
    except Exception as exc:
        logger.error("[pokemon_scanner] Ximilar pipeline failed: %s", exc)
        if has_ocr:
            return _stamp_game(await _run_legacy_pipeline(image_b64, category_id))
        return _stamp_game(asdict(ScanResult(
            status="ERROR",
            error=f"Ximilar pipeline failed: {exc}",
            processing_time_ms=0,
        )))

    # No OCR available → return Ximilar regardless of confidence
    if not has_ocr:
        return _stamp_game(ximilar_result)

    confidence = _ximilar_confidence(ximilar_result)
    x_status = ximilar_result.get("status", "ERROR")
    scan_id = str(uuid.uuid4())

    if confidence >= XIMILAR_CONFIDENCE_HIGH and x_status not in ("ERROR", "NO_MATCH"):
        # HIGH confidence — accept immediately, no OCR
        logger.info(
            "[pokemon_scanner] HIGH confidence (%.2f) — accepting Ximilar, skipping OCR, category=%s",
            confidence, category_id,
        )
        ximilar_result["scan_id"] = scan_id
        ximilar_result.setdefault("debug", {})["pipeline_tier"] = "high_confidence"
        ximilar_result.setdefault("debug", {})["engines_used"] = ["ximilar"]
        return _stamp_game(ximilar_result)

    elif confidence >= XIMILAR_CONFIDENCE_MEDIUM and x_status not in ("ERROR", "NO_MATCH"):
        # MEDIUM confidence — return optimistically, fire background OCR
        logger.info(
            "[pokemon_scanner] MEDIUM confidence (%.2f) — optimistic return + background OCR, category=%s",
            confidence, category_id,
        )
        ximilar_result["scan_id"] = scan_id
        ximilar_result["validation_pending"] = True
        ximilar_result.setdefault("debug", {})["pipeline_tier"] = "medium_confidence"
        ximilar_result.setdefault("debug", {})["engines_used"] = ["ximilar"]

        _pending_validations[scan_id] = (time.monotonic(), None)  # sentinel: OCR in progress

        import copy
        ximilar_copy = copy.deepcopy(ximilar_result)
        asyncio.create_task(_background_ocr_validate(
            scan_id, image_b64, ximilar_copy, category_id,
        ))

        return _stamp_game(ximilar_result)

    else:
        # LOW confidence — wait for OCR, merge, return
        logger.info(
            "[pokemon_scanner] LOW confidence (%.2f) — waiting for OCR pipeline, category=%s",
            confidence, category_id,
        )
        legacy_result = await _safe_run(_run_legacy_pipeline, image_b64, category_id)
        merged = _merge_results(ximilar_result, legacy_result)
        merged["scan_id"] = scan_id
        merged.setdefault("debug", {})["pipeline_tier"] = "low_confidence"
        return _stamp_game(merged)


def get_validation_result(scan_id: str) -> dict | None:
    """Check if a background OCR validation has completed.

    Returns:
      - None if scan_id is unknown
      - {"validation_status": "pending"} if OCR is still running
      - The full merged result dict if validation is complete
    """
    _cleanup_stale_validations()
    if scan_id not in _pending_validations:
        return None
    ts, result = _pending_validations[scan_id]
    if result is None:
        return {"validation_status": "pending", "scan_id": scan_id}
    _pending_validations.pop(scan_id, None)
    return result


def _cleanup_stale_validations() -> None:
    """Remove expired entries to prevent unbounded memory growth."""
    now = time.monotonic()
    stale = [k for k, (ts, _) in _pending_validations.items() if now - ts > _VALIDATION_TTL]
    for k in stale:
        _pending_validations.pop(k, None)


async def _run_legacy_pipeline(image_b64: str, category_id: str = "3") -> dict[str, Any]:
    """
    Legacy 7-stage pipeline: OCR -> Lookup -> Score -> Disambiguate.
    Used when Ximilar API token is not configured.
    """
    settings = get_settings()
    t_start = time.monotonic()
    debug_info: dict[str, Any] = {
        "engine": "legacy_ocr",
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
            error="No scanning API configured. Set XIMILAR_API_TOKEN or GOOGLE_VISION_API_KEY.",
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
    debug_info["extraction_method"] = fields.extraction_method
    debug_info["stage_times_ms"]["ocr"] = _elapsed(t_stage)

    # Fallback: if preprocessed image yielded nothing, try the raw image
    if not fields.collector_number and not fields.card_name:
        logger.info("[pokemon_scanner] Preprocessed OCR empty, retrying with raw image")
        t_stage = time.monotonic()
        fields = await run_ocr(raw_bytes, settings.google_vision_api_key)
        debug_info["ocr_raw_text"] = fields.ocr_raw_text
        debug_info["ocr_confidence"] = fields.ocr_confidence
        debug_info["extraction_method"] = fields.extraction_method
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
    is_pokemon_cat = category_id in ("3", "85")
    for sc in scored[:5]:
        try:
            if is_pokemon_cat:
                await _enrich_price(sc, settings.pokemon_tcg_api_key)
            else:
                await _enrich_price_fast(sc, settings.pokemon_tcg_api_key, category_id=category_id)
        except Exception as exc:
            logger.debug("[pokemon_scanner] Price enrichment failed for %s: %s", sc.name, exc)
    debug_info["stage_times_ms"]["price_enrich"] = _elapsed(t_stage)

    # --- Stage F: Disambiguation (if needed) ---
    disambiguation_method: Optional[str] = None
    top = scored[0]

    if len(scored) >= 2 and (scored[0].score - scored[1].score) < DISAMBIGUATION_THRESHOLD:
        ambiguous_set = [s for s in scored if (scored[0].score - s.score) < DISAMBIGUATION_THRESHOLD][:3]

        if has_ai_key():
            t_stage = time.monotonic()
            best_idx = await disambiguate_with_vision(
                image_b64, ambiguous_set, "",
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
