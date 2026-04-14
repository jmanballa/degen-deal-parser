"""
Pokemon card scanning pipeline.

Multi-stage pipeline: Capture -> Preprocess (Pillow) -> OCR (Google Vision)
-> Lookup (TCGdex + PokemonTCG) -> Score -> Disambiguate (OpenAI Vision) -> Result.

Designed for Pokemon cards only. Identification works backward from the
collector number, which uniquely identifies a card within a set.
"""
from __future__ import annotations

import base64
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
    re.compile(r"(\d{1,4})\s*/\s*(\d{1,4})"),          # 123/197
    re.compile(r"(TG\d{1,3})\s*/\s*(TG\d{1,3})", re.I), # TG12/TG30
    re.compile(r"(SVP\s*\d{2,3})", re.I),                # SVP001
    re.compile(r"(GG\d{1,3})\s*/?\s*(GG\d{1,3})?", re.I),# GG21/GG70
    re.compile(r"(SWSH\d{2,3})", re.I),                  # SWSH promo
    re.compile(r"(SV\d{2,3})", re.I),                    # SV promo
]

VARIANT_KEYWORDS = [
    "VMAX", "VSTAR", "V", "EX", "GX", "ex", "FULL ART", "TRAINER GALLERY",
    "ALT ART", "ILLUSTRATION RARE", "SPECIAL ART RARE", "REVERSE HOLO",
    "SECRET RARE", "ULTRA RARE", "RAINBOW RARE", "GOLD",
]

_tcgdex_sets_cache: list[dict] | None = None


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
    text = text.replace("O", "0").replace("o", "0")
    text = text.replace("l", "1").replace("I", "1")
    text = text.replace("\\", "/").replace("|", "/")
    text = text.replace(" ", "")
    return text


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

    # --- HP value (extract early — helps identify the name line) ---
    hp_match = re.search(r"(\d{2,3})\s*HP", raw_text, re.I)
    if hp_match:
        fields.hp_value = hp_match.group(1)

    # --- Card name: find the best candidate from top lines ---
    # Pokemon card names are near top, usually the longest meaningful text
    # that isn't HP, a number, or game mechanic text
    _SKIP_WORDS = {"BASIC", "STAGE", "WEAKNESS", "RESISTANCE", "RETREAT", "TRAINER",
                   "SUPPORTER", "ITEM", "TOOL", "STADIUM", "ENERGY", "POKEMON",
                   "POKÉMON", "ILLUSTRATOR", "REGULATION"}
    for line in lines[:8]:
        cleaned = re.sub(r"[^\w\s\-\'é.':]", "", line).strip()
        cleaned = re.sub(r"\b\d{2,3}\s*HP\b", "", cleaned, flags=re.I).strip()
        if len(cleaned) < 2:
            continue
        upper_words = set(cleaned.upper().split())
        if upper_words & _SKIP_WORDS:
            continue
        # Skip lines that are just numbers
        if re.match(r"^[\d\s/\-]+$", cleaned):
            continue
        # Skip very short single-char lines
        if len(cleaned) <= 2 and not cleaned[0].isalpha():
            continue
        fields.card_name = cleaned
        break

    # --- Set name: look for known patterns near bottom of card ---
    # Pokemon sets often appear on bottom lines, sometimes with copyright
    for line in reversed(lines):
        line_clean = line.strip()
        # Skip short, number-only, or game-mechanic lines
        if len(line_clean) < 3:
            continue
        line_upper = line_clean.upper()
        if any(kw in line_upper for kw in ["WEAKNESS", "RESISTANCE", "RETREAT", "HP",
                                            "DAMAGE", "ATTACK", "ILLUSTRATOR", "©",
                                            "NINTENDO", "CREATURES", "GAME FREAK"]):
            continue
        if fields.collector_number_raw and fields.collector_number_raw in line_clean:
            continue
        # Skip lines that are just numbers or very long (probably ability text)
        if re.match(r"^[\d\s/\-]+$", line_clean) or len(line_clean) > 50:
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
    """Try to match extracted set name to a TCGdex set ID."""
    if not set_name:
        return None
    name_lower = set_name.lower().strip()
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
    client: httpx.AsyncClient, name: str, limit: int = 10
) -> list[dict]:
    """Search TCGdex by card name."""
    resp = await client.get(f"{TCGDEX_BASE}/cards", params={"name": name})
    if resp.status_code == 200:
        results = resp.json()
        if isinstance(results, list):
            return results[:limit]
    return []


def _tcgdex_to_candidate(card: dict, source: str = "tcgdex") -> CandidateCard:
    """Convert a TCGdex card object to CandidateCard."""
    image = card.get("image", "")
    image_url = f"{image}/high.webp" if image else ""
    image_url_small = f"{image}/low.webp" if image else ""

    set_info = card.get("set") or {}

    return CandidateCard(
        id=card.get("id", ""),
        name=card.get("name", ""),
        number=card.get("localId", "") or card.get("number", ""),
        set_id=set_info.get("id", "") if isinstance(set_info, dict) else str(set_info),
        set_name=set_info.get("name", "") if isinstance(set_info, dict) else "",
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

        # Tier 2: PokemonTCG by number (across all sets)
        if not candidates and fields.collector_number:
            tier_reached = 2
            candidates = await _pokemontcg_search(
                client, number=fields.collector_number, api_key=ptcg_key,
            )

        # Tier 3: Name + number
        if not candidates and fields.card_name and fields.collector_number:
            tier_reached = 3
            candidates = await _pokemontcg_search(
                client,
                name=fields.card_name,
                number=fields.collector_number,
                api_key=ptcg_key,
            )

        # Tier 4: Name only
        if not candidates and fields.card_name:
            tier_reached = 4
            candidates = await _pokemontcg_search(
                client, name=fields.card_name, api_key=ptcg_key, limit=10,
            )

        # Tier 5: No results
        if not candidates:
            tier_reached = 5

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

        # Exact full number match (e.g. "123/197" == "123/197")
        if fields.collector_number and c.number:
            extracted_num = fields.collector_number.strip()
            candidate_num = c.number.strip()
            if extracted_num == candidate_num:
                breakdown["exact_full_number"] = SCORING_WEIGHTS["exact_full_number"]
            elif extracted_num.split("/")[0] == candidate_num.split("/")[0]:
                breakdown["exact_collector_number"] = SCORING_WEIGHTS["exact_collector_number"]

        # Fuzzy name similarity (Levenshtein normalized to 0-25)
        if fields.card_name and c.name:
            dist = _levenshtein(fields.card_name.lower(), c.name.lower())
            max_len = max(len(fields.card_name), len(c.name), 1)
            similarity = 1.0 - (dist / max_len)
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

    # --- Validate config ---
    if not settings.google_vision_api_key:
        return asdict(ScanResult(
            status="ERROR",
            error="Google Vision API key not configured (GOOGLE_VISION_API_KEY)",
            processing_time_ms=_elapsed(t_start),
        ))

    # --- Stage B: Preprocess ---
    t_stage = time.monotonic()
    try:
        raw_bytes = base64.b64decode(image_b64)
    except Exception:
        return asdict(ScanResult(
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
        return asdict(ScanResult(
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
        return asdict(ScanResult(
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

    return asdict(result)


def _elapsed(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 1)
