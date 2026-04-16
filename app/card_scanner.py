"""
AI-powered card identification using OpenAI Vision.

Takes a base64-encoded card image and returns structured card info:
- game, card_name, set_name, card_number, condition, is_foil
- stock image URL (Scryfall for MTG, PokemonTCG for Pokemon)
- market price (from Scrydex/Scryfall)
- ai_confidence score

Also handles image lookup from Scryfall (MTG) and Pokemon TCG API (Pokemon).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

import httpx
from .ai_client import get_ai_client, get_model, has_ai_key
from .config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

VISION_MODEL = get_model(default="gpt-5-nano")

SCRYFALL_NAMED_URL = "https://api.scryfall.com/cards/named"
SCRYFALL_SEARCH_URL = "https://api.scryfall.com/cards/search"
POKEMON_TCG_SEARCH_URL = "https://api.pokemontcg.io/v2/cards"

IDENTIFY_PROMPT = """You are an expert trading card grader and identifier.
Analyze this trading card image and return a JSON object with these exact keys:

{
  "game": "Pokemon" or "MTG" or "Sports" or "Other",
  "card_name": "exact card name as printed",
  "set_name": "set or expansion name if visible, else null",
  "card_number": "card number if visible e.g. '4/102' or '025/165', else null",
  "set_code": "short set code if you know it e.g. 'base1' or 'OTJ', else null",
  "language": "English" or language name if non-English,
  "condition": "NM" or "LP" or "MP" or "HP" or "DMG",
  "is_foil": true or false,
  "confidence": 0.0 to 1.0,
  "notes": "any relevant notes or if card cannot be identified clearly"
}

Condition guide:
- NM (Near Mint): virtually no wear
- LP (Lightly Played): minor edge/corner wear
- MP (Moderately Played): visible wear, some creases
- HP (Heavily Played): heavy wear, major creases
- DMG (Damaged): tears, heavy creases, writing

Be precise about card_name. If you cannot read the card clearly, set confidence below 0.5 and explain in notes.
Always return valid JSON only — no other text."""


async def identify_card_from_image(base64_image: str) -> dict[str, Any]:
    """
    Send a base64-encoded card image to AI Vision and return structured card info.

    Returns a dict with: game, card_name, set_name, card_number, set_code,
    language, condition, is_foil, confidence, notes, error (if failed).
    """
    if not has_ai_key():
        return {"error": "AI API key not configured", "confidence": 0.0}

    client = get_ai_client(timeout=30.0)
    try:
        response = client.chat.completions.create(
            model=VISION_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}",
                                "detail": "high",
                            },
                        },
                        {"type": "text", "text": IDENTIFY_PROMPT},
                    ],
                }
            ],
            response_format={"type": "json_object"},
            max_tokens=500,
        )
        raw = response.choices[0].message.content or "{}"
        data = json.loads(raw)
        return data
    except json.JSONDecodeError as exc:
        logger.warning("[card_scanner] JSON decode error: %s", exc)
        return {"error": "Invalid JSON from vision model", "confidence": 0.0}
    except Exception as exc:
        logger.error("[card_scanner] Vision API error: %s", exc)
        return {"error": str(exc), "confidence": 0.0}


async def lookup_card_image_and_price(
    card_name: str,
    *,
    game: str = "",
    set_code: Optional[str] = None,
    card_number: Optional[str] = None,
    condition: str = "NM",
    pokemon_tcg_api_key: str = "",
) -> dict[str, Any]:
    """
    Look up stock image URL and market price for an identified card.

    Returns: {image_url, market_price, source_url, set_name, card_number}
    """
    game_lower = (game or "").lower()

    async with httpx.AsyncClient(timeout=10.0) as client:
        if "mtg" in game_lower or "magic" in game_lower:
            return await _lookup_scryfall(client, card_name, set_code=set_code)
        if "pokemon" in game_lower or "pok" in game_lower:
            return await _lookup_pokemon_tcg(
                client, card_name, set_code=set_code, card_number=card_number,
                api_key=pokemon_tcg_api_key,
            )
        # Unknown game — try Scryfall first, then Pokemon
        result = await _lookup_scryfall(client, card_name, set_code=set_code)
        if result.get("image_url"):
            return result
        return await _lookup_pokemon_tcg(client, card_name, set_code=set_code, api_key=pokemon_tcg_api_key)


async def _lookup_scryfall(
    client: httpx.AsyncClient,
    card_name: str,
    *,
    set_code: Optional[str] = None,
) -> dict[str, Any]:
    params: dict[str, str] = {"exact": card_name}
    if set_code:
        params["set"] = set_code

    try:
        resp = await client.get(SCRYFALL_NAMED_URL, params=params)
        if resp.status_code == 404 and set_code:
            # Retry without set constraint
            resp = await client.get(SCRYFALL_NAMED_URL, params={"exact": card_name})
        if resp.status_code == 404:
            # Try fuzzy search
            resp = await client.get(SCRYFALL_NAMED_URL, params={"fuzzy": card_name})
        if resp.status_code != 200:
            return {}

        data = resp.json()
        images = data.get("image_uris") or {}
        # Prefer normal size; card_faces has images for double-faced cards
        image_url = images.get("normal") or images.get("small")
        if not image_url:
            faces = data.get("card_faces") or []
            if faces:
                face_images = (faces[0].get("image_uris") or {})
                image_url = face_images.get("normal") or face_images.get("small")

        prices = data.get("prices") or {}
        market_price = _safe_float(prices.get("usd"))

        return {
            "image_url": image_url,
            "market_price": market_price,
            "set_name": data.get("set_name"),
            "card_number": data.get("collector_number"),
            "source": "scryfall",
        }
    except Exception as exc:
        logger.debug("[card_scanner] Scryfall lookup failed for %s: %s", card_name, exc)
        return {}


async def _lookup_pokemon_tcg(
    client: httpx.AsyncClient,
    card_name: str,
    *,
    set_code: Optional[str] = None,
    card_number: Optional[str] = None,
    api_key: str = "",
) -> dict[str, Any]:
    headers = {}
    if api_key:
        headers["X-Api-Key"] = api_key

    # Build query
    q_parts = [f'name:"{card_name}"']
    if set_code:
        q_parts.append(f"set.id:{set_code}")
    if card_number:
        q_parts.append(f'number:"{card_number}"')

    params = {"q": " ".join(q_parts), "pageSize": "1"}

    try:
        resp = await client.get(POKEMON_TCG_SEARCH_URL, params=params, headers=headers)
        if resp.status_code != 200:
            return {}

        data = resp.json()
        cards = data.get("data") or []
        if not cards:
            # Retry with just the name (no set/number)
            params = {"q": f'name:"{card_name}"', "pageSize": "1"}
            resp = await client.get(POKEMON_TCG_SEARCH_URL, params=params, headers=headers)
            data = resp.json()
            cards = data.get("data") or []

        if not cards:
            return {}

        card = cards[0]
        images = card.get("images") or {}
        image_url = images.get("large") or images.get("small")

        prices_wrap = card.get("tcgplayer", {}).get("prices", {})
        market_price = None
        for price_type in ("normal", "holofoil", "reverseHolofoil"):
            if price_type in prices_wrap:
                market_price = _safe_float(prices_wrap[price_type].get("market"))
                if market_price:
                    break

        card_set = card.get("set") or {}
        return {
            "image_url": image_url,
            "market_price": market_price,
            "set_name": card_set.get("name"),
            "card_number": card.get("number"),
            "source": "pokemontcg",
        }
    except Exception as exc:
        logger.debug("[card_scanner] Pokemon TCG lookup failed for %s: %s", card_name, exc)
        return {}


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
        return round(f, 2) if f > 0 else None
    except (TypeError, ValueError):
        return None
