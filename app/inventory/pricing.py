"""
Auto-pricing adapters for inventory items.

Singles (MTG / Pokemon): Scrydex subscription API.
  - Configure SCRYDEX_API_KEY and SCRYDEX_BASE_URL in .env
  - If Scrydex uses a Scryfall-compatible endpoint, set SCRYDEX_BASE_URL to
    https://api.scryfall.com and leave SCRYDEX_API_KEY empty.

Slabs (PSA / BGS / CGC): Card Ladder last-solds first, then 130point / Alt fallback.
  - These services do not have official public APIs; requests target their
    public search/data endpoints. Update the URL constants below when their
    endpoints change.

Usage:
    from .pricing import fetch_price_for_item
    result = await fetch_price_for_item(item, client)
    # result is None on failure, or:
    # {"source": "scrydex", "market_price": 42.0, "low_price": 38.0, "high_price": 50.0, "raw": {...}}
"""
from __future__ import annotations

import json
import logging
import re
import html as html_lib
import asyncio
import base64
import csv
import io
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlencode

import httpx

from ..models import InventoryItem, ITEM_TYPE_SEALED, ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB, utcnow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 130point search endpoint (graded cards)
# Example: https://www.130point.com/sales/search?q=Charizard+PSA+10&output=json
# ---------------------------------------------------------------------------
POINT130_SEARCH_URL = "https://www.130point.com/sales/search"

# ---------------------------------------------------------------------------
# ALT sold-listing search (graded cards marketplace)
# scripts/alt_cli.py fetches ALT's current web-app search config, then queries
# the sold-listing Typesense collection.
# ---------------------------------------------------------------------------
ALT_BROWSE_URL = "https://alt.xyz/browse"
PRICECHARTING_GAME_URL = "https://www.pricecharting.com/game"
MYSLABS_ARCHIVE_SEARCH_URL = "https://myslabs.com/search/archive/"
XIMILAR_TCG_IDENTIFY_URL = "https://api.ximilar.com/collectibles/v2/tcg_id"
TCGTRACKING_BASE = "https://tcgtracking.com/tcgapi/v1"
TCGTRACKING_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://tcgtracking.com/",
}
TCGTRACKING_GAME_CATEGORY_IDS: dict[str, tuple[str, ...]] = {
    "pokemon": ("3", "85"),
    "pokemon jp": ("85",),
    "magic": ("1",),
    "mtg": ("1",),
    "yu-gi-oh": ("2",),
    "yugioh": ("2",),
    "one piece": ("68",),
    "lorcana": ("71",),
    "riftbound": ("89",),
    "dragon ball": ("80", "27", "23"),
    "digimon": ("63",),
    "flesh and blood": ("62",),
    "weiss schwarz": ("20",),
    "cardfight vanguard": ("16",),
    "union arena": ("81",),
}

# ---------------------------------------------------------------------------
# Card Ladder (price history for graded cards)
# Example: https://www.cardladder.com/api/search?q=Charizard+PSA+10
# ---------------------------------------------------------------------------
CARD_LADDER_SEARCH_URL = "https://www.cardladder.com/api/search"
CARD_LADDER_APP_SALES_HISTORY_URL = "https://app.cardladder.com/sales-history"
CARD_LADDER_CARDS_SEARCH_URL = "https://www.cardladder.com/cards/search"
OTHER_GRADERS = ("PSA", "BGS", "CGC", "SGC", "BECKETT")
CARD_LADDER_NOISE_EXCLUSIONS = (
    "Autograph",
    "Auto",
    "Signed",
    "Lot",
    "Reprint",
    "Proxy",
    "Custom",
    "Checklist",
)
STALE_SLAB_COMP_DAYS = 30
SLAB_PRICE_SOURCE_OPTIONS = ("all", "alt", "pricecharting", "myslabs", "card_ladder", "130point")
SLAB_PRICE_SOURCE_ALIASES = {
    "": "all",
    "all_sources": "all",
    "all sources": "all",
    "slab_comps": "all",
    "slab comps": "all",
    "price_charting": "pricecharting",
    "price charting": "pricecharting",
    "pc": "pricecharting",
    "cardladder": "card_ladder",
    "card ladder": "card_ladder",
    "cl": "card_ladder",
    "point130": "130point",
    "130 point": "130point",
    "my_slabs": "myslabs",
    "my slabs": "myslabs",
}


# ---------------------------------------------------------------------------
# Scryfall fallback (free MTG API — used when SCRYDEX_BASE_URL is unset)
# ---------------------------------------------------------------------------
SCRYFALL_NAMED_URL = "https://api.scryfall.com/cards/named"


def effective_price(item: InventoryItem) -> Optional[float]:
    """Return the price to use: list_price overrides auto_price."""
    if item.list_price is not None:
        return round(item.list_price, 2)
    if item.auto_price is not None:
        return round(item.auto_price, 2)
    return None


def _tcgtracking_category_ids_for_game(game: str | None) -> tuple[str, ...]:
    clean = re.sub(r"\s+", " ", str(game or "").strip().lower())
    return TCGTRACKING_GAME_CATEGORY_IDS.get(clean, ())


def _money_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number <= 0:
        return None
    return round(number, 2)


def _price_from_tcgtracking_variant(
    variant: dict[str, Any],
    *,
    condition: str = "NM",
) -> tuple[Optional[float], Optional[float]]:
    condition_key = re.sub(r"[^A-Z0-9]", "", str(condition or "NM").upper()) or "NM"
    conditions = variant.get("conditions") or {}
    if isinstance(conditions, dict):
        raw_condition = conditions.get(condition_key)
        if isinstance(raw_condition, dict):
            market = _money_float(
                raw_condition.get("mkt")
                or raw_condition.get("market")
                or raw_condition.get("market_price")
                or raw_condition.get("price")
            )
            low = _money_float(raw_condition.get("low") or raw_condition.get("low_price"))
            if market is not None or low is not None:
                return market, low
    return _money_float(variant.get("price") or variant.get("market_price")), _money_float(
        variant.get("low_price") or variant.get("low")
    )


def _choose_tcgtracking_variant(
    variants: list[dict[str, Any]],
    *,
    preferred_variant: str = "",
) -> dict[str, Any]:
    if not variants:
        return {}
    preferred_key = re.sub(r"[^a-z0-9]", "", preferred_variant.lower())
    if preferred_key:
        for variant in variants:
            if re.sub(r"[^a-z0-9]", "", str(variant.get("name") or "").lower()) == preferred_key:
                return variant
    for variant in variants:
        if str(variant.get("name") or "").strip().lower() == "normal":
            return variant
    return variants[0]


async def _fetch_tcgtracking_single_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    category_ids = _tcgtracking_category_ids_for_game(item.game)
    if not category_ids:
        return None
    query_parts = [
        item.card_name,
        item.set_name or "",
        item.card_number or "",
    ]
    query = " ".join(part for part in query_parts if str(part or "").strip()).strip()
    if not query:
        return None

    try:
        from .pokemon_scanner import text_search_cards
    except Exception as exc:
        logger.warning("[inventory_pricing] TCGTracking single import failed: %s", exc)
        return None

    for category_id in category_ids:
        try:
            result = await text_search_cards(
                query,
                category_id=category_id,
                use_ai_parse=False,
                max_results=6,
                include_pokemontcg_supplement=False,
                allow_cross_category_pricing=False,
                allow_pokemontcg_price_fallback=False,
            )
        except Exception as exc:
            logger.warning(
                "[inventory_pricing] TCGTracking single lookup failed for item %s category=%s: %s",
                item.id,
                category_id,
                exc,
            )
            continue
        candidates = []
        if isinstance(result.get("best_match"), dict):
            candidates.append(result["best_match"])
        candidates.extend([row for row in (result.get("candidates") or []) if isinstance(row, dict)])
        for candidate in candidates:
            variants = [row for row in (candidate.get("available_variants") or []) if isinstance(row, dict)]
            selected_variant = _choose_tcgtracking_variant(variants, preferred_variant=item.variant or "")
            market, low = _price_from_tcgtracking_variant(selected_variant, condition=item.condition or "NM")
            if market is None:
                market = _money_float(candidate.get("market_price"))
            if market is None and low is None:
                continue
            return {
                "source": "tcgtracking",
                "market_price": market,
                "low_price": low,
                "high_price": None,
                "raw": {
                    "source_detail": "tcgtracking_single_text_search",
                    "query": query,
                    "category_id": category_id,
                    "condition": item.condition or "NM",
                    "variant": selected_variant.get("name") or item.variant or "",
                    "match": candidate,
                },
            }
    return None


_SEALED_SEARCH_NOISE = {
    "the",
    "and",
    "card",
    "cards",
    "tcg",
    "trading",
    "game",
    "pokemon",
    "magic",
    "mtg",
    "yugioh",
    "sealed",
    "product",
}
_SEALED_KIND_TOKENS = {
    "box",
    "booster",
    "bundle",
    "case",
    "collection",
    "deck",
    "display",
    "elite",
    "etb",
    "pack",
    "packs",
    "premium",
    "starter",
    "tin",
    "trainer",
    "trove",
    "upc",
}


def _sealed_norm(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _sealed_query_tokens(value: Any) -> list[str]:
    return [
        token
        for token in _sealed_norm(value).split()
        if len(token) >= 2 and token not in _SEALED_SEARCH_NOISE
    ]


def _sealed_set_queries(item: InventoryItem) -> list[str]:
    candidates = [item.set_name or ""]
    stripped_name = " ".join(
        token
        for token in _sealed_query_tokens(item.card_name)
        if token not in _SEALED_KIND_TOKENS
    )
    candidates.extend([stripped_name, item.card_name or ""])
    out: list[str] = []
    for candidate in candidates:
        clean = re.sub(r"\s+", " ", str(candidate or "")).strip()
        if clean and clean.lower() not in {row.lower() for row in out}:
            out.append(clean)
    return out


def _sealed_product_score(item: InventoryItem, product: dict[str, Any], set_info: dict[str, Any]) -> int:
    product_name = str(product.get("clean_name") or product.get("name") or "")
    product_norm = _sealed_norm(product_name)
    set_norm = _sealed_norm(set_info.get("name"))
    item_norm = _sealed_norm(item.card_name)
    tokens = _sealed_query_tokens(item.card_name)
    specific_tokens = [token for token in tokens if token not in _SEALED_KIND_TOKENS]
    score = 0
    if item_norm and product_norm == item_norm:
        score += 120
    elif item_norm and product_norm.startswith(item_norm):
        score += 70
    for token in tokens:
        if token in product_norm:
            score += 8
        elif token in set_norm:
            score += 3
    if specific_tokens and all(token in product_norm or token in set_norm for token in specific_tokens):
        score += 35
    if item.set_name and _sealed_norm(item.set_name) == set_norm:
        score += 20
    return score


def _tcgtracking_sealed_price_from_row(
    product_id: str,
    pricing: dict[str, Any] | None,
) -> tuple[Optional[float], Optional[float], str]:
    product_prices = (pricing or {}).get(str(product_id), {}).get("tcg", {})
    if not isinstance(product_prices, dict):
        return None, None, ""
    fallback_low: Optional[float] = None
    for _variant_name, variant_prices in product_prices.items():
        if not isinstance(variant_prices, dict):
            continue
        market = _money_float(variant_prices.get("market"))
        low = _money_float(variant_prices.get("low"))
        if market is not None:
            return market, low, "TCGPlayer Market"
        if fallback_low is None and low is not None:
            fallback_low = low
    if fallback_low is not None:
        return fallback_low, fallback_low, "TCGPlayer Low"
    return None, None, ""


async def _fetch_tcgtracking_sealed_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    category_ids = _tcgtracking_category_ids_for_game(item.game)
    if not category_ids:
        return None
    best: tuple[int, dict[str, Any], dict[str, Any], str, dict[str, Any]] | None = None

    for category_id in category_ids:
        for search_query in _sealed_set_queries(item):
            try:
                search_resp = await client.get(
                    f"{TCGTRACKING_BASE}/{category_id}/search",
                    params={"q": search_query},
                    headers=TCGTRACKING_HEADERS,
                )
                if search_resp.status_code != 200:
                    continue
                sets = [row for row in (search_resp.json().get("sets") or []) if isinstance(row, dict)]
            except Exception as exc:
                logger.warning(
                    "[inventory_pricing] TCGTracking sealed set lookup failed for item %s q=%r: %s",
                    item.id,
                    search_query,
                    exc,
                )
                continue
            for set_info in sets[:3]:
                set_id = str(set_info.get("id") or "").strip()
                if not set_id:
                    continue
                try:
                    products_resp, pricing_resp = await asyncio.gather(
                        client.get(f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}", headers=TCGTRACKING_HEADERS),
                        client.get(f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}/pricing", headers=TCGTRACKING_HEADERS),
                    )
                    if products_resp.status_code != 200:
                        continue
                    products = [
                        row for row in (products_resp.json().get("products") or [])
                        if isinstance(row, dict)
                    ]
                    pricing = pricing_resp.json().get("prices", {}) if pricing_resp.status_code == 200 else {}
                except Exception as exc:
                    logger.warning(
                        "[inventory_pricing] TCGTracking sealed products failed for item %s set=%s: %s",
                        item.id,
                        set_id,
                        exc,
                    )
                    continue
                for product in products:
                    score = _sealed_product_score(item, product, set_info)
                    if score <= 0:
                        continue
                    product_id = str(product.get("id") or "")
                    market, low, price_label = _tcgtracking_sealed_price_from_row(product_id, pricing)
                    if market is None and low is None:
                        continue
                    row = (score, product, set_info, category_id, {"market": market, "low": low, "label": price_label})
                    if best is None or row[0] > best[0]:
                        best = row

    if best is None:
        return None
    _score, product, set_info, category_id, price_row = best
    return {
        "source": "tcgtracking",
        "market_price": price_row["market"],
        "low_price": price_row["low"],
        "high_price": None,
        "raw": {
            "source_detail": "tcgtracking_sealed_search",
            "price_label": price_row["label"],
            "category_id": category_id,
            "set": set_info,
            "product": product,
            "tcgplayer_url": product.get("tcgplayer_url"),
        },
    }


# ---------------------------------------------------------------------------
# Singles pricing (Scrydex / Scryfall)
# ---------------------------------------------------------------------------

async def fetch_single_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    *,
    api_key: str = "",
    base_url: str = "",
) -> Optional[dict[str, Any]]:
    """
    Fetch market price for a single card via Scrydex (or Scryfall as fallback).

    Scrydex API format is not yet publicly documented; this implementation targets
    their expected REST interface. Update the request construction below once you
    have API access and can confirm the exact endpoints and response shape.

    When base_url is empty or points to api.scryfall.com, falls back to the free
    Scryfall API (MTG only).
    """
    resolved_url = (base_url or "").rstrip("/")

    # Scryfall-compatible path: used when no Scrydex URL is configured, or when
    # the operator explicitly sets SCRYDEX_BASE_URL=https://api.scryfall.com
    if not resolved_url or "scryfall.com" in resolved_url:
        return await _fetch_scryfall_price(item, client)

    # Scrydex — adapt the path/params/headers once API docs are available.
    # Current implementation uses a reasonable convention; update as needed.
    endpoint = f"{resolved_url}/v1/prices"
    params: dict[str, str] = {"name": item.card_name}
    if item.set_code:
        params["set"] = item.set_code
    if item.card_number:
        params["number"] = item.card_number
    if item.game:
        params["game"] = item.game.lower()
    if item.condition:
        params["condition"] = item.condition.lower()

    headers: dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        resp = await client.get(endpoint, params=params, headers=headers, timeout=15.0)
        resp.raise_for_status()
        data = resp.json()
        market = _safe_float(data.get("market_price") or data.get("market") or data.get("price"))
        low = _safe_float(data.get("low_price") or data.get("low"))
        high = _safe_float(data.get("high_price") or data.get("high"))
        if market is None and low is None:
            logger.warning("[pricing] scrydex returned no price for %s", item.card_name)
            return None
        return {
            "source": "scrydex",
            "market_price": market,
            "low_price": low,
            "high_price": high,
            "raw": data,
        }
    except Exception as exc:
        logger.warning("[pricing] scrydex request failed for %s: %s", item.card_name, exc)
        return None


async def _fetch_scryfall_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    """Fetch price from Scryfall (free MTG-only API)."""
    params: dict[str, str] = {"exact": item.card_name}
    if item.set_code:
        params["set"] = item.set_code

    try:
        resp = await client.get(SCRYFALL_NAMED_URL, params=params, timeout=10.0)
        if resp.status_code == 404:
            logger.info("[pricing] scryfall: card not found: %s", item.card_name)
            return None
        resp.raise_for_status()
        data = resp.json()
        prices = data.get("prices") or {}
        market = _safe_float(prices.get("usd"))
        low = _safe_float(prices.get("usd_foil")) if item.game == "MTG" else None
        if market is None:
            return None
        return {
            "source": "scrydex",   # report as scrydex since that is the configured source
            "market_price": market,
            "low_price": low,
            "high_price": None,
            "raw": {"prices": prices, "name": data.get("name"), "set": data.get("set_name")},
        }
    except Exception as exc:
        logger.warning("[pricing] scryfall request failed for %s: %s", item.card_name, exc)
        return None


# ---------------------------------------------------------------------------
# Slab pricing (Card Ladder -> 130point -> Alt)
# ---------------------------------------------------------------------------

def _slab_query(item: InventoryItem) -> str:
    """Build a search query string for a graded slab."""
    parts = [item.card_name]
    if item.grading_company:
        parts.append(item.grading_company)
    if item.grade:
        parts.append(item.grade)
    return " ".join(parts)


def build_card_ladder_slab_query(item: InventoryItem, *, strict: bool = True) -> str:
    """Build a Card Ladder Sales History query for one graded slab."""
    parts: list[str] = []
    seen: set[str] = set()
    for value in (
        item.card_name,
        item.set_name,
        item.card_number,
        item.grading_company,
        item.grade,
    ):
        cleaned = str(value or "").strip()
        key = cleaned.lower()
        if cleaned and key not in seen:
            parts.append(cleaned)
            seen.add(key)

    if strict:
        grader = (item.grading_company or "").strip().upper()
        for other in OTHER_GRADERS:
            if grader and other == grader:
                continue
            parts.append(f"-{other}")
        if grader and item.grade:
            grade = _safe_float(item.grade)
            if grade is not None:
                for nearby in (grade - 1, grade + 1):
                    if nearby > 0:
                        parts.append(f"-({grader} {nearby:g})")
        for term in CARD_LADDER_NOISE_EXCLUSIONS:
            parts.append(f"-{term}")

    return " ".join(part for part in parts if part).strip()


def build_card_ladder_cli_query(item: InventoryItem, *, strict: bool = True) -> str:
    """Build the exact query key used by scripts/cardladder_cli.py."""
    try:
        from scripts import cardladder_cli
    except Exception:
        return build_card_ladder_slab_query(item, strict=strict)
    return cardladder_cli.build_slab_query(
        _card_ladder_cli_base_query(item),
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        cert=str(item.cert_number or "").strip(),
        strict=strict,
    )


def _card_ladder_cli_base_query(item: InventoryItem) -> str:
    game = str(item.game or "").strip()
    game_part = ""
    if game and game.lower() not in {"pokemon", "pokémon", "pokemon japan"}:
        game_part = game
    elif game.lower() == "pokemon japan":
        game_part = "Japanese Pokemon"
    return " ".join(
        part
        for part in (
            game_part,
            str(item.card_name or "").strip(),
            str(item.set_name or "").strip(),
            str(item.card_number or "").strip(),
        )
        if part
    )


def card_ladder_sales_history_url(query: str) -> str:
    params = urlencode({"sort": "date", "direction": "desc", "q": query})
    return f"{CARD_LADDER_APP_SALES_HISTORY_URL}?{params}"


def card_ladder_cli_status() -> dict[str, Any]:
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    profile_dir = cardladder_cli.default_profile_dir()
    cache_db = cardladder_cli.default_cache_path()
    return {
        "available": True,
        "profile_dir": str(profile_dir),
        "profile_exists": profile_dir.exists(),
        "cache_db": str(cache_db),
        "cache_exists": cache_db.exists(),
        "login_command": ".\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py login",
    }


def _fetch_card_ladder_cli_cache(
    item: InventoryItem,
    *,
    query: str = "",
    cache_path: str | Path | None = None,
    limit: int = 20,
) -> Optional[dict[str, Any]]:
    """Read saved comps from the browser-backed Card Ladder CLI cache."""
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        logger.debug("[pricing] card_ladder_cli unavailable: %s", exc)
        return None

    cache_db = Path(cache_path) if cache_path else cardladder_cli.default_cache_path()
    if not cache_db.exists():
        return None

    resolved_query = query or build_card_ladder_cli_query(item)
    base_query = _card_ladder_cli_base_query(item)
    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    cert = str(item.cert_number or "").strip()

    try:
        records = cardladder_cli.load_cached_records(
            cache_db,
            query=resolved_query,
            limit=limit,
        )
        if not records and base_query:
            records = cardladder_cli.load_cached_records(
                cache_db,
                text=base_query,
                grader=grader,
                grade=grade,
                cert=cert,
                limit=limit,
            )
    except Exception as exc:
        logger.debug("[pricing] card_ladder_cli cache read failed for %s: %s", resolved_query, exc)
        return None

    sales = _card_ladder_sales_from_cli_records(records)
    if not sales:
        return None
    prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(sales)
    if suggested is None:
        return None
    return {
        "source": "card_ladder",
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": {
            "query": resolved_query,
            "sales_history_url": card_ladder_sales_history_url(resolved_query),
            "source_detail": "card_ladder_cli_cache",
            "cache_db": str(cache_db),
            "sample_count": len(prices),
            "sales": sales[:20],
        },
    }


def _card_ladder_sales_from_cli_records(records: list[Any]) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for record in records:
        price = _safe_float(getattr(record, "price", None))
        if price is None:
            continue
        sale = {
            "title": str(getattr(record, "title", "") or "").strip(),
            "price": round(price, 2),
            "sold_date": _normalize_sale_date(getattr(record, "sold_date", "")),
            "platform": str(getattr(record, "platform", "") or "").strip(),
            "sale_type": str(getattr(record, "sale_type", "") or "").strip(),
            "url": str(getattr(record, "url", "") or "").strip(),
            "image_url": str(getattr(record, "image_url", "") or "").strip(),
        }
        key = (sale["sold_date"], sale["price"], sale["title"])
        if key in seen:
            continue
        seen.add(key)
        sales.append(sale)
        if len(sales) >= 50:
            break
    return sales


def build_alt_cli_query(item: InventoryItem) -> str:
    try:
        from scripts import alt_cli
    except Exception:
        return _slab_query(item)
    return alt_cli.build_slab_query(
        _card_ladder_cli_base_query(item),
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        cert=str(item.cert_number or "").strip(),
    )


def alt_cli_status() -> dict[str, Any]:
    try:
        from scripts import alt_cli
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    cache_db = alt_cli.default_cache_path()
    return {
        "available": True,
        "cache_db": str(cache_db),
        "cache_exists": cache_db.exists(),
    }


def _alt_card_number_filter(item: InventoryItem) -> str:
    raw = str(item.card_number or "").strip()
    if not raw:
        return ""
    return raw.split("/", 1)[0].strip()


def _fetch_alt_cli_cache(
    item: InventoryItem,
    *,
    query: str = "",
    cache_path: str | Path | None = None,
    limit: int = 20,
) -> Optional[dict[str, Any]]:
    try:
        from scripts import alt_cli
    except Exception as exc:
        logger.debug("[pricing] alt_cli unavailable: %s", exc)
        return None

    cache_db = Path(cache_path) if cache_path else alt_cli.default_cache_path()
    if not cache_db.exists():
        return None
    resolved_query = query or build_alt_cli_query(item)
    try:
        records = alt_cli.load_cached_records(
            cache_db,
            query=resolved_query,
            limit=limit,
        )
        if not records:
            records = alt_cli.load_cached_records(
                cache_db,
                text=_card_ladder_cli_base_query(item),
                grader=(item.grading_company or "").strip().upper(),
                grade=str(item.grade or "").strip(),
                limit=limit,
            )
    except Exception as exc:
        logger.debug("[pricing] alt_cli cache read failed for %s: %s", resolved_query, exc)
        return None
    return _alt_result_from_records(
        resolved_query,
        records,
        source_detail="alt_cli_cache",
        cache_db=str(cache_db),
    )


async def sync_alt_cli_for_item(
    item: InventoryItem,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    try:
        from scripts import alt_cli
    except Exception as exc:
        raise RuntimeError(f"ALT CLI is unavailable: {exc}") from exc

    query = build_alt_cli_query(item)
    if not query:
        raise RuntimeError("Card name or slab details are required before refreshing ALT.")
    records = await asyncio.to_thread(
        alt_cli.fetch_records,
        query,
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        card_number=_alt_card_number_filter(item),
        limit=limit,
    )
    if not records:
        raise RuntimeError("ALT returned no sold comps for this slab query.")
    await asyncio.to_thread(alt_cli.cache_records, alt_cli.default_cache_path(), query, records)
    result = _alt_result_from_records(query, records, source_detail="alt_typesense_live")
    if not result:
        raise RuntimeError("ALT returned sold rows, but none had usable prices.")
    return result


def _alt_result_from_records(
    query: str,
    records: list[Any],
    *,
    source_detail: str,
    cache_db: str = "",
) -> Optional[dict[str, Any]]:
    return _result_from_comp_records(
        query,
        records,
        source="alt",
        source_detail=source_detail,
        sales_history_url=alt_sales_history_url(query),
        cache_db=cache_db,
    )


def _sales_from_comp_records(records: list[Any]) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for record in records:
        price = _safe_float(getattr(record, "price", None))
        if price is None:
            continue
        sale = {
            "title": str(getattr(record, "title", "") or "").strip(),
            "price": round(price, 2),
            "sold_date": _normalize_sale_date(getattr(record, "sold_date", "")),
            "platform": str(getattr(record, "platform", "") or "").strip(),
            "sale_type": str(getattr(record, "sale_type", "") or "").strip(),
            "url": str(getattr(record, "url", "") or "").strip(),
            "image_url": str(getattr(record, "image_url", "") or "").strip(),
        }
        key = (sale["sold_date"], sale["price"], sale["title"])
        if key in seen:
            continue
        seen.add(key)
        sales.append(sale)
        if len(sales) >= 50:
            break
    return sales


def alt_sales_history_url(query: str) -> str:
    params = urlencode({"query": query, "tab": "sold"})
    return f"{ALT_BROWSE_URL}?{params}"


def build_130point_cli_query(item: InventoryItem) -> str:
    try:
        from scripts import point130_cli
    except Exception:
        return _slab_query(item)
    return point130_cli.build_slab_query(
        _card_ladder_cli_base_query(item),
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        cert=str(item.cert_number or "").strip(),
    )


def point130_cli_status() -> dict[str, Any]:
    try:
        from scripts import point130_cli
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    cache_db = point130_cli.default_cache_path()
    return {
        "available": True,
        "cache_db": str(cache_db),
        "cache_exists": cache_db.exists(),
    }


def _fetch_130point_cli_cache(
    item: InventoryItem,
    *,
    query: str = "",
    cache_path: str | Path | None = None,
    limit: int = 20,
) -> Optional[dict[str, Any]]:
    try:
        from scripts import point130_cli
    except Exception as exc:
        logger.debug("[pricing] point130_cli unavailable: %s", exc)
        return None

    cache_db = Path(cache_path) if cache_path else point130_cli.default_cache_path()
    if not cache_db.exists():
        return None
    resolved_query = query or build_130point_cli_query(item)
    try:
        records = point130_cli.load_cached_records(
            cache_db,
            query=resolved_query,
            limit=limit,
        )
        if not records:
            records = point130_cli.load_cached_records(
                cache_db,
                text=_card_ladder_cli_base_query(item),
                grader=(item.grading_company or "").strip().upper(),
                grade=str(item.grade or "").strip(),
                limit=limit,
            )
    except Exception as exc:
        logger.debug("[pricing] point130_cli cache read failed for %s: %s", resolved_query, exc)
        return None
    return _result_from_comp_records(
        resolved_query,
        records,
        source="130point",
        source_detail="130point_cli_cache",
        sales_history_url=point130_sales_history_url(resolved_query),
        cache_db=str(cache_db),
    )


def point130_sales_history_url(query: str) -> str:
    try:
        from scripts import point130_cli
        return point130_cli.sales_search_url(query)
    except Exception:
        params = urlencode({"q": query})
        return f"{POINT130_SEARCH_URL}?{params}"


def _result_from_comp_records(
    query: str,
    records: list[Any],
    *,
    source: str,
    source_detail: str,
    sales_history_url: str,
    cache_db: str = "",
) -> Optional[dict[str, Any]]:
    sales = _sales_from_comp_records(records)
    if not sales:
        return None
    prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(sales)
    if suggested is None:
        return None
    for sale in sales:
        sale.setdefault("source", source)
        sale.setdefault("sources", [source])
        sale.setdefault("source_details", [source_detail])
    raw: dict[str, Any] = {
        "query": query,
        "sales_history_url": sales_history_url,
        "source_detail": source_detail,
        "sample_count": len(prices),
        "sales": sales[:20],
    }
    if cache_db:
        raw["cache_db"] = cache_db
    return {
        "source": source,
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": raw,
    }


async def sync_card_ladder_cli_for_item(
    item: InventoryItem,
    *,
    timeout_seconds: int = 120,
    limit: int = 25,
    headless: bool = True,
) -> dict[str, Any]:
    """Refresh one slab query through the Card Ladder CLI and return cached comps."""
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        raise RuntimeError(f"Card Ladder CLI is unavailable: {exc}") from exc

    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "scripts" / "cardladder_cli.py"
    if not script_path.exists():
        raise RuntimeError(f"Card Ladder CLI script is missing: {script_path}")

    base_query = _card_ladder_cli_base_query(item)
    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    cert = str(item.cert_number or "").strip()
    if not base_query and not cert:
        raise RuntimeError("Card name or cert number is required before refreshing Card Ladder.")

    cmd = [sys.executable, str(script_path), "sync"]
    if base_query:
        cmd.append(base_query)
    if grader:
        cmd.extend(["--grader", grader])
    if grade:
        cmd.extend(["--grade", grade])
    if cert:
        cmd.extend(["--cert", cert])
    cmd.extend(["--limit", str(max(1, int(limit)))])
    if headless:
        cmd.append("--headless")
    cmd.append("--no-prompt")

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            process.communicate(),
            timeout=max(5, int(timeout_seconds)),
        )
    except asyncio.TimeoutError as exc:
        raise RuntimeError("Card Ladder refresh timed out. Run the CLI login command, then try again.") from exc

    stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
    stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
    if process.returncode != 0:
        detail = stderr or stdout or f"Card Ladder CLI exited with code {process.returncode}."
        raise RuntimeError(detail[:1000])

    query = cardladder_cli.build_slab_query(
        base_query,
        grader=grader,
        grade=grade,
        cert=cert,
        strict=True,
    )
    result = _fetch_card_ladder_cli_cache(item, query=query, limit=limit)
    if not result:
        raise RuntimeError("Card Ladder CLI finished, but no comps were saved for this slab query.")
    return result


def import_card_ladder_cli_records_for_item(
    item: InventoryItem,
    *,
    text: str,
    query: str = "",
    cache_path: str | Path | None = None,
) -> dict[str, Any]:
    """Parse pasted/exported sold rows into the Card Ladder CLI cache."""
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        raise RuntimeError(f"Card Ladder CLI parser is unavailable: {exc}") from exc

    records = _card_ladder_records_from_text(text)
    if not records:
        raise RuntimeError("No sold comps were recognized in the pasted text.")

    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    cert = str(item.cert_number or "").strip()
    for record in records:
        if grader and not getattr(record, "grader", ""):
            record.grader = grader
        if grade and not getattr(record, "grade", ""):
            record.grade = grade
        if cert and not getattr(record, "cert", ""):
            record.cert = cert

    resolved_query = query or build_card_ladder_cli_query(item)
    cache_db = Path(cache_path) if cache_path else cardladder_cli.default_cache_path()
    saved = cardladder_cli.cache_records(cache_db, resolved_query, records)
    result = _fetch_card_ladder_cli_cache(
        item,
        query=resolved_query,
        cache_path=cache_db,
        limit=max(20, len(records)),
    )
    if not result:
        raise RuntimeError("Comps were parsed, but could not be read back from the Card Ladder cache.")
    raw = result.setdefault("raw", {})
    if isinstance(raw, dict):
        raw["source_detail"] = "card_ladder_manual_import"
        raw["imported_count"] = saved
    return result


def _card_ladder_records_from_text(text: str) -> list[Any]:
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        raise RuntimeError(f"Card Ladder CLI parser is unavailable: {exc}") from exc

    raw = (text or "").strip()
    if not raw:
        return []

    records: list[Any] = []
    records.extend(_card_ladder_records_from_csv(raw, cardladder_cli))

    chunks = [chunk.strip() for chunk in re.split(r"\n\s*\n", raw) if chunk.strip()]
    for chunk in chunks:
        record = cardladder_cli.record_from_text(chunk)
        if record:
            records.append(record)

    if not records:
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        for chunk in _sliding_text_chunks(lines):
            record = cardladder_cli.record_from_text(chunk)
            if record:
                records.append(record)

    return cardladder_cli.dedupe_records(records)


def _card_ladder_records_from_csv(text: str, cardladder_cli: Any) -> list[Any]:
    first_line = text.splitlines()[0] if text.splitlines() else ""
    if "," not in first_line and "\t" not in first_line:
        return []
    try:
        sample = text[:2048]
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
    except Exception:
        dialect = csv.excel_tab if "\t" in first_line else csv.excel
    try:
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        records = []
        for row in reader:
            record = cardladder_cli.record_from_json(dict(row))
            if record:
                records.append(record)
        return records
    except Exception:
        return []


def _sliding_text_chunks(lines: list[str]) -> list[str]:
    chunks: list[str] = []
    if not lines:
        return chunks
    for size in range(3, min(8, len(lines)) + 1):
        for index in range(0, len(lines) - size + 1):
            window = lines[index:index + size]
            if any("$" in line for line in window):
                chunks.append("\n".join(window))
    if not chunks:
        chunks.extend(lines)
    return chunks


async def fetch_slab_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    *,
    source_filter: str = "all",
) -> Optional[dict[str, Any]]:
    """
    Collect slab comps from every available source and dedupe overlapping solds.
    """
    selected_source = normalize_slab_price_source(source_filter)
    results: list[dict[str, Any]] = []

    if _wants_slab_source(selected_source, "card_ladder"):
        cli_query = build_card_ladder_cli_query(item)
        result = _fetch_card_ladder_cli_cache(item, query=cli_query)
        _add_slab_price_result(results, item, result)

        if not result or selected_source == "all":
            card_ladder_query = build_card_ladder_slab_query(item)
            result = await _fetch_card_ladder_price(item, client, card_ladder_query)
            _add_slab_price_result(results, item, result)

    query = _slab_query(item)
    if _wants_slab_source(selected_source, "alt"):
        result = await _fetch_alt_price(item, client, query)
        _add_slab_price_result(results, item, result)

    if _wants_slab_source(selected_source, "130point"):
        result = await _fetch_130point_price(item, client, query)
        _add_slab_price_result(results, item, result)

    if _wants_slab_source(selected_source, "myslabs"):
        result = await _fetch_myslabs_price(item, client, query)
        _add_slab_price_result(results, item, result)

    if _wants_slab_source(selected_source, "pricecharting"):
        result = await _fetch_pricecharting_price(item, client, query)
        _add_slab_price_result(results, item, result)

    if selected_source != "all":
        return results[0] if results else None

    return combine_slab_price_results(item, results)


def normalize_slab_price_source(source: Any) -> str:
    cleaned = str(source or "").strip().lower().replace("-", "_")
    normalized = SLAB_PRICE_SOURCE_ALIASES.get(cleaned, cleaned)
    return normalized if normalized in SLAB_PRICE_SOURCE_OPTIONS else "all"


def _wants_slab_source(selected_source: str, source: str) -> bool:
    return selected_source == "all" or selected_source == source


def _add_slab_price_result(
    results: list[dict[str, Any]],
    item: InventoryItem,
    result: Optional[dict[str, Any]],
) -> None:
    if not result:
        return
    filtered = _filter_slab_price_result_for_item(item, result)
    if filtered:
        results.append(filtered)


async def fetch_ximilar_slab_price_from_image(
    image_b64: str,
    client: httpx.AsyncClient,
    *,
    api_token: str,
    category_id: str = "3",
) -> Optional[dict[str, Any]]:
    """Use Ximilar's image-first collectibles price guide for a slab photo."""
    if not api_token or not str(image_b64 or "").strip():
        return None
    clean_image = _prepare_ximilar_image_base64(image_b64)
    try:
        resp = await client.post(
            XIMILAR_TCG_IDENTIFY_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Token {api_token}",
            },
            json={
                "records": [{"_base64": clean_image}],
                "pricing": True,
                "slab_id": True,
                "slab_grade": True,
            },
            timeout=40.0,
        )
        if resp.status_code != 200:
            logger.warning("[pricing] Ximilar slab price HTTP %s: %s", resp.status_code, resp.text[:500])
            return None
        return _ximilar_price_result_from_payload(resp.json(), category_id=category_id)
    except Exception as exc:
        logger.debug("[pricing] Ximilar slab price failed: %s", exc)
        return None


def _prepare_ximilar_image_base64(image_b64: str) -> str:
    raw = str(image_b64 or "").strip()
    if "," in raw:
        raw = raw.split(",", 1)[1]
    try:
        data = base64.b64decode(raw)
    except Exception:
        return raw
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(data))
        max_dim = max(img.size)
        if max_dim > 960:
            scale = 960 / max_dim
            img = img.resize((int(img.size[0] * scale), int(img.size[1] * scale)), Image.LANCZOS)
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=84)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return raw


def _ximilar_price_result_from_payload(
    payload: dict[str, Any],
    *,
    category_id: str = "3",
) -> Optional[dict[str, Any]]:
    card_info = _ximilar_card_info(payload, category_id=category_id)
    slab_info = _ximilar_slab_info(payload)
    listings = _ximilar_listing_rows(payload)
    sales = _ximilar_sales_from_listings(
        listings,
        grading_company=str(slab_info.get("grading_company") or ""),
        grade=str(slab_info.get("grade") or ""),
    )
    prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(sales) if sales else None
    query = " ".join(
        str(part or "").strip()
        for part in (
            card_info.get("card_name"),
            card_info.get("set_name"),
            card_info.get("card_number"),
            slab_info.get("grading_company"),
            slab_info.get("grade"),
        )
        if str(part or "").strip()
    )
    if not card_info and not slab_info and not sales:
        return None
    return {
        "source": "ximilar",
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": {
            "query": query,
            "source_detail": "ximilar_price_guide",
            "sample_count": len(prices),
            "sales": sales[:20],
            "ximilar_card": card_info,
            "ximilar_slab": slab_info,
            "ximilar_status": payload.get("status") if isinstance(payload.get("status"), dict) else {},
        },
    }


def _ximilar_first_record(payload: dict[str, Any]) -> dict[str, Any]:
    records = payload.get("records") if isinstance(payload, dict) else None
    if isinstance(records, list) and records and isinstance(records[0], dict):
        return records[0]
    return {}


def _ximilar_objects(payload: dict[str, Any]) -> list[dict[str, Any]]:
    record = _ximilar_first_record(payload)
    objects = record.get("_objects")
    if isinstance(objects, list):
        return [obj for obj in objects if isinstance(obj, dict)]
    return []


def _ximilar_object(payload: dict[str, Any], name: str) -> dict[str, Any]:
    wanted = name.strip().lower()
    for obj in _ximilar_objects(payload):
        if str(obj.get("name") or "").strip().lower() == wanted:
            return obj
    return {}


def _ximilar_best_match(obj: dict[str, Any]) -> dict[str, Any]:
    identification = obj.get("_identification") if isinstance(obj, dict) else None
    if not isinstance(identification, dict):
        return {}
    best = identification.get("best_match")
    return best if isinstance(best, dict) else {}


def _ximilar_tag_name(obj: dict[str, Any], group: str) -> str:
    tags = obj.get("_tags") if isinstance(obj, dict) else None
    values = tags.get(group) if isinstance(tags, dict) else None
    if isinstance(values, list) and values and isinstance(values[0], dict):
        return str(values[0].get("name") or "").strip()
    return ""


def _ximilar_card_info(payload: dict[str, Any], *, category_id: str = "3") -> dict[str, Any]:
    card_obj = _ximilar_object(payload, "Card")
    best = _ximilar_best_match(card_obj)
    if not best:
        return {}
    card_number = str(best.get("card_number") or best.get("card_no") or "").strip()
    out_of = str(best.get("out_of") or "").strip()
    if card_number and out_of and "/" not in card_number:
        card_number = f"{card_number}/{out_of}"
    subcategory = str(best.get("subcategory") or _ximilar_tag_name(card_obj, "Subcategory") or "").strip()
    return {
        "card_name": str(best.get("name") or "").strip(),
        "full_name": str(best.get("full_name") or best.get("name") or "").strip(),
        "set_name": str(best.get("set") or "").strip(),
        "set_code": str(best.get("set_code") or "").strip(),
        "card_number": card_number,
        "rarity": str(best.get("rarity") or "").strip(),
        "year": best.get("year"),
        "game": _ximilar_game_name(subcategory, category_id),
        "links": best.get("links") if isinstance(best.get("links"), dict) else {},
    }


def _ximilar_slab_info(payload: dict[str, Any]) -> dict[str, Any]:
    slab_obj = _ximilar_object(payload, "Slab Label")
    best = _ximilar_best_match(slab_obj)
    if not best and not slab_obj:
        return {}
    company = (
        best.get("grade_company")
        or best.get("grading_company")
        or best.get("grader")
        or best.get("company")
        or _ximilar_tag_name(slab_obj, "Company")
        or ""
    )
    grade = best.get("grade_value") or best.get("grade") or _ximilar_tag_name(slab_obj, "Grade") or ""
    return {
        "card_name": str(best.get("name") or "").strip(),
        "grading_company": _normalize_grading_company(company),
        "grade": _normalize_grade_value(grade),
        "cert_number": str(
            best.get("certificate_number")
            or best.get("cert_number")
            or best.get("cert")
            or ""
        ).strip(),
        "set_name": str(best.get("set") or "").strip(),
        "card_number": str(best.get("card_no") or best.get("card_number") or "").lstrip("#").strip(),
        "raw": best,
    }


def _ximilar_game_name(subcategory: str, category_id: str = "3") -> str:
    clean = subcategory.strip().lower()
    if "pokemon" in clean:
        return "Pokemon"
    if "magic" in clean:
        return "Magic"
    if "yu-gi" in clean or "yugioh" in clean:
        return "Yu-Gi-Oh"
    if "one piece" in clean:
        return "One Piece"
    if "lorcana" in clean:
        return "Lorcana"
    if "riftbound" in clean:
        return "Riftbound"
    return {
        "1": "Magic",
        "2": "Yu-Gi-Oh",
        "3": "Pokemon",
        "68": "One Piece",
        "71": "Lorcana",
        "89": "Riftbound",
    }.get(str(category_id), "Other")


def _ximilar_listing_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            ident = id(value)
            if ident in seen:
                return
            seen.add(ident)
            if _looks_like_ximilar_listing(value):
                rows.append(value)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return rows


def _looks_like_ximilar_listing(row: dict[str, Any]) -> bool:
    if _safe_float(row.get("price")) is None:
        return False
    return bool(
        row.get("item_link")
        or row.get("url")
        or row.get("link")
        or row.get("source")
    ) and bool(
        row.get("name")
        or row.get("title")
        or row.get("item_id")
        or row.get("item_link")
    )


def _ximilar_sales_from_listings(
    listings: list[dict[str, Any]],
    *,
    grading_company: str = "",
    grade: str = "",
) -> list[dict[str, Any]]:
    sales = [_ximilar_sale_from_listing(row) for row in listings]
    sales = [sale for sale in sales if sale]
    target_grade = _normalize_grade_value(grade)
    target_company = _normalize_grading_company(grading_company)
    exact = [
        sale for sale in sales
        if (
            (not target_grade or not sale.get("grade") or sale.get("grade") == target_grade)
            and (not target_company or not sale.get("grading_company") or sale.get("grading_company") == target_company)
        )
    ]
    if exact:
        sales = exact
    sales.sort(key=_slab_sale_sort_key, reverse=True)
    return sales[:50]


def _ximilar_sale_from_listing(row: dict[str, Any]) -> dict[str, Any]:
    price = _safe_float(row.get("price"))
    if price is None:
        return {}
    url = str(row.get("item_link") or row.get("url") or row.get("link") or "").strip()
    title = str(row.get("name") or row.get("title") or row.get("item_id") or url or "Ximilar marketplace listing").strip()
    sold_date = _normalize_sale_date(
        row.get("date_of_sale")
        or row.get("sold_date")
        or row.get("date_sold")
        or row.get("date_of_creation")
        or row.get("created_at")
        or ""
    )
    grade_value = _normalize_grade_value(row.get("grade_value") or row.get("grade") or "")
    company = _normalize_grading_company(row.get("grade_company") or row.get("grading_company") or row.get("grader") or "")
    return {
        "title": title,
        "price": round(price, 2),
        "sold_date": sold_date,
        "platform": str(row.get("source") or "").strip() or "Ximilar",
        "sale_type": "Sold" if row.get("date_of_sale") else "Listing",
        "url": url,
        "image_url": str(row.get("image") or row.get("image_url") or "").strip(),
        "currency": str(row.get("currency") or "").strip(),
        "country_code": str(row.get("country_code") or "").strip(),
        "grading_company": company,
        "grade": grade_value,
        "sources": ["ximilar"],
        "source_details": ["ximilar_price_guide"],
    }


def _normalize_grading_company(value: Any) -> str:
    clean = str(value or "").strip().upper()
    if clean in {"BECKETT", "BGS/BECKETT"}:
        return "BGS"
    return clean


def _normalize_grade_value(value: Any) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"^(PSA|BGS|CGC|SGC|BECKETT)\s+", "", text).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


async def _fetch_130point_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """
    130point.com graded card sales data.

    The endpoint returns a JSON array of recent sales. We compute median price
    from the top results. Update URL/params if the site structure changes.
    """
    point_query = build_130point_cli_query(item)
    cached = _fetch_130point_cli_cache(item, query=point_query)
    if cached:
        return cached

    try:
        from scripts import point130_cli

        records = await asyncio.to_thread(point130_cli.fetch_records, point_query, limit=20)
        if records:
            await asyncio.to_thread(point130_cli.cache_records, point130_cli.default_cache_path(), point_query, records)
        return _result_from_comp_records(
            point_query,
            records,
            source="130point",
            source_detail="130point_live",
            sales_history_url=point130_sales_history_url(point_query),
        )
    except Exception as exc:
        logger.debug("[pricing] 130point failed for %s: %s", point_query or query, exc)
        return None


async def _fetch_alt_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """ALT sold-listing search using the same web-app Typesense service."""
    alt_query = build_alt_cli_query(item)
    cached = _fetch_alt_cli_cache(item, query=alt_query)
    if cached:
        return cached

    try:
        return await sync_alt_cli_for_item(item, limit=20)
    except Exception as exc:
        logger.debug("[pricing] alt failed for %s: %s", alt_query or query, exc)
        return None


async def _fetch_myslabs_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """MySlabs public sold archive for slabbed card sales."""
    myslabs_query = build_myslabs_query(item)
    try:
        resp = await client.get(
            MYSLABS_ARCHIVE_SEARCH_URL,
            params={"publish_type": "0", "q": myslabs_query, "o": "created_desc"},
            timeout=20.0,
            follow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "Mozilla/5.0 (compatible; DegenCollectibles/1.0)",
            },
        )
        if resp.status_code != 200:
            return None
        sales = _myslabs_sales_from_html(resp.text, limit=20)
        if not sales:
            return None
        prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
        suggested = _suggest_price_from_sales(sales)
        if suggested is None:
            return None
        return {
            "source": "myslabs",
            "market_price": suggested,
            "low_price": round(min(prices), 2) if prices else None,
            "high_price": round(max(prices), 2) if prices else None,
            "raw": {
                "query": myslabs_query,
                "sales_history_url": str(resp.url),
                "source_detail": "myslabs_archive",
                "sample_count": len(prices),
                "sales": sales[:20],
            },
        }
    except Exception as exc:
        logger.debug("[pricing] myslabs failed for %s: %s", myslabs_query or query, exc)
        return None


def build_myslabs_query(item: InventoryItem) -> str:
    card_number = str(item.card_number or "").strip()
    if "/" in card_number:
        card_number = card_number.split("/", 1)[0].strip()
    parts = [
        item.card_name or "",
        item.set_name or "",
        card_number,
        item.grading_company or "",
        item.grade or "",
    ]
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def _myslabs_sales_from_html(text: str, *, limit: int = 20) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    for block in re.split(r'(?=<div class="slab_item\b)', text or "")[1:]:
        end = block.find('<script type="application/ld+json"')
        if end != -1:
            block = block[:end]
        title_m = re.search(r'<div class="slab-title">\s*(.*?)\s*</div>', block, flags=re.IGNORECASE | re.DOTALL)
        price_m = re.search(r'<div class="item-price">\s*\$([^<]+)', block, flags=re.IGNORECASE | re.DOTALL)
        date_m = re.search(r'<small class="[^"]*">\s*([^<]+?)\s*</small>', block, flags=re.IGNORECASE | re.DOTALL)
        href_m = re.search(r'<a href="([^"]+)"', block, flags=re.IGNORECASE)
        image_m = re.search(r'(?:data-src|src)="([^"]+)"', block, flags=re.IGNORECASE)
        if not title_m or not price_m or not date_m:
            continue
        price = _safe_float(price_m.group(1))
        if price is None:
            continue
        title = html_lib.unescape(re.sub(r"<[^>]+>", " ", title_m.group(1)))
        title = re.sub(r"\s+", " ", title).strip()
        sold_date = _myslabs_sale_date(date_m.group(1))
        url = urljoin("https://myslabs.com", html_lib.unescape(href_m.group(1)).strip()) if href_m else ""
        image_url = html_lib.unescape(image_m.group(1)).strip() if image_m else ""
        sales.append(
            {
                "title": title,
                "price": round(price, 2),
                "sold_date": sold_date,
                "platform": "MySlabs",
                "sale_type": "",
                "url": url,
                "image_url": image_url,
                "sources": ["myslabs"],
                "source_details": ["myslabs_archive"],
            }
        )
        if len(sales) >= limit:
            break
    return sales


def _myslabs_sale_date(value: Any) -> str:
    raw = html_lib.unescape(str(value or "")).strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return _normalize_sale_date(raw)


async def _fetch_pricecharting_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """PriceCharting public Pokemon page fallback for graded slab sold listings."""
    product_url = _pricecharting_product_url(item)
    if not product_url:
        return None
    try:
        resp = await client.get(
            product_url,
            timeout=20.0,
            follow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "Mozilla/5.0 (compatible; DegenCollectibles/1.0)",
            },
        )
        if resp.status_code != 200:
            return None
        sales = _pricecharting_sales_from_html(resp.text, item)
        if not sales:
            return None
        prices = [float(s["price"]) for s in sales if _safe_float(s.get("price")) is not None]
        suggested = _suggest_price_from_sales(sales)
        if suggested is None:
            return None
        return {
            "source": "pricecharting",
            "market_price": suggested,
            "low_price": round(min(prices), 2) if prices else None,
            "high_price": round(max(prices), 2) if prices else None,
            "raw": {
                "query": query,
                "product_url": str(resp.url),
                "source_detail": "pricecharting",
                "sample_count": len(prices),
                "sales": sales[:20],
            },
        }
    except Exception as exc:
        logger.debug("[pricing] pricecharting failed for %s: %s", query, exc)
        return None


def _pricecharting_product_url(item: InventoryItem) -> Optional[str]:
    if (item.game or "").strip().lower() not in {"pokemon", "pokémon"}:
        return None
    if not item.card_name or not item.set_name or not item.card_number:
        return None
    set_slug = _slugify_for_pricecharting(item.set_name)
    name_slug = _slugify_for_pricecharting(item.card_name)
    number = str(item.card_number or "").split("/", 1)[0].strip()
    number_slug = _slugify_for_pricecharting(number)
    if not set_slug or not name_slug or not number_slug:
        return None
    return f"{PRICECHARTING_GAME_URL}/pokemon-{set_slug}/{name_slug}-{number_slug}"


def _slugify_for_pricecharting(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return re.sub(r"-{2,}", "-", cleaned)


def _pricecharting_sales_from_html(text: str, item: InventoryItem) -> list[dict[str, Any]]:
    label = _pricecharting_grade_label(item)
    completed_class = _pricecharting_completed_class_for_label(text, label)
    if not completed_class and label.startswith("Grade "):
        completed_class = _pricecharting_completed_class_for_label(text, label.replace("Grade ", ""))
    if not completed_class:
        return []
    block = _pricecharting_completed_block(text, completed_class)
    if not block:
        return []

    sales: list[dict[str, Any]] = []
    for row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", block, flags=re.IGNORECASE | re.DOTALL):
        date_m = re.search(r'<td[^>]*class="[^"]*\bdate\b[^"]*"[^>]*>\s*([^<]+)', row, flags=re.IGNORECASE)
        price_m = re.search(r'<span[^>]*class="[^"]*\bjs-price\b[^"]*"[^>]*>\s*([^<]+)', row, flags=re.IGNORECASE)
        title_td = re.search(r'<td[^>]*class="[^"]*\btitle\b[^"]*"[^>]*>(.*?)</td>', row, flags=re.IGNORECASE | re.DOTALL)
        if not date_m or not price_m or not title_td:
            continue
        price = _safe_float(price_m.group(1))
        if price is None:
            continue
        title_html = title_td.group(1)
        href_m = re.search(r'href="([^"]+)"', title_html, flags=re.IGNORECASE)
        platform_m = re.search(r"\[([^\]]+)\]", title_html)
        title = re.sub(r"<[^>]+>", " ", title_html)
        title = re.sub(r"\[[^\]]+\]", " ", title)
        title = html_lib.unescape(re.sub(r"\s+", " ", title)).strip()
        sales.append(
            {
                "title": title,
                "price": round(price, 2),
                "sold_date": _normalize_sale_date(date_m.group(1).strip()),
                "platform": platform_m.group(1).strip() if platform_m else "PriceCharting",
                "sale_type": "",
                "url": html_lib.unescape(href_m.group(1)).strip() if href_m else "",
            }
        )
        if len(sales) >= 20:
            break
    return sales


def _pricecharting_grade_label(item: InventoryItem) -> str:
    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    if grader and grade == "10":
        return f"{grader} 10"
    return f"Grade {grade}" if grade else ""


def _pricecharting_completed_class_for_label(text: str, target_label: str) -> str:
    if not target_label:
        return ""
    select_m = re.search(
        r'<select[^>]+id="completed-auctions-condition"[^>]*>(.*?)</select>',
        text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not select_m:
        return ""
    target = target_label.strip().lower()
    for value, label in re.findall(r'<option[^>]+value="([^"]*)"[^>]*>\s*([^<]+)</option>', select_m.group(1)):
        display = html_lib.unescape(label).strip()
        display = re.sub(r"\s+\(\d+\)$", "", display).strip()
        if display.lower() == target:
            return value.strip()
    return ""


def _pricecharting_completed_block(text: str, completed_class: str) -> str:
    class_pat = re.escape(completed_class)
    start_matches = list(re.finditer(
        rf'<div[^>]+class="[^"]*\b{class_pat}\b[^"]*"[^>]*>',
        text or "",
        flags=re.IGNORECASE,
    ))
    for start_m in start_matches:
        table_end = (text or "").find("</table>", start_m.end())
        if table_end == -1:
            continue
        block = (text or "")[start_m.end():table_end]
        if re.search(r'<td[^>]*class="[^"]*\bdate\b', block, flags=re.IGNORECASE) and "js-price" in block:
            return block
    return ""


async def _fetch_card_ladder_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """Card Ladder graded card price history."""
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "DegenCollectibles/1.0",
    }
    sales_url = card_ladder_sales_history_url(query)
    try:
        for endpoint, params in (
            (CARD_LADDER_SEARCH_URL, {"q": query}),
            (CARD_LADDER_CARDS_SEARCH_URL, {"q": query, "limit": "20"}),
        ):
            resp = await client.get(endpoint, params=params, timeout=15.0, headers=headers)
            if resp.status_code != 200:
                continue
            data: Any
            try:
                data = resp.json()
            except Exception:
                data = resp.text
            result = _card_ladder_result_from_payload(query, data, sales_url=sales_url)
            if result:
                return result

        resp = await client.get(
            CARD_LADDER_APP_SALES_HISTORY_URL,
            params={"sort": "date", "direction": "desc", "q": query},
            timeout=15.0,
            headers={"Accept": "text/html,application/xhtml+xml", "User-Agent": "DegenCollectibles/1.0"},
        )
        if resp.status_code == 200:
            return _card_ladder_result_from_payload(query, resp.text, sales_url=sales_url)
    except Exception as exc:
        logger.debug("[pricing] card_ladder failed for %s: %s", query, exc)
    return None


def _card_ladder_result_from_payload(
    query: str,
    payload: Any,
    *,
    sales_url: str,
) -> Optional[dict[str, Any]]:
    sales = _card_ladder_sales_from_payload(payload)
    if sales:
        prices = [float(s["price"]) for s in sales if _safe_float(s.get("price")) is not None]
        suggested = _suggest_price_from_sales(sales)
        if suggested is None:
            return None
        return {
            "source": "card_ladder",
            "market_price": suggested,
            "low_price": round(min(prices), 2) if prices else None,
            "high_price": round(max(prices), 2) if prices else None,
            "raw": {
                "query": query,
                "sales_history_url": sales_url,
                "sample_count": len(prices),
                "sales": sales[:20],
            },
        }

    market_row = _card_ladder_market_row(payload)
    if not market_row:
        return None
    market = _safe_float(
        market_row.get("market_price")
        or market_row.get("avg_price")
        or market_row.get("average_price")
        or market_row.get("price")
    )
    if market is None:
        return None
    low = _safe_float(market_row.get("low_price") or market_row.get("low"))
    high = _safe_float(market_row.get("high_price") or market_row.get("high"))
    return {
        "source": "card_ladder",
        "market_price": round(market, 2),
        "low_price": round(low, 2) if low else None,
        "high_price": round(high, 2) if high else None,
        "raw": {
            "query": query,
            "sales_history_url": sales_url,
            "sample_count": 0,
            "market_row": {
                key: market_row.get(key)
                for key in ("title", "name", "market_price", "avg_price", "price", "low_price", "high_price")
                if key in market_row
            },
        },
    }


def _card_ladder_sales_from_payload(payload: Any) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for item in _walk_payload_mappings(payload):
        sale = _card_ladder_sale_from_mapping(item)
        if not sale:
            continue
        key = (str(sale.get("sold_date") or ""), float(sale["price"]), str(sale.get("title") or ""))
        if key in seen:
            continue
        seen.add(key)
        sales.append(sale)
        if len(sales) >= 50:
            break
    return sales


def _card_ladder_sale_from_mapping(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    price = _safe_float(
        item.get("price")
        or item.get("sale_price")
        or item.get("sold_price")
        or item.get("soldPrice")
        or item.get("amount")
    )
    sold_date = (
        item.get("sold_date")
        or item.get("soldDate")
        or item.get("date_sold")
        or item.get("dateSold")
        or item.get("sold_at")
        or item.get("date")
    )
    if price is None or not sold_date:
        return None
    title = (
        item.get("title")
        or item.get("listing_title")
        or item.get("listingTitle")
        or item.get("name")
        or item.get("card_name")
        or ""
    )
    return {
        "title": str(title).strip(),
        "price": round(price, 2),
        "sold_date": _normalize_sale_date(sold_date),
        "platform": str(item.get("platform") or item.get("source") or "").strip(),
        "sale_type": str(item.get("sale_type") or item.get("type") or item.get("listing_type") or "").strip(),
        "url": str(item.get("url") or item.get("href") or item.get("link") or "").strip(),
    }


def _card_ladder_market_row(payload: Any) -> Optional[dict[str, Any]]:
    for item in _walk_payload_mappings(payload):
        if not any(key in item for key in ("market_price", "avg_price", "average_price", "price")):
            continue
        if _safe_float(item.get("market_price") or item.get("avg_price") or item.get("average_price") or item.get("price")):
            return item
    return None


def _walk_payload_mappings(payload: Any, *, depth: int = 0) -> list[dict[str, Any]]:
    if depth > 8:
        return []
    if isinstance(payload, str):
        return _walk_payload_mappings(_json_candidates_from_text(payload), depth=depth + 1)
    if isinstance(payload, dict):
        rows = [payload]
        for value in payload.values():
            rows.extend(_walk_payload_mappings(value, depth=depth + 1))
        return rows
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for value in payload:
            rows.extend(_walk_payload_mappings(value, depth=depth + 1))
        return rows
    return []


def _json_candidates_from_text(text: str) -> list[Any]:
    candidates: list[Any] = []
    for pattern in (
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>',
        r'<script[^>]+type="application/json"[^>]*>\s*(\{.*?\})\s*</script>',
    ):
        for match in re.finditer(pattern, text or "", re.IGNORECASE | re.DOTALL):
            try:
                candidates.append(json.loads(match.group(1)))
            except Exception:
                continue
    return candidates


def _normalize_sale_date(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raw = str(value or "").strip()
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    if iso_match:
        return iso_match.group(1)
    return raw


def _sale_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    if iso_match:
        try:
            return date.fromisoformat(iso_match.group(1))
        except ValueError:
            return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _suggest_price_from_sales(sales: list[dict[str, Any]]) -> Optional[float]:
    if not sales:
        return None
    for sale in sales:
        latest_price = _safe_float(sale.get("price"))
        if latest_price is None:
            continue
        latest_date = _sale_date(sale.get("sold_date") or sale.get("date"))
        if latest_date and (utcnow().date() - latest_date).days > STALE_SLAB_COMP_DAYS:
            return round(latest_price, 2)
        break
    weighted: list[float] = []
    for index, sale in enumerate(sales[:5]):
        price = _safe_float(sale.get("price"))
        if price is None:
            continue
        weighted.extend([price] * (2 if index == 0 else 1))
    if not weighted:
        return None
    weighted.sort()
    mid = len(weighted) // 2
    if len(weighted) % 2 == 0:
        return round((weighted[mid - 1] + weighted[mid]) / 2, 2)
    return round(weighted[mid], 2)


def combine_slab_price_results(
    item: InventoryItem,
    results: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    usable = [result for result in results if isinstance(result, dict)]
    if not usable:
        return None

    all_sales: list[dict[str, Any]] = []
    source_results: list[dict[str, Any]] = []
    source_urls: dict[str, str] = {}
    for result in usable:
        source = str(result.get("source") or "unknown")
        raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
        source_detail = str(raw.get("source_detail") or source)
        source_results.append(
            {
                "source": source,
                "source_detail": source_detail,
                "market_price": result.get("market_price"),
                "sample_count": raw.get("sample_count"),
                "query": raw.get("query"),
            }
        )
        url = str(raw.get("sales_history_url") or raw.get("product_url") or "")
        if url:
            source_urls[source] = url
        for sale in _sales_from_price_result(result):
            sale_sources = _clean_sources(sale.get("sources")) or [source]
            if source not in sale_sources:
                sale_sources.append(source)
            details = _clean_sources(sale.get("source_details")) or [source_detail]
            if source_detail not in details:
                details.append(source_detail)
            sale["sources"] = sale_sources
            sale["source_details"] = details
            sale["source"] = "+".join(sale_sources)
            all_sales.append(sale)

    merged_sales = _dedupe_slab_sales(all_sales)
    if not merged_sales:
        return None
    prices = [float(sale["price"]) for sale in merged_sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(merged_sales)
    if suggested is None:
        return None
    sources = sorted({source for sale in merged_sales for source in _clean_sources(sale.get("sources"))})
    return {
        "source": "slab_comps",
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": {
            "query": build_alt_cli_query(item),
            "source_detail": "multi_source_comps",
            "sources": sources,
            "source_urls": source_urls,
            "source_results": source_results,
            "sales_history_url": next(iter(source_urls.values()), ""),
            "sample_count": len(prices),
            "sales": merged_sales[:30],
        },
    }


def _sales_from_price_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    source = str(result.get("source") or "")
    raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
    sales = raw.get("sales")
    if not isinstance(sales, list):
        return []
    out: list[dict[str, Any]] = []
    for sale in sales:
        if not isinstance(sale, dict):
            continue
        price = _safe_float(sale.get("price"))
        if price is None:
            continue
        copied = dict(sale)
        copied["price"] = round(price, 2)
        copied["sold_date"] = _normalize_sale_date(copied.get("sold_date") or copied.get("date"))
        copied.setdefault("source", source)
        copied.setdefault("sources", [source] if source else [])
        out.append(copied)
    return out


def _filter_slab_price_result_for_item(
    item: InventoryItem,
    result: dict[str, Any],
) -> Optional[dict[str, Any]]:
    raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
    sales = raw.get("sales")
    if not isinstance(sales, list):
        return result
    filtered_sales = [
        sale
        for sale in sales
        if (
            isinstance(sale, dict)
            and _slab_sale_matches_item_identity(item, sale)
            and _slab_sale_matches_item_variant(item, sale)
        )
    ]
    if len(filtered_sales) == len(sales):
        return result
    if not filtered_sales:
        return None

    prices = [float(sale["price"]) for sale in filtered_sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(filtered_sales)
    if suggested is None:
        return None

    filtered = dict(result)
    filtered_raw = dict(raw)
    filtered_raw["sales"] = filtered_sales
    filtered_raw["sample_count"] = len(prices)
    filtered["raw"] = filtered_raw
    filtered["market_price"] = suggested
    filtered["low_price"] = round(min(prices), 2) if prices else None
    filtered["high_price"] = round(max(prices), 2) if prices else None
    return filtered


def _slab_sale_matches_item_identity(item: InventoryItem, sale: dict[str, Any]) -> bool:
    title = str(sale.get("title") or "")
    if not title.strip():
        return True
    expected_tokens = _slab_identity_tokens(item.card_name)
    if not expected_tokens:
        return True
    title_tokens = set(_slab_identity_tokens(title, from_title=True))
    if not title_tokens:
        return True
    required = min(2, len(expected_tokens))
    return sum(1 for token in expected_tokens if token in title_tokens) >= required


def _slab_identity_tokens(value: Any, *, from_title: bool = False) -> list[str]:
    text = html_lib.unescape(str(value or "").lower())
    text = re.sub(r"[^a-z0-9]+", " ", text)
    noise = {
        "the",
        "and",
        "card",
        "cards",
        "game",
        "tcg",
        "trading",
        "foil",
        "holo",
        "rare",
        "secret",
        "alternate",
        "alternative",
        "parallel",
        "manga",
        "wanted",
        "poster",
        "special",
        "art",
        "psa",
        "bgs",
        "cgc",
        "sgc",
        "mint",
        "gem",
        "new",
        "listing",
    }
    if not from_title:
        noise.update({"ex", "v", "vmax", "vstar", "sr", "sec", "sp"})
    tokens: list[str] = []
    for token in text.split():
        if len(token) < 3 or token.isdigit() or token in noise:
            continue
        if token not in tokens:
            tokens.append(token)
    return tokens[:5]


def _slab_sale_matches_item_variant(item: InventoryItem, sale: dict[str, Any]) -> bool:
    game = str(item.game or "").strip().lower()
    if game not in {"one piece", "one piece card game"}:
        return True

    expected = _one_piece_variant_markers(
        " ".join(str(part or "") for part in (item.card_name, item.variant))
    )
    found = _one_piece_variant_markers(str(sale.get("title") or ""))
    if not expected:
        return not found

    if "manga" in expected:
        return "manga" in found
    if "manga" in found:
        return False

    if "sp" in expected:
        return "sp" in found
    if "sp" in found:
        return False

    if "wanted" in expected:
        return "wanted" in found
    if "wanted" in found:
        return False

    if "alt" in expected:
        return "alt" in found
    if "alt" in found:
        return False

    return True


def _one_piece_variant_markers(value: str) -> set[str]:
    text = html_lib.unescape(str(value or "").lower())
    text = re.sub(r"[^a-z0-9]+", " ", text)
    markers: set[str] = set()
    if re.search(r"\bmanga\b", text):
        markers.add("manga")
    if re.search(r"\bsp\b|\bspecial\b", text):
        markers.add("sp")
    if "wanted poster" in text:
        markers.add("wanted")
    if re.search(r"\balternate art\b|\balternative art\b|\balt art\b|\baa\b|\bparallel\b", text):
        markers.add("alt")
    return markers


def _clean_sources(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(source).strip() for source in value if str(source or "").strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split("+") if part.strip()]
    return []


def _dedupe_slab_sales(sales: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    order: list[tuple[str, ...]] = []
    for sale in sales:
        key = _slab_sale_dedupe_key(sale)
        if key not in merged:
            merged[key] = dict(sale)
            merged[key]["sources"] = _clean_sources(sale.get("sources"))
            merged[key]["source_details"] = _clean_sources(sale.get("source_details"))
            order.append(key)
            continue
        current = merged[key]
        current["sources"] = sorted(set(_clean_sources(current.get("sources")) + _clean_sources(sale.get("sources"))))
        current["source_details"] = sorted(
            set(_clean_sources(current.get("source_details")) + _clean_sources(sale.get("source_details")))
        )
        current["source"] = "+".join(current["sources"])
        if not current.get("url") and sale.get("url"):
            current["url"] = sale.get("url")
        if not current.get("image_url") and sale.get("image_url"):
            current["image_url"] = sale.get("image_url")
        if len(str(sale.get("title") or "")) > len(str(current.get("title") or "")):
            current["title"] = sale.get("title")
    rows = [merged[key] for key in order]
    rows.sort(key=_slab_sale_sort_key, reverse=True)
    for row in rows:
        row["sources"] = sorted(set(_clean_sources(row.get("sources"))))
        row["source_details"] = sorted(set(_clean_sources(row.get("source_details"))))
        row["source"] = "+".join(row["sources"])
    return rows


def _slab_sale_dedupe_key(sale: dict[str, Any]) -> tuple[str, ...]:
    url_key = _normalized_listing_url(sale.get("url"))
    if url_key:
        return ("url", url_key)
    date = str(sale.get("sold_date") or sale.get("date") or "").strip().lower()
    price = _safe_float(sale.get("price"))
    price_key = f"{price:.2f}" if price is not None else ""
    title_key = _normalized_sale_title(sale.get("title"))
    platform = str(sale.get("platform") or "").strip().lower()
    return ("listing", date, price_key, platform, title_key)


def _normalized_listing_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    ebay = re.search(r"(?:itm/|item=)(\d{9,15})", raw, flags=re.IGNORECASE)
    if ebay:
        return f"ebay:{ebay.group(1)}"
    cleaned = re.sub(r"[?#].*$", "", raw.lower())
    cleaned = cleaned.replace("https://", "").replace("http://", "").replace("www.", "")
    return cleaned.rstrip("/")


def _normalized_sale_title(value: Any) -> str:
    text = html_lib.unescape(str(value or "").lower())
    text = text.replace("opens in a new window or tab", " ")
    text = re.sub(r"\bnew listing\b", " ", text)
    tokens = re.findall(r"[a-z0-9]+", text)
    return " ".join(tokens[:14])


def _slab_sale_sort_key(sale: dict[str, Any]) -> tuple[str, float]:
    date = str(sale.get("sold_date") or sale.get("date") or "")
    iso = re.match(r"^(\d{4}-\d{2}-\d{2})", date)
    date_key = iso.group(1) if iso else date
    return (date_key, float(_safe_float(sale.get("price")) or 0.0))


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

async def fetch_price_for_item(
    item: InventoryItem,
    client: httpx.AsyncClient,
    *,
    api_key: str = "",
    base_url: str = "",
) -> Optional[dict[str, Any]]:
    """Dispatch to the correct pricing source based on item_type."""
    if item.item_type == ITEM_TYPE_SLAB:
        return await fetch_slab_price(item, client)
    if item.item_type == ITEM_TYPE_SEALED:
        return await _fetch_tcgtracking_sealed_price(item, client)
    if item.item_type == ITEM_TYPE_SINGLE:
        tcgtracking_result = await _fetch_tcgtracking_single_price(item, client)
        if tcgtracking_result is not None:
            return tcgtracking_result
    return await fetch_single_price(item, client, api_key=api_key, base_url=base_url)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(value))
        if not cleaned:
            return None
        f = float(cleaned)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def price_result_to_json(result: dict[str, Any]) -> str:
    """Serialise a price result dict to JSON string for storage in raw_response_json."""
    return json.dumps(result.get("raw") or result, default=str, separators=(",", ":"))


def apply_slab_resticker_alert(
    item: InventoryItem,
    *,
    suggested_price: Any,
    previous_effective_price: Optional[float] = None,
    min_percent: float = 10.0,
    min_dollars: float = 10.0,
    source: str = "card_ladder",
) -> str:
    """
    Mutate a slab item with a resticker alert when market moves above sticker.

    Returns "created", "updated", "cleared", or "none" so callers can decide
    whether to emit notifications.
    """
    if item.item_type != ITEM_TYPE_SLAB:
        return "none"
    target = _safe_float(suggested_price)
    if target is None:
        return "none"
    dismissed_target = _safe_float(item.resticker_alert_price)
    if (
        not item.resticker_alert_active
        and item.resticker_resolved_at is not None
        and dismissed_target is not None
        and target - dismissed_target < max(0.0, min_dollars)
    ):
        return "none"

    reference = _sticker_reference_price(item, previous_effective_price=previous_effective_price)
    if reference is None:
        return "none"

    increase = round(target - reference, 2)
    percent = (increase / reference * 100.0) if reference > 0 else 0.0
    if increase < max(0.0, min_dollars) or percent < max(0.0, min_percent):
        if item.resticker_alert_active and target <= reference:
            clear_slab_resticker_alert(item, reason="Sticker price caught up to the latest slab comp.")
            return "cleared"
        return "none"

    was_active = bool(item.resticker_alert_active)
    previous_alert_price = _safe_float(item.resticker_alert_price)
    should_notify_update = (
        was_active
        and previous_alert_price is not None
        and target - previous_alert_price >= max(0.0, min_dollars)
    )

    item.resticker_alert_active = True
    item.resticker_alerted_at = utcnow()
    item.resticker_resolved_at = None
    item.resticker_reference_price = round(reference, 2)
    item.resticker_alert_price = round(target, 2)
    item.resticker_alert_reason = (
        f"{source.replace('_', ' ').title()} suggested ${target:,.2f}, "
        f"up ${increase:,.2f} ({percent:.1f}%) from sticker ${reference:,.2f}."
    )
    if not was_active:
        return "created"
    return "updated" if should_notify_update else "none"


def clear_slab_resticker_alert(item: InventoryItem, *, reason: str = "") -> None:
    item.resticker_alert_active = False
    item.resticker_resolved_at = utcnow()
    if reason:
        item.resticker_alert_reason = reason


def _sticker_reference_price(
    item: InventoryItem,
    *,
    previous_effective_price: Optional[float],
) -> Optional[float]:
    for candidate in (item.list_price, previous_effective_price, item.resticker_reference_price):
        value = _safe_float(candidate)
        if value is not None:
            return round(value, 2)
    return None
