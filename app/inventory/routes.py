"""
Inventory management routes.

All routes require at minimum 'viewer' role. Mutations (add, edit, reprice,
push-to-shopify) require 'reviewer' or above.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote, urlencode

import httpx
from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlalchemy import case
from sqlmodel import Session, select, func

# Reuse the shared Jinja2Templates instance so custom filters registered in
# app/shared.py (e.g. `money`, `pacific_datetime`) are available in
# inventory templates as well. A separate instance would have an empty
# filter env and any {{ x | money }} in the template would 500.
from ..shared import templates as _templates

from ..auth import has_legacy_role, has_permission, has_role
from .card_scanner import identify_card_from_image, lookup_card_image_and_price
from .cert_lookup import lookup_cert
from .pokemon_scanner import (
    TCGTRACKING_BASE,
    TCGTRACKING_HEADERS,
    _heuristic_parse_query,
    fetch_tcg_categories,
    get_scan_history,
    get_validation_result,
    run_pipeline as run_pokemon_pipeline,
    text_search_cards,
)
from .degen_eye_v2 import get_v2_scan_history, run_v2_pipeline, run_v2_pipeline_stream
from .degen_eye_v2_training import (
    attach_confirmed_label,
    attach_prediction,
    capture_stats as training_capture_stats,
    create_scan_capture,
    train_confirmed_captures,
)
from .phash_scanner import (
    get_index_stats as phash_index_stats,
    has_index as phash_has_index,
    reload_index as phash_reload_index,
)
from .price_cache import get_warm_stats as price_cache_stats, warm_price_cache
from ..config import get_settings
from ..csrf import CSRFProtectedRoute, issue_token
from ..db import get_session, managed_session
from .barcode import (
    generate_barcode_value,
    label_context_for_items,
    render_barcode_svg,
)
from .pricing import (
    SLAB_PRICE_SOURCE_OPTIONS,
    alt_cli_status,
    build_card_ladder_cli_query,
    card_ladder_cli_status,
    clear_slab_resticker_alert,
    effective_price,
    fetch_price_for_item,
    fetch_slab_price,
    fetch_ximilar_slab_price_from_image,
    import_card_ladder_cli_records_for_item,
    normalize_slab_price_source,
    point130_cli_status,
    sync_alt_cli_for_item,
    sync_card_ladder_cli_for_item,
)
from .price_updates import record_inventory_price_result
from .shopify import (
    apply_shopify_variant_ref,
    list_shopify_product_variants,
    resolve_shopify_access_token,
    shopify_admin_configured,
)
from ..shopify_sync import (
    SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT,
    SHOPIFY_SYNC_ERROR,
    SHOPIFY_SYNC_ISSUE_IGNORED,
    SHOPIFY_SYNC_ISSUE_LINKED,
    SHOPIFY_SYNC_ISSUE_OPEN,
    SHOPIFY_SYNC_ISSUE_RESOLVED,
    SHOPIFY_SYNC_LINKED,
    enqueue_shopify_sync_job,
    record_shopify_sync_issue,
)
from ..shopify_sync_worker import sync_inventory_item_to_shopify
from ..models import (
    GAMES,
    CONDITIONS,
    GRADING_COMPANIES,
    INVENTORY_IN_STOCK,
    INVENTORY_LISTED,
    INVENTORY_SOLD,
    INVENTORY_HELD,
    ITEM_TYPE_SINGLE,
    ITEM_TYPE_SLAB,
    ITEM_TYPE_SEALED,
    ALL_INVENTORY_STATUSES,
    InventoryItem,
    InventoryStockMovement,
    PriceHistory,
    ShopifySyncIssue,
    ShopifySyncJob,
    utcnow,
)

router = APIRouter(route_class=CSRFProtectedRoute)
settings = get_settings()
logger = logging.getLogger(__name__)

# Rate-limit bucket for /degen_eye/client_log: {username: [timestamps]}
_CLIENT_LOG_RATE: dict[str, list[float]] = {}

PAGE_SIZE = 50
PRICE_MARKUP_ACTIONS = {
    "reprice": 0.0,
    "reprice_5": 5.0,
    "reprice_10": 10.0,
}


def _inventory_price_gap_thresholds() -> tuple[float, float]:
    min_percent = max(float(getattr(settings, "inventory_price_review_threshold_percent", 10.0) or 0.0), 0.0)
    min_dollars = max(float(getattr(settings, "inventory_price_review_threshold_dollars", 5.0) or 0.0), 0.0)
    return min_percent, min_dollars


def _inventory_price_review_sql_condition():
    min_percent, min_dollars = _inventory_price_gap_thresholds()
    min_ratio = min_percent / 100.0
    under_market_gap = InventoryItem.auto_price - InventoryItem.list_price
    return (
        (InventoryItem.list_price != None)  # noqa: E711
        & (InventoryItem.auto_price != None)  # noqa: E711
        & (InventoryItem.auto_price > 0)
        & (under_market_gap >= min_dollars)
        & ((under_market_gap / InventoryItem.auto_price) >= min_ratio)
    )


def _inventory_price_review_context(item: InventoryItem) -> dict[str, Any]:
    current_price = effective_price(item)
    market_price = item.auto_price
    min_percent, min_dollars = _inventory_price_gap_thresholds()
    delta = None
    percent = None
    needs_review = False
    direction = ""
    if current_price is not None and market_price is not None and market_price > 0:
        delta = round(float(current_price) - float(market_price), 2)
        under_market_gap = round(float(market_price) - float(current_price), 2)
        percent = round((under_market_gap / float(market_price)) * 100.0, 1) if under_market_gap > 0 else 0.0
        needs_review = under_market_gap >= min_dollars and percent >= min_percent
        if needs_review:
            direction = "below"

    stale = False
    stale_hours = max(float(getattr(settings, "inventory_price_stale_hours", 24.0) or 24.0), 1.0)
    if item.status in {INVENTORY_IN_STOCK, INVENTORY_LISTED} and item.archived_at is None:
        last_priced = item.last_priced_at
        if last_priced is None:
            stale = True
        else:
            if last_priced.tzinfo is None:
                last_priced = last_priced.replace(tzinfo=timezone.utc)
            stale = last_priced < (utcnow() - timedelta(hours=stale_hours))

    return {
        "current_price": current_price,
        "market_price": market_price,
        "delta": delta,
        "delta_abs": abs(delta) if delta is not None and delta < 0 else None,
        "percent": percent,
        "needs_review": needs_review,
        "direction": direction,
        "stale": stale,
    }


def _inventory_url_with_params(request: Request, updates: dict[str, Any]) -> str:
    params = dict(request.query_params)
    for key, value in updates.items():
        if value is None or value == "":
            params.pop(key, None)
        else:
            params[key] = str(value)
    if not params:
        return request.url.path
    return f"{request.url.path}?{urlencode(params)}"


def _inventory_price_from_market(item: InventoryItem, markup_percent: float = 0.0) -> Optional[float]:
    market_price = _safe_price(item.auto_price)
    if market_price is None or market_price <= 0:
        return None
    markup = max(float(markup_percent or 0.0), 0.0)
    return round(market_price * (1.0 + (markup / 100.0)), 2)

SLAB_GRADE_OPTIONS: dict[str, tuple[str, ...]] = {
    "PSA": ("10", "9", "8", "7"),
    "SGC": ("10", "9.5", "9", "8.5", "8"),
    "CGC": ("10", "9.5", "9", "8.5", "8"),
    "BGS": ("10", "9.5", "9", "8.5", "8"),
}


def _slab_grade_options(grading_company: str | None, preferred_grade: str = "") -> tuple[str, ...]:
    company = (grading_company or "PSA").strip().upper()
    grades = list(SLAB_GRADE_OPTIONS.get(company) or SLAB_GRADE_OPTIONS["PSA"])
    preferred = preferred_grade.strip()
    if preferred and preferred not in grades:
        grades.insert(0, preferred)
    return tuple(grades[:5])

SEALED_PRODUCT_KINDS: tuple[str, ...] = (
    "Booster Box Case",
    "Booster Box",
    "Booster Display Case",
    "Booster Display",
    "Collector Booster Box",
    "Play Booster Box",
    "Draft Booster Box",
    "Set Booster Box",
    "Booster Bundle",
    "Bundle",
    "Booster Pack",
    "Sleeved Booster Pack",
    "Blister Pack",
    "Elite Trainer Box",
    "Pokemon Center Elite Trainer Box",
    "Ultra Premium Collection",
    "Super Premium Collection",
    "Build & Battle Box",
    "Collection Box",
    "Illumineer's Trove",
    "Gift Set",
    "Tin",
    "Commander Deck",
    "Structure Deck",
    "Starter Kit",
    "Starter Deck",
    "Battle Deck",
    "Deck",
    "Prerelease Kit",
    "Other",
)

_SEALED_KIND_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Pokemon Center Elite Trainer Box", ("pokemon center elite trainer box", "pokemon center etb", "pc etb")),
    ("Elite Trainer Box", ("elite trainer box", "etb")),
    ("Ultra Premium Collection", ("ultra premium collection", "upc")),
    ("Super Premium Collection", ("super premium collection", "super-premium collection", "superpremium collection", "superpremium", "super premium", "spc")),
    ("Collector Booster Box", ("collector booster box", "collector booster display", "collector booster")),
    ("Play Booster Box", ("play booster box", "play booster display")),
    ("Draft Booster Box", ("draft booster box", "draft booster display")),
    ("Set Booster Box", ("set booster box", "set booster display")),
    ("Booster Box Case", ("booster box case", "booster case")),
    ("Booster Display Case", ("booster display case", "display case")),
    ("Booster Display", ("booster display",)),
    ("Booster Box", ("booster box",)),
    ("Booster Bundle", ("booster bundle",)),
    ("Bundle", ("bundle",)),
    ("Sleeved Booster Pack", ("sleeved booster pack",)),
    ("Build & Battle Box", ("build and battle box", "build & battle box", "build battle box")),
    ("Blister Pack", ("3 pack blister", "three pack blister", "single pack blister", "blister pack", "blister")),
    ("Booster Pack", ("booster pack", "pack", "packs")),
    ("Collection Box", ("collection box", "special collection", "premium collection", "poster collection", "binder collection", "ex box", "v box", "v union")),
    ("Illumineer's Trove", ("illumineer's trove", "illumineer s trove", "illumineers trove", "trove")),
    ("Gift Set", ("gift set",)),
    ("Tin", ("tin", "mini tin")),
    ("Commander Deck", ("commander deck", "commander precon", "precon")),
    ("Structure Deck", ("structure deck",)),
    ("Battle Deck", ("battle deck",)),
    ("Starter Kit", ("starter kit", "intro kit")),
    ("Starter Deck", ("starter deck", "theme deck")),
    ("Deck", ("deck", "preconstructed deck")),
    ("Prerelease Kit", ("prerelease kit", "pre-release kit", "pre release kit", "prerelease pack", "release event deck")),
)
_SEALED_SEARCH_REMOVE_TERMS: tuple[str, ...] = tuple(
    alias
    for _kind, aliases in _SEALED_KIND_ALIASES
    for alias in aliases
)
_SEALED_GENERIC_QUERY_TOKENS = {"pokemon", "tcg", "sealed", "product", "products", "s"}
_SEALED_SEARCH_NOISE_TOKENS = _SEALED_GENERIC_QUERY_TOKENS | {
    "card",
    "cards",
    "english",
    "game",
    "games",
    "language",
    "trading",
}
_SEALED_EXCLUDE_TERMS = ("code card",)
_SEALED_KIND_COMPATIBLE_HINTS = {
    "Booster Box": {"Booster Display"},
    "Booster Display": {"Booster Box"},
    "Booster Box Case": {"Booster Display Case"},
    "Booster Display Case": {"Booster Box Case"},
}
_TCGPLAYER_PRODUCT_RE = re.compile(
    r"(?:https?://)?(?:www\.)?tcgplayer\.com/product/(\d+)(?:/([^\s?#]+))?",
    flags=re.IGNORECASE,
)
_ADD_STOCK_SEARCH_CACHE_TTL_SECONDS = 300.0
_ADD_STOCK_SEALED_CACHE: dict[str, tuple[float, list[dict[str, Any]], str]] = {}
_ADD_STOCK_SINGLE_CACHE: dict[str, tuple[float, list[dict[str, Any]], str]] = {}
_TCGTRACKING_SET_LIST_CACHE_TTL_SECONDS = 60 * 60 * 6
_TCGTRACKING_SEALED_CATALOG_CACHE_TTL_SECONDS = 60 * 60 * 6
_TCGTRACKING_SEALED_CATALOG_CATEGORY_PRODUCT_LIMIT = 50_000
_TCGTRACKING_SET_LIST_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_TCGTRACKING_SEALED_CATALOG_CACHE: dict[str, tuple[float, list[dict[str, Any]], str]] = {}
_TCGTRACKING_SEALED_CATALOG_LOCKS: dict[str, asyncio.Lock] = {}
ADD_STOCK_SEARCH_TYPE_OPTIONS: tuple[dict[str, str], ...] = (
    {"value": "both", "label": "Cards + Sealed"},
    {"value": "cards", "label": "Cards"},
    {"value": "sealed", "label": "Sealed"},
)
_ADD_STOCK_SEARCH_TYPE_VALUES = {option["value"] for option in ADD_STOCK_SEARCH_TYPE_OPTIONS}

ADD_STOCK_GAME_OPTIONS: tuple[dict[str, Any], ...] = (
    {"game": "Pokemon", "label": "Pokemon", "category_ids": ("3", "85"), "single_category_id": "3"},
    {"game": "Pokemon Japan", "label": "Pokemon Japan", "category_ids": ("85",), "single_category_id": "85"},
    {"game": "Magic", "label": "Magic", "category_ids": ("1",), "single_category_id": "1"},
    {"game": "Yu-Gi-Oh", "label": "Yu-Gi-Oh", "category_ids": ("2",), "single_category_id": "2"},
    {"game": "One Piece", "label": "One Piece", "category_ids": ("68",), "single_category_id": "68"},
    {"game": "Lorcana", "label": "Lorcana", "category_ids": ("71",), "single_category_id": "71"},
    {"game": "Riftbound", "label": "Riftbound", "category_ids": ("89",), "single_category_id": "89"},
    {"game": "Dragon Ball", "label": "Dragon Ball", "category_ids": ("80", "27", "23"), "single_category_id": "80"},
    {"game": "Digimon", "label": "Digimon", "category_ids": ("63",), "single_category_id": "63"},
    {"game": "Flesh and Blood", "label": "Flesh and Blood", "category_ids": ("62",), "single_category_id": "62"},
    {"game": "Weiss Schwarz", "label": "Weiss Schwarz", "category_ids": ("20",), "single_category_id": "20"},
    {"game": "Cardfight Vanguard", "label": "Cardfight Vanguard", "category_ids": ("16",), "single_category_id": "16"},
    {"game": "Union Arena", "label": "Union Arena", "category_ids": ("81",), "single_category_id": "81"},
    {"game": "Other", "label": "Other", "category_ids": (), "single_category_id": ""},
)
_ADD_STOCK_GAME_BY_NAME = {str(option["game"]).lower(): option for option in ADD_STOCK_GAME_OPTIONS}
_ADD_STOCK_GAME_ALIASES = {
    "pokemon jp": "Pokemon Japan",
    "pokemon japan": "Pokemon Japan",
    "japanese pokemon": "Pokemon Japan",
    "jp pokemon": "Pokemon Japan",
    "mtg": "Magic",
    "magic the gathering": "Magic",
    "magic: the gathering": "Magic",
    "yugioh": "Yu-Gi-Oh",
    "yu gi oh": "Yu-Gi-Oh",
    "yu-gi-oh": "Yu-Gi-Oh",
    "op": "One Piece",
    "opcg": "One Piece",
    "one piece card game": "One Piece",
    "disney lorcana": "Lorcana",
    "league of legends": "Riftbound",
    "league of legends tcg": "Riftbound",
    "lol tcg": "Riftbound",
    "dragon ball super": "Dragon Ball",
    "dbs": "Dragon Ball",
    "fab": "Flesh and Blood",
    "flesh & blood": "Flesh and Blood",
    "weiss": "Weiss Schwarz",
    "vanguard": "Cardfight Vanguard",
}
_ADD_STOCK_CATEGORY_TO_GAME = {
    str(category_id): str(option["game"])
    for option in ADD_STOCK_GAME_OPTIONS
    for category_id in option["category_ids"]
}
_ADD_STOCK_GAME_SEARCH_NOISE_TOKENS: dict[str, set[str]] = {
    "Pokemon Japan": {"pokemon", "pokémon", "japan", "japanese", "jp", "card", "cards", "tcg"},
    "Magic": {"magic", "the", "gathering", "mtg"},
    "Yu-Gi-Oh": {"yugioh", "yu", "gi", "oh"},
    "One Piece": {"one", "piece", "card", "game"},
    "Lorcana": {"disney", "lorcana"},
    "Riftbound": {"riftbound", "league", "of", "legends", "lol"},
    "Dragon Ball": {"dragon", "ball", "super", "fusion", "world"},
    "Flesh and Blood": {"flesh", "blood", "fab"},
    "Weiss Schwarz": {"weiss", "schwarz"},
    "Cardfight Vanguard": {"cardfight", "vanguard"},
    "Union Arena": {"union", "arena"},
}



def _get_user(request: Request):
    """Look up the current user directly from the session.

    Starlette 1.0's BaseHTTPMiddleware doesn't reliably share
    request.state between middleware and route handlers, so we
    read the session ourselves instead of relying on the
    attach_current_user middleware.
    """
    from ..shared import get_request_user
    return get_request_user(request)


def _check_role(request: Request, min_role: str) -> Optional[Response]:
    """Return a redirect/403 if the current user doesn't have min_role; None if ok."""
    user = _get_user(request)
    if not user:
        next_path = request.url.path
        return RedirectResponse(url=f"/login?next={next_path}", status_code=303)
    if min_role in {"viewer", "reviewer", "admin"}:
        try:
            with managed_session() as session:
                allowed = has_legacy_role(session, user, min_role)
        except Exception:
            allowed = has_legacy_role(None, user, min_role)
    else:
        allowed = has_role(user, min_role)
    if not allowed:
        return HTMLResponse("You do not have permission to view this page.", status_code=403)
    return None


def _require_viewer(request: Request) -> Optional[Response]:
    return _check_role(request, "viewer")


def _require_employee(request: Request) -> Optional[Response]:
    # Used for scanner / Degen Eye routes that are safe for rank-and-file
    # employees to use on the buy counter. These routes show public market
    # prices and buy-offer calculators but NOT internal cost basis / margins.
    return _check_role(request, "employee")


def _require_employee_permission(
    request: Request,
    resource_key: str,
    session: Optional[Session] = None,
) -> Optional[Response]:
    if denial := _require_employee(request):
        return denial
    user = _current_user(request)
    if not user:
        next_path = request.url.path
        return RedirectResponse(url=f"/login?next={next_path}", status_code=303)
    try:
        if session is not None:
            allowed = has_permission(session, user, resource_key)
        else:
            with managed_session() as permission_session:
                allowed = has_permission(permission_session, user, resource_key)
    except Exception:
        allowed = False
    if not allowed:
        return HTMLResponse("You do not have permission to view this page.", status_code=403)
    return None


def _can_inventory_manage(request: Request, session: Session) -> bool:
    user = _current_user(request)
    if not user:
        return False
    try:
        return has_permission(session, user, "ops.inventory.manage")
    except Exception:
        return False


def _can_inventory_view(request: Request, session: Session) -> bool:
    user = _current_user(request)
    if not user:
        return False
    try:
        return has_permission(session, user, "ops.inventory.view")
    except Exception:
        return False


def _require_inventory_manage(request: Request, session: Session) -> Optional[Response]:
    if denial := _require_employee(request):
        return denial
    if not _can_inventory_manage(request, session):
        return HTMLResponse("You do not have permission to manage inventory.", status_code=403)
    return None


def _require_reviewer(request: Request) -> Optional[Response]:
    return _check_role(request, "reviewer")


def _current_user(request: Request):
    return _get_user(request)


def _current_user_label(request: Request) -> Optional[str]:
    user = _current_user(request)
    if not user:
        return None
    return (
        getattr(user, "display_name", None)
        or getattr(user, "username", None)
        or str(getattr(user, "id", ""))
        or None
    )


def _safe_inventory_return_url(value: str | None, fallback: str) -> str:
    target = (value or "").strip()
    if target.startswith("/inventory") and not target.startswith("//") and "://" not in target:
        return target
    return fallback


def _normalize_lookup(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def _card_ladder_history_context(history: list[PriceHistory]) -> dict[str, Any]:
    for row in history:
        if row.source != "card_ladder":
            continue
        try:
            payload = json.loads(row.raw_response_json or "{}")
        except json.JSONDecodeError:
            continue
        sales = payload.get("sales") if isinstance(payload, dict) else None
        if not isinstance(sales, list):
            continue
        return {
            "sales": [sale for sale in sales[:8] if isinstance(sale, dict)],
            "sales_history_url": payload.get("sales_history_url") if isinstance(payload, dict) else "",
            "sample_count": payload.get("sample_count") if isinstance(payload, dict) else None,
            "fetched_at": row.fetched_at,
        }
    return {"sales": [], "sales_history_url": "", "sample_count": None, "fetched_at": None}


def _normalize_add_stock_game(value: str | None) -> str:
    raw = _normalize_lookup(value).replace("&", "and")
    if not raw:
        return "Pokemon"
    alias = _ADD_STOCK_GAME_ALIASES.get(raw)
    if alias:
        return alias
    for option in ADD_STOCK_GAME_OPTIONS:
        game = str(option["game"])
        label = str(option["label"])
        if raw in {_normalize_lookup(game).replace("&", "and"), _normalize_lookup(label).replace("&", "and")}:
            return game
    return "Pokemon"


def _normalize_add_stock_search_type(value: str | None) -> str:
    raw = _normalize_lookup(value).replace("_", "-")
    aliases = {
        "all": "both",
        "any": "both",
        "auto": "both",
        "card": "cards",
        "single": "cards",
        "singles": "cards",
        "sealed product": "sealed",
        "sealed products": "sealed",
        "product": "sealed",
        "products": "sealed",
    }
    normalized = aliases.get(raw, raw)
    return normalized if normalized in _ADD_STOCK_SEARCH_TYPE_VALUES else "both"


def _add_stock_game_option(game: str | None) -> dict[str, Any]:
    selected_game = _normalize_add_stock_game(game)
    return _ADD_STOCK_GAME_BY_NAME.get(selected_game.lower(), _ADD_STOCK_GAME_BY_NAME["pokemon"])


def _add_stock_category_ids_for_game(game: str | None) -> tuple[str, ...]:
    option = _add_stock_game_option(game)
    return tuple(str(category_id) for category_id in option.get("category_ids") or ())


def _add_stock_single_category_id(game: str | None) -> str:
    option = _add_stock_game_option(game)
    return str(option.get("single_category_id") or "")


def _add_stock_game_for_category_id(category_id: str | int | None) -> str:
    return _ADD_STOCK_CATEGORY_TO_GAME.get(str(category_id or ""), "Other")


def _add_stock_existing_game_values(game: str | None) -> tuple[str, ...]:
    selected_game = _normalize_add_stock_game(game)
    if selected_game == "Magic":
        return ("Magic", "MTG")
    return (selected_game,)


def _slab_search_fallback_suggestion(query: str, game: str = "Pokemon") -> Optional[dict[str, Any]]:
    selected_game = _normalize_add_stock_game(game)
    if selected_game != "Pokemon":
        return None
    parsed = _heuristic_parse_query(query, "3")
    card_name = (parsed.card_name or query).strip()
    if not card_name:
        return None
    return {
        "name": card_name,
        "game": selected_game,
        "set_name": (parsed.set_name or "").strip(),
        "set_code": "",
        "card_number": (parsed.collector_number or "").strip(),
        "image_url": "",
        "tcgplayer_url": "",
        "variants": [],
        "default_variant": "",
        "default_condition": "NM",
        "default_price": None,
        "lookup_fallback": True,
    }


def _normalize_product_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9&]+", " ", (value or "").lower())).strip()


def _sealed_catalog_key(product: dict[str, str]) -> str:
    return "|".join(
        [
            _normalize_lookup(product.get("name")),
            _normalize_lookup(product.get("set_name")),
            _normalize_lookup(product.get("upc")),
        ]
    )


def _sealed_kind_from_name(product_name: str) -> str:
    product_norm = _normalize_product_text(product_name).replace("&", "and")
    for kind, aliases in _SEALED_KIND_ALIASES:
        if any(alias.replace("&", "and") in product_norm for alias in aliases):
            return kind
    return ""


def _sealed_kind_hints_from_query(query: str) -> set[str]:
    query_norm = _normalize_product_text(query).replace("&", "and")
    hints: set[str] = set()
    for kind, aliases in _SEALED_KIND_ALIASES:
        if any(alias.replace("&", "and") in query_norm for alias in aliases):
            hints.add(kind)
    return hints


def _tcgplayer_product_ids_from_query(query: str | None) -> set[str]:
    decoded = unquote(query or "")
    return {match.group(1) for match in _TCGPLAYER_PRODUCT_RE.finditer(decoded) if match.group(1)}


def _tcgplayer_slug_terms_from_query(query: str | None) -> list[str]:
    decoded = unquote(query or "")
    terms: list[str] = []
    for match in _TCGPLAYER_PRODUCT_RE.finditer(decoded):
        slug = (match.group(2) or "").strip()
        if not slug:
            continue
        term = slug.replace("-", " ")
        if term and term not in terms:
            terms.append(term)
    return terms


def _sealed_kind_matches_query_hint(
    kind: str,
    query_kind_hints: set[str],
    product_norm: str,
) -> bool:
    if not query_kind_hints:
        return True
    if kind in query_kind_hints:
        return True
    for hint in query_kind_hints:
        if kind in _SEALED_KIND_COMPATIBLE_HINTS.get(hint, set()):
            return True
        if _normalize_product_text(hint) in product_norm:
            return True
    return False


def _add_stock_cache_key(
    query: str,
    *,
    game: str | None = None,
    category_id: str | None = None,
    limit: Optional[int] = None,
) -> str:
    key = _normalize_lookup(query)
    parts = []
    if game is not None:
        parts.append(_normalize_add_stock_game(game))
    if category_id:
        parts.append(str(category_id))
    if limit is not None:
        parts.append(str(limit))
    parts.append(key)
    return ":".join(parts)


def _add_stock_cache_is_fresh(created_at: float) -> bool:
    return (time.monotonic() - created_at) < _ADD_STOCK_SEARCH_CACHE_TTL_SECONDS


def _add_stock_query_looks_sealed(query: str) -> bool:
    query_norm = _normalize_product_text(query).replace("&", "and")
    if not query_norm:
        return False
    if _sealed_kind_hints_from_query(query_norm):
        return True
    sealed_terms = (
        "booster",
        "bundle",
        "box",
        "case",
        "collection",
        "display",
        "elite trainer",
        "etb",
        "pack",
        "packs",
        "sleeved",
        "spc",
        "super premium",
        "tin",
        "ultra premium",
        "upc",
    )
    return any(term in query_norm for term in sealed_terms)


def _strip_sealed_product_terms(query: str) -> str:
    stripped = f" {_normalize_lookup(query)} "
    for term in sorted(_SEALED_SEARCH_REMOVE_TERMS, key=len, reverse=True):
        stripped = re.sub(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def _sealed_set_search_queries(query: str, *, game: str = "Pokemon") -> list[str]:
    query_clean = _normalize_lookup(query)
    if len(query_clean) < 2:
        return []

    candidates: list[str] = []
    selected_game = _normalize_add_stock_game(game)
    noise_tokens = _SEALED_SEARCH_NOISE_TOKENS | _ADD_STOCK_GAME_SEARCH_NOISE_TOKENS.get(selected_game, set())

    def add(candidate: str) -> None:
        candidate = re.sub(r"\s+", " ", candidate).strip(" -:/")
        if len(candidate) >= 2 and candidate not in candidates:
            candidates.append(candidate)

    query_variants: list[str] = []
    for slug_term in _tcgplayer_slug_terms_from_query(query):
        slug_clean = _normalize_lookup(slug_term)
        if slug_clean and slug_clean not in query_variants:
            query_variants.append(slug_clean)
    if query_clean not in query_variants:
        query_variants.append(query_clean)

    for query_variant in query_variants:
        add(query_variant)
        stripped = _strip_sealed_product_terms(query_variant)
        add(stripped)
        generic_stripped = re.sub(r"[^a-z0-9]+", " ", stripped)
        generic_tokens = [tok for tok in generic_stripped.split() if tok not in noise_tokens]
        deduped_tokens: list[str] = []
        for token in generic_tokens:
            if deduped_tokens and deduped_tokens[-1] == token:
                continue
            deduped_tokens.append(token)
        add(" ".join(deduped_tokens))
        for size in range(min(4, len(deduped_tokens)), 1, -1):
            add(" ".join(deduped_tokens[:size]))
        for token in deduped_tokens:
            if (token.isdigit() and len(token) >= 2) or len(token) >= 5:
                add(token)
    return candidates[:8]


def _pokemon_set_search_queries(query: str) -> list[str]:
    return _sealed_set_search_queries(query, game="Pokemon")


def _tcgtracking_large_image(image_url: str | None) -> str:
    image = (image_url or "").strip()
    if not image:
        return ""
    return image.replace("_200w.jpg", "_400w.jpg")


def _tcgtracking_market_price(product_id: str, pricing: dict[str, Any] | None) -> Optional[float]:
    product_prices = (pricing or {}).get(str(product_id), {}).get("tcg", {})
    if not isinstance(product_prices, dict):
        return None
    preferred_order = ("Normal", "Holofoil", "Reverse Holofoil")
    subtype_items = list(product_prices.items())
    subtype_items.sort(
        key=lambda item: preferred_order.index(item[0]) if item[0] in preferred_order else len(preferred_order)
    )
    for _subtype_name, subtype_data in subtype_items:
        if not isinstance(subtype_data, dict):
            continue
        market = subtype_data.get("market")
        if market is None:
            continue
        try:
            return round(float(market), 2)
        except (TypeError, ValueError):
            continue
    return None


def _tcgtracking_sealed_price(
    product_id: str,
    pricing: dict[str, Any] | None,
) -> tuple[Optional[float], str]:
    product_prices = (pricing or {}).get(str(product_id), {}).get("tcg", {})
    if not isinstance(product_prices, dict):
        return None, ""
    preferred_order = ("Normal", "Holofoil", "Reverse Holofoil")
    subtype_items = list(product_prices.items())
    subtype_items.sort(
        key=lambda item: preferred_order.index(item[0]) if item[0] in preferred_order else len(preferred_order)
    )
    fallback_low: Optional[float] = None
    for _subtype_name, subtype_data in subtype_items:
        if not isinstance(subtype_data, dict):
            continue
        market = subtype_data.get("market")
        if market is not None:
            try:
                return round(float(market), 2), "TCGPlayer Market"
            except (TypeError, ValueError):
                pass
        if fallback_low is None and subtype_data.get("low") is not None:
            try:
                fallback_low = round(float(subtype_data.get("low")), 2)
            except (TypeError, ValueError):
                fallback_low = None
    if fallback_low is not None:
        return fallback_low, "TCGPlayer Low"
    return None, ""


def _tcgtracking_sealed_product(
    *,
    product: dict[str, Any],
    set_info: dict[str, Any],
    category_id: str,
    query_kind_hints: set[str],
    pricing: dict[str, Any] | None = None,
    game: str = "",
) -> Optional[dict[str, Any]]:
    product_name = str(product.get("clean_name") or product.get("name") or "").strip()
    if not product_name:
        return None
    product_norm = _normalize_product_text(product_name)
    if any(term in product_norm for term in _SEALED_EXCLUDE_TERMS):
        return None

    kind = _sealed_kind_from_name(product_name)
    if not kind:
        return None
    if not _sealed_kind_matches_query_hint(kind, query_kind_hints, product_norm):
        return None

    set_name = str(set_info.get("name") or set_info.get("set_name") or "").strip()
    set_id = str(set_info.get("id") or "").strip()
    external_id = str(product.get("id") or "").strip()
    image_url = _tcgtracking_large_image(str(product.get("image_url") or ""))
    market_price, market_price_source = _tcgtracking_sealed_price(external_id, pricing)
    return {
        "name": product_name,
        "set_name": set_name,
        "set_id": set_id,
        "kind": kind,
        "upc": "",
        "image_url": image_url,
        "image_url_small": str(product.get("image_url") or "").strip(),
        "source": "tcgtracking",
        "source_name": "TCGTracking",
        "external_id": external_id,
        "external_url": str(product.get("tcgplayer_url") or "").strip(),
        "category_id": str(category_id),
        "game": game.strip() or _add_stock_game_for_category_id(category_id),
        "market_price": market_price,
        "market_price_source": market_price_source,
    }


def _match_token(token: str, haystack: str) -> bool:
    if token in haystack:
        return True
    if len(token) > 3 and token.endswith("s") and token[:-1] in haystack:
        return True
    return False


def _sealed_product_query_score(query: str, product: dict[str, Any]) -> int:
    query_norm = _normalize_product_text(query).replace("&", "and")
    name_norm = _normalize_product_text(product.get("name")).replace("&", "and")
    set_norm = _normalize_product_text(product.get("set_name")).replace("&", "and")
    kind_norm = _normalize_product_text(product.get("kind")).replace("&", "and")
    haystack = f"{name_norm} {set_norm} {kind_norm}"
    tokens = [
        token
        for token in query_norm.split()
        if token not in _SEALED_SEARCH_NOISE_TOKENS and len(token) > 1
    ]

    score = 0
    kind_hints = _sealed_kind_hints_from_query(query)
    if kind_hints:
        if product.get("kind") in kind_hints:
            score += 30
        elif any(product.get("kind") in _SEALED_KIND_COMPATIBLE_HINTS.get(hint, set()) for hint in kind_hints):
            score += 18
        else:
            score -= 30
    product_ids = _tcgplayer_product_ids_from_query(query)
    if product_ids and str(product.get("external_id") or "") in product_ids:
        score += 100
    if query_norm and name_norm.startswith(query_norm):
        score += 28
    elif query_norm and f" {query_norm} " in f" {name_norm} ":
        score += 12
    for token in tokens:
        if _match_token(token, name_norm):
            score += 6
        elif _match_token(token, set_norm):
            score += 4
        elif _match_token(token, kind_norm):
            score += 2
        elif _match_token(token, haystack):
            score += 1

    if all(_match_token(token, haystack) for token in tokens):
        score += 8
    if " case" in f" {name_norm}" and "case" not in query_norm:
        score -= 12
    if " display" in f" {name_norm}" and "display" not in query_norm:
        score -= 8
    if "pokemon center" in name_norm and "pokemon center" not in query_norm and "pc" not in query_norm:
        score -= 8
    if "half booster box" in name_norm and "half" not in query_norm:
        score -= 10
    if name_norm == query_norm:
        score += 40
    return score


_SEALED_PRODUCT_KIND_QUERY_TOKENS = {
    "box",
    "booster",
    "bundle",
    "case",
    "collection",
    "deck",
    "display",
    "kit",
    "pack",
    "packs",
    "premium",
    "starter",
    "tin",
    "trove",
}


def _sealed_catalog_query_variants(query: str) -> list[str]:
    variants: list[str] = []
    for slug_term in _tcgplayer_slug_terms_from_query(query):
        slug_clean = _normalize_product_text(slug_term).replace("&", "and")
        if slug_clean and slug_clean not in variants:
            variants.append(slug_clean)
    query_clean = _normalize_product_text(query).replace("&", "and")
    if query_clean and query_clean not in variants:
        variants.append(query_clean)
    return variants


def _sealed_catalog_specific_tokens(query_variant: str) -> list[str]:
    return [
        token
        for token in _normalize_product_text(query_variant).replace("&", "and").split()
        if len(token) >= 4
        and not token.isdigit()
        and token not in _SEALED_SEARCH_NOISE_TOKENS
        and token not in _SEALED_PRODUCT_KIND_QUERY_TOKENS
    ]


def _sealed_catalog_product_match_score(query: str, product: dict[str, Any]) -> Optional[int]:
    product_ids = _tcgplayer_product_ids_from_query(query)
    external_id = str(product.get("external_id") or "").strip()
    if product_ids and external_id in product_ids:
        return 1000 + _sealed_product_query_score(query, product)

    name_norm = _normalize_product_text(product.get("name")).replace("&", "and")
    set_norm = _normalize_product_text(product.get("set_name")).replace("&", "and")
    kind_norm = _normalize_product_text(product.get("kind")).replace("&", "and")
    haystack = f"{name_norm} {set_norm} {kind_norm}"
    best_score: Optional[int] = None
    for query_variant in _sealed_catalog_query_variants(query):
        tokens = [
            token
            for token in query_variant.split()
            if len(token) > 1 and token not in _SEALED_SEARCH_NOISE_TOKENS
        ]
        specific_tokens = _sealed_catalog_specific_tokens(query_variant)
        if not tokens or not specific_tokens:
            continue
        if not all(_match_token(token, haystack) for token in tokens):
            continue
        if not any(_match_token(token, name_norm) for token in specific_tokens):
            continue
        score = _sealed_product_query_score(query_variant, product) + (len(specific_tokens) * 8)
        if best_score is None or score > best_score:
            best_score = score
    return best_score


def _sealed_catalog_query_can_match_product(query: str) -> bool:
    if _tcgplayer_product_ids_from_query(query):
        return True
    return any(_sealed_catalog_specific_tokens(query_variant) for query_variant in _sealed_catalog_query_variants(query))


def _sealed_catalog_category_ids_for_query(
    query: str,
    *,
    selected_game: str,
    category_ids: tuple[str, ...],
) -> tuple[str, ...]:
    if selected_game == "Pokemon" and "3" in category_ids:
        query_norm = _normalize_product_text(query)
        if not re.search(r"\b(japan|japanese|jp|jpn)\b", query_norm):
            return ("3",)
    return category_ids


def _best_sealed_product_match(
    query: str,
    products: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not products:
        return None
    return max(products, key=lambda product: _sealed_product_query_score(query, product))


def _sealed_catalog_suggestions(
    products: list[dict[str, Any]],
    existing_items: list[InventoryItem],
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    existing_keys = {
        "|".join(
            [
                _normalize_lookup(item.card_name),
                _normalize_lookup(item.set_name),
                _normalize_lookup(item.upc),
            ]
        )
        for item in existing_items
    }
    suggestions: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for product in products:
        key = _sealed_catalog_key(product)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        if _sealed_catalog_key(product) in existing_keys:
            continue
        suggestions.append(dict(product))
        if len(suggestions) >= limit:
            break
    return suggestions


def _tcgtracking_sealed_catalog_cache_lock(category_id: str) -> asyncio.Lock:
    lock = _TCGTRACKING_SEALED_CATALOG_LOCKS.get(category_id)
    if lock is None:
        lock = asyncio.Lock()
        _TCGTRACKING_SEALED_CATALOG_LOCKS[category_id] = lock
    return lock


async def _fetch_tcgtracking_set_list(
    client: httpx.AsyncClient,
    category_id: str,
) -> tuple[list[dict[str, Any]], str]:
    cached = _TCGTRACKING_SET_LIST_CACHE.get(str(category_id))
    if cached and (time.monotonic() - cached[0]) < _TCGTRACKING_SET_LIST_CACHE_TTL_SECONDS:
        return [dict(row) for row in cached[1]], ""

    sets_url = f"{TCGTRACKING_BASE}/{category_id}/sets"
    try:
        resp = await client.get(sets_url)
        if resp.status_code != 200:
            logger.warning(
                "[inventory] TCGTracking set list HTTP %s for %s: %s",
                resp.status_code,
                sets_url,
                resp.text[:200],
            )
            return [], f"{category_id} set list HTTP {resp.status_code}"
        sets = [row for row in (resp.json().get("sets") or []) if isinstance(row, dict)]
    except Exception as exc:
        logger.warning("[inventory] TCGTracking set list failed for category=%s: %s", category_id, exc)
        return [], f"{category_id} set list failed"

    _TCGTRACKING_SET_LIST_CACHE[str(category_id)] = (time.monotonic(), [dict(row) for row in sets])
    return sets, ""


async def _fetch_tcgtracking_sealed_catalog(
    client: httpx.AsyncClient,
    *,
    category_id: str,
    game: str,
) -> tuple[list[dict[str, Any]], str]:
    cache_key = str(category_id)
    cached = _TCGTRACKING_SEALED_CATALOG_CACHE.get(cache_key)
    if cached and (time.monotonic() - cached[0]) < _TCGTRACKING_SEALED_CATALOG_CACHE_TTL_SECONDS:
        return [dict(row) for row in cached[1]], cached[2]

    async with _tcgtracking_sealed_catalog_cache_lock(cache_key):
        cached = _TCGTRACKING_SEALED_CATALOG_CACHE.get(cache_key)
        if cached and (time.monotonic() - cached[0]) < _TCGTRACKING_SEALED_CATALOG_CACHE_TTL_SECONDS:
            return [dict(row) for row in cached[1]], cached[2]

        set_infos, set_error = await _fetch_tcgtracking_set_list(client, cache_key)
        if not set_infos:
            return [], set_error
        product_total = 0
        for set_info in set_infos:
            try:
                product_total += int(set_info.get("product_count") or 0)
            except (TypeError, ValueError):
                continue
        if product_total > _TCGTRACKING_SEALED_CATALOG_CATEGORY_PRODUCT_LIMIT:
            return [], f"{category_id} sealed catalog too large"

        semaphore = asyncio.Semaphore(16)

        async def fetch_set_products(set_info: dict[str, Any]) -> list[dict[str, Any]]:
            set_id = str(set_info.get("id") or "").strip()
            if not set_id:
                return []
            products_url = f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}"
            try:
                async with semaphore:
                    resp = await client.get(products_url)
                if resp.status_code != 200:
                    log_fn = logger.debug if resp.status_code == 404 else logger.warning
                    log_fn(
                        "[inventory] TCGTracking catalog set HTTP %s for %s: %s",
                        resp.status_code,
                        products_url,
                        resp.text[:200],
                    )
                    return []
                raw_products = resp.json().get("products") or []
            except Exception as exc:
                logger.warning(
                    "[inventory] TCGTracking catalog set failed for category=%s set=%s: %s",
                    category_id,
                    set_id,
                    exc,
                )
                return []

            sealed_products: list[dict[str, Any]] = []
            for raw_product in raw_products:
                if not isinstance(raw_product, dict):
                    continue
                sealed = _tcgtracking_sealed_product(
                    product=raw_product,
                    set_info=set_info,
                    category_id=category_id,
                    query_kind_hints=set(),
                    pricing=None,
                    game=game,
                )
                if sealed:
                    sealed_products.append(sealed)
            return sealed_products

        chunks = await asyncio.gather(*(fetch_set_products(set_info) for set_info in set_infos))
        catalog = [dict(product) for chunk in chunks for product in chunk]
        _TCGTRACKING_SEALED_CATALOG_CACHE[cache_key] = (time.monotonic(), catalog, set_error)
        return [dict(row) for row in catalog], set_error


async def _enrich_tcgtracking_sealed_product_prices(
    client: httpx.AsyncClient,
    products: list[dict[str, Any]],
) -> None:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for product in products:
        category_id = str(product.get("category_id") or "").strip()
        set_id = str(product.get("set_id") or "").strip()
        if not category_id or not set_id:
            continue
        grouped.setdefault((category_id, set_id), []).append(product)

    async def fetch_pricing(category_id: str, set_id: str) -> tuple[tuple[str, str], dict[str, Any]]:
        pricing_url = f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}/pricing"
        try:
            resp = await client.get(pricing_url)
            if resp.status_code == 200:
                return (category_id, set_id), (resp.json().get("prices") or {})
            logger.warning(
                "[inventory] TCGTracking catalog pricing HTTP %s for %s: %s",
                resp.status_code,
                pricing_url,
                resp.text[:200],
            )
        except Exception as exc:
            logger.warning(
                "[inventory] TCGTracking catalog pricing failed for category=%s set=%s: %s",
                category_id,
                set_id,
                exc,
            )
        return (category_id, set_id), {}

    pricing_by_set = dict(await asyncio.gather(*(fetch_pricing(category_id, set_id) for category_id, set_id in grouped)))
    for set_key, set_products in grouped.items():
        pricing = pricing_by_set.get(set_key) or {}
        for product in set_products:
            price, source = _tcgtracking_sealed_price(str(product.get("external_id") or ""), pricing)
            if price is not None:
                product["market_price"] = price
                product["market_price_source"] = source


def _merge_sealed_products(
    primary: list[dict[str, Any]],
    secondary: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for product in [*primary, *secondary]:
        external_id = str(product.get("external_id") or "").strip()
        key = f"id:{external_id}" if external_id else _sealed_catalog_key(product)
        if key in seen:
            continue
        seen.add(key)
        merged.append(product)
        if len(merged) >= limit:
            break
    return merged


async def _search_tcgtracking_sealed_catalog_products(
    client: httpx.AsyncClient,
    query: str,
    *,
    category_ids: tuple[str, ...],
    selected_game: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    if not _sealed_catalog_query_can_match_product(query):
        return [], ""

    matches: list[dict[str, Any]] = []
    errors: list[str] = []
    catalog_category_ids = _sealed_catalog_category_ids_for_query(
        query,
        selected_game=selected_game,
        category_ids=category_ids,
    )
    for category_id in catalog_category_ids:
        catalog, error = await _fetch_tcgtracking_sealed_catalog(
            client,
            category_id=str(category_id),
            game=_add_stock_game_for_category_id(category_id) or selected_game,
        )
        if error:
            errors.append(error)
        for product in catalog:
            score = _sealed_catalog_product_match_score(query, product)
            if score is None:
                continue
            product_copy = dict(product)
            product_copy["_catalog_score"] = score
            matches.append(product_copy)

    if not matches:
        return [], "; ".join(errors)

    matches.sort(
        key=lambda product: (
            int(product.get("_catalog_score") or 0),
            _sealed_product_query_score(query, product),
        ),
        reverse=True,
    )
    top_matches = matches[:limit]
    await _enrich_tcgtracking_sealed_product_prices(client, top_matches)
    for product in top_matches:
        product.pop("_catalog_score", None)
    return top_matches, "; ".join(errors)


async def _search_sealed_products(
    query: str,
    *,
    game: str = "Pokemon",
    limit: int = 24,
) -> tuple[list[dict[str, Any]], str]:
    selected_game = _normalize_add_stock_game(game)
    category_ids = _add_stock_category_ids_for_game(selected_game)
    if not category_ids:
        return [], f"Product search is not set up for {selected_game} yet. You can still create the product below."

    search_queries = _sealed_set_search_queries(query, game=selected_game)
    if not search_queries:
        return [], ""

    query_kind_hints = _sealed_kind_hints_from_query(query)
    products: list[dict[str, Any]] = []
    seen_sets: set[tuple[str, str]] = set()
    successful_search = False
    found_sets = False
    errors: list[str] = []
    timeout = httpx.Timeout(12.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout, headers=TCGTRACKING_HEADERS) as client:
        for category_id in category_ids:
            for search_query in search_queries:
                search_url = f"{TCGTRACKING_BASE}/{category_id}/search"
                try:
                    search_resp = await client.get(search_url, params={"q": search_query})
                    if search_resp.status_code != 200:
                        errors.append(f"{category_id} search HTTP {search_resp.status_code}")
                        logger.warning(
                            "[inventory] TCGTracking sealed set search HTTP %s for %s q=%r: %s",
                            search_resp.status_code,
                            search_url,
                            search_query,
                            search_resp.text[:200],
                        )
                        continue
                    successful_search = True
                    sets = search_resp.json().get("sets") or []
                    if sets:
                        found_sets = True
                except Exception as exc:
                    errors.append(f"{category_id} search failed")
                    logger.warning(
                        "[inventory] TCGTracking sealed set search failed for category=%s q=%r: %s",
                        category_id,
                        search_query,
                        exc,
                    )
                    continue

                for set_info in sets[:3]:
                    set_id = str(set_info.get("id") or "").strip()
                    if not set_id:
                        continue
                    set_key = (str(category_id), set_id)
                    if set_key in seen_sets:
                        continue
                    seen_sets.add(set_key)
                    products_url = f"{TCGTRACKING_BASE}/{category_id}/sets/{set_id}"
                    try:
                        products_resp, pricing_resp = await asyncio.gather(
                            client.get(products_url),
                            client.get(f"{products_url}/pricing"),
                        )
                        if products_resp.status_code != 200:
                            errors.append(f"{category_id}/{set_id} products HTTP {products_resp.status_code}")
                            logger.warning(
                                "[inventory] TCGTracking sealed products HTTP %s for %s: %s",
                                products_resp.status_code,
                                products_url,
                                products_resp.text[:200],
                            )
                            continue
                        raw_products = products_resp.json().get("products") or []
                        pricing = pricing_resp.json().get("prices", {}) if pricing_resp.status_code == 200 else {}
                    except Exception as exc:
                        errors.append(f"{category_id}/{set_id} products failed")
                        logger.warning(
                            "[inventory] TCGTracking sealed products failed for category=%s set=%s: %s",
                            category_id,
                            set_id,
                            exc,
                        )
                        continue

                    for raw_product in raw_products:
                        sealed = _tcgtracking_sealed_product(
                            product=raw_product,
                            set_info=set_info,
                            category_id=str(category_id),
                            query_kind_hints=query_kind_hints,
                            pricing=pricing,
                            game=_add_stock_game_for_category_id(category_id) or selected_game,
                        )
                        if not sealed:
                            continue
                        products.append(sealed)

        if len(products) < limit:
            catalog_products, catalog_error = await _search_tcgtracking_sealed_catalog_products(
                client,
                query,
                category_ids=category_ids,
                selected_game=selected_game,
                limit=limit,
            )
            if catalog_error:
                errors.append(catalog_error)
            if catalog_products:
                products = _merge_sealed_products(products, catalog_products, limit=max(limit * 2, limit))

    requested_product_ids = _tcgplayer_product_ids_from_query(query)
    if requested_product_ids:
        exact_products = [
            product
            for product in products
            if str(product.get("external_id") or "").strip() in requested_product_ids
        ]
        if exact_products:
            products = exact_products

    if products:
        products.sort(key=lambda product: _sealed_product_query_score(query, product), reverse=True)
    warning = ""
    if errors and (not successful_search or (found_sets and not products)):
        warning = f"{selected_game} product search is unavailable right now. You can still create the product below."
    return products[:limit], warning


async def _search_pokemon_sealed_products(
    query: str,
    *,
    limit: int = 24,
) -> tuple[list[dict[str, Any]], str]:
    return await _search_sealed_products(query, game="Pokemon", limit=limit)


def _parse_bulk_sealed_lines(bulk_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, raw_line in enumerate((bulk_text or "").splitlines()):
        line = raw_line.strip()
        if not line:
            continue
        match = re.match(r"^\s*(\d+)\s*(?:x|×)?\s+(.+?)\s*$", line, flags=re.IGNORECASE)
        if match:
            quantity = int(match.group(1))
            query = match.group(2).strip()
        else:
            quantity = 1
            query = line
        rows.append(
            {
                "row_index": idx,
                "line": line,
                "quantity": quantity,
                "query": query,
                "error": "" if query and quantity > 0 else "Needs a product name and quantity.",
            }
        )
    return rows


async def _build_bulk_sealed_preview(bulk_text: str, *, game: str = "Pokemon") -> list[dict[str, Any]]:
    rows = _parse_bulk_sealed_lines(bulk_text)
    selected_game = _normalize_add_stock_game(game)
    for row in rows:
        row["product"] = None
        row["matches"] = []
        row["warning"] = ""
        if row.get("error"):
            continue
        products, warning = await _search_sealed_products(str(row["query"]), game=selected_game, limit=16)
        row["warning"] = warning
        row["matches"] = products[:5]
        row["product"] = _best_sealed_product_match(str(row["query"]), products)
        if not row["product"]:
            row["error"] = "No API match. Search this one manually below."
        else:
            row["auto_price"] = row["product"].get("market_price")
            row["list_price"] = row["product"].get("market_price")
            row["product_name"] = row["product"].get("name")
            row["set_name"] = row["product"].get("set_name")
            row["sealed_product_kind"] = row["product"].get("kind")
            row["upc"] = row["product"].get("upc")
            row["image_url"] = row["product"].get("image_url")
    return rows


def _bulk_received_message(count: int, units: int) -> str:
    if count <= 0:
        return ""
    product_word = "product" if count == 1 else "products"
    unit_word = "unit" if units == 1 else "units"
    return f"Added {units} {unit_word} across {count} {product_word}."


def _safe_price(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _condition_code(value: str | None) -> str:
    clean = _normalize_lookup(value).replace(" ", "_")
    aliases = {
        "near_mint": "NM",
        "nm": "NM",
        "lightly_played": "LP",
        "lp": "LP",
        "moderately_played": "MP",
        "mp": "MP",
        "heavily_played": "HP",
        "hp": "HP",
        "damaged": "DMG",
        "dmg": "DMG",
        "dm": "DMG",
    }
    return aliases.get(clean, (value or "").strip().upper())


def _single_variant_condition_prices(variant: dict[str, Any]) -> dict[str, dict[str, Optional[float]]]:
    prices: dict[str, dict[str, Optional[float]]] = {}
    raw_conditions = variant.get("conditions") or {}
    if isinstance(raw_conditions, dict):
        for raw_condition, raw_prices in raw_conditions.items():
            code = _condition_code(str(raw_condition))
            if code not in CONDITIONS or not isinstance(raw_prices, dict):
                continue
            market = _safe_price(raw_prices.get("mkt") or raw_prices.get("market") or raw_prices.get("price"))
            low = _safe_price(raw_prices.get("low") or raw_prices.get("low_price"))
            if market is not None or low is not None:
                prices[code] = {"market": market, "low": low}

    variant_market = _safe_price(variant.get("price") or variant.get("market_price"))
    variant_low = _safe_price(variant.get("low_price") or variant.get("low"))
    if "NM" not in prices and (variant_market is not None or variant_low is not None):
        prices["NM"] = {"market": variant_market, "low": variant_low}
    return {condition: prices[condition] for condition in CONDITIONS if condition in prices}


def _single_lookup_variants(match: dict[str, Any]) -> list[dict[str, Any]]:
    raw_variants = match.get("available_variants") or []
    variants: list[dict[str, Any]] = []
    if isinstance(raw_variants, list):
        for raw_variant in raw_variants:
            if not isinstance(raw_variant, dict):
                continue
            name = str(raw_variant.get("name") or "Market").strip() or "Market"
            condition_prices = _single_variant_condition_prices(raw_variant)
            market_price = _safe_price(raw_variant.get("price") or raw_variant.get("market_price"))
            low_price = _safe_price(raw_variant.get("low_price") or raw_variant.get("low"))
            if market_price is None and condition_prices:
                market_price = next(
                    (row.get("market") for row in condition_prices.values() if row.get("market") is not None),
                    None,
                )
            variants.append(
                {
                    "name": name,
                    "market_price": market_price,
                    "low_price": low_price,
                    "condition_prices": condition_prices,
                }
            )

    if not variants:
        market_price = _safe_price(match.get("market_price"))
        variants.append(
            {
                "name": "Market",
                "market_price": market_price,
                "low_price": None,
                "condition_prices": {"NM": {"market": market_price, "low": None}} if market_price is not None else {},
            }
        )
    return variants


def _single_lookup_suggestions(search_result: dict[str, Any]) -> list[dict[str, Any]]:
    game = search_result.get("game") or "Pokemon"
    raw_matches: list[dict[str, Any]] = []
    best = search_result.get("best_match")
    if isinstance(best, dict):
        raw_matches.append(best)
    for candidate in search_result.get("candidates") or []:
        if isinstance(candidate, dict):
            raw_matches.append(candidate)

    suggestions: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for match in raw_matches:
        name = str(match.get("name") or match.get("card_name") or "").strip()
        if not name:
            continue
        card_number = str(match.get("number") or match.get("card_number") or "").strip()
        set_name = str(match.get("set_name") or "").strip()
        key = (_normalize_lookup(name), _normalize_lookup(set_name), _normalize_lookup(card_number))
        if key in seen:
            continue
        seen.add(key)
        variants = _single_lookup_variants(match)
        default_variant = variants[0] if variants else {}
        default_price = None
        default_prices = default_variant.get("condition_prices") or {}
        if isinstance(default_prices, dict):
            default_price = (default_prices.get("NM") or {}).get("market")
        if default_price is None:
            default_price = default_variant.get("market_price")
        suggestions.append(
            {
                "name": name,
                "game": game,
                "set_name": set_name,
                "set_code": str(match.get("set_id") or match.get("set_code") or "").strip(),
                "card_number": card_number,
                "image_url": str(match.get("image_url") or match.get("image_url_small") or "").strip(),
                "tcgplayer_url": str(match.get("tcgplayer_url") or "").strip(),
                "variants": variants,
                "default_variant": default_variant.get("name") or "",
                "default_condition": "NM",
                "default_price": default_price,
            }
        )
    return suggestions[:8]


async def _cached_add_stock_sealed_search(
    query: str,
    *,
    game: str = "Pokemon",
    limit: int = 24,
) -> tuple[list[dict[str, Any]], str]:
    selected_game = _normalize_add_stock_game(game)
    cache_key = _add_stock_cache_key(query, game=selected_game, limit=limit)
    cached = _ADD_STOCK_SEALED_CACHE.get(cache_key)
    if cached and _add_stock_cache_is_fresh(cached[0]):
        return [dict(product) for product in cached[1]], cached[2]

    products, warning = await _search_sealed_products(query, game=selected_game, limit=limit)
    _ADD_STOCK_SEALED_CACHE[cache_key] = (
        time.monotonic(),
        [dict(product) for product in products],
        warning,
    )
    return products, warning


async def _cached_add_stock_single_search(
    query: str,
    *,
    game: str = "Pokemon",
) -> tuple[list[dict[str, Any]], str]:
    selected_game = _normalize_add_stock_game(game)
    category_id = _add_stock_single_category_id(selected_game)
    if not category_id:
        return [], f"Manual single lookup is not set up for {selected_game} yet."

    cache_key = _add_stock_cache_key(query, game=selected_game, category_id=category_id)
    cached = _ADD_STOCK_SINGLE_CACHE.get(cache_key)
    if cached and _add_stock_cache_is_fresh(cached[0]):
        return [dict(card) for card in cached[1]], cached[2]

    try:
        single_search = await text_search_cards(
            query,
            category_id=category_id,
            use_ai_parse=False,
            max_results=6,
            include_pokemontcg_supplement=False,
        )
    except Exception as exc:
        logger.warning("[inventory] single lookup failed for %r: %r", query, exc)
        error = f"{selected_game} single lookup is unavailable right now. Try again in a minute."
        _ADD_STOCK_SINGLE_CACHE[cache_key] = (time.monotonic(), [], error)
        return [], error

    suggestions: list[dict[str, Any]] = []
    error = ""
    if single_search.get("status") in {"MATCHED", "AMBIGUOUS"}:
        suggestions = _single_lookup_suggestions(single_search)
    if not suggestions:
        error = single_search.get("error") or f"No {selected_game} singles found for '{query}'."
    _ADD_STOCK_SINGLE_CACHE[cache_key] = (
        time.monotonic(),
        [dict(card) for card in suggestions],
        error,
    )
    return suggestions, error


def _variant_price_for_condition(
    variants: list[dict[str, Any]],
    *,
    variant_name: str,
    condition: str,
) -> tuple[Optional[float], Optional[float], dict[str, Any]]:
    selected = None
    for variant in variants:
        if str(variant.get("name") or "") == variant_name:
            selected = variant
            break
    if selected is None and variants:
        selected = variants[0]
    if selected is None:
        return None, None, {}
    condition_prices = selected.get("condition_prices") or {}
    row = condition_prices.get(_condition_code(condition)) if isinstance(condition_prices, dict) else None
    if isinstance(row, dict):
        market = _safe_price(row.get("market"))
        low = _safe_price(row.get("low"))
    else:
        market = None
        low = None
    if market is None:
        market = _safe_price(selected.get("market_price"))
    if low is None:
        low = _safe_price(selected.get("low_price"))
    return market, low, selected


async def _inventory_sealed_template_context(
    request: Request,
    session: Session,
    *,
    game: str = "Pokemon",
    q: str = "",
    single_q: str = "",
    search_type: str = "both",
    received: int = 0,
    single_received: int = 0,
    error: str = "",
    single_error: str = "",
    bulk_text: str = "",
    bulk_rows: Optional[list[dict[str, Any]]] = None,
    bulk_location: str = "",
    bulk_source: str = "",
    bulk_notes: str = "",
    bulk_received: int = 0,
    bulk_units: int = 0,
) -> dict[str, Any]:
    selected_game = _normalize_add_stock_game(game)
    selected_search_type = _normalize_add_stock_search_type(search_type if isinstance(search_type, str) else "sealed")
    search_text = (single_q or q or "").strip()
    game_values = _add_stock_existing_game_values(selected_game)
    existing_items: list[InventoryItem] = []
    if search_text and selected_search_type != "cards":
        query = select(InventoryItem).where(
            InventoryItem.item_type == ITEM_TYPE_SEALED,
            InventoryItem.game.in_(game_values),
            InventoryItem.archived_at == None,  # noqa: E711
        )
        like = f"%{search_text}%"
        query = query.where(
            InventoryItem.card_name.ilike(like)
            | InventoryItem.set_name.ilike(like)
            | InventoryItem.upc.ilike(like)
            | InventoryItem.sealed_product_kind.ilike(like)
            | InventoryItem.location.ilike(like)
        )
        existing_items = session.exec(
            query.order_by(InventoryItem.updated_at.desc(), InventoryItem.created_at.desc()).limit(12)
        ).all()

    api_products: list[dict[str, Any]] = []
    catalog_error = ""
    single_results: list[dict[str, Any]] = []
    single_lookup_error = ""

    if search_text:
        if selected_search_type == "sealed":
            api_products, catalog_error = await _cached_add_stock_sealed_search(
                search_text,
                game=selected_game,
                limit=24,
            )
        elif selected_search_type == "cards":
            single_results, single_lookup_error_candidate = await _cached_add_stock_single_search(
                search_text,
                game=selected_game,
            )
            if single_lookup_error_candidate and not single_results:
                single_lookup_error = single_lookup_error_candidate
        else:
            looks_sealed = _add_stock_query_looks_sealed(search_text)
            if looks_sealed:
                api_products, catalog_error = await _cached_add_stock_sealed_search(
                    search_text,
                    game=selected_game,
                    limit=24,
                )
            else:
                (api_products, catalog_error), (single_results, single_lookup_error_candidate) = await asyncio.gather(
                    _cached_add_stock_sealed_search(search_text, game=selected_game, limit=24),
                    _cached_add_stock_single_search(search_text, game=selected_game),
                )
                if single_results:
                    api_products = [
                        product
                        for product in api_products
                        if _sealed_catalog_product_match_score(search_text, product) is not None
                    ]
                    if not api_products:
                        catalog_error = ""
                if single_lookup_error_candidate and not single_results and not existing_items and not api_products:
                    single_lookup_error = single_lookup_error_candidate

    suggestions = _sealed_catalog_suggestions(api_products, existing_items, limit=12)
    received_item = session.get(InventoryItem, received) if received else None
    if received_item and received_item.item_type != ITEM_TYPE_SEALED:
        received_item = None
    single_received_item = session.get(InventoryItem, single_received) if single_received else None
    if single_received_item and single_received_item.item_type != ITEM_TYPE_SINGLE:
        single_received_item = None

    recent_movements = session.exec(
        select(InventoryStockMovement)
        .where(InventoryStockMovement.reason == "receive")
        .order_by(InventoryStockMovement.created_at.desc())
        .limit(12)
    ).all()
    movement_item_ids = {row.item_id for row in recent_movements}
    movement_items = {}
    if movement_item_ids:
        movement_items = {
            item.id: item
            for item in session.exec(
                select(InventoryItem).where(InventoryItem.id.in_(movement_item_ids))
            ).all()
        }

    return {
        "current_user": _current_user(request),
        "csrf_token": issue_token(request),
        "can_view_inventory": _can_inventory_view(request, session),
        "can_manage_inventory": _can_inventory_manage(request, session),
        "selected_game": selected_game,
        "game_options": ADD_STOCK_GAME_OPTIONS,
        "search_type": selected_search_type,
        "search_type_options": ADD_STOCK_SEARCH_TYPE_OPTIONS,
        "scan_url": "/degen_eye/v2" if selected_game == "Pokemon" else "/degen_eye",
        "q": search_text,
        "single_q": search_text,
        "has_search": bool(search_text),
        "error": error,
        "single_error": single_error or single_lookup_error,
        "catalog_error": catalog_error,
        "received_item": received_item,
        "single_received_item": single_received_item,
        "bulk_message": _bulk_received_message(bulk_received, bulk_units),
        "bulk_text": bulk_text,
        "bulk_rows": bulk_rows or [],
        "bulk_location": bulk_location,
        "bulk_source": bulk_source,
        "bulk_notes": bulk_notes,
        "existing_items": existing_items,
        "suggestions": suggestions,
        "recent_movements": recent_movements,
        "movement_items": movement_items,
        "product_kinds": SEALED_PRODUCT_KINDS,
        "single_results": single_results,
        "conditions": CONDITIONS,
    }


def _find_existing_sealed_item(
    session: Session,
    *,
    game: str,
    product_name: str,
    set_name: str = "",
    upc: str = "",
) -> Optional[InventoryItem]:
    upc_clean = upc.strip()
    if upc_clean:
        found = session.exec(
            select(InventoryItem).where(
                InventoryItem.item_type == ITEM_TYPE_SEALED,
                InventoryItem.game == game,
                InventoryItem.upc == upc_clean,
            )
        ).first()
        if found:
            return found

    name_norm = _normalize_lookup(product_name)
    set_norm = _normalize_lookup(set_name)
    if not name_norm:
        return None
    candidates = session.exec(
        select(InventoryItem).where(
            InventoryItem.item_type == ITEM_TYPE_SEALED,
            InventoryItem.game == game,
        )
    ).all()
    for item in candidates:
        if _normalize_lookup(item.card_name) != name_norm:
            continue
        if set_norm and _normalize_lookup(item.set_name) != set_norm:
            continue
        return item
    return None


def _receive_sealed_stock(
    session: Session,
    *,
    item_id: Optional[int] = None,
    game: str = "Pokemon",
    product_name: str,
    set_name: str = "",
    sealed_product_kind: str = "",
    upc: str = "",
    image_url: str = "",
    quantity: int,
    unit_cost: Optional[float] = None,
    list_price: Optional[float] = None,
    auto_price: Optional[float] = None,
    low_price: Optional[float] = None,
    location: str = "",
    source: str = "",
    notes: str = "",
    price_payload: Optional[dict[str, Any]] = None,
    actor_label: Optional[str] = None,
) -> tuple[InventoryItem, InventoryStockMovement, bool]:
    if quantity < 1:
        raise ValueError("Quantity must be at least 1.")
    product_name = product_name.strip()
    if not product_name:
        raise ValueError("Product name is required.")

    created = False
    item: Optional[InventoryItem] = session.get(InventoryItem, item_id) if item_id else None
    if item and item.item_type != ITEM_TYPE_SEALED:
        raise ValueError("Selected item is not a sealed product.")

    if item is None:
        item = _find_existing_sealed_item(
            session,
            game=game,
            product_name=product_name,
            set_name=set_name,
            upc=upc,
        )

    if item is None:
        item = InventoryItem(
            barcode="PENDING",
            item_type=ITEM_TYPE_SEALED,
            game=game,
            card_name=product_name,
            set_name=set_name.strip() or None,
            sealed_product_kind=sealed_product_kind.strip() or None,
            upc=upc.strip() or None,
            location=location.strip() or None,
            language="English",
            condition="Sealed",
            quantity=0,
            cost_basis=unit_cost,
            list_price=list_price,
            auto_price=auto_price,
            last_priced_at=utcnow() if auto_price is not None else None,
            image_url=image_url.strip() or None,
            status=INVENTORY_IN_STOCK,
            created_at=utcnow(),
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        item.barcode = generate_barcode_value(item.id)
        created = True
    else:
        if sealed_product_kind.strip() and not item.sealed_product_kind:
            item.sealed_product_kind = sealed_product_kind.strip()
        if upc.strip() and not item.upc:
            item.upc = upc.strip()
        if image_url.strip() and not item.image_url:
            item.image_url = image_url.strip()
        if set_name.strip() and not item.set_name:
            item.set_name = set_name.strip()
        if auto_price is not None:
            item.auto_price = auto_price
            item.last_priced_at = utcnow()
        if list_price is not None:
            item.list_price = list_price
        if item.archived_at is not None:
            item.archived_at = None
            item.archived_by = None
            item.archive_reason = None

    before_qty = max(0, item.quantity or 0)
    after_qty = before_qty + quantity
    if unit_cost is not None:
        if item.cost_basis is not None and before_qty > 0:
            item.cost_basis = round(
                ((item.cost_basis * before_qty) + (unit_cost * quantity)) / after_qty,
                2,
            )
        else:
            item.cost_basis = unit_cost
    if location.strip():
        item.location = location.strip()
    item.quantity = after_qty
    item.status = INVENTORY_IN_STOCK
    item.updated_at = utcnow()
    session.add(item)
    movement = InventoryStockMovement(
        item_id=item.id,
        reason="receive",
        quantity_delta=quantity,
        quantity_before=before_qty,
        quantity_after=after_qty,
        unit_cost=unit_cost,
        total_cost=round(unit_cost * quantity, 2) if unit_cost is not None else None,
        location=location.strip() or item.location,
        source=source.strip() or None,
        notes=notes.strip() or None,
        created_by=actor_label,
        created_at=utcnow(),
    )
    session.add(movement)
    if auto_price is not None or low_price is not None:
        session.add(
            PriceHistory(
                item_id=item.id,
                source="tcgtracking",
                market_price=auto_price,
                low_price=low_price,
                high_price=None,
                raw_response_json=json.dumps(price_payload or {}, sort_keys=True),
            )
        )
    session.commit()
    session.refresh(item)
    session.refresh(movement)
    return item, movement, created


def _find_existing_single_item(
    session: Session,
    *,
    game: str,
    card_name: str,
    set_name: str = "",
    card_number: str = "",
    variant: str = "",
    condition: str = "",
) -> Optional[InventoryItem]:
    name_norm = _normalize_lookup(card_name)
    if not name_norm:
        return None
    candidates = session.exec(
        select(InventoryItem).where(
            InventoryItem.item_type == ITEM_TYPE_SINGLE,
            InventoryItem.game == game,
        )
    ).all()
    set_norm = _normalize_lookup(set_name)
    number_norm = _normalize_lookup(card_number)
    variant_norm = _normalize_lookup(variant)
    condition_norm = _normalize_lookup(condition)
    for item in candidates:
        if _normalize_lookup(item.card_name) != name_norm:
            continue
        if _normalize_lookup(item.set_name) != set_norm:
            continue
        if _normalize_lookup(item.card_number) != number_norm:
            continue
        if _normalize_lookup(item.variant) != variant_norm:
            continue
        if _normalize_lookup(item.condition) != condition_norm:
            continue
        return item
    return None


def _receive_single_stock(
    session: Session,
    *,
    game: str = "Pokemon",
    card_name: str,
    set_name: str = "",
    set_code: str = "",
    card_number: str = "",
    variant: str = "",
    condition: str = "NM",
    image_url: str = "",
    quantity: int,
    unit_cost: Optional[float] = None,
    list_price: Optional[float] = None,
    auto_price: Optional[float] = None,
    low_price: Optional[float] = None,
    location: str = "",
    source: str = "",
    notes: str = "",
    price_payload: Optional[dict[str, Any]] = None,
    actor_label: Optional[str] = None,
) -> tuple[InventoryItem, InventoryStockMovement, bool]:
    if quantity < 1:
        raise ValueError("Quantity must be at least 1.")
    card_name = card_name.strip()
    if not card_name:
        raise ValueError("Card name is required.")
    condition = _condition_code(condition or "NM")
    if condition not in CONDITIONS:
        raise ValueError("Choose a valid condition.")

    item = _find_existing_single_item(
        session,
        game=game,
        card_name=card_name,
        set_name=set_name,
        card_number=card_number,
        variant=variant,
        condition=condition,
    )
    created = False
    if item is None:
        item = InventoryItem(
            barcode="PENDING",
            item_type=ITEM_TYPE_SINGLE,
            game=game,
            card_name=card_name,
            set_name=set_name.strip() or None,
            set_code=set_code.strip() or None,
            card_number=card_number.strip() or None,
            variant=variant.strip() or None,
            language="English",
            condition=condition,
            quantity=0,
            cost_basis=unit_cost,
            auto_price=auto_price,
            list_price=list_price,
            last_priced_at=utcnow() if auto_price is not None else None,
            location=location.strip() or None,
            image_url=image_url.strip() or None,
            notes=notes.strip() or None,
            status=INVENTORY_IN_STOCK,
            created_at=utcnow(),
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        item.barcode = generate_barcode_value(item.id)
        created = True
    else:
        if set_code.strip() and not item.set_code:
            item.set_code = set_code.strip()
        if image_url.strip() and not item.image_url:
            item.image_url = image_url.strip()
        if variant.strip() and not item.variant:
            item.variant = variant.strip()
        if auto_price is not None:
            item.auto_price = auto_price
            item.last_priced_at = utcnow()
        if list_price is not None:
            item.list_price = list_price
        if notes.strip() and not item.notes:
            item.notes = notes.strip()
        if item.archived_at is not None:
            item.archived_at = None
            item.archived_by = None
            item.archive_reason = None

    before_qty = max(0, item.quantity or 0)
    after_qty = before_qty + quantity
    if unit_cost is not None:
        if item.cost_basis is not None and before_qty > 0:
            item.cost_basis = round(
                ((item.cost_basis * before_qty) + (unit_cost * quantity)) / after_qty,
                2,
            )
        else:
            item.cost_basis = unit_cost
    if location.strip():
        item.location = location.strip()
    item.quantity = after_qty
    item.status = INVENTORY_IN_STOCK
    item.updated_at = utcnow()
    session.add(item)

    movement = InventoryStockMovement(
        item_id=item.id,
        reason="receive",
        quantity_delta=quantity,
        quantity_before=before_qty,
        quantity_after=after_qty,
        unit_cost=unit_cost,
        total_cost=round(unit_cost * quantity, 2) if unit_cost is not None else None,
        location=location.strip() or item.location,
        source=source.strip() or None,
        notes=notes.strip() or None,
        created_by=actor_label,
        created_at=utcnow(),
    )
    session.add(movement)

    if auto_price is not None or low_price is not None:
        session.add(
            PriceHistory(
                item_id=item.id,
                source="tcgtracking",
                market_price=auto_price,
                low_price=low_price,
                high_price=None,
                raw_response_json=json.dumps(price_payload or {}, sort_keys=True),
            )
        )

    session.commit()
    session.refresh(item)
    session.refresh(movement)
    return item, movement, created


def _find_existing_slab_item(
    session: Session,
    *,
    card_name: str,
    grading_company: str = "",
    grade: str = "",
    cert_number: str = "",
) -> Optional[InventoryItem]:
    cert_clean = (cert_number or "").strip()
    company_clean = (grading_company or "").strip().upper()
    if cert_clean and company_clean:
        found = session.exec(
            select(InventoryItem).where(
                InventoryItem.item_type == ITEM_TYPE_SLAB,
                InventoryItem.grading_company == company_clean,
                InventoryItem.cert_number == cert_clean,
            )
        ).first()
        if found:
            return found

    name_norm = _normalize_lookup(card_name)
    grade_norm = _normalize_lookup(grade)
    if not name_norm:
        return None
    candidates = session.exec(
        select(InventoryItem).where(InventoryItem.item_type == ITEM_TYPE_SLAB)
    ).all()
    for item in candidates:
        if _normalize_lookup(item.card_name) != name_norm:
            continue
        if company_clean and (item.grading_company or "").upper() != company_clean:
            continue
        if grade_norm and _normalize_lookup(item.grade) != grade_norm:
            continue
        return item
    return None


def _receive_slab_stock(
    session: Session,
    *,
    game: str = "Other",
    card_name: str,
    set_name: str = "",
    card_number: str = "",
    grading_company: str = "",
    grade: str = "",
    cert_number: str = "",
    quantity: int = 1,
    unit_cost: Optional[float] = None,
    list_price: Optional[float] = None,
    auto_price: Optional[float] = None,
    location: str = "",
    source: str = "",
    notes: str = "",
    price_payload: Optional[dict[str, Any]] = None,
    actor_label: Optional[str] = None,
) -> tuple[InventoryItem, InventoryStockMovement, bool]:
    if quantity < 1:
        raise ValueError("Quantity must be at least 1.")
    card_name = card_name.strip()
    if not card_name:
        raise ValueError("Card name is required.")
    company_clean = grading_company.strip().upper()

    item = _find_existing_slab_item(
        session,
        card_name=card_name,
        grading_company=company_clean,
        grade=grade,
        cert_number=cert_number,
    )
    created = False
    if item is None:
        item = InventoryItem(
            barcode="PENDING",
            item_type=ITEM_TYPE_SLAB,
            game=game.strip() or "Other",
            card_name=card_name,
            set_name=set_name.strip() or None,
            card_number=card_number.strip() or None,
            language="English",
            quantity=0,
            grading_company=company_clean or None,
            grade=grade.strip() or None,
            cert_number=cert_number.strip() or None,
            cost_basis=unit_cost,
            auto_price=auto_price,
            list_price=list_price,
            last_priced_at=utcnow() if auto_price is not None else None,
            location=location.strip() or None,
            notes=notes.strip() or None,
            status=INVENTORY_IN_STOCK,
            created_at=utcnow(),
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        item.barcode = generate_barcode_value(item.id)
        created = True
    else:
        if set_name.strip() and not item.set_name:
            item.set_name = set_name.strip()
        if card_number.strip() and not item.card_number:
            item.card_number = card_number.strip()
        if company_clean and not item.grading_company:
            item.grading_company = company_clean
        if grade.strip() and not item.grade:
            item.grade = grade.strip()
        if cert_number.strip() and not item.cert_number:
            item.cert_number = cert_number.strip()
        if auto_price is not None:
            item.auto_price = auto_price
            item.last_priced_at = utcnow()
        if list_price is not None:
            item.list_price = list_price
        if notes.strip() and not item.notes:
            item.notes = notes.strip()
        if item.archived_at is not None:
            item.archived_at = None
            item.archived_by = None
            item.archive_reason = None

    before_qty = max(0, item.quantity or 0)
    after_qty = before_qty + quantity
    if unit_cost is not None:
        if item.cost_basis is not None and before_qty > 0:
            item.cost_basis = round(
                ((item.cost_basis * before_qty) + (unit_cost * quantity)) / after_qty,
                2,
            )
        else:
            item.cost_basis = unit_cost
    if location.strip():
        item.location = location.strip()
    item.quantity = after_qty
    item.status = INVENTORY_IN_STOCK
    item.updated_at = utcnow()
    session.add(item)

    movement = InventoryStockMovement(
        item_id=item.id,
        reason="receive",
        quantity_delta=quantity,
        quantity_before=before_qty,
        quantity_after=after_qty,
        unit_cost=unit_cost,
        total_cost=round(unit_cost * quantity, 2) if unit_cost is not None else None,
        location=location.strip() or item.location,
        source=source.strip() or None,
        notes=notes.strip() or None,
        created_by=actor_label,
        created_at=utcnow(),
    )
    session.add(movement)

    if auto_price is not None:
        session.add(
            PriceHistory(
                item_id=item.id,
                source=(price_payload or {}).get("source") or "card_ladder",
                market_price=auto_price,
                low_price=(price_payload or {}).get("low_price"),
                high_price=(price_payload or {}).get("high_price"),
                raw_response_json=json.dumps(price_payload or {}, sort_keys=True),
            )
        )

    session.commit()
    session.refresh(item)
    session.refresh(movement)
    return item, movement, created


def _capture_user_payload(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    if not user:
        return {}
    return {
        "id": getattr(user, "id", None),
        "username": getattr(user, "username", None),
        "display_name": getattr(user, "display_name", None),
        "role": getattr(user, "role", None),
    }


def _capture_request_meta(request: Request) -> dict[str, Any]:
    return {
        "user_agent": (request.headers.get("user-agent") or "")[:300],
    }


def _tag_v2_capture_result(payload: dict[str, Any], capture_id: Optional[str]) -> None:
    if not capture_id or not isinstance(payload, dict):
        return
    payload["capture_id"] = capture_id
    debug = payload.setdefault("debug", {})
    if isinstance(debug, dict):
        debug["v2_capture_id"] = capture_id


def _truthy_form_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _render_v2_training_page(summary: Optional[dict[str, Any]] = None) -> str:
    stats = training_capture_stats()
    phash_stats = phash_index_stats()
    summary_html = ""
    if summary is not None:
        summary_html = (
            "<section>"
            "<h2>Last Run</h2>"
            f"<pre>{html.escape(json.dumps(summary, indent=2, default=str))}</pre>"
            "</section>"
        )
    stats_json = html.escape(json.dumps(stats, indent=2, default=str))
    phash_json = html.escape(json.dumps({
        "card_count": phash_stats.get("card_count"),
        "metadata": phash_stats.get("metadata"),
        "index_path": phash_stats.get("index_path"),
    }, indent=2, default=str))
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Degen Eye v2 Training</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 24px; color: #0f172a; background: #f8fafc; }}
    main {{ max-width: 920px; margin: 0 auto; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin-top: 24px; font-size: 18px; }}
    p {{ color: #475569; line-height: 1.45; }}
    form, section {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; margin-top: 16px; }}
    label {{ display: block; font-weight: 700; margin: 12px 0 6px; }}
    input[type="number"] {{ width: 120px; padding: 8px; border: 1px solid #cbd5e1; border-radius: 6px; }}
    .check {{ display: flex; gap: 8px; align-items: center; margin-top: 12px; }}
    .check label {{ margin: 0; font-weight: 600; }}
    button {{ margin-top: 16px; border: 0; border-radius: 6px; padding: 10px 14px; font-weight: 800; cursor: pointer; background: #111827; color: #fff; }}
    button.secondary {{ background: #475569; }}
    pre {{ white-space: pre-wrap; background: #0f172a; color: #e2e8f0; padding: 14px; border-radius: 8px; overflow: auto; }}
    a {{ color: #2563eb; font-weight: 700; text-decoration: none; }}
  </style>
</head>
<body>
<main>
  <p><a href="/degen_eye/v2">Back to scanner</a></p>
  <h1>Degen Eye v2 Training</h1>
  <p>Confirmed batch-review labels can be promoted into the local pHash index as real employee photo examples. Unconfirmed scans are saved for review and evaluation, but not trusted as labels.</p>
  <form method="post" action="/degen_eye/v2/train-captures">
    <label for="limit">Max confirmed captures to process</label>
    <input id="limit" name="limit" type="number" min="1" max="2000" value="200">
    <div class="check">
      <input id="dry_run" name="dry_run" type="checkbox" value="true">
      <label for="dry_run">Dry run only</label>
    </div>
    <div class="check">
      <input id="include_indexed" name="include_indexed" type="checkbox" value="true">
      <label for="include_indexed">Reprocess already-indexed captures</label>
    </div>
    <div class="check">
      <input id="reload_current_worker" name="reload_current_worker" type="checkbox" value="true" checked>
      <label for="reload_current_worker">Reload pHash index in this worker after training</label>
    </div>
    <button type="submit">Train From Confirmed Captures</button>
  </form>
  <form method="post" action="/degen_eye/v2/reload-index">
    <button class="secondary" type="submit">Reload pHash Index Only</button>
  </form>
  {summary_html}
  <section><h2>Capture Stats</h2><pre>{stats_json}</pre></section>
  <section><h2>Index Stats</h2><pre>{phash_json}</pre></section>
</main>
</body>
</html>"""


# ---------------------------------------------------------------------------
# List view
# ---------------------------------------------------------------------------

@router.get("/inventory", response_class=HTMLResponse)
async def inventory_list(
    request: Request,
    session: Session = Depends(get_session),
    status: str = Query(default=""),
    game: str = Query(default=""),
    item_type: str = Query(default=""),
    q: str = Query(default=""),
    deleted: str = Query(default=""),
    updated: int = Query(default=0),
    repriced: int = Query(default=0),
    price_errors: int = Query(default=0),
    market_refreshed: int = Query(default=0),
    market_errors: int = Query(default=0),
    archived: str = Query(default=""),
    resticker: str = Query(default=""),
    price_review: str = Query(default=""),
    edit: str = Query(default=""),
    page: int = Query(default=1, ge=1),
):
    if denial := _require_employee_permission(request, "ops.inventory.view", session):
        return denial

    query = select(InventoryItem)
    price_review_condition = _inventory_price_review_sql_condition()
    if archived == "1":
        query = query.where(InventoryItem.archived_at != None)  # noqa: E711
    elif archived != "all":
        query = query.where(InventoryItem.archived_at == None)  # noqa: E711
    if status and status in ALL_INVENTORY_STATUSES:
        query = query.where(InventoryItem.status == status)
    if game:
        query = query.where(InventoryItem.game == game)
    if item_type and item_type in (ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB, ITEM_TYPE_SEALED):
        query = query.where(InventoryItem.item_type == item_type)
    if resticker == "1":
        query = query.where(InventoryItem.resticker_alert_active == True)  # noqa: E712
    if price_review == "1":
        query = query.where(price_review_condition)
    if q:
        like = f"%{q}%"
        query = query.where(
            InventoryItem.card_name.ilike(like)
            | InventoryItem.barcode.ilike(like)
            | InventoryItem.set_name.ilike(like)
            | InventoryItem.variant.ilike(like)
            | InventoryItem.cert_number.ilike(like)
            | InventoryItem.upc.ilike(like)
            | InventoryItem.location.ilike(like)
        )

    total = session.exec(
        select(func.count()).select_from(query.subquery())
    ).one()
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(page, total_pages)
    offset = (page - 1) * PAGE_SIZE

    price_gap_expr = InventoryItem.auto_price - InventoryItem.list_price
    items = session.exec(
        query.order_by(
            case((price_review_condition, 0), else_=1),
            price_gap_expr.desc(),
            InventoryItem.created_at.desc(),
        ).offset(offset).limit(PAGE_SIZE)
    ).all()

    can_manage_inventory = _can_inventory_manage(request, session)
    edit_mode = can_manage_inventory and edit == "1"
    active_items = InventoryItem.archived_at == None  # noqa: E711
    stale_cutoff = utcnow() - timedelta(
        hours=max(float(getattr(settings, "inventory_price_stale_hours", 24.0) or 24.0), 1.0)
    )
    stale_price_condition = (
        (InventoryItem.status.in_([INVENTORY_IN_STOCK, INVENTORY_LISTED]))
        & (
            (InventoryItem.last_priced_at == None)  # noqa: E711
            | (InventoryItem.last_priced_at < stale_cutoff)
        )
    )
    inventory_summary = {
        "all": session.exec(select(func.count()).where(active_items)).one(),
        "archived": session.exec(
            select(func.count()).where(InventoryItem.archived_at != None)  # noqa: E711
        ).one(),
        "sealed": session.exec(
            select(func.count()).where(InventoryItem.item_type == ITEM_TYPE_SEALED, active_items)
        ).one(),
        "singles": session.exec(
            select(func.count()).where(InventoryItem.item_type == ITEM_TYPE_SINGLE, active_items)
        ).one(),
        "slabs": session.exec(
            select(func.count()).where(InventoryItem.item_type == ITEM_TYPE_SLAB, active_items)
        ).one(),
        "resticker_alerts": session.exec(
            select(func.count()).where(InventoryItem.resticker_alert_active == True, active_items)  # noqa: E712
        ).one(),
        "price_reviews": session.exec(
            select(func.count()).where(active_items, price_review_condition)
        ).one(),
        "price_stale": session.exec(
            select(func.count()).where(active_items, stale_price_condition)
        ).one(),
        "in_stock": session.exec(
            select(func.count()).where(InventoryItem.status == INVENTORY_IN_STOCK, active_items)
        ).one(),
        "listed": session.exec(
            select(func.count()).where(InventoryItem.status == INVENTORY_LISTED, active_items)
        ).one(),
    }

    list_return_url = request.url.path
    if request.url.query:
        list_return_url = f"{list_return_url}?{request.url.query}"

    return _templates.TemplateResponse(
        request,
        "inventory.html",
        {
            "current_user": _current_user(request),
            "items": items,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "status_filter": status,
            "game_filter": game,
            "type_filter": item_type,
            "q": q,
            "deleted": deleted,
            "updated": updated,
            "repriced": repriced,
            "price_errors": price_errors,
            "market_refreshed": market_refreshed,
            "market_errors": market_errors,
            "archived_filter": archived,
            "resticker_filter": resticker,
            "price_review_filter": price_review,
            "edit_mode": edit_mode,
            "edit_mode_url": _inventory_url_with_params(request, {"edit": "1"}),
            "view_mode_url": _inventory_url_with_params(request, {"edit": ""}),
            "can_manage_inventory": can_manage_inventory,
            "list_return_url": list_return_url,
            "games": GAMES,
            "statuses": sorted(ALL_INVENTORY_STATUSES),
            "inventory_summary": inventory_summary,
            "effective_price": effective_price,
            "price_review": _inventory_price_review_context,
        },
    )


# ---------------------------------------------------------------------------
# Barcode scan lookup (JSON)
# ---------------------------------------------------------------------------

@router.get("/inventory/api/lookup", response_class=JSONResponse)
async def inventory_lookup(
    request: Request,
    barcode: str = Query(default=""),
    session: Session = Depends(get_session),
):
    # Scanner barcode probe. Safe for employees — returns only whether a
    # barcode exists + its internal id, not cost basis or price. The returned
    # redirect URL points at /inventory/{id} which still requires inventory view, so
    # employees scanning a known barcode won't accidentally see cost data.
    if denial := _require_employee_permission(request, "ops.inventory.view", session):
        return denial
    if not barcode:
        return JSONResponse({"found": False})
    item = session.exec(
        select(InventoryItem).where(
            InventoryItem.barcode == barcode.strip(),
            InventoryItem.archived_at == None,  # noqa: E711
        )
    ).first()
    if not item:
        return JSONResponse({"found": False, "barcode": barcode})
    return JSONResponse({"found": True, "item_id": item.id, "redirect": f"/inventory/{item.id}"})


# ---------------------------------------------------------------------------
# Add stock / receiving
# ---------------------------------------------------------------------------

@router.get("/inventory/add-stock", response_class=HTMLResponse)
@router.get("/inventory/sealed", response_class=HTMLResponse)
async def inventory_sealed_page(
    request: Request,
    session: Session = Depends(get_session),
    game: str = Query(default="Pokemon"),
    q: str = Query(default=""),
    single_q: str = Query(default=""),
    search_type: str = Query(default="both"),
    received: int = Query(default=0),
    single_received: int = Query(default=0),
    error: str = Query(default=""),
    single_error: str = Query(default=""),
    bulk_received: int = Query(default=0),
    bulk_units: int = Query(default=0),
):
    if denial := _require_employee_permission(request, "ops.inventory.receive", session):
        return denial

    return _templates.TemplateResponse(
        request,
        "inventory_sealed.html",
        await _inventory_sealed_template_context(
            request,
            session,
            game=game,
            q=q,
            single_q=single_q,
            search_type=search_type,
            received=received,
            single_received=single_received,
            error=error,
            single_error=single_error,
            bulk_received=bulk_received,
            bulk_units=bulk_units,
        ),
    )


@router.post("/inventory/sealed/bulk-preview", response_class=HTMLResponse)
async def inventory_sealed_bulk_preview(
    request: Request,
    session: Session = Depends(get_session),
    game: str = Form(default="Pokemon"),
    bulk_text: str = Form(default=""),
    bulk_location: str = Form(default=""),
    bulk_source: str = Form(default=""),
    bulk_notes: str = Form(default=""),
):
    if denial := _require_employee_permission(request, "ops.inventory.receive", session):
        return denial

    selected_game = _normalize_add_stock_game(game)
    bulk_rows = await _build_bulk_sealed_preview(bulk_text, game=selected_game)
    return _templates.TemplateResponse(
        request,
        "inventory_sealed.html",
        await _inventory_sealed_template_context(
            request,
            session,
            game=selected_game,
            bulk_text=bulk_text,
            bulk_rows=bulk_rows,
            bulk_location=bulk_location,
            bulk_source=bulk_source,
            bulk_notes=bulk_notes,
        ),
    )


@router.post("/inventory/sealed/bulk-receive")
async def inventory_sealed_bulk_receive(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := _require_employee_permission(request, "ops.inventory.receive", session):
        return denial

    form = await request.form()
    selected_rows = {str(value) for value in form.getlist("receive_row")}
    payloads = form.getlist("bulk_row")
    selected_game = _normalize_add_stock_game(str(form.get("game") or "Pokemon"))
    location = str(form.get("bulk_location") or "").strip()
    source = str(form.get("bulk_source") or "").strip()
    notes = str(form.get("bulk_notes") or "").strip()
    added_products = 0
    added_units = 0
    try:
        for payload in payloads:
            row = json.loads(str(payload))
            row_index = str(row.get("row_index", ""))
            if row_index not in selected_rows:
                continue
            quantity_raw = str(form.get(f"bulk_quantity_{row_index}") or row.get("quantity") or "0")
            quantity = int(quantity_raw) if quantity_raw.isdigit() else 0
            list_price_raw = form.get(f"bulk_list_price_{row_index}")
            if list_price_raw is None:
                list_price_raw = row.get("list_price") or ""
            auto_price = _parse_float(str(row.get("auto_price") or ""))
            item, _movement, _created = _receive_sealed_stock(
                session,
                game=selected_game,
                product_name=str(row.get("product_name") or ""),
                set_name=str(row.get("set_name") or ""),
                sealed_product_kind=str(row.get("sealed_product_kind") or ""),
                upc=str(row.get("upc") or ""),
                image_url=str(row.get("image_url") or ""),
                quantity=quantity,
                list_price=_parse_float(str(list_price_raw)),
                auto_price=auto_price,
                price_payload={
                    "source": "bulk_add_stock",
                    "market_price": auto_price,
                    "row": row,
                },
                location=location,
                source=source,
                notes=notes,
                actor_label=_current_user_label(request),
            )
            added_products += 1
            added_units += quantity
    except (json.JSONDecodeError, ValueError) as exc:
        params = urlencode({"game": selected_game, "error": str(exc)})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)

    params = urlencode({"game": selected_game, "bulk_received": added_products, "bulk_units": added_units})
    return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)


@router.post("/inventory/sealed/receive")
async def inventory_sealed_receive(
    request: Request,
    session: Session = Depends(get_session),
    item_id: str = Form(default=""),
    game: str = Form(default="Pokemon"),
    product_name: str = Form(default=""),
    set_name: str = Form(default=""),
    sealed_product_kind: str = Form(default=""),
    search_type: str = Form(default="sealed"),
    upc: str = Form(default=""),
    image_url: str = Form(default=""),
    quantity: str = Form(default="1"),
    unit_cost: str = Form(default=""),
    list_price: str = Form(default=""),
    auto_price: str = Form(default=""),
    location: str = Form(default=""),
    source: str = Form(default=""),
    notes: str = Form(default=""),
):
    if denial := _require_employee_permission(request, "ops.inventory.receive", session):
        return denial

    selected_game = _normalize_add_stock_game(game)
    selected_search_type = _normalize_add_stock_search_type(search_type if isinstance(search_type, str) else "sealed")
    quantity_clean = str(quantity or "").strip()
    quantity_value = int(quantity_clean) if quantity_clean.isdigit() else 0
    item_id_int = int(item_id) if item_id.strip().isdigit() else None
    unit_cost_value = _parse_float(unit_cost)
    list_price_value = _parse_float(list_price)
    auto_price_value = _parse_float(auto_price)
    if unit_cost_value is not None and unit_cost_value < 0:
        params = urlencode({"game": selected_game, "search_type": selected_search_type, "q": product_name or "", "error": "Unit cost cannot be negative."})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)
    if list_price_value is not None and list_price_value < 0:
        params = urlencode({"game": selected_game, "search_type": selected_search_type, "q": product_name or "", "error": "Sell price cannot be negative."})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)
    try:
        item, _movement, _created = _receive_sealed_stock(
            session,
            item_id=item_id_int,
            game=selected_game,
            product_name=product_name,
            set_name=set_name,
            sealed_product_kind=sealed_product_kind,
            upc=upc,
            image_url=image_url,
            quantity=quantity_value,
            unit_cost=unit_cost_value,
            list_price=list_price_value,
            auto_price=auto_price_value,
            price_payload={
                "source": "add_stock_search",
                "market_price": auto_price_value,
                "product_name": product_name,
                "set_name": set_name,
                "sealed_product_kind": sealed_product_kind,
                "upc": upc,
            },
            location=location,
            source=source,
            notes=notes,
            actor_label=_current_user_label(request),
        )
    except ValueError as exc:
        params = urlencode({"game": selected_game, "search_type": selected_search_type, "q": product_name or "", "error": str(exc)})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)

    params = urlencode({"game": selected_game, "search_type": selected_search_type, "received": item.id, "q": item.card_name})
    return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)


@router.post("/inventory/singles/receive")
async def inventory_singles_receive(
    request: Request,
    session: Session = Depends(get_session),
    game: str = Form(default="Pokemon"),
    card_name: str = Form(default=""),
    set_name: str = Form(default=""),
    set_code: str = Form(default=""),
    card_number: str = Form(default=""),
    variant: str = Form(default=""),
    search_type: str = Form(default="cards"),
    variants_json: str = Form(default="[]"),
    condition: str = Form(default="NM"),
    image_url: str = Form(default=""),
    quantity: str = Form(default="1"),
    unit_cost: str = Form(default=""),
    list_price: str = Form(default=""),
    location: str = Form(default=""),
    source: str = Form(default="Manual Lookup"),
    notes: str = Form(default=""),
):
    if denial := _require_employee_permission(request, "ops.inventory.receive", session):
        return denial

    selected_game = _normalize_add_stock_game(game)
    selected_search_type = _normalize_add_stock_search_type(search_type if isinstance(search_type, str) else "cards")
    quantity_clean = str(quantity or "").strip()
    quantity_value = int(quantity_clean) if quantity_clean.isdigit() else 0
    unit_cost_value = _parse_float(unit_cost)
    list_price_value = _parse_float(list_price)
    if unit_cost_value is not None and unit_cost_value < 0:
        params = urlencode({"game": selected_game, "search_type": selected_search_type, "q": card_name or "", "single_error": "Unit cost cannot be negative."})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)
    if list_price_value is not None and list_price_value < 0:
        params = urlencode({"game": selected_game, "search_type": selected_search_type, "q": card_name or "", "single_error": "Sell price cannot be negative."})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)

    try:
        parsed_variants = json.loads(variants_json or "[]")
    except json.JSONDecodeError:
        parsed_variants = []
    if not isinstance(parsed_variants, list):
        parsed_variants = []

    auto_price, low_price, selected_variant = _variant_price_for_condition(
        parsed_variants,
        variant_name=variant,
        condition=condition,
    )

    try:
        item, _movement, _created = _receive_single_stock(
            session,
            game=selected_game,
            card_name=card_name,
            set_name=set_name,
            set_code=set_code,
            card_number=card_number,
            variant=variant,
            condition=condition,
            image_url=image_url,
            quantity=quantity_value,
            unit_cost=unit_cost_value,
            list_price=list_price_value,
            auto_price=auto_price,
            low_price=low_price,
            location=location,
            source=source,
            notes=notes,
            price_payload={
                "selected_variant": selected_variant,
                "all_variants": parsed_variants,
                "condition": _condition_code(condition),
            },
            actor_label=_current_user_label(request),
        )
    except ValueError as exc:
        params = urlencode({"game": selected_game, "search_type": selected_search_type, "q": card_name or "", "single_error": str(exc)})
        return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)

    params = urlencode({"game": selected_game, "search_type": selected_search_type, "single_received": item.id, "q": item.card_name})
    return RedirectResponse(f"/inventory/add-stock?{params}", status_code=303)


# ---------------------------------------------------------------------------
# Scan mode page
# ---------------------------------------------------------------------------

@router.get("/inventory/scan", response_class=HTMLResponse)
async def inventory_scan_page(request: Request, session: Session = Depends(get_session)):
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan.html",
        {
            "current_user": _current_user(request),
            "team_shell": request.query_params.get("team_shell") == "1",
        },
    )


# ---------------------------------------------------------------------------
# Print labels
# ---------------------------------------------------------------------------

@router.get("/inventory/labels", response_class=HTMLResponse)
async def inventory_labels(
    request: Request,
    session: Session = Depends(get_session),
    ids: str = Query(default=""),
    status: str = Query(default=""),
    layout: str = Query(default="sheet"),
):
    if denial := _require_employee_permission(request, "ops.inventory.view", session):
        return denial

    items: list[InventoryItem] = []
    if ids:
        id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
        if id_list:
            items = session.exec(
                select(InventoryItem).where(
                    InventoryItem.id.in_(id_list),
                    InventoryItem.archived_at == None,  # noqa: E711
                )
            ).all()
    elif status and status in ALL_INVENTORY_STATUSES:
        items = session.exec(
            select(InventoryItem).where(
                InventoryItem.status == status,
                InventoryItem.archived_at == None,  # noqa: E711
            )
        ).all()

    labels = label_context_for_items(items)
    layout = layout if layout in {"sheet", "thermal"} else "sheet"
    return _templates.TemplateResponse(
        request,
        "inventory_labels.html",
        {"current_user": _current_user(request), "labels": labels, "layout": layout},
    )


# ---------------------------------------------------------------------------
# Add new item
# ---------------------------------------------------------------------------

@router.get("/inventory/new", response_class=HTMLResponse)
async def inventory_new_form(request: Request):
    with managed_session() as permission_session:
        if denial := _require_inventory_manage(request, permission_session):
            return denial
        can_manage_inventory = _can_inventory_manage(request, permission_session)
        can_view_inventory = _can_inventory_view(request, permission_session)
    return _templates.TemplateResponse(
        request,
        "inventory_new.html",
        {
            "current_user": _current_user(request),
            "can_manage_inventory": can_manage_inventory,
            "can_view_inventory": can_view_inventory,
            "games": GAMES,
            "conditions": CONDITIONS,
            "grading_companies": GRADING_COMPANIES,
            "item_types": [ITEM_TYPE_SINGLE],
            "error": None,
        },
    )


@router.post("/inventory/new")
async def inventory_new_submit(
    request: Request,
    session: Session = Depends(get_session),
    item_type: str = Form(...),
    game: str = Form(...),
    card_name: str = Form(...),
    set_name: str = Form(default=""),
    set_code: str = Form(default=""),
    card_number: str = Form(default=""),
    variant: str = Form(default=""),
    language: str = Form(default="English"),
    condition: str = Form(default=""),
    quantity: int = Form(default=1),
    grading_company: str = Form(default=""),
    grade: str = Form(default=""),
    cert_number: str = Form(default=""),
    cost_basis: str = Form(default=""),
    list_price: str = Form(default=""),
    notes: str = Form(default=""),
    auto_price_on_save: str = Form(default=""),
    push_shopify_on_save: str = Form(default=""),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    if item_type == ITEM_TYPE_SLAB:
        params = urlencode(
            {
                "error": (
                    "Use Add Slabs for graded inventory so cert lookup, grade comps, "
                    "and resticker tracking stay together."
                )
            }
        )
        return RedirectResponse(f"/inventory/scan/slabs?{params}", status_code=303)

    card_name = card_name.strip()
    if not card_name:
        return _templates.TemplateResponse(
            request,
            "inventory_new.html",
            {
                "current_user": _current_user(request),
                "can_manage_inventory": _can_inventory_manage(request, session),
                "can_view_inventory": _can_inventory_view(request, session),
                "games": GAMES,
                "conditions": CONDITIONS,
                "grading_companies": GRADING_COMPANIES,
                "item_types": [ITEM_TYPE_SINGLE],
                "error": "Card name is required.",
            },
            status_code=400,
        )

    item = InventoryItem(
        barcode="PENDING",  # replaced after insert gives us an id
        item_type=item_type,
        game=game,
        card_name=card_name,
        set_name=set_name.strip() or None,
        set_code=set_code.strip() or None,
        card_number=card_number.strip() or None,
        variant=variant.strip() or None,
        language=language or "English",
        condition=condition.strip() or None,
        quantity=max(1, quantity),
        grading_company=grading_company.strip() or None,
        grade=grade.strip() or None,
        cert_number=cert_number.strip() or None,
        cost_basis=_parse_float(cost_basis),
        list_price=_parse_float(list_price),
        notes=notes.strip() or None,
        status=INVENTORY_IN_STOCK,
        created_at=utcnow(),
    )
    session.add(item)
    session.commit()
    session.refresh(item)

    # Assign barcode now that we have the id
    item.barcode = generate_barcode_value(item.id)
    session.add(item)
    session.commit()
    session.refresh(item)

    # Auto-price
    if auto_price_on_save == "on" and settings.inventory_auto_price_enabled:
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                result = await fetch_price_for_item(
                    item,
                    client,
                    api_key=settings.scrydex_api_key,
                    base_url=settings.scrydex_base_url,
                )
            if result:
                actor = _current_user(request)
                record_inventory_price_result(
                    session,
                    item,
                    result,
                    request=request,
                    actor_user_id=getattr(actor, "id", None),
                )
                session.commit()
                session.refresh(item)
        except Exception as exc:
            logger.warning("[inventory] auto-price failed on new item %s: %s", item.id, exc)

    # Push to Shopify
    if push_shopify_on_save == "on" or settings.inventory_auto_shopify_push:
        if shopify_admin_configured(settings):
            try:
                await _sync_inventory_item_to_shopify_now(session, item, source="Inventory New")
            except Exception as exc:
                logger.warning("[inventory] shopify push failed on new item %s: %s", item.id, exc)

    return RedirectResponse(f"/inventory/{item.id}", status_code=303)


async def _reprice_inventory_items(
    session: Session,
    items: list[InventoryItem],
    *,
    request: Optional[Request] = None,
    markup_percent: float = 0.0,
) -> tuple[int, int]:
    repriced_count = 0
    error_count = 0
    for item in items:
        if item.archived_at is not None:
            continue
        target_price = _inventory_price_from_market(item, markup_percent=markup_percent)
        if target_price is None:
            error_count += 1
            continue
        item.list_price = target_price
        item.updated_at = utcnow()
        if (
            item.resticker_alert_active
            and item.resticker_alert_price is not None
            and target_price >= item.resticker_alert_price
        ):
            clear_slab_resticker_alert(
                item,
                reason="Store price matched current market price.",
            )
        session.add(item)
        enqueue_shopify_sync_job(session, item, action="reprice", source="Inventory Reprice")
        repriced_count += 1
    session.commit()
    return repriced_count, error_count


async def _refresh_inventory_market_prices(
    session: Session,
    items: list[InventoryItem],
    *,
    request: Optional[Request] = None,
) -> tuple[int, int]:
    refreshed_count = 0
    error_count = 0
    actor = _current_user(request) if request is not None else None
    actor_user_id = getattr(actor, "id", None)
    async with httpx.AsyncClient(timeout=20.0) as client:
        for item in items:
            if item.archived_at is not None:
                continue
            try:
                result = await fetch_price_for_item(
                    item,
                    client,
                    api_key=settings.scrydex_api_key,
                    base_url=settings.scrydex_base_url,
                )
                if result and _safe_price(result.get("market_price")) is not None:
                    record_inventory_price_result(
                        session,
                        item,
                        result,
                        request=request,
                        actor_user_id=actor_user_id,
                    )
                    enqueue_shopify_sync_job(
                        session,
                        item,
                        action="market_refresh",
                        source="Inventory Market Refresh",
                    )
                    session.commit()
                    refreshed_count += 1
                    logger.info("[inventory] refreshed market price for item %s", item.id)
                else:
                    error_count += 1
                    logger.info(
                        "[inventory] market refresh skipped item %s because no market price was found",
                        item.id,
                    )
            except Exception as exc:
                session.rollback()
                error_count += 1
                logger.warning("[inventory] market refresh failed for item %s: %s", item.id, exc)
    return refreshed_count, error_count


def _bulk_edit_inventory_items(
    session: Session,
    items: list[InventoryItem],
    form: Any,
    *,
    actor: Optional[str],
) -> tuple[int, Optional[str]]:
    now = utcnow()
    updated_count = 0
    for item in items:
        if item.archived_at is not None:
            continue

        changed = False
        before_qty = max(0, item.quantity or 0)
        qty_raw = str(form.get(f"bulk_qty_{item.id}") or "").strip()
        if qty_raw:
            try:
                after_qty = int(qty_raw)
            except ValueError:
                return updated_count, f"Quantity must be a whole number for {item.card_name}."
            if after_qty < 0:
                return updated_count, f"Quantity cannot be negative for {item.card_name}."
            if after_qty != before_qty:
                item.quantity = after_qty
                changed = True
                session.add(
                    InventoryStockMovement(
                        item_id=item.id,
                        reason="bulk_edit_qty",
                        quantity_delta=after_qty - before_qty,
                        quantity_before=before_qty,
                        quantity_after=after_qty,
                        location=item.location,
                        source="Inventory Bulk Edit",
                        notes="Quantity changed from inventory edit mode.",
                        created_by=actor,
                        created_at=now,
                    )
                )

        cost_raw = str(form.get(f"bulk_cost_{item.id}") or "").strip()
        next_cost = _parse_float(cost_raw)
        if cost_raw and next_cost is None:
            return updated_count, f"Cost must be a valid number for {item.card_name}."
        if next_cost != item.cost_basis:
            item.cost_basis = next_cost
            changed = True

        price_raw = str(form.get(f"bulk_price_{item.id}") or "").strip()
        next_price = _parse_float(price_raw)
        if price_raw and next_price is None:
            return updated_count, f"Price must be a valid number for {item.card_name}."
        if next_price != item.list_price:
            item.list_price = next_price
            changed = True

        if changed:
            if item.resticker_alert_active and (
                item.item_type != ITEM_TYPE_SLAB
                or (
                    item.list_price is not None
                    and item.resticker_alert_price is not None
                    and item.list_price >= item.resticker_alert_price
                )
            ):
                clear_slab_resticker_alert(item, reason="Sticker price updated from inventory bulk edit.")
            item.updated_at = now
            session.add(item)
            enqueue_shopify_sync_job(session, item, action="bulk_edit", source="Inventory Bulk Edit")
            updated_count += 1

    session.commit()
    return updated_count, None


async def _sync_inventory_item_to_shopify_now(
    session: Session,
    item: InventoryItem,
    *,
    source: str = "Inventory",
) -> tuple[bool, str]:
    return await sync_inventory_item_to_shopify(session, item, source=source)


@router.post("/inventory/bulk-action")
async def inventory_bulk_action(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    form = await request.form()
    item_ids = [int(value) for value in form.getlist("item_id") if str(value).isdigit()]
    action = str(form.get("bulk_action") or "").strip()
    if not item_ids:
        return RedirectResponse("/inventory", status_code=303)

    items = session.exec(select(InventoryItem).where(InventoryItem.id.in_(item_ids))).all()
    if action == "print_labels":
        params = urlencode({"ids": ",".join(str(item.id) for item in items if item.archived_at is None)})
        return RedirectResponse(f"/inventory/labels?{params}", status_code=303)
    if action == "refresh_market":
        refreshed_count, error_count = await _refresh_inventory_market_prices(
            session,
            items,
            request=request,
        )
        params = urlencode({"market_refreshed": refreshed_count, "market_errors": error_count})
        return RedirectResponse(f"/inventory?{params}", status_code=303)
    if action in PRICE_MARKUP_ACTIONS:
        repriced_count, error_count = await _reprice_inventory_items(
            session,
            items,
            request=request,
            markup_percent=PRICE_MARKUP_ACTIONS[action],
        )
        params = urlencode({"repriced": repriced_count, "price_errors": error_count})
        return RedirectResponse(f"/inventory?{params}", status_code=303)
    if action == "bulk_edit":
        updated_count, error = _bulk_edit_inventory_items(
            session,
            items,
            form,
            actor=_current_user_label(request),
        )
        if error:
            return HTMLResponse(error, status_code=400)
        params = urlencode({"updated": updated_count})
        return RedirectResponse(f"/inventory?{params}", status_code=303)

    now = utcnow()
    actor = _current_user_label(request)
    updated_count = 0
    reason = str(form.get("bulk_reason") or "").strip()
    location = str(form.get("bulk_location") or "").strip()

    if action == "set_location" and not location:
        return HTMLResponse("Location is required for bulk location updates.", status_code=400)
    if action not in {"set_location", "mark_held", "archive"}:
        return HTMLResponse("Choose a valid bulk action.", status_code=400)

    for item in items:
        if item.archived_at is not None:
            continue
        before_qty = max(0, item.quantity or 0)
        if action == "set_location":
            item.location = location
            movement_reason = "bulk_location"
        elif action == "mark_held":
            item.status = INVENTORY_HELD
            movement_reason = "bulk_status"
        else:
            item.archived_at = now
            item.archived_by = actor
            item.archive_reason = reason or "Bulk archive"
            movement_reason = "bulk_archive"
        item.updated_at = now
        session.add(item)
        session.add(
            InventoryStockMovement(
                item_id=item.id,
                reason=movement_reason,
                quantity_delta=0,
                quantity_before=before_qty,
                quantity_after=before_qty,
                location=location or item.location,
                source="Bulk Action",
                notes=reason or None,
                created_by=actor,
                created_at=now,
            )
        )
        enqueue_shopify_sync_job(session, item, action=action, source="Inventory Bulk Action")
        updated_count += 1

    session.commit()
    params = urlencode({"updated": updated_count})
    return RedirectResponse(f"/inventory?{params}", status_code=303)


# ---------------------------------------------------------------------------
# Shopify sync reconciliation
# ---------------------------------------------------------------------------

@router.get("/inventory/shopify-sync", response_class=HTMLResponse)
async def inventory_shopify_sync_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    active_items = InventoryItem.archived_at == None  # noqa: E711
    linked_items = session.exec(
        select(InventoryItem)
        .where(active_items, InventoryItem.shopify_variant_id != None)  # noqa: E711
        .order_by(InventoryItem.shopify_sync_status.asc(), InventoryItem.updated_at.desc())
        .limit(100)
    ).all()
    unlinked_items = session.exec(
        select(InventoryItem)
        .where(active_items, InventoryItem.shopify_variant_id == None)  # noqa: E711
        .order_by(InventoryItem.updated_at.desc())
        .limit(100)
    ).all()
    issues = session.exec(
        select(ShopifySyncIssue)
        .where(ShopifySyncIssue.status == SHOPIFY_SYNC_ISSUE_OPEN)
        .order_by(ShopifySyncIssue.last_seen_at.desc())
        .limit(100)
    ).all()
    recent_jobs = session.exec(
        select(ShopifySyncJob)
        .order_by(ShopifySyncJob.created_at.desc())
        .limit(30)
    ).all()
    summary = {
        "linked": len(linked_items),
        "unlinked": len(unlinked_items),
        "issues": len(issues),
        "errors": session.exec(
            select(func.count()).where(
                active_items,
                InventoryItem.shopify_sync_status == SHOPIFY_SYNC_ERROR,
            )
        ).one(),
    }
    return _templates.TemplateResponse(
        request,
        "inventory_shopify_sync.html",
        {
            "current_user": _current_user(request),
            "linked_items": linked_items,
            "unlinked_items": unlinked_items,
            "issues": issues,
            "recent_jobs": recent_jobs,
            "summary": summary,
            "effective_price": effective_price,
        },
    )


@router.post("/inventory/shopify-sync/scan")
async def inventory_shopify_sync_scan_catalog(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial
    access_token = resolve_shopify_access_token(settings)
    if not settings.shopify_store_domain or not access_token:
        return HTMLResponse("SHOPIFY_STORE_DOMAIN and a Shopify Admin token must be configured.", status_code=400)
    try:
        variants = await list_shopify_product_variants(
            store_domain=settings.shopify_store_domain,
            access_token=access_token,
        )
    except Exception as exc:
        return HTMLResponse(f"Could not scan Shopify catalog: {html.escape(str(exc))}", status_code=502)

    linked = 0
    queued = 0
    for variant in variants:
        sku = (variant.sku or "").strip()
        item = session.exec(select(InventoryItem).where(InventoryItem.barcode == sku)).first() if sku else None
        if item and item.archived_at is None:
            apply_shopify_variant_ref(item, variant)
            item.shopify_sync_status = SHOPIFY_SYNC_LINKED
            item.updated_at = utcnow()
            session.add(item)
            enqueue_shopify_sync_job(session, item, action="catalog_link", source="Shopify Catalog Scan")
            linked += 1
            continue
        record_title = variant.product_title or variant.title or sku or "Shopify product"
        record_shopify_sync_issue(
            session,
            issue_type=SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT,
            shopify_sku=sku or None,
            shopify_title=record_title,
            shopify_product_id=variant.product_id,
            shopify_variant_id=variant.variant_id,
            shopify_inventory_item_id=variant.inventory_item_id,
            inventory_item_id=None,
            message="Shopify catalog item is not linked to a Degen inventory item.",
            payload={
                "sku": sku,
                "product_title": variant.product_title,
                "variant_title": variant.title,
                "product_status": variant.product_status,
            },
        )
        queued += 1
    session.commit()
    params = urlencode({"catalog_linked": linked, "catalog_queued": queued})
    return RedirectResponse(f"/inventory/shopify-sync?{params}", status_code=303)


@router.post("/inventory/shopify-sync/retry")
async def inventory_shopify_sync_retry(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial
    form = await request.form()
    item_ids = [int(value) for value in form.getlist("item_id") if str(value).isdigit()]
    issue_id = str(form.get("issue_id") or "").strip()
    if issue_id.isdigit():
        issue = session.get(ShopifySyncIssue, int(issue_id))
        if issue and issue.inventory_item_id:
            item_ids.append(issue.inventory_item_id)
    synced = 0
    failed = 0
    for item_id in sorted(set(item_ids)):
        item = session.get(InventoryItem, item_id)
        if not item or item.archived_at is not None:
            continue
        ok, _message = await _sync_inventory_item_to_shopify_now(
            session,
            item,
            source="Shopify Sync Retry",
        )
        if ok:
            synced += 1
        else:
            failed += 1
    params = urlencode({"synced": synced, "failed": failed})
    return RedirectResponse(f"/inventory/shopify-sync?{params}", status_code=303)


@router.post("/inventory/shopify-sync/link")
async def inventory_shopify_sync_link(
    request: Request,
    session: Session = Depends(get_session),
    item_id: int = Form(...),
    issue_id: str = Form(default=""),
    shopify_product_id: str = Form(default=""),
    shopify_variant_id: str = Form(default=""),
    shopify_inventory_item_id: str = Form(default=""),
    shopify_location_id: str = Form(default=""),
    shopify_sku: str = Form(default=""),
):
    if denial := _require_inventory_manage(request, session):
        return denial
    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Inventory item not found.", status_code=404)
    item.shopify_product_id = shopify_product_id.strip() or item.shopify_product_id
    item.shopify_variant_id = shopify_variant_id.strip() or item.shopify_variant_id
    item.shopify_inventory_item_id = shopify_inventory_item_id.strip() or item.shopify_inventory_item_id
    item.shopify_location_id = shopify_location_id.strip() or item.shopify_location_id
    item.shopify_sku = shopify_sku.strip() or item.shopify_sku or item.barcode
    item.shopify_sync_status = SHOPIFY_SYNC_LINKED
    item.updated_at = utcnow()
    session.add(item)
    if issue_id.isdigit():
        issue = session.get(ShopifySyncIssue, int(issue_id))
        if issue:
            issue.inventory_item_id = item.id
            issue.status = SHOPIFY_SYNC_ISSUE_LINKED
            issue.resolved_by = _current_user_label(request)
            issue.resolved_at = utcnow()
            issue.resolution_note = "Linked to existing Degen inventory item."
            session.add(issue)
    enqueue_shopify_sync_job(session, item, action="link", source="Shopify Sync Link")
    session.commit()
    return RedirectResponse("/inventory/shopify-sync?linked=1", status_code=303)


@router.post("/inventory/shopify-sync/ignore")
async def inventory_shopify_sync_ignore(
    request: Request,
    session: Session = Depends(get_session),
    issue_id: int = Form(...),
    resolution_note: str = Form(default=""),
):
    if denial := _require_inventory_manage(request, session):
        return denial
    issue = session.get(ShopifySyncIssue, issue_id)
    if not issue:
        return HTMLResponse("Sync issue not found.", status_code=404)
    issue.status = SHOPIFY_SYNC_ISSUE_IGNORED
    issue.resolved_by = _current_user_label(request)
    issue.resolved_at = utcnow()
    issue.resolution_note = resolution_note.strip() or "Ignored from Shopify sync queue."
    session.add(issue)
    session.commit()
    return RedirectResponse("/inventory/shopify-sync?ignored=1", status_code=303)


@router.post("/inventory/shopify-sync/import")
async def inventory_shopify_sync_import(
    request: Request,
    session: Session = Depends(get_session),
    issue_id: int = Form(...),
):
    if denial := _require_inventory_manage(request, session):
        return denial
    issue = session.get(ShopifySyncIssue, issue_id)
    if not issue:
        return HTMLResponse("Sync issue not found.", status_code=404)
    item = InventoryItem(
        barcode=issue.shopify_sku if (issue.shopify_sku or "").startswith("DGN-") else "PENDING",
        item_type=ITEM_TYPE_SEALED,
        game="Other",
        card_name=issue.shopify_title or issue.shopify_sku or "Shopify Product",
        quantity=max(0, issue.quantity or 0),
        list_price=issue.unit_price,
        shopify_product_id=issue.shopify_product_id,
        shopify_variant_id=issue.shopify_variant_id,
        shopify_inventory_item_id=issue.shopify_inventory_item_id,
        shopify_location_id=issue.shopify_location_id,
        shopify_sku=issue.shopify_sku,
        shopify_sync_status=SHOPIFY_SYNC_LINKED,
        status=INVENTORY_LISTED,
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    session.add(item)
    session.commit()
    session.refresh(item)
    if item.barcode == "PENDING":
        item.barcode = generate_barcode_value(item.id)
    session.add(
        InventoryStockMovement(
            item_id=item.id,
            reason="shopify_import",
            quantity_delta=item.quantity,
            quantity_before=0,
            quantity_after=item.quantity,
            source="Shopify Sync Import",
            notes=f"Imported from Shopify issue {issue.id}",
            created_by=_current_user_label(request),
            created_at=utcnow(),
        )
    )
    issue.inventory_item_id = item.id
    issue.status = SHOPIFY_SYNC_ISSUE_RESOLVED
    issue.resolved_by = _current_user_label(request)
    issue.resolved_at = utcnow()
    issue.resolution_note = "Imported into Degen inventory."
    session.add(item)
    session.add(issue)
    enqueue_shopify_sync_job(session, item, action="import", source="Shopify Sync Import")
    session.commit()
    return RedirectResponse(f"/inventory/{item.id}", status_code=303)


# ---------------------------------------------------------------------------
# Item detail + edit
# ---------------------------------------------------------------------------

@router.get("/inventory/{item_id}", response_class=HTMLResponse)
async def inventory_item_detail(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_employee_permission(request, "ops.inventory.view", session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)
    can_manage_inventory = _can_inventory_manage(request, session)
    if item.archived_at is not None and not can_manage_inventory:
        return HTMLResponse("Item not found.", status_code=404)

    history = session.exec(
        select(PriceHistory)
        .where(PriceHistory.item_id == item_id)
        .order_by(PriceHistory.fetched_at.desc())
        .limit(20)
    ).all()
    stock_movements = session.exec(
        select(InventoryStockMovement)
        .where(InventoryStockMovement.item_id == item_id)
        .order_by(InventoryStockMovement.created_at.desc())
        .limit(30)
    ).all()

    barcode_svg = render_barcode_svg(item.barcode)

    return _templates.TemplateResponse(
        request,
        "inventory_item.html",
        {
            "current_user": _current_user(request),
            "item": item,
            "price_history": history,
            "card_ladder_history": _card_ladder_history_context(history),
            "stock_movements": stock_movements,
            "barcode_svg": barcode_svg,
            "effective_price": effective_price(item),
            "can_manage_inventory": can_manage_inventory,
            "can_view_inventory": _can_inventory_view(request, session),
            "games": GAMES,
            "conditions": CONDITIONS,
            "grading_companies": GRADING_COMPANIES,
            "item_types": [ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB, ITEM_TYPE_SEALED],
            "statuses": sorted(ALL_INVENTORY_STATUSES),
        },
    )


@router.post("/inventory/{item_id}/edit")
async def inventory_item_edit(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
    card_name: str = Form(...),
    set_name: str = Form(default=""),
    set_code: str = Form(default=""),
    card_number: str = Form(default=""),
    variant: str = Form(default=""),
    game: str = Form(default=""),
    item_type: str = Form(default=""),
    language: str = Form(default="English"),
    sealed_product_kind: str = Form(default=""),
    upc: str = Form(default=""),
    location: str = Form(default=""),
    condition: str = Form(default=""),
    quantity: Optional[int] = Form(default=None),
    grading_company: str = Form(default=""),
    grade: str = Form(default=""),
    cert_number: str = Form(default=""),
    cost_basis: str = Form(default=""),
    list_price: str = Form(default=""),
    notes: str = Form(default=""),
    status: str = Form(default=""),
    image_url: str = Form(default=""),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    item.card_name = card_name.strip() or item.card_name
    item.set_name = set_name.strip() or None
    item.set_code = set_code.strip() or None
    item.card_number = card_number.strip() or None
    item.variant = variant.strip() or None
    item.game = game or item.game
    item.item_type = item_type or item.item_type
    item.language = language or "English"
    item.sealed_product_kind = sealed_product_kind.strip() or None
    item.upc = upc.strip() or None
    item.location = location.strip() or None
    item.condition = condition.strip() or None
    before_qty = max(0, item.quantity or 0)
    if quantity is not None:
        after_qty = max(0, quantity)
        if after_qty != before_qty:
            item.quantity = after_qty
            session.add(
                InventoryStockMovement(
                    item_id=item.id,
                    reason="manual_edit",
                    quantity_delta=after_qty - before_qty,
                    quantity_before=before_qty,
                    quantity_after=after_qty,
                    location=location.strip() or item.location,
                    source="Inventory Edit",
                    notes="Quantity changed from item edit form.",
                    created_by=_current_user_label(request),
                    created_at=utcnow(),
                )
            )
    item.grading_company = grading_company.strip() or None
    item.grade = grade.strip() or None
    item.cert_number = cert_number.strip() or None
    item.cost_basis = _parse_float(cost_basis)
    item.list_price = _parse_float(list_price)
    if item.resticker_alert_active and (
        item.item_type != ITEM_TYPE_SLAB
        or (
            item.list_price is not None
            and item.resticker_alert_price is not None
            and item.list_price >= item.resticker_alert_price
        )
    ):
        clear_slab_resticker_alert(item, reason="Sticker price updated from inventory edit.")
    item.notes = notes.strip() or None
    if status and status in ALL_INVENTORY_STATUSES:
        item.status = status
    item.image_url = image_url.strip() or item.image_url
    item.updated_at = utcnow()
    session.add(item)
    enqueue_shopify_sync_job(session, item, action="edit", source="Inventory Edit")
    session.commit()
    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


@router.post("/inventory/{item_id}/adjust-stock")
async def inventory_item_adjust_stock(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
    quantity_delta: Optional[int] = Form(default=None),
    target_quantity: Optional[int] = Form(default=None),
    reason: str = Form(default="adjustment"),
    location: str = Form(default=""),
    source: str = Form(default="Manual Adjustment"),
    notes: str = Form(default=""),
    return_to: str = Form(default=""),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)
    if item.archived_at is not None:
        return HTMLResponse("Restore this item before adjusting stock.", status_code=400)

    before_qty = max(0, item.quantity or 0)
    if target_quantity is not None:
        if target_quantity < 0:
            return HTMLResponse("Quantity cannot be negative.", status_code=400)
        after_qty = target_quantity
        quantity_delta_value = after_qty - before_qty
        if not (reason or "").strip():
            reason = "stock_count"
    else:
        if quantity_delta is None:
            return HTMLResponse("Adjustment quantity is required.", status_code=400)
        if quantity_delta == 0:
            return HTMLResponse("Adjustment quantity cannot be zero.", status_code=400)
        quantity_delta_value = quantity_delta
        after_qty = before_qty + quantity_delta_value
    if after_qty < 0:
        return HTMLResponse("Adjustment would make quantity negative.", status_code=400)

    reason_clean = (reason or "adjustment").strip() or "adjustment"
    redirect_to = _safe_inventory_return_url(return_to, f"/inventory/{item_id}")
    if quantity_delta_value == 0:
        return RedirectResponse(redirect_to, status_code=303)

    location_clean = location.strip() or item.location
    item.quantity = after_qty
    if location.strip():
        item.location = location.strip()
    if quantity_delta_value > 0 and item.status == INVENTORY_SOLD:
        item.status = INVENTORY_IN_STOCK
        item.sold_at = None
        item.sold_price = None
    elif after_qty == 0 and reason_clean in {"sale", "sold"}:
        item.status = INVENTORY_SOLD
        item.sold_at = item.sold_at or utcnow()
    item.updated_at = utcnow()
    session.add(item)
    session.add(
        InventoryStockMovement(
            item_id=item.id,
            reason=reason_clean,
            quantity_delta=quantity_delta_value,
            quantity_before=before_qty,
            quantity_after=after_qty,
            location=location_clean,
            source=source.strip() or None,
            notes=notes.strip() or None,
            created_by=_current_user_label(request),
            created_at=utcnow(),
        )
    )
    enqueue_shopify_sync_job(session, item, action="quantity", source=source.strip() or "Inventory Adjustment")
    session.commit()
    if redirect_to == "/inventory" or redirect_to.startswith("/inventory?"):
        redirect_to = f"{redirect_to}{'&' if '?' in redirect_to else '?'}updated=1"
    return RedirectResponse(redirect_to, status_code=303)


@router.post("/inventory/{item_id}/delete")
async def inventory_item_delete(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    archived_name = item.card_name
    form = await request.form()
    archive_reason = str(form.get("archive_reason") or "").strip()
    item.archived_at = utcnow()
    item.archived_by = _current_user_label(request)
    item.archive_reason = archive_reason or None
    item.updated_at = utcnow()
    session.add(item)
    enqueue_shopify_sync_job(session, item, action="archive", source="Inventory Archive")
    session.commit()
    logger.info("[inventory] archived item %s (%s)", item_id, archived_name)

    params = urlencode({"deleted": archived_name})
    return RedirectResponse(f"/inventory?{params}", status_code=303)


@router.post("/inventory/{item_id}/restore")
async def inventory_item_restore(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    item.archived_at = None
    item.archived_by = None
    item.archive_reason = None
    item.updated_at = utcnow()
    session.add(item)
    enqueue_shopify_sync_job(session, item, action="restore", source="Inventory Restore")
    session.commit()
    logger.info("[inventory] restored archived item %s (%s)", item_id, item.card_name)
    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


# ---------------------------------------------------------------------------
# On-demand reprice
# ---------------------------------------------------------------------------

@router.post("/inventory/{item_id}/reprice")
async def inventory_reprice(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
    return_to: str = Form(default=""),
    markup_percent: str = Form(default="0"),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    markup_value = _parse_float(markup_percent) or 0.0
    repriced_count, error_count = await _reprice_inventory_items(
        session,
        [item],
        request=request,
        markup_percent=markup_value,
    )
    if repriced_count:
        logger.info("[inventory] repriced item %s", item_id)
    if error_count:
        logger.info("[inventory] reprice skipped item %s because market price is missing", item_id)

    redirect_to = _safe_inventory_return_url(return_to, f"/inventory/{item_id}")
    if redirect_to == "/inventory" or redirect_to.startswith("/inventory?"):
        params = urlencode({"repriced": repriced_count, "price_errors": error_count})
        redirect_to = f"{redirect_to}{'&' if '?' in redirect_to else '?'}{params}"
    return RedirectResponse(redirect_to, status_code=303)


@router.post("/inventory/{item_id}/refresh-market")
async def inventory_refresh_market_price(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
    return_to: str = Form(default=""),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)
    if item.archived_at is not None:
        return HTMLResponse("Restore this item before refreshing market price.", status_code=400)

    refreshed_count, error_count = await _refresh_inventory_market_prices(
        session,
        [item],
        request=request,
    )
    redirect_to = _safe_inventory_return_url(return_to, f"/inventory/{item_id}")
    params = urlencode({"market_refreshed": refreshed_count, "market_errors": error_count})
    redirect_to = f"{redirect_to}{'&' if '?' in redirect_to else '?'}{params}"
    return RedirectResponse(redirect_to, status_code=303)


@router.post("/inventory/{item_id}/resticker/apply")
async def inventory_resticker_apply(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)
    if item.item_type != ITEM_TYPE_SLAB:
        return HTMLResponse("Resticker alerts are only available for slabs.", status_code=400)

    target = item.resticker_alert_price or item.auto_price
    if target is None:
        return RedirectResponse(f"/inventory/{item_id}", status_code=303)

    item.list_price = round(float(target), 2)
    item.updated_at = utcnow()
    clear_slab_resticker_alert(item, reason="Sticker price applied.")
    session.add(item)
    session.commit()
    return RedirectResponse(f"/inventory/labels?ids={item_id}&layout=thermal", status_code=303)


@router.post("/inventory/{item_id}/resticker/dismiss")
async def inventory_resticker_dismiss(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    clear_slab_resticker_alert(item, reason="Resticker alert dismissed.")
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


# ---------------------------------------------------------------------------
# Push to Shopify
# ---------------------------------------------------------------------------

@router.post("/inventory/{item_id}/push-shopify")
async def inventory_push_shopify(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_inventory_manage(request, session):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    if not shopify_admin_configured(settings):
        return HTMLResponse(
            "SHOPIFY_STORE_DOMAIN and a Shopify Admin token must be configured.", status_code=400
        )

    await _sync_inventory_item_to_shopify_now(session, item, source="Inventory Item Detail")

    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


# ---------------------------------------------------------------------------
# Barcode SVG endpoint
# ---------------------------------------------------------------------------

@router.get("/inventory/{item_id}/barcode.svg")
async def inventory_barcode_svg(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_employee_permission(request, "ops.inventory.view", session):
        return denial
    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Not found.", status_code=404)
    svg = render_barcode_svg(item.barcode)
    return Response(content=svg, media_type="image/svg+xml")


# ---------------------------------------------------------------------------
# Camera scan pages
# ---------------------------------------------------------------------------

@router.get("/inventory/scan/singles", response_class=HTMLResponse)
async def inventory_scan_singles_page(request: Request, session: Session = Depends(get_session)):
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_singles.html",
        {"current_user": _current_user(request)},
    )


@router.get("/inventory/scan/slabs", response_class=HTMLResponse)
async def inventory_scan_slabs_page(
    request: Request,
    session: Session = Depends(get_session),
    q: str = Query(default=""),
    error: str = Query(default=""),
):
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_slabs.html",
        {
            "current_user": _current_user(request),
            "grading_companies": GRADING_COMPANIES,
            "games": GAMES,
            "initial_query": q,
            "initial_error": error,
            "slab_price_sources": [
                {"value": source, "label": _slab_price_source_label(source)}
                for source in SLAB_PRICE_SOURCE_OPTIONS
            ],
        },
    )


@router.get("/inventory/scan/batch-review", response_class=HTMLResponse)
async def inventory_batch_review_page(request: Request, session: Session = Depends(get_session)):
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_batch_review.html",
        {"current_user": _current_user(request), "conditions": CONDITIONS},
    )


# ---------------------------------------------------------------------------
# Pokemon card scanner (multi-stage pipeline)
# ---------------------------------------------------------------------------

@router.get("/degen_eye", response_class=HTMLResponse)
async def inventory_scan_pokemon_page(request: Request, session: Session = Depends(get_session)):
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_pokemon.html",
        {
            "current_user": _current_user(request),
            "conditions": CONDITIONS,
            "team_shell": request.query_params.get("team_shell") == "1",
        },
    )


@router.get("/degen_eye/categories")
async def inventory_scan_categories(request: Request, session: Session = Depends(get_session)):
    """Return TCGTracking categories with preferred ordering."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    categories = await fetch_tcg_categories()
    return JSONResponse({"categories": categories})


@router.post("/degen_eye/identify")
async def inventory_scan_pokemon_identify(request: Request, session: Session = Depends(get_session)):
    """
    Run the full card scanning pipeline on a base64 image.

    Request body: {"image": "<base64 string>", "category_id": "3"}
    Response: ScanResult JSON with best_match, candidates, extracted_fields, debug.
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    base64_image = (body.get("image") or "").strip()
    if not base64_image:
        return JSONResponse({"error": "Missing image field"}, status_code=400)

    if "," in base64_image:
        base64_image = base64_image.split(",", 1)[1]

    category_id = (body.get("category_id") or "3").strip()
    mode = (body.get("mode") or "balanced").strip().lower()

    result = await run_pokemon_pipeline(base64_image, category_id=category_id, mode=mode)

    status_code = 200
    if result.get("status") == "ERROR":
        status_code = 422

    return JSONResponse(result, status_code=status_code)


@router.post("/degen_eye/client_log")
async def inventory_scan_pokemon_client_log(request: Request, session: Session = Depends(get_session)):
    """Append a client-side error report for post-mortem analysis.

    Hardened against abuse:
    - 8 KB payload cap (rejects with 413)
    - per-user rate limit: 30 entries / 5 min
    - log file rotation at 5 MB → .log.1 (1 generation kept)
    - disk errors return 500 (no silent failures)
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    # Cap payload before parsing JSON.
    raw = await request.body()
    if len(raw) > 8 * 1024:
        return JSONResponse({"error": "payload too large"}, status_code=413)

    try:
        body = json.loads(raw) if raw else {}
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    if not isinstance(body, dict):
        body = {"raw": body}

    user = _get_user(request)
    username = user.username if user else "anonymous"

    # Simple in-memory rate limit per user. Module-level dict keyed by
    # username → deque of recent timestamps; trim to last 5 min and reject if
    # more than 30 entries in the window.
    now_ts = datetime.now(timezone.utc).timestamp()
    window_start = now_ts - 300.0  # 5 minutes
    bucket = _CLIENT_LOG_RATE.setdefault(username, [])
    # Drop old entries
    bucket[:] = [t for t in bucket if t >= window_start]
    if len(bucket) >= 30:
        return JSONResponse(
            {"error": "rate limit — try again later"}, status_code=429
        )
    bucket.append(now_ts)

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "user": username,
        "ua": (request.headers.get("user-agent", "") or "")[:512],
        "ip": request.client.host if request.client else "",
        **{k: v for k, v in body.items() if k not in ("ts", "user", "ua", "ip")},
    }

    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_path = log_dir / "degen_eye_client.log"
    rotated_path = log_dir / "degen_eye_client.log.1"
    try:
        log_dir.mkdir(exist_ok=True)
        # Rotate if current file > 5 MB.
        if log_path.exists() and log_path.stat().st_size > 5 * 1024 * 1024:
            if rotated_path.exists():
                rotated_path.unlink()
            log_path.rename(rotated_path)
        with open(log_path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as exc:
        logger.exception("degen_eye client_log write failed: %s", exc)
        return JSONResponse(
            {"error": "log write failed"}, status_code=500
        )

    return JSONResponse({"ok": True})


@router.post("/degen_eye/text-search")
async def inventory_scan_pokemon_text_search(request: Request, session: Session = Depends(get_session)):
    """Search for cards by text query (name, set, number)."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    query = (body.get("query") or "").strip()
    if not query:
        return JSONResponse({"error": "Missing query field"}, status_code=400)
    category_id = (body.get("category_id") or "3").strip()
    result = await text_search_cards(query, category_id=category_id)
    return JSONResponse(result)


@router.post("/inventory/scan/slab-ximilar")
async def inventory_scan_slab_ximilar(request: Request, session: Session = Depends(get_session)):
    """Identify a slab photo and fetch Ximilar price-guide listings."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing slab photo."}, status_code=400)
    if not settings.ximilar_api_token:
        return JSONResponse({"error": "XIMILAR_API_TOKEN is not configured."}, status_code=503)

    game = _normalize_add_stock_game(body.get("game") or "Pokemon")
    category_id = _ximilar_category_for_game(game)
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            result = await fetch_ximilar_slab_price_from_image(
                image_b64,
                client,
                api_token=settings.ximilar_api_token,
                category_id=category_id,
            )
    except Exception as exc:
        logger.warning("[inventory/scan] Ximilar slab scan failed: %s", exc)
        result = None

    if result is None:
        return JSONResponse(
            {"error": "Ximilar did not return a card or price-guide result for that slab photo."},
            status_code=502,
        )

    raw = result.get("raw") if isinstance(result, dict) else {}
    raw = raw if isinstance(raw, dict) else {}
    card_info = raw.get("ximilar_card") if isinstance(raw.get("ximilar_card"), dict) else {}
    slab_info = raw.get("ximilar_slab") if isinstance(raw.get("ximilar_slab"), dict) else {}
    card_name = (
        str(card_info.get("card_name") or "").strip()
        or str(slab_info.get("card_name") or "").strip()
        or "Ximilar slab scan"
    )
    grading_company = (
        str(slab_info.get("grading_company") or "").strip()
        or (body.get("grading_company") or "PSA").strip().upper()
        or "PSA"
    )
    preview_item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game=str(card_info.get("game") or game or "Pokemon"),
        card_name=card_name,
        set_name=str(card_info.get("set_name") or slab_info.get("set_name") or "").strip() or None,
        card_number=str(card_info.get("card_number") or slab_info.get("card_number") or "").strip() or None,
        grading_company=grading_company,
        grade=str(slab_info.get("grade") or "").strip() or None,
        cert_number=str(slab_info.get("cert_number") or "").strip() or None,
    )
    payload = _slab_comps_lookup_payload(preview_item, result)
    card = {
        "name": preview_item.card_name,
        "card_name": preview_item.card_name,
        "game": preview_item.game,
        "set_name": preview_item.set_name or "",
        "card_number": preview_item.card_number or "",
        "image_url": "",
        "lookup_source_label": "Ximilar",
    }
    response: dict[str, Any] = {
        "card": card,
        "cards": [card],
        "cert": {
            "cert_number": preview_item.cert_number or "",
            "grading_company": preview_item.grading_company or "",
            "grade": preview_item.grade or "",
        },
        "cert_number": preview_item.cert_number or "",
        "grading_company": preview_item.grading_company or "",
        "grade_comps": [payload],
        "selected_price_source": "ximilar",
        "selected_price_source_label": "Ximilar",
        "ximilar": {
            "card": card_info,
            "slab": slab_info,
        },
    }
    if not payload.get("last_solds"):
        response["lookup_warning"] = (
            "Ximilar identified the slab, but the price guide did not return marketplace listings. "
            "The account may need price-guide access enabled, or this card may not have current listing data."
        )
    return JSONResponse(response)


@router.post("/inventory/scan/slab-comps")
async def inventory_scan_slab_comps(request: Request, session: Session = Depends(get_session)):
    """Fetch slab comps from the pricing chain using manually entered slab details."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    card_name = (body.get("card_name") or "").strip()
    if not card_name:
        return JSONResponse({"error": "card_name is required"}, status_code=400)
    price_source = _slab_price_source_from_body(body)

    preview_item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game=(body.get("game") or "Other").strip() or "Other",
        card_name=card_name,
        set_name=(body.get("set_name") or "").strip() or None,
        card_number=(body.get("card_number") or "").strip() or None,
        grading_company=(body.get("grading_company") or "").strip().upper() or None,
        grade=(body.get("grade") or "").strip() or None,
        cert_number=(body.get("cert_number") or "").strip() or None,
    )

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            result = await fetch_slab_price(
                preview_item,
                client,
                source_filter=price_source,
            )
    except Exception as exc:
        logger.warning("[inventory/scan] slab comps failed for %s: %s", card_name, exc)
        result = None

    response = _slab_comps_lookup_payload(preview_item, result)
    response["selected_price_source"] = price_source
    response["selected_price_source_label"] = _slab_price_source_label(price_source)
    if result is None:
        response["lookup_warning"] = f"No {_slab_price_source_label(price_source)} comps came back for those slab details."
    return JSONResponse(response)


@router.post("/inventory/scan/slab-search")
async def inventory_scan_slab_search(request: Request, session: Session = Depends(get_session)):
    """Find card-name matches for the slab receiving workflow."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    query = (body.get("query") or "").strip()
    if len(query) < 2:
        return JSONResponse({"error": "Enter a card name or cert number."}, status_code=400)

    game = _normalize_add_stock_game(body.get("game") or "Pokemon")
    cards, warning = await _cached_add_stock_single_search(query, game=game)
    if not cards:
        fallback = _slab_search_fallback_suggestion(query, game=game)
        if fallback:
            cards = [fallback]
            warning = (
                "Card database lookup timed out, so this is a manual card-name match. "
                "Click it to pull Card Ladder grade comps."
            )
    return JSONResponse(
        {
            "query": query,
            "game": game,
            "cards": cards,
            "warning": warning,
        }
    )


@router.post("/inventory/scan/slab-grade-comps")
async def inventory_scan_slab_grade_comps(request: Request, session: Session = Depends(get_session)):
    """Fetch Card Ladder slab comps for the common grades of a selected card."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    card_name = (body.get("card_name") or "").strip()
    if not card_name:
        return JSONResponse({"error": "card_name is required"}, status_code=400)

    grading_company = (body.get("grading_company") or "PSA").strip().upper() or "PSA"
    preferred_grade = (body.get("grade") or "").strip()
    grades = _slab_grade_options(grading_company, preferred_grade=preferred_grade)
    game = _normalize_add_stock_game(body.get("game") or "Pokemon")
    set_name = (body.get("set_name") or "").strip()
    card_number = (body.get("card_number") or "").strip()
    cert_number = (body.get("cert_number") or "").strip()
    price_source = _slab_price_source_from_body(body)

    preview_items = [
        InventoryItem(
            barcode="PREVIEW",
            item_type=ITEM_TYPE_SLAB,
            game=game,
            card_name=card_name,
            set_name=set_name or None,
            card_number=card_number or None,
            grading_company=grading_company,
            grade=grade,
            cert_number=cert_number or None,
        )
        for grade in grades
    ]

    results: list[Any] = []
    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            results = await asyncio.gather(
                *[
                    fetch_slab_price(
                        item,
                        client,
                        source_filter=price_source,
                    )
                    for item in preview_items
                ],
                return_exceptions=True,
            )
    except Exception as exc:
        logger.warning("[inventory/scan] slab grade comps failed for %s: %s", card_name, exc)
        results = [None for _ in preview_items]

    grade_comps: list[dict[str, Any]] = []
    for item, result in zip(preview_items, results):
        if isinstance(result, Exception):
            logger.debug(
                "[inventory/scan] slab comp failed for %s %s %s: %s",
                item.card_name,
                item.grading_company,
                item.grade,
                result,
            )
            result = None
        payload = _slab_comps_lookup_payload(item, result)
        payload["selected_price_source"] = price_source
        payload["selected_price_source_label"] = _slab_price_source_label(price_source)
        if result is None:
            payload["lookup_warning"] = f"No {_slab_price_source_label(price_source)} comps came back for this grade."
        grade_comps.append(payload)

    response: dict[str, Any] = {
        "card": {
            "card_name": card_name,
            "game": game,
            "set_name": set_name,
            "card_number": card_number,
            "image_url": (body.get("image_url") or "").strip(),
        },
        "grading_company": grading_company,
        "cert_number": cert_number,
        "grade_comps": grade_comps,
        "selected_price_source": price_source,
        "selected_price_source_label": _slab_price_source_label(price_source),
        "alt_cli": alt_cli_status(),
        "card_ladder_cli": card_ladder_cli_status(),
        "point130_cli": point130_cli_status(),
    }
    if not any(row.get("last_solds") or row.get("suggested_price") for row in grade_comps):
        response["lookup_warning"] = f"No {_slab_price_source_label(price_source)} comps came back for those slab details."
    return JSONResponse(response)


@router.post("/inventory/scan/slab-cardladder-refresh")
async def inventory_scan_slab_cardladder_refresh(request: Request, session: Session = Depends(get_session)):
    """Refresh one selected slab grade through the browser-backed Card Ladder CLI."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    card_name = (body.get("card_name") or "").strip()
    if not card_name:
        return JSONResponse({"error": "card_name is required"}, status_code=400)

    preview_item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game=_normalize_add_stock_game(body.get("game") or "Pokemon"),
        card_name=card_name,
        set_name=(body.get("set_name") or "").strip() or None,
        card_number=(body.get("card_number") or "").strip() or None,
        grading_company=(body.get("grading_company") or "PSA").strip().upper() or "PSA",
        grade=(body.get("grade") or "").strip() or None,
        cert_number=(body.get("cert_number") or "").strip() or None,
    )

    try:
        result = await sync_card_ladder_cli_for_item(preview_item, timeout_seconds=120, limit=25)
    except Exception as exc:
        status = card_ladder_cli_status()
        return JSONResponse(
            {
                "error": str(exc),
                "lookup_warning": (
                    "Card Ladder refresh needs the local CLI browser session. "
                    "Run the login command once if this is the first refresh."
                ),
                "card_ladder_cli": status,
                "login_command": status.get("login_command"),
            },
            status_code=502,
        )

    payload = _slab_comps_lookup_payload(preview_item, result)
    payload["card_ladder_cli"] = card_ladder_cli_status()
    return JSONResponse(payload)


@router.post("/inventory/scan/slab-alt-refresh")
async def inventory_scan_slab_alt_refresh(request: Request, session: Session = Depends(get_session)):
    """Refresh one selected slab grade through ALT sold-listing search."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    card_name = (body.get("card_name") or "").strip()
    if not card_name:
        return JSONResponse({"error": "card_name is required"}, status_code=400)

    preview_item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game=_normalize_add_stock_game(body.get("game") or "Pokemon"),
        card_name=card_name,
        set_name=(body.get("set_name") or "").strip() or None,
        card_number=(body.get("card_number") or "").strip() or None,
        grading_company=(body.get("grading_company") or "PSA").strip().upper() or "PSA",
        grade=(body.get("grade") or "").strip() or None,
        cert_number=(body.get("cert_number") or "").strip() or None,
    )

    try:
        result = await sync_alt_cli_for_item(preview_item, limit=20)
    except Exception as exc:
        return JSONResponse(
            {
                "error": str(exc),
                "alt_cli": alt_cli_status(),
            },
            status_code=502,
        )

    payload = _slab_comps_lookup_payload(preview_item, result)
    payload["alt_cli"] = alt_cli_status()
    return JSONResponse(payload)


@router.post("/inventory/scan/slab-cardladder-import")
async def inventory_scan_slab_cardladder_import(request: Request, session: Session = Depends(get_session)):
    """Import pasted/exported Card Ladder sold rows into the local comps cache."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    card_name = (body.get("card_name") or "").strip()
    text = (body.get("text") or "").strip()
    if not card_name:
        return JSONResponse({"error": "card_name is required"}, status_code=400)
    if not text:
        return JSONResponse({"error": "Paste the Card Ladder sold rows first."}, status_code=400)

    preview_item = InventoryItem(
        barcode="PREVIEW",
        item_type=ITEM_TYPE_SLAB,
        game=_normalize_add_stock_game(body.get("game") or "Pokemon"),
        card_name=card_name,
        set_name=(body.get("set_name") or "").strip() or None,
        card_number=(body.get("card_number") or "").strip() or None,
        grading_company=(body.get("grading_company") or "PSA").strip().upper() or "PSA",
        grade=(body.get("grade") or "").strip() or None,
        cert_number=(body.get("cert_number") or "").strip() or None,
    )

    try:
        result = import_card_ladder_cli_records_for_item(preview_item, text=text)
    except Exception as exc:
        return JSONResponse(
            {
                "error": str(exc),
                "card_ladder_cli": card_ladder_cli_status(),
            },
            status_code=422,
        )

    payload = _slab_comps_lookup_payload(preview_item, result)
    payload["card_ladder_cli"] = card_ladder_cli_status()
    return JSONResponse(payload)


def _slab_comps_lookup_payload(
    item: InventoryItem,
    result: Optional[dict[str, Any]],
) -> dict[str, Any]:
    raw = result.get("raw") if isinstance(result, dict) else {}
    raw = raw if isinstance(raw, dict) else {}
    last_solds = _lookup_sales_from_price_raw(raw, source=str(result.get("source") if result else ""))
    source_detail = str(raw.get("source_detail") or "")
    return {
        "cert_number": item.cert_number,
        "grading_company": item.grading_company,
        "grade": item.grade,
        "card_name": item.card_name,
        "set_name": item.set_name,
        "card_number": item.card_number,
        "game": item.game,
        "last_solds": last_solds,
        "suggested_price": result.get("market_price") if result else None,
        "data_points": raw.get("sample_count") or len(last_solds),
        "price_source": result.get("source") if result else None,
        "source_detail": source_detail,
        "sales_history_url": str(raw.get("sales_history_url") or raw.get("product_url") or ""),
        "card_ladder_query": str(raw.get("query") or build_card_ladder_cli_query(item)),
        "card_ladder_cache_hit": source_detail == "card_ladder_cli_cache",
    }


def _slab_price_source_from_body(body: dict[str, Any]) -> str:
    return normalize_slab_price_source(
        body.get("price_source")
        or body.get("source")
        or body.get("comp_source")
        or body.get("pricing_source")
    )


def _slab_price_source_label(source: str) -> str:
    source = normalize_slab_price_source(source)
    labels = {
        "all": "All Sources",
        "alt": "ALT",
        "pricecharting": "PriceCharting",
        "myslabs": "MySlabs",
        "card_ladder": "Card Ladder",
        "130point": "130point",
    }
    return labels.get(source, source.replace("_", " ").title())


def _ximilar_category_for_game(game: str) -> str:
    clean = (game or "").strip().lower()
    if clean in {"pokemon japan", "pokemon jp", "japanese pokemon", "jp pokemon"}:
        return "3"
    if clean in {"magic", "mtg", "magic: the gathering"}:
        return "1"
    if clean in {"yu-gi-oh", "yugioh", "yu gi oh"}:
        return "2"
    if clean in {"one piece", "onepiece"}:
        return "68"
    if clean == "lorcana":
        return "71"
    if clean == "riftbound":
        return "89"
    return "3"


def _lookup_sales_from_price_raw(raw: dict[str, Any], *, source: str = "") -> list[dict[str, Any]]:
    sales = raw.get("sales")
    if not isinstance(sales, list):
        return []
    out: list[dict[str, Any]] = []
    for sale in sales[:20]:
        if not isinstance(sale, dict):
            continue
        out.append(
            {
                "date": str(sale.get("sold_date") or sale.get("date") or ""),
                "price": _parse_float(str(sale.get("price") or "")),
                "source": source or "card_ladder",
                "sources": sale.get("sources") if isinstance(sale.get("sources"), list) else [source or "card_ladder"],
                "source_details": sale.get("source_details") if isinstance(sale.get("source_details"), list) else [],
                "title": str(sale.get("title") or ""),
                "platform": str(sale.get("platform") or ""),
                "url": str(sale.get("url") or ""),
                "image_url": str(sale.get("image_url") or ""),
            }
        )
    return out


@router.get("/degen_eye/history")
async def inventory_scan_pokemon_history(request: Request, session: Session = Depends(get_session)):
    """Return recent scan results as JSON for debugging."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return JSONResponse(get_scan_history())


@router.get("/degen_eye/validate/{scan_id}")
async def inventory_scan_pokemon_validate(request: Request, scan_id: str, session: Session = Depends(get_session)):
    """Poll for background OCR validation result."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    result = get_validation_result(scan_id)
    if result is None:
        return JSONResponse({"error": "Unknown scan_id"}, status_code=404)
    return JSONResponse(result)


@router.get("/degen_eye/debug", response_class=HTMLResponse)
async def inventory_scan_pokemon_debug_page(request: Request, session: Session = Depends(get_session)):
    """Live debug page showing recent scan history — open on desktop while scanning on phone."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return HTMLResponse("""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pokemon Scanner Debug Log</title>
<style>
  body{font-family:monospace;background:#1a1a2e;color:#e0e0e0;margin:0;padding:20px;}
  h1{color:#f0c674;font-size:18px;margin:0 0 4px;}
  .sub{color:#888;font-size:12px;margin-bottom:16px;}
  .entry{background:#16213e;border:1px solid #333;border-radius:8px;padding:14px;margin-bottom:12px;}
  .entry.MATCHED{border-left:4px solid #4caf50;}
  .entry.AMBIGUOUS{border-left:4px solid #ff9800;}
  .entry.NO_MATCH{border-left:4px solid #f44336;}
  .entry.ERROR{border-left:4px solid #f44336;}
  .ts{color:#888;font-size:11px;}
  .status{font-weight:bold;font-size:13px;margin-bottom:6px;}
  .status.MATCHED{color:#4caf50;} .status.AMBIGUOUS{color:#ff9800;}
  .status.NO_MATCH{color:#f44336;} .status.ERROR{color:#f44336;}
  .field{margin:3px 0;font-size:12px;line-height:1.5;}
  .label{color:#f0c674;} .val{color:#e0e0e0;}
  .ocr{background:#0d1117;padding:8px;border-radius:4px;white-space:pre-wrap;font-size:11px;
       max-height:150px;overflow-y:auto;margin-top:6px;color:#aaa;border:1px solid #222;}
  .empty{color:#666;text-align:center;padding:40px;font-size:14px;}
  #auto{margin-bottom:12px;display:flex;align-items:center;gap:8px;font-size:12px;color:#888;}
</style>
</head><body>
<h1>Pokemon Scanner Debug Log</h1>
<div class="sub">Auto-refreshes every 3s. Open this on your desktop while scanning on your phone.</div>
<div id="auto"><input type="checkbox" id="autoRefresh" checked> Auto-refresh
  <button onclick="loadHistory()" style="margin-left:8px;padding:4px 10px;border-radius:4px;
    border:1px solid #444;background:#16213e;color:#e0e0e0;cursor:pointer;">Refresh Now</button></div>
<div id="log"></div>
<script>
function loadHistory(){
  fetch('/degen_eye/history').then(r=>r.json()).then(data=>{
    var el=document.getElementById('log');
    if(!data.length){el.innerHTML='<div class="empty">No scans yet. Start scanning on your phone.</div>';return;}
    el.innerHTML=data.map(function(e){
      var s=e.status||'ERROR';
      var h='<div class="entry '+s+'">';
      h+='<div class="status '+s+'">'+s+'</div>';
      h+='<div class="ts">'+e.timestamp+'  |  '+(e.processing_time_ms||0).toFixed(0)+'ms</div>';
      if(e.error) h+='<div class="field"><span class="label">Error: </span>'+esc(e.error)+'</div>';
      h+='<div class="field"><span class="label">Extracted: </span>name='+esc(e.extracted_name)+', number='+esc(e.extracted_number)+', set='+esc(e.extracted_set)+'</div>';
      if(e.best_match_name) h+='<div class="field"><span class="label">Best Match: </span>'+esc(e.best_match_name)+' #'+esc(e.best_match_number)+' | '+esc(e.best_match_set)+' | score='+e.best_match_score+' ('+e.best_match_confidence+')' + (e.best_match_price?' | $'+Number(e.best_match_price).toFixed(2):'')+'</div>';
      h+='<div class="field"><span class="label">Candidates: </span>'+e.candidates_count+'</div>';
      if(e.extraction_method) h+='<div class="field"><span class="label">Extraction: </span>'+e.extraction_method+'</div>';
      if(e.disambiguation) h+='<div class="field"><span class="label">Disambiguation: </span>'+e.disambiguation+'</div>';
      if(e.ocr_text) h+='<div class="field"><span class="label">OCR Text:</span><div class="ocr">'+esc(e.ocr_text)+'</div></div>';
      h+='</div>';
      return h;
    }).join('');
  }).catch(function(){});
}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
loadHistory();
setInterval(function(){if(document.getElementById('autoRefresh').checked)loadHistory();},3000);
</script>
</body></html>""")


# ---------------------------------------------------------------------------
# Degen Eye v2 — pHash-first scanner (Pokemon MVP, targets sub-1-second)
# ---------------------------------------------------------------------------
# v2 runs entirely locally for identification: OpenCV card detection,
# perceptual-hash nearest-neighbor lookup against a pre-built index of
# every Pokemon card, and a pre-warmed TCGTracking price cache. v1 is
# untouched — both scanners coexist under /degen_eye.

@router.get("/degen_eye/v2", response_class=HTMLResponse)
async def degen_eye_v2_page(request: Request, session: Session = Depends(get_session)):
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_pokemon_v2.html",
        {
            "current_user": _current_user(request),
            "conditions": CONDITIONS,
            "team_shell": request.query_params.get("team_shell") == "1",
        },
    )


@router.post("/degen_eye/v2/scan")
async def degen_eye_v2_scan(request: Request, session: Session = Depends(get_session)):
    """Non-streaming v2 scan — accepts a base64 image, returns a full ScanResult.

    Request body: {"image": "<base64>", "category_id": "3"}
    Response shape mirrors v1's /degen_eye/identify so the existing batch
    UI helpers work without changes.
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing image field"}, status_code=400)
    if len(image_b64) > _V2_MAX_SCAN_IMAGE_B64_CHARS:
        return JSONResponse({"error": "Image is too large"}, status_code=413)
    category_id = (body.get("category_id") or "3").strip()

    capture_id = await asyncio.to_thread(
        create_scan_capture,
        image_b64,
        source="v2_scan",
        category_id=category_id,
        employee=_capture_user_payload(request),
        request_meta=_capture_request_meta(request),
    )
    result = await run_v2_pipeline(image_b64, category_id=category_id)
    _tag_v2_capture_result(result, capture_id)
    if capture_id:
        await asyncio.to_thread(attach_prediction, capture_id, result)
    status_code = 422 if result.get("status") == "ERROR" else 200
    return JSONResponse(result, status_code=status_code)


@router.post("/degen_eye/v2/scan-init")
async def degen_eye_v2_scan_init(request: Request, session: Session = Depends(get_session)):
    """Prepare a streaming scan and return a ``scan_id`` to connect via SSE.

    We stash the uploaded image in a short-lived file keyed by scan_id; the
    SSE endpoint atomically claims it and runs the pipeline. This keeps the
    URL query-string small and works when scan-init and scan-stream land on
    different web workers.
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing image field"}, status_code=400)
    if len(image_b64) > _V2_MAX_SCAN_IMAGE_B64_CHARS:
        return JSONResponse({"error": "Image is too large"}, status_code=413)
    category_id = (body.get("category_id") or "3").strip()

    scan_id = uuid.uuid4().hex
    capture_id = await asyncio.to_thread(
        create_scan_capture,
        image_b64,
        source="v2_stream",
        category_id=category_id,
        employee=_capture_user_payload(request),
        scan_id=scan_id,
        request_meta=_capture_request_meta(request),
    )
    try:
        _write_v2_pending_scan(scan_id, image_b64, category_id, capture_id=capture_id)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except OSError as exc:
        logger.warning("[degen_eye_v2] failed to stage pending scan: %s", exc)
        return JSONResponse({"error": "Unable to stage scan"}, status_code=500)
    return JSONResponse({"scan_id": scan_id, "capture_id": capture_id})


# Small file-backed buffer so scan-init can hand a scan off to scan-stream
# without blowing the SSE URL past browser query-length limits. This must not
# be process-local: with multiple uvicorn/gunicorn workers, the POST and SSE GET
# can land in different processes on the same host.
_V2_PENDING_DIR = Path(__file__).resolve().parent.parent / "data" / "v2_pending_scans"
_V2_PENDING_TTL = 120.0
_V2_PENDING_MAX = 200
_V2_MAX_SCAN_IMAGE_B64_CHARS = 12 * 1024 * 1024
_V2_MAX_DETECT_IMAGE_B64_CHARS = 2 * 1024 * 1024
_V2_HEX_CHARS = set("0123456789abcdef")


def _is_v2_scan_id(scan_id: str) -> bool:
    scan_id = (scan_id or "").strip().lower()
    return len(scan_id) == 32 and all(c in _V2_HEX_CHARS for c in scan_id)


def _v2_pending_path(scan_id: str) -> Path:
    return _V2_PENDING_DIR / f"{scan_id}.json"


def _iter_v2_pending_files() -> list[Path]:
    if not _V2_PENDING_DIR.exists():
        return []
    return [
        p for p in _V2_PENDING_DIR.iterdir()
        if p.is_file() and (p.name.endswith(".json") or p.name.endswith(".json.claimed"))
    ]


def _evict_stale_v2_pending() -> None:
    now = time.time()
    _V2_PENDING_DIR.mkdir(parents=True, exist_ok=True)
    files = _iter_v2_pending_files()
    for path in files:
        try:
            if now - path.stat().st_mtime > _V2_PENDING_TTL:
                path.unlink(missing_ok=True)
        except OSError:
            logger.debug("[degen_eye_v2] unable to inspect/remove pending file %s", path, exc_info=True)

    active = [p for p in _iter_v2_pending_files() if p.name.endswith(".json")]
    overflow = len(active) - _V2_PENDING_MAX
    if overflow > 0:
        oldest = sorted(active, key=lambda p: p.stat().st_mtime)[:overflow]
        for path in oldest:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.debug("[degen_eye_v2] unable to evict pending file %s", path, exc_info=True)


def _write_v2_pending_scan(
    scan_id: str,
    image_b64: str,
    category_id: str,
    *,
    capture_id: Optional[str] = None,
) -> None:
    if not _is_v2_scan_id(scan_id):
        raise ValueError("Invalid scan id")
    if len(image_b64) > _V2_MAX_SCAN_IMAGE_B64_CHARS:
        raise ValueError("Image is too large")
    _evict_stale_v2_pending()
    payload = {
        "created_at": time.time(),
        "image": image_b64,
        "category_id": category_id,
        "capture_id": capture_id,
    }
    path = _v2_pending_path(scan_id)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(path)
    except OSError:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _claim_v2_pending_scan(scan_id: str) -> Optional[tuple[str, str, Optional[str]]]:
    if not _is_v2_scan_id(scan_id):
        return None
    _evict_stale_v2_pending()
    path = _v2_pending_path(scan_id)
    claimed = path.with_name(f"{path.name}.claimed")
    try:
        path.replace(claimed)
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.warning("[degen_eye_v2] failed to claim scan_id=%s: %s", scan_id, exc)
        return None

    try:
        payload = json.loads(claimed.read_text(encoding="utf-8"))
        created_at = float(payload.get("created_at") or 0)
        if time.time() - created_at > _V2_PENDING_TTL:
            return None
        image_b64 = str(payload.get("image") or "")
        category_id = str(payload.get("category_id") or "3")
        capture_id = str(payload.get("capture_id") or "").strip() or None
        if not image_b64:
            return None
        return (image_b64, category_id, capture_id)
    except Exception as exc:
        logger.warning("[degen_eye_v2] failed to read pending scan %s: %s", scan_id, exc)
        return None
    finally:
        try:
            claimed.unlink(missing_ok=True)
        except OSError:
            logger.debug("[degen_eye_v2] unable to remove claimed pending scan %s", claimed, exc_info=True)


def _count_v2_pending_scans() -> int:
    _evict_stale_v2_pending()
    return len([p for p in _iter_v2_pending_files() if p.name.endswith(".json")])


@router.get("/degen_eye/v2/scan-stream")
async def degen_eye_v2_scan_stream(
    request: Request,
    scan_id: str,
    session: Session = Depends(get_session),
):
    """Server-Sent Events stream of a v2 scan's progressive results.

    Events emitted in order: detected, identified, price, variants, done.
    ``event: error`` is emitted on unrecoverable failures before done.
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    pending = _claim_v2_pending_scan(scan_id)
    if pending is None:
        return JSONResponse(
            {"error": "Unknown or expired scan_id; POST to /degen_eye/v2/scan-init first"},
            status_code=404,
        )
    image_b64, category_id, capture_id = pending

    async def _event_source():
        import json as _json
        try:
            yield ": connected\n\n"
            async for event_name, payload in run_v2_pipeline_stream(image_b64, category_id):
                if event_name in {"done", "error"}:
                    _tag_v2_capture_result(payload, capture_id)
                    if capture_id:
                        await asyncio.to_thread(attach_prediction, capture_id, payload)
                safe_payload = _json.dumps(payload, default=str)
                yield f"event: {event_name}\ndata: {safe_payload}\n\n"
                # Disconnection check — bail early if the client closed the tab.
                if await request.is_disconnected():
                    logger.info("[degen_eye_v2] SSE client disconnected mid-scan (scan_id=%s)", scan_id)
                    return
        except Exception as exc:
            logger.exception("[degen_eye_v2] SSE pipeline crashed: %s", exc)
            error_payload = {"status": "ERROR", "error": str(exc), "debug": {"mode": "v2"}}
            _tag_v2_capture_result(error_payload, capture_id)
            if capture_id:
                await asyncio.to_thread(attach_prediction, capture_id, error_payload)
            yield f"event: error\ndata: {_json.dumps(error_payload, default=str)}\n\n"

    from fastapi.responses import StreamingResponse as _StreamingResponse
    return _StreamingResponse(
        _event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",  # disable nginx buffering so events flush
            "Connection": "keep-alive",
        },
    )


@router.post("/degen_eye/v2/detect-only")
async def degen_eye_v2_detect_only(request: Request, session: Session = Depends(get_session)):
    """Fast card-edge detection endpoint used by Phase B auto-capture.

    Accepts a small thumbnail image (~400px JPEG base64) and runs ONLY the
    OpenCV detection + quad scoring — no pHash lookup, no price. Response
    target latency: < 100ms so the frontend can poll ~3x/second.

    Response shape:
        {
            "found": bool,
            "reason": str,
            "box": [x, y, w, h]?,           # only when found
            "corners": [[x,y], ...]?,       # only when found, 4 entries
            "stability_hash": str?,         # rounded-to-10px corner hash
            "score": float?,                # quad score for debugging
            "elapsed_ms": float,
        }
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    import base64 as _b64
    import time as _time
    from .card_detect import detect_box

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing image field"}, status_code=400)
    if "," in image_b64:
        image_b64 = image_b64.split(",", 1)[1]
    if len(image_b64) > _V2_MAX_DETECT_IMAGE_B64_CHARS:
        return JSONResponse({"error": "Image is too large"}, status_code=413)
    try:
        raw = _b64.b64decode(image_b64)
    except Exception:
        return JSONResponse({"error": "Invalid base64"}, status_code=400)

    t_start = _time.monotonic()
    result = await asyncio.to_thread(detect_box, raw)
    result["elapsed_ms"] = round((_time.monotonic() - t_start) * 1000, 1)
    return JSONResponse(result)


@router.get("/degen_eye/v2/stats")
async def degen_eye_v2_stats(request: Request, session: Session = Depends(get_session)):
    """Admin-ish JSON: index size, last build, cache warm state."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return JSONResponse({
        "phash_index": phash_index_stats(),
        "price_cache": price_cache_stats(),
        "training_captures": training_capture_stats(),
        "pending_scans": _count_v2_pending_scans(),
        "v2_history_entries": len(get_v2_scan_history()),
    })


@router.get("/degen_eye/v2/training", response_class=HTMLResponse)
async def degen_eye_v2_training_page(request: Request):
    """Reviewer page for capture counts and one-click confirmed-capture training."""
    if denial := _require_reviewer(request):
        return denial
    return HTMLResponse(_render_v2_training_page())


@router.post("/degen_eye/v2/train-captures")
async def degen_eye_v2_train_captures(request: Request):
    """Promote confirmed employee captures into the pHash index."""
    if denial := _require_reviewer(request):
        return denial
    try:
        form = await request.form()
    except Exception:
        form = {}
    try:
        limit = int(form.get("limit") or 200)
    except (TypeError, ValueError):
        limit = 200
    limit = max(1, min(limit, 2000))
    dry_run = _truthy_form_value(form.get("dry_run"))
    include_indexed = _truthy_form_value(form.get("include_indexed"))
    reload_current_worker = _truthy_form_value(form.get("reload_current_worker"))

    summary = await asyncio.to_thread(
        train_confirmed_captures,
        limit=limit,
        include_indexed=include_indexed,
        dry_run=dry_run,
    )
    if reload_current_worker and not dry_run:
        summary["reloaded_current_worker_card_count"] = await asyncio.to_thread(phash_reload_index)
    accept = request.headers.get("accept") or ""
    if "text/html" in accept:
        return HTMLResponse(_render_v2_training_page(summary))
    return JSONResponse(summary)


@router.get("/degen_eye/v2/history")
async def degen_eye_v2_history(request: Request, session: Session = Depends(get_session)):
    """v2-only scan history. Separate from v1's /degen_eye/history so v2
    debugging doesn't pollute the v1 ops log and vice versa."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return JSONResponse(get_v2_scan_history())


@router.get("/degen_eye/v2/debug", response_class=HTMLResponse)
async def degen_eye_v2_debug_page(request: Request, session: Session = Depends(get_session)):
    """Live debug page for v2 scans only. Mirrors v1's /degen_eye/debug UX
    but points at /degen_eye/v2/history so only v2 entries show up."""
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial
    return HTMLResponse("""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Degen Eye v2 Debug Log</title>
<style>
  body{font-family:monospace;background:#0b0e13;color:#e0e0e0;margin:0;padding:20px;}
  h1{color:#00c2ff;font-size:18px;margin:0 0 4px;}
  .sub{color:#888;font-size:12px;margin-bottom:16px;}
  .entry{background:#101725;border:1px solid #1f2a3a;border-radius:8px;padding:14px;margin-bottom:12px;}
  .entry.MATCHED{border-left:4px solid #4caf50;}
  .entry.AMBIGUOUS{border-left:4px solid #ff9800;}
  .entry.NO_MATCH{border-left:4px solid #f44336;}
  .entry.ERROR{border-left:4px solid #f44336;}
  .ts{color:#888;font-size:11px;}
  .status{font-weight:bold;font-size:13px;margin-bottom:6px;}
  .status.MATCHED{color:#4caf50;} .status.AMBIGUOUS{color:#ff9800;}
  .status.NO_MATCH{color:#f44336;} .status.ERROR{color:#f44336;}
  .field{margin:3px 0;font-size:12px;line-height:1.5;}
  .label{color:#00c2ff;} .val{color:#e0e0e0;}
  .dbg{background:#070a10;padding:8px;border-radius:4px;white-space:pre-wrap;font-size:11px;
       max-height:220px;overflow-y:auto;margin-top:6px;color:#aaa;border:1px solid #1f2a3a;}
  .empty{color:#666;text-align:center;padding:40px;font-size:14px;}
  #auto{margin-bottom:12px;display:flex;align-items:center;gap:8px;font-size:12px;color:#888;}
</style>
</head><body>
<h1>Degen Eye v2 Debug Log</h1>
<div class="sub">Only v2 scans (pHash + optional Ximilar fallback). For v1 scans, open <a style="color:#888;" href="/degen_eye/debug">/degen_eye/debug</a>.</div>
<div id="auto"><input type="checkbox" id="autoRefresh" checked> Auto-refresh
  <button onclick="loadHistory()" style="margin-left:8px;padding:4px 10px;border-radius:4px;
    border:1px solid #1f2a3a;background:#101725;color:#e0e0e0;cursor:pointer;">Refresh Now</button></div>
<div id="log"></div>
<script>
function loadHistory(){
  fetch('/degen_eye/v2/history').then(r=>r.json()).then(data=>{
    var el = document.getElementById('log');
    if (!data || !data.length) {
      el.innerHTML = '<div class="empty">No v2 scans yet. Scan a card on /degen_eye/v2 to see entries here.</div>';
      return;
    }
    el.innerHTML = data.map(function(e){
      var status = e.status || 'UNKNOWN';
      var dbg = e.debug || {};
      var v2 = dbg.v2 || {};
      var fields = '';
      if (e.best_match_name) {
        fields += '<div class="field"><span class="label">Best:</span> <span class="val">' +
          (e.best_match_name||'') + ' #' + (e.best_match_number||'') +
          ' | ' + (e.best_match_set||'') + '</span></div>';
        fields += '<div class="field"><span class="label">Confidence:</span> <span class="val">' +
          (e.best_match_confidence||'') + ' score=' + (e.best_match_score||0).toFixed(1) + '</span></div>';
        if (e.best_match_price != null) {
          fields += '<div class="field"><span class="label">Price:</span> <span class="val">$' +
            Number(e.best_match_price).toFixed(2) + '</span></div>';
        }
      }
      if (e.processing_time_ms != null) {
        fields += '<div class="field"><span class="label">Total:</span> <span class="val">' +
          Math.round(e.processing_time_ms) + 'ms</span></div>';
      }
      if (v2.stages_ms) {
        fields += '<div class="field"><span class="label">Stages:</span> <span class="val">' +
          JSON.stringify(v2.stages_ms) + '</span></div>';
      }
      if (v2.phash && v2.phash.top && v2.phash.top.length) {
        fields += '<div class="field"><span class="label">pHash top:</span> <span class="val">' +
          (v2.phash.top[0].distance + ' (' + v2.phash.top[0].confidence + ')') + '</span></div>';
      }
      if (v2.raw_image_preferred) {
        fields += '<div class="field"><span class="label">Raw-image fallback:</span> <span class="val">' +
          JSON.stringify(v2.raw_image_preferred) + '</span></div>';
      }
      if (dbg.engines_used) {
        fields += '<div class="field"><span class="label">Engines:</span> <span class="val">' +
          dbg.engines_used.join(', ') + '</span></div>';
      }
      if (e.error) {
        fields += '<div class="field"><span class="label">Error:</span> <span class="val" style="color:#f88;">' +
          e.error + '</span></div>';
      }
      return '<div class="entry ' + status + '">' +
        '<div class="ts">' + (e.timestamp||'') + '</div>' +
        '<div class="status ' + status + '">' + status + '</div>' +
        fields +
        '<details><summary style="cursor:pointer;color:#666;font-size:11px;margin-top:6px;">Raw debug</summary>' +
        '<div class="dbg">' + JSON.stringify(dbg, null, 2) + '</div></details>' +
      '</div>';
    }).join('');
  }).catch(function(err){
    document.getElementById('log').innerHTML = '<div class="empty">Failed to load: ' + err + '</div>';
  });
}
loadHistory();
setInterval(function(){if(document.getElementById('autoRefresh').checked)loadHistory();},3000);
</script>
</body></html>""")


@router.post("/degen_eye/v2/warm")
async def degen_eye_v2_warm(request: Request):
    """Trigger an on-demand price-cache warm (reviewer-only — it's network-heavy)."""
    if denial := _require_reviewer(request):
        return denial
    stats = await warm_price_cache()
    return JSONResponse(stats)


@router.post("/degen_eye/v2/reload-index")
async def degen_eye_v2_reload_index(request: Request):
    """Reload the in-memory pHash index after an offline rebuild/training run."""
    if denial := _require_reviewer(request):
        return denial
    card_count = await asyncio.to_thread(phash_reload_index)
    payload = {"card_count": card_count, "phash_index": phash_index_stats()}
    if "text/html" in (request.headers.get("accept") or ""):
        return HTMLResponse(_render_v2_training_page({"reload": payload}))
    return JSONResponse(payload)


# ---------------------------------------------------------------------------
# AI card identification (JSON API) — generic, kept for MTG/other games
# ---------------------------------------------------------------------------

@router.post("/inventory/scan/identify")
async def inventory_scan_identify(request: Request, session: Session = Depends(get_session)):
    """
    Accept a base64-encoded card image, run AI identification, then fetch
    the stock image + market price from Scryfall / Pokemon TCG API.

    Request body: {"image": "<base64 string>"}
    Response: card info + image_url + market_price
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    base64_image = (body.get("image") or "").strip()
    if not base64_image:
        return JSONResponse({"error": "Missing image field"}, status_code=400)

    # Strip data URI prefix if the client sent it
    if "," in base64_image:
        base64_image = base64_image.split(",", 1)[1]

    card_info = await identify_card_from_image(base64_image)

    if card_info.get("error"):
        return JSONResponse(
            {"error": card_info["error"], "confidence": card_info.get("confidence", 0)},
            status_code=422,
        )

    confidence = float(card_info.get("confidence") or 0)
    if confidence < 0.3:
        return JSONResponse(
            {
                "error": "Could not identify card clearly",
                "confidence": confidence,
                "notes": card_info.get("notes"),
            },
            status_code=422,
        )

    # Enrich with stock image + market price
    try:
        lookup = await lookup_card_image_and_price(
            card_info.get("card_name", ""),
            game=card_info.get("game", ""),
            set_code=card_info.get("set_code"),
            card_number=card_info.get("card_number"),
            pokemon_tcg_api_key=settings.pokemon_tcg_api_key,
        )
    except Exception as exc:
        logger.warning("[inventory/scan] image lookup failed: %s", exc)
        lookup = {}

    return JSONResponse({**card_info, **lookup, "confidence": confidence})


# ---------------------------------------------------------------------------
# Slab cert lookup (JSON API)
# ---------------------------------------------------------------------------

@router.post("/inventory/scan/cert")
async def inventory_scan_cert(request: Request, session: Session = Depends(get_session)):
    """
    Look up a graded slab by certificate number.

    Request body: {"cert_number": "...", "grading_company": "PSA"|"BGS"|"CGC"|"SGC"}
    Response: card details + last_solds + suggested_price
    """
    if denial := _require_employee_permission(request, "ops.degen_eye.view", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    cert_number = (body.get("cert_number") or "").strip()
    grading_company = (body.get("grading_company") or "PSA").strip().upper()

    if not cert_number:
        return JSONResponse({"error": "cert_number is required"}, status_code=400)

    result = await lookup_cert(
        cert_number,
        grading_company,
        psa_api_key=settings.psa_api_key,
    )

    if result.get("error") and not result.get("card_name"):
        result["manual_entry_required"] = True
        result["lookup_warning"] = (
            "The grading company blocked automatic cert details. "
            "Enter the slab details below, then fetch Card Ladder comps."
        )
        return JSONResponse(result)

    return JSONResponse(result)


# ---------------------------------------------------------------------------
# Batch confirm — bulk create inventory items from camera scan batch
# ---------------------------------------------------------------------------

@router.post("/inventory/batch/confirm")
async def inventory_batch_confirm(
    request: Request,
    session: Session = Depends(get_session),
):
    """
    Accept a JSON array of scanned card objects and create InventoryItem records.

    Request body: [
      {
        "card_name": "...",
        "game": "...",
        "condition": "NM",
        "set_name": "...",
        "card_number": "...",
        "image_url": "...",
        "auto_price": 4.99,
        "is_foil": false,
        "notes": "..."
      },
      ...
    ]

    Response: {"created": N, "items": [{"id": ..., "barcode": ..., "card_name": ...}]}
    """
    if denial := _require_employee_permission(request, "ops.inventory.receive", session):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    if not isinstance(body, list) or not body:
        return JSONResponse({"error": "Expected a non-empty JSON array"}, status_code=400)

    created = []
    confirmed_by = _capture_user_payload(request)
    capture_label_updates: list[tuple[str, dict[str, Any], int]] = []
    for raw in body:
        if not isinstance(raw, dict):
            continue
        card_name = (raw.get("card_name") or "").strip()
        if not card_name:
            continue
        item_type = (raw.get("item_type") or ITEM_TYPE_SINGLE).strip().lower()

        if item_type == ITEM_TYPE_SLAB:
            auto_price = _parse_float(str(raw.get("auto_price") or raw.get("suggested_price") or ""))
            source = str(raw.get("price_source") or "card_ladder")
            item, _movement, _created = _receive_slab_stock(
                session,
                game=(raw.get("game") or "Other").strip(),
                card_name=card_name,
                set_name=(raw.get("set_name") or "").strip(),
                card_number=(raw.get("card_number") or "").strip(),
                grading_company=(raw.get("grading_company") or "").strip(),
                grade=(raw.get("grade") or "").strip(),
                cert_number=(raw.get("cert_number") or "").strip(),
                quantity=1,
                unit_cost=_parse_float(str(raw.get("cost_basis") or "")),
                list_price=_parse_float(str(raw.get("list_price") or "")),
                auto_price=auto_price,
                location=(raw.get("location") or "").strip(),
                source=(raw.get("source") or "Slab Lookup").strip(),
                notes=(raw.get("notes") or "").strip(),
                price_payload={
                    "source": source,
                    "query": raw.get("card_name") or "",
                    "sales": raw.get("last_solds") if isinstance(raw.get("last_solds"), list) else [],
                    "sample_count": raw.get("data_points"),
                    "market_price": auto_price,
                },
                actor_label=_current_user_label(request),
            )
        else:
            item = InventoryItem(
                barcode="PENDING",
                item_type=ITEM_TYPE_SINGLE,
                game=(raw.get("game") or "Other").strip(),
                card_name=card_name,
                set_name=(raw.get("set_name") or "").strip() or None,
                card_number=(raw.get("card_number") or "").strip() or None,
                variant=(raw.get("variant") or "").strip() or None,
                condition=(raw.get("condition") or "").strip() or None,
                image_url=(raw.get("image_url") or "").strip() or None,
                auto_price=_parse_float(str(raw.get("auto_price") or "")),
                notes=(raw.get("notes") or "").strip() or None,
                status=INVENTORY_IN_STOCK,
                created_at=utcnow(),
            )
            session.add(item)
            session.flush()  # get item.id without full commit

            item.barcode = generate_barcode_value(item.id)
            session.add(item)
        created.append({"id": item.id, "barcode": item.barcode, "card_name": item.card_name})
        capture_id = (raw.get("_v2_capture_id") or raw.get("capture_id") or "").strip()
        if capture_id:
            label = dict(raw)
            label.update({
                "card_name": item.card_name,
                "game": item.game,
                "set_name": item.set_name or "",
                "card_number": item.card_number or "",
                "variant": item.variant or "",
                "condition": item.condition or "",
                "image_url": item.image_url or "",
                "auto_price": item.auto_price,
                "notes": item.notes or "",
            })
            capture_label_updates.append((capture_id, label, int(item.id)))

    session.commit()
    for capture_id, label, inventory_item_id in capture_label_updates:
        attach_confirmed_label(
            capture_id,
            label,
            inventory_item_id=inventory_item_id,
            confirmed_by=confirmed_by,
        )
    return JSONResponse({"created": len(created), "items": created})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_float(value: str) -> Optional[float]:
    if not value or not value.strip():
        return None
    try:
        return round(float(value.strip().replace(",", "")), 2)
    except ValueError:
        return None
