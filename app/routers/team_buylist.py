"""
Staff buylist routes.

This is the employee-facing quote builder used at the counter when a customer
wants to sell cards to the store. Pricing is calculated server-side from a
manager-owned JSON config stored in AppSetting so the first version does not
need a schema migration.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from copy import deepcopy
from typing import Any, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlmodel import Session, select

from ..auth import has_permission
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AppSetting, AuditLog, BuylistSubmission, User, utcnow
from ..inventory.pokemon_scanner import text_search_cards
from ..shared import templates
from ..inventory.tcgplayer_sales import fetch_tcgplayer_public_sales, tcgplayer_product_id_from_url
from .team import _nav_context
from .team_admin import _permission_gate

router = APIRouter()
logger = logging.getLogger(__name__)


BUYLIST_CONFIG_KEY = "staff_buylist_config"
BUYLIST_SEARCH_RESULT_LIMIT = 12
BUYLIST_SEARCH_CACHE_TTL_SECONDS = 300
BUYLIST_SEARCH_CACHE_MAX = 128
BUYLIST_SEARCH_MIN_CHARS = 2
BUYLIST_SEARCH_MAX_CHARS = 500
BUYLIST_ALL_GAMES_VALUE = "__all__"
BUYLIST_PRODUCT_TYPE_CARD = "card"
BUYLIST_PRODUCT_TYPE_SEALED = "sealed"
BUYLIST_PRODUCT_TYPES = {BUYLIST_PRODUCT_TYPE_CARD, BUYLIST_PRODUCT_TYPE_SEALED}
CONDITION_PRICING_PERCENTAGE = "percentage_modifiers"
CONDITION_PRICING_TCGPLAYER = "tcgplayer_market"
CONDITION_PRICING_MODES = {CONDITION_PRICING_PERCENTAGE, CONDITION_PRICING_TCGPLAYER}
BUYLIST_SUBMISSION_STATUSES = ("submitted", "approved", "paid", "rejected")
NO_MARKET_PRICE_NOTE = "No market price found. Manager review required."
BUYLIST_EDIT_PERMISSION = "admin.buylist.edit"

_BUYLIST_SEARCH_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
BUYLIST_SEARCH_CACHE_VERSION = "buylist-search-v4"

BUYLIST_GAMES: tuple[dict[str, str], ...] = (
    {"game": "Pokemon", "label": "Pokemon", "category_id": "3"},
    {"game": "Pokemon JP", "label": "Pokemon JP", "category_id": "85"},
    {"game": "Magic", "label": "Magic: The Gathering", "category_id": "1"},
    {"game": "Yu-Gi-Oh", "label": "Yu-Gi-Oh", "category_id": "2"},
    {"game": "One Piece", "label": "One Piece", "category_id": "68"},
    {"game": "Lorcana", "label": "Disney Lorcana", "category_id": "71"},
    {"game": "Riftbound", "label": "Riftbound", "category_id": "89"},
)

_GAME_BY_NAME = {row["game"].lower(): row for row in BUYLIST_GAMES}
_GAME_BY_CATEGORY = {row["category_id"]: row for row in BUYLIST_GAMES}

DEFAULT_BUYLIST_CONFIG: dict[str, Any] = {
    "enabled_games": ["Pokemon", "Pokemon JP", "Magic", "Yu-Gi-Oh", "One Piece", "Lorcana", "Riftbound"],
    "default_game": "Pokemon",
    "default_payment": "cash",
    "condition_pricing_mode": CONDITION_PRICING_TCGPLAYER,
    "cash_ranges": [
        {"min": 0.0, "max": 0.49, "type": "fixed", "value": 0.01},
        {"min": 0.5, "max": 0.99, "type": "fixed", "value": 0.10},
        {"min": 1.0, "max": 2.99, "type": "fixed", "value": 0.20},
        {"min": 3.0, "max": 24.99, "type": "percentage", "value": 50.0},
        {"min": 25.0, "max": 99.99, "type": "percentage", "value": 60.0},
        {"min": 100.0, "max": 999999.0, "type": "percentage", "value": 65.0},
    ],
    "trade_ranges": [
        {"min": 0.0, "max": 0.49, "type": "fixed", "value": 0.02},
        {"min": 0.5, "max": 0.99, "type": "fixed", "value": 0.15},
        {"min": 1.0, "max": 2.99, "type": "fixed", "value": 0.25},
        {"min": 3.0, "max": 24.99, "type": "percentage", "value": 60.0},
        {"min": 25.0, "max": 99.99, "type": "percentage", "value": 70.0},
        {"min": 100.0, "max": 999999.0, "type": "percentage", "value": 75.0},
    ],
    "condition_modifiers": {"NM": 100.0, "LP": 85.0, "MP": 65.0, "HP": 45.0, "DMG": 25.0},
    "language_modifiers": {"English": 100.0, "Japanese": 90.0, "Other": 80.0},
    "printing_modifiers": {
        "Normal": 100.0,
        "Holofoil": 100.0,
        "Reverse Holofoil": 95.0,
        "Foil": 100.0,
        "1st Edition": 100.0,
        "Unlimited": 100.0,
    },
    "hotlist_rules": [],
    "darklist_rules": [],
    "checkout_note": "",
}

CONDITION_OPTIONS = ("NM", "LP", "MP", "HP", "DMG")
LANGUAGE_OPTIONS = ("English", "Japanese", "Other")
_CONDITION_ALIASES: dict[str, tuple[str, ...]] = {
    "NM": ("NM", "NEAR MINT", "NEARMINT"),
    "LP": ("LP", "LIGHTLY PLAYED", "LIGHT PLAY", "EXCELLENT"),
    "MP": ("MP", "MODERATELY PLAYED", "MODERATE PLAY", "PLAYED"),
    "HP": ("HP", "HEAVILY PLAYED", "HEAVY PLAY"),
    "DMG": ("DMG", "DM", "DAMAGED"),
}


def _portal_or_404() -> None:
    if not get_settings().employee_portal_enabled:
        raise HTTPException(status_code=404)


def _require_team_user(
    request: Request,
    session: Session,
) -> tuple[Optional[Response], Optional[User]]:
    _portal_or_404()
    user: Optional[User] = getattr(request.state, "current_user", None)
    if user is None:
        return RedirectResponse("/team/login", status_code=303), None
    return None, user


def _require_buylist_admin(
    request: Request,
    session: Session,
) -> tuple[Optional[Response], Optional[User]]:
    return _permission_gate(request, session, BUYLIST_EDIT_PERMISSION)


def _can_manage_buylist_pricing(session: Session, user: Optional[User]) -> bool:
    if user is None or getattr(user, "role", None) not in {"admin", "manager", "reviewer"}:
        return False
    return has_permission(session, user, BUYLIST_EDIT_PERMISSION)


def _deep_merge(defaults: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(defaults)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def get_buylist_config(session: Session) -> dict[str, Any]:
    row = session.get(AppSetting, BUYLIST_CONFIG_KEY)
    if row is None or not (row.value or "").strip():
        return deepcopy(DEFAULT_BUYLIST_CONFIG)
    try:
        saved = json.loads(row.value)
    except json.JSONDecodeError:
        saved = {}
    if not isinstance(saved, dict):
        saved = {}
    config = _deep_merge(DEFAULT_BUYLIST_CONFIG, saved)
    config["enabled_games"] = [
        game for game in config.get("enabled_games", []) if game.lower() in _GAME_BY_NAME
    ] or list(DEFAULT_BUYLIST_CONFIG["enabled_games"])
    if str(config.get("default_game") or "").lower() not in _GAME_BY_NAME:
        config["default_game"] = "Pokemon"
    if config["default_game"] not in config["enabled_games"]:
        config["default_game"] = config["enabled_games"][0]
    if config.get("condition_pricing_mode") not in CONDITION_PRICING_MODES:
        config["condition_pricing_mode"] = DEFAULT_BUYLIST_CONFIG["condition_pricing_mode"]
    return config


def save_buylist_config(session: Session, config: dict[str, Any]) -> None:
    row = session.get(AppSetting, BUYLIST_CONFIG_KEY)
    if row is None:
        row = AppSetting(key=BUYLIST_CONFIG_KEY)
        session.add(row)
    row.value = json.dumps(config, sort_keys=True)
    session.commit()
    _BUYLIST_SEARCH_CACHE.clear()


def _buylist_config_fingerprint(config: dict[str, Any]) -> str:
    raw = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _buylist_search_cache_key(
    query: str,
    category_id: str,
    config: dict[str, Any],
    *,
    product_type: str = BUYLIST_PRODUCT_TYPE_CARD,
) -> str:
    normalized = re.sub(r"\s+", " ", query.strip().lower())
    product_type = _normalize_product_type(product_type)
    return (
        f"{BUYLIST_SEARCH_CACHE_VERSION}:{product_type}:{category_id}:"
        f"{normalized}:{_buylist_config_fingerprint(config)}"
    )


def _buylist_search_cache_get(key: str) -> Optional[dict[str, Any]]:
    cached = _BUYLIST_SEARCH_CACHE.get(key)
    if not cached:
        return None
    expires_at, payload = cached
    if expires_at <= time.monotonic():
        _BUYLIST_SEARCH_CACHE.pop(key, None)
        return None
    response = deepcopy(payload)
    response["cached"] = True
    return response


def _buylist_search_cache_set(key: str, payload: dict[str, Any]) -> None:
    if len(_BUYLIST_SEARCH_CACHE) >= BUYLIST_SEARCH_CACHE_MAX:
        oldest_key = min(_BUYLIST_SEARCH_CACHE, key=lambda item: _BUYLIST_SEARCH_CACHE[item][0])
        _BUYLIST_SEARCH_CACHE.pop(oldest_key, None)
    cached_payload = deepcopy(payload)
    cached_payload["cached"] = False
    _BUYLIST_SEARCH_CACHE[key] = (
        time.monotonic() + BUYLIST_SEARCH_CACHE_TTL_SECONDS,
        cached_payload,
    )


def _enabled_game_options(config: dict[str, Any]) -> list[dict[str, str]]:
    enabled = {str(game).lower() for game in config.get("enabled_games", [])}
    return [row for row in BUYLIST_GAMES if row["game"].lower() in enabled]


def _default_buylist_game(config: dict[str, Any]) -> str:
    enabled = _enabled_game_options(config)
    configured = str(config.get("default_game") or "Pokemon").strip()
    if _is_all_games(configured):
        configured = "Pokemon"
    row = _GAME_BY_NAME.get(configured.lower())
    if row and any(row["game"] == option["game"] for option in enabled):
        return row["game"]
    pokemon = _GAME_BY_NAME["pokemon"]
    if any(pokemon["game"] == option["game"] for option in enabled):
        return pokemon["game"]
    return (enabled[0] if enabled else pokemon)["game"]


def _is_all_games(value: str | None) -> bool:
    normalized = (value or "").strip().lower()
    return normalized in {
        BUYLIST_ALL_GAMES_VALUE,
        "all",
        "all games",
        "all-games",
    }


def _normalize_product_type(value: str | None) -> str:
    normalized = (value or BUYLIST_PRODUCT_TYPE_CARD).strip().lower()
    return normalized if normalized in BUYLIST_PRODUCT_TYPES else BUYLIST_PRODUCT_TYPE_CARD


def _category_for_game(game: str | None, config: dict[str, Any]) -> str:
    enabled = _enabled_game_options(config)
    default_game = _default_buylist_game(config)
    selected = (game or default_game or "Pokemon").strip().lower()
    if _is_all_games(selected):
        selected = default_game.lower()
    row = _GAME_BY_NAME.get(selected)
    if row and any(row["game"] == option["game"] for option in enabled):
        return row["category_id"]
    return (enabled[0] if enabled else _GAME_BY_NAME["pokemon"])["category_id"]


def _game_for_category(category_id: str) -> str:
    return _GAME_BY_CATEGORY.get(str(category_id), _GAME_BY_NAME["pokemon"])["game"]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if number != number:
        return default
    return number


def _money(value: float) -> float:
    return round(max(0.0, float(value or 0.0)) + 1e-9, 2)


def _range_offer(market_price: float, ranges: list[dict[str, Any]]) -> tuple[float, str]:
    price = max(0.0, float(market_price or 0.0))
    selected: Optional[dict[str, Any]] = None
    for row in ranges or []:
        low = _float(row.get("min"), 0.0)
        high = _float(row.get("max"), 999999.0)
        if low <= price <= high:
            selected = row
            break
    if selected is None and ranges:
        selected = ranges[-1]
    if not selected:
        return 0.0, "No range"
    mode = str(selected.get("type") or "percentage").lower()
    value = _float(selected.get("value"), 0.0)
    if mode == "fixed":
        return value, f"Fixed ${value:g}"
    if mode == "by_appointment":
        return 0.0, "By appointment"
    return price * (value / 100.0), f"{value:g}%"


def _modifier_percent(config: dict[str, Any], group: str, key: str, fallback: float = 100.0) -> float:
    options = config.get(group) or {}
    if key in options:
        return _float(options.get(key), fallback)
    normalized = key.strip().lower()
    for opt_key, opt_value in options.items():
        if str(opt_key).strip().lower() == normalized:
            return _float(opt_value, fallback)
    return _float(options.get("Other"), fallback)


def _normalize_condition(condition: str | None) -> str:
    normalized = (condition or "NM").strip().upper()
    if normalized == "DM":
        return "DMG"
    for canonical, aliases in _CONDITION_ALIASES.items():
        if normalized in aliases:
            return canonical
    return normalized if normalized in CONDITION_OPTIONS else "NM"


def _condition_alias_tokens(condition: str) -> tuple[str, ...]:
    return _CONDITION_ALIASES.get(_normalize_condition(condition), (condition,))


def _condition_price_value(value: Any) -> Optional[float]:
    if isinstance(value, dict):
        for key in ("mkt", "market", "market_price", "price"):
            if key in value:
                price = _float(value.get(key), -1.0)
                if price > 0:
                    return price
        return None
    price = _float(value, -1.0)
    return price if price > 0 else None


def _condition_raw_value_from_conditions(
    conditions: dict[str, Any],
    condition: str,
) -> Any:
    if not isinstance(conditions, dict):
        return None
    aliases = {alias.upper() for alias in _condition_alias_tokens(condition)}
    for raw_key, raw_value in conditions.items():
        key = str(raw_key or "").strip().upper()
        if key in aliases:
            return raw_value
    for raw_key, raw_value in conditions.items():
        compact = re.sub(r"[^A-Z]", "", str(raw_key or "").upper())
        for alias in aliases:
            alias_compact = re.sub(r"[^A-Z]", "", alias)
            if compact and (
                compact == alias_compact
                or (len(alias_compact) >= 6 and alias_compact in compact)
            ):
                return raw_value
    return None


def _condition_market_price_from_conditions(
    conditions: dict[str, Any],
    condition: str,
) -> Optional[float]:
    return _condition_price_value(_condition_raw_value_from_conditions(conditions, condition))


def _condition_price_metric(value: Any) -> dict[str, Any]:
    metric: dict[str, Any] = {}
    if isinstance(value, dict):
        for out_key, source_keys in (
            ("market", ("mkt", "market", "market_price", "price")),
            ("low", ("low", "lowest", "lowest_price", "low_price")),
            ("high", ("hi", "high", "highest", "highest_price", "high_price")),
        ):
            for source_key in source_keys:
                if source_key in value:
                    price = _condition_price_value(value.get(source_key))
                    if price is not None:
                        metric[out_key] = _money(price)
                        break
        for source_key in ("cnt", "count", "price_count", "listing_count"):
            if source_key in value:
                count = int(max(0, _float(value.get(source_key), 0.0)))
                if count:
                    metric["listing_count"] = count
                    break
        for source_key in ("sku", "sku_id", "skuId"):
            if value.get(source_key):
                metric["sku_id"] = str(value.get(source_key))
                break
    else:
        price = _condition_price_value(value)
        if price is not None:
            metric["market"] = _money(price)
    return metric


def _variant_matches(variant: dict[str, Any], selected_variant: str) -> bool:
    return str(variant.get("name") or "").strip().lower() == selected_variant.strip().lower()


def _condition_market_prices_for_variant(
    variants: list[dict[str, Any]],
    selected_variant: str,
) -> dict[str, float]:
    variant = _select_variant([row for row in variants if isinstance(row, dict)], selected_variant)
    if not variant:
        return {}
    conditions = variant.get("conditions") or {}
    prices: dict[str, float] = {}
    for condition in CONDITION_OPTIONS:
        price = _condition_market_price_from_conditions(conditions, condition)
        if price is not None:
            prices[condition] = _money(price)
    return prices


def _condition_price_metrics_for_variant(
    variants: list[dict[str, Any]],
    selected_variant: str,
) -> dict[str, dict[str, Any]]:
    variant = _select_variant([row for row in variants if isinstance(row, dict)], selected_variant)
    if not variant:
        return {}
    conditions = variant.get("conditions") or {}
    metrics: dict[str, dict[str, Any]] = {}
    for condition in CONDITION_OPTIONS:
        raw_value = _condition_raw_value_from_conditions(conditions, condition)
        metric = _condition_price_metric(raw_value)
        if metric:
            metrics[condition] = metric
    return metrics


def _condition_market_price_from_item(
    item: dict[str, Any],
    condition: str,
    variant: str,
) -> Optional[float]:
    prices = item.get("condition_market_prices") or item.get("condition_prices") or {}
    if isinstance(prices, dict):
        price = _condition_market_price_from_conditions(prices, condition)
        if price is not None:
            return price
        normalized = _normalize_condition(condition)
        if normalized in prices:
            return _condition_price_value(prices.get(normalized))

    variants = item.get("available_variants") or []
    if isinstance(variants, list):
        return _condition_market_prices_for_variant(
            [row for row in variants if isinstance(row, dict)],
            variant,
        ).get(_normalize_condition(condition))
    return None


def _condition_pricing_mode(config: dict[str, Any]) -> str:
    mode = str(config.get("condition_pricing_mode") or CONDITION_PRICING_TCGPLAYER).strip().lower()
    return mode if mode in CONDITION_PRICING_MODES else CONDITION_PRICING_TCGPLAYER


def _pattern_matches(pattern: str, product: dict[str, Any]) -> bool:
    needle = (pattern or "").strip().lower()
    if not needle:
        return False
    haystacks = [
        str(product.get("id") or ""),
        str(product.get("product_id") or ""),
        str(product.get("name") or ""),
        str(product.get("set_name") or ""),
        str(product.get("number") or ""),
        str(product.get("upc") or ""),
        str(product.get("sealed_product_kind") or ""),
        str(product.get("item_type") or ""),
    ]
    return any(needle in value.lower() for value in haystacks if value)


def _list_adjustment(config: dict[str, Any], product: dict[str, Any]) -> tuple[float, list[str], bool]:
    multiplier = 1.0
    notes: list[str] = []
    blocked = False
    for rule in config.get("hotlist_rules") or []:
        pattern = str(rule.get("pattern") or "")
        if _pattern_matches(pattern, product):
            boost = _float(rule.get("percent"), 0.0)
            multiplier *= 1.0 + boost / 100.0
            notes.append(f"Hotlist +{boost:g}%")
    for rule in config.get("darklist_rules") or []:
        pattern = str(rule.get("pattern") or "")
        if _pattern_matches(pattern, product):
            penalty = _float(rule.get("percent"), 0.0)
            if penalty >= 100:
                blocked = True
            multiplier *= max(0.0, 1.0 - penalty / 100.0)
            notes.append(f"Darklist -{penalty:g}%")
    return multiplier, notes, blocked


def calculate_buylist_offer(
    config: dict[str, Any],
    *,
    market_price: float,
    condition_market_price: Optional[float] = None,
    condition: str = "NM",
    language: str = "English",
    printing: str = "Normal",
    product: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    product = product or {}
    is_sealed = str(product.get("item_type") or "").strip().lower() == BUYLIST_PRODUCT_TYPE_SEALED
    condition = "Sealed" if is_sealed else _normalize_condition(condition)
    language = (language or "English").strip() or "English"
    printing = (printing or "Normal").strip() or "Normal"

    pricing_mode = _condition_pricing_mode(config)
    condition_market = _float(condition_market_price, -1.0)
    using_condition_market = (
        not is_sealed
        and pricing_mode == CONDITION_PRICING_TCGPLAYER
        and condition_market > 0
    )
    effective_market_price = condition_market if using_condition_market else market_price

    base_cash, cash_rule = _range_offer(effective_market_price, config.get("cash_ranges") or [])
    base_trade, trade_rule = _range_offer(effective_market_price, config.get("trade_ranges") or [])

    condition_mod = 100.0 if (is_sealed or using_condition_market) else _modifier_percent(config, "condition_modifiers", condition)
    language_mod = 100.0 if is_sealed else _modifier_percent(config, "language_modifiers", language)
    printing_mod = 100.0 if is_sealed else _modifier_percent(config, "printing_modifiers", printing)
    list_multiplier, list_notes, blocked = _list_adjustment(config, product)
    missing_market_price = effective_market_price <= 0
    if missing_market_price:
        blocked = True
    total_multiplier = (condition_mod / 100.0) * (language_mod / 100.0) * (printing_mod / 100.0) * list_multiplier

    cash = 0.0 if blocked else base_cash * total_multiplier
    trade = 0.0 if blocked else base_trade * total_multiplier
    if is_sealed:
        condition_note = "Sealed product TCGPlayer market"
        condition_source = BUYLIST_PRODUCT_TYPE_SEALED
    elif using_condition_market:
        condition_note = f"{condition} TCGPlayer market"
        condition_source = CONDITION_PRICING_TCGPLAYER
    elif pricing_mode == CONDITION_PRICING_TCGPLAYER:
        condition_note = f"{condition} modifier fallback {condition_mod:g}%"
        condition_source = "modifier_fallback"
    else:
        condition_note = f"{condition} {condition_mod:g}%"
        condition_source = CONDITION_PRICING_PERCENTAGE
    notes = [condition_note, *list_notes] if is_sealed else [
        condition_note,
        f"{language} {language_mod:g}%",
        f"{printing} {printing_mod:g}%",
        *list_notes,
    ]
    if missing_market_price:
        notes.append(NO_MARKET_PRICE_NOTE)
    if blocked:
        notes.append("Not buying")

    return {
        "market_price": _money(effective_market_price),
        "base_market_price": _money(market_price),
        "condition_market_price": _money(condition_market) if using_condition_market else None,
        "condition_price_source": condition_source,
        "cash_offer": _money(cash),
        "trade_offer": _money(trade),
        "cash_rule": cash_rule,
        "trade_rule": trade_rule,
        "modifier_percent": round(total_multiplier * 100.0, 2),
        "notes": notes,
        "blocked": blocked,
    }


def _select_variant(
    variants: list[dict[str, Any]],
    selected_variant: str | None = None,
) -> dict[str, Any] | None:
    selected = (selected_variant or "").strip()
    if selected:
        for variant in variants:
            if str(variant.get("name") or "").strip().lower() == selected.lower():
                return variant
    for variant in variants:
        if str(variant.get("name") or "").strip().lower() == "normal":
            return variant
    for variant in variants:
        if variant.get("price") is not None:
            return variant
    return variants[0] if variants else None


def _variant_price(candidate: dict[str, Any], selected_variant: str | None = None) -> tuple[float, str]:
    variants = [row for row in (candidate.get("available_variants") or []) if isinstance(row, dict)]
    selected = (selected_variant or candidate.get("variant") or "").strip()
    variant = _select_variant(variants, selected)
    if variant:
        price = variant.get("price")
        if price is not None:
            return _float(price), str(variant.get("name") or selected or "Market")
    return _float(candidate.get("market_price")), selected or "Normal"


def _tcgplayer_product_id_from_candidate(candidate: dict[str, Any]) -> str:
    for key in ("tcgplayer_product_id", "tcgplayer_id", "product_id", "external_id"):
        value = str(candidate.get(key) or "").strip()
        if re.fullmatch(r"\d{1,12}", value):
            return value
    for key in ("tcgplayer_url", "external_url", "url"):
        product_id = tcgplayer_product_id_from_url(str(candidate.get(key) or ""))
        if product_id:
            return product_id
    return ""


def _candidate_payload(
    candidate: dict[str, Any],
    config: dict[str, Any],
    *,
    category_id: str,
) -> dict[str, Any]:
    market_price, variant = _variant_price(candidate)
    variants = candidate.get("available_variants") or []
    condition_market_prices = _condition_market_prices_for_variant(
        [row for row in variants if isinstance(row, dict)],
        variant,
    )
    condition_price_metrics = _condition_price_metrics_for_variant(
        [row for row in variants if isinstance(row, dict)],
        variant,
    )
    tcgplayer_url = str(candidate.get("tcgplayer_url") or candidate.get("external_url") or "").strip()
    tcgplayer_product_id = _tcgplayer_product_id_from_candidate(candidate)
    product = {
        "id": candidate.get("id") or "",
        "product_id": tcgplayer_product_id or candidate.get("product_id") or candidate.get("id") or "",
        "name": candidate.get("name") or "",
        "set_name": candidate.get("set_name") or "",
        "number": candidate.get("number") or "",
    }
    offer = calculate_buylist_offer(
        config,
        market_price=market_price,
        condition_market_price=condition_market_prices.get("NM"),
        condition="NM",
        language="Japanese" if category_id == "85" else "English",
        printing=variant,
        product=product,
    )
    return {
        "id": candidate.get("id") or "",
        "product_id": tcgplayer_product_id or candidate.get("product_id") or candidate.get("id") or "",
        "tcgplayer_product_id": tcgplayer_product_id,
        "item_type": BUYLIST_PRODUCT_TYPE_CARD,
        "game": _game_for_category(category_id),
        "category_id": category_id,
        "name": candidate.get("name") or "",
        "set_name": candidate.get("set_name") or "",
        "number": candidate.get("number") or "",
        "rarity": candidate.get("rarity") or "",
        "variant": variant,
        "available_variants": variants,
        "condition_market_prices": condition_market_prices,
        "condition_price_metrics": condition_price_metrics,
        "condition_pricing_mode": _condition_pricing_mode(config),
        "image_url": candidate.get("image_url") or candidate.get("image_url_small") or "",
        "image_url_small": candidate.get("image_url_small") or candidate.get("image_url") or "",
        "external_url": tcgplayer_url,
        "tcgplayer_url": tcgplayer_url,
        "market_price": offer["market_price"],
        "base_market_price": offer["base_market_price"],
        "cash_offer": offer["cash_offer"],
        "trade_offer": offer["trade_offer"],
        "pricing_notes": offer["notes"],
        "blocked": offer["blocked"],
    }


async def _search_buylist_sealed_products(
    query: str,
    *,
    game: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    from ..inventory.routes import _cached_add_stock_sealed_search

    return await _cached_add_stock_sealed_search(query, game=game, limit=limit)


def _dedupe_key(payload: dict[str, Any]) -> str:
    display_name = str(payload.get("name") or "").strip().lower()
    display_set = str(payload.get("set_name") or "").strip().lower()
    display_number = str(payload.get("number") or "").strip().lower()
    display_number_core = display_number.split("/", 1)[0].lstrip("0") or display_number
    display_game = str(payload.get("game") or "").strip().lower()
    if display_name and display_set and display_number_core:
        return f"display:{display_game}:{display_name}:{display_set}:{display_number_core}"
    for key in ("tcgplayer_product_id", "product_id", "id", "external_id"):
        value = str(payload.get(key) or "").strip().lower()
        if value:
            return f"{key}:{value}"
    return "|".join(
        str(payload.get(key) or "").strip().lower()
        for key in ("game", "name", "set_name", "number", "upc")
    )


def _dedupe_payloads(payloads: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for payload in payloads:
        key = _dedupe_key(payload)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
        if len(deduped) >= limit:
            break
    return deduped


def _sealed_product_payload(
    product: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    market_price = _float(product.get("market_price"), 0.0)
    product_ref = {
        "id": product.get("external_id") or "",
        "product_id": product.get("external_id") or "",
        "item_type": BUYLIST_PRODUCT_TYPE_SEALED,
        "name": product.get("name") or "",
        "set_name": product.get("set_name") or "",
        "number": product.get("upc") or "",
        "upc": product.get("upc") or "",
        "sealed_product_kind": product.get("kind") or "",
    }
    offer = calculate_buylist_offer(
        config,
        market_price=market_price,
        condition="Sealed",
        language="",
        printing="Sealed Product",
        product=product_ref,
    )
    product_id = product.get("external_id") or _sealed_product_fallback_id(product)
    return {
        "id": product_id,
        "product_id": product_id,
        "tcgplayer_product_id": product_id if re.fullmatch(r"\d{1,12}", str(product_id)) else "",
        "item_type": BUYLIST_PRODUCT_TYPE_SEALED,
        "game": product.get("game") or "",
        "category_id": str(product.get("category_id") or ""),
        "name": product.get("name") or "",
        "set_name": product.get("set_name") or "",
        "number": product.get("upc") or "",
        "upc": product.get("upc") or "",
        "sealed_product_kind": product.get("kind") or "",
        "rarity": product.get("kind") or "",
        "variant": "Sealed Product",
        "available_variants": [],
        "condition_market_prices": {},
        "condition_price_metrics": {},
        "condition_pricing_mode": _condition_pricing_mode(config),
        "image_url": product.get("image_url") or product.get("image_url_small") or "",
        "image_url_small": product.get("image_url_small") or product.get("image_url") or "",
        "external_url": product.get("external_url") or "",
        "market_price": offer["market_price"],
        "base_market_price": offer["base_market_price"],
        "cash_offer": offer["cash_offer"],
        "trade_offer": offer["trade_offer"],
        "pricing_notes": offer["notes"],
        "blocked": offer["blocked"],
    }


def _sealed_product_fallback_id(product: dict[str, Any]) -> str:
    parts = [
        str(product.get("game") or ""),
        str(product.get("name") or ""),
        str(product.get("set_name") or ""),
        str(product.get("kind") or ""),
        str(product.get("upc") or ""),
    ]
    return "sealed:" + hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _parse_ranges(form: dict[str, Any], prefix: str) -> list[dict[str, Any]]:
    ranges: list[dict[str, Any]] = []
    for index in range(8):
        min_value = str(form.get(f"{prefix}_min_{index}") or "").strip()
        max_value = str(form.get(f"{prefix}_max_{index}") or "").strip()
        value = str(form.get(f"{prefix}_value_{index}") or "").strip()
        mode = str(form.get(f"{prefix}_type_{index}") or "percentage").strip().lower()
        if not (min_value or max_value or value):
            continue
        ranges.append(
            {
                "min": _float(min_value, 0.0),
                "max": _float(max_value, 999999.0),
                "type": mode if mode in {"percentage", "fixed", "by_appointment"} else "percentage",
                "value": _float(value, 0.0),
            }
        )
    return ranges


def _parse_modifier_group(form: dict[str, Any], prefix: str, defaults: dict[str, float]) -> dict[str, float]:
    values: dict[str, float] = {}
    for key, default in defaults.items():
        field = f"{prefix}_{re.sub(r'[^a-z0-9]+', '_', key.lower()).strip('_')}"
        values[key] = _float(form.get(field), _float(default, 100.0))
    return values


def _parse_list_rules(raw: str) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    for line in (raw or "").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if "," in text:
            pattern, percent = text.split(",", 1)
        else:
            pattern, percent = text, "0"
        pattern = pattern.strip()
        if pattern:
            rules.append({"pattern": pattern, "percent": _float(percent, 0.0)})
    return rules


def _rules_to_text(rules: list[dict[str, Any]]) -> str:
    return "\n".join(
        f"{str(rule.get('pattern') or '').strip()}, {float(rule.get('percent') or 0):g}"
        for rule in rules or []
        if str(rule.get("pattern") or "").strip()
    )


def _json_loads(value: str, fallback: Any, *, label: str = "buylist_json") -> Any:
    try:
        parsed = json.loads(value or "")
    except (TypeError, json.JSONDecodeError):
        logger.warning("Invalid %s; using fallback.", label, exc_info=True)
        return deepcopy(fallback)
    return parsed if parsed is not None else deepcopy(fallback)


def _actor_label(user: Optional[User]) -> str:
    if user is None:
        return "buylist"
    return (
        getattr(user, "display_name", None)
        or getattr(user, "username", None)
        or f"user:{getattr(user, 'id', '')}"
        or "buylist"
    )


def _inventory_game_for_buylist(game: str | None) -> str:
    value = (game or "Pokemon").strip()
    if value == "Pokemon JP":
        return "Pokemon"
    if value == "MTG":
        return "Magic"
    return value or "Pokemon"


def _line_selected_unit_cost(line: dict[str, Any], payment_view: str) -> float:
    if payment_view == "trade":
        return _money(_float(line.get("unit_trade"), _float(line.get("trade_offer"), 0.0)))
    return _money(_float(line.get("unit_cash"), _float(line.get("cash_offer"), 0.0)))


def _submission_to_view(row: BuylistSubmission, submitter: Optional[User]) -> dict[str, Any]:
    return {
        "row": row,
        "submitter": submitter,
        "totals": _json_loads(row.totals_json, {}, label=f"buylist_submission.{row.id}.totals_json"),
        "lines": _json_loads(row.lines_json, [], label=f"buylist_submission.{row.id}.lines_json"),
        "inventory_result": _json_loads(
            row.inventory_result_json,
            {},
            label=f"buylist_submission.{row.id}.inventory_result_json",
        ),
    }


def _buylist_submission_status_counts(session: Session) -> tuple[dict[str, int], int]:
    counts = {status_key: 0 for status_key in BUYLIST_SUBMISSION_STATUSES}
    unknown_count = 0
    for raw_status in session.exec(select(BuylistSubmission.status)).all():
        status_key = str(raw_status or "").strip()
        if status_key in counts:
            counts[status_key] += 1
        else:
            unknown_count += 1
    return counts, unknown_count


def _receive_submission_inventory(
    session: Session,
    submission: BuylistSubmission,
    *,
    actor: User,
    location: str = "",
) -> dict[str, Any]:
    from ..inventory.routes import _receive_sealed_stock, _receive_single_stock

    payment_view = (submission.payment_view or "cash").strip().lower()
    if payment_view not in {"cash", "trade"}:
        payment_view = "cash"
    lines = _json_loads(
        submission.lines_json,
        [],
        label=f"buylist_submission.{submission.id}.lines_json",
    )
    if not isinstance(lines, list) or not lines:
        raise ValueError("Submission has no line items.")

    created_items: list[dict[str, Any]] = []
    actor_label = _actor_label(actor)
    location = (location or "Buylist intake").strip()
    source = f"Staff Buylist #{submission.id}"
    notes = f"Customer: {submission.customer_name or 'Customer'}"
    if submission.notes:
        notes = f"{notes}; {submission.notes[:400]}"

    for line in lines:
        if not isinstance(line, dict):
            continue
        quantity = max(1, min(int(_float(line.get("quantity"), 1)), 999))
        unit_cost = _line_selected_unit_cost(line, payment_view)
        market_price = _float(line.get("market_price"), _float(line.get("base_market_price"), 0.0))
        item_type = _normalize_product_type(str(line.get("item_type") or BUYLIST_PRODUCT_TYPE_CARD))
        if item_type == BUYLIST_PRODUCT_TYPE_SEALED:
            item, movement, created = _receive_sealed_stock(
                session,
                game=_inventory_game_for_buylist(str(line.get("game") or "")),
                product_name=str(line.get("name") or ""),
                set_name=str(line.get("set_name") or ""),
                sealed_product_kind=str(line.get("sealed_product_kind") or line.get("rarity") or ""),
                upc=str(line.get("upc") or line.get("number") or ""),
                image_url=str(line.get("image_url") or ""),
                quantity=quantity,
                unit_cost=unit_cost,
                list_price=market_price or None,
                location=location,
                source=source,
                notes=notes,
                actor_label=actor_label,
            )
        else:
            item, movement, created = _receive_single_stock(
                session,
                game=_inventory_game_for_buylist(str(line.get("game") or "")),
                card_name=str(line.get("name") or ""),
                set_name=str(line.get("set_name") or ""),
                set_code=str(line.get("set_code") or ""),
                card_number=str(line.get("number") or ""),
                variant=str(line.get("variant") or "Normal"),
                condition=str(line.get("condition") or "NM"),
                image_url=str(line.get("image_url") or ""),
                quantity=quantity,
                unit_cost=unit_cost,
                list_price=market_price or None,
                auto_price=market_price or None,
                location=location,
                source=source,
                notes=notes,
                price_payload={"buylist_line": line},
                actor_label=actor_label,
            )
            language = str(line.get("language") or "").strip()
            if not language:
                language = "Japanese" if str(line.get("game") or "").strip() == "Pokemon JP" else "English"
            if language not in LANGUAGE_OPTIONS:
                language = "Other"
            if language and item.language != language:
                item.language = language
                session.add(item)
                session.commit()
        created_items.append(
            {
                "inventory_item_id": item.id,
                "movement_id": movement.id,
                "name": item.card_name,
                "item_type": item.item_type,
                "quantity": quantity,
                "unit_cost": unit_cost,
                "created": created,
            }
        )

    if not created_items:
        raise ValueError("No valid line items were available to receive.")
    return {"items": created_items, "location": location, "payment_view": payment_view}


@router.get("/team/buylist", response_class=HTMLResponse)
def staff_buylist_page(request: Request, session: Session = Depends(get_session)):
    denial, user = _require_team_user(request, session)
    if denial:
        return denial
    config = get_buylist_config(session)
    can_manage_buylist_pricing = _can_manage_buylist_pricing(session, user)
    return templates.TemplateResponse(
        request,
        "team/buylist.html",
        {
            "request": request,
            "title": "Buylist",
            "active": "buylist",
            "current_user": user,
            "config": config,
            "game_options": _enabled_game_options(config),
            "search_default_game": _default_buylist_game(config),
            "condition_options": CONDITION_OPTIONS,
            "language_options": LANGUAGE_OPTIONS,
            "csrf_token": issue_token(request),
            "can_manage_buylist_pricing": can_manage_buylist_pricing,
            **_nav_context(session, user),
        },
    )


@router.get("/team/buylist/search")
async def staff_buylist_search(
    request: Request,
    q: str = Query(default=""),
    game: str = Query(default="Pokemon"),
    product_type: str = Query(default=BUYLIST_PRODUCT_TYPE_CARD),
    session: Session = Depends(get_session),
):
    denial, _user = _require_team_user(request, session)
    if denial:
        return denial
    config = get_buylist_config(session)
    query = re.sub(r"\s+", " ", (q or "").strip())
    if not query:
        return JSONResponse({"ok": True, "cards": [], "message": "Enter a card name or number."})
    if len(query) < BUYLIST_SEARCH_MIN_CHARS:
        return JSONResponse(
            {
                "ok": True,
                "cards": [],
                "message": f"Type at least {BUYLIST_SEARCH_MIN_CHARS} characters.",
            }
        )
    if len(query) > BUYLIST_SEARCH_MAX_CHARS:
        return JSONResponse(
            {
                "ok": False,
                "cards": [],
                "error": f"Search is too long. Keep it under {BUYLIST_SEARCH_MAX_CHARS} characters.",
            },
            status_code=400,
        )
    selected_game = _default_buylist_game(config) if _is_all_games(game) else (game or _default_buylist_game(config))
    category_id = _category_for_game(selected_game, config)
    product_type = _normalize_product_type(product_type)
    cache_key = _buylist_search_cache_key(
        query,
        category_id,
        config,
        product_type=product_type,
    )
    cached = _buylist_search_cache_get(cache_key)
    if cached:
        return JSONResponse(cached)

    if product_type == BUYLIST_PRODUCT_TYPE_SEALED:
        game_name = _game_for_category(category_id)
        raw_products, warning = await _search_buylist_sealed_products(
            query,
            game=game_name,
            limit=BUYLIST_SEARCH_RESULT_LIMIT,
        )
        cards = [
            _sealed_product_payload(product, config)
            for product in raw_products
            if isinstance(product, dict)
        ]
        if cards:
            warning = ""
        payload = {
            "ok": True,
            "status": "MATCHED" if cards else "NO_MATCH",
            "product_type": product_type,
            "game": game_name,
            "category_id": category_id,
            "cards": cards,
            "error": warning or ("" if cards else f"No sealed products found for '{query}'."),
            "processing_time_ms": None,
            "cached": False,
        }
        _buylist_search_cache_set(cache_key, payload)
        return JSONResponse(payload)

    result = await text_search_cards(
        query,
        category_id=category_id,
        use_ai_parse=False,
        max_results=BUYLIST_SEARCH_RESULT_LIMIT,
        include_pokemontcg_supplement=False,
        allow_cross_category_pricing=False,
        allow_pokemontcg_price_fallback=False,
    )
    raw_cards = result.get("candidates") or [] if isinstance(result, dict) else []
    cards = [
        _candidate_payload(card, config, category_id=category_id)
        for card in raw_cards
        if isinstance(card, dict)
    ]
    cards = _dedupe_payloads(cards, limit=BUYLIST_SEARCH_RESULT_LIMIT)
    warning = result.get("error") if isinstance(result, dict) else None
    processing_time_ms = result.get("processing_time_ms") if isinstance(result, dict) else None
    status = result.get("status") if isinstance(result, dict) else None
    game_name = _game_for_category(category_id)
    payload = {
        "ok": True,
        "status": status,
        "product_type": product_type,
        "game": game_name,
        "category_id": category_id,
        "cards": cards,
        "error": warning,
        "processing_time_ms": processing_time_ms,
        "cached": False,
    }
    _buylist_search_cache_set(cache_key, payload)
    return JSONResponse(payload)


@router.get("/team/buylist/sales-history")
async def staff_buylist_sales_history(
    request: Request,
    product_id: str = Query(default=""),
    tcgplayer_url: str = Query(default=""),
    condition: str = Query(default="NM"),
    variant: str = Query(default=""),
    language: str = Query(default="English"),
    session: Session = Depends(get_session),
):
    denial, _user = _require_team_user(request, session)
    if denial:
        return denial

    resolved_product_id = str(product_id or "").strip()
    if not re.fullmatch(r"\d{1,12}", resolved_product_id):
        resolved_product_id = tcgplayer_product_id_from_url(tcgplayer_url)
    if not resolved_product_id:
        return JSONResponse(
            {"ok": False, "error": "Missing TCGplayer product id", "history": None},
            status_code=400,
        )

    history = await fetch_tcgplayer_public_sales(
        resolved_product_id,
        selected_condition=condition,
        selected_variant=variant,
        selected_language=language,
        product_url=tcgplayer_url,
    )
    errors = history.get("errors") or []
    return JSONResponse(
        {
            "ok": bool(history.get("ok")),
            "history": history,
            "error": "; ".join(str(error) for error in errors if error),
        }
    )


@router.post("/team/buylist/quote")
async def staff_buylist_quote(request: Request, session: Session = Depends(get_session)):
    denial, _user = _require_team_user(request, session)
    if denial:
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON body"}, status_code=400)
    config = get_buylist_config(session)
    lines: list[dict[str, Any]] = []
    totals = {"cash": 0.0, "trade": 0.0, "quantity": 0}
    for raw in body.get("items") or []:
        if not isinstance(raw, dict):
            continue
        quantity = max(1, min(int(_float(raw.get("quantity"), 1)), 999))
        item_type = _normalize_product_type(str(raw.get("item_type") or BUYLIST_PRODUCT_TYPE_CARD))
        if item_type == BUYLIST_PRODUCT_TYPE_SEALED:
            condition = "Sealed"
            language = ""
            variant = "Sealed Product"
        else:
            condition = str(raw.get("condition") or "NM").strip().upper()
            if condition == "DM":
                condition = "DMG"
            if condition not in CONDITION_OPTIONS:
                condition = "NM"
            language = str(raw.get("language") or "English").strip() or "English"
            if language not in LANGUAGE_OPTIONS:
                language = "Other"
            variant = str(raw.get("variant") or "Normal").strip() or "Normal"
        product = {
            "id": raw.get("id") or "",
            "product_id": raw.get("product_id") or raw.get("id") or "",
            "item_type": item_type,
            "name": raw.get("name") or "",
            "set_name": raw.get("set_name") or "",
            "number": raw.get("number") or "",
            "upc": raw.get("upc") or raw.get("number") or "",
            "sealed_product_kind": raw.get("sealed_product_kind") or "",
        }
        base_market_price = _float(raw.get("base_market_price"), _float(raw.get("market_price"), 0.0))
        condition_market_price = _condition_market_price_from_item(raw, condition, variant)
        offer = calculate_buylist_offer(
            config,
            market_price=base_market_price,
            condition_market_price=condition_market_price,
            condition=condition,
            language=language,
            printing=variant,
            product=product,
        )
        line_cash = _money(offer["cash_offer"] * quantity)
        line_trade = _money(offer["trade_offer"] * quantity)
        totals["cash"] += line_cash
        totals["trade"] += line_trade
        totals["quantity"] += quantity
        lines.append(
            {
                **raw,
                "item_type": item_type,
                "quantity": quantity,
                "condition": condition,
                "language": language,
                "variant": variant,
                "market_price": offer["market_price"],
                "base_market_price": offer["base_market_price"],
                "condition_market_price": offer["condition_market_price"],
                "condition_price_source": offer["condition_price_source"],
                "unit_cash": offer["cash_offer"],
                "unit_trade": offer["trade_offer"],
                "line_cash": line_cash,
                "line_trade": line_trade,
                "pricing_notes": offer["notes"],
                "blocked": offer["blocked"],
            }
        )
    return JSONResponse(
        {
            "ok": True,
            "lines": lines,
            "totals": {
                "cash": _money(totals["cash"]),
                "trade": _money(totals["trade"]),
                "quantity": int(totals["quantity"]),
                "items": len(lines),
            },
        }
    )


@router.post("/team/buylist/save", dependencies=[Depends(require_csrf)])
async def staff_buylist_save(request: Request, session: Session = Depends(get_session)):
    denial, user = _require_team_user(request, session)
    if denial:
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON body"}, status_code=400)

    quote_response = await staff_buylist_quote(request, session)
    if getattr(quote_response, "status_code", 200) >= 400:
        return quote_response
    quote_payload = json.loads(quote_response.body.decode("utf-8"))
    if not quote_payload.get("ok"):
        return quote_response
    blocked_lines = [
        line for line in quote_payload.get("lines") or [] if isinstance(line, dict) and line.get("blocked")
    ]
    if blocked_lines:
        return JSONResponse(
            {
                "ok": False,
                "error": "Remove manager-review items before saving this quote.",
            },
            status_code=400,
        )

    details = {
        "customer_name": str(body.get("customer_name") or "").strip()[:200],
        "customer_contact": str(body.get("customer_contact") or "").strip()[:200],
        "payment_view": str(body.get("payment_view") or "").strip()[:20],
        "notes": str(body.get("notes") or "").strip()[:2000],
        "totals": quote_payload.get("totals") or {},
        "lines": quote_payload.get("lines") or [],
    }
    payment_view = details["payment_view"] if details["payment_view"] in {"cash", "trade"} else "cash"
    submission = BuylistSubmission(
        submitted_by_user_id=user.id if user else 0,
        customer_name=details["customer_name"],
        customer_contact=details["customer_contact"],
        payment_view=payment_view,
        status="submitted",
        totals_json=json.dumps(details["totals"], sort_keys=True),
        lines_json=json.dumps(details["lines"], sort_keys=True),
        notes=details["notes"],
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    session.add(submission)
    session.commit()
    session.refresh(submission)
    audit_details = {
        "buylist_submission_id": submission.id,
        "submitted_by_user_id": user.id if user else None,
        "payment_view": payment_view,
        "totals": details["totals"],
        "line_count": len(details["lines"]),
        "has_customer_name": bool(details["customer_name"]),
        "has_customer_contact": bool(details["customer_contact"]),
        "has_notes": bool(details["notes"]),
    }
    session.add(
        AuditLog(
            actor_user_id=user.id if user else None,
            action="staff_buylist.quote_saved",
            resource_key="team.buylist",
            details_json=json.dumps(audit_details, sort_keys=True),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return JSONResponse(
        {
            "ok": True,
            "quote": quote_payload,
            "submission_id": submission.id,
            "message": "Buylist submitted",
        }
    )


@router.get("/team/admin/buylist/submissions", response_class=HTMLResponse)
def admin_buylist_submissions_page(
    request: Request,
    status: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.view")
    if denial:
        return denial
    filter_status = status if status in BUYLIST_SUBMISSION_STATUSES else None
    stmt = select(BuylistSubmission)
    if filter_status:
        stmt = stmt.where(BuylistSubmission.status == filter_status)
    stmt = stmt.order_by(BuylistSubmission.created_at.desc(), BuylistSubmission.id.desc())
    rows = list(session.exec(stmt).all())

    submitter_ids = {row.submitted_by_user_id for row in rows}
    submitters: dict[int, User] = {}
    if submitter_ids:
        submitters = {
            row.id: row
            for row in session.exec(select(User).where(User.id.in_(submitter_ids))).all()
        }
    counts, unknown_status_count = _buylist_submission_status_counts(session)

    return templates.TemplateResponse(
        request,
        "team/admin/buylist_submissions.html",
        {
            "request": request,
            "title": "Buylist queue",
            "active": "buylist-submissions",
            "current_user": user,
            "submissions": [
                _submission_to_view(row, submitters.get(row.submitted_by_user_id))
                for row in rows
            ],
            "filter_status": filter_status,
            "statuses": BUYLIST_SUBMISSION_STATUSES,
            "counts": counts,
            "unknown_status_count": unknown_status_count,
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/buylist/submissions/{submission_id}/approve",
    dependencies=[Depends(require_csrf)],
)
async def admin_buylist_submission_approve(
    request: Request,
    submission_id: int,
    location: str = Form(default="Buylist intake"),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    row = session.get(BuylistSubmission, submission_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Buylist submission not found")
    if row.status != "submitted":
        raise HTTPException(status_code=409, detail=f"Cannot approve {row.status} submission")

    inventory_result = _receive_submission_inventory(
        session,
        row,
        actor=user,
        location=location,
    )
    now = utcnow()
    row.status = "approved"
    row.approved_by_user_id = user.id
    row.approved_at = now
    row.status_changed_at = now
    row.updated_at = now
    row.inventory_result_json = json.dumps(inventory_result, sort_keys=True)
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=user.id,
            action="staff_buylist.approved",
            resource_key=f"team.buylist.{row.id}",
            details_json=json.dumps(
                {"buylist_submission_id": row.id, "inventory_result": inventory_result},
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse(
        "/team/admin/buylist/submissions?flash=Approved+and+received+into+inventory.",
        status_code=303,
    )


@router.post(
    "/team/admin/buylist/submissions/{submission_id}/reject",
    dependencies=[Depends(require_csrf)],
)
async def admin_buylist_submission_reject(
    request: Request,
    submission_id: int,
    decision_notes: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    row = session.get(BuylistSubmission, submission_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Buylist submission not found")
    if row.status != "submitted":
        raise HTTPException(status_code=409, detail=f"Cannot reject {row.status} submission")
    now = utcnow()
    row.status = "rejected"
    row.rejected_by_user_id = user.id
    row.rejected_at = now
    row.status_changed_at = now
    row.updated_at = now
    row.decision_notes = decision_notes.strip()[:2000]
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=user.id,
            action="staff_buylist.rejected",
            resource_key=f"team.buylist.{row.id}",
            details_json=json.dumps(
                {"buylist_submission_id": row.id, "notes": row.decision_notes},
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse(
        "/team/admin/buylist/submissions?flash=Rejected.",
        status_code=303,
    )


@router.post(
    "/team/admin/buylist/submissions/{submission_id}/mark-paid",
    dependencies=[Depends(require_csrf)],
)
async def admin_buylist_submission_mark_paid(
    request: Request,
    submission_id: int,
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.supply.approve")
    if denial:
        return denial
    row = session.get(BuylistSubmission, submission_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Buylist submission not found")
    if row.status != "approved":
        raise HTTPException(status_code=409, detail=f"Cannot mark {row.status} submission paid")
    now = utcnow()
    row.status = "paid"
    row.paid_by_user_id = user.id
    row.paid_at = now
    row.status_changed_at = now
    row.updated_at = now
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=user.id,
            action="staff_buylist.paid",
            resource_key=f"team.buylist.{row.id}",
            details_json=json.dumps({"buylist_submission_id": row.id}, sort_keys=True),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse(
        "/team/admin/buylist/submissions?flash=Marked+paid.",
        status_code=303,
    )


@router.get("/team/admin/buylist", response_class=HTMLResponse)
def staff_buylist_admin_page(
    request: Request,
    saved: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_buylist_admin(request, session)
    if denial:
        return denial
    config = get_buylist_config(session)
    return templates.TemplateResponse(
        request,
        "team/admin/buylist.html",
        {
            "request": request,
            "title": "Buylist Pricing",
            "active": "buylist",
            "current_user": user,
            "config": config,
            "game_options": BUYLIST_GAMES,
            "condition_options": CONDITION_OPTIONS,
            "language_options": LANGUAGE_OPTIONS,
            "cash_ranges": (config.get("cash_ranges") or [])[:8],
            "trade_ranges": (config.get("trade_ranges") or [])[:8],
            "hotlist_text": _rules_to_text(config.get("hotlist_rules") or []),
            "darklist_text": _rules_to_text(config.get("darklist_rules") or []),
            "saved": saved,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/admin/buylist", dependencies=[Depends(require_csrf)])
async def staff_buylist_admin_save(
    request: Request,
    enabled_games: list[str] = Form(default=[]),
    default_game: str = Form(default="Pokemon"),
    default_payment: str = Form(default="cash"),
    condition_pricing_mode: str = Form(default=CONDITION_PRICING_TCGPLAYER),
    checkout_note: str = Form(default=""),
    hotlist_rules: str = Form(default=""),
    darklist_rules: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, _user = _require_buylist_admin(request, session)
    if denial:
        return denial
    form = dict(await request.form())
    enabled = [game for game in enabled_games if game.lower() in _GAME_BY_NAME]
    if not enabled:
        enabled = list(DEFAULT_BUYLIST_CONFIG["enabled_games"])
    if default_game.lower() not in _GAME_BY_NAME or default_game not in enabled:
        default_game = enabled[0]
    default_payment = (default_payment or "cash").strip().lower()
    if default_payment not in {"cash", "trade"}:
        default_payment = "cash"
    condition_pricing_mode = (condition_pricing_mode or CONDITION_PRICING_TCGPLAYER).strip().lower()
    if condition_pricing_mode not in CONDITION_PRICING_MODES:
        condition_pricing_mode = CONDITION_PRICING_TCGPLAYER

    config = get_buylist_config(session)
    config.update(
        {
            "enabled_games": enabled,
            "default_game": default_game,
            "default_payment": default_payment,
            "condition_pricing_mode": condition_pricing_mode,
            "cash_ranges": _parse_ranges(form, "cash"),
            "trade_ranges": _parse_ranges(form, "trade"),
            "condition_modifiers": _parse_modifier_group(
                form,
                "condition",
                DEFAULT_BUYLIST_CONFIG["condition_modifiers"],
            ),
            "language_modifiers": _parse_modifier_group(
                form,
                "language",
                DEFAULT_BUYLIST_CONFIG["language_modifiers"],
            ),
            "printing_modifiers": _parse_modifier_group(
                form,
                "printing",
                DEFAULT_BUYLIST_CONFIG["printing_modifiers"],
            ),
            "hotlist_rules": _parse_list_rules(hotlist_rules),
            "darklist_rules": _parse_list_rules(darklist_rules),
            "checkout_note": checkout_note.strip()[:2000],
        }
    )
    if not config["cash_ranges"]:
        config["cash_ranges"] = deepcopy(DEFAULT_BUYLIST_CONFIG["cash_ranges"])
    if not config["trade_ranges"]:
        config["trade_ranges"] = deepcopy(DEFAULT_BUYLIST_CONFIG["trade_ranges"])
    save_buylist_config(session, config)
    return RedirectResponse("/team/admin/buylist?saved=1", status_code=303)
