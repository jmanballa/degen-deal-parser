"""
TikTok Streamer Dashboard routes.

Extracted from app/main.py -- all routes under /tiktok/streamer/.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import and_, func, or_
from sqlmodel import Session, select

from ..csrf import CSRFProtectedRoute
from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..shared import (  # noqa: F401 - explicit imports for underscore-prefixed names
    _BUILD_VERSION,
    _GMV_CACHE_TTL_SECONDS,
    _get_app_setting,
    _get_live_analytics_snapshot,
    _get_live_session_snapshot,
    _get_live_sessions_list,
    _fetch_live_product_performance_list,
    _gmv_cache,
    _gmv_cache_lock,
    _resolve_tiktok_api_creds,
    _save_stream_range,
    _set_app_setting,
    _stream_range,
    _stream_range_source,
)
from ..db import get_session
from ..models import TikTokAuth, TikTokOrder
from ..reporting import TIKTOK_PAID_STATUSES

router = APIRouter(route_class=CSRFProtectedRoute)

STREAM_CREATOR_CHOICES: tuple[dict[str, str], ...] = (
    {"id": "degencollectibles", "label": "Main", "handle": "degencollectibles"},
    {"id": "degenboss0", "label": "Secondary", "handle": "degenboss0"},
)
DEFAULT_STREAM_CREATOR = "degencollectibles"
CREATOR_ORDER_ATTRIBUTION_TIME_WINDOW = "time_window"
CREATOR_ORDER_ATTRIBUTION_LIVE_PRODUCTS = "live_products"
CREATOR_ORDER_ATTRIBUTION_NO_SESSION = "no_session"
STREAM_METRIC_SOURCE_TIKTOK_LIVE_SESSION = "tiktok_live_session"
STREAM_METRIC_SOURCE_LOCAL_ORDER_ESTIMATE = "local_order_estimate"
_LIVE_PRODUCTS_CACHE_TTL_SECONDS = 10.0
_live_products_cache: dict[str, Any] = {}
_live_products_cache_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers (only used by streamer routes)
# ---------------------------------------------------------------------------

def _stream_session_recency_key(session_data: dict) -> tuple[int, int, int]:
    end_ts = int(session_data.get("end_time") or 0)
    start_ts = int(session_data.get("start_time") or 0)
    is_active = 1 if end_ts == 0 and start_ts > 0 else 0
    return (is_active, end_ts, start_ts)


def _sorted_stream_sessions() -> list[dict]:
    return sorted(_get_live_sessions_list(), key=_stream_session_recency_key, reverse=True)


def _session_username(session_data: Optional[dict]) -> str:
    if not session_data:
        return ""
    return str(session_data.get("username") or "").strip().lstrip("@").lower()


def _stream_session_is_live(session_data: Optional[dict]) -> bool:
    if not session_data:
        return False
    start_ts = int(session_data.get("start_time") or 0)
    end_ts = int(session_data.get("end_time") or 0)
    if start_ts <= 0:
        return False
    if end_ts <= 0:
        return True
    now_ts = datetime.now(timezone.utc).timestamp()
    return (now_ts - end_ts) < 900


def _stream_session_interval(session_data: Optional[dict]) -> Optional[tuple[int, int]]:
    if not session_data:
        return None
    start_ts = int(session_data.get("start_time") or 0)
    if start_ts <= 0:
        return None
    end_ts = int(session_data.get("end_time") or 0)
    if end_ts <= 0:
        end_ts = int(datetime.now(timezone.utc).timestamp())
    if end_ts < start_ts:
        end_ts = start_ts
    return start_ts, end_ts


def _overlapping_creator_handles(session_data: Optional[dict], creator: str) -> list[str]:
    return sorted({_session_username(session) for session in _overlapping_creator_sessions(session_data, creator)})


def _overlapping_creator_sessions(session_data: Optional[dict], creator: str) -> list[dict]:
    selected_interval = _stream_session_interval(session_data)
    if not selected_interval:
        return []
    selected_id = str((session_data or {}).get("id") or "").strip()
    selected_start, selected_end = selected_interval
    overlaps: list[dict] = []
    for other in _sorted_stream_sessions():
        other_username = _session_username(other)
        if other_username == creator or not _normalize_creator(other_username):
            continue
        if selected_id and str(other.get("id") or "").strip() == selected_id:
            continue
        other_interval = _stream_session_interval(other)
        if not other_interval:
            continue
        other_start, other_end = other_interval
        if max(selected_start, other_start) <= min(selected_end, other_end):
            copied = dict(other)
            copied["ok"] = True
            overlaps.append(copied)
    return overlaps


def _normalize_creator(value: Optional[str]) -> str:
    creator = str(value or "").strip().lstrip("@").lower()
    allowed = {choice["id"] for choice in STREAM_CREATOR_CHOICES}
    return creator if creator in allowed else ""


def _creator_label(creator: str) -> str:
    for choice in STREAM_CREATOR_CHOICES:
        if choice["id"] == creator:
            return f"{choice['label']} - @{choice['handle']}"
    return f"@{creator}" if creator else ""


def _creator_options(selected_creator: str) -> list[dict[str, Any]]:
    return [
        {
            "id": choice["id"],
            "label": f"{choice['label']} - @{choice['handle']}",
            "handle": choice["handle"],
            "selected": choice["id"] == selected_creator,
        }
        for choice in STREAM_CREATOR_CHOICES
    ]


def _find_stream_session(stream_id: Optional[str]) -> Optional[dict]:
    wanted = str(stream_id or "").strip()
    if not wanted:
        return None
    for session_data in _get_live_sessions_list():
        if str(session_data.get("id") or "").strip() == wanted:
            selected = dict(session_data)
            selected["ok"] = True
            return selected
    return None


def _creator_from_stream_id(stream_id: Optional[str]) -> str:
    session_data = _find_stream_session(stream_id)
    return _normalize_creator(_session_username(session_data))


def _creator_stream_sessions(creator: str) -> list[dict]:
    sessions = []
    for session_data in _sorted_stream_sessions():
        if _session_username(session_data) == creator:
            copied = dict(session_data)
            copied["ok"] = True
            sessions.append(copied)
    return sessions


def _format_stream_session_label(session_data: dict) -> str:
    title = str(session_data.get("title") or "").strip() or "Untitled live"
    username = str(session_data.get("username") or "").strip()
    start_ts = int(session_data.get("start_time") or 0)
    date_label = ""
    if start_ts > 0:
        start_dt = datetime.fromtimestamp(start_ts, tz=PACIFIC_TZ)
        date_label = f"{start_dt.strftime('%b')} {start_dt.day}"
    bits = [title]
    if username:
        bits.append(f"@{username}")
    if date_label:
        bits.append(date_label)
    return " - ".join(bits)


def _build_stream_context(
    selected_creator: Optional[str] = None,
    legacy_stream_id: Optional[str] = None,
) -> dict[str, Any]:
    creator = _normalize_creator(selected_creator) or _creator_from_stream_id(legacy_stream_id) or DEFAULT_STREAM_CREATOR
    creator_sessions = _creator_stream_sessions(creator)
    live_session = creator_sessions[0] if creator_sessions else None
    snapshot = _get_live_session_snapshot()
    if not live_session and snapshot.get("ok") and _session_username(snapshot) == creator:
        live_session = dict(snapshot)

    start = None
    end = None
    source = "creator"

    if live_session and live_session.get("ok"):
        start_ts = int(live_session.get("start_time") or 0)
        end_ts = int(live_session.get("end_time") or 0)
        if start_ts > 0:
            start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            end = datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts > 0 else None
    overlap_handles = _overlapping_creator_handles(live_session, creator) if live_session else []
    if start is None:
        order_attribution = CREATOR_ORDER_ATTRIBUTION_NO_SESSION
    elif overlap_handles:
        order_attribution = CREATOR_ORDER_ATTRIBUTION_LIVE_PRODUCTS
    else:
        order_attribution = CREATOR_ORDER_ATTRIBUTION_TIME_WINDOW

    return {
        "selected_creator": creator,
        "selected_creator_label": _creator_label(creator),
        "selected_stream_id": str(live_session.get("id") or "") if live_session else "",
        "selected_stream_label": _format_stream_session_label(live_session) if live_session else "",
        "start": start,
        "end": end,
        "source": source,
        "sessions": creator_sessions,
        "live_session": live_session,
        "is_live": _stream_session_is_live(live_session) if live_session else False,
        "creator_filter_enabled": True,
        "creator_order_attribution": order_attribution,
        "creator_order_overlap_handles": overlap_handles,
    }


def _stream_context_cache_key(stream_context: Optional[dict[str, Any]]) -> tuple:
    stream_context = stream_context or {}
    start = stream_context.get("start")
    end = stream_context.get("end")
    return (
        stream_context.get("selected_creator"),
        stream_context.get("selected_stream_id"),
        start.isoformat() if isinstance(start, datetime) else "",
        end.isoformat() if isinstance(end, datetime) else "",
        stream_context.get("source") or "",
        stream_context.get("creator_order_attribution") or "",
    )


def _clean_account_identifier(value: Any) -> str:
    return str(value or "").strip()


def _clean_creator_match_text(value: Any) -> str:
    return "".join(ch for ch in str(value or "").strip().lstrip("@").lower() if ch.isalnum())


def _new_account_scope(selected_creator: str = "") -> dict[str, Any]:
    return {
        "shop_ids": set(),
        "shop_ciphers": set(),
        "seller_ids": set(),
        "include_unattributed": selected_creator == DEFAULT_STREAM_CREATOR,
        "source": "",
    }


def _add_account_scope_value(scope: dict[str, Any], key: str, value: Any) -> None:
    cleaned = _clean_account_identifier(value)
    if not cleaned:
        return
    if key == "shop_ids" and cleaned.startswith("pending:"):
        return
    scope.setdefault(key, set()).add(cleaned)


def _account_scope_has_identity(scope: Optional[dict[str, Any]]) -> bool:
    scope = scope or {}
    return bool(scope.get("shop_ids") or scope.get("shop_ciphers") or scope.get("seller_ids"))


def _account_scope_from_live_session(
    live_session: Optional[dict[str, Any]],
    selected_creator: str = "",
) -> dict[str, Any]:
    scope = _new_account_scope(selected_creator)
    if not live_session:
        return scope
    for key in ("shop_id", "shopId", "tiktok_shop_id", "tiktokShopId"):
        _add_account_scope_value(scope, "shop_ids", live_session.get(key))
    for key in ("shop_cipher", "shopCipher"):
        _add_account_scope_value(scope, "shop_ciphers", live_session.get(key))
    for key in ("seller_id", "sellerId"):
        _add_account_scope_value(scope, "seller_ids", live_session.get(key))
    if _account_scope_has_identity(scope):
        scope["source"] = "live_session"
    return scope


def _auth_row_matches_creator(auth_row: TikTokAuth, selected_creator: str) -> bool:
    creator_key = _clean_creator_match_text(selected_creator)
    if not creator_key:
        return False
    for value in (
        auth_row.shop_name,
        auth_row.seller_name,
        auth_row.open_id,
        auth_row.tiktok_shop_id,
        auth_row.shop_cipher,
        auth_row.raw_payload,
    ):
        text_key = _clean_creator_match_text(value)
        if creator_key and creator_key in text_key:
            return True
    return False


def _add_auth_row_to_account_scope(scope: dict[str, Any], auth_row: TikTokAuth) -> None:
    _add_account_scope_value(scope, "shop_ids", auth_row.tiktok_shop_id)
    _add_account_scope_value(scope, "shop_ciphers", auth_row.shop_cipher)
    _add_account_scope_value(scope, "seller_ids", auth_row.seller_id)


def _stream_account_scope_for_context(
    session: Optional[Session],
    stream_context: Optional[dict[str, Any]],
) -> dict[str, Any]:
    stream_context = stream_context or {}
    selected_creator = stream_context.get("selected_creator") or DEFAULT_STREAM_CREATOR
    scope = _account_scope_from_live_session(stream_context.get("live_session"), selected_creator)
    if _account_scope_has_identity(scope) or session is None:
        return scope

    configured_shop_id = (settings.tiktok_shop_id or "").strip()
    configured_shop_cipher = (settings.tiktok_shop_cipher or "").strip()
    if selected_creator == DEFAULT_STREAM_CREATOR:
        _add_account_scope_value(scope, "shop_ids", configured_shop_id)
        _add_account_scope_value(scope, "shop_ciphers", configured_shop_cipher)

    auth_rows = list(
        session.exec(select(TikTokAuth).order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())).all()
    )
    if selected_creator == DEFAULT_STREAM_CREATOR:
        if not _account_scope_has_identity(scope) and len(auth_rows) == 1:
            _add_auth_row_to_account_scope(scope, auth_rows[0])
            scope["source"] = "single_auth"
        elif _account_scope_has_identity(scope):
            scope["source"] = "configured_main"
        return scope

    matched = [row for row in auth_rows if _auth_row_matches_creator(row, selected_creator)]
    if not matched:
        non_main_rows = [
            row
            for row in auth_rows
            if not (
                (configured_shop_id and row.tiktok_shop_id == configured_shop_id)
                or (configured_shop_cipher and row.shop_cipher == configured_shop_cipher)
            )
        ]
        if len(non_main_rows) == 1:
            matched = non_main_rows
    for row in matched:
        _add_auth_row_to_account_scope(scope, row)
    if matched and _account_scope_has_identity(scope):
        scope["source"] = "auth_row"
    return scope


def _apply_tiktok_account_scope(stmt, account_scope: Optional[dict[str, Any]]):
    if not _account_scope_has_identity(account_scope):
        return stmt
    account_scope = account_scope or {}
    predicates = []
    shop_ids = sorted(account_scope.get("shop_ids") or [])
    shop_ciphers = sorted(account_scope.get("shop_ciphers") or [])
    seller_ids = sorted(account_scope.get("seller_ids") or [])
    if shop_ids:
        predicates.append(TikTokOrder.shop_id.in_(shop_ids))
    if shop_ciphers:
        predicates.append(TikTokOrder.shop_cipher.in_(shop_ciphers))
    if seller_ids:
        predicates.append(TikTokOrder.seller_id.in_(seller_ids))
    if account_scope.get("include_unattributed"):
        predicates.append(
            and_(
                TikTokOrder.shop_id == None,
                TikTokOrder.shop_cipher == None,
                TikTokOrder.seller_id == None,
            )
        )
    if not predicates:
        return stmt
    return stmt.where(or_(*predicates))


def _creator_order_rows_are_precise(stream_context: Optional[dict[str, Any]]) -> bool:
    stream_context = stream_context or {}
    if not stream_context.get("creator_filter_enabled"):
        return True
    return stream_context.get("creator_order_attribution") in {
        CREATOR_ORDER_ATTRIBUTION_TIME_WINDOW,
        CREATOR_ORDER_ATTRIBUTION_LIVE_PRODUCTS,
    }


def _creator_order_attribution_message(stream_context: Optional[dict[str, Any]]) -> str:
    stream_context = stream_context or {}
    attribution = stream_context.get("creator_order_attribution")
    if attribution == CREATOR_ORDER_ATTRIBUTION_LIVE_PRODUCTS:
        handles = [f"@{h}" for h in stream_context.get("creator_order_overlap_handles") or []]
        suffix = f" ({', '.join(handles)})" if handles else ""
        return f"Filtering by TikTok live product attribution{suffix}."
    if attribution == CREATOR_ORDER_ATTRIBUTION_NO_SESSION:
        return "No live session was found for this creator."
    return ""


def _parse_order_line_items(order: TikTokOrder) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for field_name in ("line_items_json", "line_items_summary_json"):
        raw_text = getattr(order, field_name, "") or ""
        try:
            raw_items = json.loads(raw_text) if raw_text else []
        except (json.JSONDecodeError, TypeError):
            raw_items = []
        if isinstance(raw_items, dict):
            raw_items = [raw_items]
        if isinstance(raw_items, list):
            items.extend([item for item in raw_items if isinstance(item, dict)])
    return items


def _order_product_ids(order: TikTokOrder) -> set[str]:
    product_ids: set[str] = set()
    for item in _parse_order_line_items(order):
        product_id = str(
            item.get("product_id") or item.get("item_id") or item.get("id") or ""
        ).strip()
        if product_id:
            product_ids.add(product_id)
    return product_ids


def _safe_metric_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_metric_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _session_metric_float(session_data: Optional[dict], key: str, default: float = 0.0) -> float:
    return _safe_metric_float((session_data or {}).get(key), default)


def _session_metric_int(session_data: Optional[dict], key: str, default: int = 0) -> int:
    return _safe_metric_int((session_data or {}).get(key), default)


def _session_avg_price(session_data: Optional[dict]) -> float:
    avg_price = _session_metric_float(session_data, "avg_price")
    if avg_price > 0:
        return avg_price
    gmv = _session_metric_float(session_data, "gmv")
    items = _session_metric_int(session_data, "items_sold") or _session_metric_int(session_data, "sku_orders")
    return gmv / items if gmv > 0 and items > 0 else 0.0


def _session_datetime_window(session_data: Optional[dict]) -> tuple[Optional[datetime], Optional[datetime]]:
    interval = _stream_session_interval(session_data)
    if not interval:
        return None, None
    start_ts, end_ts = interval
    return datetime.fromtimestamp(start_ts, tz=timezone.utc), datetime.fromtimestamp(end_ts, tz=timezone.utc)


def _line_item_product_id(item: dict[str, Any]) -> str:
    return str(item.get("product_id") or item.get("item_id") or item.get("id") or "").strip()


def _line_item_title(item: dict[str, Any]) -> str:
    return str(
        item.get("product_name") or item.get("title") or item.get("item_name") or item.get("sku_name") or ""
    ).strip() or "Unknown item"


def _line_item_quantity(item: dict[str, Any]) -> int:
    return max(_safe_metric_int(item.get("quantity") or item.get("sku_quantity") or item.get("qty"), 1), 1)


def _line_item_unit_price(item: dict[str, Any]) -> float:
    for key in ("sale_price", "sku_sale_price", "price", "unit_price", "original_price"):
        if item.get(key) not in (None, ""):
            value = _safe_metric_float(item.get(key), 0.0)
            if value > 0:
                return value
    return 0.0


def _line_item_image(item: dict[str, Any]) -> Optional[str]:
    return str(item.get("sku_image") or item.get("product_image") or item.get("image_url") or "").strip() or None


def _aggregate_product_stats(orders: list[TikTokOrder]) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for order in orders:
        order_items = _parse_order_line_items(order)
        if not order_items:
            continue
        seen_in_order: set[str] = set()
        for item in order_items:
            product_id = _line_item_product_id(item)
            if not product_id:
                continue
            quantity = _line_item_quantity(item)
            unit_price = _line_item_unit_price(item)
            revenue = round(unit_price * quantity, 2)
            row = stats.setdefault(
                product_id,
                {
                    "id": product_id,
                    "name": _line_item_title(item),
                    "qty": 0,
                    "orders": 0,
                    "direct_gmv": 0.0,
                    "sku_image": _line_item_image(item),
                },
            )
            if product_id not in seen_in_order:
                row["orders"] += 1
                seen_in_order.add(product_id)
            row["qty"] += quantity
            row["direct_gmv"] = round(row["direct_gmv"] + revenue, 2)
            if not row.get("sku_image"):
                row["sku_image"] = _line_item_image(item)
    for row in stats.values():
        orders_count = max(int(row.get("orders") or 0), 1)
        row["avg_order_value"] = round(float(row.get("direct_gmv") or 0) / orders_count, 2)
    return sorted(
        stats.values(),
        key=lambda row: (int(row.get("orders") or 0), float(row.get("direct_gmv") or 0)),
        reverse=True,
    )


def _orders_for_session_window(db_session: Session, session_data: Optional[dict]) -> list[TikTokOrder]:
    start_dt, end_dt = _session_datetime_window(session_data)
    if start_dt is None:
        return []
    query = select(TikTokOrder).where(TikTokOrder.created_at >= start_dt)
    if end_dt is not None:
        query = query.where(TikTokOrder.created_at <= end_dt)
    account_scope = _stream_account_scope_for_context(
        db_session,
        {
            "selected_creator": _session_username(session_data),
            "live_session": session_data or {},
        },
    )
    query = _apply_tiktok_account_scope(query, account_scope)
    rows = db_session.exec(query.order_by(TikTokOrder.created_at.desc())).all()
    return [order for order in rows if _is_enriched_order(order)]


def _infer_product_ids_for_creator_session(
    db_session: Session,
    session_data: Optional[dict],
    *,
    limit_cap: int = 20,
) -> tuple[set[str], list[dict[str, Any]]]:
    orders = _orders_for_session_window(db_session, session_data)
    product_stats = _aggregate_product_stats(orders)
    if not product_stats:
        return set(), []

    avg_price = _session_avg_price(session_data)
    price_cap = avg_price * 4 if avg_price > 0 else None
    frequent_cutoff = max(3, int((_session_metric_int(session_data, "items_sold") or 0) * 0.02))
    candidates: list[dict[str, Any]] = []
    for product in product_stats:
        avg_order_value = _safe_metric_float(product.get("avg_order_value"))
        order_count = _safe_metric_int(product.get("orders"))
        if price_cap is None or avg_order_value <= price_cap or order_count >= frequent_cutoff:
            candidates.append(product)
    if not candidates:
        candidates = product_stats

    def _candidate_sort_key(product: dict[str, Any]) -> tuple[int, int, float]:
        avg_order_value = _safe_metric_float(product.get("avg_order_value"))
        order_count = _safe_metric_int(product.get("orders"))
        direct_gmv = _safe_metric_float(product.get("direct_gmv"))
        price_match = price_cap is None or avg_order_value <= price_cap
        return (1 if price_match else 0, order_count, direct_gmv)

    candidates = sorted(candidates, key=_candidate_sort_key, reverse=True)

    limit = max(
        _session_metric_int(session_data, "products_added"),
        _session_metric_int(session_data, "different_products_sold"),
    )
    if limit <= 0:
        limit = 8
    limit = max(1, min(limit, limit_cap))
    selected = candidates[:limit]
    return {str(product.get("id") or "").strip() for product in selected if product.get("id")}, selected


def _infer_live_product_scope(db_session: Session, stream_context: Optional[dict[str, Any]]) -> dict[str, Any]:
    stream_context = stream_context or {}
    selected_creator = stream_context.get("selected_creator") or ""
    live_session = stream_context.get("live_session") or {}
    if not live_session:
        return {"available": False, "products": [], "product_ids": set(), "exclude_product_ids": set(), "source": ""}

    if selected_creator == DEFAULT_STREAM_CREATOR:
        selected_ids, selected_products = _infer_product_ids_for_creator_session(
            db_session,
            live_session,
            limit_cap=100,
        )
        excluded_ids: set[str] = set()
        excluded_products: list[dict[str, Any]] = []
        for other_session in _overlapping_creator_sessions(live_session, selected_creator):
            product_ids, products = _infer_product_ids_for_creator_session(db_session, other_session)
            excluded_ids.update(product_ids)
            excluded_products.extend(products)
        if excluded_ids:
            return {
                "available": True,
                "products": excluded_products,
                "product_ids": set(),
                "selected_product_ids": selected_ids,
                "selected_products": selected_products,
                "exclude_product_ids": excluded_ids,
                "source": "local_overlap_exclusion",
            }
        return {
            "available": bool(selected_ids),
            "products": selected_products,
            "product_ids": set(),
            "selected_product_ids": selected_ids,
            "selected_products": selected_products,
            "exclude_product_ids": set(),
            "source": "local_main_inference" if selected_ids else "",
        }

    product_ids, products = _infer_product_ids_for_creator_session(db_session, live_session)
    return {
        "available": bool(product_ids),
        "products": products,
        "product_ids": product_ids,
        "selected_product_ids": product_ids,
        "selected_products": products,
        "exclude_product_ids": set(),
        "source": "local_product_inference" if product_ids else "",
    }


def _fetch_live_product_scope(stream_context: Optional[dict[str, Any]]) -> dict[str, Any]:
    stream_context = stream_context or {}
    live_id = str(stream_context.get("selected_stream_id") or "").strip()
    if not live_id:
        return {"available": False, "products": [], "product_ids": set(), "exclude_product_ids": set(), "source": ""}

    now = time.monotonic()
    with _live_products_cache_lock:
        cached = _live_products_cache.get(live_id)
        if cached and now - cached.get("at", 0) < _LIVE_PRODUCTS_CACHE_TTL_SECONDS:
            return cached["data"]

    scope: dict[str, Any] = {"available": False, "products": [], "product_ids": set(), "exclude_product_ids": set(), "source": ""}
    if _fetch_live_product_performance_list is None:
        return scope

    access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
    app_secret = (settings.tiktok_app_secret or "").strip()
    if not access_token or not shop_cipher or not app_key or not app_secret:
        return scope

    try:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            products = _fetch_live_product_performance_list(
                client,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=app_key,
                app_secret=app_secret,
                access_token=access_token,
                shop_cipher=shop_cipher,
                live_id=live_id,
                currency="USD",
            )
    except Exception:
        products = []

    product_ids = {str(p.get("id") or "").strip() for p in products if str(p.get("id") or "").strip()}
    scope = {
        "available": bool(products),
        "products": products,
        "product_ids": product_ids,
        "selected_product_ids": product_ids,
        "selected_products": products,
        "exclude_product_ids": set(),
        "source": "tiktok_live_products" if products else "",
    }
    with _live_products_cache_lock:
        _live_products_cache[live_id] = {"at": time.monotonic(), "data": scope}
    return scope


def _should_use_live_product_scope(stream_context: Optional[dict[str, Any]]) -> bool:
    stream_context = stream_context or {}
    return bool(
        stream_context.get("creator_filter_enabled")
        and stream_context.get("selected_stream_id")
        and stream_context.get("creator_order_attribution") == CREATOR_ORDER_ATTRIBUTION_LIVE_PRODUCTS
    )


def _filter_orders_to_live_products(
    orders: list[TikTokOrder],
    stream_context: Optional[dict[str, Any]],
    product_scope: Optional[dict[str, Any]] = None,
    db_session: Optional[Session] = None,
) -> tuple[list[TikTokOrder], dict[str, Any]]:
    if not _should_use_live_product_scope(stream_context):
        return orders, product_scope or {
            "available": False,
            "products": [],
            "product_ids": set(),
            "exclude_product_ids": set(),
            "source": "",
        }
    scope = product_scope or _fetch_live_product_scope(stream_context)
    if not scope.get("product_ids") and not scope.get("exclude_product_ids") and db_session is not None:
        scope = _infer_live_product_scope(db_session, stream_context)
    selected_creator = (stream_context or {}).get("selected_creator") or ""
    if selected_creator == DEFAULT_STREAM_CREATOR:
        if scope.get("product_ids") and not scope.get("selected_product_ids"):
            scope = {
                **scope,
                "selected_product_ids": scope.get("product_ids") or set(),
                "selected_products": scope.get("products") or [],
            }
        explicit_live_product_ids = set(scope.get("product_ids") or set())
        if db_session is not None and (
            not scope.get("exclude_product_ids") or not scope.get("selected_product_ids")
        ):
            inferred_scope = _infer_live_product_scope(db_session, stream_context)
            scope = {
                **inferred_scope,
                **scope,
                "exclude_product_ids": scope.get("exclude_product_ids") or inferred_scope.get("exclude_product_ids") or set(),
                "selected_product_ids": scope.get("selected_product_ids") or inferred_scope.get("selected_product_ids") or set(),
                "selected_products": scope.get("selected_products") or inferred_scope.get("selected_products") or [],
            }
        exclude_product_ids = set(scope.get("exclude_product_ids") or set())
        if explicit_live_product_ids:
            exclude_product_ids -= explicit_live_product_ids
            scope = {**scope, "exclude_product_ids": exclude_product_ids}
            return [
                order
                for order in orders
                if (not _order_product_ids(order)) or (_order_product_ids(order) & explicit_live_product_ids)
            ], scope
        if exclude_product_ids:
            return [order for order in orders if not (_order_product_ids(order) & exclude_product_ids)], scope
        return orders, scope
    product_ids = scope.get("product_ids") or set()
    exclude_product_ids = scope.get("exclude_product_ids") or set()
    if exclude_product_ids:
        return [order for order in orders if not (_order_product_ids(order) & exclude_product_ids)], scope
    if not product_ids:
        return orders, scope
    return [order for order in orders if _order_product_ids(order) & product_ids], scope


def _live_product_top_sellers(product_scope: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    products = (product_scope or {}).get("products") or []
    sellers: list[dict[str, Any]] = []
    for product in products:
        title = str(product.get("name") or "").strip() or "Unknown item"
        sellers.append({
            "title": title,
            "variant": None,
            "sku_image": None,
            "qty": int(product.get("items_sold") or product.get("sku_orders") or 0),
            "revenue": round(float(product.get("direct_gmv") or 0), 2),
            "buyers": [],
        })
    return sorted(sellers, key=lambda row: row["revenue"], reverse=True)[:8]


def _apply_stream_order_scope(
    stmt,
    stream_context: Optional[dict[str, Any]],
    fallback_start: Optional[datetime] = None,
    account_scope: Optional[dict[str, Any]] = None,
):
    stream_context = stream_context or {}
    start = stream_context.get("start") or fallback_start
    end = stream_context.get("end")
    if stream_context.get("creator_filter_enabled") and stream_context.get("start") is None:
        return stmt.where(TikTokOrder.id == -1)
    if start is not None:
        stmt = stmt.where(TikTokOrder.created_at >= start)
    if end is not None:
        stmt = stmt.where(TikTokOrder.created_at <= end)
    return _apply_tiktok_account_scope(stmt, account_scope)


def _scoped_tiktok_order_count(
    session: Session,
    stream_context: Optional[dict[str, Any]],
    fallback_start: Optional[datetime],
) -> int:
    account_scope = _stream_account_scope_for_context(session, stream_context)
    count_query = _apply_stream_order_scope(
        select(func.count()).select_from(TikTokOrder),
        stream_context,
        fallback_start=fallback_start,
        account_scope=account_scope,
    )
    return int(session.exec(count_query).one())


def _load_scoped_stream_orders(
    session: Session,
    stream_context: Optional[dict[str, Any]],
    fallback_start: Optional[datetime],
    *,
    since_updated_at: Optional[datetime] = None,
    order_by_updated: bool = False,
) -> tuple[list[TikTokOrder], dict[str, Any]]:
    account_scope = _stream_account_scope_for_context(session, stream_context)
    query = _apply_stream_order_scope(
        select(TikTokOrder),
        stream_context,
        fallback_start=fallback_start,
        account_scope=account_scope,
    )
    if since_updated_at is not None:
        query = query.where(TikTokOrder.updated_at > since_updated_at)
    if order_by_updated:
        query = query.order_by(TikTokOrder.updated_at.desc())
    else:
        query = query.order_by(TikTokOrder.created_at.desc())
    orders = session.exec(query).all()
    orders = [order for order in orders if _is_enriched_order(order)]
    return _filter_orders_to_live_products(orders, stream_context, db_session=session)


def _latest_updated_at_for_orders(orders: list[TikTokOrder]) -> Optional[datetime]:
    latest = None
    for order in orders:
        updated_at = order.updated_at
        if updated_at is None:
            continue
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        if latest is None or updated_at > latest:
            latest = updated_at
    return latest


def _live_room_id_for_stream(chat_info: dict, stream_context: Optional[dict[str, Any]]) -> Optional[str]:
    room_id = str((chat_info or {}).get("room_id") or "").strip()
    if not room_id:
        return None
    live_session = (stream_context or {}).get("live_session") or {}
    session_username = str(live_session.get("username") or "").strip().lstrip("@").lower()
    chat_username = str(settings.tiktok_live_username or "").strip().lstrip("@").lower()
    if session_username and chat_username and session_username != chat_username:
        return None
    return room_id


def _apply_tiktok_session_metrics(gmv_data: dict[str, Any], stream_context: Optional[dict[str, Any]]) -> dict[str, Any]:
    result = dict(gmv_data)
    selected_creator = (stream_context or {}).get("selected_creator") or ""
    if selected_creator == DEFAULT_STREAM_CREATOR:
        return result
    live_session = (stream_context or {}).get("live_session") or {}
    if not live_session.get("ok"):
        return result
    tiktok_gmv = round(_session_metric_float(live_session, "gmv"), 2)
    tiktok_orders = _session_metric_int(live_session, "sku_orders")
    tiktok_items = _session_metric_int(live_session, "items_sold")
    has_tiktok_totals = tiktok_gmv > 0 or tiktok_orders > 0 or tiktok_items > 0
    has_local_totals = (
        _safe_metric_float(result.get("stream_gmv")) > 0
        or _safe_metric_int(result.get("stream_orders")) > 0
        or _safe_metric_int(result.get("stream_items")) > 0
    )
    if not has_tiktok_totals and has_local_totals:
        result["stream_metric_source"] = STREAM_METRIC_SOURCE_LOCAL_ORDER_ESTIMATE
        result["stream_metric_label"] = "local order estimate"
        result["stream_metric_note"] = "TikTok attribution delayed"
        return result

    result["stream_gmv"] = tiktok_gmv
    for target_key, session_key, value in (
        ("stream_orders", "sku_orders", tiktok_orders),
        ("stream_items", "items_sold", tiktok_items),
    ):
        if session_key in live_session:
            result[target_key] = value
    result["stream_metric_source"] = STREAM_METRIC_SOURCE_TIKTOK_LIVE_SESSION
    result["stream_metric_label"] = "TikTok live attribution"
    return result

def _is_enriched_order(o: TikTokOrder) -> bool:
    """Return True if the order has been enriched with full API data.

    Webhook payloads arrive with only an order ID; the enrichment step
    fills in prices and line items.  Before enrichment completes the row
    has subtotal_price/total_price = None and line_items_json = '[]'.
    Legitimate $0 orders (giveaways / ZERO_LOTTERY) will have real
    line-item detail (700+ chars).
    """
    if o.subtotal_price or o.total_price:
        return True
    li = o.line_items_json or ""
    return len(li) > 4

def _build_streamer_order_card(order: TikTokOrder) -> dict:
    """Build a card-ready dict from a TikTokOrder, extracting sku_image from raw line items."""
    raw_items: list[dict] = []
    try:
        raw_items = json.loads(order.line_items_json) if order.line_items_json else []
    except (json.JSONDecodeError, TypeError):
        pass
    if not isinstance(raw_items, list):
        raw_items = [raw_items] if isinstance(raw_items, dict) else []

    summary_items: list[dict] = []
    try:
        summary_items = json.loads(order.line_items_summary_json) if order.line_items_summary_json else []
    except (json.JSONDecodeError, TypeError):
        pass
    if not isinstance(summary_items, list):
        summary_items = []

    items: list[dict] = []
    for idx, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            continue
        title = str(
            raw.get("product_name") or raw.get("sku_name") or raw.get("title") or raw.get("item_name") or ""
        ).strip()
        if not title and idx < len(summary_items):
            title = str(summary_items[idx].get("title") or "").strip()
        if not title:
            title = "Unknown item"

        sku_name = str(raw.get("sku_name") or "").strip()
        variant = sku_name if sku_name and sku_name.lower() != "default" and sku_name.lower() != title.lower() else None

        qty_raw = raw.get("quantity") or raw.get("sku_quantity") or raw.get("count")
        try:
            quantity = int(qty_raw or 0)
        except (TypeError, ValueError):
            quantity = 0
        if quantity < 1 and idx < len(summary_items):
            try:
                quantity = int(summary_items[idx].get("quantity") or 1)
            except (TypeError, ValueError):
                quantity = 1
        if quantity < 1:
            quantity = 1

        sku_image = str(
            raw.get("sku_image") or raw.get("product_image") or raw.get("image_url") or ""
        ).strip() or None
        if not sku_image and idx < len(summary_items):
            sku_image = str(summary_items[idx].get("sku_image") or "").strip() or None

        unit_price: float = 0.0
        for price_key in ("sale_price", "sku_sale_price", "price", "unit_price"):
            val = raw.get(price_key)
            if val is not None:
                try:
                    unit_price = float(val)
                except (TypeError, ValueError):
                    pass
                else:
                    break

        items.append({
            "title": title,
            "variant": variant,
            "quantity": quantity,
            "sku_image": sku_image,
            "unit_price": unit_price,
        })

    if not items and summary_items:
        for si in summary_items:
            if not isinstance(si, dict):
                continue
            items.append({
                "title": str(si.get("title") or "Unknown item"),
                "variant": None,
                "quantity": int(si.get("quantity") or 1),
                "sku_image": str(si.get("sku_image") or "").strip() or None,
                "unit_price": float(si.get("unit_price") or 0),
            })

    created_at_val = order.created_at
    if created_at_val is not None and hasattr(created_at_val, "tzinfo") and created_at_val.tzinfo is None:
        created_at_val = created_at_val.replace(tzinfo=timezone.utc)
    updated_at_val = order.updated_at
    if updated_at_val is not None and hasattr(updated_at_val, "tzinfo") and updated_at_val.tzinfo is None:
        updated_at_val = updated_at_val.replace(tzinfo=timezone.utc)

    return {
        "tiktok_order_id": order.tiktok_order_id,
        "order_number": order.order_number or "",
        "customer_name": order.customer_name or "Guest",
        "created_at": created_at_val.isoformat() if created_at_val else "",
        "updated_at": updated_at_val.isoformat() if updated_at_val else "",
        "total_price": float(order.total_price or 0),
        "order_status": (order.order_status or "").strip().lower(),
        "financial_status": (order.financial_status or "").strip().lower(),
        "items": items,
    }

_buyer_lifetime_cache: dict = {}
_buyer_lifetime_cache_lock = threading.Lock()
_BUYER_LIFETIME_CACHE_TTL = 120  # seconds

def _compute_buyer_lifetime_totals(session: Session) -> dict[str, float]:
    """Return a dict mapping lowercased buyer name -> lifetime GMV across all paid orders.

    Uses SQL GROUP BY and caches for 120s to avoid full-table scans on every poll.
    """
    now = time.monotonic()
    with _buyer_lifetime_cache_lock:
        if now - _buyer_lifetime_cache.get("at", 0) < _BUYER_LIFETIME_CACHE_TTL and "data" in _buyer_lifetime_cache:
            return _buyer_lifetime_cache["data"]

    status_col = func.coalesce(
        func.lower(func.trim(TikTokOrder.financial_status)),
        func.lower(func.trim(TikTokOrder.order_status)),
        "",
    )
    buyer_name_expr = func.lower(func.trim(func.coalesce(TikTokOrder.customer_name, "guest")))
    rows = session.exec(
        select(
            buyer_name_expr,
            func.sum(func.coalesce(TikTokOrder.subtotal_price, TikTokOrder.total_price, 0.0)),
        )
        .where(status_col.in_(list(TIKTOK_PAID_STATUSES)))
        .group_by(buyer_name_expr)
    ).all()
    agg = {(name or "guest"): float(total or 0) for name, total in rows}

    with _buyer_lifetime_cache_lock:
        _buyer_lifetime_cache["data"] = agg
        _buyer_lifetime_cache["at"] = time.monotonic()
    return agg

def _enrich_cards_with_buyer_totals(cards: list[dict], buyer_totals: dict[str, float]) -> None:
    """Attach buyer_lifetime_spent to each card in-place."""
    for card in cards:
        buyer_key = (card.get("customer_name") or "").strip().lower() or "guest"
        card["buyer_lifetime_spent"] = round(buyer_totals.get(buyer_key, 0.0), 2)

def _streamer_session_gmv(session: Session, stream_context: Optional[dict[str, Any]] = None) -> dict:
    """Cached wrapper -- returns GMV data, recomputing at most once per _GMV_CACHE_TTL_SECONDS."""
    cache_key = _stream_context_cache_key(stream_context)
    now = time.monotonic()
    with _gmv_cache_lock:
        cached_at = _gmv_cache.get("at", 0)
        if (
            now - cached_at < _GMV_CACHE_TTL_SECONDS
            and _gmv_cache.get("key") == cache_key
            and "data" in _gmv_cache
        ):
            return _gmv_cache["data"]
    result = _streamer_session_gmv_uncached(session, stream_context=stream_context)
    with _gmv_cache_lock:
        _gmv_cache["data"] = result
        _gmv_cache["at"] = time.monotonic()
        _gmv_cache["key"] = cache_key
    return result

def _streamer_session_gmv_uncached(session: Session, stream_context: Optional[dict[str, Any]] = None) -> dict:
    """Calculate today's GMV and top sellers for the streamer dashboard (Pacific time)."""
    stream_context = stream_context or {}
    account_scope = (
        _stream_account_scope_for_context(session, stream_context)
        if stream_context.get("creator_filter_enabled")
        else {}
    )
    now_pacific = datetime.now(PACIFIC_TZ)
    today_start_pacific = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_start_pacific.astimezone(timezone.utc)

    today_query = select(TikTokOrder).where(TikTokOrder.created_at >= today_start_utc)
    today_query = _apply_tiktok_account_scope(today_query, account_scope)
    today_orders = session.exec(today_query).all()

    gmv = 0.0
    paid_count = 0
    product_agg: dict[str, dict] = {}
    customer_agg: dict[str, dict] = {}

    for o in today_orders:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status not in TIKTOK_PAID_STATUSES:
            continue
        order_gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        gmv += order_gmv
        paid_count += 1

        buyer_name = (o.customer_name or "").strip() or "Guest"
        buyer_key = buyer_name.lower()
        if buyer_key in customer_agg:
            customer_agg[buyer_key]["spent"] += order_gmv
            customer_agg[buyer_key]["orders"] += 1
        else:
            customer_agg[buyer_key] = {"name": buyer_name, "spent": order_gmv, "orders": 1, "order_list": []}

        raw_items: list[dict] = []
        try:
            raw_items = json.loads(o.line_items_json) if o.line_items_json else []
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(raw_items, list):
            raw_items = [raw_items] if isinstance(raw_items, dict) else []

        order_items_merged: dict[str, dict] = {}
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title = str(
                raw.get("product_name") or raw.get("sku_name") or raw.get("title") or raw.get("item_name") or ""
            ).strip()
            if not title:
                title = "Unknown item"

            sku_name = str(raw.get("sku_name") or "").strip()
            variant = sku_name if sku_name and sku_name.lower() != "default" and sku_name.lower() != title.lower() else None
            key = (title.lower() + "||" + (variant or "").lower()).strip()

            qty_raw = raw.get("quantity") or raw.get("sku_quantity") or raw.get("count")
            try:
                qty = max(int(qty_raw or 0), 1)
            except (TypeError, ValueError):
                qty = 1

            unit_price = 0.0
            for pk in ("sale_price", "sku_sale_price", "price", "unit_price"):
                val = raw.get(pk)
                if val is not None:
                    try:
                        unit_price = float(val)
                    except (TypeError, ValueError):
                        continue
                    break

            sku_image = str(
                raw.get("sku_image") or raw.get("product_image") or raw.get("image_url") or ""
            ).strip() or None

            if key in order_items_merged:
                order_items_merged[key]["qty"] += qty
            else:
                order_items_merged[key] = {"title": title, "qty": qty, "sku_image": sku_image}

            if key in product_agg:
                product_agg[key]["qty"] += qty
                product_agg[key]["revenue"] += round(qty * unit_price, 2)
                if not product_agg[key]["sku_image"] and sku_image:
                    product_agg[key]["sku_image"] = sku_image
            else:
                product_agg[key] = {
                    "title": title,
                    "variant": variant,
                    "sku_image": sku_image,
                    "qty": qty,
                    "revenue": round(qty * unit_price, 2),
                    "buyers": {},
                }
            if buyer_name not in product_agg[key].get("buyers", {}):
                product_agg[key].setdefault("buyers", {})[buyer_name] = 0
            product_agg[key]["buyers"][buyer_name] += qty

        if len(customer_agg[buyer_key]["order_list"]) < 20:
            customer_agg[buyer_key]["order_list"].append({
                "order_number": o.order_number,
                "total": round(order_gmv, 2),
                "items": list(order_items_merged.values()),
                "created_at": o.created_at.isoformat() if o.created_at else None,
            })

    top_sellers = sorted(product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:8]
    for ts in top_sellers:
        ts["revenue"] = round(ts["revenue"], 2)
        bd = ts.pop("buyers", {})
        ts["buyers"] = sorted(
            [{"name": n, "qty": q} for n, q in bd.items()],
            key=lambda x: x["qty"], reverse=True,
        )

    top_buyers = sorted(customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
    for tb in top_buyers:
        tb["spent"] = round(tb["spent"], 2)

    result: dict[str, Any] = {
        "session_gmv": round(gmv, 2),
        "session_orders": paid_count,
        "session_total_orders": len(today_orders),
        "top_sellers": top_sellers,
        "top_buyers": top_buyers,
    }

    if stream_context.get("creator_filter_enabled"):
        sr_start = stream_context.get("start")
        sr_end = stream_context.get("end")
    else:
        sr_start = stream_context.get("start") or _stream_range.get("start")
        sr_end = stream_context.get("end") if "end" in stream_context else _stream_range.get("end")
    if sr_start is not None:
        result["stream_start_utc"] = sr_start.isoformat()
        if sr_end:
            result["stream_end_utc"] = sr_end.isoformat()

        stream_gmv = 0.0
        stream_paid = 0
        stream_items = 0
        stream_product_agg: dict[str, dict] = {}
        stream_customer_agg: dict[str, dict] = {}
        q = select(TikTokOrder).where(TikTokOrder.created_at >= sr_start)
        if sr_end is not None:
            q = q.where(TikTokOrder.created_at <= sr_end)
        q = _apply_tiktok_account_scope(q, account_scope)
        stream_orders_rows = session.exec(q).all()
        product_scope = None
        if _should_use_live_product_scope(stream_context):
            stream_orders_rows, product_scope = _filter_orders_to_live_products(
                stream_orders_rows,
                stream_context,
                db_session=session,
            )
        for o in stream_orders_rows:
            status = (o.financial_status or o.order_status or "").lower().strip()
            if status not in TIKTOK_PAID_STATUSES:
                continue
            o_gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
            stream_gmv += o_gmv
            stream_paid += 1

            buyer_name = (o.customer_name or "").strip() or "Guest"
            buyer_key = buyer_name.lower()
            if buyer_key in stream_customer_agg:
                stream_customer_agg[buyer_key]["spent"] += o_gmv
                stream_customer_agg[buyer_key]["orders"] += 1
            else:
                stream_customer_agg[buyer_key] = {"name": buyer_name, "spent": o_gmv, "orders": 1, "order_list": []}

            try:
                s_items = json.loads(o.line_items_json) if o.line_items_json else []
            except (json.JSONDecodeError, TypeError):
                s_items = []
            if not isinstance(s_items, list):
                s_items = [s_items] if isinstance(s_items, dict) else []
            s_order_merged: dict[str, dict] = {}
            for it in s_items:
                if not isinstance(it, dict):
                    continue
                s_title = str(
                    it.get("product_name") or it.get("sku_name") or it.get("title") or it.get("item_name") or ""
                ).strip() or "Unknown item"
                s_sku = str(it.get("sku_name") or "").strip()
                s_variant = s_sku if s_sku and s_sku.lower() != "default" and s_sku.lower() != s_title.lower() else None
                s_key = (s_title.lower() + "||" + (s_variant or "").lower()).strip()
                try:
                    s_qty = max(int(it.get("quantity") or it.get("sku_quantity") or 1), 1)
                except (TypeError, ValueError):
                    s_qty = 1
                stream_items += s_qty
                s_unit = 0.0
                for pk in ("sale_price", "sku_sale_price", "price", "unit_price"):
                    val = it.get(pk)
                    if val is not None:
                        try:
                            s_unit = float(val)
                        except (TypeError, ValueError):
                            continue
                        break
                s_img = str(it.get("sku_image") or it.get("product_image") or it.get("image_url") or "").strip() or None
                if s_key in s_order_merged:
                    s_order_merged[s_key]["qty"] += s_qty
                else:
                    s_order_merged[s_key] = {"title": s_title, "qty": s_qty, "sku_image": s_img}
                if s_key in stream_product_agg:
                    stream_product_agg[s_key]["qty"] += s_qty
                    stream_product_agg[s_key]["revenue"] += round(s_qty * s_unit, 2)
                    if not stream_product_agg[s_key]["sku_image"] and s_img:
                        stream_product_agg[s_key]["sku_image"] = s_img
                else:
                    stream_product_agg[s_key] = {
                        "title": s_title, "variant": s_variant, "sku_image": s_img,
                        "qty": s_qty, "revenue": round(s_qty * s_unit, 2),
                        "buyers": {},
                    }
                if buyer_name not in stream_product_agg[s_key].get("buyers", {}):
                    stream_product_agg[s_key].setdefault("buyers", {})[buyer_name] = 0
                stream_product_agg[s_key]["buyers"][buyer_name] += s_qty

            if len(stream_customer_agg[buyer_key]["order_list"]) < 20:
                stream_customer_agg[buyer_key]["order_list"].append({
                    "order_number": o.order_number,
                    "total": round(o_gmv, 2),
                    "items": list(s_order_merged.values()),
                    "created_at": o.created_at.isoformat() if o.created_at else None,
                })

        stream_top_sellers = sorted(stream_product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:8]
        for sts in stream_top_sellers:
            sts["revenue"] = round(sts["revenue"], 2)
            sbd = sts.pop("buyers", {})
            sts["buyers"] = sorted(
                [{"name": n, "qty": q} for n, q in sbd.items()],
                key=lambda x: x["qty"], reverse=True,
            )
        stream_top_buyers = sorted(stream_customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
        for stb in stream_top_buyers:
            stb["spent"] = round(stb["spent"], 2)

        result["stream_gmv"] = round(stream_gmv, 2)
        result["stream_orders"] = stream_paid
        result["stream_items"] = stream_items
        result["stream_top_sellers"] = stream_top_sellers or _live_product_top_sellers(product_scope)
        result["stream_top_buyers"] = stream_top_buyers

    return _apply_tiktok_session_metrics(result, stream_context)

def _compute_order_velocity(session: Session, stream_context: Optional[dict[str, Any]] = None) -> list[dict]:
    """Compute per-minute order counts for the current stream window."""
    stream_context = stream_context or {}
    account_scope = (
        _stream_account_scope_for_context(session, stream_context)
        if stream_context.get("creator_filter_enabled")
        else {}
    )
    start = stream_context.get("start") if stream_context.get("creator_filter_enabled") else stream_context.get("start") or _stream_range.get("start")
    if not start:
        return []
    if stream_context.get("creator_filter_enabled"):
        end_dt = stream_context.get("end") or datetime.now(timezone.utc)
    else:
        end_dt = stream_context.get("end") or _stream_range.get("end") or datetime.now(timezone.utc)
    order_times_query = (
        select(TikTokOrder.created_at)
        .where(TikTokOrder.created_at >= start, TikTokOrder.created_at <= end_dt)
        .order_by(TikTokOrder.created_at)
    )
    order_times_query = _apply_tiktok_account_scope(order_times_query, account_scope)
    orders = session.exec(order_times_query).all()
    if _should_use_live_product_scope(stream_context):
        scoped_orders_query = (
            select(TikTokOrder)
            .where(TikTokOrder.created_at >= start, TikTokOrder.created_at <= end_dt)
            .order_by(TikTokOrder.created_at)
        )
        scoped_orders_query = _apply_tiktok_account_scope(scoped_orders_query, account_scope)
        scoped_orders = session.exec(scoped_orders_query).all()
        scoped_orders, _scope = _filter_orders_to_live_products(
            scoped_orders,
            stream_context,
            db_session=session,
        )
        orders = [order.created_at for order in scoped_orders]
    if not orders:
        return []
    buckets: dict[int, int] = {}
    for ts in orders:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        minute_key = int(ts.timestamp()) // 60
        buckets[minute_key] = buckets.get(minute_key, 0) + 1
    start_min = int(start.timestamp()) // 60
    end_min = int(end_dt.timestamp()) // 60
    result = []
    for m in range(max(start_min, end_min - 59), end_min + 1):
        result.append({"minute": m, "count": buckets.get(m, 0)})
    return result

def _compute_stream_duration_minutes(gmv_data: dict) -> float | None:
    start_utc = gmv_data.get("stream_start_utc")
    end_utc = gmv_data.get("stream_end_utc")
    if not start_utc:
        return None
    try:
        st = datetime.fromisoformat(start_utc) if isinstance(start_utc, str) else start_utc
        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        if end_utc:
            et = datetime.fromisoformat(end_utc) if isinstance(end_utc, str) else end_utc
            if et.tzinfo is None:
                et = et.replace(tzinfo=timezone.utc)
        else:
            et = datetime.now(timezone.utc)
        return round((et - st).total_seconds() / 60, 1)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/tiktok/streamer", response_class=HTMLResponse)
def tiktok_streamer_page(
    request: Request,
    creator: Optional[str] = Query(default=None),
    stream: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    # Employees can see the live stream dashboard (TikTok GMV + goal bar +
    # order feed). TikTok numbers are explicitly visible to floor staff so
    # they can chase goals during the stream.
    if denial := require_role_response(request, "employee"):
        return denial

    stream_context = _build_stream_context(creator, legacy_stream_id=stream)
    selected_creator = stream_context.get("selected_creator") or DEFAULT_STREAM_CREATOR
    selected_stream_id = stream_context.get("selected_stream_id") or ""
    current_streamer = get_current_streamer(session) or ""

    page_load_floor = datetime.now(timezone.utc) - timedelta(hours=24)
    scoped_orders, _product_scope = _load_scoped_stream_orders(
        session,
        stream_context,
        page_load_floor,
    )
    orders = scoped_orders[:50]

    cards = [_build_streamer_order_card(o) for o in orders]
    buyer_totals = _compute_buyer_lifetime_totals(session)
    _enrich_cards_with_buyer_totals(cards, buyer_totals)

    # Scope MAX(updated_at) to 24h window so old orders getting status-update
    # webhooks don't advance the cursor and cause stale orders to reappear.
    latest_updated_at = _latest_updated_at_for_orders(scoped_orders)
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()

    total_count = len(scoped_orders)
    gmv_data = _streamer_session_gmv(session, stream_context=stream_context)

    chat_info = get_chat_status()
    live_room_id = _live_room_id_for_stream(chat_info, stream_context)
    live_analytics = _get_live_analytics_snapshot()

    live_session = stream_context.get("live_session") or {}
    stream_data = {
        "stream_gmv": gmv_data.get("stream_gmv"),
        "stream_orders": gmv_data.get("stream_orders"),
        "stream_items": gmv_data.get("stream_items"),
        "stream_start_utc": gmv_data.get("stream_start_utc"),
        "tiktok_gmv": live_session.get("gmv") if live_session.get("ok") else None,
        "tiktok_items_sold": live_session.get("items_sold") if live_session.get("ok") else None,
        "tiktok_customers": live_session.get("customers") if live_session.get("ok") else None,
        "live_title": live_session.get("title") if live_session.get("ok") else None,
        "stream_range_source": stream_context.get("source"),
        "selected_creator": selected_creator,
        "selected_creator_label": stream_context.get("selected_creator_label") or "",
        "selected_stream_id": selected_stream_id,
        "selected_stream_label": stream_context.get("selected_stream_label") or "",
        "creator_filter_enabled": stream_context.get("creator_filter_enabled"),
        "creator_order_attribution": stream_context.get("creator_order_attribution"),
        "creator_order_attribution_message": _creator_order_attribution_message(stream_context),
        "stream_metric_source": gmv_data.get("stream_metric_source"),
        "stream_metric_label": gmv_data.get("stream_metric_label"),
        "stream_metric_note": gmv_data.get("stream_metric_note"),
        "live_room_id": live_room_id,
        "is_live": stream_context.get("is_live"),
    }

    return templates.TemplateResponse(request, "tiktok_streamer.html", {
        "request": request,
        "title": "TikTok Live Orders",
        "orders_json": json.dumps(cards),
        "latest_updated_at": latest_updated_at_text,
        "total_count": total_count,
        "session_gmv": gmv_data["session_gmv"],
        "session_orders": gmv_data["session_orders"],
        "session_total_orders": gmv_data["session_total_orders"],
        "top_sellers_json": json.dumps(gmv_data.get("top_sellers", [])),
        "top_buyers_json": json.dumps(gmv_data.get("top_buyers", [])),
        "stream_top_sellers_json": json.dumps(gmv_data.get("stream_top_sellers", [])),
        "stream_top_buyers_json": json.dumps(gmv_data.get("stream_top_buyers", [])),
        "live_analytics_json": json.dumps(live_analytics),
        "stream_data_json": json.dumps(stream_data),
        "stream_sessions_json": json.dumps(stream_context.get("sessions") or []),
        "build_version": _BUILD_VERSION,
        "chat_status": chat_info["status"],
        "current_user": getattr(request.state, "current_user", None),
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "current_streamer": current_streamer,
        "creator_options": _creator_options(selected_creator),
        "selected_creator": selected_creator,
        "selected_creator_label": stream_context.get("selected_creator_label") or "",
        "selected_stream_id": selected_stream_id,
        "selected_stream_label": stream_context.get("selected_stream_label") or "",
        "creator_filter_enabled": stream_context.get("creator_filter_enabled"),
        "creator_order_attribution": stream_context.get("creator_order_attribution"),
        "creator_order_attribution_message": _creator_order_attribution_message(stream_context),
        "live_room_id": live_room_id,
        "gmv_goal": float(_get_app_setting(session, "stream_gmv_goal", "0") or "0"),
        "high_value_threshold": float(_get_app_setting(session, "high_value_threshold", "100") or "100"),
        "vip_buyer_threshold": float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000"),
        "team_shell": request.query_params.get("team_shell") == "1",
    })

@router.get("/tiktok/streamer/poll")
def tiktok_streamer_poll(
    request: Request,
    since: Optional[str] = Query(default=None),
    creator: Optional[str] = Query(default=None),
    stream: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "employee"):
        return denial
    stream_context = _build_stream_context(creator, legacy_stream_id=stream)
    selected_creator = stream_context.get("selected_creator") or DEFAULT_STREAM_CREATOR
    selected_stream_id = stream_context.get("selected_stream_id") or ""
    created_at_floor = datetime.now(timezone.utc) - timedelta(hours=24)

    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            since_dt = None

    scoped_orders, _product_scope = _load_scoped_stream_orders(
        session,
        stream_context,
        created_at_floor,
        order_by_updated=True,
    )
    new_orders, _product_scope = _load_scoped_stream_orders(
        session,
        stream_context,
        created_at_floor,
        since_updated_at=since_dt,
        order_by_updated=True,
    )
    orders = new_orders[:20]
    cards = [_build_streamer_order_card(o) for o in orders]
    if cards:
        buyer_totals = _compute_buyer_lifetime_totals(session)
        _enrich_cards_with_buyer_totals(cards, buyer_totals)

    # Scope MAX(updated_at) to the same 24h window used for order queries.
    # Using global MAX caused the cursor to jump forward when old orders got
    # status-update webhooks, which made stale orders reappear at the top.
    latest_updated_at = _latest_updated_at_for_orders(scoped_orders)
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()

    total_count = len(scoped_orders)

    gmv_data = _streamer_session_gmv(session, stream_context=stream_context)
    live_session = stream_context.get("live_session") or {}
    current_streamer = get_current_streamer(session) or ""
    chat_info = get_chat_status()
    live_room_id = _live_room_id_for_stream(chat_info, stream_context)

    return {
        "orders": cards,
        "latest_updated_at": latest_updated_at_text,
        "total_count": total_count,
        "session_gmv": gmv_data["session_gmv"],
        "session_orders": gmv_data["session_orders"],
        "session_total_orders": gmv_data["session_total_orders"],
        "top_sellers": gmv_data.get("top_sellers", []),
        "top_buyers": gmv_data.get("top_buyers", []),
        "stream_top_sellers": gmv_data.get("stream_top_sellers", []),
        "stream_top_buyers": gmv_data.get("stream_top_buyers", []),
        "live_analytics": _get_live_analytics_snapshot(),
        "stream_gmv": gmv_data.get("stream_gmv"),
        "stream_orders": gmv_data.get("stream_orders"),
        "stream_items": gmv_data.get("stream_items"),
        "stream_start_utc": gmv_data.get("stream_start_utc"),
        "tiktok_gmv": live_session.get("gmv") if live_session.get("ok") else None,
        "stream_range_source": stream_context.get("source"),
        "stream_metric_source": gmv_data.get("stream_metric_source"),
        "stream_metric_label": gmv_data.get("stream_metric_label"),
        "stream_metric_note": gmv_data.get("stream_metric_note"),
        "stream_sessions": stream_context.get("sessions") or [],
        "is_live": stream_context.get("is_live"),
        "live_room_id": live_room_id,
        "build_version": _BUILD_VERSION,
        "gmv_goal": float(_get_app_setting(session, "stream_gmv_goal", "0") or "0"),
        "high_value_threshold": float(_get_app_setting(session, "high_value_threshold", "100") or "100"),
        "vip_buyer_threshold": float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000"),
        "order_velocity": _compute_order_velocity(session, stream_context=stream_context),
        "stream_end_utc": gmv_data.get("stream_end_utc"),
        "stream_duration_minutes": _compute_stream_duration_minutes(gmv_data),
        "current_streamer": current_streamer,
        "selected_creator": selected_creator,
        "selected_creator_label": stream_context.get("selected_creator_label") or "",
        "selected_stream_id": selected_stream_id,
        "selected_stream_label": stream_context.get("selected_stream_label") or "",
        "creator_filter_enabled": stream_context.get("creator_filter_enabled"),
        "creator_order_attribution": stream_context.get("creator_order_attribution"),
        "creator_order_attribution_message": _creator_order_attribution_message(stream_context),
    }

@router.get("/tiktok/streamer/goal")
def get_streamer_goal(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "employee"):
        return denial
    return {
        "gmv_goal": float(_get_app_setting(session, "stream_gmv_goal", "0") or "0"),
        "high_value_threshold": float(_get_app_setting(session, "high_value_threshold", "100") or "100"),
        "vip_buyer_threshold": float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000"),
    }

@router.post("/tiktok/streamer/goal")
def set_streamer_goal(request: Request, session: Session = Depends(get_session), body: dict = None):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if body is None:
        body = {}
    if "goal" in body:
        _set_app_setting(session, "stream_gmv_goal", str(float(body["goal"])))
    if "high_value_threshold" in body:
        _set_app_setting(session, "high_value_threshold", str(float(body["high_value_threshold"])))
    if "vip_buyer_threshold" in body:
        _set_app_setting(session, "vip_buyer_threshold", str(float(body["vip_buyer_threshold"])))
    if "vip_presence_timeout_min" in body:
        _set_app_setting(session, "vip_presence_timeout_min", str(max(1.0, float(body["vip_presence_timeout_min"]))))
    return {"ok": True}

@router.get("/tiktok/streamer/config", response_class=HTMLResponse)
def tiktok_streamer_config(request: Request, session: Session = Depends(get_session)):
    """Backend page to configure the stream date range shown on the streamer dashboard."""
    if denial := require_role_response(request, "admin"):
        return denial
    current_goal = float(_get_app_setting(session, "stream_gmv_goal", "0") or "0")
    current_threshold = float(_get_app_setting(session, "high_value_threshold", "100") or "100")
    s = _stream_range["start"]
    e = _stream_range["end"]
    start_val = s.astimezone(PACIFIC_TZ).strftime("%Y-%m-%dT%H:%M") if s else ""
    end_val = e.astimezone(PACIFIC_TZ).strftime("%Y-%m-%dT%H:%M") if e else ""
    source = _stream_range_source

    live_snap = _get_live_session_snapshot()
    has_auto = bool(live_snap.get("ok") and live_snap.get("start_time"))
    auto_title = str(live_snap.get("title") or "") if has_auto else ""
    auto_gmv = float(live_snap.get("gmv") or 0) if has_auto else 0.0
    auto_start_ts = int(live_snap.get("start_time") or 0) if has_auto else 0
    auto_end_ts = int(live_snap.get("end_time") or 0) if has_auto else 0
    if auto_start_ts > 0:
        auto_start_str = datetime.fromtimestamp(auto_start_ts, tz=PACIFIC_TZ).strftime("%b %d, %Y %I:%M %p")
    else:
        auto_start_str = ""
    if auto_end_ts > 0:
        auto_end_str = datetime.fromtimestamp(auto_end_ts, tz=PACIFIC_TZ).strftime("%b %d, %Y %I:%M %p")
    else:
        auto_end_str = ""

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stream Config</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css">
<style>
  body {{ font-family: system-ui, sans-serif; background: #0f0f0f; color: #e5e5e5;
         display: flex; justify-content: center; padding: 40px 16px; margin: 0; }}
  .card {{ background: #1a1a1a; border-radius: 16px; padding: 32px; max-width: 480px; width: 100%; }}
  h1 {{ font-size: 20px; margin: 0 0 8px; }}
  .subtitle {{ font-size: 13px; color: #888; margin-bottom: 24px; }}
  label {{ display: block; font-size: 11px; font-weight: 700; text-transform: uppercase;
           letter-spacing: .1em; color: #888; margin-bottom: 6px; }}
  .fp-input {{ width: 100%; padding: 12px 14px; border-radius: 10px;
    border: 1px solid #333; background: #111; color: #e5e5e5; font-size: 15px;
    margin-bottom: 18px; box-sizing: border-box; cursor: pointer; }}
  .fp-input:focus {{ outline: none; border-color: #22c55e; }}
  .row {{ display: flex; gap: 12px; margin-top: 8px; }}
  button {{ flex: 1; padding: 13px; border: none; border-radius: 10px; font-size: 14px;
            font-weight: 700; cursor: pointer; transition: all .15s; }}
  button:hover {{ opacity: .85; }}
  .btn-save {{ background: #22c55e; color: #000; }}
  .btn-clear {{ background: #333; color: #ccc; }}
  .btn-auto {{ background: #6366f1; color: #fff; }}
  .status {{ margin-top: 16px; font-size: 13px; color: #22c55e; display: none; text-align: center;
             padding: 8px; border-radius: 8px; background: rgba(34,197,94,.1); }}
  a {{ color: #22c55e; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .flatpickr-calendar {{ font-family: system-ui, sans-serif !important; }}
  .source-badge {{ display: inline-block; font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .08em; padding: 3px 8px; border-radius: 6px; }}
  .source-auto {{ background: rgba(99,102,241,.15); color: #818cf8; }}
  .source-manual {{ background: rgba(34,197,94,.15); color: #22c55e; }}
  .auto-card {{ background: #111; border: 1px solid #333; border-radius: 12px; padding: 16px;
    margin-bottom: 20px; }}
  .auto-card h3 {{ font-size: 13px; margin: 0 0 8px; color: #818cf8; }}
  .auto-row {{ font-size: 12px; color: #aaa; margin: 3px 0; }}
  .auto-row strong {{ color: #e5e5e5; }}
  hr {{ border: none; border-top: 1px solid #333; margin: 20px 0; }}
</style>
</head><body>
<div class="card">
  <h1>Stream Config <span class="source-badge {'source-auto' if source == 'auto' else 'source-manual'}">{source}</span></h1>
  <p class="subtitle">Set the date range for the current stream. GMV will be calculated from orders in this window.</p>
  {'<div class="auto-card" id="auto-card"><h3>Latest Detected Stream</h3>' +
   '<div class="auto-row"><strong>' + auto_title + '</strong></div>' +
   '<div class="auto-row">Start: <strong>' + auto_start_str + '</strong></div>' +
   '<div class="auto-row">End: <strong>' + (auto_end_str or 'ongoing') + '</strong></div>' +
   '<div class="auto-row">TikTok GMV: <strong style="color:#22c55e;">$' + f'{auto_gmv:,.2f}' + '</strong></div>' +
   '<div class="row" style="margin-top:12px;"><button class="btn-auto" onclick="useAuto()">Use This Stream</button></div></div>'
   if has_auto else '<div class="auto-card"><h3>Auto-Detection</h3><div class="auto-row">No live sessions detected yet. Data appears after a stream ends.</div></div>'}
  <hr>
  <label>Stream Start (Pacific)</label>
  <input class="fp-input" id="start" placeholder="Click to pick date & time" readonly>
  <label>Stream End (Pacific) — leave blank for ongoing</label>
  <input class="fp-input" id="end" placeholder="Click to pick date & time (optional)" readonly>
  <div class="row">
    <button class="btn-save" onclick="save()">Save (Manual)</button>
    <button class="btn-clear" onclick="clearRange()">Clear</button>
  </div>
  <div class="status" id="status"></div>
  <hr>
  <label>Stream GMV Goal ($)</label>
  <input class="fp-input" id="gmv-goal" type="number" min="0" step="100" value="{current_goal:.0f}" placeholder="e.g. 5000" style="cursor:text;">
  <label>High-Value Order Threshold ($)</label>
  <input class="fp-input" id="hv-threshold" type="number" min="0" step="10" value="{current_threshold:.0f}" placeholder="e.g. 100" style="cursor:text;">
  <label>VIP Buyer Lifetime Spend Threshold ($)</label>
  <input class="fp-input" id="vip-threshold" type="number" min="0" step="500" value="{float(_get_app_setting(session, 'vip_buyer_threshold', '5000') or '5000'):.0f}" placeholder="e.g. 5000" style="cursor:text;">
  <label>VIP Chat Presence Timeout (minutes)</label>
  <input class="fp-input" id="vip-presence-timeout" type="number" min="1" step="5" value="{float(_get_app_setting(session, 'vip_presence_timeout_min', '30') or '30'):.0f}" placeholder="e.g. 30" style="cursor:text;">
  <p style="font-size:11px;color:#666;margin:-12px 0 18px;">How long after a VIP's last activity before they drop off the "In Chat" panel. For long streams, try 60-120 min. They reappear instantly if they interact again.</p>
  <div class="row">
    <button style="background:#6366f1;color:#fff;" onclick="saveGoalSettings()">Save Goal Settings</button>
  </div>
  <div class="status" id="goal-status"></div>
  <p style="margin-top:24px;font-size:12px;text-align:center;">
    <a href="/tiktok/streamer">&larr; Back to Streamer Dashboard</a>
  </p>
</div>
<script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
<script>
var fpOpts = {{
  enableTime: true,
  dateFormat: 'Y-m-d H:i',
  altInput: true,
  altFormat: 'M j, Y  h:i K',
  time_24hr: false,
  theme: 'dark',
  disableMobile: true
}};
var fpStart = flatpickr('#start', Object.assign({{}}, fpOpts, {{
  defaultDate: '{start_val}'.replace('T', ' ') || null
}}));
var fpEnd = flatpickr('#end', Object.assign({{}}, fpOpts, {{
  defaultDate: '{end_val}'.replace('T', ' ') || null
}}));
function flash(msg, ok) {{
  var el = document.getElementById('status');
  el.textContent = msg;
  el.style.color = ok ? '#22c55e' : '#ef4444';
  el.style.background = ok ? 'rgba(34,197,94,.1)' : 'rgba(239,68,68,.1)';
  el.style.display = 'block';
  setTimeout(function() {{ el.style.display = 'none'; }}, 3000);
}}
function save() {{
  var s = document.getElementById('start').value;
  var e = document.getElementById('end').value;
  fetch('/tiktok/streamer/config', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ start: s, end: e }}) }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{ flash(d.ok ? 'Saved (manual)!' : (d.error || 'Error'), d.ok); }});
}}
function clearRange() {{
  fpStart.clear();
  fpEnd.clear();
  fetch('/tiktok/streamer/config', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ start: '', end: '' }}) }})
    .then(function(r) {{ return r.json(); }})
    .then(function() {{ flash('Cleared!', true); }});
}}
function useAuto() {{
  fetch('/tiktok/streamer/config/auto', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}} }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      if (d.ok) {{ flash('Switched to auto mode!', true); setTimeout(function(){{ location.reload(); }}, 1000); }}
      else {{ flash('Error switching to auto', false); }}
    }});
}}
function saveGoalSettings() {{
  var goal = parseFloat(document.getElementById('gmv-goal').value) || 0;
  var threshold = parseFloat(document.getElementById('hv-threshold').value) || 100;
  var vipThreshold = parseFloat(document.getElementById('vip-threshold').value) || 5000;
  var presenceTimeout = parseFloat(document.getElementById('vip-presence-timeout').value) || 30;
  fetch('/tiktok/streamer/goal', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ goal: goal, high_value_threshold: threshold, vip_buyer_threshold: vipThreshold, vip_presence_timeout_min: presenceTimeout }}) }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      var el = document.getElementById('goal-status');
      el.textContent = 'Saved!';
      el.style.color = '#22c55e';
      el.style.background = 'rgba(34,197,94,.1)';
      el.style.display = 'block';
      setTimeout(function() {{ el.style.display = 'none'; }}, 3000);
    }});
}}
</script>
</body></html>"""
    return HTMLResponse(content=html)

@router.post("/tiktok/streamer/config")
def tiktok_streamer_config_save(request: Request, body: dict = None):
    if denial := require_role_response(request, "admin"):
        return denial
    if body is None:
        body = {}
    start_str = (body.get("start") or "").strip()
    end_str = (body.get("end") or "").strip()

    if start_str:
        try:
            naive = datetime.fromisoformat(start_str)
            _stream_range["start"] = naive.replace(tzinfo=PACIFIC_TZ).astimezone(timezone.utc)
        except (ValueError, TypeError):
            return {"ok": False, "error": "Invalid start date"}
    else:
        _stream_range["start"] = None

    if end_str:
        try:
            naive = datetime.fromisoformat(end_str)
            _stream_range["end"] = naive.replace(tzinfo=PACIFIC_TZ).astimezone(timezone.utc)
        except (ValueError, TypeError):
            return {"ok": False, "error": "Invalid end date"}
    else:
        _stream_range["end"] = None

    _save_stream_range(source="manual")
    return {"ok": True, "start": str(_stream_range["start"] or ""), "end": str(_stream_range["end"] or "")}

@router.post("/tiktok/streamer/config/auto")
def tiktok_streamer_config_auto(request: Request):
    """Switch stream range to auto mode, applying the latest detected session immediately."""
    if denial := require_role_response(request, "admin"):
        return denial
    snap = _get_live_session_snapshot()
    start_ts = snap.get("start_time") or 0
    end_ts = snap.get("end_time") or 0
    if isinstance(start_ts, int) and start_ts > 0:
        _stream_range["start"] = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        _stream_range["end"] = datetime.fromtimestamp(end_ts, tz=timezone.utc) if isinstance(end_ts, int) and end_ts > 0 else None
    _save_stream_range(source="auto")
    return {
        "ok": True,
        "source": "auto",
        "start": str(_stream_range["start"] or ""),
        "end": str(_stream_range["end"] or ""),
        "session": snap if snap.get("ok") else None,
    }

@router.get("/tiktok/streamer/chat/poll")
def tiktok_streamer_chat_poll(
    request: Request,
    since: int = Query(default=0),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "employee"):
        return denial
    messages = get_live_chat_messages(since_idx=since)
    status_info = get_chat_status()
    latest_idx = messages[-1]["idx"] if messages else since

    vip_threshold = float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000")
    presence_timeout_min = float(_get_app_setting(session, "vip_presence_timeout_min", "30") or "30")
    vip_in_chat: list[dict] = []
    if vip_threshold > 0:
        viewers = get_stream_viewers(presence_timeout=presence_timeout_min * 60)
        buyer_totals = _compute_buyer_lifetime_totals(session)
        for v in viewers:
            key = v["username"].strip().lower()
            spent = buyer_totals.get(key, 0.0)
            if spent >= vip_threshold:
                vip_in_chat.append({
                    "username": v["username"],
                    "lifetime_spent": round(spent, 2),
                    "active": v["active"],
                })
        vip_in_chat.sort(key=lambda x: x["lifetime_spent"], reverse=True)

    return {
        "messages": messages,
        "latest_idx": latest_idx,
        "status": status_info["status"],
        "viewer_count": status_info["viewer_count"],
        "vip_in_chat": vip_in_chat,
    }


# ---------------------------------------------------------------------------
# Giveaway endpoints
# ---------------------------------------------------------------------------

@router.post("/tiktok/streamer/giveaway/start")
async def tiktok_giveaway_start(request: Request, body: dict = None):
    if denial := require_role_response(request, "admin"):
        return denial

    from ..tiktok_giveaway import get_giveaway_state, run_giveaway, _giveaway_task
    import app.tiktok_giveaway as _gmod

    state = get_giveaway_state()
    if state.status == "running":
        return {"ok": False, "error": "A giveaway is already running."}

    body = body or {}
    product_name = (body.get("product_name") or "").strip()
    if not product_name:
        return {"ok": False, "error": "product_name is required."}

    winners = int(body.get("winners", 1))
    keyword = (body.get("keyword") or "giveaway").strip()
    duration = (body.get("duration") or "5 min").strip()

    loop = asyncio.get_event_loop()
    task = loop.create_task(run_giveaway(product_name, winners, keyword, duration))
    _gmod._giveaway_task = task

    return {"ok": True, "message": "Giveaway started."}


@router.get("/tiktok/streamer/giveaway/status")
def tiktok_giveaway_status(request: Request):
    if denial := require_role_response(request, "employee"):
        return denial

    from ..tiktok_giveaway import get_giveaway_state
    return get_giveaway_state().to_dict()


@router.post("/tiktok/streamer/giveaway/cancel")
def tiktok_giveaway_cancel(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial

    from ..tiktok_giveaway import cancel_giveaway
    cancelled = cancel_giveaway()
    return {"ok": cancelled, "message": "Cancelled." if cancelled else "Nothing to cancel."}


@router.get("/tiktok/streamer/giveaway/health")
async def tiktok_giveaway_health(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial

    from ..tiktok_giveaway import check_session_health
    return await check_session_health()
