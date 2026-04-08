import asyncio
import csv
import hashlib
from html import escape
import json
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import distinct, func
from sqlalchemy.exc import OperationalError
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, delete, select

from .auth import authenticate_user, has_role, seed_default_users
from .attachment_storage import attachment_cache_path, generate_thumbnail, warm_attachment_cache, write_attachment_cache_file
from .bookkeeping import (
    refresh_bookkeeping_import_from_source,
    import_bookkeeping_file,
    get_bookkeeping_status_by_message_ids,
    list_bookkeeping_imports,
    list_detected_bookkeeping_posts,
    reconcile_bookkeeping_import,
)
from .cache import cache_get, cache_invalidate, cache_set
from .backfill_requests import (
    backfill_request_loop,
    cancel_backfill_request,
    enqueue_backfill_request,
    list_recent_backfill_requests,
    requeue_interrupted_backfill_requests,
    trigger_backfill_claim_attempt,
)
from .channels import (
    get_available_channel_choices,
    get_channel_filter_choices,
    get_expense_category_filter_choices,
    get_watched_channels,
    normalize_channel_ids,
    update_backfill_window,
    upsert_watched_channel,
)
from .config import get_settings
from .corrections import (
    get_correction_pattern_counts,
    get_learning_signal,
    get_learning_signals,
    promote_correction_pattern,
    save_review_correction,
    snapshot_message_parse,
)
from .db import (
    dispose_engine,
    engine,
    get_session,
    init_db,
    is_sqlite_lock_error,
    managed_session,
    recent_db_failure,
    run_write_with_retry,
)
from .discord_ingest import (
    discord_runtime_state,
    get_discord_client,
    list_available_discord_channels,
    periodic_attachment_repair_loop,
    recent_message_audit_loop,
    parse_iso_datetime,
    recover_attachment_assets_for_message,
    run_discord_bot,
    seed_channels_from_env,
)
from .financials import compute_financials
from .models import (
    AppSetting,
    AttachmentAsset,
    BackfillRequest,
    BIG_HIT_THRESHOLD,
    BookkeepingImport,
    DiscordMessage,
    expand_parse_status_filter_values,
    LiveHit,
    STREAMER_COLORS,
    OperationsLog,
    ParseAttempt,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    PLATFORMS,
    ReparseRun,
    ShopifyOrder,
    Streamer,
    STREAMERS,
    StreamSchedule,
    TikTokAuth,
    TikTokOrder,
    TikTokProduct,
    TikTokSyncState,
    User,
    WatchedChannel,
    normalize_money_value,
    normalize_parse_status,
    signed_money_delta,
    utcnow,
)
from .ops_log import count_recent_errors, list_operations_logs, list_operations_logs_for_backfill_request, parse_operations_log_details
from .reparse_runs import list_recent_reparse_runs, safe_create_reparse_run, safe_finalize_reparse_run_queue
from .reparse import reparse_message_row, reparse_message_rows
from .reporting import (
    build_financial_summary,
    build_reporting_periods,
    build_shopify_reporting_summary,
    build_tiktok_orders_page_data as build_tiktok_orders_page_reporting_data,
    build_tiktok_reporting_summary,
    classify_tiktok_reporting_status,
    get_financial_rows,
    get_shopify_reporting_rows,
    get_tiktok_reporting_rows,
    parse_report_datetime,
)
from .runtime_logging import resolve_runtime_log_path, setup_runtime_file_logging, structured_log_line
from .runtime_monitor import get_runtime_heartbeat_status, runtime_heartbeat_loop
from .schemas import HealthOut
from .shopify_ingest import (
    backfill_shopify_orders,
    read_shopify_backfill_state,
    update_shopify_backfill_state,
    upsert_shopify_order,
    validate_shopify_webhook,
)
from .display_media import (
    extract_image_urls,
    get_cached_attachment_map,
    merge_display_attachment_urls,
    normalize_attachment_urls_for_row,
    row_has_images,
)
from .transactions import build_transaction_summary, get_transactions, rebuild_transactions, sync_transaction_from_message
from .tiktok_auth_refresh import refresh_tiktok_auth_if_needed as _refresh_tiktok_auth_fn
from .tiktok_ingest import (
    TikTokIngestError,
    _build_webhook_signature_candidates,
    exchange_tiktok_authorization_code,
    parse_tiktok_webhook_headers,
    parse_tiktok_webhook_payload,
    upsert_tiktok_auth_from_callback,
    upsert_tiktok_order_from_payload,
)
from .tiktok_live_chat import (
    get_chat_status,
    get_recent_messages as get_live_chat_messages,
    get_room_id as get_live_room_id,
    start_live_chat,
    stop_live_chat,
)
from .worker import (
    STALE_PROCESSING_AFTER,
    clear_parsed_fields,
    parser_loop,
    periodic_stitch_audit_loop,
    queue_auto_reprocess_candidates,
    queue_reparse_range,
)
try:
    from scripts.tiktok_backfill import backfill_tiktok_orders as pull_tiktok_orders
    from scripts.tiktok_backfill import backfill_tiktok_products as pull_tiktok_products
    from scripts.tiktok_backfill import refresh_access_token as refresh_tiktok_access_token
    from scripts.tiktok_backfill import fetch_tiktok_order_details as _fetch_tiktok_order_details
    from scripts.tiktok_backfill import order_record_from_payload as _order_record_from_payload
    from scripts.tiktok_backfill import (
        fetch_tiktok_categories as _fetch_tiktok_categories,
        fetch_tiktok_category_attributes as _fetch_tiktok_category_attributes,
        fetch_tiktok_brands as _fetch_tiktok_brands,
        upload_tiktok_product_image as _upload_tiktok_product_image,
        create_tiktok_product as _create_tiktok_product,
        fetch_tiktok_product_detail as _fetch_tiktok_product_detail,
        product_record_from_payload as _product_record_from_payload,
        upsert_tiktok_product_row as _upsert_tiktok_product_row,
        fetch_tiktok_live_analytics as _fetch_tiktok_live_analytics,
        fetch_live_session_list as _fetch_live_session_list,
        fetch_overview_performance_daily as _fetch_overview_performance_daily,
        fetch_stream_performance_per_minutes as _fetch_stream_performance_per_minutes,
    )
except Exception:  # pragma: no cover - fallback if the script module is unavailable
    pull_tiktok_orders = None
    pull_tiktok_products = None
    refresh_tiktok_access_token = None
    _fetch_tiktok_order_details = None
    _order_record_from_payload = None
    _fetch_tiktok_categories = None
    _fetch_tiktok_category_attributes = None
    _fetch_tiktok_brands = None
    _upload_tiktok_product_image = None
    _create_tiktok_product = None
    _fetch_tiktok_product_detail = None
    _fetch_tiktok_live_analytics = None
    _fetch_live_session_list = None
    _fetch_overview_performance_daily = None
    _fetch_stream_performance_per_minutes = None
    _product_record_from_payload = None
    _upsert_tiktok_product_row = None


settings = get_settings()
setup_runtime_file_logging("app.log")

REPORT_SOURCE_ALL = "all"
REPORT_SOURCE_DISCORD = "discord"
REPORT_SOURCE_SHOPIFY = "shopify"
REPORT_SOURCE_TIKTOK = "tiktok"
REPORT_SOURCE_OPTIONS = {
    REPORT_SOURCE_ALL,
    REPORT_SOURCE_DISCORD,
    REPORT_SOURCE_SHOPIFY,
    REPORT_SOURCE_TIKTOK,
}

FINANCE_WINDOW_MTD = "mtd"
FINANCE_WINDOW_30D = "30d"
FINANCE_WINDOW_90D = "90d"
FINANCE_WINDOW_YTD = "ytd"
FINANCE_WINDOW_OPTIONS = {
    FINANCE_WINDOW_MTD,
    FINANCE_WINDOW_30D,
    FINANCE_WINDOW_90D,
    FINANCE_WINDOW_YTD,
}
FINANCE_WINDOW_LABELS = {
    FINANCE_WINDOW_MTD: "Month to date",
    FINANCE_WINDOW_30D: "Last 30 days",
    FINANCE_WINDOW_90D: "Last 90 days",
    FINANCE_WINDOW_YTD: "Year to date",
    "custom": "Custom range",
}


def count_rows(session: Session, stmt) -> int:
    count_stmt = select(func.count()).select_from(stmt.order_by(None).subquery())
    return int(session.exec(count_stmt).one())


def normalize_report_source(value: Optional[str]) -> str:
    if value is not None and not isinstance(value, str):
        value = getattr(value, "default", None)
    normalized = (value or "").strip().lower()
    return normalized if normalized in REPORT_SOURCE_OPTIONS else REPORT_SOURCE_ALL


def build_reports_url(
    *,
    source: str = REPORT_SOURCE_ALL,
    start: str = "",
    end: str = "",
    channel_id: str = "",
    entry_kind: str = "",
) -> str:
    params: dict[str, str] = {}
    if source and source != REPORT_SOURCE_ALL:
        params["source"] = source
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if channel_id:
        params["channel_id"] = channel_id
    if entry_kind:
        params["entry_kind"] = entry_kind
    if not params:
        return "/reports"
    return f"/reports?{urlencode(params)}"


def normalize_finance_window(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower()
    return normalized if normalized in FINANCE_WINDOW_OPTIONS else FINANCE_WINDOW_MTD


def build_finance_url(
    *,
    start: str = "",
    end: str = "",
    window: str = FINANCE_WINDOW_MTD,
) -> str:
    params: dict[str, str] = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if not params:
        params["window"] = normalize_finance_window(window)
    if not params:
        return "/finance"
    return f"/finance?{urlencode(params)}"


def build_tiktok_orders_url(
    *,
    start: str = "",
    end: str = "",
    stream: str = "",
    financial_status: str = "",
    fulfillment_status: str = "",
    order_status: str = "",
    source: str = "",
    currency: str = "",
    search: str = "",
    sort_by: str = "",
    sort_dir: str = "",
    page: int = 1,
    limit: int = 50,
) -> str:
    params: dict[str, str] = {}
    if stream:
        params["stream"] = stream
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if financial_status:
        params["financial_status"] = financial_status
    if fulfillment_status:
        params["fulfillment_status"] = fulfillment_status
    if order_status:
        params["order_status"] = order_status
    if source:
        params["source"] = source
    if currency:
        params["currency"] = currency
    if search:
        params["search"] = search
    if sort_by:
        params["sort_by"] = sort_by
    if sort_dir:
        params["sort_dir"] = sort_dir
    if page > 1:
        params["page"] = str(page)
    if limit and limit != 50:
        params["limit"] = str(limit)
    if not params:
        return "/tiktok/orders"
    return f"/tiktok/orders?{urlencode(params)}"


def build_tiktok_sort_url(
    *,
    current_sort_by: str,
    current_sort_dir: str,
    target_sort_by: str,
    start: str = "",
    end: str = "",
    financial_status: str = "",
    fulfillment_status: str = "",
    order_status: str = "",
    source: str = "",
    currency: str = "",
    search: str = "",
    limit: int = 50,
) -> str:
    next_dir = "asc"
    if current_sort_by == target_sort_by and current_sort_dir == "asc":
        next_dir = "desc"
    return build_tiktok_orders_url(
        start=start,
        end=end,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        source=source,
        currency=currency,
        search=search,
        sort_by=target_sort_by,
        sort_dir=next_dir,
        limit=limit,
    )



def normalize_filesystem_path(path: Path) -> str:
    normalized = os.path.normpath(str(path))
    if normalized.startswith("\\\\?\\"):
        return normalized[4:]
    return normalized


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=normalize_filesystem_path(BASE_DIR / "templates"))
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")

_live_analytics_cache: dict[str, object] = {}
_live_analytics_lock = threading.Lock()
_LIVE_ANALYTICS_POLL_SECONDS = 60

_gmv_cache: dict[str, Any] = {}
_gmv_cache_lock = threading.Lock()
_GMV_CACHE_TTL_SECONDS = 10

_stream_range: dict[str, Optional[datetime]] = {"start": None, "end": None}
_stream_range_source: str = "manual"

_live_session_cache: dict[str, object] = {}
_live_session_lock = threading.Lock()
_live_sessions_list_cache: list[dict] = []
_live_sessions_list_lock = threading.Lock()

PARSE_STATUS_OPTIONS = [
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_PARSED,
    PARSE_REVIEW_REQUIRED,
    PARSE_FAILED,
    PARSE_IGNORED,
]
DEAL_TYPE_OPTIONS = ["", "sell", "buy", "trade", "unknown"]
ENTRY_KIND_OPTIONS = ["", "sale", "buy", "trade", "expense", "unknown"]
PAYMENT_METHOD_OPTIONS = ["", "cash", "zelle", "venmo", "paypal", "card", "mixed", "trade", "unknown"]
CASH_DIRECTION_OPTIONS = ["", "to_store", "from_store", "none", "unknown"]
CATEGORY_OPTIONS = ["", "slabs", "singles", "sealed", "packs", "mixed", "accessories", "unknown"]
IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"]
NEARBY_IMAGE_AUDIT_WINDOW_SECONDS = 30
WORKER_RUNTIME_NAME = (settings.worker_runtime_name or "").strip()
if not WORKER_RUNTIME_NAME:
    if settings.runtime_name.endswith("_web"):
        WORKER_RUNTIME_NAME = f"{settings.runtime_name[:-4]}_worker"
    elif settings.runtime_name.endswith("_app"):
        WORKER_RUNTIME_NAME = f"{settings.runtime_name[:-4]}_worker"
    else:
        WORKER_RUNTIME_NAME = settings.runtime_name
APP_HEARTBEAT_RUNTIME_NAME = f"{settings.runtime_name}_app"
APP_RUNTIME_LABEL = "Web App"
WORKER_RUNTIME_LABEL = (settings.worker_runtime_label or "").strip() or "Ingest Worker"
LEARNED_RULE_EVENT_TYPES = (
    "queue.learned_rule_applied",
    "queue.learned_rule_skipped",
    "queue.learned_rule_rejected",
)


def normalize_status_filter(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if value == "review_queue":
        return value
    return normalize_parse_status(value)


def status_filter_values(value: str) -> list[str]:
    return sorted(expand_parse_status_filter_values([value]))


def format_pacific_datetime(value: object, include_zone: bool = True) -> str:
    if value in (None, ""):
        return ""

    parsed: Optional[datetime] = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
    else:
        return str(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    localized = parsed.astimezone(PACIFIC_TZ)
    suffix = f" {localized.tzname()}" if include_zone and localized.tzname() else ""
    return f"{localized.strftime('%Y-%m-%d %H:%M:%S')}{suffix}"


templates.env.filters["pacific_datetime"] = format_pacific_datetime


def format_pacific_date(value: object) -> str:
    if value in (None, ""):
        return ""

    parsed: Optional[datetime] = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return ""
    else:
        return ""

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(PACIFIC_TZ).strftime("%Y-%m-%d")


def summarize_message_snippet(text: Optional[str], max_length: int = 120) -> str:
    normalized = " ".join((text or "").split())
    if not normalized:
        return ""
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[:max_length - 3].rstrip()}..."


def build_learned_rule_label(details: dict) -> str:
    pattern_type = str(details.get("pattern_type") or "unknown").replace("_", " ")
    correction_source = details.get("correction_source")
    if correction_source:
        return f"{pattern_type} ({correction_source})"
    return pattern_type


def build_learned_rule_log_rows(session: Session, *, limit: int = 50) -> list[dict]:
    logs = session.exec(
        select(OperationsLog)
        .where(OperationsLog.event_type.in_(LEARNED_RULE_EVENT_TYPES))
        .order_by(OperationsLog.created_at.desc(), OperationsLog.id.desc())
        .limit(limit)
    ).all()

    details_by_log_id: dict[int, dict] = {}
    message_ids: list[int] = []
    for row in logs:
        details = parse_operations_log_details(row)
        details_by_log_id[row.id or 0] = details
        message_id = details.get("message_id")
        if isinstance(message_id, int):
            message_ids.append(message_id)

    message_map: dict[int, DiscordMessage] = {}
    if message_ids:
        message_rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.id.in_(sorted(set(message_ids))))
        ).all()
        message_map = {
            row.id: row
            for row in message_rows
            if row.id is not None
        }

    items: list[dict] = []
    for row in logs:
        details = details_by_log_id.get(row.id or 0, {})
        message_id = details.get("message_id")
        source_row = message_map.get(message_id) if isinstance(message_id, int) else None
        outcome = str(details.get("status") or row.event_type.removeprefix("queue.learned_rule_"))
        snippet_source = source_row.content if source_row is not None else details.get("normalized_text")
        items.append(
            {
                "created_at": format_pacific_datetime(row.created_at),
                "outcome": outcome,
                "rule_matched": build_learned_rule_label(details),
                "message_snippet": summarize_message_snippet(snippet_source),
                "reason": details.get("reason") or "",
            }
        )

    return items


def get_shopify_order_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    financial_status: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "date",
    sort_dir: str = "desc",
    page: int = 1,
    limit: Optional[int] = None,
) -> list[ShopifyOrder]:
    stmt = select(ShopifyOrder)
    if start:
        stmt = stmt.where(ShopifyOrder.created_at >= start)
    if end:
        stmt = stmt.where(ShopifyOrder.created_at <= end)
    if financial_status:
        stmt = stmt.where(ShopifyOrder.financial_status == financial_status)
    if source:
        stmt = stmt.where(ShopifyOrder.source == source)
    if search:
        pattern = f"%{search.strip().lower()}%"
        stmt = stmt.where(
            func.lower(ShopifyOrder.order_number).like(pattern)
            | func.lower(func.coalesce(ShopifyOrder.customer_name, "")).like(pattern)
        )
    safe_sort_by = sort_by if sort_by in {"date", "gross", "tax", "net"} else "date"
    safe_sort_dir = sort_dir if sort_dir in {"asc", "desc"} else "desc"
    net_expr = func.coalesce(ShopifyOrder.subtotal_ex_tax, ShopifyOrder.total_price - func.coalesce(ShopifyOrder.total_tax, 0.0))
    sort_column = {
        "date": ShopifyOrder.created_at,
        "gross": ShopifyOrder.total_price,
        "tax": func.coalesce(ShopifyOrder.total_tax, -1.0 if safe_sort_dir == "asc" else 0.0),
        "net": net_expr,
    }[safe_sort_by]
    if safe_sort_dir == "asc":
        stmt = stmt.order_by(sort_column.asc(), ShopifyOrder.id.asc())
    else:
        stmt = stmt.order_by(sort_column.desc(), ShopifyOrder.id.desc())
    if limit:
        offset = (max(page, 1) - 1) * limit
        stmt = stmt.offset(offset).limit(limit)
    return session.exec(stmt).all()


def count_shopify_order_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    financial_status: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    stmt = select(ShopifyOrder)
    if start:
        stmt = stmt.where(ShopifyOrder.created_at >= start)
    if end:
        stmt = stmt.where(ShopifyOrder.created_at <= end)
    if financial_status:
        stmt = stmt.where(ShopifyOrder.financial_status == financial_status)
    if source:
        stmt = stmt.where(ShopifyOrder.source == source)
    if search:
        pattern = f"%{search.strip().lower()}%"
        stmt = stmt.where(
            func.lower(ShopifyOrder.order_number).like(pattern)
            | func.lower(func.coalesce(ShopifyOrder.customer_name, "")).like(pattern)
        )
    return count_rows(session, stmt)


def build_shopify_item_summary(line_items_json: str) -> str:
    try:
        items = json.loads(line_items_json or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(items, list):
        return ""
    parts: list[str] = []
    for item in items[:4]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("title") or "").strip()
        quantity = int(item.get("quantity") or 0)
        if not name:
            continue
        parts.append(f"{name} x{quantity or 1}")
    summary = ", ".join(parts)
    if len(items) > 4:
        summary = f"{summary}, +{len(items) - 4} more"
    return summary


def build_tiktok_item_summary(line_items_summary_json: str, line_items_json: str = "") -> str:
    try:
        items = json.loads(line_items_summary_json or "[]")
    except json.JSONDecodeError:
        items = []
    if not isinstance(items, list) or not items:
        try:
            items = json.loads(line_items_json or "[]")
        except json.JSONDecodeError:
            return ""
        if not isinstance(items, list):
            return ""
    parts: list[str] = []
    for item in items[:4]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("title") or item.get("product_name") or item.get("item_name") or "").strip()
        quantity = int(item.get("quantity") or 0)
        if not name:
            continue
        parts.append(f"{name} x{quantity or 1}")
    summary = ", ".join(parts)
    if len(items) > 4:
        summary = f"{summary}, +{len(items) - 4} more"
    return summary

def build_shopify_orders_url(
    *,
    financial_status: str = "",
    source: str = "",
    start: str = "",
    end: str = "",
    search: str = "",
    sort_by: str = "",
    sort_dir: str = "",
    page: int = 1,
) -> str:
    params: dict[str, str] = {}
    if financial_status:
        params["financial_status"] = financial_status
    if source:
        params["source"] = source
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if search:
        params["search"] = search
    if sort_by:
        params["sort_by"] = sort_by
    if sort_dir:
        params["sort_dir"] = sort_dir
    if page > 1:
        params["page"] = str(page)
    if not params:
        return "/shopify/orders"
    return f"/shopify/orders?{urlencode(params)}"


def build_shopify_sort_url(
    *,
    current_sort_by: str,
    current_sort_dir: str,
    target_sort_by: str,
    financial_status: str = "",
    source: str = "",
    start: str = "",
    end: str = "",
    search: str = "",
) -> str:
    next_dir = "asc"
    if current_sort_by == target_sort_by and current_sort_dir == "asc":
        next_dir = "desc"
    return build_shopify_orders_url(
        financial_status=financial_status,
        source=source,
        start=start,
        end=end,
        search=search,
        sort_by=target_sort_by,
        sort_dir=next_dir,
    )


def build_shopify_order_summary(
    orders: list[ShopifyOrder],
) -> dict[str, object]:
    totals = {
        "orders": len(orders),
        "gross_revenue": 0.0,
        "total_tax": 0.0,
        "net_revenue": 0.0,
        "paid_orders": 0,
        "refunded_orders": 0,
        "avg_order_value": 0.0,
    }
    status_breakdown: dict[str, int] = {}
    source_breakdown: dict[str, int] = {}

    for order in orders:
        gross_value = float(order.total_price or 0.0)
        tax_value = float(order.total_tax or 0.0)
        net_value = float(order.subtotal_ex_tax if order.subtotal_ex_tax is not None else gross_value - tax_value)
        totals["gross_revenue"] += gross_value
        totals["total_tax"] += tax_value
        totals["net_revenue"] += net_value
        status = (order.financial_status or "unknown").strip() or "unknown"
        source_name = (order.source or "unknown").strip() or "unknown"
        status_breakdown[status] = status_breakdown.get(status, 0) + 1
        source_breakdown[source_name] = source_breakdown.get(source_name, 0) + 1
        if status == "paid":
            totals["paid_orders"] += 1
        if status == "refunded":
            totals["refunded_orders"] += 1

    if totals["orders"]:
        totals["avg_order_value"] = round(totals["gross_revenue"] / totals["orders"], 2)
    totals["gross_revenue"] = round(totals["gross_revenue"], 2)
    totals["total_tax"] = round(totals["total_tax"], 2)
    totals["net_revenue"] = round(totals["net_revenue"], 2)

    return {
        "totals": totals,
        "status_breakdown": dict(sorted(status_breakdown.items(), key=lambda item: (-item[1], item[0]))),
        "source_breakdown": dict(sorted(source_breakdown.items(), key=lambda item: (-item[1], item[0]))),
    }


def merge_shopify_order_summaries(
    left: dict[str, object], right: dict[str, object]
) -> dict[str, object]:
    lt = left["totals"]  # type: ignore[index]
    rt = right["totals"]  # type: ignore[index]
    orders = int(lt["orders"]) + int(rt["orders"])
    gross = float(lt["gross_revenue"]) + float(rt["gross_revenue"])
    tax = float(lt["total_tax"]) + float(rt["total_tax"])
    net = float(lt["net_revenue"]) + float(rt["net_revenue"])
    paid = int(lt["paid_orders"]) + int(rt["paid_orders"])
    refunded = int(lt["refunded_orders"]) + int(rt["refunded_orders"])
    status_breakdown: dict[str, int] = dict(left["status_breakdown"])  # type: ignore[arg-type]
    for k, v in dict(right["status_breakdown"]).items():  # type: ignore[arg-type]
        status_breakdown[k] = status_breakdown.get(k, 0) + int(v)
    source_breakdown: dict[str, int] = dict(left["source_breakdown"])  # type: ignore[arg-type]
    for k, v in dict(right["source_breakdown"]).items():  # type: ignore[arg-type]
        source_breakdown[k] = source_breakdown.get(k, 0) + int(v)
    avg = round(gross / orders, 2) if orders else 0.0
    merged_totals = {
        "orders": orders,
        "gross_revenue": round(gross, 2),
        "total_tax": round(tax, 2),
        "net_revenue": round(net, 2),
        "paid_orders": paid,
        "refunded_orders": refunded,
        "avg_order_value": avg,
    }
    return {
        "totals": merged_totals,
        "status_breakdown": dict(sorted(status_breakdown.items(), key=lambda item: (-item[1], item[0]))),
        "source_breakdown": dict(sorted(source_breakdown.items(), key=lambda item: (-item[1], item[0]))),
    }


def empty_transaction_summary() -> dict[str, object]:
    return build_transaction_summary([])


def empty_shopify_reporting_summary() -> dict[str, object]:
    return build_shopify_reporting_summary([])


def build_report_period_comparison_rows(
    session: Session,
    *,
    periods: list[dict[str, object]],
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for period in periods:
        start = period.get("start")
        end = period.get("end")
        discord_rows = get_transactions(
            session,
            start=start if isinstance(start, datetime) else None,
            end=end if isinstance(end, datetime) else None,
            channel_id=channel_id,
            entry_kind=entry_kind,
        )
        discord_summary = build_transaction_summary(discord_rows)
        shopify_rows = get_shopify_reporting_rows(
            session,
            start=start if isinstance(start, datetime) else None,
            end=end if isinstance(end, datetime) else None,
        )
        shopify_summary = build_shopify_reporting_summary(shopify_rows)
        tiktok_rows = get_tiktok_reporting_rows(
            session,
            start=start if isinstance(start, datetime) else None,
            end=end if isinstance(end, datetime) else None,
        )
        tiktok_summary = build_tiktok_reporting_summary(tiktok_rows)
        discord_gross = round(float(discord_summary["totals"].get("money_in", 0.0) or 0.0), 2)
        discord_outflow = round(float(discord_summary["totals"].get("money_out", 0.0) or 0.0), 2)
        discord_net = round(float(discord_summary["totals"].get("net", 0.0) or 0.0), 2)
        shopify_gross = round(float(shopify_summary["gross_revenue"] or 0.0), 2)
        shopify_tax = round(float(shopify_summary["total_tax"] or 0.0), 2)
        shopify_net = round(float(shopify_summary["net_revenue"] or 0.0), 2)
        tiktok_gross = round(float(tiktok_summary["gross_revenue"] or 0.0), 2)
        tiktok_tax = round(float(tiktok_summary["total_tax"] or 0.0), 2)
        tiktok_net = round(float(tiktok_summary["net_revenue"] or 0.0), 2)
        rows.append(
            {
                "key": period.get("key") or "",
                "label": period.get("label") or "Period",
                "discord_gross": discord_gross,
                "discord_outflow": discord_outflow,
                "discord_net": discord_net,
                "shopify_gross": shopify_gross,
                "shopify_tax": shopify_tax,
                "shopify_net": shopify_net,
                "tiktok_gross": tiktok_gross,
                "tiktok_tax": tiktok_tax,
                "tiktok_net": tiktok_net,
                "combined_revenue": round(discord_gross + shopify_net + tiktok_net, 2),
                "shopify_tax_unknown_orders": int(shopify_summary["tax_unknown_orders"] or 0),
                "tiktok_tax_unknown_orders": int(tiktok_summary["tax_unknown_orders"] or 0),
            }
        )
    return rows


def shift_month_start(value: datetime, months: int) -> datetime:
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    return value.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def format_finance_range_label(start: datetime, end: datetime) -> str:
    start_local = start.astimezone(PACIFIC_TZ)
    end_local = end.astimezone(PACIFIC_TZ)
    if start_local.year == end_local.year:
        return f"{start_local.strftime('%b %d')} - {end_local.strftime('%b %d, %Y')}"
    return f"{start_local.strftime('%b %d, %Y')} - {end_local.strftime('%b %d, %Y')}"


def safe_percent(numerator: float, denominator: float) -> Optional[float]:
    if abs(float(denominator or 0.0)) < 0.005:
        return None
    return round((float(numerator or 0.0) / float(denominator)) * 100.0, 1)


def percent_change(current: float, prior: float) -> Optional[float]:
    if abs(float(prior or 0.0)) < 0.005:
        return None
    return round(((float(current or 0.0) - float(prior or 0.0)) / abs(float(prior))) * 100.0, 1)


def format_signed_money(value: float) -> str:
    amount = round(float(value or 0.0), 2)
    sign = "+" if amount > 0 else "-" if amount < 0 else ""
    return f"{sign}{format_dashboard_money(abs(amount))}"


def format_percent_value(value: Optional[float]) -> str:
    if value is None:
        return "--"
    return f"{float(value):.1f}%"


def format_percent_points(value: Optional[float]) -> str:
    if value is None:
        return "--"
    return f"{float(value):+.1f} pts"


def resolve_finance_range(
    *,
    start: Optional[str],
    end: Optional[str],
    window: Optional[str],
) -> dict[str, object]:
    normalized_window = normalize_finance_window(window)
    now_local = datetime.now(PACIFIC_TZ)
    today_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end_local = today_start_local + timedelta(days=1) - timedelta(microseconds=1)

    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    manual_range = bool(start_dt or end_dt)

    if manual_range:
        if end_dt is None:
            end_dt = today_end_local.astimezone(timezone.utc)
        if start_dt is None:
            end_local = end_dt.astimezone(PACIFIC_TZ)
            start_local = end_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            start_dt = start_local.astimezone(timezone.utc)
        selected_window = "custom"
    else:
        if normalized_window == FINANCE_WINDOW_30D:
            start_local = today_start_local - timedelta(days=29)
        elif normalized_window == FINANCE_WINDOW_90D:
            start_local = today_start_local - timedelta(days=89)
        elif normalized_window == FINANCE_WINDOW_YTD:
            start_local = today_start_local.replace(month=1, day=1)
        else:
            start_local = today_start_local.replace(day=1)
        start_dt = start_local.astimezone(timezone.utc)
        end_dt = today_end_local.astimezone(timezone.utc)
        selected_window = normalized_window

    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    current_span = end_dt - start_dt
    previous_end_dt = start_dt - timedelta(microseconds=1)
    previous_start_dt = previous_end_dt - current_span

    start_local = start_dt.astimezone(PACIFIC_TZ)
    end_local = end_dt.astimezone(PACIFIC_TZ)
    day_count = max((end_local.date() - start_local.date()).days + 1, 1)

    return {
        "start_dt": start_dt,
        "end_dt": end_dt,
        "previous_start_dt": previous_start_dt,
        "previous_end_dt": previous_end_dt,
        "selected_start": start_local.strftime("%Y-%m-%d"),
        "selected_end": end_local.strftime("%Y-%m-%d"),
        "selected_window": selected_window,
        "window_label": FINANCE_WINDOW_LABELS.get(selected_window, FINANCE_WINDOW_LABELS[FINANCE_WINDOW_MTD]),
        "label": format_finance_range_label(start_dt, end_dt),
        "previous_label": format_finance_range_label(previous_start_dt, previous_end_dt),
        "day_count": day_count,
        "as_of_label": now_local.strftime("%b %d, %Y %I:%M %p"),
    }


def compose_finance_statement(
    *,
    discord_summary: dict[str, object],
    shopify_summary: dict[str, object],
    tiktok_summary: dict[str, object],
    day_count: int,
) -> dict[str, object]:
    discord_totals = discord_summary.get("totals", {})
    expense_categories = discord_summary.get("expense_categories", {})

    discord_money_in = round(float(discord_totals.get("money_in", 0.0) or 0.0), 2)
    discord_money_out = round(float(discord_totals.get("money_out", 0.0) or 0.0), 2)
    discord_sales = round(float(discord_totals.get("sales", 0.0) or 0.0), 2)
    discord_buys = round(float(discord_totals.get("buys", 0.0) or 0.0), 2)
    discord_trade_in = round(float(discord_totals.get("trade_cash_in", 0.0) or 0.0), 2)
    discord_trade_out = round(float(discord_totals.get("trade_cash_out", 0.0) or 0.0), 2)
    inventory_expense = round(float(expense_categories.get("inventory", 0.0) or 0.0), 2)

    inventory_spend = round(discord_buys + discord_trade_out + inventory_expense, 2)
    operating_expenses = round(max(discord_money_out - inventory_spend, 0.0), 2)

    shopify_net_revenue = round(float(shopify_summary.get("net_revenue", 0.0) or 0.0), 2)
    shopify_tax = round(float(shopify_summary.get("total_tax", 0.0) or 0.0), 2)
    tiktok_net_revenue = round(float(tiktok_summary.get("net_revenue", 0.0) or 0.0), 2)
    tiktok_tax = round(float(tiktok_summary.get("total_tax", 0.0) or 0.0), 2)

    revenue = round(discord_money_in + shopify_net_revenue + tiktok_net_revenue, 2)
    gross_profit = round(revenue - inventory_spend, 2)
    operating_profit = round(gross_profit - operating_expenses, 2)
    external_tax = round(shopify_tax + tiktok_tax, 2)

    gross_margin_pct = safe_percent(gross_profit, revenue)
    operating_margin_pct = safe_percent(operating_profit, revenue)
    inventory_ratio_pct = safe_percent(inventory_spend, revenue)
    opex_ratio_pct = safe_percent(operating_expenses, revenue)

    return {
        "discord_rows": int(discord_summary.get("rows", 0) or 0),
        "discord_revenue": discord_money_in,
        "discord_money_out": discord_money_out,
        "discord_sales": discord_sales,
        "discord_buys": discord_buys,
        "discord_trade_in": discord_trade_in,
        "discord_trade_out": discord_trade_out,
        "inventory_expense": inventory_expense,
        "inventory_spend": inventory_spend,
        "operating_expenses": operating_expenses,
        "shopify_net_revenue": shopify_net_revenue,
        "shopify_tax": shopify_tax,
        "shopify_paid_orders": int(shopify_summary.get("paid_orders", 0) or 0),
        "shopify_tax_unknown_orders": int(shopify_summary.get("tax_unknown_orders", 0) or 0),
        "tiktok_net_revenue": tiktok_net_revenue,
        "tiktok_tax": tiktok_tax,
        "tiktok_paid_orders": int(tiktok_summary.get("paid_orders", 0) or 0),
        "tiktok_tax_unknown_orders": int(tiktok_summary.get("tax_unknown_orders", 0) or 0),
        "revenue": revenue,
        "gross_profit": gross_profit,
        "operating_profit": operating_profit,
        "external_tax": external_tax,
        "gross_margin_pct": gross_margin_pct,
        "operating_margin_pct": operating_margin_pct,
        "inventory_ratio_pct": inventory_ratio_pct,
        "opex_ratio_pct": opex_ratio_pct,
        "review_required": int(discord_summary.get("counts", {}).get("needs_review", 0) or 0),
        "tax_unknown_orders": int(shopify_summary.get("tax_unknown_orders", 0) or 0)
        + int(tiktok_summary.get("tax_unknown_orders", 0) or 0),
        "day_count": max(day_count, 1),
        "avg_daily_revenue": round(revenue / max(day_count, 1), 2),
        "avg_daily_profit": round(operating_profit / max(day_count, 1), 2),
        "revenue_display": format_dashboard_money(revenue),
        "gross_profit_display": format_dashboard_money(gross_profit),
        "operating_profit_display": format_dashboard_money(operating_profit),
        "inventory_spend_display": format_dashboard_money(inventory_spend),
        "operating_expenses_display": format_dashboard_money(operating_expenses),
        "external_tax_display": format_dashboard_money(external_tax),
        "discord_revenue_display": format_dashboard_money(discord_money_in),
        "shopify_net_revenue_display": format_dashboard_money(shopify_net_revenue),
        "tiktok_net_revenue_display": format_dashboard_money(tiktok_net_revenue),
        "discord_sales_display": format_dashboard_money(discord_sales),
        "discord_buys_display": format_dashboard_money(discord_buys),
        "discord_trade_in_display": format_dashboard_money(discord_trade_in),
        "discord_trade_out_display": format_dashboard_money(discord_trade_out),
        "inventory_expense_display": format_dashboard_money(inventory_expense),
        "avg_daily_revenue_display": format_dashboard_money(round(revenue / max(day_count, 1), 2)),
        "avg_daily_profit_display": format_dashboard_money(round(operating_profit / max(day_count, 1), 2)),
        "gross_margin_display": format_percent_value(gross_margin_pct),
        "operating_margin_display": format_percent_value(operating_margin_pct),
        "inventory_ratio_display": format_percent_value(inventory_ratio_pct),
        "opex_ratio_display": format_percent_value(opex_ratio_pct),
    }


def build_finance_range_snapshot(
    session: Session,
    *,
    start: datetime,
    end: datetime,
    day_count: int,
) -> dict[str, object]:
    transactions = get_transactions(session, start=start, end=end)
    discord_summary = build_transaction_summary(transactions)
    shopify_rows = get_shopify_reporting_rows(session, start=start, end=end)
    shopify_summary = build_shopify_reporting_summary(shopify_rows)
    tiktok_rows = get_tiktok_reporting_rows(session, start=start, end=end)
    tiktok_summary = build_tiktok_reporting_summary(tiktok_rows)
    statement = compose_finance_statement(
        discord_summary=discord_summary,
        shopify_summary=shopify_summary,
        tiktok_summary=tiktok_summary,
        day_count=day_count,
    )
    return {
        "transactions": transactions,
        "discord_summary": discord_summary,
        "shopify_summary": shopify_summary,
        "tiktok_summary": tiktok_summary,
        "statement": statement,
    }


def build_finance_statement_rows(
    current_statement: dict[str, object],
    prior_statement: dict[str, object],
) -> list[dict[str, object]]:
    row_specs = [
        ("Discord cash-in revenue", "discord_revenue", "money"),
        ("Shopify net revenue", "shopify_net_revenue", "money"),
        ("TikTok Shop net revenue", "tiktok_net_revenue", "money"),
        ("Total revenue", "revenue", "money"),
        ("Inventory cash deployed", "inventory_spend", "money"),
        ("Gross profit", "gross_profit", "money"),
        ("Operating expenses", "operating_expenses", "money"),
        ("Operating profit", "operating_profit", "money"),
        ("Operating margin", "operating_margin_pct", "percent"),
    ]

    rows: list[dict[str, object]] = []
    for label, key, kind in row_specs:
        current_value = float(current_statement.get(key, 0.0) or 0.0)
        prior_value = float(prior_statement.get(key, 0.0) or 0.0)
        if kind == "percent":
            current_display = format_percent_value(current_value)
            prior_display = format_percent_value(prior_value)
        else:
            current_display = format_dashboard_money(current_value)
            prior_display = format_dashboard_money(prior_value)
        rows.append(
            {
                "label": label,
                "current_display": current_display,
                "prior_display": prior_display,
            }
        )
    return rows


def build_finance_kpi_rows(
    current_statement: dict[str, object],
    prior_statement: dict[str, object],
) -> list[dict[str, object]]:
    kpi_specs = [
        ("Net Revenue", "revenue", "money", "up", "Discord cash in + platform net sales"),
        ("Gross Profit", "gross_profit", "money", "up", "Revenue less inventory cash deployment"),
        ("Operating Profit", "operating_profit", "money", "up", "Gross profit after operating spend"),
        ("Operating Margin", "operating_margin_pct", "percent", "up", "Operating profit divided by revenue"),
        ("Inventory Spend", "inventory_spend", "money", "down", "Buys, trade cash out, and inventory-tagged expenses"),
        ("Tax Collected", "external_tax", "money", "neutral", "Shopify and TikTok tax tracked separately from net revenue"),
    ]

    rows: list[dict[str, object]] = []
    for label, key, kind, preferred_direction, footnote in kpi_specs:
        current_value = float(current_statement.get(key, 0.0) or 0.0)
        prior_value = float(prior_statement.get(key, 0.0) or 0.0)
        delta = round(current_value - prior_value, 2)
        if kind == "percent":
            value_display = format_percent_value(current_value)
            delta_display = format_percent_points(delta)
        else:
            value_display = format_dashboard_money(current_value)
            delta_display = format_signed_money(delta)

        tone = "neutral"
        if preferred_direction == "neutral" or abs(delta) < 0.005:
            tone = "neutral"
        elif preferred_direction == "up":
            tone = "positive" if delta > 0 else "negative"
        elif preferred_direction == "down":
            tone = "positive" if delta < 0 else "negative"

        rows.append(
            {
                "label": label,
                "value_display": value_display,
                "delta_display": delta_display,
                "footnote": footnote,
                "tone": tone,
            }
        )
    return rows


def build_finance_source_mix_rows(statement: dict[str, object]) -> list[dict[str, object]]:
    source_values = {
        "Discord": float(statement.get("discord_revenue", 0.0) or 0.0),
        "Shopify": float(statement.get("shopify_net_revenue", 0.0) or 0.0),
        "TikTok Shop": float(statement.get("tiktok_net_revenue", 0.0) or 0.0),
    }
    filtered = {label: value for label, value in source_values.items() if value > 0}
    if not filtered:
        filtered = source_values

    max_value = max([abs(value) for value in filtered.values()] or [1.0])
    total_value = sum(float(value or 0.0) for value in filtered.values()) or 1.0
    return [
        {
            "label": label,
            "value": round(value, 2),
            "value_display": format_dashboard_money(value),
            "share_display": format_percent_value(safe_percent(value, total_value)),
            "width_pct": round((abs(value) / max_value) * 100.0, 1) if max_value else 0.0,
        }
        for label, value in sorted(filtered.items(), key=lambda item: (-item[1], item[0]))
    ]


def build_finance_spend_mix_rows(statement: dict[str, object]) -> list[dict[str, object]]:
    spend_values = {
        "Buys": float(statement.get("discord_buys", 0.0) or 0.0),
        "Trade cash out": float(statement.get("discord_trade_out", 0.0) or 0.0),
        "Inventory-tagged expenses": float(statement.get("inventory_expense", 0.0) or 0.0),
        "Operating expenses": float(statement.get("operating_expenses", 0.0) or 0.0),
    }
    filtered = {label: value for label, value in spend_values.items() if value > 0}
    if not filtered:
        filtered = spend_values

    max_value = max([abs(value) for value in filtered.values()] or [1.0])
    total_value = sum(float(value or 0.0) for value in filtered.values()) or 1.0
    return [
        {
            "label": label,
            "value": round(value, 2),
            "value_display": format_dashboard_money(value),
            "share_display": format_percent_value(safe_percent(value, total_value)),
            "width_pct": round((abs(value) / max_value) * 100.0, 1) if max_value else 0.0,
        }
        for label, value in sorted(filtered.items(), key=lambda item: (-item[1], item[0]))
    ]


def build_finance_channel_rows(transactions) -> list[dict[str, object]]:
    channel_totals: dict[str, dict[str, object]] = {}
    for row in transactions:
        channel_label = row.channel_name or row.channel_id or "Unknown channel"
        bucket = channel_totals.setdefault(
            channel_label,
            {
                "label": channel_label,
                "count": 0,
                "money_in": 0.0,
                "money_out": 0.0,
                "net": 0.0,
            },
        )
        money_in = normalize_money_value(row.money_in)
        money_out = normalize_money_value(row.money_out)
        bucket["count"] = int(bucket["count"]) + 1
        bucket["money_in"] = float(bucket["money_in"]) + money_in
        bucket["money_out"] = float(bucket["money_out"]) + money_out
        bucket["net"] = float(bucket["net"]) + signed_money_delta(money_in, money_out)

    rows = [
        {
            "label": bucket["label"],
            "count": int(bucket["count"]),
            "money_in_display": format_dashboard_money(float(bucket["money_in"])),
            "money_out_display": format_dashboard_money(float(bucket["money_out"])),
            "net_display": format_dashboard_money(float(bucket["net"])),
        }
        for bucket in sorted(
            channel_totals.values(),
            key=lambda item: (-float(item["net"]), -int(item["count"]), str(item["label"]).lower()),
        )[:6]
    ]
    return rows


def build_finance_notes(
    *,
    current_statement: dict[str, object],
    prior_statement: dict[str, object],
    range_label: str,
    prior_label: str,
    source_mix_rows: list[dict[str, object]],
    top_channels: list[dict[str, object]],
) -> list[dict[str, str]]:
    notes: list[dict[str, str]] = []

    profit_delta = float(current_statement.get("operating_profit", 0.0) or 0.0) - float(
        prior_statement.get("operating_profit", 0.0) or 0.0
    )
    margin_label = str(current_statement.get("operating_margin_display") or "--")
    if float(current_statement.get("operating_profit", 0.0) or 0.0) >= 0:
        notes.append(
            {
                "title": "Profit posture",
                "body": (
                    f"{range_label} closed at {current_statement['operating_profit_display']} "
                    f"of operating profit and {margin_label} margin, "
                    f"{format_signed_money(profit_delta)} versus {prior_label}."
                ),
            }
        )
    else:
        notes.append(
            {
                "title": "Profit posture",
                "body": (
                    f"{range_label} is currently running at {current_statement['operating_profit_display']} "
                    f"operating profit with {margin_label} margin, "
                    f"{format_signed_money(profit_delta)} versus {prior_label}."
                ),
            }
        )

    if source_mix_rows:
        lead_source = source_mix_rows[0]
        notes.append(
            {
                "title": "Lead revenue source",
                "body": (
                    f"{lead_source['label']} is contributing {lead_source['share_display']} "
                    f"of selected-range revenue at {lead_source['value_display']}."
                ),
            }
        )

    tax_unknown_orders = int(current_statement.get("tax_unknown_orders", 0) or 0)
    review_required = int(current_statement.get("review_required", 0) or 0)
    if tax_unknown_orders:
        notes.append(
            {
                "title": "Data coverage",
                "body": (
                    f"{tax_unknown_orders} external orders are missing tax detail, "
                    "so platform net sales on this page are intentionally conservative."
                ),
            }
        )
    elif review_required:
        notes.append(
            {
                "title": "Review backlog",
                "body": (
                    f"{review_required} Discord transactions in this range still need human review. "
                    "The P&L is usable, but the cash mix can still tighten up as those rows are approved."
                ),
            }
        )
    else:
        notes.append(
            {
                "title": "Data coverage",
                "body": "No tax gaps or in-range Discord review backlog were detected for this view.",
            }
        )

    if top_channels:
        lead_channel = top_channels[0]
        notes.append(
            {
                "title": "Best Discord lane",
                "body": (
                    f"{lead_channel['label']} led Discord activity with {lead_channel['net_display']} net "
                    f"across {lead_channel['count']} transaction"
                    f"{'' if lead_channel['count'] == 1 else 's'}."
                ),
            }
        )

    return notes[:4]


def build_finance_quality_rows(
    *,
    current_statement: dict[str, object],
    range_data: dict[str, object],
) -> list[dict[str, str]]:
    return [
        {
            "label": "Range length",
            "value": f"{range_data['day_count']} days",
            "detail": str(range_data["label"]),
        },
        {
            "label": "Discord rows",
            "value": str(current_statement["discord_rows"]),
            "detail": f"{current_statement['review_required']} still flagged for review",
        },
        {
            "label": "Paid platform orders",
            "value": str(
                int(current_statement["shopify_paid_orders"]) + int(current_statement["tiktok_paid_orders"])
            ),
            "detail": (
                f"Shopify {current_statement['shopify_paid_orders']} | "
                f"TikTok {current_statement['tiktok_paid_orders']}"
            ),
        },
        {
            "label": "Tax completeness",
            "value": str(current_statement["tax_unknown_orders"]),
            "detail": "Orders missing tax detail on external platforms",
        },
    ]


def build_finance_monthly_rows(session: Session, *, months: int = 6) -> list[dict[str, object]]:
    now_local = datetime.now(PACIFIC_TZ)
    current_month_start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows: list[dict[str, object]] = []

    for offset in range(months - 1, -1, -1):
        month_start_local = shift_month_start(current_month_start, -offset)
        next_month_start_local = shift_month_start(month_start_local, 1)
        month_end_local = next_month_start_local - timedelta(microseconds=1)
        day_count = max((month_end_local.date() - month_start_local.date()).days + 1, 1)
        snapshot = build_finance_range_snapshot(
            session,
            start=month_start_local.astimezone(timezone.utc),
            end=month_end_local.astimezone(timezone.utc),
            day_count=day_count,
        )
        statement = snapshot["statement"]
        rows.append(
            {
                "label": month_start_local.strftime("%b %Y"),
                "revenue_display": statement["revenue_display"],
                "operating_profit_display": statement["operating_profit_display"],
                "margin_display": statement["operating_margin_display"],
                "discord_display": statement["discord_revenue_display"],
                "shopify_display": statement["shopify_net_revenue_display"],
                "tiktok_display": statement["tiktok_net_revenue_display"],
            }
        )

    return rows


def run_shopify_backfill_in_background(*, since: Optional[str], limit: Optional[int]) -> None:
    runtime_name = f"{settings.runtime_name}_shopify_backfill"
    started_at = utcnow()
    update_shopify_backfill_state(
        is_running=True,
        last_started_at=started_at,
        last_finished_at=None,
        last_since=since,
        last_limit=limit,
        last_summary=None,
        last_error=None,
    )
    try:
        with managed_session() as session:
            summary = backfill_shopify_orders(
                session,
                store_domain=settings.shopify_store_domain,
                api_key=settings.shopify_api_key,
                since=since,
                limit=limit,
                dry_run=False,
                runtime_name=runtime_name,
            )
        update_shopify_backfill_state(
            is_running=False,
            last_finished_at=utcnow(),
            last_summary={
                "fetched": summary.fetched,
                "inserted": summary.inserted,
                "updated": summary.updated,
                "failed": summary.failed,
            },
            last_error=None,
        )
    except Exception as exc:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="shopify.backfill.failed",
                success=False,
                error=str(exc),
                since=since,
                limit=limit,
            )
        )
        update_shopify_backfill_state(
            is_running=False,
            last_finished_at=utcnow(),
            last_error=str(exc),
        )


_tiktok_state_lock = threading.Lock()
_tiktok_state = {
    "last_authorization_at": None,
    "last_callback": None,
    "last_webhook_at": None,
    "last_webhook": None,
    "is_pull_running": False,
    "last_pull_started_at": None,
    "last_pull_finished_at": None,
    "last_pull_at": None,
    "last_pull": None,
    "last_error": None,
}

_background_task_state_lock = threading.Lock()
_background_task_state = {
    "failed_tasks": {},
    "last_failure": None,
    "last_failure_at": None,
}


def read_tiktok_integration_state() -> dict[str, object]:
    with _tiktok_state_lock:
        return dict(_tiktok_state)


def _tiktok_state_to_db_row(state: dict) -> TikTokSyncState:
    import json as _json
    return TikTokSyncState(
        id=1,
        last_authorization_at=state.get("last_authorization_at"),
        last_callback_json=_json.dumps(state.get("last_callback") or {}, default=str),
        last_webhook_at=state.get("last_webhook_at"),
        last_webhook_json=_json.dumps(state.get("last_webhook") or {}, default=str),
        is_pull_running=bool(state.get("is_pull_running", False)),
        last_pull_started_at=state.get("last_pull_started_at"),
        last_pull_finished_at=state.get("last_pull_finished_at"),
        last_pull_at=state.get("last_pull_at"),
        last_pull_json=_json.dumps(state.get("last_pull") or {}, default=str),
        last_error=str(state["last_error"]) if state.get("last_error") is not None else None,
        updated_at=utcnow(),
    )


def _persist_tiktok_state(state: dict) -> None:
    """Fire-and-forget: write the current state dict to the DB singleton row."""
    try:
        with managed_session() as session:
            row = _tiktok_state_to_db_row(state)
            session.merge(row)
            session.commit()
    except Exception:
        pass  # state persistence is best-effort; don't crash the request


def update_tiktok_integration_state(**changes: object) -> dict[str, object]:
    with _tiktok_state_lock:
        _tiktok_state.update(changes)
        snapshot = dict(_tiktok_state)
    _persist_tiktok_state(snapshot)
    return snapshot


def _load_tiktok_state_from_db() -> None:
    """Populate the in-memory state dict from the DB on startup."""
    import json as _json
    try:
        with managed_session() as session:
            row = session.get(TikTokSyncState, 1)
        if row is None:
            return
        with _tiktok_state_lock:
            _tiktok_state["last_authorization_at"] = row.last_authorization_at
            _tiktok_state["last_callback"] = _json.loads(row.last_callback_json or "{}")
            _tiktok_state["last_webhook_at"] = row.last_webhook_at
            _tiktok_state["last_webhook"] = _json.loads(row.last_webhook_json or "{}")
            _tiktok_state["is_pull_running"] = False  # always reset on restart
            _tiktok_state["last_pull_started_at"] = row.last_pull_started_at
            _tiktok_state["last_pull_finished_at"] = row.last_pull_finished_at
            _tiktok_state["last_pull_at"] = row.last_pull_at
            _tiktok_state["last_pull"] = _json.loads(row.last_pull_json or "{}")
            _tiktok_state["last_error"] = row.last_error
    except Exception:
        pass  # best-effort; proceed with empty state


def read_background_task_state() -> dict[str, object]:
    with _background_task_state_lock:
        state = dict(_background_task_state)
        failed_tasks = state.get("failed_tasks")
        if isinstance(failed_tasks, dict):
            state["failed_tasks"] = dict(failed_tasks)
        return state


def _background_task_alert_messages(state: Optional[dict[str, object]] = None) -> list[str]:
    task_state = state if state is not None else read_background_task_state()
    failed_tasks = task_state.get("failed_tasks")
    if not isinstance(failed_tasks, dict) or not failed_tasks:
        return []
    alerts: list[str] = []
    for task_name, failure in sorted(failed_tasks.items(), key=lambda item: str(item[0]).lower()):
        error_message = ""
        if isinstance(failure, dict):
            error_message = str(failure.get("error") or "").strip()
        error_label = error_message or "unknown error"
        task_label = str(task_name).replace("_", " ").replace("-", " ").strip() or "background task"
        alerts.append(f"{task_label} failed: {error_label}")
    return alerts


def reset_background_task_failures() -> None:
    with _background_task_state_lock:
        _background_task_state.update(
            {
                "failed_tasks": {},
                "last_failure": None,
                "last_failure_at": None,
            }
        )


def record_background_task_failure(
    *,
    task_name: str,
    runtime_name: str,
    error_message: str,
) -> dict[str, object]:
    failure = {
        "task_name": task_name,
        "runtime_name": runtime_name,
        "error": error_message,
        "failed_at": utcnow(),
    }
    with _background_task_state_lock:
        failed_tasks = dict(_background_task_state.get("failed_tasks") or {})
        failed_tasks[task_name] = failure
        _background_task_state.update(
            {
                "failed_tasks": failed_tasks,
                "last_failure": failure,
                "last_failure_at": failure["failed_at"],
            }
        )
        return dict(_background_task_state)


def track_background_task_failure(
    task: asyncio.Task,
    *,
    runtime_name: str,
    task_name: str,
    stop_event: asyncio.Event,
) -> asyncio.Task:
    def _handle_completion(done_task: asyncio.Task) -> None:
        if done_task.cancelled():
            return
        try:
            done_task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            record_background_task_failure(
                task_name=task_name,
                runtime_name=runtime_name,
                error_message=str(exc),
            )
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="runtime.background_task.failed",
                    success=False,
                    task_name=task_name,
                    error=str(exc),
                )
            )
            return
        if stop_event.is_set():
            return
        error_message = "background task exited unexpectedly"
        record_background_task_failure(
            task_name=task_name,
            runtime_name=runtime_name,
            error_message=error_message,
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="runtime.background_task.failed",
                success=False,
                task_name=task_name,
                error=error_message,
            )
        )

    task.add_done_callback(_handle_completion)
    return task


def track_background_task(
    task: asyncio.Task,
    *,
    runtime_name: str,
    task_name: str,
    stop_event: asyncio.Event,
) -> asyncio.Task:
    return track_background_task_failure(
        task,
        runtime_name=runtime_name,
        task_name=task_name,
        stop_event=stop_event,
    )


def _normalize_optional_query_message(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return ""


def summarize_tiktok_query_params(params: dict[str, str]) -> dict[str, str]:
    allowed_keys = ("app_key", "code", "locale", "shop_region", "shop_id", "state")
    summary: dict[str, str] = {}
    for key in allowed_keys:
        value = (params.get(key) or "").strip()
        if value:
            summary[key] = value if key != "code" else f"{value[:8]}..."
    return summary


def summarize_tiktok_payload(payload: object) -> dict[str, object]:
    if isinstance(payload, dict):
        return {
            "kind": "object",
            "keys": sorted(str(key) for key in payload.keys()),
        }
    if isinstance(payload, list):
        return {
            "kind": "list",
            "length": len(payload),
        }
    return {
        "kind": type(payload).__name__,
    }


def get_latest_tiktok_auth_row(session: Session) -> Optional[TikTokAuth]:
    stmt = select(TikTokAuth).order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())
    return session.exec(stmt).first()


def ensure_tiktok_auth_row(session: Session) -> Optional[TikTokAuth]:
    auth_row = get_latest_tiktok_auth_row(session)
    configured_shop_id = (settings.tiktok_shop_id or "").strip()
    configured_shop_cipher = (settings.tiktok_shop_cipher or "").strip()
    configured_access_token = (settings.tiktok_access_token or "").strip()
    configured_refresh_token = (settings.tiktok_refresh_token or "").strip()
    configured_app_key = (settings.tiktok_app_key or "").strip()
    configured_redirect_uri = (settings.tiktok_redirect_uri or "").strip()

    has_configured_identity = bool(configured_shop_id or configured_shop_cipher)
    has_configured_tokens = bool(configured_access_token or configured_refresh_token)
    if not has_configured_identity or not has_configured_tokens or not configured_app_key:
        return auth_row

    # Once we have a persisted auth row, it becomes the source of truth.
    # Configured tokens are only for first-install bootstrap or manual recovery.
    if auth_row is not None:
        return auth_row

    received_at = utcnow()
    sync_state = read_tiktok_integration_state()
    last_callback = sync_state.get("last_callback")
    shop_region = None
    if isinstance(last_callback, dict):
        callback_query = last_callback.get("query")
        if isinstance(callback_query, dict):
            shop_region = str(callback_query.get("shop_region") or "").strip() or None
    upsert_tiktok_auth_from_callback(
        session,
        TikTokAuth,
        token_result={
            "access_token": configured_access_token or None,
            "refresh_token": configured_refresh_token or None,
            "shop_id": configured_shop_id or None,
            "shop_cipher": configured_shop_cipher or None,
            "shop_region": shop_region,
        },
        app_key=configured_app_key,
        redirect_uri=configured_redirect_uri,
        fallback_shop_id=configured_shop_id or None,
        source="configured_env",
        received_at=received_at,
        dry_run=False,
    )
    session.commit()
    update_tiktok_integration_state(
        last_authorization_at=received_at,
        last_error=None,
    )
    return get_latest_tiktok_auth_row(session)


def _resolve_tiktok_pull_credentials(auth_row: Optional[TikTokAuth]) -> tuple[str, str, str]:
    auth_shop_id = (auth_row.tiktok_shop_id if auth_row else "") or ""
    shop_id = auth_shop_id.strip()
    if not shop_id or shop_id.startswith("pending:"):
        shop_id = (settings.tiktok_shop_id or "").strip()

    shop_cipher = ((auth_row.shop_cipher if auth_row else "") or "").strip()
    if not shop_cipher:
        shop_cipher = (settings.tiktok_shop_cipher or "").strip()

    access_token = ((auth_row.access_token if auth_row else "") or "").strip()
    if not access_token:
        access_token = (settings.tiktok_access_token or "").strip()

    return shop_id, shop_cipher, access_token


def describe_tiktok_sync_status(auth_row: Optional[TikTokAuth], sync_state: dict[str, object]) -> dict[str, object]:
    last_pull = sync_state.get("last_pull")
    pull_status = "idle"
    if isinstance(last_pull, dict):
        pull_status = str(last_pull.get("status") or "idle").strip() or "idle"
    is_running = bool(sync_state.get("is_pull_running"))

    has_tokens = bool(
        (auth_row and ((auth_row.access_token or "").strip() or (auth_row.refresh_token or "").strip()))
        or (settings.tiktok_access_token or "").strip()
        or (settings.tiktok_refresh_token or "").strip()
    )
    shop_key = (settings.tiktok_shop_id or "").strip() or (auth_row.tiktok_shop_id if auth_row else "")
    shop_cipher = (settings.tiktok_shop_cipher or "").strip() or (auth_row.shop_cipher if auth_row else "")
    resolved_identifier = shop_key or shop_cipher or (auth_row.seller_id if auth_row else "") or (auth_row.open_id if auth_row else "")

    if auth_row and has_tokens:
        status_label = "Connected"
        status_tone = "ok"
    elif auth_row:
        status_label = "Pending token refresh"
        status_tone = "warn"
    else:
        status_label = "Not connected"
        status_tone = "bad"

    if is_running:
        sync_label = "Sync running"
    elif pull_status in {"success"}:
        sync_label = "Sync healthy"
    elif pull_status in {"waiting", "skipped"}:
        sync_label = "Waiting for identifiers"
    elif pull_status in {"failed", "error"}:
        sync_label = "Sync error"
    else:
        sync_label = "Idle"

    return {
        "status_label": status_label,
        "status_tone": status_tone,
        "sync_label": sync_label,
        "sync_status": "running" if is_running else pull_status,
        "shop_key": resolved_identifier or "unknown",
        "has_tokens": has_tokens,
        "is_running": is_running,
        "last_pull_started_at": sync_state.get("last_pull_started_at"),
        "last_pull_finished_at": sync_state.get("last_pull_finished_at"),
        "last_pull": last_pull if isinstance(last_pull, dict) else {},
    }


def _format_tiktok_status_timestamp(value: object) -> str:
    if value in (None, ""):
        return "none yet"
    if isinstance(value, datetime):
        return format_pacific_datetime(value)
    if isinstance(value, str):
        raw_value = value.strip()
        if not raw_value:
            return "none yet"
        try:
            parsed_value = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return raw_value
        return format_pacific_datetime(parsed_value)
    return str(value)


def build_tiktok_status_snapshot(session: Session) -> dict[str, object]:
    auth_row = get_latest_tiktok_auth_row(session)
    integration_state = read_tiktok_integration_state()
    sync_snapshot = describe_tiktok_sync_status(auth_row, integration_state)
    last_callback = integration_state.get("last_callback")
    if not isinstance(last_callback, dict):
        last_callback = {}
    last_webhook = integration_state.get("last_webhook")
    if not isinstance(last_webhook, dict):
        last_webhook = {}
    last_pull = sync_snapshot.get("last_pull") if isinstance(sync_snapshot.get("last_pull"), dict) else {}
    last_pull_started = sync_snapshot.get("last_pull_started_at")
    last_pull_finished = sync_snapshot.get("last_pull_finished_at")
    return {
        "status_label": sync_snapshot["status_label"],
        "status_tone": sync_snapshot["status_tone"],
        "sync_label": sync_snapshot["sync_label"],
        "sync_status": sync_snapshot["sync_status"],
        "shop_key": sync_snapshot["shop_key"],
        "has_tokens": sync_snapshot["has_tokens"],
        "is_running": sync_snapshot["is_running"],
        "last_authorization_label": _format_tiktok_status_timestamp(
            integration_state.get("last_authorization_at")
            or (auth_row.updated_at if auth_row and getattr(auth_row, "updated_at", None) else None)
        ),
        "last_callback_label": _format_tiktok_status_timestamp(last_callback.get("received_at")),
        "last_callback": last_callback,
        "last_webhook_label": _format_tiktok_status_timestamp(integration_state.get("last_webhook_at")),
        "last_webhook": last_webhook,
        "last_pull_started_label": _format_tiktok_status_timestamp(last_pull_started),
        "last_pull_finished_label": _format_tiktok_status_timestamp(last_pull_finished),
        "last_pull": last_pull,
        "last_error": integration_state.get("last_error") or "",
    }


def resolve_tiktok_shop_pull_base_url() -> str:
    explicit_shop_api_base = (settings.tiktok_shop_api_base_url or "").strip()
    if explicit_shop_api_base:
        return explicit_shop_api_base
    generic_base = (settings.tiktok_api_base_url or "").strip()
    if generic_base and "open-api" in generic_base:
        return generic_base
    return "https://open-api.tiktokglobalshop.com"


def _refresh_tiktok_auth_if_needed(
    session: Session,
    *,
    runtime_name: str,
    force: bool = False,
) -> Optional[dict[str, object]]:
    return _refresh_tiktok_auth_fn(
        session,
        runtime_name=runtime_name,
        force=force,
        resolve_base_url=resolve_tiktok_shop_pull_base_url,
        update_state=update_tiktok_integration_state,
    )


def run_tiktok_pull_cycle(
    *,
    runtime_name: str,
    limit: Optional[int] = None,
    lookback_hours: Optional[float] = None,
    since: Optional[str] = None,
    trigger: str = "automatic",
) -> dict[str, object]:
    if pull_tiktok_orders is None:
        return {"status": "disabled", "reason": "TikTok backfill helper unavailable"}
    if not settings.tiktok_sync_enabled and trigger == "automatic":
        return {"status": "disabled", "reason": "TikTok automatic sync disabled"}
    if not (settings.tiktok_app_key or "").strip() or not (settings.tiktok_app_secret or "").strip():
        return {"status": "disabled", "reason": "TikTok app credentials are missing"}

    started_at = utcnow()
    update_tiktok_integration_state(
        is_pull_running=True,
        last_pull_started_at=started_at,
        last_pull_finished_at=None,
        last_error=None,
        last_pull={
            "status": "running",
            "runtime": runtime_name,
            "trigger": trigger,
        },
    )

    try:
      with managed_session() as session:
        auth_row = ensure_tiktok_auth_row(session)
        if auth_row is None and not ((settings.tiktok_shop_id or "").strip() and (settings.tiktok_access_token or "").strip()):
            result = {"status": "waiting", "reason": "TikTok auth has not been captured yet", "trigger": trigger}
            update_tiktok_integration_state(
                is_pull_running=False,
                last_pull_finished_at=utcnow(),
                last_pull_at=utcnow(),
                last_pull={**result, "runtime": runtime_name},
            )
            return result

        refresh_result = _refresh_tiktok_auth_if_needed(session, runtime_name=runtime_name)
        if refresh_result is not None:
            auth_row = ensure_tiktok_auth_row(session)

        shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
        if not shop_id and not shop_cipher:
            result = {
                "status": "waiting",
                "reason": "missing shop identifier",
                "runtime": runtime_name,
                "trigger": trigger,
            }
            update_tiktok_integration_state(
                is_pull_running=False,
                last_pull_finished_at=utcnow(),
                last_pull_at=utcnow(),
                last_pull=result,
            )
            return result
        if not access_token:
            result = {
                "status": "waiting",
                "reason": "missing access token",
                "runtime": runtime_name,
                "shop_id": shop_id,
                "trigger": trigger,
            }
            update_tiktok_integration_state(
                is_pull_running=False,
                last_pull_finished_at=utcnow(),
                last_pull_at=utcnow(),
                last_pull=result,
            )
            return result

        since_dt = parse_report_datetime(since)
        if since_dt is None:
            since_dt = utcnow() - timedelta(hours=max(float(lookback_hours or settings.tiktok_sync_lookback_hours or 0.0), 1.0))
        # Gap detection: if the newest order in DB is older than our lookback window,
        # extend since_dt back to cover the gap (capped at startup_backfill_days).
        if trigger not in ("startup", "manual"):
            gap_floor = utcnow() - timedelta(days=max(int(settings.tiktok_startup_backfill_days or 1), 1))
            newest_created = session.exec(select(func.max(TikTokOrder.created_at))).one()
            if newest_created is not None:
                if newest_created.tzinfo is None:
                    newest_created = newest_created.replace(tzinfo=timezone.utc)
            if newest_created is not None and newest_created < since_dt:
                gap_since = max(newest_created - timedelta(hours=1), gap_floor)
                if gap_since < since_dt:
                    since_dt = gap_since
        safe_limit = max(int(limit or settings.tiktok_sync_limit or 0), 1)
        def _run_pull_with_current_credentials(current_access_token: str):
            return pull_tiktok_orders(
                session,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=(settings.tiktok_app_key or "").strip(),
                app_secret=(settings.tiktok_app_secret or "").strip(),
                access_token=current_access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                since=since_dt,
                limit=safe_limit,
                dry_run=False,
                runtime_name=runtime_name,
            )

        try:
            summary = _run_pull_with_current_credentials(access_token)
        except httpx.HTTPStatusError as exc:
            if exc.response is None or exc.response.status_code != 401:
                raise
            refresh_result = _refresh_tiktok_auth_if_needed(
                session,
                runtime_name=f"{runtime_name}_401_refresh",
                force=True,
            )
            if refresh_result is None:
                raise
            auth_row = ensure_tiktok_auth_row(session)
            shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
            summary = _run_pull_with_current_credentials(access_token)
        last_pull = {
            "status": "success",
            "runtime": runtime_name,
            "shop_id": shop_id or None,
            "shop_cipher": shop_cipher or None,
            "trigger": trigger,
            "since": since_dt.isoformat(),
            "limit": safe_limit,
            "fetched": summary.fetched,
            "inserted": summary.inserted,
            "updated": summary.updated,
            "failed": summary.failed,
            "detail_calls": summary.detail_calls,
        }
        update_tiktok_integration_state(
            is_pull_running=False,
            last_pull_finished_at=utcnow(),
            last_pull_at=utcnow(),
            last_pull=last_pull,
            last_error=None,
        )
        session.commit()
        return last_pull
    except Exception:
        update_tiktok_integration_state(
            is_pull_running=False,
            last_pull_finished_at=utcnow(),
            last_pull_at=utcnow(),
            last_pull={"status": "failed", "runtime": runtime_name, "trigger": trigger},
            last_error="pull cycle crashed",
        )
        raise


async def periodic_tiktok_pull_loop(stop_event: asyncio.Event) -> None:
    runtime_name = f"{settings.runtime_name}_tiktok_pull"
    if not settings.tiktok_sync_enabled:
        print("[tiktok] periodic pull loop disabled by configuration")
        return
    if pull_tiktok_orders is None:
        print("[tiktok] periodic pull loop disabled because backfill helper could not be imported")
        return
    if not ((settings.tiktok_app_key or "").strip() and (settings.tiktok_app_secret or "").strip()):
        print("[tiktok] periodic pull loop disabled because TikTok credentials are missing")
        return

    interval_seconds = max(int(settings.tiktok_sync_interval_minutes or 0), 1) * 60
    while not stop_event.is_set():
        try:
            result = await asyncio.to_thread(
                run_tiktok_pull_cycle,
                runtime_name=runtime_name,
                limit=settings.tiktok_sync_limit,
                lookback_hours=settings.tiktok_sync_lookback_hours,
                trigger="automatic",
            )
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.pull.cycle",
                    success=result.get("status") == "success",
                    status=str(result.get("status") or "unknown"),
                    shop_id=result.get("shop_id"),
                    reason=result.get("reason"),
                    fetched=result.get("fetched"),
                    inserted=result.get("inserted"),
                    updated=result.get("updated"),
                    failed=result.get("failed"),
                    detail_calls=result.get("detail_calls"),
                )
            )
        except Exception as exc:
            update_tiktok_integration_state(
                last_pull_at=utcnow(),
                last_pull={"status": "failed", "runtime": runtime_name, "error": str(exc)},
                last_error=str(exc),
            )
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.pull.failed",
                    success=False,
                    error=str(exc),
                )
            )

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue


async def tiktok_startup_backfill(stop_event: asyncio.Event) -> None:
    """On startup, pull up to tiktok_startup_backfill_days of history to fill any gaps."""
    runtime_name = f"{settings.runtime_name}_tiktok_startup"
    if pull_tiktok_orders is None or not settings.tiktok_sync_enabled:
        return
    if not ((settings.tiktok_app_key or "").strip() and (settings.tiktok_app_secret or "").strip()):
        return
    backfill_days = max(int(settings.tiktok_startup_backfill_days or 1), 1)
    # Small delay so auth bootstrap and DB init complete first.
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=8.0)
    except asyncio.TimeoutError:
        pass
    if stop_event.is_set():
        return
    full_backfill_floor = utcnow() - timedelta(days=backfill_days)
    since_dt = full_backfill_floor
    with managed_session() as session:
        newest_created = session.exec(select(func.max(TikTokOrder.created_at))).one()
        if newest_created is not None:
            if newest_created.tzinfo is None:
                newest_created = newest_created.replace(tzinfo=timezone.utc)
            since_dt = max(newest_created - timedelta(hours=2), full_backfill_floor)
    print(
        structured_log_line(
            runtime=runtime_name,
            action="tiktok.startup_backfill.start",
            since=since_dt.isoformat(),
            backfill_days=backfill_days,
        )
    )
    try:
        result = await asyncio.to_thread(
            run_tiktok_pull_cycle,
            runtime_name=runtime_name,
            since=since_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            limit=5000,
            trigger="startup",
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.startup_backfill.complete",
                success=result.get("status") == "success",
                status=str(result.get("status") or "unknown"),
                fetched=result.get("fetched"),
                inserted=result.get("inserted"),
                updated=result.get("updated"),
                failed=result.get("failed"),
            )
        )
    except Exception as exc:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.startup_backfill.failed",
                success=False,
                error=str(exc),
            )
        )


def _enrich_tiktok_order_from_api(order_id: str) -> None:
    """Fetch full order details from TikTok API and update the DB record."""
    if _fetch_tiktok_order_details is None or _order_record_from_payload is None:
        return
    runtime_name = f"{settings.runtime_name}_tiktok_webhook_enrich"
    try:
        with managed_session() as session:
            auth_row = get_latest_tiktok_auth_row(session)
            if auth_row is None:
                return
            shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
            if not access_token or (not shop_id and not shop_cipher):
                return

            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                details = _fetch_tiktok_order_details(
                    client,
                    base_url=resolve_tiktok_shop_pull_base_url(),
                    app_key=(settings.tiktok_app_key or "").strip(),
                    app_secret=(settings.tiktok_app_secret or "").strip(),
                    access_token=access_token,
                    shop_id=shop_id,
                    shop_cipher=shop_cipher,
                    order_ids=[order_id],
                )

            if not details:
                return
            record = _order_record_from_payload(
                details[0],
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                source="webhook_enriched",
            )
            from .tiktok_ingest import upsert_tiktok_order
            upsert_tiktok_order(session, TikTokOrder, record)
            _enrich_delay = 0.4
            for _enrich_attempt in range(4):
                try:
                    session.commit()
                    break
                except Exception as _enrich_exc:
                    if is_sqlite_lock_error(_enrich_exc) and _enrich_attempt < 3:
                        time.sleep(_enrich_delay)
                        _enrich_delay *= 2
                        continue
                    raise
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.webhook.order_enriched",
                    success=True,
                    tiktok_order_id=order_id,
                )
            )
    except Exception as exc:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.order_enrich_failed",
                success=False,
                error=str(exc),
                tiktok_order_id=order_id,
            )
            )


def _start_tiktok_webhook_enrichment(order_id: str) -> None:
    if not order_id or _fetch_tiktok_order_details is None:
        return
    threading.Thread(
        target=_enrich_tiktok_order_from_api,
        args=(order_id,),
        daemon=True,
        name=f"tiktok-enrich-{order_id[:12]}",
    ).start()


def run_tiktok_pull_in_background(*, since: Optional[str], limit: Optional[int], trigger: str = "manual") -> None:
    runtime_name = f"{settings.runtime_name}_tiktok_manual"
    try:
        result = run_tiktok_pull_cycle(
            runtime_name=runtime_name,
            since=since,
            limit=limit,
            lookback_hours=settings.tiktok_sync_lookback_hours,
            trigger=trigger,
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.pull.background_complete",
                success=result.get("status") == "success",
                status=str(result.get("status") or "unknown"),
                trigger=trigger,
                shop_id=result.get("shop_id"),
                reason=result.get("reason"),
                fetched=result.get("fetched"),
                inserted=result.get("inserted"),
                updated=result.get("updated"),
                failed=result.get("failed"),
            )
        )
    except Exception as exc:
        update_tiktok_integration_state(
            is_pull_running=False,
            last_pull_finished_at=utcnow(),
            last_pull_at=utcnow(),
            last_pull={"status": "failed", "runtime": runtime_name, "trigger": trigger, "error": str(exc)},
            last_error=str(exc),
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.pull.background_failed",
                success=False,
                error=str(exc),
                trigger=trigger,
            )
        )


def local_runtime_details() -> dict:
    background_task_alerts = _background_task_alert_messages()
    discord_status = discord_runtime_state.get("status")
    discord_error = discord_runtime_state.get("error")
    if background_task_alerts:
        discord_status = "degraded"
        discord_error = background_task_alerts[0]
    return {
        "discord_status": discord_status,
        "discord_error": discord_error,
        "parser_worker_enabled": settings.parser_worker_enabled,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
        "periodic_attachment_repair_enabled": settings.periodic_attachment_repair_enabled,
        "periodic_attachment_repair_interval_minutes": settings.periodic_attachment_repair_interval_minutes,
        "periodic_attachment_repair_lookback_hours": settings.periodic_attachment_repair_lookback_hours,
        "periodic_attachment_repair_limit": settings.periodic_attachment_repair_limit,
        "periodic_attachment_repair_min_age_minutes": settings.periodic_attachment_repair_min_age_minutes,
        "periodic_stitch_audit_enabled": settings.periodic_stitch_audit_enabled,
        "periodic_stitch_audit_interval_minutes": settings.periodic_stitch_audit_interval_minutes,
        "tiktok_sync_enabled": settings.tiktok_sync_enabled,
        "tiktok_sync_interval_minutes": settings.tiktok_sync_interval_minutes,
        "tiktok_sync_lookback_hours": settings.tiktok_sync_lookback_hours,
        "tiktok_sync_limit": settings.tiktok_sync_limit,
        "backfill_queue_expected": settings.discord_ingest_enabled,
        "last_recent_audit_at": discord_runtime_state.get("last_recent_audit_at"),
        "last_recent_audit_summary": discord_runtime_state.get("last_recent_audit_summary"),
        "last_attachment_repair_at": discord_runtime_state.get("last_attachment_repair_at"),
        "last_attachment_repair_summary": discord_runtime_state.get("last_attachment_repair_summary"),
        "background_task_alerts": background_task_alerts,
        "background_task_failure_count": len(background_task_alerts),
    }


def app_runtime_details() -> dict:
    background_task_alerts = _background_task_alert_messages()
    return {
        "service_mode": "web-app",
        "discord_status": "degraded" if background_task_alerts else "running",
        "discord_error": background_task_alerts[0] if background_task_alerts else "",
        "parser_worker_enabled": settings.parser_worker_enabled,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
        "periodic_stitch_audit_enabled": settings.periodic_stitch_audit_enabled,
        "tiktok_sync_enabled": settings.tiktok_sync_enabled,
        "tiktok_sync_interval_minutes": settings.tiktok_sync_interval_minutes,
        "last_tiktok_pull_at": read_tiktok_integration_state().get("last_pull_at"),
        "last_tiktok_pull": read_tiktok_integration_state().get("last_pull"),
        "background_task_alerts": background_task_alerts,
        "background_task_failure_count": len(background_task_alerts),
    }


def row_looks_transactional(row: DiscordMessage) -> bool:
    if row.amount is not None:
        return True
    if row.deal_type in {"sell", "buy", "trade"}:
        return True
    content = (row.content or "").lower()
    return any(token in content for token in ["sold", "sell", "bought", "buy", "cash", "zelle", "venmo", "paypal", "trade", "$"])


def find_nearby_image_candidates(session: Session, rows: list[DiscordMessage]) -> dict[int, dict]:
    target_cached_assets_by_message_id = get_cached_attachment_map(
        session,
        [row.id for row in rows if row.id is not None],
    )
    targets = [
        row for row in rows
        if row.id is not None
        and not row.is_deleted
        and not row.stitched_group_id
        and row.channel_id
        and row.author_name
        and not row_has_images(
            row,
            cached_assets=target_cached_assets_by_message_id.get(row.id) if row.id is not None else None,
        )
        and row_looks_transactional(row)
    ]
    if not targets:
        return {}

    channel_ids = sorted({row.channel_id for row in targets if row.channel_id})
    author_names = sorted({row.author_name for row in targets if row.author_name})
    min_created = min(row.created_at for row in targets) - timedelta(seconds=NEARBY_IMAGE_AUDIT_WINDOW_SECONDS)
    max_created = max(row.created_at for row in targets) + timedelta(seconds=NEARBY_IMAGE_AUDIT_WINDOW_SECONDS)

    candidate_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id.in_(channel_ids))
        .where(DiscordMessage.author_name.in_(author_names))
        .where(DiscordMessage.created_at >= min_created)
        .where(DiscordMessage.created_at <= max_created)
    ).all()
    cached_assets_by_message_id = get_cached_attachment_map(
        session,
        [candidate.id for candidate in candidate_rows if candidate.id is not None],
    )

    results: dict[int, dict] = {}
    for row in targets:
        best_match: Optional[dict] = None
        for candidate in candidate_rows:
            if candidate.id == row.id or candidate.is_deleted:
                continue
            if candidate.channel_id != row.channel_id or candidate.author_name != row.author_name:
                continue
            if candidate.stitched_group_id:
                continue
            cached_assets = cached_assets_by_message_id.get(candidate.id)
            _, candidate_images = normalize_attachment_urls_for_row(candidate, cached_assets)
            if not candidate_images:
                continue
            delta_seconds = abs((candidate.created_at - row.created_at).total_seconds())
            if delta_seconds > NEARBY_IMAGE_AUDIT_WINDOW_SECONDS:
                continue
            if best_match is None or delta_seconds < best_match["delta_seconds"]:
                best_match = {
                    "message_id": candidate.id,
                    "image_url": candidate_images[0],
                    "image_urls": candidate_images,
                    "time": format_pacific_datetime(candidate.created_at),
                    "message": (candidate.content or "").strip(),
                    "delta_seconds": int(delta_seconds),
                }
        if best_match:
            results[row.id] = best_match

    return results


def build_message_stmt(
    *,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
):
    normalized_status = normalize_status_filter(status)
    after_dt = parse_report_datetime(after)
    before_dt = parse_report_datetime(before, end_of_day=True)
    stmt = select(DiscordMessage)

    if normalized_status:
        if normalized_status == "review_queue":
            stmt = stmt.where(
                DiscordMessage.parse_status.in_(
                    [*status_filter_values(PARSE_REVIEW_REQUIRED), PARSE_FAILED]
                )
            )
        else:
            stmt = stmt.where(DiscordMessage.parse_status.in_(status_filter_values(normalized_status)))
    else:
        stmt = stmt.where(DiscordMessage.parse_status.not_in(status_filter_values(PARSE_IGNORED)))

    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)

    if entry_kind:
        stmt = stmt.where(DiscordMessage.entry_kind == entry_kind)

    if expense_category:
        stmt = stmt.where(DiscordMessage.expense_category == expense_category)

    if after_dt:
        stmt = stmt.where(DiscordMessage.created_at >= after_dt)

    if before_dt:
        stmt = stmt.where(DiscordMessage.created_at <= before_dt)

    return stmt

def message_list_item(row: DiscordMessage) -> dict:
    attachment_urls = json.loads(row.attachment_urls_json or "[]")
    item_names = json.loads(row.item_names_json or "[]")
    items_in = json.loads(row.items_in_json or "[]")
    items_out = json.loads(row.items_out_json or "[]")
    stitched_ids = json.loads(row.stitched_message_ids_json or "[]")

    image_urls = extract_image_urls(attachment_urls)

    amount_display = ""
    payment_display = row.payment_method or ""
    if row.amount is not None:
        if row.deal_type == "trade":
            if row.cash_direction == "to_store":
                amount_display = f"In ${row.amount:g}"
            elif row.cash_direction == "from_store":
                amount_display = f"Out ${row.amount:g}"
            elif row.cash_direction == "none":
                amount_display = f"${row.amount:g} (no cash flow)"
            else:
                amount_display = f"${row.amount:g} (direction unclear)"
        else:
            amount_display = f"${row.amount:g}"

    if row.deal_type == "trade" and payment_display:
        if row.cash_direction == "to_store":
            payment_display = f"{payment_display} (to store)"
        elif row.cash_direction == "from_store":
            payment_display = f"{payment_display} (from store)"

    return {
        "id": row.id,
        "time": format_pacific_datetime(row.created_at),
        "date": format_pacific_date(row.created_at),
        "edited_at": format_pacific_datetime(row.edited_at),
        "last_seen_at": format_pacific_datetime(row.last_seen_at),
        "last_stitched_at": format_pacific_datetime(row.last_stitched_at),
        "deleted_at": format_pacific_datetime(row.deleted_at),
        "is_deleted": row.is_deleted,
        "channel": row.channel_name,
        "channel_id": row.channel_id,
        "author": row.author_name,
        "message": row.content,
        "status": normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review),
        "type": row.deal_type,
        "amount": row.amount,
        "amount_display": amount_display,
        "payment": row.payment_method,
        "payment_display": payment_display,
        "cash_direction": row.cash_direction,
        "category": row.category,
        "items": item_names,
        "items_in": items_in,
        "items_out": items_out,
        "trade_summary": row.trade_summary,
        "confidence": row.confidence,
        "needs_review": row.needs_review,
        "notes": row.notes,
        "entry_kind": row.entry_kind,
        "money_in": row.money_in,
        "money_out": row.money_out,
        "expense_category": row.expense_category,
        "reviewed_by": row.reviewed_by,
        "reviewed_at": format_pacific_datetime(row.reviewed_at),
        "last_error": row.last_error,
        "has_images": len(image_urls) > 0,
        "image_urls": image_urls,
        "first_image_url": image_urls[0] if image_urls else None,
        "first_thumb_url": None,
        "parse_attempts": row.parse_attempts,
        "stitched_group_id": row.stitched_group_id,
        "stitched_primary": row.stitched_primary,
        "stitched_message_ids": stitched_ids,
        "stitched_count": len(stitched_ids),
    }


def build_message_list_items(
    session: Session,
    rows: list[DiscordMessage],
    *,
    expense_category: Optional[str] = None,
) -> list[dict]:
    items = [message_list_item(row) for row in rows]
    row_by_id = {row.id: row for row in rows if row.id is not None}
    nearby_image_candidates = find_nearby_image_candidates(session, rows)
    bookkeeping_status_by_message_id = get_bookkeeping_status_by_message_ids(
        session,
        [item["id"] for item in items if item.get("id") is not None],
    )
    learning_signals_by_message = get_learning_signals(
        session,
        [item["message"] or "" for item in items],
    )

    grouped_ids: set[int] = set()
    for item in items:
        grouped_ids.update(item["stitched_message_ids"])

    grouped_rows_by_id: dict[int, DiscordMessage] = {}
    if grouped_ids:
        grouped_rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.id.in_(grouped_ids))
        ).all()
        grouped_rows_by_id = {row.id: row for row in grouped_rows}

    attachment_message_ids = [row.id for row in rows if row.id is not None]
    attachment_message_ids.extend(grouped_id for grouped_id in grouped_ids if grouped_id is not None)
    cached_assets_by_message_id = get_cached_attachment_map(session, attachment_message_ids)

    stale_processing_ids: set[int] = set()
    processing_ids = [row.id for row in rows if row.id is not None and row.parse_status == PARSE_PROCESSING]
    if processing_ids:
        unfinished_attempts = session.exec(
            select(ParseAttempt).where(
                ParseAttempt.message_id.in_(processing_ids),
                ParseAttempt.finished_at == None,  # noqa: E711
            )
        ).all()
        now = utcnow()
        for attempt in unfinished_attempts:
            if attempt.message_id is None or attempt.started_at is None:
                continue
            started_at = attempt.started_at
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            if started_at <= now - STALE_PROCESSING_AFTER:
                stale_processing_ids.add(attempt.message_id)

    for item in items:
        grouped_messages = []
        grouped_attachment_urls: list[str] = []
        grouped_image_urls: list[str] = []
        for grouped_id in item["stitched_message_ids"]:
            grouped_row = grouped_rows_by_id.get(grouped_id)
            if not grouped_row:
                continue
            grouped_assets = cached_assets_by_message_id.get(grouped_row.id)
            normalized_grouped_attachment_urls, normalized_grouped_image_urls = normalize_attachment_urls_for_row(
                grouped_row,
                grouped_assets,
            )
            grouped_messages.append(
                {
                    "id": grouped_row.id,
                    "time": format_pacific_datetime(grouped_row.created_at),
                    "author": grouped_row.author_name or "",
                    "message": (grouped_row.content or "").strip(),
                    "is_self": grouped_row.id == item["id"],
                    "has_image": bool(normalized_grouped_image_urls),
                    "attachment_urls": normalized_grouped_attachment_urls,
                    "image_urls": normalized_grouped_image_urls,
                    "first_image_url": normalized_grouped_image_urls[0] if normalized_grouped_image_urls else None,
                }
            )
            for url in normalized_grouped_attachment_urls:
                if url not in grouped_attachment_urls:
                    grouped_attachment_urls.append(url)
            for url in normalized_grouped_image_urls:
                if url not in grouped_image_urls:
                    grouped_image_urls.append(url)
        item["grouped_messages"] = grouped_messages
        cached_assets = cached_assets_by_message_id.get(item["id"])
        if cached_assets:
            item["attachment_urls"] = cached_assets["all_urls"]
            item["image_urls"] = cached_assets["image_urls"]
            item["first_image_url"] = cached_assets["image_urls"][0] if cached_assets["image_urls"] else None
            item["has_images"] = bool(cached_assets["image_urls"])
        elif item["id"] is not None:
            original_urls = list(item.get("attachment_urls") or [])
            proxy_urls = [
                f"/messages/{item['id']}/attachments/{index}"
                for index, _url in enumerate(original_urls)
            ]
            image_proxy_urls = [
                proxy_urls[index]
                for index, url in enumerate(original_urls)
                if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
            ]
            item["attachment_urls"] = proxy_urls
            item["image_urls"] = image_proxy_urls
            item["first_image_url"] = image_proxy_urls[0] if image_proxy_urls else None
            item["has_images"] = bool(image_proxy_urls)
        item = apply_cached_or_proxy_attachment_urls(
            session,
            item,
            extra_attachment_groups=[grouped_attachment_urls] if grouped_attachment_urls else None,
            extra_image_groups=[grouped_image_urls] if grouped_image_urls else None,
            prefetched_assets=cached_assets_by_message_id,
        )
        item["bookkeeping"] = bookkeeping_status_by_message_id.get(
            item["id"],
            {"status": "unmatched", "label": "Unmatched", "sheet_name": ""},
        )
        item["learning"] = learning_signals_by_message.get(
            item["message"] or "",
            {"exact_match": False, "promoted_rule": False, "similar_count": 0},
        )
        item["nearby_image"] = nearby_image_candidates.get(item["id"])
        item["possible_missing_image"] = item["nearby_image"] is not None
        item["is_stale_processing"] = item["id"] in stale_processing_ids
        item["detail_url"] = build_message_detail_url(
            item["id"],
            return_path="/review-table" if item["needs_review"] or item["status"] in {"needs_review", "failed"} else "/table",
            status="review_queue" if item["needs_review"] or item["status"] in {"needs_review", "failed"} else item["status"],
            channel_id=item["channel_id"],
            expense_category=expense_category,
            after=item["date"],
            before=item["date"],
            sort_by="time",
            sort_dir="desc",
            limit=100,
        ) if item.get("id") is not None else ""

        source_row = row_by_id.get(item["id"])
        item["action_links"] = build_row_action_links(
            item["id"],
            channel_id=item["channel_id"],
            created_at=source_row.created_at if source_row else None,
            status=item["status"],
            expense_category=expense_category,
        ) if item.get("id") is not None else {}
        item["attention"] = build_row_attention(item)

    return items


def message_detail_item(row: DiscordMessage) -> dict:
    attachment_urls = json.loads(row.attachment_urls_json or "[]")
    return {
        "id": row.id,
        "discord_message_id": row.discord_message_id,
        "guild_id": row.guild_id,
        "channel_id": row.channel_id,
        "channel_name": row.channel_name,
        "author_id": row.author_id,
        "author_name": row.author_name,
        "content": row.content,
        "attachment_urls": attachment_urls,
        "image_urls": extract_image_urls(attachment_urls),
        "created_at": format_pacific_datetime(row.created_at),
        "ingested_at": format_pacific_datetime(row.ingested_at),
        "last_seen_at": format_pacific_datetime(row.last_seen_at),
        "parse_status": normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review),
        "parse_attempts": row.parse_attempts,
        "last_error": row.last_error,
        "deal_type": row.deal_type,
        "amount": row.amount,
        "payment_method": row.payment_method,
        "cash_direction": row.cash_direction,
        "category": row.category,
        "item_names": json.loads(row.item_names_json or "[]"),
        "items_in": json.loads(row.items_in_json or "[]"),
        "items_out": json.loads(row.items_out_json or "[]"),
        "trade_summary": row.trade_summary,
        "notes": row.notes,
        "confidence": row.confidence,
        "needs_review": row.needs_review,
        "image_summary": row.image_summary,
        "reviewed_by": row.reviewed_by,
        "reviewed_at": format_pacific_datetime(row.reviewed_at),
        "edited_at": format_pacific_datetime(row.edited_at),
        "deleted_at": format_pacific_datetime(row.deleted_at),
        "last_stitched_at": format_pacific_datetime(row.last_stitched_at),
        "entry_kind": row.entry_kind,
        "money_in": row.money_in,
        "money_out": row.money_out,
        "expense_category": row.expense_category,
    }


def apply_cached_or_proxy_attachment_urls(
    session: Session,
    item: dict,
    *,
    extra_attachment_groups: list[list[str]] | None = None,
    extra_image_groups: list[list[str]] | None = None,
    prefetched_assets: dict[int, dict[str, list[str]]] | None = None,
) -> dict:
    message_id = item.get("id")
    if message_id is None:
        return item

    if prefetched_assets is not None:
        cached_assets = prefetched_assets.get(message_id)
    else:
        cached_assets = get_cached_attachment_map(
            session,
            [message_id],
        ).get(message_id)
    if cached_assets:
        base_attachment_urls = list(cached_assets["all_urls"])
        base_image_urls = list(cached_assets["image_urls"])
    else:
        original_urls = list(item.get("attachment_urls") or [])
        base_attachment_urls = [
            f"/messages/{message_id}/attachments/{index}"
            for index, _url in enumerate(original_urls)
        ]
        base_image_urls = [
            base_attachment_urls[index]
            for index, url in enumerate(original_urls)
            if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
        ]

    merged_attachment_urls, merged_image_urls = merge_display_attachment_urls(
        base_attachment_urls,
        *(extra_attachment_groups or []),
        image_groups=[base_image_urls, *(extra_image_groups or [])],
    )
    if cached_assets or extra_attachment_groups:
        item["attachment_urls"] = merged_attachment_urls
        item["image_urls"] = merged_image_urls
        item["first_image_url"] = merged_image_urls[0] if merged_image_urls else None
        item["has_images"] = bool(merged_image_urls)
        item["first_thumb_url"] = _thumb_url(item.get("first_image_url"))
        return item

    item["attachment_urls"] = base_attachment_urls
    item["image_urls"] = base_image_urls
    item["first_image_url"] = base_image_urls[0] if base_image_urls else None
    item["has_images"] = bool(base_image_urls)
    item["first_thumb_url"] = _thumb_url(item.get("first_image_url"))
    return item


def _thumb_url(url: str | None) -> str | None:
    if url and url.startswith("/attachments/") and "/thumb" not in url:
        return url + "/thumb"
    return url


def build_return_url(
    return_path: str,
    *,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = None,
    page: Optional[int] = None,
    limit: Optional[int] = None,
) -> str:
    params: dict[str, str] = {}

    if status:
        params["status"] = status
    if channel_id:
        params["channel_id"] = channel_id
    if expense_category:
        params["expense_category"] = expense_category
    if after:
        params["after"] = after
    if before:
        params["before"] = before
    if sort_by:
        params["sort_by"] = sort_by
    if sort_dir:
        params["sort_dir"] = sort_dir
    if page and page > 1:
        params["page"] = str(page)
    if limit:
        params["limit"] = str(limit)

    if not params:
        return return_path
    return f"{return_path}?{urlencode(params)}"


def coerce_int(value: object) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_message_detail_url(
    message_id: Optional[int],
    *,
    return_path: str = "/table",
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = None,
    page: Optional[int] = None,
    limit: Optional[int] = None,
) -> str:
    if message_id is None:
        return ""
    params: dict[str, str] = {"return_path": return_path}
    if status:
        params["status"] = status
    if channel_id:
        params["channel_id"] = channel_id
    if expense_category:
        params["expense_category"] = expense_category
    if after:
        params["after"] = after
    if before:
        params["before"] = before
    if sort_by:
        params["sort_by"] = sort_by
    if sort_dir:
        params["sort_dir"] = sort_dir
    if page and page > 1:
        params["page"] = str(page)
    if limit:
        params["limit"] = str(limit)
    return f"/deals/{message_id}?{urlencode(params)}"


def build_row_action_links(
    message_id: Optional[int],
    *,
    channel_id: Optional[str] = None,
    created_at: Optional[datetime | str] = None,
    status: Optional[str] = None,
    expense_category: Optional[str] = None,
) -> dict[str, str]:
    day = format_pacific_date(created_at)
    detail_status = "review_queue" if status in {PARSE_FAILED, PARSE_REVIEW_REQUIRED, "failed", "needs_review"} else status
    return {
        "open_row": build_message_detail_url(
            message_id,
            return_path="/review-table" if detail_status == "review_queue" else "/table",
            status=detail_status,
            channel_id=channel_id,
            expense_category=expense_category,
            after=day,
            before=day,
            sort_by="time",
            sort_dir="desc",
            limit=100,
        ),
        "open_table": build_return_url(
            "/table",
            status=status if status not in {"review_queue"} else None,
            channel_id=channel_id,
            expense_category=expense_category,
            after=day,
            before=day,
            sort_by="time",
            sort_dir="desc",
            limit=100,
        ),
        "open_review_queue": build_return_url(
            "/review-table",
            channel_id=channel_id,
            expense_category=expense_category,
            after=day,
            before=day,
            sort_by="time",
            sort_dir="desc",
            limit=100,
        ),
    }


def build_queue_action_links() -> dict[str, str]:
    return {
        "open_processing": build_return_url("/table", status="processing", sort_by="time", sort_dir="desc", limit=100),
        "open_failed": build_return_url("/table", status="failed", sort_by="time", sort_dir="desc", limit=100),
        "open_review_queue": build_return_url("/review-table", sort_by="time", sort_dir="desc", limit=100),
    }


def build_row_attention(item: dict) -> dict:
    reasons: list[dict[str, str]] = []
    if item.get("status") == "failed":
        reasons.append({"label": "Parse failed", "tone": "danger"})
    elif item.get("is_stale_processing"):
        reasons.append({"label": "Stuck in processing", "tone": "danger"})
    elif item.get("needs_review"):
        reasons.append({"label": "Needs review", "tone": "warn"})

    if item.get("stitched_group_id") and not item.get("stitched_primary"):
        reasons.append({"label": "Grouped child row", "tone": "warn"})
    elif item.get("stitched_count", 0) > 1:
        reasons.append({"label": "Grouped deal", "tone": "warn"})

    if item.get("possible_missing_image"):
        reasons.append({"label": "Nearby image clue", "tone": "warn"})

    bookkeeping_status = (item.get("bookkeeping") or {}).get("status")
    if bookkeeping_status == "matched_amount_only":
        reasons.append({"label": "Bookkeeping partial match", "tone": "warn"})
    elif bookkeeping_status == "unmatched" and item.get("status") == "parsed":
        reasons.append({"label": "Bookkeeping unmatched", "tone": "warn"})

    parse_attempts = int(item.get("parse_attempts") or 0)
    if parse_attempts >= 2:
        reasons.append({"label": f"Repeated attempts ({parse_attempts})", "tone": "warn"})

    error_snippet = summarize_message_snippet(item.get("last_error"), limit=90) if item.get("last_error") else ""
    level = ""
    if any(reason["tone"] == "danger" for reason in reasons):
        level = "danger"
    elif reasons:
        level = "warn"

    return {
        "level": level,
        "summary": " | ".join(reason["label"] for reason in reasons),
        "reasons": reasons,
        "error_snippet": error_snippet,
        "has_attention": bool(reasons or error_snippet),
        "is_failed": item.get("status") == "failed",
        "has_image_clue": bool(item.get("first_image_url") or item.get("possible_missing_image")),
        "is_grouped": bool(item.get("stitched_count", 0) > 1),
        "has_bookkeeping_mismatch": bookkeeping_status in {"matched_amount_only", "unmatched"},
    }


def build_review_shortcuts(items: list[dict]) -> list[dict[str, object]]:
    shortcut_specs = [
        ("failed", "Next Failed", lambda item: item.get("attention", {}).get("is_failed")),
        ("image", "Next Image Clue", lambda item: item.get("attention", {}).get("has_image_clue")),
        ("grouped", "Next Grouped", lambda item: item.get("attention", {}).get("is_grouped")),
        ("bookkeeping", "Next Bookkeeping Mismatch", lambda item: item.get("attention", {}).get("has_bookkeeping_mismatch")),
    ]
    shortcuts: list[dict[str, object]] = []
    for key, label, predicate in shortcut_specs:
        count = sum(1 for item in items if predicate(item))
        shortcuts.append(
            {
                "key": key,
                "label": label,
                "count": count,
                "enabled": count > 0,
            }
        )
    return shortcuts


def parse_optional_float(value: Optional[str]) -> Optional[float]:
    text = (value or "").strip()
    if not text:
        return None
    return float(text)


def parse_string_list(value: Optional[str]) -> list[str]:
    text = (value or "").strip()
    if not text:
        return []

    normalized = text.replace("\r", "")
    parts: list[str] = []
    for line in normalized.split("\n"):
        for part in line.split(","):
            cleaned = part.strip()
            if cleaned:
                parts.append(cleaned)
    return parts


def compute_manual_financials(
    *,
    row: DiscordMessage,
    deal_type: Optional[str],
    category: Optional[str],
    amount: Optional[float],
    cash_direction: Optional[str],
    entry_kind_override: Optional[str],
    expense_category_override: Optional[str],
):
    financials = compute_financials(
        parsed_type=deal_type,
        parsed_category=category,
        amount=amount,
        cash_direction=cash_direction,
        message_text=row.content or "",
    )

    entry_kind = entry_kind_override or financials.entry_kind
    expense_category = expense_category_override or financials.expense_category
    normalized_amount = round(float(amount or 0.0), 2)
    money_in = 0.0
    money_out = 0.0

    if entry_kind == "sale":
        money_in = normalized_amount
    elif entry_kind in {"buy", "expense"}:
        money_out = normalized_amount
    elif entry_kind == "trade":
        if cash_direction == "to_store":
            money_in = normalized_amount
        elif cash_direction == "from_store":
            money_out = normalized_amount

    return entry_kind, money_in, money_out, expense_category


def get_message_rows(
    session: Session,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    sort_by: str = "time",
    sort_dir: str = "desc",
    page: int = 1,
    limit: int = 100,
):
    stmt = build_message_stmt(
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
    )

    sort_map = {
        "time": DiscordMessage.created_at,
        "channel": DiscordMessage.channel_name,
        "author": DiscordMessage.author_name,
        "status": DiscordMessage.parse_status,
        "entry_kind": DiscordMessage.entry_kind,
        "type": DiscordMessage.deal_type,
        "amount": DiscordMessage.amount,
        "payment": DiscordMessage.payment_method,
        "category": DiscordMessage.category,
        "review": DiscordMessage.needs_review,
    }
    sort_column = sort_map.get(sort_by, DiscordMessage.created_at)

    if sort_dir == "asc":
        stmt = stmt.order_by(sort_column.asc(), DiscordMessage.created_at.asc())
    else:
        stmt = stmt.order_by(sort_column.desc(), DiscordMessage.created_at.desc())

    total_rows = count_rows(session, stmt)
    page = max(page, 1)
    offset = (page - 1) * limit
    rows = session.exec(stmt.offset(offset).limit(limit)).all()

    return rows, total_rows


def get_ordered_message_ids(
    session: Session,
    *,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    sort_by: str = "time",
    sort_dir: str = "desc",
) -> list[int]:
    rows, _ = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=1,
        limit=5000,
    )
    return [row.id for row in rows if row.id is not None]

def get_summary(
    session: Session,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> dict:
    stmt = build_message_stmt(
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    summary_subquery = stmt.order_by(None).subquery()

    raw_status_counts = session.exec(
        select(summary_subquery.c.parse_status, func.count())
        .group_by(summary_subquery.c.parse_status)
    ).all()
    status_counts: dict[str, int] = {}
    for raw_status, count in raw_status_counts:
        normalized_status = normalize_parse_status(raw_status)
        status_counts[normalized_status] = status_counts.get(normalized_status, 0) + int(count)
    total = sum(status_counts.values())
    with_images = int(
        session.exec(
            select(func.count()).select_from(summary_subquery).where(
                summary_subquery.c.attachment_urls_json != "[]"
            )
        ).one()
    )
    deleted = int(
        session.exec(
            select(func.count()).select_from(summary_subquery).where(
                summary_subquery.c.is_deleted == True  # noqa: E712
            )
        ).one()
    )

    return {
        "total": total,
        "parsed": status_counts.get(PARSE_PARSED, 0),
        "processing": status_counts.get(PARSE_PROCESSING, 0),
        "queued": status_counts.get(PARSE_PENDING, 0),
        "failed": status_counts.get(PARSE_FAILED, 0),
        "needs_review": status_counts.get(PARSE_REVIEW_REQUIRED, 0),
        "ignored": status_counts.get(PARSE_IGNORED, 0),
        "with_images": with_images,
        "deleted": deleted,
    }


def build_pagination(page: int, limit: int, total_rows: int) -> dict:
    total_pages = max((total_rows + limit - 1) // limit, 1)
    safe_page = min(max(page, 1), total_pages)

    return {
        "page": safe_page,
        "limit": limit,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "has_previous": safe_page > 1,
        "has_next": safe_page < total_pages,
        "previous_page": safe_page - 1,
        "next_page": safe_page + 1,
        "start_row": 0 if total_rows == 0 else (safe_page - 1) * limit + 1,
        "end_row": min(safe_page * limit, total_rows),
    }


def get_review_history_rows(
    session: Session,
    *,
    page: int = 1,
    limit: int = 50,
) -> tuple[list[DiscordMessage], int]:
    stmt = (
        select(DiscordMessage)
        .where(DiscordMessage.reviewed_at != None)  # noqa: E711
        .order_by(DiscordMessage.reviewed_at.desc(), DiscordMessage.created_at.desc())
    )
    total_rows = count_rows(session, stmt)
    offset = (max(page, 1) - 1) * limit
    rows = session.exec(stmt.offset(offset).limit(limit)).all()
    return rows, total_rows


def get_partner_channel_choices(session: Session) -> list[dict]:
    watched_channels = [
        row for row in get_watched_channels(session)
        if row.is_enabled
    ]
    return [
        {
            "channel_id": row.channel_id,
            "channel_name": row.channel_name or row.channel_id,
        }
        for row in watched_channels
    ]


def get_partner_deal_rows(
    session: Session,
    *,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    page: int = 1,
    limit: int = 25,
) -> tuple[list[DiscordMessage], int]:
    watched_channel_ids = [
        row.channel_id
        for row in get_watched_channels(session)
        if row.is_enabled
    ]
    if not watched_channel_ids:
        return [], 0

    stmt = (
        select(DiscordMessage)
        .where(DiscordMessage.channel_id.in_(watched_channel_ids))
        .where(DiscordMessage.parse_status == PARSE_PARSED)
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
    )

    if channel_id and channel_id in watched_channel_ids:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)
    if entry_kind:
        stmt = stmt.where(DiscordMessage.entry_kind == entry_kind)

    after_dt = parse_report_datetime(after)
    before_dt = parse_report_datetime(before, end_of_day=True)
    if after_dt:
        stmt = stmt.where(DiscordMessage.created_at >= after_dt)
    if before_dt:
        stmt = stmt.where(DiscordMessage.created_at <= before_dt)

    stmt = stmt.order_by(DiscordMessage.created_at.desc())
    total_rows = count_rows(session, stmt)
    offset = (max(page, 1) - 1) * limit
    rows = session.exec(stmt.offset(offset).limit(limit)).all()
    return rows, total_rows


def next_sort_direction(current_sort_by: str, current_sort_dir: str, target_sort_by: str) -> str:
    if current_sort_by == target_sort_by and current_sort_dir == "desc":
        return "asc"
    return "desc"


def sort_indicator(current_sort_by: str, current_sort_dir: str, target_sort_by: str) -> str:
    if current_sort_by != target_sort_by:
        return ""
    return "^" if current_sort_dir == "asc" else "v"


def build_watched_channel_groups(
    watched_channels: list[WatchedChannel],
    available_discord_channels: list[dict],
) -> list[dict]:
    metadata_by_channel_id = {
        channel["channel_id"]: channel
        for channel in available_discord_channels
    }
    grouped: dict[str, list[WatchedChannel]] = {}

    for watched_channel in watched_channels:
        metadata = metadata_by_channel_id.get(watched_channel.channel_id, {})
        category_name = metadata.get("category_name") or "Other"
        grouped.setdefault(category_name, []).append(watched_channel)

    return [
        {
            "category_name": category_name,
            "channels": sorted(
                channels,
                key=lambda row: (row.channel_name or row.channel_id).lower(),
            ),
        }
        for category_name, channels in sorted(grouped.items(), key=lambda item: item[0].lower())
    ]


def _load_stream_range() -> None:
    """Load persisted stream range and source from DB into memory."""
    global _stream_range_source
    try:
        with managed_session() as session:
            for key in ("stream_start_utc", "stream_end_utc"):
                row = session.get(AppSetting, key)
                if row and row.value:
                    dt = datetime.fromisoformat(row.value)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    field = "start" if "start" in key else "end"
                    _stream_range[field] = dt
            src_row = session.get(AppSetting, "stream_range_source")
            if src_row and src_row.value:
                _stream_range_source = src_row.value
    except Exception as exc:
        print(structured_log_line(runtime="app", action="stream_range.load_failed", success=False, error=str(exc)))


def _save_stream_range(source: Optional[str] = None) -> None:
    """Persist current stream range and source to DB."""
    global _stream_range_source
    if source is not None:
        _stream_range_source = source

    def _do(session: Session):
        for field, key in (("start", "stream_start_utc"), ("end", "stream_end_utc")):
            val = _stream_range.get(field)
            val_str = val.isoformat() if val else ""
            existing = session.get(AppSetting, key)
            if existing:
                existing.value = val_str
                session.add(existing)
            else:
                session.add(AppSetting(key=key, value=val_str))
        src_existing = session.get(AppSetting, "stream_range_source")
        if src_existing:
            src_existing.value = _stream_range_source
            session.add(src_existing)
        else:
            session.add(AppSetting(key="stream_range_source", value=_stream_range_source))
        session.commit()
    try:
        run_write_with_retry(_do)
    except Exception as exc:
        print(structured_log_line(runtime="app", action="stream_range.save_failed", success=False, error=str(exc)))


def _warm_cache_sync() -> tuple[int, int]:
    with managed_session() as session:
        return warm_attachment_cache(session)


async def lifespan(app: FastAPI):
    init_db()
    _load_stream_range()
    _load_tiktok_state_from_db()
    with managed_session() as session:
        seed_default_users(session)
        requeue_interrupted_backfill_requests(session)
    seed_channels_from_env()
    reset_background_task_failures()

    stop_event = asyncio.Event()
    app.state.stop_event = stop_event
    heartbeat_stop_event = threading.Event()
    app.state.heartbeat_stop_event = heartbeat_stop_event
    app_heartbeat_thread = threading.Thread(
        target=runtime_heartbeat_loop,
        kwargs={
            "stop_event": heartbeat_stop_event,
            "runtime_name": APP_HEARTBEAT_RUNTIME_NAME,
            "host_name": socket.gethostname(),
            "details_provider": app_runtime_details,
        },
        name="app-heartbeat",
        daemon=True,
    )
    app_heartbeat_thread.start()
    app.state.app_heartbeat_thread = app_heartbeat_thread

    background_tasks: list[asyncio.Task] = []

    if settings.discord_ingest_enabled or settings.parser_worker_enabled:
        heartbeat_thread = threading.Thread(
            target=runtime_heartbeat_loop,
            kwargs={
                "stop_event": heartbeat_stop_event,
                "runtime_name": WORKER_RUNTIME_NAME,
                "host_name": socket.gethostname(),
                "details_provider": local_runtime_details,
            },
            name="local-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
        app.state.heartbeat_thread = heartbeat_thread
    else:
        app.state.heartbeat_thread = None

    if settings.discord_ingest_enabled:
        discord_task = track_background_task(
            asyncio.create_task(run_discord_bot(stop_event), name="discord-ingest"),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="discord-ingest",
            stop_event=stop_event,
        )
        background_tasks.append(discord_task)
        app.state.discord_task = discord_task
        backfill_task = track_background_task(
            asyncio.create_task(
                backfill_request_loop(stop_event, get_discord_client),
                name="backfill-queue",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="backfill-queue",
            stop_event=stop_event,
        )
        background_tasks.append(backfill_task)
        app.state.backfill_task = backfill_task
        recent_audit_task = track_background_task(
            asyncio.create_task(
                recent_message_audit_loop(stop_event, get_discord_client),
                name="recent-message-audit",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="recent-message-audit",
            stop_event=stop_event,
        )
        background_tasks.append(recent_audit_task)
        app.state.recent_audit_task = recent_audit_task
        if settings.periodic_attachment_repair_enabled:
            attachment_repair_task = track_background_task(
                asyncio.create_task(
                    periodic_attachment_repair_loop(stop_event, get_discord_client),
                    name="attachment-repair-audit",
                ),
                runtime_name=WORKER_RUNTIME_NAME,
                task_name="attachment-repair-audit",
                stop_event=stop_event,
            )
            background_tasks.append(attachment_repair_task)
            app.state.attachment_repair_task = attachment_repair_task
        else:
            app.state.attachment_repair_task = None
    else:
        app.state.discord_task = None
        app.state.backfill_task = None
        app.state.recent_audit_task = None
        app.state.attachment_repair_task = None

    if settings.discord_ingest_enabled and settings.parser_worker_enabled:
        stitch_audit_task = track_background_task(
            asyncio.create_task(
                periodic_stitch_audit_loop(stop_event),
                name="stitch-audit",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="stitch-audit",
            stop_event=stop_event,
        )
        background_tasks.append(stitch_audit_task)
        app.state.stitch_audit_task = stitch_audit_task
    else:
        app.state.stitch_audit_task = None

    if settings.parser_worker_enabled:
        worker_task = track_background_task(
            asyncio.create_task(parser_loop(stop_event), name="parser-worker"),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="parser-worker",
            stop_event=stop_event,
        )
        background_tasks.append(worker_task)
        app.state.worker_task = worker_task
    else:
        app.state.worker_task = None
        print("[worker] parser worker disabled by configuration")

    if (settings.tiktok_app_key or "").strip() and (settings.tiktok_app_secret or "").strip():
        tiktok_pull_task = track_background_task(
            asyncio.create_task(
                periodic_tiktok_pull_loop(stop_event),
                name="tiktok-order-pull",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="tiktok-order-pull",
            stop_event=stop_event,
        )
        background_tasks.append(tiktok_pull_task)
        app.state.tiktok_pull_task = tiktok_pull_task
        tiktok_backfill_task = asyncio.create_task(
            tiktok_startup_backfill(stop_event),
            name="tiktok-startup-backfill",
        )
        background_tasks.append(tiktok_backfill_task)
        app.state.tiktok_backfill_task = tiktok_backfill_task
    else:
        app.state.tiktok_pull_task = None
        app.state.tiktok_backfill_task = None

    live_analytics_stop = threading.Event()
    app.state.live_analytics_stop = live_analytics_stop
    live_analytics_thread = threading.Thread(
        target=_poll_tiktok_live_analytics,
        args=(live_analytics_stop,),
        name="tiktok-live-analytics",
        daemon=True,
    )
    live_analytics_thread.start()
    app.state.live_analytics_thread = live_analytics_thread

    live_chat_username = (settings.tiktok_live_username or "").strip()
    live_chat_api_key = (settings.tiktok_live_api_key or "").strip()
    if live_chat_username and live_chat_api_key:
        live_chat_task = asyncio.create_task(
            start_live_chat(live_chat_username, live_chat_api_key),
            name="tiktok-live-chat",
        )
        background_tasks.append(live_chat_task)
        app.state.live_chat_task = live_chat_task
        print(f"[tiktok-live-chat] starting for @{live_chat_username}")
    else:
        app.state.live_chat_task = None

    yield

    stop_event.set()
    heartbeat_stop_event.set()
    live_analytics_stop.set()
    await stop_live_chat()

    if background_tasks:
        done, pending = await asyncio.wait(background_tasks, timeout=10)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if done:
            await asyncio.gather(*done, return_exceptions=True)
    heartbeat_thread = getattr(app.state, "heartbeat_thread", None)
    if heartbeat_thread:
        heartbeat_thread.join(timeout=5)
    app_heartbeat_thread = getattr(app.state, "app_heartbeat_thread", None)
    if app_heartbeat_thread:
        app_heartbeat_thread.join(timeout=5)


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    session_cookie=settings.session_cookie_name,
    https_only=settings.session_https_only,
    same_site=settings.session_same_site,
    domain=settings.effective_session_domain or None,
)
app.mount("/static", StaticFiles(directory=normalize_filesystem_path(BASE_DIR / "static")), name="static")


@app.exception_handler(OperationalError)
async def handle_operational_error(request: Request, exc: OperationalError):
    dispose_engine()
    error_text = str(exc).lower()
    is_sqlite_busy = "database is locked" in error_text or "sqlite_busy" in error_text
    if is_sqlite_busy:
        payload = {
            "ok": False,
            "error": "SQLite is temporarily busy.",
            "detail": "The local database is handling another write right now. Please retry in a few seconds.",
        }
        html_message = (
            '<meta http-equiv="refresh" content="3">'
            "<h1>SQLite temporarily busy</h1>"
            "<p>The local database is handling another write right now. Retrying automatically&hellip;</p>"
        )
    else:
        payload = {
            "ok": False,
            "error": "Database connection is temporarily unavailable.",
            "detail": "The shared database did not accept the connection cleanly. Please retry in a few seconds.",
        }
        html_message = (
            "<h1>Database temporarily unavailable</h1>"
            "<p>The shared database connection dropped unexpectedly. Please retry in a few seconds.</p>"
        )
    headers = {"Retry-After": "5"}
    wants_json = (
        request.url.path.startswith("/admin/parser-progress")
        or request.url.path.startswith("/health")
        or "application/json" in request.headers.get("accept", "")
    )
    if wants_json:
        return JSONResponse(status_code=503, content=payload, headers=headers)
    return HTMLResponse(html_message, status_code=503, headers=headers)


@app.get("/attachments/{asset_id}")
def attachment_asset(request: Request, asset_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    asset_meta = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.filename, AttachmentAsset.content_type)
        .where(AttachmentAsset.id == asset_id)
    ).first()
    if not asset_meta:
        raise HTTPException(status_code=404, detail="Attachment not found")

    _, filename, content_type = asset_meta

    etag = f'"{asset_id}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match.strip() == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "public, max-age=31536000, immutable"})

    file_path = attachment_cache_path(
        asset_id,
        filename=filename,
        content_type=content_type,
    )
    if not file_path.exists():
        asset = session.get(AttachmentAsset, asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Attachment not found")
        file_path = write_attachment_cache_file(
            asset_id,
            filename=asset.filename,
            content_type=asset.content_type,
            data=asset.data,
        )

    media_type = content_type or "application/octet-stream"
    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "ETag": etag,
    }
    if filename:
        headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return FileResponse(path=file_path, media_type=media_type, headers=headers)


@app.get("/attachments/{asset_id}/thumb")
def attachment_thumbnail(request: Request, asset_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    etag = f'"thumb-{asset_id}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match.strip() == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "public, max-age=31536000, immutable"})

    asset_meta = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.filename, AttachmentAsset.content_type, AttachmentAsset.is_image)
        .where(AttachmentAsset.id == asset_id)
    ).first()
    if not asset_meta:
        raise HTTPException(status_code=404, detail="Attachment not found")

    _, filename, content_type, is_image = asset_meta
    if not is_image:
        return RedirectResponse(url=f"/attachments/{asset_id}", status_code=307)

    file_path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
    if not file_path.exists():
        asset = session.get(AttachmentAsset, asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Attachment not found")
        file_path = write_attachment_cache_file(
            asset_id, filename=asset.filename, content_type=asset.content_type, data=asset.data,
        )

    thumb_path = generate_thumbnail(file_path, asset_id)
    if thumb_path and thumb_path.exists():
        return FileResponse(
            path=thumb_path,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=31536000, immutable", "ETag": etag},
        )
    return FileResponse(
        path=file_path,
        media_type=content_type or "application/octet-stream",
        headers={"Cache-Control": "public, max-age=31536000, immutable", "ETag": etag},
    )


@app.get("/messages/{message_id}/attachments/{attachment_index}")
async def message_attachment_fallback(
    request: Request,
    message_id: int,
    attachment_index: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    attachment_urls = json.loads(row.attachment_urls_json or "[]")
    if attachment_index < 0 or attachment_index >= len(attachment_urls):
        raise HTTPException(status_code=404, detail="Attachment not found")

    cached_assets = session.exec(
        select(AttachmentAsset.id)
        .where(AttachmentAsset.message_id == message_id)
        .order_by(AttachmentAsset.id.asc())
    ).all()
    if attachment_index < len(cached_assets):
        asset_id = cached_assets[attachment_index][0]
        return RedirectResponse(url=f"/attachments/{asset_id}", status_code=307)

    recovered = await recover_attachment_assets_for_message(
        channel_id=row.channel_id,
        discord_message_id=row.discord_message_id,
        message_row_id=message_id,
    )
    if recovered:
        refreshed_assets = session.exec(
            select(AttachmentAsset.id)
            .where(AttachmentAsset.message_id == message_id)
            .order_by(AttachmentAsset.id.asc())
        ).all()
        if attachment_index < len(refreshed_assets):
            asset_id = refreshed_assets[attachment_index][0]
            return RedirectResponse(url=f"/attachments/{asset_id}", status_code=307)

    return RedirectResponse(url=attachment_urls[attachment_index], status_code=307)


PUBLIC_PATH_PREFIXES = (
    "/static",
    "/health",
    "/login",
    "/webhooks/shopify",
    "/webhooks/tiktok",
    "/integrations/tiktok/callback",
)


def user_role_for_path(path: str) -> Optional[str]:
    if path.startswith("/table") or path.startswith("/review-table") or path.startswith("/bookkeeping") or path.startswith("/admin"):
        return "admin"
    if path.startswith("/api/review"):
        return "reviewer"
    if path.startswith("/review") or path.startswith("/messages") or path.startswith("/channels"):
        return "reviewer"
    if path.startswith("/reports") or path.startswith("/shopify-orders") or path.startswith("/shopify/orders") or path.startswith("/tiktok/orders") or path.startswith("/tiktok/products") or path == "/tiktok":
        return "viewer"
    if path == "/":
        return "viewer"
    return None


def get_request_user(request: Request) -> Optional[User]:
    session_data = request.scope.get("session") or {}
    user_id = session_data.get("user_id")
    if not user_id:
        return None
    if recent_db_failure():
        return None
    with managed_session() as session:
        user = session.get(User, user_id)
        if not user or not user.is_active:
            return None
        return user


def redirect_to_login(request: Request) -> RedirectResponse:
    next_path = request.url.path
    if request.url.query:
        next_path = f"{next_path}?{request.url.query}"
    return RedirectResponse(url=f"/login?next={urlencode({'next': next_path})[5:]}", status_code=303)


def app_home_for_role(role: str) -> str:
    if role == "admin":
        return "/dashboard"
    if role == "reviewer":
        return "/review"
    return "/dashboard"


def require_role_response(request: Request, minimum_role: str) -> Optional[Response]:
    user = getattr(request.state, "current_user", None)
    if user is None:
        user = get_request_user(request)
    if not user:
        return redirect_to_login(request)
    if not has_role(user, minimum_role):
        return HTMLResponse("You do not have permission to view this page.", status_code=403)
    request.state.current_user = user
    return None


def current_user_label(request: Optional[Request]) -> str:
    if request is None:
        return "system"
    user = getattr(request.state, "current_user", None)
    if not user:
        return "system"
    return user.display_name or user.username


@app.middleware("http")
async def attach_current_user(request: Request, call_next):
    if request.url.path.startswith(PUBLIC_PATH_PREFIXES):
        request.state.current_user = None
        return await call_next(request)
    request.state.current_user = get_request_user(request)
    return await call_next(request)


@app.get("/health", response_model=HealthOut)
def health():
    try:
        with managed_session() as session:
            db_health = get_database_health(session)
            local_runtime = get_runtime_heartbeat_status(
                session,
                APP_HEARTBEAT_RUNTIME_NAME,
                runtime_label=APP_RUNTIME_LABEL,
                updated_at_formatter=format_pacific_datetime,
            )
        return HealthOut(
            ok=bool(db_health["ok"]),
            db_ok=bool(db_health["ok"]),
            local_runtime_status=local_runtime["status"],
            local_runtime_label=local_runtime["label"],
            local_runtime_needs_attention=local_runtime["needs_attention"],
            local_runtime_updated_at=local_runtime["updated_at"],
        )
    except Exception as exc:
        return HealthOut(
            ok=False,
            db_ok=False,
            local_runtime_status="unknown",
            local_runtime_label="Unknown",
            local_runtime_needs_attention=True,
            error=str(exc),
        )


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = get_request_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return RedirectResponse(url=app_home_for_role(user.role), status_code=303)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    user = getattr(request.state, "current_user", None)
    review_summary = get_summary(session, status="review_queue")
    overall_summary = get_summary(session)
    dashboard_snapshot = build_dashboard_snapshot(session)
    dashboard_snapshot.setdefault("today", {})
    dashboard_snapshot["today"].setdefault(
        "shopify",
        {
            "order_count": 0,
            "gross": 0.0,
            "tax": 0.0,
            "net": 0.0,
            "refunds": 0.0,
            "tax_missing_count": 0,
            "includes_tax_count": 0,
            "gross_display": format_dashboard_money(0.0),
            "tax_display": format_dashboard_money(0.0),
            "net_display": format_dashboard_money(0.0),
            "refunds_display": format_dashboard_money(0.0),
        },
    )
    dashboard_snapshot["today"].setdefault(
        "revenue",
        {
            "discord_sales": 0.0,
            "discord_trade_in": 0.0,
            "discord_total": 0.0,
            "shopify_total": 0.0,
            "tiktok_total": 0.0,
            "total": 0.0,
            "total_display": format_dashboard_money(0.0),
            "discord_total_display": format_dashboard_money(0.0),
            "discord_sales_display": format_dashboard_money(0.0),
            "discord_trade_in_display": format_dashboard_money(0.0),
            "shopify_total_display": format_dashboard_money(0.0),
            "tiktok_total_display": format_dashboard_money(0.0),
        },
    )
    dashboard_snapshot["today"].setdefault(
        "purchases",
        {
            "buys": 0.0,
            "trade_out": 0.0,
            "expenses": 0.0,
            "shopify_refunds": 0.0,
            "total": 0.0,
            "total_display": format_dashboard_money(0.0),
            "buys_display": format_dashboard_money(0.0),
            "trade_out_display": format_dashboard_money(0.0),
            "expenses_display": format_dashboard_money(0.0),
            "shopify_refunds_display": format_dashboard_money(0.0),
            "has_shopify_refunds": False,
        },
    )
    parser_progress = get_parser_progress(session)
    today_start_local = utcnow().astimezone(PACIFIC_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start_local = today_start_local + timedelta(days=1)
    today_start = today_start_local.astimezone(timezone.utc)
    tomorrow_start = tomorrow_start_local.astimezone(timezone.utc)
    tiktok_rows = get_tiktok_reporting_rows(session, start=today_start, end=tomorrow_start)
    tiktok_summary = build_tiktok_reporting_summary(tiktok_rows)
    tiktok_auth_row = get_latest_tiktok_auth_row(session)
    tiktok_sync_snapshot = describe_tiktok_sync_status(tiktok_auth_row, read_tiktok_integration_state())
    dashboard_snapshot["today"]["tiktok"] = {
        "order_count": int(tiktok_summary.get("orders", 0) or 0),
        "paid_order_count": int(tiktok_summary.get("paid_orders", 0) or 0),
        "gross": round(float(tiktok_summary.get("gross_revenue", 0.0) or 0.0), 2),
        "tax": round(float(tiktok_summary.get("total_tax", 0.0) or 0.0), 2),
        "net": round(float(tiktok_summary.get("net_revenue", 0.0) or 0.0), 2),
        "order_count_display": str(int(tiktok_summary.get("orders", 0) or 0)),
        "paid_order_count_display": str(int(tiktok_summary.get("paid_orders", 0) or 0)),
        "gross_display": format_dashboard_money(float(tiktok_summary.get("gross_revenue", 0.0) or 0.0)),
        "tax_display": format_dashboard_money(float(tiktok_summary.get("total_tax", 0.0) or 0.0)),
        "net_display": format_dashboard_money(float(tiktok_summary.get("net_revenue", 0.0) or 0.0)),
    }
    dashboard_snapshot["today"]["revenue"]["tiktok_total"] = round(float(tiktok_summary.get("net_revenue", 0.0) or 0.0), 2)
    dashboard_snapshot["today"]["revenue"]["tiktok_total_display"] = format_dashboard_money(
        float(tiktok_summary.get("net_revenue", 0.0) or 0.0)
    )
    dashboard_snapshot["today"]["revenue"]["total"] = round(
        float(dashboard_snapshot["today"]["revenue"].get("discord_total", 0.0) or 0.0)
        + float(dashboard_snapshot["today"]["revenue"].get("shopify_total", 0.0) or 0.0)
        + float(dashboard_snapshot["today"]["revenue"].get("tiktok_total", 0.0) or 0.0),
        2,
    )
    dashboard_snapshot["today"]["revenue"]["total_display"] = format_dashboard_money(
        float(dashboard_snapshot["today"]["revenue"]["total"] or 0.0)
    )
    tiktok_orders: list[dict] = []
    recent_tiktok_rows = session.exec(
        select(TikTokOrder)
        .where(TikTokOrder.created_at >= today_start)
        .where(TikTokOrder.created_at <= tomorrow_start)
        .order_by(TikTokOrder.created_at.desc(), TikTokOrder.id.desc())
        .limit(25)
    ).all()
    for order in recent_tiktok_rows:
        if classify_tiktok_reporting_status(order) != "paid":
            continue
        fulfillment_value = (order.fulfillment_status or order.order_status or "").strip().lower()
        if fulfillment_value in {"fulfilled", "completed", "delivered"}:
            fulfillment_label = "Completed"
        elif fulfillment_value in {"awaiting_shipment", "awaiting_collection"}:
            fulfillment_label = "Awaiting shipment"
        elif fulfillment_value in {"partial", "partially_shipped"}:
            fulfillment_label = "Partial"
        else:
            fulfillment_label = (order.fulfillment_status or order.order_status or "").strip() or "Pending"
        tiktok_orders.append(
            {
                "id": order.id,
                "order_number": order.order_number or order.tiktok_order_id,
                "created_at": format_pacific_datetime(order.created_at, include_zone=False),
                "customer_name": (order.customer_name or "").strip() or "Customer",
                "total_price": round(float(order.total_price or 0.0), 2),
                "fulfillment_label": fulfillment_label,
            }
        )
        if len(tiktok_orders) >= 10:
            break
    tiktok_recent_order_count = len(tiktok_orders)
    recent_reviewed = build_message_list_items(
        session,
        session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.reviewed_at != None)  # noqa: E711
            .order_by(DiscordMessage.reviewed_at.desc())
            .limit(5)
        ).all(),
    )
    recent_deals = build_message_list_items(
        session,
        session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.is_deleted == False)  # noqa: E712
            .where(DiscordMessage.parse_status.in_(status_filter_values(PARSE_PARSED)))
            .order_by(DiscordMessage.created_at.desc(), DiscordMessage.id.desc())
            .limit(10)
        ).all(),
    )
    shopify_connected = True
    shopify_message = ""
    shopify_orders: list[dict] = []
    try:
        recent_shopify_rows = session.exec(
            select(ShopifyOrder)
            .where(ShopifyOrder.financial_status == "paid")
            .order_by(ShopifyOrder.created_at.desc(), ShopifyOrder.id.desc())
            .limit(10)
        ).all()
        for order in recent_shopify_rows:
            fulfillment_status = (order.fulfillment_status or "").strip().lower()
            if fulfillment_status == "fulfilled":
                fulfillment_label = "Shipped"
            elif fulfillment_status == "partial":
                fulfillment_label = "Partial"
            else:
                fulfillment_label = "Pending"
            shopify_orders.append(
                {
                    "id": order.id,
                    "order_number": order.order_number,
                    "created_at": format_pacific_datetime(order.created_at, include_zone=False),
                    "customer_name": (order.customer_name or "").strip() or "Customer",
                    "total_price": round(float(order.total_price or 0.0), 2),
                    "fulfillment_label": fulfillment_label,
                }
            )
    except OperationalError:
        shopify_connected = False
        shopify_message = "Shopify not connected yet"

    _raw_promotions = list_operations_logs(
        session,
        event_type_prefix="queue.auto_promoted_correction_pattern",
        limit=10,
        since=utcnow() - timedelta(days=7),
    )
    recent_promotions = [
        {
            "normalized_text": (parse_operations_log_details(row) or {}).get("normalized_text") or row.message,
            "promoted_at": row.created_at,
        }
        for row in _raw_promotions
    ]

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "title": "Dashboard",
            "current_user": user,
            "review_summary": review_summary,
            "overall_summary": overall_summary,
            "dashboard_snapshot": dashboard_snapshot,
            "recent_reviewed": recent_reviewed,
            "recent_deals": recent_deals,
            "shopify_connected": shopify_connected,
            "shopify_message": shopify_message,
            "shopify_orders": shopify_orders,
            "tiktok_connected": bool(tiktok_auth_row or tiktok_sync_snapshot.get("has_tokens")),
            "tiktok_message": "" if (tiktok_auth_row or tiktok_sync_snapshot.get("has_tokens")) else "TikTok Shop not connected yet",
            "tiktok_orders": tiktok_orders,
            "tiktok_recent_order_count": tiktok_recent_order_count,
            "parser_progress": parser_progress,
            "tiktok_summary": tiktok_summary,
            "tiktok_sync_snapshot": tiktok_sync_snapshot,
            "tiktok_auth_row": tiktok_auth_row,
            "tiktok_today_rows": tiktok_rows,
            "tiktok_today_totals": tiktok_summary.get("totals", {}),
            "recent_promotions": recent_promotions,
        },
    )


@app.get("/partner", response_class=HTMLResponse)
def partner_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/dashboard", status_code=301)


@app.get("/status", response_class=HTMLResponse)
def status_page(
    request: Request,
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    def _normalize_optional_query_text(value: object) -> str:
        candidate = getattr(value, "default", value)
        if candidate is None:
            return ""
        if isinstance(candidate, str):
            return candidate.strip()
        return str(candidate).strip()

    success_text = _normalize_optional_query_text(success)
    error_text = _normalize_optional_query_text(error)
    status_snapshot = build_status_snapshot(session)
    query_alerts = [message for message in (error_text, success_text) if message]
    if query_alerts:
        status_snapshot["alert_messages"] = [
            *status_snapshot.get("alert_messages", []),
            *query_alerts,
        ]
    health_snapshot = build_health_snapshot(session)
    debug_snapshot = build_debug_snapshot(session)

    return templates.TemplateResponse(
        request,
        "status.html",
        {
            "request": request,
            "title": "System Status",
            "current_user": getattr(request.state, "current_user", None),
            "snapshot": status_snapshot,
            "health": health_snapshot,
            "debug": debug_snapshot,
        },
    )


@app.get("/status.json")
def status_json(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return build_status_snapshot(session)


@app.get("/ops-log", response_class=HTMLResponse)
def operations_log_page(
    request: Request,
    event_type_prefix: Optional[str] = Query(default=None),
    level: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    until: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    since_dt = parse_report_datetime(since)
    until_dt = parse_report_datetime(until, end_of_day=True)

    logs = serialize_operations_logs(
        list_operations_logs(
            session,
            event_type_prefix=event_type_prefix or None,
            level=level or None,
            since=since_dt,
            until=until_dt,
        )
    )

    return templates.TemplateResponse(
        request,
        "ops_log.html",
        {
            "request": request,
            "title": "Operations Log",
            "current_user": getattr(request.state, "current_user", None),
            "logs": logs,
            "snapshot": build_status_snapshot(session),
            "error_badge_count": count_recent_errors(session),
            "selected_event_type_prefix": event_type_prefix or "",
            "selected_level": level or "",
            "selected_since": since or "",
            "selected_until": until or "",
            "event_type_prefixes": ["queue.", "ingest.", "backfill"],
        },
    )


@app.get("/ops-log/error-count")
def ops_log_error_count(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    return {"count": count_recent_errors(session)}


@app.get("/ops-log/backfill/{request_id}", response_class=HTMLResponse)
def backfill_request_detail_page(
    request_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    row = session.get(BackfillRequest, request_id)
    if not row:
        raise HTTPException(status_code=404, detail="Backfill request not found")

    return templates.TemplateResponse(
        request,
        "backfill_request_detail.html",
        {
            "request": request,
            "title": f"Backfill Request {request_id}",
            "current_user": getattr(request.state, "current_user", None),
            "backfill_request": serialize_backfill_request_detail(row),
            "logs": serialize_operations_logs(
                list_operations_logs_for_backfill_request(session, request_id=request_id)
            ),
            "snapshot": build_backfill_queue_snapshot(session),
        },
    )


@app.get("/admin", response_class=HTMLResponse)
def admin_home_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    watched_channels = get_watched_channels(session)
    enabled_channels = [row for row in watched_channels if row.is_enabled]
    backfill_channels = [row for row in watched_channels if row.backfill_enabled]
    review_summary = get_summary(session, status="review_queue")
    overall_summary = get_summary(session)

    return templates.TemplateResponse(
        request,
        "admin_home.html",
        {
            "request": request,
            "title": "Admin Hub",
            "current_user": getattr(request.state, "current_user", None),
            "review_summary": review_summary,
            "overall_summary": overall_summary,
            "parser_progress": get_parser_progress(session),
            "watched_channels": watched_channels,
            "enabled_channel_count": len(enabled_channels),
            "backfill_channel_count": len(backfill_channels),
        },
    )


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(
    request: Request,
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    users = session.exec(select(User).order_by(User.created_at.asc())).all()
    return templates.TemplateResponse(
        request,
        "admin_users.html",
        {
            "request": request,
            "title": "User Management",
            "current_user": getattr(request.state, "current_user", None),
            "users": users,
            "current_admin_username": settings.admin_username.strip().lower(),
            "success": success,
            "error": error,
        },
    )


@app.post("/admin/users/create")
def admin_create_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    display_name: str = Form(default=""),
    role: str = Form(default="viewer"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    normalized = (username or "").strip().lower()
    if not normalized or not password:
        return RedirectResponse(url="/admin/users?error=Username+and+password+are+required", status_code=303)
    if role not in ("viewer", "reviewer", "admin"):
        return RedirectResponse(url="/admin/users?error=Invalid+role", status_code=303)
    existing = session.exec(select(User).where(User.username == normalized)).first()
    if existing:
        return RedirectResponse(url=f"/admin/users?error=User+{normalized}+already+exists", status_code=303)
    from .auth import hash_password
    session.add(User(
        username=normalized,
        password_hash=hash_password(password),
        display_name=(display_name or "").strip() or normalized,
        role=role,
        is_active=True,
        created_at=utcnow(),
        updated_at=utcnow(),
    ))
    session.commit()
    return RedirectResponse(url=f"/admin/users?success=Created+user+{normalized}", status_code=303)


@app.post("/admin/users/{user_id}/toggle")
def admin_toggle_user(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    user = session.get(User, user_id)
    if not user:
        return RedirectResponse(url="/admin/users?error=User+not+found", status_code=303)
    if user.username == settings.admin_username.strip().lower():
        return RedirectResponse(url="/admin/users?error=Cannot+disable+the+primary+admin+account", status_code=303)
    user.is_active = not user.is_active
    user.updated_at = utcnow()
    session.add(user)
    session.commit()
    action = "enabled" if user.is_active else "disabled"
    return RedirectResponse(url=f"/admin/users?success=User+{user.username}+{action}", status_code=303)


@app.get("/admin/debug", response_class=HTMLResponse)
def admin_debug_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return RedirectResponse(url="/status", status_code=301)


@app.get("/admin/logs", response_class=HTMLResponse)
def admin_logs_page(
    request: Request,
    file: str = Query(default="app"),
    lines: int = Query(default=200, ge=10, le=2000),
):
    role_response = require_role_response(request, "admin")
    if role_response:
        return role_response

    allowed_files = {"app": "app.log", "worker": "worker.log"}
    log_filename = allowed_files.get(file, "app.log")
    log_path = resolve_runtime_log_path(log_filename)

    tail_lines: list[str] = []
    if log_path.exists():
        try:
            raw = log_path.read_text(encoding="utf-8", errors="replace")
            all_lines = raw.splitlines()
            tail_lines = all_lines[-lines:]
        except OSError:
            tail_lines = ["(unable to read log file)"]
    else:
        tail_lines = [f"(log file not found: {log_path})"]

    log_content = "\n".join(tail_lines)
    nav_links = " | ".join(
        f'<a href="/admin/logs?file={k}&lines={lines}" style="{"font-weight:bold" if k == file else ""}">{k}.log</a>'
        for k in allowed_files
    )

    return HTMLResponse(
        f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Logs — {log_filename}</title>
<style>
body {{ margin:0; padding:20px; background:#0a0c10; color:#c8d0da; font-family:monospace; font-size:13px; }}
nav {{ margin-bottom:16px; font-size:14px; }}
nav a {{ color:#ff8844; text-decoration:none; margin-right:12px; }}
pre {{ white-space:pre-wrap; word-break:break-all; line-height:1.6; }}
h1 {{ font-size:18px; color:#eee; margin:0 0 8px; }}
.controls {{ margin-bottom:12px; color:#888; font-size:12px; }}
.controls a {{ color:#ff8844; }}
</style></head><body>
<h1>{log_filename}</h1>
<nav>{nav_links}</nav>
<div class="controls">
Showing last {len(tail_lines)} lines &mdash;
<a href="/admin/logs?file={file}&lines=50">50</a> |
<a href="/admin/logs?file={file}&lines=200">200</a> |
<a href="/admin/logs?file={file}&lines=500">500</a> |
<a href="/admin/logs?file={file}&lines=1000">1000</a>
&mdash; <a href="/status">&larr; Status</a>
</div>
<pre>{log_content}</pre>
<script>window.scrollTo(0, document.body.scrollHeight);</script>
</body></html>"""
    )


@app.get("/admin/health", response_class=HTMLResponse)
def admin_health_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return RedirectResponse(url="/status", status_code=301)


@app.get("/deals", response_class=HTMLResponse)
def deals_page(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    rows, total_rows = get_partner_deal_rows(
        session,
        channel_id=channel_id,
        entry_kind=entry_kind,
        after=after,
        before=before,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows)
    channels = get_partner_channel_choices(session)
    watched_channel_ids = {row["channel_id"] for row in channels}
    summary_rows = [
        row for row in get_financial_rows(
            session,
            start=parse_report_datetime(after),
            end=parse_report_datetime(before, end_of_day=True),
            channel_id=channel_id if channel_id else None,
        )
        if row.channel_id in watched_channel_ids and normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review) == PARSE_PARSED and not row.is_deleted
    ]
    summary = build_financial_summary(summary_rows)
    return templates.TemplateResponse(
        request,
        "deals.html",
        {
            "request": request,
            "title": "Deals",
            "rows": items,
            "channels": channels,
            "selected_channel_id": channel_id or "",
            "selected_entry_kind": entry_kind or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_limit": limit,
            "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
            "summary": summary,
            "current_user": getattr(request.state, "current_user", None),
        },
    )


@app.get("/deals/{message_id}", response_class=HTMLResponse)
def deal_detail_page(
    message_id: int,
    request: Request,
    return_path: str = Query(default="/deals"),
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    watched_channel_ids = {
        row.channel_id
        for row in get_watched_channels(session)
        if row.is_enabled
    }
    row = session.get(DiscordMessage, message_id)
    if not row or row.channel_id not in watched_channel_ids or normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review) != PARSE_PARSED:
        raise HTTPException(status_code=404, detail="Deal not found")

    item = build_message_list_items(session, [row])[0]
    item["trade_summary"] = row.trade_summary
    item["notes"] = row.notes
    item["image_summary"] = row.image_summary
    item["reviewed_by"] = row.reviewed_by
    item["reviewed_at"] = format_pacific_datetime(row.reviewed_at)
    item["parse_status"] = normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review)
    item["needs_review"] = row.needs_review
    item["is_deleted"] = row.is_deleted
    item["confidence"] = row.confidence
    item["parse_attempts"] = row.parse_attempts
    item["discord_message_id"] = row.discord_message_id
    item["channel_name"] = row.channel_name
    item["item_names"] = json.loads(row.item_names_json or "[]")
    item["items_in"] = json.loads(row.items_in_json or "[]")
    item["items_out"] = json.loads(row.items_out_json or "[]")
    item["last_error"] = row.last_error
    back_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    if entry_kind:
        separator = "&" if "?" in back_url else "?"
        back_url = f"{back_url}{separator}entry_kind={entry_kind}"
    learning_signal = get_learning_signal(session, row.content or "")

    return templates.TemplateResponse(
        request,
        "deal_detail.html",
        {
            "request": request,
            "title": f"Deal {message_id}",
            "deal": item,
            "back_url": back_url,
            "success": success,
            "error": error,
            "current_user": getattr(request.state, "current_user", None),
            "parse_status_options": PARSE_STATUS_OPTIONS,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "correction_patterns": get_correction_pattern_counts(session=session),
            "learning_signal": learning_signal,
            "return_path": return_path,
            "selected_status": status or "",
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by or "",
            "selected_sort_dir": sort_dir or "",
            "selected_page": page or 1,
            "selected_limit": limit or 25,
        },
    )


@app.get("/login", response_class=HTMLResponse)
def login_page(
    request: Request,
    next: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    user = get_request_user(request)
    if user:
        return RedirectResponse(url=app_home_for_role(user.role), status_code=303)

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "title": "Sign In",
            "next_url": next or "",
            "error": error,
        },
    )


@app.post("/login")
def login_form(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    user = authenticate_user(session, username, password)
    if not user:
        return RedirectResponse(
            url=f"/login?error=Invalid+username+or+password&next={urlencode({'next': next or ''})[5:]}",
            status_code=303,
        )

    request.session["user_id"] = user.id
    if next and next.startswith("/") and not next.startswith("//"):
        redirect_target = next
    else:
        redirect_target = app_home_for_role(user.role)
    return RedirectResponse(url=redirect_target, status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


def validate_backfill_range(
    after: Optional[str],
    before: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[datetime], Optional[datetime]]:
    try:
        after_dt = parse_iso_datetime(after, end_of_day=False)
        before_dt = parse_iso_datetime(before, end_of_day=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid backfill date: {exc}") from exc

    if after_dt and before_dt and after_dt > before_dt:
        raise HTTPException(status_code=400, detail="Backfill 'after' must be before 'before'")

    return after, before, after_dt, before_dt


def queue_backfill_request(
    session: Session,
    request: Request,
    *,
    channel_id: Optional[str],
    after_dt: Optional[datetime],
    before_dt: Optional[datetime],
    limit: Optional[int],
    oldest_first: bool,
) -> str:
    queued = enqueue_backfill_request(
        session,
        channel_id=channel_id,
        after=after_dt,
        before=before_dt,
        limit_per_channel=limit,
        oldest_first=oldest_first,
        requested_by=current_user_label(request),
    )
    target = f"channel+{channel_id}" if channel_id else "all+backfill-enabled+watched+channels"
    return (
        f"Queued+backfill+request+{queued.id}+for+{target}."
        "+A+runtime+with+Discord+enabled+and+ready+will+claim+it,+and+the+request+will+be+requeued+if+a+deploy+restart+interrupts+the+run."
    )


def get_backfill_target_channel_ids(
    session: Session,
    *,
    channel_id: Optional[str],
) -> list[str]:
    if channel_id:
        return [str(channel_id)]

    return [
        row.channel_id
        for row in get_watched_channels(session)
        if row.backfill_enabled
    ]


def persist_backfill_window_for_targets(
    session: Session,
    *,
    channel_ids: list[str],
    after_dt: Optional[datetime],
    before_dt: Optional[datetime],
) -> None:
    if not channel_ids:
        return

    for target_channel_id in channel_ids:
        update_backfill_window(
            session,
            channel_id=target_channel_id,
            backfill_after=after_dt,
            backfill_before=before_dt,
        )
    session.commit()


def serialize_backfill_requests(rows: list) -> list[dict]:
    def parse_result(row) -> dict:
        try:
            return json.loads(row.result_json or "{}")
        except json.JSONDecodeError:
            return {}

    return [
        {
            "id": row.id,
            "channel_id": row.channel_id or "",
            "target_label": row.channel_id or "all backfill-enabled watched channels",
            "status": row.status,
            "requested_by": row.requested_by or "system",
            "after": format_pacific_datetime(row.after) if row.after else "no start",
            "before": format_pacific_datetime(row.before) if row.before else "no end",
            "inserted_count": row.inserted_count,
            "skipped_count": row.skipped_count,
            "progress": (parse_result(row).get("progress") or {}),
            "created_at": format_pacific_datetime(row.created_at),
            "started_at": format_pacific_datetime(row.started_at) if row.started_at else "",
            "finished_at": format_pacific_datetime(row.finished_at) if row.finished_at else "",
            "error_message": row.error_message or "",
            "detail_url": f"/ops-log/backfill/{row.id}" if row.id is not None else "",
            "can_cancel": row.status in {"queued", "processing"},
        }
        for row in rows
    ]


def serialize_operations_logs(rows: list) -> list[dict]:
    serialized: list[dict] = []
    for row in rows:
        details = parse_operations_log_details(row)
        message_id = coerce_int(details.get("message_id") or details.get("row_id") or details.get("source_message_id"))
        raw_channel_id = details.get("channel_id")
        if raw_channel_id is None:
            channel_id = None
        else:
            channel_id = str(raw_channel_id).strip() or None
        created_at = (
            details.get("message_created_at")
            or details.get("row_created_at")
            or details.get("created_at")
            or row.created_at
        )
        serialized.append(
            {
                "id": row.id,
                "event_type": row.event_type,
                "level": row.level,
                "source": row.source,
                "message": row.message,
                "created_at": format_pacific_datetime(row.created_at),
                "details": details,
                "action_links": build_row_action_links(
                    message_id,
                    channel_id=channel_id,
                    created_at=created_at,
                    status=details.get("current_state") or details.get("parse_status"),
                ) if message_id or channel_id else {},
            }
        )
    return serialized


def summarize_message_snippet(message: Optional[str], *, limit: int = 120) -> str:
    text = (message or "").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "(no message content)"
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def serialize_reparse_run_summary(row: ReparseRun | None) -> dict | None:
    if row is None:
        return None
    return {
        "run_id": row.run_id,
        "status": row.status,
        "source": row.source,
        "reason": row.reason or "",
        "requested_at": format_pacific_datetime(row.requested_at),
        "finished_at": format_pacific_datetime(row.finished_at) if row.finished_at else "",
        "duration_ms": row.duration_ms,
        "channel_id": row.channel_id or "",
        "range_after": format_pacific_datetime(row.range_after) if row.range_after else "",
        "range_before": format_pacific_datetime(row.range_before) if row.range_before else "",
        "selected_count": row.selected_count,
        "queued_count": row.queued_count,
        "already_queued_count": row.already_queued_count,
        "skipped_reviewed_count": row.skipped_reviewed_count,
        "succeeded_count": row.succeeded_count,
        "failed_count": row.failed_count,
        "error_message": row.error_message or "",
    }


def serialize_reparse_runs(rows: list[ReparseRun]) -> list[dict]:
    return [
        {
            "run_id": row.run_id,
            "source": row.source,
            "reason": row.reason,
            "requested_at": format_pacific_datetime(row.requested_at),
            "finished_at": format_pacific_datetime(row.finished_at),
            "duration_ms": row.duration_ms,
            "range_after": format_pacific_datetime(row.range_after),
            "range_before": format_pacific_datetime(row.range_before),
            "channel_id": row.channel_id,
            "requested_statuses": json.loads(row.requested_statuses_json or "[]"),
            "include_reviewed": row.include_reviewed,
            "force_reviewed": row.force_reviewed,
            "selected_count": row.selected_count,
            "queued_count": row.queued_count,
            "already_queued_count": row.already_queued_count,
            "skipped_reviewed_count": row.skipped_reviewed_count,
            "succeeded_count": row.succeeded_count,
            "failed_count": row.failed_count,
            "first_message_id": row.first_message_id,
            "last_message_id": row.last_message_id,
            "first_message_created_at": format_pacific_datetime(row.first_message_created_at),
            "last_message_created_at": format_pacific_datetime(row.last_message_created_at),
            "status": row.status,
            "error_message": row.error_message,
        }
        for row in rows
    ]


def build_reparse_run_table_rows(rows: list[ReparseRun]) -> list[dict]:
    items = serialize_reparse_runs(rows)
    for item in items:
        range_after = item.get("range_after") or "none"
        range_before = item.get("range_before") or "none"
        item["range_label"] = f"{range_after} to {range_before}"
        item["force_used"] = bool(item.get("force_reviewed"))
        item["reparsed_count"] = int(item.get("succeeded_count") or 0)
        item["skipped_count"] = int(item.get("already_queued_count") or 0) + int(item.get("skipped_reviewed_count") or 0)
        duration_ms = item.get("duration_ms")
        if duration_ms is None:
            item["duration_label"] = ""
        elif duration_ms >= 1000:
            item["duration_label"] = f"{duration_ms / 1000:.1f}s"
        else:
            item["duration_label"] = f"{duration_ms}ms"
    return items


def get_runtime_status_pair(session: Session) -> tuple[dict, dict]:
    app_runtime = get_runtime_heartbeat_status(
        session,
        APP_HEARTBEAT_RUNTIME_NAME,
        runtime_label=APP_RUNTIME_LABEL,
        updated_at_formatter=format_pacific_datetime,
    )
    worker_runtime = get_runtime_heartbeat_status(
        session,
        WORKER_RUNTIME_NAME,
        runtime_label=WORKER_RUNTIME_LABEL,
        updated_at_formatter=format_pacific_datetime,
    )
    return app_runtime, worker_runtime


def get_database_health(session: Session) -> dict:
    checked_at = utcnow()
    health = {
        "ok": True,
        "status": "healthy",
        "label": "Healthy",
        "needs_attention": False,
        "alert_message": "",
        "checked_at": checked_at.isoformat(),
        "checked_at_label": format_pacific_datetime(checked_at),
    }

    try:
        session.exec(select(1)).one()
    except OperationalError as exc:
        error_text = str(exc)
        health["ok"] = False
        health["needs_attention"] = True
        health["status"] = "busy" if "database is locked" in error_text.lower() else "down"
        health["label"] = "Busy" if health["status"] == "busy" else "Down"
        if health["status"] == "busy":
            health["alert_message"] = (
                "SQLite is currently busy handling another write. "
                "The database is reachable, but some updates may retry briefly."
            )
        else:
            health["alert_message"] = f"Database probe failed: {error_text}"
        return health
    except Exception as exc:
        health["ok"] = False
        health["needs_attention"] = True
        health["status"] = "down"
        health["label"] = "Down"
        health["alert_message"] = f"Database probe failed: {exc}"
        return health

    if recent_db_failure():
        health["status"] = "degraded"
        health["label"] = "Busy"
        health["needs_attention"] = True
        health["alert_message"] = (
            "SQLite recently reported write contention. "
            "The database is reachable, but writes may still be retrying."
        )

    return health


def serialize_backfill_request_detail(row: BackfillRequest) -> dict:
    result = {}
    try:
        result = json.loads(row.result_json or "{}")
    except json.JSONDecodeError:
        result = {}
    progress = result.get("progress") or {}
    final_result = result.get("final_result") or result
    status = str(row.status or "queued")
    stage = str(progress.get("stage") or status)
    waiting_reason = str(progress.get("waiting_reason") or "").strip()
    terminal_statuses = {"completed", "failed", "cancelled"}
    is_terminal = status in terminal_statuses

    if status == "processing":
        worker_status_label = "Claimed by worker"
        worker_status_tone = "info"
    elif is_terminal:
        worker_status_label = "No longer claimed"
        worker_status_tone = "success" if status == "completed" else "warning"
    else:
        worker_status_label = "Not claimed yet"
        worker_status_tone = "warning" if stage == "waiting_for_discord" else "info"

    if is_terminal:
        discord_wait_label = "No"
        discord_wait_reason = ""
    elif stage == "waiting_for_discord":
        discord_wait_label = "Yes"
        discord_wait_reason = waiting_reason or "Discord client is not ready yet."
    else:
        discord_wait_label = "No"
        discord_wait_reason = ""

    if status == "queued":
        progress_label = (
            "Waiting for Discord"
            if stage == "waiting_for_discord"
            else "Waiting in backfill queue"
        )
    elif status == "processing":
        progress_label = (
            "Discovering messages"
            if stage == "discovering_messages"
            else "Preparing channel discovery"
        )
    elif status == "completed":
        progress_label = "Completed"
    elif status == "failed":
        progress_label = "Failed"
    elif status == "cancelled":
        progress_label = "Cancelled"
    else:
        progress_label = stage.replace("_", " ")

    if is_terminal:
        finished_label = format_pacific_datetime(row.finished_at) if row.finished_at else status
        last_progress_label = (
            format_pacific_datetime(progress.get("last_progress_at"))
            if progress.get("last_progress_at")
            else "Finished with no additional progress updates"
        )
    else:
        finished_label = "Not finished"
        last_progress_label = (
            format_pacific_datetime(progress.get("last_progress_at"))
            if progress.get("last_progress_at")
            else "No progress yet"
        )

    return {
        "id": row.id,
        "target_label": row.channel_id or "all backfill-enabled watched channels",
        "status": status,
        "requested_by": row.requested_by or "system",
        "after": format_pacific_datetime(row.after) if row.after else "no start",
        "before": format_pacific_datetime(row.before) if row.before else "no end",
        "inserted_count": row.inserted_count,
        "skipped_count": row.skipped_count,
        "created_at": format_pacific_datetime(row.created_at),
        "started_at": format_pacific_datetime(row.started_at) if row.started_at else "",
        "finished_at": format_pacific_datetime(row.finished_at) if row.finished_at else "",
        "finished_label": finished_label,
        "error_message": row.error_message or "",
        "progress": progress,
        "progress_label": progress_label,
        "last_progress_label": last_progress_label,
        "worker_status_label": worker_status_label,
        "worker_status_tone": worker_status_tone,
        "discord_wait_label": discord_wait_label,
        "discord_wait_reason": discord_wait_reason,
        "result": final_result,
        "can_cancel": row.status in {"queued", "processing"},
    }


def build_backfill_queue_snapshot(session: Session) -> dict:
    def _count_status(st: str) -> int:
        return int(
            session.exec(
                select(func.count()).select_from(BackfillRequest).where(BackfillRequest.status == st)
            ).one()
        )

    queued = _count_status("queued")
    processing = _count_status("processing")
    completed = _count_status("completed")
    failed = _count_status("failed")
    cancelled = _count_status("cancelled")
    backlog = queued + processing
    if processing > 0 and queued > 0:
        queue_health_label = f"{processing} running, {queued} waiting"
    elif processing > 0:
        queue_health_label = f"{processing} running, no waiting backlog"
    elif queued > 0:
        queue_health_label = f"{queued} waiting, none claimed"
    else:
        queue_health_label = "No backfill requests queued or running"

    if processing > 0:
        worker_claim_label = f"{processing} request(s) claimed by a worker"
    elif queued > 0:
        worker_claim_label = "No queued request is claimed yet"
    else:
        worker_claim_label = "No backfill worker claim needed right now"

    return {
        "queued": queued,
        "processing": processing,
        "completed": completed,
        "failed": failed,
        "cancelled": cancelled,
        "queue_backlog": backlog,
        "queue_is_moving": processing > 0,
        "queue_health_label": queue_health_label,
        "worker_claim_label": worker_claim_label,
    }


def recompute_financial_fields(session: Session) -> int:
    rows = session.exec(
        select(DiscordMessage).where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED]))
            )
        )
    ).all()

    updated = 0
    for row in rows:
        financials = compute_financials(
            parsed_type=row.deal_type,
            parsed_category=row.category,
            amount=row.amount,
            cash_direction=row.cash_direction,
            message_text=row.content or "",
        )
        row.entry_kind = financials.entry_kind
        row.money_in = financials.money_in
        row.money_out = financials.money_out
        row.expense_category = financials.expense_category
        session.add(row)
        sync_transaction_from_message(session, row)
        updated += 1

    session.commit()
    return updated


def csv_response(filename: str, rows: list[dict]) -> Response:
    buffer = StringIO()
    fieldnames = list(rows[0].keys()) if rows else ["message"]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    return Response(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def build_bar_chart_rows(values: dict[str, float]) -> list[dict]:
    if not values:
        return []
    max_value = max(abs(float(value)) for value in values.values()) or 1.0
    return [
        {
            "label": label,
            "value": round(float(value), 2),
            "width_pct": round((abs(float(value)) / max_value) * 100, 1),
        }
        for label, value in values.items()
    ]


def get_parser_progress(
    session: Session,
    *,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> dict:
    stmt = build_message_stmt(
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    summary_subquery = stmt.order_by(None).subquery()

    raw_status_counts = session.exec(
        select(summary_subquery.c.parse_status, func.count())
        .group_by(summary_subquery.c.parse_status)
    ).all()
    status_counts: dict[str, int] = {}
    for raw_status, count in raw_status_counts:
        normalized_status = normalize_parse_status(raw_status)
        status_counts[normalized_status] = status_counts.get(normalized_status, 0) + int(count)
    total = sum(status_counts.values())
    parsed = status_counts.get(PARSE_PARSED, 0)
    processing = status_counts.get(PARSE_PROCESSING, 0)
    queued = status_counts.get(PARSE_PENDING, 0)
    failed = status_counts.get(PARSE_FAILED, 0)
    needs_review = status_counts.get(PARSE_REVIEW_REQUIRED, 0)
    ignored = status_counts.get(PARSE_IGNORED, 0)
    completed = parsed + needs_review + failed + ignored
    pending = queued + processing
    percent_complete = round((completed / total) * 100, 1) if total else 100.0
    processing_ids = [
        row_id
        for row_id in session.exec(
            select(summary_subquery.c.id).where(summary_subquery.c.parse_status == PARSE_PROCESSING)
        ).all()
        if row_id is not None
    ]
    processing_with_images = int(
        session.exec(
            select(func.count())
            .select_from(summary_subquery)
            .where(
                summary_subquery.c.parse_status == PARSE_PROCESSING,
                summary_subquery.c.attachment_urls_json != "[]",
            )
        ).one()
    ) if processing else 0
    recovered_stale = int(
        session.exec(
            select(func.count())
            .select_from(summary_subquery)
            .where(summary_subquery.c.last_error.like("Recovered from stale processing state%"))
        ).one()
    ) if total else 0

    oldest_processing_started_at = None
    oldest_processing_age_seconds = None
    stale_processing = 0
    usage_aggregate = session.exec(
        select(
            func.count(ParseAttempt.id),
            func.coalesce(func.sum(ParseAttempt.input_tokens), 0),
            func.coalesce(func.sum(ParseAttempt.cached_input_tokens), 0),
            func.coalesce(func.sum(ParseAttempt.output_tokens), 0),
            func.coalesce(func.sum(ParseAttempt.total_tokens), 0),
            func.coalesce(func.sum(ParseAttempt.estimated_cost_usd), 0.0),
        )
        .join(summary_subquery, ParseAttempt.message_id == summary_subquery.c.id)
    ).one()
    usage_summary = {
        "attempts": int(usage_aggregate[0] or 0),
        "input_tokens": int(usage_aggregate[1] or 0),
        "cached_input_tokens": int(usage_aggregate[2] or 0),
        "output_tokens": int(usage_aggregate[3] or 0),
        "total_tokens": int(usage_aggregate[4] or 0),
        "estimated_cost_usd": round(float(usage_aggregate[5] or 0.0), 4),
    }

    if processing_ids:
        unfinished_attempts = session.exec(
            select(ParseAttempt).where(
                ParseAttempt.message_id.in_(processing_ids),
                ParseAttempt.finished_at == None,  # noqa: E711
            )
        ).all()
        attempt_started_times = []
        now = utcnow()
        for attempt in unfinished_attempts:
            started_at = attempt.started_at
            if started_at is None:
                continue
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            attempt_started_times.append(started_at)
            if started_at <= now - STALE_PROCESSING_AFTER:
                stale_processing += 1
        if attempt_started_times:
            oldest_processing_started_at = min(attempt_started_times)
            oldest_processing_age_seconds = max(
                0,
                int((now - oldest_processing_started_at).total_seconds()),
            )

    oldest_processing_age_label = "none"
    if oldest_processing_age_seconds is not None:
        minutes = oldest_processing_age_seconds // 60
        hours, minutes = divmod(minutes, 60)
        if hours:
            oldest_processing_age_label = f"{hours}h {minutes}m"
        else:
            oldest_processing_age_label = f"{minutes}m"

    return {
        "total": total,
        "completed": completed,
        "parsed": parsed,
        "needs_review": needs_review,
        "failed": failed,
        "ignored": ignored,
        "processing": processing,
        "queued": queued,
        "pending": pending,
        "percent_complete": percent_complete,
        "is_active": pending > 0,
        "processing_with_images": processing_with_images,
        "stale_processing": stale_processing,
        "recovered_stale": recovered_stale,
        "oldest_processing_age_seconds": oldest_processing_age_seconds,
        "oldest_processing_age_label": oldest_processing_age_label,
        "stale_threshold_minutes": int(STALE_PROCESSING_AFTER.total_seconds() // 60),
        "usage": usage_summary,
        "local_runtime": get_runtime_heartbeat_status(
            session,
            WORKER_RUNTIME_NAME,
            runtime_label=WORKER_RUNTIME_LABEL,
            updated_at_formatter=format_pacific_datetime,
        ),
    }


def build_status_snapshot(session: Session) -> dict:
    app_runtime, worker_runtime = get_runtime_status_pair(session)
    db_health = get_database_health(session)
    parser_progress = get_parser_progress(session)
    tiktok_sync = build_tiktok_status_snapshot(session)
    background_task_alerts = _background_task_alert_messages()
    latest_ingested_row = session.exec(
        select(DiscordMessage)
        .order_by(DiscordMessage.ingested_at.desc())
        .limit(1)
    ).first()
    latest_reviewed_row = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.reviewed_at != None)  # noqa: E711
        .order_by(DiscordMessage.reviewed_at.desc())
        .limit(1)
    ).first()
    latest_parse_finished_attempt = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.finished_at != None)  # noqa: E711
        .order_by(ParseAttempt.finished_at.desc())
        .limit(1)
    ).first()
    latest_parse_finished_row = (
        session.get(DiscordMessage, latest_parse_finished_attempt.message_id)
        if latest_parse_finished_attempt and latest_parse_finished_attempt.message_id is not None
        else None
    )
    queue_action_links = build_queue_action_links()

    queue_backlog = parser_progress["queued"] + parser_progress["processing"]
    if parser_progress["processing"] > 0:
        queue_state_label = "Processing"
        queue_state_detail = "Worker is actively handling queued rows."
    elif parser_progress["queued"] > 0:
        queue_state_label = "Waiting"
        queue_state_detail = "Rows are queued but not yet in flight."
    else:
        queue_state_label = "Idle"
        queue_state_detail = "No parser backlog is waiting right now."

    split_runtime_notice = ""
    if app_runtime["is_running"] and not worker_runtime["is_running"]:
        split_runtime_notice = (
            "Web UI is running, but the separate worker process has not reported a heartbeat yet. "
            "If you are using the split local setup, start the worker terminal with scripts/run_local_worker.ps1."
        )
    elif worker_runtime["is_running"] and not app_runtime["is_running"]:
        split_runtime_notice = (
            "Worker is running, but the web UI has not reported a heartbeat yet. "
            "Start the web terminal with scripts/run_local_web.ps1."
        )
    elif not app_runtime["is_running"] and not worker_runtime["is_running"]:
        split_runtime_notice = "Neither local process has reported a heartbeat yet."

    recovery_enabled = bool(settings.startup_backfill_enabled)
    recovery_window_hours = float(settings.startup_backfill_lookback_hours or 0.0)
    recovery_window_label = f"last {recovery_window_hours:g}h" if recovery_window_hours else "the recent window"
    if recovery_enabled:
        if worker_runtime["is_running"]:
            recovery_status_label = "Ready"
            recovery_status_detail = (
                f"Recent-message recovery is enabled and ready to backfill the {recovery_window_label} "
                "of watched channels on worker startup."
            )
            recovery_status_tone = "ok"
        else:
            recovery_status_label = "Waiting"
            recovery_status_detail = (
                f"Recent-message recovery is enabled, but the worker is offline. Start the worker "
                f"to let it backfill the {recovery_window_label} of watched channels on startup."
            )
            recovery_status_tone = "warn"
    else:
        recovery_status_label = "Off"
        recovery_status_detail = (
            "Recent-message recovery is disabled, so downtime gaps will not be recovered automatically."
        )
        recovery_status_tone = "bad"

    stitch_enabled = bool(settings.stitch_enabled)
    stitch_window_seconds = int(settings.stitch_window_seconds or 0)
    stitch_max_messages = int(settings.stitch_max_messages or 0)
    stitch_window_label = f"last {stitch_window_seconds}s" if stitch_window_seconds else "recent nearby messages"
    stitch_limit_label = f"up to {stitch_max_messages} messages" if stitch_max_messages else "the configured message limit"
    if stitch_enabled:
        if worker_runtime["is_running"]:
            stitch_status_label = "Ready"
            stitch_status_detail = (
                f"Stitch recovery is enabled and active while the worker is running; it groups {stitch_limit_label} "
                f"within {stitch_window_label}."
            )
            stitch_status_tone = "ok"
        else:
            stitch_status_label = "Waiting"
            stitch_status_detail = (
                f"Stitch recovery is enabled, but the worker is offline. Start the worker to let it stitch "
                f"{stitch_limit_label} within {stitch_window_label}."
            )
            stitch_status_tone = "warn"
    else:
        stitch_status_label = "Off"
        stitch_status_detail = "Stitch recovery is disabled, so nearby messages will stay separate."
        stitch_status_tone = "bad"

    return {
        "db_ok": db_health["ok"] and not db_health["needs_attention"],
        "db_health": db_health,
        "local_runtime": worker_runtime,
        "app_runtime": app_runtime,
        "worker_runtime": worker_runtime,
        "parser_progress": parser_progress,
        "queue_backlog": queue_backlog,
        "queue_is_moving": parser_progress["processing"] > 0 or parser_progress["queued"] == 0,
        "queue_state_label": queue_state_label,
        "queue_state_detail": queue_state_detail,
        "split_runtime_notice": split_runtime_notice,
        "recent_message_recovery": {
            "enabled": recovery_enabled,
            "status_label": recovery_status_label,
            "status_detail": recovery_status_detail,
            "status_tone": recovery_status_tone,
            "window_label": recovery_window_label,
        },
        "tiktok_sync": tiktok_sync,
        "stitch_recovery": {
            "enabled": stitch_enabled,
            "status_label": stitch_status_label,
            "status_detail": stitch_status_detail,
            "status_tone": stitch_status_tone,
            "window_label": stitch_window_label,
            "limit_label": stitch_limit_label,
        },
        "alert_messages": [
            message
            for message in (
                db_health.get("alert_message"),
                app_runtime.get("alert_message"),
                worker_runtime.get("alert_message"),
                *background_task_alerts,
            )
            if message
        ],
        "recent_activity": {
            "latest_ingested_label": format_pacific_datetime(latest_ingested_row.ingested_at if latest_ingested_row else None),
            "latest_ingested_links": build_row_action_links(
                latest_ingested_row.id if latest_ingested_row else None,
                channel_id=latest_ingested_row.channel_id if latest_ingested_row else None,
                created_at=latest_ingested_row.created_at if latest_ingested_row else None,
                status=normalize_parse_status(latest_ingested_row.parse_status, is_deleted=latest_ingested_row.is_deleted, needs_review=latest_ingested_row.needs_review) if latest_ingested_row else None,
            ),
            "latest_reviewed_label": format_pacific_datetime(latest_reviewed_row.reviewed_at if latest_reviewed_row else None),
            "latest_reviewed_links": build_row_action_links(
                latest_reviewed_row.id if latest_reviewed_row else None,
                channel_id=latest_reviewed_row.channel_id if latest_reviewed_row else None,
                created_at=latest_reviewed_row.created_at if latest_reviewed_row else None,
                status=normalize_parse_status(latest_reviewed_row.parse_status, is_deleted=latest_reviewed_row.is_deleted, needs_review=latest_reviewed_row.needs_review) if latest_reviewed_row else None,
            ),
            "latest_parse_finished_label": format_pacific_datetime(latest_parse_finished_attempt.finished_at if latest_parse_finished_attempt else None),
            "latest_parse_finished_links": build_row_action_links(
                latest_parse_finished_attempt.message_id if latest_parse_finished_attempt else None,
                channel_id=latest_parse_finished_row.channel_id if latest_parse_finished_row else None,
                created_at=latest_parse_finished_row.created_at if latest_parse_finished_row else None,
                status=normalize_parse_status(latest_parse_finished_row.parse_status, is_deleted=latest_parse_finished_row.is_deleted, needs_review=latest_parse_finished_row.needs_review) if latest_parse_finished_row else None,
            ),
        },
        "runtime_flags": {
            "discord_ingest_enabled": bool(worker_runtime["details"].get("discord_ingest_enabled", settings.discord_ingest_enabled)),
            "parser_worker_enabled": bool(worker_runtime["details"].get("parser_worker_enabled", settings.parser_worker_enabled)),
        },
        "action_links": queue_action_links,
    }


def build_debug_snapshot(session: Session) -> dict:
    parser_progress = get_parser_progress(session)
    app_runtime, worker_runtime = get_runtime_status_pair(session)

    queue_counts = {
        PARSE_PENDING: 0,
        PARSE_PROCESSING: 0,
        PARSE_PARSED: 0,
        PARSE_REVIEW_REQUIRED: 0,
        PARSE_FAILED: 0,
        PARSE_IGNORED: 0,
    }
    for raw_status, count in session.exec(
        select(DiscordMessage.parse_status, func.count(DiscordMessage.id))
        .group_by(DiscordMessage.parse_status)
    ).all():
        normalized = normalize_parse_status(raw_status)
        if normalized in queue_counts:
            queue_counts[normalized] += count

    processing_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.parse_status == PARSE_PROCESSING)
        .order_by(DiscordMessage.created_at.asc(), DiscordMessage.id.asc())
    ).all()
    cutoff = utcnow() - STALE_PROCESSING_AFTER
    stuck_processing: list[dict] = []
    for row in processing_rows:
        attempts = session.exec(
            select(ParseAttempt)
            .where(ParseAttempt.message_id == row.id)
            .order_by(ParseAttempt.started_at.desc(), ParseAttempt.id.desc())
        ).all()
        latest_attempt = attempts[0] if attempts else None
        last_attempted_at = latest_attempt.started_at if latest_attempt else None
        comparison_time = last_attempted_at or row.edited_at or row.ingested_at or row.created_at
        if comparison_time.tzinfo is None:
            comparison_time = comparison_time.replace(tzinfo=timezone.utc)
        if comparison_time > cutoff:
            continue
        stuck_processing.append(
            {
                "message_id": row.id,
                "discord_message_id": row.discord_message_id,
                "channel": row.channel_name,
                "channel_id": row.channel_id,
                "current_status": normalize_parse_status(row.parse_status),
                "last_updated_label": format_pacific_datetime(row.edited_at or row.ingested_at or row.created_at),
                "last_attempted_at_label": format_pacific_datetime(last_attempted_at),
                "retry_count": row.parse_attempts or len(attempts),
                "latest_error": row.last_error or (latest_attempt.error if latest_attempt else None),
                "action_links": build_row_action_links(
                    row.id,
                    channel_id=row.channel_id,
                    created_at=row.created_at,
                    status=normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review),
                ),
            }
        )

    recent_worker_failures = serialize_operations_logs(
        session.exec(
            select(OperationsLog)
            .where(OperationsLog.source == "worker")
            .where(OperationsLog.level.in_(["warning", "error"]))
            .order_by(OperationsLog.created_at.desc(), OperationsLog.id.desc())
            .limit(20)
        ).all()
    )
    recent_backfill_failures = serialize_operations_logs(
        session.exec(
            select(OperationsLog)
            .where(OperationsLog.event_type.like("backfill%"))
            .where(OperationsLog.level == "error")
            .order_by(OperationsLog.created_at.desc(), OperationsLog.id.desc())
            .limit(10)
        ).all()
    )

    return {
        "app_runtime": app_runtime,
        "worker_runtime": worker_runtime,
        "parser_progress": parser_progress,
        "queue_counts": {
            "pending": queue_counts[PARSE_PENDING],
            "processing": queue_counts[PARSE_PROCESSING],
            "parsed": queue_counts[PARSE_PARSED],
            "review_required": queue_counts[PARSE_REVIEW_REQUIRED],
            "failed": queue_counts[PARSE_FAILED],
            "ignored": queue_counts[PARSE_IGNORED],
        },
        "stuck_processing": stuck_processing,
        "recent_worker_failures": recent_worker_failures,
        "recent_backfill_failures": recent_backfill_failures,
        "worker_actively_draining_queue": parser_progress["processing"] > 0 or parser_progress["queued"] == 0,
        "log_paths": {
            "app": normalize_filesystem_path(resolve_runtime_log_path("app.log")),
            "worker": normalize_filesystem_path(resolve_runtime_log_path("worker.log")),
        },
        "action_links": build_queue_action_links(),
    }


def build_health_snapshot(session: Session) -> dict:
    debug = build_debug_snapshot(session)

    recent_failed_rows_query = (
        select(DiscordMessage, ParseAttempt)
        .join(ParseAttempt, ParseAttempt.message_id == DiscordMessage.id, isouter=True)
        .where(DiscordMessage.parse_status == PARSE_FAILED)
        .order_by(ParseAttempt.finished_at.desc(), ParseAttempt.id.desc(), DiscordMessage.id.desc())
        .limit(30)
    )
    recent_failed_rows_raw = session.exec(recent_failed_rows_query).all()
    recent_failed_rows: list[dict] = []
    seen_message_ids: set[int] = set()
    for row, attempt in recent_failed_rows_raw:
        if row.id is None or row.id in seen_message_ids:
            continue
        seen_message_ids.add(row.id)
        recent_failed_rows.append(
            {
                "message_id": row.id,
                "channel": row.channel_name or row.channel_id,
                "author": row.author_name or "",
                "status": normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review),
                "snippet": summarize_message_snippet(row.content),
                "error_reason": row.last_error or (attempt.error if attempt else "") or "unknown",
                "last_attempted_at": format_pacific_datetime(
                    attempt.finished_at or attempt.started_at if attempt else row.edited_at or row.ingested_at or row.created_at
                ),
            }
        )
        if len(recent_failed_rows) >= 15:
            break

    latest_reparse_run = serialize_reparse_run_summary(
        session.exec(
            select(ReparseRun)
            .order_by(ReparseRun.requested_at.desc(), ReparseRun.id.desc())
            .limit(1)
        ).first()
    )

    return {
        "queue_counts": debug["queue_counts"],
        "stuck_processing": debug["stuck_processing"],
        "recent_failed_rows": recent_failed_rows,
        "last_reparse_run": latest_reparse_run,
    }


def format_dashboard_money(value: float) -> str:
    amount = round(float(value or 0.0), 2)
    if abs(amount) >= 1000:
        return f"${amount:,.0f}"
    if amount == int(amount):
        return f"${amount:,.0f}"
    return f"${amount:,.2f}"


def build_dashboard_snapshot(session: Session) -> dict:
    now_pacific = datetime.now(PACIFIC_TZ)
    today_start = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + timedelta(days=1)
    today_start_utc = today_start.astimezone(timezone.utc)
    tomorrow_start_utc = tomorrow_start.astimezone(timezone.utc)

    today_rows = get_financial_rows(
        session,
        start=today_start_utc,
        end=tomorrow_start_utc,
    )
    today_summary = build_financial_summary(today_rows)
    today_counts = today_summary["counts"]
    today_totals = today_summary["totals"]

    recent_ingested_count = int(
        session.exec(
            select(func.count())
            .select_from(DiscordMessage)
            .where(DiscordMessage.ingested_at >= today_start_utc)
        ).one()
    )
    reviewed_today_count = int(
        session.exec(
            select(func.count())
            .select_from(DiscordMessage)
            .where(DiscordMessage.reviewed_at != None)  # noqa: E711
            .where(DiscordMessage.reviewed_at >= today_start_utc)
        ).one()
    )

    top_channel_rows = session.exec(
        select(
            DiscordMessage.channel_id,
            func.coalesce(func.max(DiscordMessage.channel_name), DiscordMessage.channel_id),
            func.count(),
            func.coalesce(func.sum(DiscordMessage.money_in), 0.0),
            func.coalesce(func.sum(DiscordMessage.money_out), 0.0),
        )
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED]))
            )
        )
        .where(
            (DiscordMessage.stitched_group_id == None) | (DiscordMessage.stitched_primary == True)
        )
        .where(DiscordMessage.created_at >= today_start_utc)
        .where(DiscordMessage.created_at < tomorrow_start_utc)
        .group_by(DiscordMessage.channel_id)
        .order_by(func.count().desc(), func.max(DiscordMessage.created_at).desc())
        .limit(5)
    ).all()

    top_channels = [
        {
            "channel_id": channel_id,
            "channel_name": channel_name or channel_id,
            "deal_count": int(deal_count or 0),
            "money_in": float(money_in or 0.0),
            "money_out": float(money_out or 0.0),
            "money_in_display": format_dashboard_money(float(money_in or 0.0)),
            "money_out_display": format_dashboard_money(float(money_out or 0.0)),
            "net_display": format_dashboard_money(float(money_in or 0.0) - float(money_out or 0.0)),
        }
        for channel_id, channel_name, deal_count, money_in, money_out in top_channel_rows
    ]

    payment_mix = [
        {"label": label.replace("_", " ").title(), "value": format_dashboard_money(amount)}
        for label, amount in list(today_summary["payment_methods"].items())[:4]
    ]
    category_mix = [
        {"label": label.replace("_", " ").title(), "value": format_dashboard_money(amount)}
        for label, amount in list(today_summary["deal_categories"].items())[:4]
    ]
    discord_sales = round(float(today_totals.get("sales", 0.0) or 0.0), 2)
    discord_trade_in = round(float(today_totals.get("trade_cash_in", 0.0) or 0.0), 2)
    discord_buys = round(float(today_totals.get("buys", 0.0) or 0.0), 2)
    discord_trade_out = round(float(today_totals.get("trade_cash_out", 0.0) or 0.0), 2)
    discord_expenses = round(float(today_totals.get("expenses", 0.0) or 0.0), 2)
    shopify_today = {
        "order_count": 0,
        "gross": 0.0,
        "tax": 0.0,
        "net": 0.0,
        "refunds": 0.0,
        "tax_missing_count": 0,
        "includes_tax_count": 0,
        "gross_display": format_dashboard_money(0.0),
        "tax_display": format_dashboard_money(0.0),
        "net_display": format_dashboard_money(0.0),
        "refunds_display": format_dashboard_money(0.0),
    }
    try:
        shopify_rows_today = session.exec(
            select(ShopifyOrder)
            .where(ShopifyOrder.created_at >= today_start_utc)
            .where(ShopifyOrder.created_at < tomorrow_start_utc)
        ).all()
        for order in shopify_rows_today:
            status = (order.financial_status or "").strip().lower()
            if status == "paid":
                shopify_today["order_count"] += 1
                gross_value = float(order.total_price or 0.0)
                shopify_today["gross"] += gross_value
                if order.total_tax is None:
                    shopify_today["tax_missing_count"] += 1
                else:
                    shopify_today["tax"] += float(order.total_tax or 0.0)
                if order.subtotal_ex_tax is None:
                    shopify_today["net"] += gross_value
                    shopify_today["includes_tax_count"] += 1
                else:
                    shopify_today["net"] += float(order.subtotal_ex_tax or 0.0)
            elif status == "refunded":
                shopify_today["refunds"] += float(order.total_price or 0.0)
        for key in ("gross", "tax", "net", "refunds"):
            shopify_today[key] = round(float(shopify_today[key] or 0.0), 2)
    except OperationalError:
        pass
    shopify_today["gross_display"] = format_dashboard_money(shopify_today["gross"])
    shopify_today["tax_display"] = format_dashboard_money(shopify_today["tax"])
    shopify_today["net_display"] = format_dashboard_money(shopify_today["net"])
    shopify_today["refunds_display"] = format_dashboard_money(shopify_today["refunds"])
    revenue = {
        "discord_sales": discord_sales,
        "discord_trade_in": discord_trade_in,
        "discord_total": round(discord_sales + discord_trade_in, 2),
        "shopify_total": round(float(shopify_today["net"] or 0.0), 2),
    }
    revenue["total"] = round(revenue["discord_total"] + revenue["shopify_total"], 2)
    purchases = {
        "buys": discord_buys,
        "trade_out": discord_trade_out,
        "expenses": discord_expenses,
        "shopify_refunds": round(float(shopify_today["refunds"] or 0.0), 2),
    }
    purchases["total"] = round(
        purchases["buys"] + purchases["trade_out"] + purchases["expenses"] + purchases["shopify_refunds"],
        2,
    )

    return {
        "today_label": today_start.strftime("%A, %b %d"),
        "today": {
            "rows": today_summary["rows"],
            "sales_count": int(today_counts.get("sale", 0)),
            "buy_count": int(today_counts.get("buy", 0)),
            "trade_count": int(today_counts.get("trade", 0)),
            "expense_count": int(today_counts.get("expense", 0)),
            "needs_review_count": int(today_counts.get("needs_review", 0)),
            "reviewed_today_count": reviewed_today_count,
            "recent_ingested_count": recent_ingested_count,
            "sales_display": format_dashboard_money(today_totals.get("sales", 0.0)),
            "buys_display": format_dashboard_money(today_totals.get("buys", 0.0)),
            "trade_in_display": format_dashboard_money(today_totals.get("trade_cash_in", 0.0)),
            "trade_out_display": format_dashboard_money(today_totals.get("trade_cash_out", 0.0)),
            "revenue": {
                **revenue,
                "total_display": format_dashboard_money(revenue["total"]),
                "discord_total_display": format_dashboard_money(revenue["discord_total"]),
                "discord_sales_display": format_dashboard_money(revenue["discord_sales"]),
                "discord_trade_in_display": format_dashboard_money(revenue["discord_trade_in"]),
                "shopify_total_display": format_dashboard_money(revenue["shopify_total"]),
            },
            "purchases": {
                **purchases,
                "total_display": format_dashboard_money(purchases["total"]),
                "buys_display": format_dashboard_money(purchases["buys"]),
                "trade_out_display": format_dashboard_money(purchases["trade_out"]),
                "expenses_display": format_dashboard_money(purchases["expenses"]),
                "shopify_refunds_display": format_dashboard_money(purchases["shopify_refunds"]),
                "has_shopify_refunds": purchases["shopify_refunds"] > 0,
            },
            "shopify": shopify_today,
        },
        "top_channels": top_channels,
        "payment_mix": payment_mix,
        "category_mix": category_mix,
    }


@app.get("/channels")
def list_channels(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    return get_channel_filter_choices(session)


@app.get("/messages")
def list_messages(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    return {
        "rows": build_message_list_items(session, rows),
        "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
    }


@app.get("/api/review")
def review_queue_api(
    request: Request,
    status: Optional[str] = Query(default="review_queue"),
    channel_id: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    return {
        "rows": build_message_list_items(session, rows),
        "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
    }


@app.get("/messages/{message_id}")
def get_message(request: Request, message_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    target = f"/deals/{message_id}" if row else "/deals"
    return RedirectResponse(url=target, status_code=301)


@app.get("/admin/parser-progress")
def admin_parser_progress(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return get_parser_progress(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
    )


@app.get("/admin/queue-state-counts")
def admin_queue_state_counts(
    request: Request,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return {
        "counts": get_summary(
            session,
            status=status,
            channel_id=channel_id,
            entry_kind=entry_kind,
            expense_category=expense_category,
            after=after,
            before=before,
        ),
        "progress": get_parser_progress(
            session,
            status=status,
            channel_id=channel_id,
            entry_kind=entry_kind,
            expense_category=expense_category,
            after=after,
            before=before,
        ),
    }


@app.get("/table/messages/{message_id}", response_class=HTMLResponse)
def message_detail_page(
    message_id: int,
    request: Request,
    return_path: str = Query(default="/table"),
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.get(DiscordMessage, message_id)
    target = "/deals"
    if row:
        target = build_return_url(
            f"/deals/{message_id}",
            status=status,
            channel_id=channel_id,
            expense_category=expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
        separator = "&" if "?" in target else "?"
        if success:
            target = f"{target}{separator}success={success}"
            separator = "&"
        if error:
            target = f"{target}{separator}error={error}"
    return RedirectResponse(url=target, status_code=301)


@app.post("/admin/corrections/promote-form")
def promote_correction_form(
    request: Request,
    normalized_text: str = Form(...),
    return_to: str = Form(default="/table"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    promoted_count = promote_correction_pattern(session, normalized_text)
    session.commit()
    separator = "&" if "?" in return_to else "?"
    success = f"Promoted {promoted_count} correction pattern(s) into parser memory."
    return RedirectResponse(url=f"{return_to}{separator}success={success}", status_code=303)


@app.post("/messages/{message_id}/edit-form")
def edit_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    parse_status: str = Form(default=PARSE_PARSED),
    needs_review: Optional[str] = Form(default=None),
    deal_type: Optional[str] = Form(default=None),
    amount: Optional[str] = Form(default=None),
    payment_method: Optional[str] = Form(default=None),
    cash_direction: Optional[str] = Form(default=None),
    category: Optional[str] = Form(default=None),
    entry_kind: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    confidence: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    trade_summary: Optional[str] = Form(default=None),
    item_names_text: Optional[str] = Form(default=None),
    items_in_text: Optional[str] = Form(default=None),
    items_out_text: Optional[str] = Form(default=None),
    approve_after_save: Optional[str] = Form(default=None),
    stay_on_detail: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reviewer_label = current_user_label(request)
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")
    parsed_before = snapshot_message_parse(row)

    try:
        parsed_amount = parse_optional_float(amount)
        parsed_confidence = parse_optional_float(confidence)
    except ValueError:
        detail_url = build_return_url(
            f"/deals/{message_id}",
            status=status,
            channel_id=channel_id,
            expense_category=filter_expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
        separator = "&" if "?" in detail_url else "?"
        return RedirectResponse(
            url=f"{detail_url}{separator}error=Amount+and+confidence+must+be+valid+numbers",
            status_code=303,
        )

    row.parse_status = normalize_parse_status(parse_status or PARSE_PARSED)
    row.needs_review = bool(needs_review)
    if row.parse_status == PARSE_REVIEW_REQUIRED or row.needs_review:
        row.parse_status = PARSE_REVIEW_REQUIRED
        row.needs_review = True
    elif row.parse_status == PARSE_PARSED:
        row.needs_review = False

    normalized_deal_type = (deal_type or "").strip() or None
    normalized_payment_method = (payment_method or "").strip() or None
    normalized_cash_direction = row.cash_direction if cash_direction is None else ((cash_direction or "").strip() or None)
    normalized_category = (category or "").strip() or None
    normalized_entry_kind = (entry_kind or "").strip() or None
    normalized_expense_category = (expense_category or "").strip() or None
    if normalized_deal_type != "trade":
        normalized_cash_direction = None

    row.deal_type = normalized_deal_type
    row.amount = parsed_amount
    row.payment_method = normalized_payment_method
    row.cash_direction = normalized_cash_direction
    row.category = normalized_category
    row.notes = (notes or "").strip() or None
    row.trade_summary = (trade_summary or "").strip() or None
    row.confidence = parsed_confidence
    row.item_names_json = json.dumps(parse_string_list(item_names_text))
    row.items_in_json = json.dumps(parse_string_list(items_in_text))
    row.items_out_json = json.dumps(parse_string_list(items_out_text))

    entry_kind_value, money_in, money_out, expense_category_value = compute_manual_financials(
        row=row,
        deal_type=normalized_deal_type,
        category=normalized_category,
        amount=parsed_amount,
        cash_direction=normalized_cash_direction,
        entry_kind_override=normalized_entry_kind,
        expense_category_override=normalized_expense_category,
    )
    row.entry_kind = entry_kind_value
    row.money_in = money_in
    row.money_out = money_out
    row.expense_category = expense_category_value
    if approve_after_save:
        row.parse_status = PARSE_PARSED
        row.needs_review = False
    if row.parse_status == PARSE_PARSED and not row.needs_review:
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
    elif row.parse_status != PARSE_PARSED or row.needs_review:
        row.reviewed_by = None
        row.reviewed_at = None
    row.last_error = None if row.parse_status in {PARSE_PARSED, PARSE_REVIEW_REQUIRED} else row.last_error

    session.add(row)
    save_review_correction(session, row, parsed_before=parsed_before)
    sync_transaction_from_message(session, row)
    session.commit()

    redirect_target = build_return_url(
        f"/deals/{message_id}" if stay_on_detail else return_path,
        status=status,
        channel_id=channel_id,
        expense_category=filter_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_target else "?"
    success_message = (
        f"Saved+manual+correction+and+approved+message+{message_id}"
        if approve_after_save
        else f"Saved+manual+correction+for+message+{message_id}"
    )
    return RedirectResponse(
        url=f"{redirect_target}{separator}success={success_message}",
        status_code=303,
    )


@app.get("/reports/summary")
def report_summary(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    summary = build_financial_summary(rows)
    summary["filters"] = {
        "start": start_dt.isoformat() if start_dt else None,
        "end": end_dt.isoformat() if end_dt else None,
        "channel_id": channel_id,
    }
    return summary


@app.post("/webhooks/shopify/orders")
async def shopify_orders_webhook(request: Request):
    raw_body = await request.body()
    received_hmac = request.headers.get("X-Shopify-Hmac-Sha256")
    topic = request.headers.get("X-Shopify-Topic") or "unknown"
    webhook_runtime = f"{settings.runtime_name}_shopify"

    if not validate_shopify_webhook(
        raw_body=raw_body,
        shared_secret=settings.shopify_webhook_secret,
        received_hmac=received_hmac,
    ):
        body_hash = hashlib.sha256(raw_body).hexdigest()
        print(
            structured_log_line(
                runtime=webhook_runtime,
                action="shopify.webhook.rejected",
                success=False,
                error="Invalid Shopify webhook HMAC",
                topic=topic,
                request_path=str(request.url.path),
                body_sha256=body_hash,
            )
        )
        raise HTTPException(status_code=401, detail="Invalid Shopify webhook signature")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
        received_at = utcnow()
        def write_shopify_order(session: Session) -> str:
            return upsert_shopify_order(
                session,
                payload,
                source="webhook",
                received_at=received_at,
                runtime_name=webhook_runtime,
            )

        outcome = run_write_with_retry(write_shopify_order)
    except Exception as exc:
        print(
            structured_log_line(
                runtime=webhook_runtime,
                action="shopify.webhook.failed",
                success=False,
                error=str(exc),
                topic=topic,
            )
        )
        raise HTTPException(status_code=400, detail="Invalid Shopify payload") from exc

    print(
        structured_log_line(
            runtime=webhook_runtime,
            action="shopify.webhook.received",
            success=True,
            topic=topic,
            shopify_order_id=payload.get("id"),
            order_number=payload.get("name") or payload.get("order_number"),
            operation=outcome,
        )
    )
    return Response(status_code=200)


@app.get("/integrations/tiktok/callback")
def tiktok_oauth_callback(request: Request):
    query_params = dict(request.query_params)
    runtime_name = f"{settings.runtime_name}_tiktok"
    received_at = utcnow()
    app_key = (query_params.get("app_key") or "").strip()
    code = (query_params.get("code") or "").strip()
    if not code:
        update_tiktok_integration_state(
            last_error="Missing authorization code",
            last_callback={
                "received_at": received_at.isoformat(),
                "query": summarize_tiktok_query_params(query_params),
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.oauth.callback.failed",
                success=False,
                error="Missing TikTok authorization code",
                request_path=str(request.url.path),
                query=summarize_tiktok_query_params(query_params),
            )
        )
        return RedirectResponse(
            url="/status?error=TikTok+callback+missing+authorization+code",
            status_code=303,
        )

    expected_app_key = (settings.tiktok_app_key or "").strip()
    if expected_app_key and app_key and app_key != expected_app_key:
        update_tiktok_integration_state(
            last_error="App key mismatch",
            last_callback={
                "received_at": received_at.isoformat(),
                "query": summarize_tiktok_query_params(query_params),
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.oauth.callback.failed",
                success=False,
                error="TikTok app key mismatch",
                request_path=str(request.url.path),
                query=summarize_tiktok_query_params(query_params),
            )
        )
        return RedirectResponse(
            url="/status?error=TikTok+callback+app+key+mismatch",
            status_code=303,
        )

    callback_summary = {
        "received_at": received_at.isoformat(),
        "query": summarize_tiktok_query_params(query_params),
    }
    missing_auth_config = [
        label
        for label, value in (
            ("app key", settings.tiktok_app_key),
            ("app secret", settings.tiktok_app_secret),
        )
        if not (value or "").strip()
    ]
    if not missing_auth_config:
        try:
            token_result = exchange_tiktok_authorization_code(
                auth_code=code,
                app_key=(settings.tiktok_app_key or "").strip(),
                app_secret=(settings.tiktok_app_secret or "").strip(),
                runtime_name=runtime_name,
            )

            def persist_tiktok_auth(session: Session):
                status, auth_record = upsert_tiktok_auth_from_callback(
                    session,
                    TikTokAuth,
                    token_result=token_result,
                    app_key=(settings.tiktok_app_key or "").strip(),
                    redirect_uri=(settings.tiktok_redirect_uri or "").strip(),
                    fallback_shop_id=(settings.tiktok_shop_id or "").strip(),
                    pending_key_seed=code,
                    source="oauth_callback",
                    received_at=received_at,
                    dry_run=False,
                )
                auth_row = session.exec(
                    select(TikTokAuth).where(
                        TikTokAuth.tiktok_shop_id == auth_record["tiktok_shop_id"]
                    )
                ).first()
                shop_name = auth_row.shop_name if auth_row is not None else None
                return status, auth_record, shop_name

            auth_status, auth_record, shop_name = run_write_with_retry(persist_tiktok_auth)
            auth_lookup_key = str(auth_record.get("tiktok_shop_id") or "").strip()
            auth_pending = auth_lookup_key.startswith("pending:")
            callback_summary["auth_status"] = auth_status
            callback_summary["shop_key_status"] = "pending" if auth_pending else "resolved"
            if auth_pending:
                callback_summary["pending_shop_key"] = auth_lookup_key
            else:
                callback_summary["shop_id"] = auth_lookup_key
            callback_summary["shop_region"] = query_params.get("shop_region") or auth_record.get("shop_region")
            if shop_name:
                callback_summary["shop_name"] = shop_name
        except Exception as exc:
            callback_summary["exchange_error"] = str(exc)
            update_tiktok_integration_state(
                last_error=str(exc),
                last_callback=callback_summary,
            )
            request.session["tiktok_callback"] = callback_summary
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.oauth.callback.exchange_failed",
                    success=False,
                    error=str(exc),
                    request_path=str(request.url.path),
                    query=summarize_tiktok_query_params(query_params),
                )
            )
            return RedirectResponse(
                url="/status?error=TikTok+authorization+exchange+failed",
                status_code=303,
            )
    else:
        missing_label = ", ".join(missing_auth_config)
        config_error = f"TikTok auth config missing: {missing_label}"
        callback_summary["exchange_error"] = config_error
        update_tiktok_integration_state(
            last_error=config_error,
            last_callback=callback_summary,
        )
        request.session["tiktok_callback"] = callback_summary
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.oauth.callback.misconfigured",
                success=False,
                error=config_error,
                request_path=str(request.url.path),
                query=summarize_tiktok_query_params(query_params),
            )
        )
        return RedirectResponse(
            url=f"/status?{urlencode({'error': config_error})}",
            status_code=303,
        )

    update_tiktok_integration_state(
        last_authorization_at=received_at,
        last_callback=callback_summary,
        last_error=None,
    )
    request.session["tiktok_callback"] = callback_summary
    print(
        structured_log_line(
            runtime=runtime_name,
            action="tiktok.oauth.callback.received",
            success=True,
            request_path=str(request.url.path),
            query=summarize_tiktok_query_params(query_params),
            shop_key_status=callback_summary.get("shop_key_status") or "resolved",
        )
    )
    success_message = "TikTok authorization captured"
    if callback_summary.get("shop_key_status") == "pending":
        success_message = "TikTok authorization captured; waiting for shop identifier"
    return RedirectResponse(
        url=f"/status?{urlencode({'success': success_message})}",
        status_code=303,
    )


@app.get("/integrations/tiktok/creator-callback")
def tiktok_creator_oauth_callback(request: Request):
    """Handle Creator-type OAuth callback — stores creator_access_token for live analytics."""
    query_params = dict(request.query_params)
    runtime_name = f"{settings.runtime_name}_tiktok_creator"
    received_at = utcnow()
    code = (query_params.get("code") or "").strip()

    if not code:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.failed",
            success=False, error="Missing authorization code",
        ))
        return RedirectResponse(
            url="/status?error=Creator+callback+missing+authorization+code",
            status_code=303,
        )

    app_key = (settings.tiktok_app_key or "").strip()
    app_secret = (settings.tiktok_app_secret or "").strip()
    if not app_key or not app_secret:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.misconfigured",
            success=False, error="Missing app_key or app_secret",
        ))
        return RedirectResponse(
            url="/status?error=Creator+auth+config+missing", status_code=303,
        )

    try:
        token_result = exchange_tiktok_authorization_code(
            auth_code=code,
            app_key=app_key,
            app_secret=app_secret,
            runtime_name=runtime_name,
        )
    except Exception as exc:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.exchange_failed",
            success=False, error=str(exc),
        ))
        return RedirectResponse(
            url="/status?error=Creator+token+exchange+failed", status_code=303,
        )

    creator_access = token_result.access_token
    creator_refresh = token_result.refresh_token
    creator_expires = token_result.access_token_expires_at

    if not creator_access:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.no_token",
            success=False, error="Token exchange returned empty access_token",
        ))
        return RedirectResponse(
            url="/status?error=Creator+token+empty", status_code=303,
        )

    def _persist_creator_token(session: Session):
        auth_row = get_latest_tiktok_auth_row(session)
        if auth_row is None:
            print(structured_log_line(
                runtime=runtime_name, action="tiktok.creator.callback.no_seller_auth",
                success=False,
                error="No existing TikTokAuth row — authorize as Seller first",
            ))
            return False
        auth_row.creator_access_token = creator_access
        auth_row.creator_refresh_token = creator_refresh
        auth_row.creator_token_expires_at = creator_expires
        auth_row.updated_at = received_at
        session.add(auth_row)
        session.commit()
        return True

    try:
        ok = run_write_with_retry(_persist_creator_token)
    except Exception as exc:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.db_failed",
            success=False, error=str(exc)[:400],
        ))
        return RedirectResponse(
            url="/status?error=Creator+token+save+failed", status_code=303,
        )

    if not ok:
        return RedirectResponse(
            url="/status?error=No+seller+auth+row+found.+Authorize+as+seller+first.",
            status_code=303,
        )

    print(structured_log_line(
        runtime=runtime_name, action="tiktok.creator.callback.success",
        success=True, expires_at=str(creator_expires),
    ))
    return RedirectResponse(
        url=f"/status?{urlencode({'success': 'Creator authorization captured — live analytics enabled'})}",
        status_code=303,
    )


@app.post("/webhooks/tiktok/orders")
async def tiktok_orders_webhook(request: Request):
    raw_body = await request.body()
    runtime_name = f"{settings.runtime_name}_tiktok"
    body_hash = hashlib.sha256(raw_body).hexdigest()

    primary_secret = (settings.tiktok_app_secret or "").strip()
    configured_shop_id = (settings.tiktok_shop_id or "").strip()

    topic = (
        request.headers.get("X-TikTok-Topic")
        or request.headers.get("X-Event-Type")
        or request.headers.get("X-TT-Event")
        or "unknown"
    )

    try:
        payload = parse_tiktok_webhook_payload(
            raw_body,
            app_secret=primary_secret,
            headers=request.headers,
            request_path=str(request.url.path),
            strict_signature=True,
        )
    except Exception as exc:
        update_tiktok_integration_state(
            last_error=str(exc),
            last_webhook={
                "received_at": utcnow().isoformat(),
                "topic": topic,
                "body_sha256": body_hash,
                "error": str(exc),
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.failed",
                success=False,
                error=str(exc),
                topic=topic,
                request_path=str(request.url.path),
                body_sha256=body_hash,
            )
        )
        raise HTTPException(status_code=400, detail="Invalid TikTok webhook payload") from exc

    sig_verified = payload.pop("_signature_verified", False)
    payload_shop_id = str(payload.get("shop_id") or "").strip()
    shop_id_matches = (
        not configured_shop_id
        or not payload_shop_id
        or payload_shop_id == configured_shop_id
    )

    if not sig_verified and not shop_id_matches:
        err = f"Unverified webhook with mismatched shop_id (got {payload_shop_id})"
        update_tiktok_integration_state(
            last_error=err,
            last_webhook={
                "received_at": utcnow().isoformat(),
                "topic": topic,
                "body_sha256": body_hash,
                "error": err,
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.rejected",
                success=False,
                error=err,
                topic=topic,
                payload_shop_id=payload_shop_id,
                configured_shop_id=configured_shop_id,
            )
        )
        raise HTTPException(status_code=400, detail="Invalid TikTok webhook payload")

    if not sig_verified:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.signature_unverified",
                success=True,
                topic=topic,
                request_path=str(request.url.path),
                body_sha256=body_hash,
                shop_id_matches=shop_id_matches,
                payload_shop_id=payload_shop_id,
            )
        )

    order_upsert_status = ""
    if isinstance(payload, dict):
        try:
            def persist_tiktok_order(session: Session):
                return upsert_tiktok_order_from_payload(
                    session,
                    TikTokOrder,
                    payload,
                    source="webhook",
                    received_at=utcnow(),
                    dry_run=False,
                )

            order_upsert_status, order_record = run_write_with_retry(persist_tiktok_order)
        except Exception as exc:
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.webhook.order_upsert_failed",
                    success=False,
                    error=str(exc),
                    topic=topic,
                    request_path=str(request.url.path),
                    body_sha256=body_hash,
                )
            )
            return Response(status_code=500)
        else:
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.webhook.order_upserted",
                    success=True,
                    topic=topic,
                    order_status=order_upsert_status,
                    tiktok_order_id=order_record.get("tiktok_order_id"),
                    shop_id=order_record.get("shop_id"),
                )
            )
            enrich_order_id = (order_record.get("tiktok_order_id") or "").strip()
            _start_tiktok_webhook_enrichment(enrich_order_id)
    else:
        order_record = {}

    webhook_summary = {
        "received_at": utcnow().isoformat(),
        "topic": topic,
        "body_sha256": body_hash,
        "payload": summarize_tiktok_payload(payload),
        "order_status": order_upsert_status or None,
        "tiktok_order_id": order_record.get("tiktok_order_id"),
    }
    update_tiktok_integration_state(
        last_webhook_at=utcnow(),
        last_webhook=webhook_summary,
        last_error=None,
    )
    print(
        structured_log_line(
            runtime=runtime_name,
            action="tiktok.webhook.received",
            success=True,
            topic=topic,
            request_path=str(request.url.path),
            body_sha256=body_hash,
            payload=summarize_tiktok_payload(payload),
        )
    )
    return Response(status_code=200)


@app.post("/shopify/backfill")
def shopify_backfill_start(
    request: Request,
    since: Optional[str] = Form(default=None),
    limit: Optional[str] = Form(default="250"),
):
    if denial := require_role_response(request, "admin"):
        return denial

    state = read_shopify_backfill_state()
    if state.get("is_running"):
        return RedirectResponse(
            url="/shopify/orders?success=Backfill+already+running+orders+will+appear+shortly",
            status_code=303,
        )

    raw_limit = (limit or "").strip()
    safe_limit: Optional[int]
    if not raw_limit:
        safe_limit = None
    else:
        try:
            parsed_limit = int(raw_limit)
        except ValueError:
            return RedirectResponse(
                url="/shopify/orders?error=Limit+must+be+a+whole+number+or+blank",
                status_code=303,
            )
        safe_limit = max(1, parsed_limit)
    worker = threading.Thread(
        target=run_shopify_backfill_in_background,
        kwargs={"since": (since or "").strip() or None, "limit": safe_limit},
        daemon=True,
        name="shopify-backfill",
    )
    worker.start()
    return RedirectResponse(
        url="/shopify/orders?success=Backfill+started+orders+will+appear+shortly",
        status_code=303,
    )


@app.get("/shopify-orders")
def shopify_orders_redirect(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/shopify/orders", status_code=307)


@app.get("/shopify/orders", response_class=HTMLResponse)
def shopify_orders_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    financial_status: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    total_rows = count_shopify_order_rows(
        session,
        start=start_dt,
        end=end_dt,
        financial_status=financial_status,
        source=source,
        search=search,
    )
    pagination = build_pagination(page=page, limit=50, total_rows=total_rows)
    orders = get_shopify_order_rows(
        session,
        start=start_dt,
        end=end_dt,
        financial_status=financial_status,
        source=source,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=pagination["page"],
        limit=pagination["limit"],
    )
    summary: dict[str, object] = {}
    summary_chunk_size = 1000
    summary_page = 1
    while True:
        chunk = get_shopify_order_rows(
            session,
            start=start_dt,
            end=end_dt,
            financial_status=financial_status,
            source=source,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=summary_page,
            limit=summary_chunk_size,
        )
        if not chunk:
            break
        part = build_shopify_order_summary(chunk)
        summary = merge_shopify_order_summaries(summary, part) if summary else part
        summary_page += 1
        if len(chunk) < summary_chunk_size:
            break
    if not summary:
        summary = build_shopify_order_summary([])
    status_rows = session.exec(select(distinct(ShopifyOrder.financial_status))).all()
    financial_statuses = sorted(
        {
            str(value[0] if isinstance(value, tuple) else value).strip()
            for value in status_rows
            if str(value[0] if isinstance(value, tuple) else value).strip()
        }
    )
    order_rows = [
        {
            "order": order,
            "customer_label": order.customer_name or "Guest",
            "items_summary": build_shopify_item_summary(order.line_items_json),
            "net_amount": round(
                float(order.subtotal_ex_tax if order.subtotal_ex_tax is not None else float(order.total_price or 0.0) - float(order.total_tax or 0.0)),
                2,
            ),
        }
        for order in orders
    ]
    backfill_state = read_shopify_backfill_state()

    return templates.TemplateResponse(
        request,
        "shopify_orders.html",
        {
            "request": request,
            "title": "Shopify Orders",
            "current_user": getattr(request.state, "current_user", None),
            "success": success,
            "error": error,
            "selected_start": start or "",
            "selected_end": end or "",
            "selected_financial_status": financial_status or "",
            "selected_source": source or "",
            "selected_search": search or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "financial_statuses": [value for value in financial_statuses if value],
            "summary": summary,
            "orders": order_rows,
            "pagination": pagination,
            "page_url": build_shopify_orders_url,
            "sort_url": build_shopify_sort_url,
            "backfill_state": backfill_state,
        },
    )


@app.get("/reports/messages")
def report_messages(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    if entry_kind:
        rows = [row for row in rows if row.entry_kind == entry_kind]
    return build_message_list_items(session, rows)


@app.get("/reports/export.csv")
def report_transactions_csv(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    transactions = get_transactions(
        session,
        start=start_dt,
        end=end_dt,
        channel_id=channel_id,
        entry_kind=entry_kind,
    )
    rows = [
        {
            "transaction_id": row.id,
            "source_message_id": row.source_message_id,
            "occurred_at": row.occurred_at.isoformat(sep=" ", timespec="seconds"),
            "channel_name": row.channel_name or "",
            "author_name": row.author_name or "",
            "entry_kind": row.entry_kind or "",
            "deal_type": row.deal_type or "",
            "amount": row.amount or "",
            "money_in": row.money_in or "",
            "money_out": row.money_out or "",
            "payment_method": row.payment_method or "",
            "cash_direction": row.cash_direction or "",
            "category": row.category or "",
            "expense_category": row.expense_category or "",
            "needs_review": row.needs_review,
            "confidence": row.confidence or "",
            "notes": row.notes or "",
            "trade_summary": row.trade_summary or "",
            "source_content": row.source_content or "",
        }
        for row in transactions
    ]
    return csv_response("transactions-report.csv", rows or [{"message": "No transactions matched the current filters"}])


@app.get("/messages/export.csv")
def messages_csv(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    rows, _ = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=1,
        limit=50000,
    )
    items = build_message_list_items(session, rows, expense_category=expense_category)
    export_rows = [
        {
            "message_id": item["id"],
            "time": item["time"] or "",
            "channel": item["channel"] or "",
            "channel_id": item["channel_id"] or "",
            "author": item["author"] or "",
            "status": item["status"] or "",
            "entry_kind": item["entry_kind"] or "",
            "deal_type": item["type"] or "",
            "amount": item["amount"] if item["amount"] is not None else "",
            "payment_method": item["payment"] or "",
            "cash_direction": item["cash_direction"] or "",
            "category": item["category"] or "",
            "money_in": item["money_in"] if item["money_in"] is not None else "",
            "money_out": item["money_out"] if item["money_out"] is not None else "",
            "expense_category": item["expense_category"] or "",
            "needs_review": item["needs_review"],
            "notes": item["notes"] or "",
            "message": item["message"] or "",
        }
        for item in items
    ]
    return csv_response("messages-export.csv", export_rows or [{"message": "No messages matched the current filters"}])


@app.get("/reports", response_class=HTMLResponse)
def reports_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=REPORT_SOURCE_ALL),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    selected_source = normalize_report_source(source)
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)

    reports_cache_key = f"reports:{start or ''}:{end or ''}:{channel_id or ''}:{entry_kind or ''}:{selected_source}"
    cached_reports = cache_get(reports_cache_key)
    if cached_reports is None:
        transactions_all = get_transactions(
            session,
            start=start_dt,
            end=end_dt,
            channel_id=channel_id,
            entry_kind=entry_kind,
        )
        discord_summary = build_transaction_summary(transactions_all)
        shopify_rows = get_shopify_reporting_rows(session, start=start_dt, end=end_dt)
        shopify_summary = build_shopify_reporting_summary(shopify_rows)
        tiktok_rows = get_tiktok_reporting_rows(session, start=start_dt, end=end_dt)
        tiktok_summary = build_tiktok_reporting_summary(tiktok_rows)
        shopify_timeline_map: dict[str, dict[str, float | int]] = {}
        for row in shopify_rows:
            status = (row.financial_status or "").strip().lower()
            if status != "paid":
                continue
            created_at = row.created_at
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            day_key = created_at.astimezone(PACIFIC_TZ).date().isoformat()
            bucket = shopify_timeline_map.setdefault(
                day_key,
                {
                    "date": day_key,
                    "orders": 0,
                    "gross": 0.0,
                    "tax": 0.0,
                    "net": 0.0,
                    "tax_unknown_orders": 0,
                },
            )
            bucket["orders"] = int(bucket["orders"]) + 1
            gross_value = float(row.total_price or 0.0)
            bucket["gross"] = float(bucket["gross"]) + gross_value
            if row.total_tax is None:
                bucket["tax_unknown_orders"] = int(bucket["tax_unknown_orders"]) + 1
                continue
            tax_value = float(row.total_tax or 0.0)
            net_value = float(row.subtotal_ex_tax) if row.subtotal_ex_tax is not None else gross_value - tax_value
            bucket["tax"] = float(bucket["tax"]) + tax_value
            bucket["net"] = float(bucket["net"]) + net_value
        shopify_daily_totals = [
            {
                "date": day_key,
                "orders": int(values["orders"]),
                "gross": round(float(values["gross"]), 2),
                "tax": round(float(values["tax"]), 2),
                "net": round(float(values["net"]), 2),
                "tax_unknown_orders": int(values["tax_unknown_orders"]),
            }
            for day_key, values in sorted(shopify_timeline_map.items())
        ]
        tiktok_daily_totals = list(tiktok_summary.get("daily_totals", []))
        period_rows = build_report_period_comparison_rows(
            session,
            periods=build_reporting_periods(selected_start=start_dt, selected_end=end_dt),
            channel_id=channel_id,
            entry_kind=entry_kind,
        )
        cached_reports = {
            "discord_summary": discord_summary,
            "shopify_summary": shopify_summary,
            "tiktok_summary": tiktok_summary,
            "shopify_daily_totals": shopify_daily_totals,
            "tiktok_daily_totals": tiktok_daily_totals,
            "period_rows": period_rows,
        }
        cache_set(reports_cache_key, cached_reports)
    else:
        discord_summary = cached_reports["discord_summary"]
        shopify_summary = cached_reports["shopify_summary"]
        tiktok_summary = cached_reports["tiktok_summary"]
        shopify_daily_totals = cached_reports["shopify_daily_totals"]
        tiktok_daily_totals = cached_reports["tiktok_daily_totals"]
        period_rows = cached_reports["period_rows"]

    transactions = get_transactions(
        session,
        start=start_dt,
        end=end_dt,
        channel_id=channel_id,
        entry_kind=entry_kind,
        limit=50,
    )
    summary = discord_summary
    channels = get_channel_filter_choices(session)
    report_totals = {
        "discord_gross": round(float(discord_summary["totals"].get("money_in", 0.0) or 0.0), 2),
        "discord_outflow": round(float(discord_summary["totals"].get("money_out", 0.0) or 0.0), 2),
        "discord_net": round(float(discord_summary["totals"].get("net", 0.0) or 0.0), 2),
        "shopify_gross": round(float(shopify_summary["gross_revenue"] or 0.0), 2),
        "shopify_tax": round(float(shopify_summary["total_tax"] or 0.0), 2),
        "shopify_net": round(float(shopify_summary["net_revenue"] or 0.0), 2),
        "tiktok_gross": round(float(tiktok_summary["gross_revenue"] or 0.0), 2),
        "tiktok_tax": round(float(tiktok_summary["total_tax"] or 0.0), 2),
        "tiktok_net": round(float(tiktok_summary["net_revenue"] or 0.0), 2),
        "combined_revenue": round(
            float(discord_summary["totals"].get("money_in", 0.0) or 0.0)
            + float(shopify_summary["net_revenue"] or 0.0)
            + float(tiktok_summary["net_revenue"] or 0.0),
            2,
        ),
    }

    return templates.TemplateResponse(
        request,
        "reports.html",
        {
            "request": request,
            "title": "Reports",
            "channels": channels,
            "selected_start": start or "",
            "selected_end": end or "",
            "selected_channel_id": channel_id or "",
            "selected_entry_kind": entry_kind or "",
            "selected_source": selected_source,
            "summary": summary,
            "discord_summary": discord_summary,
            "shopify_summary": shopify_summary,
            "tiktok_summary": tiktok_summary,
            "report_totals": report_totals,
            "period_rows": period_rows,
            "show_discord_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_DISCORD},
            "show_shopify_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_SHOPIFY},
            "show_tiktok_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_TIKTOK},
            "reports_url": build_reports_url,
            "expense_chart": build_bar_chart_rows(summary["expense_categories"]),
            "channel_chart": build_bar_chart_rows(summary["channel_net"]),
            "transactions": transactions[-50:],
            "shopify_daily_totals": shopify_daily_totals,
            "tiktok_daily_totals": tiktok_daily_totals,
        },
    )


@app.get("/pnl", include_in_schema=False)
def finance_redirect(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/finance", status_code=307)


@app.get("/finance", response_class=HTMLResponse)
def finance_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    window: Optional[str] = Query(default=FINANCE_WINDOW_MTD),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    range_data = resolve_finance_range(start=start, end=end, window=window)
    finance_cache_key = f"finance:{start or ''}:{end or ''}:{window or ''}"
    cached_finance = cache_get(finance_cache_key)
    if cached_finance is None:
        current_snapshot = build_finance_range_snapshot(
            session,
            start=range_data["start_dt"],
            end=range_data["end_dt"],
            day_count=int(range_data["day_count"]),
        )
        prior_snapshot = build_finance_range_snapshot(
            session,
            start=range_data["previous_start_dt"],
            end=range_data["previous_end_dt"],
            day_count=int(range_data["day_count"]),
        )
        cache_set(finance_cache_key, {"current": current_snapshot, "prior": prior_snapshot})
    else:
        current_snapshot = cached_finance["current"]
        prior_snapshot = cached_finance["prior"]

    current_statement = current_snapshot["statement"]
    prior_statement = prior_snapshot["statement"]
    source_mix_rows = build_finance_source_mix_rows(current_statement)
    spend_mix_rows = build_finance_spend_mix_rows(current_statement)
    top_channels = build_finance_channel_rows(current_snapshot["transactions"])
    analyst_notes = build_finance_notes(
        current_statement=current_statement,
        prior_statement=prior_statement,
        range_label=str(range_data["label"]),
        prior_label=str(range_data["previous_label"]),
        source_mix_rows=source_mix_rows,
        top_channels=top_channels,
    )
    quick_windows = [
        {
            "label": FINANCE_WINDOW_LABELS[window_key],
            "url": build_finance_url(window=window_key),
            "active": range_data["selected_window"] == window_key,
        }
        for window_key in (
            FINANCE_WINDOW_MTD,
            FINANCE_WINDOW_30D,
            FINANCE_WINDOW_90D,
            FINANCE_WINDOW_YTD,
        )
    ]

    return templates.TemplateResponse(
        request,
        "finance.html",
        {
            "request": request,
            "title": "Executive Finance",
            "current_user": getattr(request.state, "current_user", None),
            "selected_start": range_data["selected_start"],
            "selected_end": range_data["selected_end"],
            "selected_window": range_data["selected_window"],
            "range_data": range_data,
            "quick_windows": quick_windows,
            "current_statement": current_statement,
            "prior_statement": prior_statement,
            "kpi_rows": build_finance_kpi_rows(current_statement, prior_statement),
            "statement_rows": build_finance_statement_rows(current_statement, prior_statement),
            "source_mix_rows": source_mix_rows,
            "spend_mix_rows": spend_mix_rows,
            "top_channels": top_channels,
            "analyst_notes": analyst_notes,
            "quality_rows": build_finance_quality_rows(
                current_statement=current_statement,
                range_data=range_data,
            ),
            "monthly_rows": build_finance_monthly_rows(session),
            "finance_url": build_finance_url,
        },
    )


@app.get("/tiktok", include_in_schema=False)
def tiktok_orders_redirect(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/tiktok/orders", status_code=307)


@app.post("/tiktok/orders/sync-form")
def tiktok_orders_sync_form(
    request: Request,
    since: Optional[str] = Form(default=None),
    limit: Optional[str] = Form(default=""),
):
    if denial := require_role_response(request, "admin"):
        return denial

    if read_tiktok_integration_state().get("is_pull_running"):
        return RedirectResponse(
            url="/tiktok/orders?success=TikTok+sync+already+running",
            status_code=303,
        )

    raw_limit = (limit or "").strip()
    safe_limit: Optional[int]
    if not raw_limit:
        safe_limit = settings.tiktok_sync_limit
    else:
        try:
            safe_limit = max(int(raw_limit), 1)
        except ValueError:
            return RedirectResponse(
                url="/tiktok/orders?error=Sync+limit+must+be+a+number",
                status_code=303,
            )

    thread = threading.Thread(
        target=run_tiktok_pull_in_background,
        kwargs={
            "since": (since or "").strip() or None,
            "limit": safe_limit,
            "trigger": "manual",
        },
        daemon=True,
        name="tiktok-pull-manual",
    )
    thread.start()
    return RedirectResponse(
        url="/tiktok/orders?success=Started+TikTok+sync+orders+will+appear+shortly",
        status_code=303,
    )


def _get_tiktok_filter_options(session: Session) -> dict[str, list[str]]:
    def _distinct(col):
        try:
            return sorted({v for v in session.exec(select(col).distinct()).all() if v not in (None, "")})
        except Exception as exc:
            print(structured_log_line(runtime="app", action="tiktok.filter_options.distinct_failed", success=False, error=str(exc)))
            return []
    return {
        "financial_statuses": _distinct(TikTokOrder.financial_status),
        "fulfillment_statuses": _distinct(TikTokOrder.fulfillment_status),
        "order_statuses": _distinct(TikTokOrder.order_status),
        "source_options": _distinct(TikTokOrder.source),
        "currency_options": _distinct(TikTokOrder.currency),
    }


def _collect_tiktok_orders_page_data(
    session: Session,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    order_status: Optional[str] = None,
    source: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "date",
    sort_dir: str = "desc",
    page: int = 1,
    limit: int = 50,
) -> dict[str, object]:
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    auth_row = ensure_tiktok_auth_row(session)
    integration_state = read_tiktok_integration_state()
    sync_snapshot = describe_tiktok_sync_status(auth_row, integration_state)
    page_data = build_tiktok_orders_page_reporting_data(
        session,
        start=start_dt,
        end=end_dt,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        source=source,
        currency=currency,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    recent_orders = [
        {
            "order": row,
            "customer_label": (row.customer_name or "").strip() or "Guest",
            "items_summary": build_tiktok_item_summary(
                row.line_items_summary_json or "",
                row.line_items_json or "",
            ),
            "net_amount": (
                round(float(row.subtotal_ex_tax or 0.0), 2)
                if row.subtotal_ex_tax is not None
                else round(float(row.total_price or 0.0) - float(row.total_tax or 0.0), 2)
            ),
        }
        for row in page_data["rows"]
    ]
    return {
        "summary": page_data["summary"],
        "orders": recent_orders,
        "auth_row": auth_row,
        "sync_snapshot": sync_snapshot,
        "integration_state": integration_state,
        "daily_totals": page_data.get("daily_totals", []),
        "line_item_summary": page_data.get("line_item_summary", {}),
        "total_count": page_data.get("total_count", 0),
        "page": page_data.get("page", max(page, 1)),
        "page_size": page_data.get("page_size", limit),
        "has_more": page_data.get("has_more", False),
    }


@app.get("/tiktok/orders", response_class=HTMLResponse)
def tiktok_orders_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    stream: Optional[str] = Query(default=None),
    financial_status: Optional[str] = Query(default=None),
    fulfillment_status: Optional[str] = Query(default=None),
    order_status: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    currency: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1),
    limit: int = Query(default=50, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    effective_start = start
    effective_end = end
    if stream:
        stream_sessions = _get_live_sessions_list()
        match = next((s for s in stream_sessions if s.get("id") == stream), None)
        if match:
            s_ts = match.get("start_time") or 0
            e_ts = match.get("end_time") or 0
            if s_ts > 0:
                effective_start = datetime.fromtimestamp(s_ts, tz=PACIFIC_TZ).strftime("%Y-%m-%d")
            if e_ts > 0:
                effective_end = datetime.fromtimestamp(e_ts, tz=PACIFIC_TZ).strftime("%Y-%m-%d")

    page_data = _collect_tiktok_orders_page_data(
        session,
        start=effective_start,
        end=effective_end,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        source=source,
        currency=currency,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    total_count = int(page_data.get("total_count", 0) or 0)
    pagination = build_pagination(page=page, limit=limit, total_rows=total_count)
    filter_opts = _get_tiktok_filter_options(session)
    sync_since_default = (utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    unfiltered_total = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())
    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()
    orders = page_data["orders"]
    context = {
        "request": request,
        "title": "TikTok Orders",
        "success": success,
        "error": error,
        "summary": page_data["summary"],
        "orders": orders,
        "recent_orders": orders,  # kept for test compatibility
        "daily_totals": page_data.get("daily_totals", []),
        "line_item_summary": page_data.get("line_item_summary", {}),
        "auth_row": page_data["auth_row"],
        "sync_snapshot": page_data["sync_snapshot"],
        "integration_state": page_data["integration_state"],
        "pagination": pagination,
        "selected_start": effective_start or "",
        "selected_end": effective_end or "",
        "selected_stream": stream or "",
        "stream_sessions": _get_live_sessions_list(),
        "selected_financial_status": financial_status or "",
        "selected_fulfillment_status": fulfillment_status or "",
        "selected_order_status": order_status or "",
        "selected_source": source or "",
        "selected_currency": currency or "",
        "selected_search": search or "",
        "selected_sort_by": sort_by,
        "selected_sort_dir": sort_dir,
        "financial_statuses": filter_opts["financial_statuses"],
        "fulfillment_statuses": filter_opts["fulfillment_statuses"],
        "order_statuses": filter_opts["order_statuses"],
        "source_options": filter_opts["source_options"],
        "currency_options": filter_opts["currency_options"],
        "auto_sync_enabled": bool(settings.tiktok_sync_enabled),
        "sync_interval_minutes": int(settings.tiktok_sync_interval_minutes or 0),
        "sync_since_default": sync_since_default,
        "sync_limit_default": int(settings.tiktok_sync_limit or 250),
        "current_user": getattr(request.state, "current_user", None),
        "page_url": build_tiktok_orders_url,
        "sort_url": build_tiktok_sort_url,
        "unfiltered_total": unfiltered_total,
        "latest_updated_at": latest_updated_at_text,
    }
    return templates.TemplateResponse(request, "tiktok_orders.html", context)


@app.get("/tiktok/orders/poll")
def tiktok_orders_poll(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    total = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())
    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()
    return {"total": total, "latest_updated_at": latest_updated_at_text}


# ---------------------------------------------------------------------------
# Streamer dashboard
# ---------------------------------------------------------------------------

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


def _poll_tiktok_live_analytics(stop_event: threading.Event) -> None:
    """Background thread: poll TikTok LIVE analytics endpoint every 60s."""
    if _fetch_tiktok_live_analytics is None:
        return
    runtime_name = f"{settings.runtime_name}_live_analytics"
    while not stop_event.is_set():
        _poll_access_token = ""
        _poll_shop_cipher = ""
        try:
            with managed_session() as session:
                auth_row = get_latest_tiktok_auth_row(session)
            if auth_row is None:
                stop_event.wait(_LIVE_ANALYTICS_POLL_SECONDS)
                continue

            shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
            _poll_access_token = access_token or ""
            _poll_shop_cipher = shop_cipher or ""
            if not access_token or not shop_cipher:
                stop_event.wait(_LIVE_ANALYTICS_POLL_SECONDS)
                continue

            creator_token = (auth_row.creator_access_token or "").strip() if auth_row else ""
            room_id = get_live_room_id() or ""

            with httpx.Client(timeout=20.0, follow_redirects=True) as client:
                result = _fetch_tiktok_live_analytics(
                    client,
                    base_url=resolve_tiktok_shop_pull_base_url(),
                    app_key=(settings.tiktok_app_key or "").strip(),
                    app_secret=(settings.tiktok_app_secret or "").strip(),
                    access_token=access_token,
                    shop_cipher=shop_cipher,
                    currency="USD",
                    creator_access_token=creator_token,
                    live_room_id=room_id,
                )

            if result.get("rpc_error"):
                with _live_analytics_lock:
                    _live_analytics_cache["ok"] = False
                    _live_analytics_cache["rpc_error"] = True
            else:
                with _live_analytics_lock:
                    _live_analytics_cache.update(result)
                    _live_analytics_cache["ok"] = True
                    _live_analytics_cache.pop("rpc_error", None)
                source = result.get("source", "unknown")
                if source == "live_core_stats":
                    print(structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.live_analytics.realtime_ok",
                        success=True,
                        gmv=result.get("gmv"),
                    ))
        except Exception as exc:
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.live_analytics.poll_failed",
                    success=False,
                    error=str(exc)[:600],
                )
            )
            with _live_analytics_lock:
                _live_analytics_cache["ok"] = False

        if _poll_access_token and _poll_shop_cipher:
            _poll_live_session_list(runtime_name, _poll_access_token, _poll_shop_cipher)

        stop_event.wait(_LIVE_ANALYTICS_POLL_SECONDS)


def _poll_live_session_list(runtime_name: str, access_token: str, shop_cipher: str) -> None:
    """Fetch the live session list and auto-update stream range if source is 'auto'."""
    global _stream_range_source
    if _fetch_live_session_list is None:
        return
    try:
        now = datetime.now(timezone.utc)
        start_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        end_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            sessions = _fetch_live_session_list(
                client,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=(settings.tiktok_app_key or "").strip(),
                app_secret=(settings.tiktok_app_secret or "").strip(),
                access_token=access_token,
                shop_cipher=shop_cipher,
                start_date=start_str,
                end_date=end_str,
            )
        with _live_sessions_list_lock:
            _live_sessions_list_cache.clear()
            _live_sessions_list_cache.extend(sessions)

        if not sessions:
            return

        latest = max(sessions, key=lambda s: s.get("end_time") or 0)
        with _live_session_lock:
            _live_session_cache.update(latest)
            _live_session_cache["ok"] = True

        if _stream_range_source == "manual":
            return

        start_ts = latest.get("start_time") or 0
        end_ts = latest.get("end_time") or 0
        if start_ts <= 0:
            return
        new_start = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        new_end = datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts > 0 else None

        changed = (_stream_range.get("start") != new_start or _stream_range.get("end") != new_end)
        if changed:
            _stream_range["start"] = new_start
            _stream_range["end"] = new_end
            _save_stream_range(source="auto")
            print(structured_log_line(
                runtime=runtime_name,
                action="tiktok.live_session.auto_range_set",
                success=True,
                live_id=latest.get("id"),
                title=latest.get("title"),
                gmv=latest.get("gmv"),
                start_time=start_ts,
                end_time=end_ts,
            ))
    except Exception as exc:
        print(structured_log_line(
            runtime=runtime_name,
            action="tiktok.live_session.poll_failed",
            success=False,
            error=str(exc)[:400],
        ))


def _get_live_session_snapshot() -> dict:
    with _live_session_lock:
        return dict(_live_session_cache)


def _is_currently_live() -> bool:
    """Return True if the latest known stream session ended within the last 15 minutes."""
    snap = _get_live_session_snapshot()
    if not snap.get("ok"):
        return False
    end_ts = snap.get("end_time") or 0
    if end_ts <= 0:
        return False
    now_ts = datetime.now(timezone.utc).timestamp()
    return (now_ts - end_ts) < 900


def _get_live_sessions_list() -> list[dict]:
    with _live_sessions_list_lock:
        return list(_live_sessions_list_cache)


def _get_live_analytics_snapshot() -> dict:
    with _live_analytics_lock:
        return dict(_live_analytics_cache)


def _resolve_tiktok_api_creds() -> tuple[str, str, str]:
    """Return (access_token, shop_cipher, app_key) from the latest TikTok auth row, or empty strings."""
    try:
        with managed_session() as session:
            auth_row = get_latest_tiktok_auth_row(session)
        if auth_row is None:
            return "", "", ""
        _shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
        return access_token or "", shop_cipher or "", (settings.tiktok_app_key or "").strip()
    except Exception as exc:
        print(structured_log_line(runtime="app", action="tiktok.resolve_creds_failed", success=False, error=str(exc)[:300]))
        return "", "", ""


def _streamer_session_gmv(session: Session) -> dict:
    """Cached wrapper — returns GMV data, recomputing at most once per _GMV_CACHE_TTL_SECONDS."""
    now = time.monotonic()
    with _gmv_cache_lock:
        cached_at = _gmv_cache.get("at", 0)
        if now - cached_at < _GMV_CACHE_TTL_SECONDS and "data" in _gmv_cache:
            return _gmv_cache["data"]
    result = _streamer_session_gmv_uncached(session)
    with _gmv_cache_lock:
        _gmv_cache["data"] = result
        _gmv_cache["at"] = time.monotonic()
    return result


def _streamer_session_gmv_uncached(session: Session) -> dict:
    """Calculate today's GMV and top sellers for the streamer dashboard (Pacific time)."""
    now_pacific = datetime.now(PACIFIC_TZ)
    today_start_pacific = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_start_pacific.astimezone(timezone.utc)

    today_orders = session.exec(
        select(TikTokOrder).where(TikTokOrder.created_at >= today_start_utc)
    ).all()

    paid_statuses = {
        "paid", "completed", "awaiting_shipment", "awaiting_collection",
        "awaiting_delivery", "in_transit", "delivered",
    }
    gmv = 0.0
    paid_count = 0
    product_agg: dict[str, dict] = {}
    customer_agg: dict[str, dict] = {}

    for o in today_orders:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status not in paid_statuses:
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
            customer_agg[buyer_key] = {"name": buyer_name, "spent": order_gmv, "orders": 1}

        raw_items: list[dict] = []
        try:
            raw_items = json.loads(o.line_items_json) if o.line_items_json else []
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(raw_items, list):
            raw_items = [raw_items] if isinstance(raw_items, dict) else []

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
                }

    top_sellers = sorted(product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:8]
    for ts in top_sellers:
        ts["revenue"] = round(ts["revenue"], 2)

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

    sr_start = _stream_range.get("start")
    sr_end = _stream_range.get("end")
    if sr_start is not None:
        stream_gmv = 0.0
        stream_paid = 0
        stream_items = 0
        stream_product_agg: dict[str, dict] = {}
        stream_customer_agg: dict[str, dict] = {}
        q = select(TikTokOrder).where(TikTokOrder.created_at >= sr_start)
        if sr_end is not None:
            q = q.where(TikTokOrder.created_at <= sr_end)
        stream_orders_rows = session.exec(q).all()
        for o in stream_orders_rows:
            status = (o.financial_status or o.order_status or "").lower().strip()
            if status not in paid_statuses:
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
                stream_customer_agg[buyer_key] = {"name": buyer_name, "spent": o_gmv, "orders": 1}

            try:
                s_items = json.loads(o.line_items_json) if o.line_items_json else []
            except (json.JSONDecodeError, TypeError):
                s_items = []
            if not isinstance(s_items, list):
                s_items = [s_items] if isinstance(s_items, dict) else []
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
                if s_key in stream_product_agg:
                    stream_product_agg[s_key]["qty"] += s_qty
                    stream_product_agg[s_key]["revenue"] += round(s_qty * s_unit, 2)
                    if not stream_product_agg[s_key]["sku_image"] and s_img:
                        stream_product_agg[s_key]["sku_image"] = s_img
                else:
                    stream_product_agg[s_key] = {
                        "title": s_title, "variant": s_variant, "sku_image": s_img,
                        "qty": s_qty, "revenue": round(s_qty * s_unit, 2),
                    }

        stream_top_sellers = sorted(stream_product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:8]
        for sts in stream_top_sellers:
            sts["revenue"] = round(sts["revenue"], 2)
        stream_top_buyers = sorted(stream_customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
        for stb in stream_top_buyers:
            stb["spent"] = round(stb["spent"], 2)

        result["stream_gmv"] = round(stream_gmv, 2)
        result["stream_orders"] = stream_paid
        result["stream_items"] = stream_items
        result["stream_top_sellers"] = stream_top_sellers
        result["stream_top_buyers"] = stream_top_buyers
        result["stream_start_utc"] = sr_start.isoformat()
        if sr_end:
            result["stream_end_utc"] = sr_end.isoformat()

    return result


# ---------------------------------------------------------------------------
# TikTok Analytics helpers + routes
# ---------------------------------------------------------------------------

_REFUND_STATUSES = {"refunded", "refund_requested", "cancelled", "cancel_requested"}

def _enrich_orders_for_range(session: Session, start_utc: datetime, end_utc: Optional[datetime]) -> dict:
    """Compute top sellers, top buyers, refund rate, and AOV from local orders in [start, end]."""
    paid_statuses = {
        "paid", "completed", "awaiting_shipment", "awaiting_collection",
        "awaiting_delivery", "in_transit", "delivered",
    }
    q = select(TikTokOrder).where(TikTokOrder.created_at >= start_utc)
    if end_utc is not None:
        q = q.where(TikTokOrder.created_at <= end_utc)
    rows = session.exec(q).all()

    gmv = 0.0
    paid_count = 0
    total_items = 0
    refund_count = 0
    product_agg: dict[str, dict] = {}
    customer_agg: dict[str, dict] = {}

    for o in rows:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status in _REFUND_STATUSES:
            refund_count += 1
        if status not in paid_statuses:
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
            customer_agg[buyer_key] = {"name": buyer_name, "spent": order_gmv, "orders": 1}

        raw_items: list[dict] = []
        try:
            raw_items = json.loads(o.line_items_json) if o.line_items_json else []
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(raw_items, list):
            raw_items = [raw_items] if isinstance(raw_items, dict) else []

        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title = str(
                raw.get("product_name") or raw.get("sku_name") or raw.get("title") or raw.get("item_name") or ""
            ).strip() or "Unknown item"
            sku_name = str(raw.get("sku_name") or "").strip()
            variant = sku_name if sku_name and sku_name.lower() != "default" and sku_name.lower() != title.lower() else None
            key = (title.lower() + "||" + (variant or "").lower()).strip()

            qty_raw = raw.get("quantity") or raw.get("sku_quantity") or raw.get("count")
            try:
                qty = max(int(qty_raw or 0), 1)
            except (TypeError, ValueError):
                qty = 1
            total_items += qty

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

            if key in product_agg:
                product_agg[key]["qty"] += qty
                product_agg[key]["revenue"] += round(qty * unit_price, 2)
                if not product_agg[key]["sku_image"] and sku_image:
                    product_agg[key]["sku_image"] = sku_image
            else:
                product_agg[key] = {
                    "title": title, "variant": variant, "sku_image": sku_image,
                    "qty": qty, "revenue": round(qty * unit_price, 2),
                }

    top_sellers = sorted(product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:10]
    for ts in top_sellers:
        ts["revenue"] = round(ts["revenue"], 2)
    top_buyers = sorted(customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
    for tb in top_buyers:
        tb["spent"] = round(tb["spent"], 2)

    aov = round(gmv / paid_count, 2) if paid_count > 0 else 0.0
    refund_rate = round(refund_count / len(rows) * 100, 1) if rows else 0.0

    return {
        "gmv": round(gmv, 2),
        "paid_orders": paid_count,
        "total_orders": len(rows),
        "total_items": total_items,
        "aov": aov,
        "refund_count": refund_count,
        "refund_rate": refund_rate,
        "top_sellers": top_sellers,
        "top_buyers": top_buyers,
    }


@app.get("/tiktok/analytics/api/debug")
def tiktok_analytics_debug(request: Request):
    """Diagnostic endpoint -- shows what credentials and data sources are available."""
    access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
    return {
        "has_access_token": bool(access_token),
        "access_token_preview": (access_token[:8] + "...") if access_token else "",
        "has_shop_cipher": bool(shop_cipher),
        "shop_cipher_preview": (shop_cipher[:8] + "...") if shop_cipher else "",
        "has_app_key": bool(app_key),
        "app_key_preview": (app_key[:8] + "...") if app_key else "",
        "app_secret_set": bool((settings.tiktok_app_secret or "").strip()),
        "base_url": resolve_tiktok_shop_pull_base_url(),
        "cached_sessions_count": len(_get_live_sessions_list()),
        "live_session_cache": {k: v for k, v in _get_live_session_snapshot().items() if k != "ok"},
        "live_analytics_cache_ok": _get_live_analytics_snapshot().get("ok"),
    }


@app.get("/tiktok/analytics", response_class=HTMLResponse)
def tiktok_analytics_page(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return templates.TemplateResponse(request, "tiktok_analytics.html", {
        "request": request,
        "title": "TikTok Stream Analytics",
        "current_user": getattr(request.state, "current_user", None),
    })


def _build_daily_from_local_orders(session: Session, days: int) -> list[dict]:
    """Build daily GMV/orders breakdown from local TikTokOrder data as a fallback."""
    paid_statuses = {
        "paid", "completed", "awaiting_shipment", "awaiting_collection",
        "awaiting_delivery", "in_transit", "delivered",
    }
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=days)
    rows = session.exec(
        select(TikTokOrder).where(TikTokOrder.created_at >= start_utc)
    ).all()
    daily: dict[str, dict] = {}
    for o in rows:
        if not o.created_at:
            continue
        dt = o.created_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        day_key = dt.astimezone(PACIFIC_TZ).strftime("%Y-%m-%d")
        if day_key not in daily:
            daily[day_key] = {"date": day_key, "gmv": 0.0, "sku_orders": 0, "customers": 0, "items_sold": 0,
                              "click_to_order_rate": "", "click_through_rate": "", "_buyers": set()}
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status not in paid_statuses:
            continue
        order_gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        daily[day_key]["gmv"] += order_gmv
        daily[day_key]["sku_orders"] += 1
        buyer = (o.customer_name or "").strip().lower() or "guest"
        daily[day_key]["_buyers"].add(buyer)
        try:
            items = json.loads(o.line_items_json) if o.line_items_json else []
            if isinstance(items, list):
                for it in items:
                    qty = int(it.get("quantity") or it.get("sku_quantity") or 1)
                    daily[day_key]["items_sold"] += qty
        except Exception:
            pass
    result = []
    for d in sorted(daily.values(), key=lambda x: x["date"]):
        d["customers"] = len(d.pop("_buyers", set()))
        d["gmv"] = round(d["gmv"], 2)
        result.append(d)
    return result


@app.get("/tiktok/analytics/api/daily")
def tiktok_analytics_daily(
    request: Request,
    days: int = Query(default=30, ge=7, le=90),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    source = "tiktok_api"
    intervals = []

    if _fetch_overview_performance_daily is not None:
        access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
        if access_token and shop_cipher:
            now = datetime.now(timezone.utc)
            start_str = (now - timedelta(days=days)).strftime("%Y-%m-%d")
            end_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                with httpx.Client(timeout=25.0, follow_redirects=True) as client:
                    intervals = _fetch_overview_performance_daily(
                        client,
                        base_url=resolve_tiktok_shop_pull_base_url(),
                        app_key=app_key,
                        app_secret=(settings.tiktok_app_secret or "").strip(),
                        access_token=access_token,
                        shop_cipher=shop_cipher,
                        start_date=start_str,
                        end_date=end_str,
                    )
            except Exception:
                intervals = []

    if not intervals:
        intervals = _build_daily_from_local_orders(session, days)
        source = "local_orders"

    return {"intervals": intervals, "source": source}


@app.get("/tiktok/analytics/api/streams")
def tiktok_analytics_streams(
    request: Request,
    days: int = Query(default=30, ge=7, le=90),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    sessions: list[dict] = []

    if _fetch_live_session_list is not None:
        access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
        if access_token and shop_cipher:
            now = datetime.now(timezone.utc)
            start_str = (now - timedelta(days=days)).strftime("%Y-%m-%d")
            end_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                with httpx.Client(timeout=25.0, follow_redirects=True) as client:
                    sessions = _fetch_live_session_list(
                        client,
                        base_url=resolve_tiktok_shop_pull_base_url(),
                        app_key=app_key,
                        app_secret=(settings.tiktok_app_secret or "").strip(),
                        access_token=access_token,
                        shop_cipher=shop_cipher,
                        start_date=start_str,
                        end_date=end_str,
                    )
            except Exception:
                sessions = []

    source = "tiktok_api" if sessions else "none"

    if not sessions:
        cached = _get_live_sessions_list()
        if cached:
            sessions = cached
            source = "cache"

    for i, s in enumerate(sessions):
        if i > 0:
            prev = sessions[i - 1]
            prev_gmv = prev.get("gmv") or 0
            cur_gmv = s.get("gmv") or 0
            s["gmv_delta_pct"] = round((cur_gmv - prev_gmv) / prev_gmv * 100, 1) if prev_gmv > 0 else None
        else:
            s["gmv_delta_pct"] = None
        dur_s = (s.get("end_time") or 0) - (s.get("start_time") or 0)
        s["duration_hours"] = round(dur_s / 3600, 1) if dur_s > 0 else 0
        s["revenue_per_hour"] = round(s.get("gmv", 0) / (dur_s / 3600), 2) if dur_s > 3600 else None
    sessions.sort(key=lambda s: s.get("end_time") or 0, reverse=True)
    return {"streams": sessions, "source": source}


@app.get("/tiktok/analytics/api/stream/{live_id}")
def tiktok_analytics_stream_detail(
    request: Request,
    live_id: str,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()

    per_minutes = None
    if _fetch_stream_performance_per_minutes and access_token and shop_cipher:
        with httpx.Client(timeout=25.0, follow_redirects=True) as client:
            per_minutes = _fetch_stream_performance_per_minutes(
                client,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=app_key,
                app_secret=(settings.tiktok_app_secret or "").strip(),
                access_token=access_token,
                shop_cipher=shop_cipher,
                live_id=live_id,
            )

    streams = _get_live_sessions_list()
    stream_info = next((s for s in streams if s.get("id") == live_id), None)

    local_enrichment = {}
    if stream_info:
        start_ts = stream_info.get("start_time") or 0
        end_ts = stream_info.get("end_time") or 0
        if start_ts > 0:
            start_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            end_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts > 0 else None
            local_enrichment = _enrich_orders_for_range(session, start_utc, end_utc)

    return {
        "live_id": live_id,
        "stream": stream_info,
        "per_minutes": per_minutes,
        "local": local_enrichment,
    }


@app.get("/tiktok/streamer", response_class=HTMLResponse)
def tiktok_streamer_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    orders = session.exec(
        select(TikTokOrder).order_by(TikTokOrder.created_at.desc()).limit(50)
    ).all()

    cards = [_build_streamer_order_card(o) for o in orders]

    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()

    total_count = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())
    gmv_data = _streamer_session_gmv(session)

    chat_info = get_chat_status()
    live_analytics = _get_live_analytics_snapshot()

    live_session = _get_live_session_snapshot()
    stream_data = {
        "stream_gmv": gmv_data.get("stream_gmv"),
        "stream_orders": gmv_data.get("stream_orders"),
        "stream_items": gmv_data.get("stream_items"),
        "stream_start_utc": gmv_data.get("stream_start_utc"),
        "tiktok_gmv": live_session.get("gmv") if live_session.get("ok") else None,
        "tiktok_items_sold": live_session.get("items_sold") if live_session.get("ok") else None,
        "tiktok_customers": live_session.get("customers") if live_session.get("ok") else None,
        "live_title": live_session.get("title") if live_session.get("ok") else None,
        "stream_range_source": _stream_range_source,
        "is_live": _is_currently_live(),
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
        "stream_sessions_json": json.dumps(_get_live_sessions_list()),
        "chat_status": chat_info["status"],
        "current_user": getattr(request.state, "current_user", None),
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "current_streamer": get_current_streamer(session) or "",
    })


@app.get("/tiktok/streamer/poll")
def tiktok_streamer_poll(
    request: Request,
    since: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    created_at_floor = datetime.now(timezone.utc) - timedelta(hours=24)

    query = (
        select(TikTokOrder)
        .where(TikTokOrder.created_at >= created_at_floor)
        .order_by(TikTokOrder.updated_at.desc())
        .limit(20)
    )
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            query = (
                select(TikTokOrder)
                .where(
                    TikTokOrder.updated_at > since_dt,
                    TikTokOrder.created_at >= created_at_floor,
                )
                .order_by(TikTokOrder.updated_at.desc())
                .limit(20)
            )
        except (ValueError, TypeError):
            pass

    orders = session.exec(query).all()
    cards = [_build_streamer_order_card(o) for o in orders]

    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()

    total_count = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())

    gmv_data = _streamer_session_gmv(session)

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
        "tiktok_gmv": (lambda snap: snap.get("gmv") if snap.get("ok") else None)(_get_live_session_snapshot()),
        "stream_range_source": _stream_range_source,
        "stream_sessions": _get_live_sessions_list(),
        "is_live": _is_currently_live(),
    }


@app.get("/tiktok/streamer/config", response_class=HTMLResponse)
def tiktok_streamer_config(request: Request):
    """Backend page to configure the stream date range shown on the streamer dashboard."""
    if denial := require_role_response(request, "admin"):
        return denial
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
</script>
</body></html>"""
    return HTMLResponse(content=html)


@app.post("/tiktok/streamer/config")
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


@app.post("/tiktok/streamer/config/auto")
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


@app.get("/tiktok/streamer/chat/poll")
def tiktok_streamer_chat_poll(
    request: Request,
    since: int = Query(default=0),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    messages = get_live_chat_messages(since_idx=since)
    status_info = get_chat_status()
    latest_idx = messages[-1]["idx"] if messages else since
    return {
        "messages": messages,
        "latest_idx": latest_idx,
        "status": status_info["status"],
        "viewer_count": status_info["viewer_count"],
    }


_tiktok_product_sync_state: dict[str, object] = {
    "is_running": False,
    "last_finished_at": None,
    "last_error": None,
}
_tiktok_product_sync_lock = threading.Lock()


def _read_tiktok_product_sync_state() -> dict[str, object]:
    with _tiktok_product_sync_lock:
        return dict(_tiktok_product_sync_state)


def _update_tiktok_product_sync_state(**changes: object) -> None:
    with _tiktok_product_sync_lock:
        _tiktok_product_sync_state.update(changes)


def run_tiktok_product_sync_background(*, limit: Optional[int], trigger: str = "manual") -> None:
    runtime_name = f"{settings.runtime_name}_tiktok_product_sync"
    _update_tiktok_product_sync_state(is_running=True, last_error=None)
    try:
        if pull_tiktok_products is None:
            _update_tiktok_product_sync_state(is_running=False, last_error="product sync unavailable")
            return

        with managed_session() as session:
            auth_row = ensure_tiktok_auth_row(session)
            shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
            if not shop_id and not shop_cipher:
                _update_tiktok_product_sync_state(is_running=False, last_error="missing shop identity")
                return
            if not access_token:
                _update_tiktok_product_sync_state(is_running=False, last_error="missing access token")
                return
            summary = pull_tiktok_products(
                session,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=(settings.tiktok_app_key or "").strip(),
                app_secret=(settings.tiktok_app_secret or "").strip(),
                access_token=access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                limit=limit,
                dry_run=False,
                runtime_name=runtime_name,
            )
            session.commit()
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.products.sync_complete",
                    success=True,
                    trigger=trigger,
                    fetched=summary.fetched,
                    inserted=summary.inserted,
                    updated=summary.updated,
                    failed=summary.failed,
                )
            )
        _update_tiktok_product_sync_state(is_running=False, last_finished_at=utcnow(), last_error=None)
    except Exception as exc:
        _update_tiktok_product_sync_state(is_running=False, last_finished_at=utcnow(), last_error=str(exc))
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.products.sync_failed",
                success=False,
                error=str(exc),
                trigger=trigger,
            )
        )


@app.post("/tiktok/products/sync-form")
def tiktok_products_sync_form(
    request: Request,
    limit: Optional[str] = Form(default=""),
):
    if denial := require_role_response(request, "admin"):
        return denial
    sync_state = _read_tiktok_product_sync_state()
    if sync_state.get("is_running"):
        return RedirectResponse(url="/tiktok/products?success=Product+sync+already+running", status_code=303)

    raw_limit = (limit or "").strip()
    safe_limit: Optional[int]
    if not raw_limit:
        safe_limit = 200
    else:
        try:
            safe_limit = max(int(raw_limit), 1)
        except ValueError:
            return RedirectResponse(url="/tiktok/products?error=Limit+must+be+a+number", status_code=303)

    thread = threading.Thread(
        target=run_tiktok_product_sync_background,
        kwargs={"limit": safe_limit, "trigger": "manual"},
        daemon=True,
        name="tiktok-product-sync-manual",
    )
    thread.start()
    return RedirectResponse(url="/tiktok/products?success=Started+product+sync", status_code=303)


def _get_tiktok_product_filter_options(session: Session) -> dict[str, list[str]]:
    def _distinct(col):
        try:
            return sorted({v for v in session.exec(select(col).distinct()).all() if v not in (None, "")})
        except Exception:
            return []
    return {
        "statuses": _distinct(TikTokProduct.status),
        "audit_statuses": _distinct(TikTokProduct.audit_status),
        "source_options": _distinct(TikTokProduct.source),
    }


def _build_product_sku_summary(skus_json: str) -> dict[str, object]:
    try:
        skus = json.loads(skus_json) if skus_json else []
    except (json.JSONDecodeError, TypeError):
        skus = []
    if not isinstance(skus, list):
        skus = []
    count = len(skus)
    prices = [s.get("price") or 0 for s in skus if isinstance(s, dict)]
    total_inventory = sum(s.get("inventory") or 0 for s in skus if isinstance(s, dict))
    min_price = min(prices) if prices else 0
    max_price = max(prices) if prices else 0
    return {
        "count": count,
        "min_price": round(float(min_price), 2),
        "max_price": round(float(max_price), 2),
        "total_inventory": total_inventory,
    }


@app.get("/tiktok/products", response_class=HTMLResponse)
def tiktok_products_page(
    request: Request,
    status: Optional[str] = Query(default=None),
    audit_status: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="updated"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    query = select(TikTokProduct)
    if status:
        query = query.where(TikTokProduct.status == status)
    if audit_status:
        query = query.where(TikTokProduct.audit_status == audit_status)
    if search:
        term = f"%{search}%"
        query = query.where(
            (TikTokProduct.title.ilike(term))
            | (TikTokProduct.tiktok_product_id.ilike(term))
            | (TikTokProduct.category_name.ilike(term))
            | (TikTokProduct.brand_name.ilike(term))
        )

    sort_column_map = {
        "title": TikTokProduct.title,
        "status": TikTokProduct.status,
        "updated": TikTokProduct.updated_at,
        "created": TikTokProduct.created_at,
        "synced": TikTokProduct.synced_at,
    }
    sort_col = sort_column_map.get(sort_by, TikTokProduct.updated_at)
    query = query.order_by(sort_col.desc() if sort_dir == "desc" else sort_col.asc(), TikTokProduct.id.desc())

    count_query = select(func.count()).select_from(TikTokProduct)
    if query.whereclause is not None:
        count_query = count_query.where(query.whereclause)
    total_count = session.exec(count_query).one()
    offset = (max(page, 1) - 1) * limit
    rows = session.exec(query.offset(offset).limit(limit)).all()
    has_more = (offset + limit) < total_count

    products = []
    for row in rows:
        sku_info = _build_product_sku_summary(row.skus_json)
        products.append({
            "product": row,
            "sku_count": sku_info["count"],
            "min_price": sku_info["min_price"],
            "max_price": sku_info["max_price"],
            "total_inventory": sku_info["total_inventory"],
            "price_label": (
                f"${sku_info['min_price']:.2f}" if sku_info["min_price"] == sku_info["max_price"]
                else f"${sku_info['min_price']:.2f} - ${sku_info['max_price']:.2f}"
            ) if sku_info["count"] > 0 else "-",
        })

    filter_options = _get_tiktok_product_filter_options(session)
    sync_state = _read_tiktok_product_sync_state()

    summary_total = int(session.exec(select(func.count()).select_from(TikTokProduct)).one())
    summary_active = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "ACTIVATE")
    ).one())
    summary_draft = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "DRAFT")
    ).one())
    summary_deactivated = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(
            TikTokProduct.status.in_(["SELLER_DEACTIVATED", "PLATFORM_DEACTIVATED"])
        )
    ).one())

    return templates.TemplateResponse(request, "tiktok_products.html", {
        "request": request,
        "title": "TikTok Products",
        "products": products,
        "total_count": total_count,
        "page": max(page, 1),
        "page_size": limit,
        "has_more": has_more,
        "filter_status": status or "",
        "filter_audit_status": audit_status or "",
        "filter_search": search or "",
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "filter_options": filter_options,
        "sync_state": sync_state,
        "success_message": success,
        "error_message": error,
        "summary_total": summary_total,
        "summary_active": summary_active,
        "summary_draft": summary_draft,
        "summary_deactivated": summary_deactivated,
        "current_user": getattr(request.state, "current_user", None),
    })


@app.get("/tiktok/products/poll")
def tiktok_products_poll(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    total = int(session.exec(select(func.count()).select_from(TikTokProduct)).one())
    active = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "ACTIVATE")
    ).one())
    sync_state = _read_tiktok_product_sync_state()
    latest_synced = session.exec(select(func.max(TikTokProduct.synced_at))).one()
    latest_synced_text = None
    if latest_synced is not None:
        if latest_synced.tzinfo is None:
            latest_synced = latest_synced.replace(tzinfo=timezone.utc)
        latest_synced_text = latest_synced.isoformat()
    return {
        "total": total,
        "active": active,
        "is_syncing": sync_state.get("is_running", False),
        "last_error": sync_state.get("last_error"),
        "latest_synced_at": latest_synced_text,
    }


def _get_tiktok_api_client_context(session: Session) -> dict[str, Any]:
    auth_row = ensure_tiktok_auth_row(session)
    shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
    return {
        "base_url": resolve_tiktok_shop_pull_base_url(),
        "app_key": (settings.tiktok_app_key or "").strip(),
        "app_secret": (settings.tiktok_app_secret or "").strip(),
        "access_token": access_token,
        "shop_id": shop_id,
        "shop_cipher": shop_cipher,
    }


@app.get("/tiktok/products/categories")
def tiktok_products_categories_api(
    request: Request,
    keyword: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    if _fetch_tiktok_categories is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        categories = _fetch_tiktok_categories(
            client, keyword=keyword or None, **ctx,
        )
    return {"categories": categories}


@app.get("/tiktok/products/categories/{category_id}/attributes")
def tiktok_products_category_attributes_api(
    request: Request,
    category_id: str,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    if _fetch_tiktok_category_attributes is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        attributes = _fetch_tiktok_category_attributes(
            client, category_id=category_id, **ctx,
        )
    return {"attributes": attributes}


@app.get("/tiktok/products/brands")
def tiktok_products_brands_api(
    request: Request,
    brand_name: Optional[str] = Query(default=None),
    category_id: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    if _fetch_tiktok_brands is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        brands = _fetch_tiktok_brands(
            client, brand_name=brand_name or None, category_id=category_id or None, **ctx,
        )
    return {"brands": brands}


@app.post("/tiktok/products/upload-image")
async def tiktok_products_upload_image(
    request: Request,
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    if _upload_tiktok_product_image is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    image_data = await file.read()
    if len(image_data) > 5 * 1024 * 1024:
        return JSONResponse({"error": "Image must be under 5MB"}, status_code=400)
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        try:
            uri = _upload_tiktok_product_image(
                client,
                image_data=image_data,
                file_name=file.filename or "image.jpg",
                **ctx,
            )
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=500)
    return {"uri": uri}


@app.get("/tiktok/products/new", response_class=HTMLResponse)
def tiktok_products_new_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return templates.TemplateResponse(request, "tiktok_product_form.html", {
        "request": request,
        "title": "New TikTok Product",
        "mode": "create",
        "product": None,
        "current_user": getattr(request.state, "current_user", None),
    })


@app.post("/tiktok/products/create")
async def tiktok_products_create(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    if _create_tiktok_product is None:
        return RedirectResponse(url="/tiktok/products?error=TikTok+API+helpers+unavailable", status_code=303)

    form = await request.form()
    title = (form.get("title") or "").strip()
    description = (form.get("description") or "").strip()
    category_id = (form.get("category_id") or "").strip()
    brand_id = (form.get("brand_id") or "").strip()

    if not title:
        return RedirectResponse(url="/tiktok/products/new?error=Title+is+required", status_code=303)
    if not category_id:
        return RedirectResponse(url="/tiktok/products/new?error=Category+is+required", status_code=303)

    image_uris_raw = form.get("image_uris") or ""
    image_uris = [u.strip() for u in image_uris_raw.split(",") if u.strip()]
    main_images = [{"uri": uri} for uri in image_uris]

    skus = []
    sku_index = 0
    while True:
        price_key = f"sku_price_{sku_index}"
        if price_key not in form:
            break
        price = (form.get(price_key) or "0").strip()
        inventory = (form.get(f"sku_inventory_{sku_index}") or "0").strip()
        seller_sku = (form.get(f"sku_seller_sku_{sku_index}") or "").strip()
        sku_entry: dict[str, Any] = {
            "sales_attributes": [],
            "price": {
                "amount": price,
                "currency": "USD",
            },
            "inventory": [{"quantity": int(inventory or 0)}],
        }
        if seller_sku:
            sku_entry["seller_sku"] = seller_sku
        skus.append(sku_entry)
        sku_index += 1

    if not skus:
        skus = [{
            "sales_attributes": [],
            "price": {"amount": (form.get("price") or "0").strip(), "currency": "USD"},
            "inventory": [{"quantity": int((form.get("inventory") or "0").strip() or 0)}],
        }]
        seller_sku_single = (form.get("seller_sku") or "").strip()
        if seller_sku_single:
            skus[0]["seller_sku"] = seller_sku_single

    product_body: dict[str, Any] = {
        "title": title,
        "description": description or title,
        "category_id": category_id,
        "main_images": main_images,
        "skus": skus,
        "is_cod_allowed": False,
    }
    if brand_id:
        product_body["brand"] = {"id": brand_id}

    save_as_draft = (form.get("save_as_draft") or "").strip()
    if save_as_draft == "1":
        product_body["save_mode"] = "AS_DRAFT"

    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return RedirectResponse(url="/tiktok/products/new?error=TikTok+auth+not+configured", status_code=303)

    try:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            result = _create_tiktok_product(client, product_body=product_body, **ctx)
    except Exception as exc:
        error_msg = str(exc)[:200].replace(" ", "+")
        return RedirectResponse(url=f"/tiktok/products/new?error={error_msg}", status_code=303)

    new_product_id = result.get("product_id") or result.get("id") or ""
    if new_product_id and _upsert_tiktok_product_row is not None:
        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                detail = _fetch_tiktok_product_detail(
                    client, product_id=str(new_product_id), **ctx,
                )
            _upsert_tiktok_product_row(
                session, detail,
                shop_id=ctx["shop_id"], shop_cipher=ctx["shop_cipher"],
                source="created", dry_run=False,
            )
            session.commit()
        except Exception as exc:
            print(structured_log_line(runtime="app", action="tiktok.product_detail_upsert_failed", success=False, error=str(exc)))

    return RedirectResponse(url="/tiktok/products?success=Product+created+successfully", status_code=303)


@app.get("/tiktok/products/{product_id}", response_class=HTMLResponse)
def tiktok_product_detail_page(
    request: Request,
    product_id: str,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    product = session.exec(
        select(TikTokProduct).where(TikTokProduct.tiktok_product_id == product_id)
    ).first()
    if product is None:
        return RedirectResponse(url="/tiktok/products?error=Product+not+found", status_code=303)

    try:
        skus = json.loads(product.skus_json) if product.skus_json else []
    except (json.JSONDecodeError, TypeError):
        skus = []
    try:
        images = json.loads(product.images_json) if product.images_json else []
    except (json.JSONDecodeError, TypeError):
        images = []

    return templates.TemplateResponse(request, "tiktok_product_detail.html", {
        "request": request,
        "title": product.title or "Product Detail",
        "product": product,
        "skus": skus,
        "images": images,
        "current_user": getattr(request.state, "current_user", None),
    })


@app.get("/bookkeeping", response_class=HTMLResponse)
def bookkeeping_page(
    request: Request,
    import_id: Optional[int] = Query(default=None),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    imports = list_bookkeeping_imports(session)
    selected_import = None
    reconciliation = None
    detected_posts = list_detected_bookkeeping_posts(session)
    for post in detected_posts:
        post["action_links"] = build_row_action_links(
            post.get("message_id"),
            channel_id=None,
            created_at=post.get("created_at"),
        )
    if import_id:
        selected_import = session.get(BookkeepingImport, import_id)
        if selected_import:
            reconciliation = reconcile_bookkeeping_import(session, import_id)
            for entry in reconciliation["entries"]:
                matched_transaction = entry.get("matched_transaction")
                entry["action_links"] = build_row_action_links(
                    matched_transaction.source_message_id if matched_transaction and matched_transaction.source_message_id is not None else None,
                    channel_id=matched_transaction.channel_id if matched_transaction else None,
                    created_at=matched_transaction.occurred_at if matched_transaction else entry.get("occurred_at"),
                    status="parsed" if matched_transaction else None,
                )
            enriched_unmatched_transactions = []
            for row in reconciliation["unmatched_transactions"]:
                action_links = build_row_action_links(
                    row.source_message_id if row.source_message_id is not None else None,
                    channel_id=row.channel_id,
                    created_at=row.occurred_at,
                    status="parsed",
                )
                enriched_unmatched_transactions.append(
                    {
                        "occurred_at": row.occurred_at,
                        "channel_name": row.channel_name,
                        "channel_id": row.channel_id,
                        "entry_kind": row.entry_kind,
                        "amount": row.amount,
                        "payment_method": row.payment_method,
                        "category": row.category,
                        "expense_category": row.expense_category,
                        "notes": row.notes,
                        "action_links": action_links,
                    }
                )
            reconciliation["unmatched_transactions"] = enriched_unmatched_transactions

    return templates.TemplateResponse(
        request,
        "bookkeeping.html",
        {
            "request": request,
            "title": "Bookkeeping Reconciliation",
            "imports": imports,
            "selected_import": selected_import,
            "reconciliation": reconciliation,
            "detected_posts": detected_posts,
            "success": success,
            "error": error,
        },
    )


@app.post("/bookkeeping/import-form")
async def bookkeeping_import_form(
    request: Request,
    show_label: str = Form(...),
    show_date: Optional[str] = Form(default=None),
    range_start: Optional[str] = Form(default=None),
    range_end: Optional[str] = Form(default=None),
    source_url: Optional[str] = Form(default=None),
    upload_file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    if not upload_file.filename:
        return RedirectResponse(
            url="/bookkeeping?error=Choose+a+CSV+or+XLSX+file+first",
            status_code=303,
        )

    try:
        imported = import_bookkeeping_file(
            session,
            filename=upload_file.filename,
            content=await upload_file.read(),
            show_label=show_label.strip(),
            show_date=parse_report_datetime(show_date),
            range_start=parse_report_datetime(range_start),
            range_end=parse_report_datetime(range_end, end_of_day=True),
            source_url=(source_url or "").strip() or None,
        )
        return RedirectResponse(
            url=f"/bookkeeping?import_id={imported.id}&success=Imported+{imported.row_count}+bookkeeping+rows",
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/bookkeeping?error={str(exc).replace(' ', '+')}",
            status_code=303,
        )


@app.post("/bookkeeping/import-detected/{message_id}")
async def bookkeeping_import_detected_message(
    request: Request,
    message_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        return RedirectResponse(
            url="/bookkeeping?error=Detected+bookkeeping+message+not+found",
            status_code=303,
        )

    from .bookkeeping import extract_google_sheet_url, auto_import_public_google_sheet

    sheet_url = extract_google_sheet_url(row.content or "")
    if not sheet_url:
        return RedirectResponse(
            url="/bookkeeping?error=No+Google+Sheet+link+found+on+that+message",
            status_code=303,
        )

    try:
        import_id = await auto_import_public_google_sheet(
            message_text=row.content or "",
            created_at=row.created_at,
            sheet_url=sheet_url,
        )
        if import_id:
            return RedirectResponse(
                url=f"/bookkeeping?import_id={import_id}&success=Imported+detected+Google+Sheet",
                status_code=303,
            )
        return RedirectResponse(
            url="/bookkeeping?error=Import+did+not+create+a+bookkeeping+record",
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/bookkeeping?error={str(exc).replace(' ', '+')}",
            status_code=303,
        )


@app.post("/bookkeeping/refresh-import/{import_id}")
async def bookkeeping_refresh_import(
    request: Request,
    import_id: int,
):
    if denial := require_role_response(request, "admin"):
        return denial
    try:
        refreshed_import_id = await refresh_bookkeeping_import_from_source(import_id)
        return RedirectResponse(
            url=f"/bookkeeping?import_id={refreshed_import_id}&success=Refreshed+bookkeeping+import",
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/bookkeeping?import_id={import_id}&error={str(exc).replace(' ', '+')}",
            status_code=303,
        )


@app.post("/messages/{message_id}/retry")
def retry_message(request: Request, message_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    row.parse_status = PARSE_PENDING
    row.parse_attempts = 0
    row.last_error = None
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    return {"ok": True, "message": f"Message {message_id} re-queued for parsing."}


@app.post("/messages/{message_id}/approve")
def approve_message(request: Request, message_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    row.needs_review = False
    row.parse_status = PARSE_PARSED
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()
    return {"ok": True, "message": f"Message {message_id} approved."}


@app.post("/messages/{message_id}/approve-form")
def approve_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reviewer_label = current_user_label(request)
    row = session.get(DiscordMessage, message_id)
    if row:
        row.needs_review = False
        row.parse_status = PARSE_PARSED
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
        session.add(row)
        sync_transaction_from_message(session, row)
        session.commit()
    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Approved+message+{message_id}", status_code=303)


@app.post("/messages/bulk/approve-form")
def bulk_approve_messages_form(
    request: Request,
    message_ids: list[int] = Form(default=[]),
    return_path: str = Form(default="/review-table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reviewer_label = current_user_label(request)
    updated = 0
    for message_id in message_ids:
        row = session.get(DiscordMessage, message_id)
        if not row:
            continue
        row.needs_review = False
        row.parse_status = PARSE_PARSED
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
        session.add(row)
        sync_transaction_from_message(session, row)
        updated += 1
    session.commit()

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Approved+{updated}+messages", status_code=303)


@app.post("/messages/bulk/reparse-form")
@app.post("/messages/bulk/retry-form")
def bulk_reparse_messages_form(
    request: Request,
    message_ids: list[int] = Form(default=[]),
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rows = [
        row
        for row in (
            session.get(DiscordMessage, message_id)
            for message_id in message_ids
        )
        if row is not None
    ]
    updated = reparse_message_rows(session, rows, reason="manual bulk reparse", reset_attempts=True)

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Reparsed+{updated}+messages", status_code=303)


@app.post("/messages/bulk/reparse-filtered-form")
@app.post("/messages/bulk/requeue-filtered-form")
def bulk_reparse_filtered_messages_form(
    request: Request,
    return_path: str = Form(default="/review"),
    status: Optional[str] = Form(default="review_queue"),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial

    stmt = build_message_stmt(
        status=status or "review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    row_ids = [
        row_id
        for row_id in session.exec(stmt.with_only_columns(DiscordMessage.id)).all()
        if row_id is not None
    ]

    def reparse_chunk(chunk_ids: list[int]) -> int:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.id.in_(chunk_ids))
        ).all()
        return reparse_message_rows(session, rows, reason="manual filtered reparse", reset_attempts=True)

    updated = 0
    chunk_size = 25
    for start_index in range(0, len(row_ids), chunk_size):
        updated += reparse_chunk(row_ids[start_index:start_index + chunk_size])

    redirect_url = build_return_url(
        return_path,
        status=status if return_path not in {"/review", "/review-table"} else None,
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(
        url=f"{redirect_url}{separator}success=Reparsed+{updated}+filtered+review+rows",
        status_code=303,
    )


@app.post("/messages/{message_id}/reparse-form")
@app.post("/messages/{message_id}/retry-form")
def reparse_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reparse_message_row(session, message_id, reason="manual row reparse", reset_attempts=True)

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Reparsed+message+{message_id}", status_code=303)


@app.post("/messages/{message_id}/mark-incorrect-form")
def mark_incorrect_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    row.needs_review = True
    row.parse_status = PARSE_REVIEW_REQUIRED
    row.last_error = "Manually marked incorrect for review."
    row.reviewed_by = None
    row.reviewed_at = None
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    selected_expense_category = filter_expense_category or expense_category
    detail_url = build_return_url(
        f"/deals/{message_id}",
        status="review_queue" if return_path == "/review-table" else status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in detail_url else "?"
    return RedirectResponse(
        url=f"{detail_url}{separator}success=Marked+message+{message_id}+incorrect+and+sent+to+review",
        status_code=303,
    )


@app.post("/messages/{message_id}/disregard-form")
def disregard_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    clear_parsed_fields(row)
    row.parse_status = PARSE_IGNORED
    row.needs_review = False
    row.last_error = None
    row.reviewed_by = current_user_label(request)
    row.reviewed_at = utcnow()
    row.notes = "Manually disregarded in review."
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(
        url=f"{redirect_url}{separator}success=Disregarded+message+{message_id}",
        status_code=303,
    )


@app.post("/admin/clear")
def clear_all_messages(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        attempts = session.exec(select(ParseAttempt)).all()
        rows = session.exec(select(DiscordMessage)).all()
        count = len(rows)
        for attempt in attempts:
            session.delete(attempt)
        for row in rows:
            session.delete(row)
        session.commit()

    return {"ok": True, "deleted": count}
@app.post("/admin/clear/form")
def clear_all_messages_form(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        attempts = session.exec(select(ParseAttempt)).all()
        rows = session.exec(select(DiscordMessage)).all()
        count = len(rows)
        for attempt in attempts:
            session.delete(attempt)
        for row in rows:
            session.delete(row)
        session.commit()

    return RedirectResponse(
        url=f"/table?success=Cleared+{count}+messages",
        status_code=303,
    )


@app.post("/admin/recompute-financials")
def admin_recompute_financials(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    updated = recompute_financial_fields(session)
    return {"ok": True, "updated": updated}


@app.post("/admin/recompute-financials/form")
def admin_recompute_financials_form(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    updated = recompute_financial_fields(session)
    return RedirectResponse(
        url=f"/table?success=Recomputed+financial+fields+for+{updated}+messages",
        status_code=303,
    )


@app.post("/admin/warm-attachment-cache")
def admin_warm_attachment_cache(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    extracted, already_cached = warm_attachment_cache(session)
    return {"ok": True, "extracted": extracted, "already_cached": already_cached}


@app.post("/admin/warm-attachment-cache/form")
def admin_warm_attachment_cache_form(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    extracted, already_cached = warm_attachment_cache(session)
    return RedirectResponse(
        url=f"/table?success=Cache+warmed:+{extracted}+extracted,+{already_cached}+already+cached",
        status_code=303,
    )


@app.post("/admin/rebuild-transactions")
def admin_rebuild_transactions(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    rebuilt = rebuild_transactions(session)
    return {"ok": True, "rebuilt": rebuilt}


@app.post("/admin/rebuild-transactions/form")
def admin_rebuild_transactions_form(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    rebuilt = rebuild_transactions(session)
    return RedirectResponse(
        url=f"/table?success=Rebuilt+{rebuilt}+normalized+transactions",
        status_code=303,
    )


@app.post("/admin/parser/reprocess-form")
def admin_parser_reprocess_form(
    request: Request,
    return_path: str = Form(default="/table"),
    force: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    queued = queue_auto_reprocess_candidates(
        session,
        force=bool(force),
    )
    separator = "&" if "?" in return_path else "?"
    mode_label = "manual+full" if force else "manual"
    return RedirectResponse(
        url=f"{return_path}{separator}success=Queued+{queued}+rows+for+{mode_label}+parser+reprocess",
        status_code=303,
    )


@app.post("/admin/parser/reparse-range")
def admin_parser_reparse_range(
    request: Request,
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    include_failed: Optional[str] = Form(default=None),
    include_ignored: Optional[str] = Form(default=None),
    include_reviewed: Optional[str] = Form(default=None),
    force_reviewed: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    start = parse_report_datetime(after)
    end = parse_report_datetime(before, end_of_day=True)
    if start is None and end is None:
        raise HTTPException(status_code=400, detail="Provide after and/or before to define a reparse range.")
    if include_reviewed and not force_reviewed:
        raise HTTPException(
            status_code=400,
            detail="Reviewed rows require force_reviewed to avoid overwriting manual review corrections.",
        )

    include_statuses = [PARSE_PARSED, PARSE_REVIEW_REQUIRED]
    if include_failed:
        include_statuses.append("failed")
    if include_ignored:
        include_statuses.append("ignored")

    run_id = safe_create_reparse_run(
        source="admin_api",
        reason="manual range reparse",
        range_after=start,
        range_before=end,
        channel_id=channel_id or None,
        include_reviewed=bool(include_reviewed),
        force_reviewed=bool(force_reviewed),
        requested_statuses=include_statuses,
    )

    result = queue_reparse_range(
        session,
        start=start,
        end=end,
        channel_id=channel_id or None,
        include_statuses=include_statuses,
        include_reviewed=bool(include_reviewed),
        reason="manual range reparse",
        reparse_run_id=run_id,
    )
    safe_finalize_reparse_run_queue(
        run_id=run_id,
        selected_count=result["matched"],
        queued_count=result["queued"],
        already_queued_count=result["already_queued"],
        skipped_reviewed_count=result["skipped_reviewed"],
        first_message_id=result["first_message_id"],
        last_message_id=result["last_message_id"],
        first_message_created_at=result["first_message_created_at"],
        last_message_created_at=result["last_message_created_at"],
    )
    return {
        "ok": True,
        "run_id": run_id,
        "queued": result["queued"],
        "matched": result["matched"],
        "channel_id": channel_id or None,
        "after": after or None,
        "before": before or None,
        "included_statuses": include_statuses,
        "include_reviewed": bool(include_reviewed),
    }


@app.post("/admin/parser/reparse-range-form")
def admin_parser_reparse_range_form(
    request: Request,
    return_path: str = Form(default="/table"),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    include_failed: Optional[str] = Form(default=None),
    include_ignored: Optional[str] = Form(default=None),
    include_reviewed: Optional[str] = Form(default=None),
    force_reviewed: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    start = parse_report_datetime(after)
    end = parse_report_datetime(before, end_of_day=True)
    if start is None and end is None:
        separator = "&" if "?" in return_path else "?"
        return RedirectResponse(
            url=f"{return_path}{separator}error=Provide+after+and/or+before+to+define+a+reparse+range",
            status_code=303,
        )
    if include_reviewed and not force_reviewed:
        separator = "&" if "?" in return_path else "?"
        return RedirectResponse(
            url=(
                f"{return_path}{separator}"
                "error=Reviewed+rows+require+force_reviewed+to+avoid+overwriting+manual+corrections"
            ),
            status_code=303,
        )

    include_statuses = [PARSE_PARSED, PARSE_REVIEW_REQUIRED]
    if include_failed:
        include_statuses.append("failed")
    if include_ignored:
        include_statuses.append("ignored")

    run_id = safe_create_reparse_run(
        source="admin_form",
        reason="manual range reparse",
        range_after=start,
        range_before=end,
        channel_id=channel_id or None,
        include_reviewed=bool(include_reviewed),
        force_reviewed=bool(force_reviewed),
        requested_statuses=include_statuses,
    )

    result = queue_reparse_range(
        session,
        start=start,
        end=end,
        channel_id=channel_id or None,
        include_statuses=include_statuses,
        include_reviewed=bool(include_reviewed),
        reason="manual range reparse",
        reparse_run_id=run_id,
    )
    safe_finalize_reparse_run_queue(
        run_id=run_id,
        selected_count=result["matched"],
        queued_count=result["queued"],
        already_queued_count=result["already_queued"],
        skipped_reviewed_count=result["skipped_reviewed"],
        first_message_id=result["first_message_id"],
        last_message_id=result["last_message_id"],
        first_message_created_at=result["first_message_created_at"],
        last_message_created_at=result["last_message_created_at"],
    )
    separator = "&" if "?" in return_path else "?"
    return RedirectResponse(
        url=(
            f"{return_path}{separator}"
            f"success=Queued+{result['queued']}+rows+for+parser+range+reparse"
        ),
        status_code=303,
    )


@app.get("/admin/parser/reparse-runs", response_class=HTMLResponse)
def admin_parser_reparse_runs_page(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    rows = list_recent_reparse_runs(session, limit=limit)
    return templates.TemplateResponse(
        request,
        "reparse_runs.html",
        {
            "request": request,
            "title": "Reparse Runs",
            "current_user": getattr(request.state, "current_user", None),
            "runs": build_reparse_run_table_rows(rows),
            "limit": limit,
        },
    )


@app.get("/admin/parser/reparse-runs.json")
def admin_parser_reparse_runs_json(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return {
        "runs": serialize_reparse_runs(list_recent_reparse_runs(session, limit=limit)),
    }


@app.get("/admin/parser/learned-rule-log", response_class=HTMLResponse)
def admin_parser_learned_rule_log_page(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return templates.TemplateResponse(
        request,
        "learned_rule_log.html",
        {
            "request": request,
            "title": "Learned Rule Log",
            "current_user": getattr(request.state, "current_user", None),
            "events": build_learned_rule_log_rows(session, limit=limit),
            "limit": limit,
        },
    )

@app.post("/admin/clear/channel/{channel_id}")
def clear_channel_messages(request: Request, channel_id: str):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)
        row_ids = [row.id for row in rows if row.id is not None]
        if row_ids:
            session.exec(delete(ParseAttempt).where(ParseAttempt.message_id.in_(row_ids)))
        for row in rows:
            session.delete(row)

        session.commit()

    return {
        "ok": True,
        "channel_id": channel_id,
        "deleted": count,
    }
@app.post("/admin/clear/channel")
def clear_channel_messages_form(
    request: Request,
    channel_id: str = Form(...),
):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)
        channel_name = rows[0].channel_name if rows else channel_id
        row_ids = [row.id for row in rows if row.id is not None]
        if row_ids:
            session.exec(delete(ParseAttempt).where(ParseAttempt.message_id.in_(row_ids)))
        for row in rows:
            session.delete(row)

        session.commit()

    return RedirectResponse(
        url=f"/table?success=Cleared+{count}+messages+from+{channel_name}",
        status_code=303,
    )

@app.post("/admin/backfill")
async def admin_backfill(
    request: Request,
    channel_id: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    oldest_first: bool = Form(default=True),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    _, _, after_dt, before_dt = validate_backfill_range(after, before)
    target_channel_ids = get_backfill_target_channel_ids(session, channel_id=channel_id)
    if not target_channel_ids:
        raise HTTPException(status_code=400, detail="No backfill-enabled watched channels are available for this request")
    persist_backfill_window_for_targets(
        session,
        channel_ids=target_channel_ids,
        after_dt=after_dt,
        before_dt=before_dt,
    )
    queued_message = queue_backfill_request(
        session,
        request,
        channel_id=channel_id,
        after_dt=after_dt,
        before_dt=before_dt,
        limit=limit,
        oldest_first=oldest_first,
    )
    trigger_backfill_claim_attempt(get_discord_client())
    return {"ok": True, "queued": True, "message": queued_message.replace("+", " ")}


@app.post("/admin/backfill/form")
async def admin_backfill_form(
    request: Request,
    channel_id: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    oldest_first: bool = Form(default=True),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    try:
        _, _, after_dt, before_dt = validate_backfill_range(after, before)
        target_channel_ids = get_backfill_target_channel_ids(session, channel_id=channel_id)
        if not target_channel_ids:
            return RedirectResponse(
                url="/table?error=No+backfill-enabled+watched+channels+are+available+for+this+request",
                status_code=303,
            )
        persist_backfill_window_for_targets(
            session,
            channel_ids=target_channel_ids,
            after_dt=after_dt,
            before_dt=before_dt,
        )
        queued_message = queue_backfill_request(
            session,
            request,
            channel_id=channel_id,
            after_dt=after_dt,
            before_dt=before_dt,
            limit=limit,
            oldest_first=oldest_first,
        )
        trigger_backfill_claim_attempt(get_discord_client())
        return RedirectResponse(url=f"/table?success={queued_message}", status_code=303)

    except Exception as e:
        return RedirectResponse(url=f"/table?error={str(e)}", status_code=303)


@app.post("/admin/backfill/cancel")
def admin_cancel_backfill(
    request: Request,
    request_id: int = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    ok, message = cancel_backfill_request(
        session,
        request_id,
        requested_by=current_user_label(request),
    )
    destination = "success" if ok else "error"
    encoded_message = message.replace(" ", "+")
    return RedirectResponse(url=f"/table?{destination}={encoded_message}", status_code=303)


@app.get("/table", response_class=HTMLResponse)
def messages_table(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=REPORT_SOURCE_ALL),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    selected_source = normalize_report_source(source)
    shopify_table_only = selected_source == REPORT_SOURCE_SHOPIFY

    if shopify_table_only:
        rows = []
        total_rows = 0
        items = []
    else:
        rows, total_rows = get_message_rows(
            session,
            status=status,
            channel_id=channel_id,
            expense_category=expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
        items = build_message_list_items(session, rows, expense_category=expense_category)
    channels = get_channel_filter_choices(session)
    expense_category_options = get_expense_category_filter_choices(session)
    summary = (
        get_summary(
            session,
            status=status,
            channel_id=channel_id,
            expense_category=expense_category,
            after=after,
            before=before,
        )
        if not shopify_table_only
        else {"total": 0, "parsed": 0, "needs_review": 0, "failed": 0, "queued": 0, "processing": 0, "ignored": 0, "with_images": 0}
    )
    watched_channels = get_watched_channels(session)
    available_discord_channels, has_live_available_discord_channels = get_available_channel_choices(session)
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    if shopify_table_only:
        financial_summary = build_financial_summary([])
    else:
        financial_rows = get_financial_rows(
            session,
            start=parse_report_datetime(after),
            end=parse_report_datetime(before, end_of_day=True),
            channel_id=channel_id,
        )
        if expense_category:
            financial_rows = [row for row in financial_rows if row.expense_category == expense_category]
        financial_summary = build_financial_summary(financial_rows)
    recent_backfill_requests = serialize_backfill_requests(list_recent_backfill_requests(session))
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    parser_progress = get_parser_progress(
        session,
        status=status,
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    review_shortcuts = []

    return templates.TemplateResponse(
        request,
        "messages_table.html",
        {
            "request": request,
            "title": "Messages Table",
            "return_path": "/table",
            "is_review_page": False,
            "rows": items,
            "channels": channels,
            "expense_category_options": expense_category_options,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_status": status or "",
            "selected_source": selected_source,
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by or "time",
            "selected_sort_dir": sort_dir or "desc",
            "selected_limit": limit,
            "pagination": pagination,
            "summary": summary,
            "financial_summary": financial_summary,
            "recent_backfill_requests": recent_backfill_requests,
            "parser_progress": parser_progress,
            "success": success,
            "error": error,
            "watched_channels": watched_channels,
            "watched_channel_groups": watched_channel_groups,
            "available_discord_channels": available_discord_channels,
            "has_live_available_discord_channels": has_live_available_discord_channels,
            "next_sort_direction": next_sort_direction,
            "sort_indicator": sort_indicator,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "review_shortcuts": review_shortcuts,
            "shopify_source_notice": {
                "link": build_shopify_orders_url(start=after or "", end=before or ""),
                "message": "Shopify orders are on the Shopify Orders page.",
            } if shopify_table_only else None,
        },
    )


@app.get("/review-table", response_class=HTMLResponse)
def review_table(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows, expense_category=expense_category)
    channels = get_channel_filter_choices(session)
    expense_category_options = get_expense_category_filter_choices(session)
    summary = get_summary(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    financial_rows = get_financial_rows(session)
    if expense_category:
        financial_rows = [row for row in financial_rows if (row.expense_category or "") == expense_category]
    financial_summary = build_financial_summary(financial_rows)
    watched_channels = get_watched_channels(session)
    available_discord_channels, has_live_available_discord_channels = get_available_channel_choices(session)
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    recent_backfill_requests = serialize_backfill_requests(list_recent_backfill_requests(session))
    parser_progress = get_parser_progress(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    review_shortcuts = build_review_shortcuts(items)

    return templates.TemplateResponse(
        request,
        "messages_table.html",
        {
            "request": request,
            "title": "Review Queue",
            "return_path": "/review-table",
            "is_review_page": True,
            "rows": items,
            "channels": channels,
            "expense_category_options": expense_category_options,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_status": "review_queue",
            "selected_source": REPORT_SOURCE_DISCORD,
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "selected_limit": limit,
            "pagination": pagination,
            "summary": summary,
            "financial_summary": financial_summary,
            "recent_backfill_requests": recent_backfill_requests,
            "parser_progress": parser_progress,
            "success": success,
            "error": error,
            "watched_channels": watched_channels,
            "watched_channel_groups": watched_channel_groups,
            "available_discord_channels": available_discord_channels,
            "has_live_available_discord_channels": has_live_available_discord_channels,
            "next_sort_direction": next_sort_direction,
            "sort_indicator": sort_indicator,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "review_shortcuts": review_shortcuts,
            "shopify_source_notice": None,
        },
    )


@app.get("/review", response_class=HTMLResponse)
def reviewer_queue_page(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows, expense_category=expense_category)
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    channels = get_channel_filter_choices(session)
    expense_category_options = get_expense_category_filter_choices(session)
    summary = get_summary(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )

    return templates.TemplateResponse(
        request,
        "review_queue.html",
        {
            "request": request,
            "title": "Review Queue",
            "rows": items,
            "channels": channels,
            "expense_category_options": expense_category_options,
            "summary": summary,
            "pagination": pagination,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "selected_limit": limit,
            "success": success,
            "error": error,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "current_user": getattr(request.state, "current_user", None),
        },
    )


@app.get("/review/focus/{message_id}", response_class=HTMLResponse)
def reviewer_focus_page(
    message_id: int,  # build_message_list_items supplies cached/proxy attachment URLs
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    item = build_message_list_items(session, [row], expense_category=expense_category)[0]
    ordered_ids = get_ordered_message_ids(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    current_index = ordered_ids.index(message_id) if message_id in ordered_ids else -1
    previous_id = ordered_ids[current_index - 1] if current_index > 0 else None
    next_id = ordered_ids[current_index + 1] if current_index >= 0 and current_index < len(ordered_ids) - 1 else None

    return templates.TemplateResponse(
        request,
        "review_focus.html",
        {
            "request": request,
            "title": f"Review Deal {message_id}",
            "message": item,
            "success": success,
            "error": error,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "selected_page": page,
            "selected_limit": limit,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "back_url": build_return_url(
                "/review",
                channel_id=channel_id,
                expense_category=expense_category,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ),
            "previous_url": build_return_url(
                f"/review/focus/{previous_id}",
                channel_id=channel_id,
                expense_category=expense_category,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ) if previous_id else None,
            "next_url": build_return_url(
                f"/review/focus/{next_id}",
                channel_id=channel_id,
                expense_category=expense_category,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ) if next_id else None,
            "current_position": current_index + 1 if current_index >= 0 else None,
            "queue_size": len(ordered_ids),
            "current_user": getattr(request.state, "current_user", None),
        },
    )


@app.get("/review/history", response_class=HTMLResponse)
def reviewer_history_page(
    request: Request,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial

    rows, total_rows = get_review_history_rows(session, page=page, limit=limit)
    items = build_message_list_items(session, rows)
    return templates.TemplateResponse(
        request,
        "review_history.html",
        {
            "request": request,
            "title": "Review History",
            "rows": items,
            "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
            "current_user": getattr(request.state, "current_user", None),
        },
    )
@app.get("/admin/channels")
def admin_list_channels(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    rows = get_watched_channels(session)
    return [
        {
            "id": row.id,
            "channel_id": row.channel_id,
            "channel_name": row.channel_name,
            "is_enabled": row.is_enabled,
            "backfill_enabled": row.backfill_enabled,
        }
        for row in rows
    ]


@app.post("/admin/channels/add")
async def admin_add_channel(
    request: Request,
    channel_ids: Optional[list[str]] = Form(default=None),
    manual_channel_ids: Optional[str] = Form(default=None),
    channel_name: Optional[str] = Form(default=None),
    backfill_after: Optional[str] = Form(default=None),
    backfill_before: Optional[str] = Form(default=None),
    backfill_enabled: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    manual_ids = []
    if manual_channel_ids:
        manual_ids = [
            piece.strip()
            for piece in manual_channel_ids.replace("\r", ",").replace("\n", ",").split(",")
        ]
    cleaned_channel_ids = normalize_channel_ids([*(channel_ids or []), *manual_ids])
    if not cleaned_channel_ids:
        return RedirectResponse(
            url="/table?error=Select+at+least+one+valid+channel",
            status_code=303,
        )

    try:
        _, _, after_dt, before_dt = validate_backfill_range(backfill_after, backfill_before)
    except HTTPException as exc:
        return RedirectResponse(url=f"/table?error={exc.detail}", status_code=303)

    should_enable_backfill = backfill_enabled is not None
    saved_channels = []
    for channel_id in cleaned_channel_ids:
        saved_channels.append(
            upsert_watched_channel(
                session,
                channel_id=channel_id,
                channel_name=channel_name if len(cleaned_channel_ids) == 1 else None,
                is_enabled=True,
                backfill_enabled=should_enable_backfill,
                backfill_after=after_dt,
                backfill_before=before_dt,
            )
        )

    if after_dt or before_dt:
        client = get_discord_client()
        if client is None or not client.is_ready():
            queued_count = 0
            for channel in saved_channels:
                queue_backfill_request(
                    session,
                    request,
                    channel_id=channel.channel_id,
                    after_dt=after_dt,
                    before_dt=before_dt,
                    limit=None,
                    oldest_first=True,
                )
                queued_count += 1
            return RedirectResponse(
                url=f"/table?success=Saved+{len(saved_channels)}+channels+and+queued+{queued_count}+backfill+request(s)+for+the+worker",
                status_code=303,
            )

        total_inserted = 0
        total_skipped = 0
        failed_channels: list[str] = []
        for channel in saved_channels:
            result = await client.backfill_channel(
                channel_id=int(channel.channel_id),
                after=after_dt,
                before=before_dt,
                oldest_first=True,
            )
            if result.get("ok"):
                total_inserted += result.get("inserted", 0)
                total_skipped += result.get("skipped", 0)
            else:
                failed_channels.append(channel.channel_id)

        if failed_channels:
            failed_text = ",".join(failed_channels)
            return RedirectResponse(
                url=f"/table?error=Saved+{len(saved_channels)}+channels+but+backfill+failed+for:+{failed_text}",
                status_code=303,
            )

        msg = (
            f"Saved+{len(saved_channels)}+channels+and+backfilled+range:"
            f"+inserted={total_inserted},+skipped={total_skipped}"
        )
        return RedirectResponse(url=f"/table?success={msg}", status_code=303)

    return RedirectResponse(
        url=f"/table?success=Saved+{len(saved_channels)}+channel(s)",
        status_code=303,
    )

@app.post("/admin/channels/toggle")
def admin_toggle_channel(
    request: Request,
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    row.is_enabled = not row.is_enabled
    row.updated_at = utcnow()
    session.add(row)
    session.commit()

    state = "enabled" if row.is_enabled else "disabled"
    return RedirectResponse(
        url=f"/table?success=Channel+{channel_id}+{state}",
        status_code=303,
    )


@app.post("/admin/channels/toggle-backfill")
def admin_toggle_channel_backfill(
    request: Request,
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    row.backfill_enabled = not row.backfill_enabled
    row.updated_at = utcnow()
    session.add(row)
    session.commit()

    state = "enabled" if row.backfill_enabled else "disabled"
    return RedirectResponse(
        url=f"/table?success=Backfill+for+channel+{channel_id}+{state}",
        status_code=303,
    )


@app.post("/admin/channels/remove")
def admin_remove_channel(
    request: Request,
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    session.delete(row)
    session.commit()

    return RedirectResponse(
        url=f"/table?success=Removed+channel+{channel_id}",
        status_code=303,
    )
@app.get("/admin/discord/channels")
def admin_list_discord_channels(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    channels = list_available_discord_channels()
    return channels


# ===========================================================================
# Streamer helpers
# ===========================================================================


def get_streamer_names(session: Session) -> list[str]:
    """Return active streamer names from DB; fall back to STREAMERS constant."""
    db_streamers = session.exec(
        select(Streamer).where(Streamer.is_active == True).order_by(Streamer.name)
    ).all()
    if db_streamers:
        return [s.display_name or s.name for s in db_streamers]
    return STREAMERS


def _now_pacific() -> datetime:
    """Return current time in US/Pacific."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Los_Angeles"))


def get_current_streamer(session: Session) -> Optional[str]:
    """Return the streamer name for whoever is scheduled right now (Pacific time), or None."""
    now = _now_pacific()
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")

    schedules = session.exec(
        select(StreamSchedule).where(StreamSchedule.date == today)
    ).all()
    for sched in schedules:
        if sched.start_time <= current_time <= sched.end_time:
            streamer = session.get(Streamer, sched.streamer_id)
            if streamer and streamer.is_active:
                return streamer.display_name or streamer.name
    return None


# ===========================================================================
# Live Hit Tracker
# ===========================================================================


def _build_hits_stmt(
    *,
    streamer: Optional[str] = None,
    after: Optional[datetime] = None,
    before: Optional[datetime] = None,
    search: Optional[str] = None,
    min_value: Optional[float] = None,
):
    from sqlalchemy import or_

    stmt = select(LiveHit).where(LiveHit.is_deleted == False)
    if streamer:
        stmt = stmt.where(LiveHit.streamer_name == streamer)
    if after:
        stmt = stmt.where(LiveHit.hit_at >= after)
    if before:
        stmt = stmt.where(LiveHit.hit_at <= before)
    if search:
        like = f"%{search}%"
        stmt = stmt.where(
            or_(
                LiveHit.customer_name.ilike(like),
                LiveHit.order_number.ilike(like),
                LiveHit.hit_note.ilike(like),
            )
        )
    if min_value is not None:
        stmt = stmt.where(LiveHit.estimated_value >= min_value)
    return stmt.order_by(LiveHit.hit_at.desc())


def _parse_hit_at(raw: Optional[str]) -> datetime:
    """Parse a datetime-local string from a form field; fall back to utcnow."""
    if raw:
        try:
            return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return utcnow()


def _parse_optional_float(raw: Optional[str]) -> Optional[float]:
    if raw and raw.strip():
        try:
            return float(raw.strip())
        except ValueError:
            pass
    return None


def _hit_to_dict(h: LiveHit) -> dict:
    return {
        "id": h.id,
        "hit_at": h.hit_at.isoformat() if h.hit_at else None,
        "streamer_name": h.streamer_name,
        "customer_name": h.customer_name or "",
        "order_number": h.order_number or "",
        "hit_note": h.hit_note,
        "estimated_value": h.estimated_value,
        "order_value": h.order_value,
        "platform": h.platform or "",
        "stream_label": h.stream_label or "",
        "notes": h.notes or "",
        "created_by": h.created_by or "",
        "created_at": h.created_at.isoformat() if h.created_at else None,
        "is_big_hit": (h.estimated_value or 0) >= BIG_HIT_THRESHOLD,
    }


@app.get("/hits", response_class=HTMLResponse)
def hits_list_page(
    request: Request,
    streamer: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    min_value: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    after_dt: Optional[datetime] = None
    before_dt: Optional[datetime] = None
    if after:
        try:
            after_dt = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if before:
        try:
            before_dt = datetime.fromisoformat(before).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    min_val = _parse_optional_float(min_value)

    stmt = _build_hits_stmt(
        streamer=streamer,
        after=after_dt,
        before=before_dt,
        search=search,
        min_value=min_val,
    )
    if platform:
        stmt = stmt.where(LiveHit.platform == platform)

    total_rows = count_rows(session, stmt)
    offset = (max(page, 1) - 1) * limit
    hits = session.exec(stmt.offset(offset).limit(limit)).all()
    pagination = build_pagination(page, limit, total_rows)

    # Summary stats for current filter (fetch all rows without pagination)
    all_hits = session.exec(stmt).all()
    total_value = sum(h.estimated_value or 0 for h in all_hits)
    big_hits_count = sum(1 for h in all_hits if (h.estimated_value or 0) >= BIG_HIT_THRESHOLD)
    active_streamers = len({h.streamer_name for h in all_hits})

    return templates.TemplateResponse(request, "hits.html", {
        "request": request,
        "title": "Live Hit Tracker",
        "current_user": getattr(request.state, "current_user", None),
        "hits": hits,
        "pagination": pagination,
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
        "total_rows": total_rows,
        "total_value": total_value,
        "big_hits_count": big_hits_count,
        "active_streamers": active_streamers,
        # filter values for form re-population
        "sel_streamer": streamer or "",
        "sel_platform": platform or "",
        "sel_after": after or "",
        "sel_before": before or "",
        "sel_search": search or "",
        "sel_min_value": min_value or "",
        "sel_limit": limit,
    })


@app.get("/hits/new", response_class=HTMLResponse)
def hits_new_page(
    request: Request,
    success: Optional[str] = Query(default=None),
    last_streamer: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    recent = session.exec(
        select(LiveHit)
        .where(LiveHit.is_deleted == False)
        .order_by(LiveHit.hit_at.desc())
        .limit(5)
    ).all()

    return templates.TemplateResponse(request, "hits_new.html", {
        "request": request,
        "title": "Log a Hit",
        "current_user": getattr(request.state, "current_user", None),
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "success": success,
        "last_streamer": last_streamer or get_current_streamer(session) or "",
        "recent_hits": recent,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
    })


@app.post("/hits/new")
def hits_new_submit(
    request: Request,
    streamer_name: str = Form(...),
    hit_note: str = Form(...),
    customer_name: Optional[str] = Form(default=None),
    order_number: Optional[str] = Form(default=None),
    estimated_value: Optional[str] = Form(default=None),
    order_value: Optional[str] = Form(default=None),
    platform: Optional[str] = Form(default=None),
    stream_label: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    hit_at_raw: Optional[str] = Form(default=None),
    add_another: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = LiveHit(
        streamer_name=streamer_name.strip(),
        hit_note=hit_note.strip(),
        customer_name=(customer_name or "").strip() or None,
        order_number=(order_number or "").strip() or None,
        estimated_value=_parse_optional_float(estimated_value),
        order_value=_parse_optional_float(order_value),
        platform=(platform or "").strip() or None,
        stream_label=(stream_label or "").strip() or None,
        notes=(notes or "").strip() or None,
        hit_at=_parse_hit_at(hit_at_raw),
        created_by=current_user_label(request),
    )
    session.add(hit)
    session.commit()

    qs = f"success=1&last_streamer={hit.streamer_name}"
    if add_another:
        return RedirectResponse(url=f"/hits/new?{qs}", status_code=303)
    return RedirectResponse(url=f"/hits?success=1", status_code=303)


@app.get("/hits/{hit_id}/edit", response_class=HTMLResponse)
def hits_edit_page(
    request: Request,
    hit_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = session.get(LiveHit, hit_id)
    if not hit or hit.is_deleted:
        return RedirectResponse(url="/hits?error=Hit+not+found", status_code=303)

    return templates.TemplateResponse(request, "hits_edit.html", {
        "request": request,
        "title": "Edit Hit",
        "current_user": getattr(request.state, "current_user", None),
        "hit": hit,
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
    })


@app.post("/hits/{hit_id}/edit")
def hits_edit_submit(
    request: Request,
    hit_id: int,
    streamer_name: str = Form(...),
    hit_note: str = Form(...),
    customer_name: Optional[str] = Form(default=None),
    order_number: Optional[str] = Form(default=None),
    estimated_value: Optional[str] = Form(default=None),
    order_value: Optional[str] = Form(default=None),
    platform: Optional[str] = Form(default=None),
    stream_label: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    hit_at_raw: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = session.get(LiveHit, hit_id)
    if not hit or hit.is_deleted:
        return RedirectResponse(url="/hits?error=Hit+not+found", status_code=303)

    hit.streamer_name = streamer_name.strip()
    hit.hit_note = hit_note.strip()
    hit.customer_name = (customer_name or "").strip() or None
    hit.order_number = (order_number or "").strip() or None
    hit.estimated_value = _parse_optional_float(estimated_value)
    hit.order_value = _parse_optional_float(order_value)
    hit.platform = (platform or "").strip() or None
    hit.stream_label = (stream_label or "").strip() or None
    hit.notes = (notes or "").strip() or None
    hit.hit_at = _parse_hit_at(hit_at_raw)
    hit.updated_at = utcnow()
    session.add(hit)
    session.commit()

    return RedirectResponse(url="/hits?success=1", status_code=303)


@app.post("/hits/{hit_id}/delete")
def hits_delete(
    request: Request,
    hit_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = session.get(LiveHit, hit_id)
    if hit and not hit.is_deleted:
        hit.is_deleted = True
        hit.updated_at = utcnow()
        session.add(hit)
        session.commit()

    return RedirectResponse(url="/hits?success=1", status_code=303)


@app.get("/api/hits/export")
def hits_export_csv(
    request: Request,
    streamer: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    min_value: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    after_dt: Optional[datetime] = None
    before_dt: Optional[datetime] = None
    if after:
        try:
            after_dt = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if before:
        try:
            before_dt = datetime.fromisoformat(before).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    stmt = _build_hits_stmt(
        streamer=streamer,
        after=after_dt,
        before=before_dt,
        search=search,
        min_value=_parse_optional_float(min_value),
    )
    if platform:
        stmt = stmt.where(LiveHit.platform == platform)

    hits = session.exec(stmt).all()
    rows = [_hit_to_dict(h) for h in hits]
    # Drop the computed field; keep only DB columns for export
    for r in rows:
        r.pop("is_big_hit", None)

    return csv_response("hits_export.csv", rows if rows else [
        {"message": "No hits found for the given filters"}
    ])


@app.get("/api/hits/summary")
def hits_summary_json(
    request: Request,
    streamer: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    after_dt: Optional[datetime] = None
    before_dt: Optional[datetime] = None
    if after:
        try:
            after_dt = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if before:
        try:
            before_dt = datetime.fromisoformat(before).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    hits = session.exec(_build_hits_stmt(streamer=streamer, after=after_dt, before=before_dt)).all()

    per_streamer: dict = {}
    total_value = 0.0
    big_hits = 0
    for h in hits:
        per_streamer[h.streamer_name] = per_streamer.get(h.streamer_name, 0) + 1
        total_value += h.estimated_value or 0
        if (h.estimated_value or 0) >= BIG_HIT_THRESHOLD:
            big_hits += 1

    return {
        "total_hits": len(hits),
        "total_estimated_value": round(total_value, 2),
        "big_hits_count": big_hits,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
        "per_streamer": per_streamer,
    }


@app.post("/api/hits")
async def hits_api_create(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)

    streamer_name = (body.get("streamer_name") or "").strip()
    hit_note = (body.get("hit_note") or "").strip()
    if not streamer_name or not hit_note:
        return JSONResponse({"ok": False, "error": "streamer_name and hit_note are required"}, status_code=422)

    hit = LiveHit(
        streamer_name=streamer_name,
        hit_note=hit_note,
        customer_name=(body.get("customer_name") or "").strip() or None,
        order_number=(body.get("order_number") or "").strip() or None,
        estimated_value=_parse_optional_float(str(body.get("estimated_value", "")) if body.get("estimated_value") is not None else ""),
        order_value=_parse_optional_float(str(body.get("order_value", "")) if body.get("order_value") is not None else ""),
        platform=(body.get("platform") or "").strip() or None,
        stream_label=(body.get("stream_label") or "").strip() or None,
        notes=(body.get("notes") or "").strip() or None,
        hit_at=_parse_hit_at(body.get("hit_at")),
        created_by=current_user_label(request),
    )
    session.add(hit)
    session.commit()
    session.refresh(hit)

    return JSONResponse({"ok": True, "id": hit.id, "hit": _hit_to_dict(hit)})


@app.get("/api/hits/recent")
def hits_api_recent(
    request: Request,
    limit: int = Query(default=5, ge=1, le=50),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hits = session.exec(
        select(LiveHit)
        .where(LiveHit.is_deleted == False)
        .order_by(LiveHit.hit_at.desc())
        .limit(limit)
    ).all()

    return {"hits": [_hit_to_dict(h) for h in hits]}


# ===========================================================================
# Stream Manager — Streamer Profiles & Schedule
# ===========================================================================


@app.get("/stream-manager", response_class=HTMLResponse)
def stream_manager_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamers = session.exec(
        select(Streamer).where(Streamer.is_active == True).order_by(Streamer.name)
    ).all()

    # Get schedule for the next 7 days (Pacific time)
    now_pst = _now_pacific()
    today = now_pst.strftime("%Y-%m-%d")
    end_date = (now_pst + timedelta(days=7)).strftime("%Y-%m-%d")
    schedules = session.exec(
        select(StreamSchedule)
        .where(StreamSchedule.date >= today, StreamSchedule.date <= end_date)
        .order_by(StreamSchedule.date, StreamSchedule.start_time)
    ).all()

    # Build a lookup for streamer names by id
    streamer_map = {s.id: s for s in streamers}

    # Enrich schedules with streamer info
    schedule_items = []
    for sched in schedules:
        s = streamer_map.get(sched.streamer_id)
        schedule_items.append({
            "id": sched.id,
            "date": sched.date,
            "start_time": sched.start_time,
            "end_time": sched.end_time,
            "title": sched.title or "",
            "notes": sched.notes or "",
            "streamer_name": (s.display_name or s.name) if s else "Unknown",
            "streamer_color": (s.color or "#fe2c55") if s else "#fe2c55",
            "streamer_emoji": (s.avatar_emoji or "🎮") if s else "🎮",
        })

    current_streamer = get_current_streamer(session)

    return templates.TemplateResponse(request, "stream_manager.html", {
        "request": request,
        "title": "Stream Manager",
        "current_user": getattr(request.state, "current_user", None),
        "streamers": streamers,
        "streamer_colors": STREAMER_COLORS,
        "schedules": schedule_items,
        "current_streamer": current_streamer,
        "today": today,
    })


@app.post("/stream-manager/streamer/add")
def stream_manager_add_streamer(
    request: Request,
    name: str = Form(...),
    display_name: Optional[str] = Form(default=None),
    color: Optional[str] = Form(default=None),
    avatar_emoji: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamer = Streamer(
        name=name.strip(),
        display_name=(display_name or "").strip() or None,
        color=(color or "").strip() or None,
        avatar_emoji=(avatar_emoji or "").strip() or None,
    )
    session.add(streamer)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Streamer+added", status_code=303)


@app.post("/stream-manager/streamer/{streamer_id}/edit")
def stream_manager_edit_streamer(
    request: Request,
    streamer_id: int,
    name: str = Form(...),
    display_name: Optional[str] = Form(default=None),
    color: Optional[str] = Form(default=None),
    avatar_emoji: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamer = session.get(Streamer, streamer_id)
    if not streamer:
        return RedirectResponse(url="/stream-manager?error=Streamer+not+found", status_code=303)

    streamer.name = name.strip()
    streamer.display_name = (display_name or "").strip() or None
    streamer.color = (color or "").strip() or None
    streamer.avatar_emoji = (avatar_emoji or "").strip() or None
    streamer.updated_at = utcnow()
    session.add(streamer)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Streamer+updated", status_code=303)


@app.post("/stream-manager/streamer/{streamer_id}/delete")
def stream_manager_delete_streamer(
    request: Request,
    streamer_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    streamer = session.get(Streamer, streamer_id)
    if streamer:
        streamer.is_active = False
        streamer.updated_at = utcnow()
        session.add(streamer)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Streamer+removed", status_code=303)


@app.post("/stream-manager/schedule/add")
def stream_manager_add_schedule(
    request: Request,
    streamer_id: str = Form(...),
    date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    title: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    sched = StreamSchedule(
        streamer_id=int(streamer_id),
        date=date.strip(),
        start_time=start_time.strip(),
        end_time=end_time.strip(),
        title=(title or "").strip() or None,
        notes=(notes or "").strip() or None,
    )
    session.add(sched)
    session.commit()
    return RedirectResponse(url="/stream-manager?success=Shift+added", status_code=303)


@app.post("/stream-manager/schedule/{schedule_id}/delete")
def stream_manager_delete_schedule(
    request: Request,
    schedule_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    sched = session.get(StreamSchedule, schedule_id)
    if sched:
        session.delete(sched)
        session.commit()
    return RedirectResponse(url="/stream-manager?success=Shift+removed", status_code=303)


@app.get("/api/stream-manager/current-streamer")
def api_current_streamer(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    return {"current_streamer": get_current_streamer(session)}
