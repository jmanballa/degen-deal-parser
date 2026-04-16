"""
Shared helpers, constants, state management, and utility functions.

Extracted from main.py to be imported by router modules.
This module must NOT import from main.py or any router module.
"""
import asyncio
import csv
import hashlib
import hmac
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
from fastapi import Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select

from .auth import authenticate_user, has_role
from .attachment_storage import attachment_cache_path, generate_thumbnail, warm_attachment_cache, write_attachment_cache_file
from .bookkeeping import (
    get_bookkeeping_status_by_message_ids,
)
from .cache import cache_get, cache_invalidate, cache_set
from .backfill_requests import (
    enqueue_backfill_request,
)
from .channels import (
    get_channel_filter_choices,
    get_expense_category_filter_choices,
    get_watched_channels,
    update_backfill_window,
)
from .config import get_settings
from .corrections import (
    get_learning_signal,
    get_learning_signals,
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
    parse_iso_datetime,
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
    StreamAccount,
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
    build_tiktok_buyer_insights,
    build_tiktok_orders_page_data as build_tiktok_orders_page_reporting_data,
    build_tiktok_product_performance,
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
    mark_inventory_sold_from_shopify_order,
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
    get_stream_viewers,
    start_live_chat,
    stop_live_chat,
)
from .worker import (
    STALE_PROCESSING_AFTER,
    clear_parsed_fields,
    parser_loop,
    periodic_inventory_price_loop,
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
except ImportError:
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


def _safe_json_load(value: str | None):
    """Safely decode an optional JSON string. Returns None if empty or invalid."""
    if not value:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


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

def _compute_build_version() -> str:
    import subprocess as _sp
    try:
        return _sp.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(Path(__file__).resolve().parent.parent),
            stderr=_sp.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        try:
            return hashlib.md5(
                str(Path(__file__).stat().st_mtime).encode()
            ).hexdigest()[:10]
        except Exception:
            return "unknown"

_BUILD_VERSION: str = _compute_build_version()


def _get_live_session_snapshot() -> dict:
    with _live_session_lock:
        return dict(_live_session_cache)

def _is_currently_live() -> bool:
    """Return True if a stream is actively running or ended within the last 15 minutes."""
    snap = _get_live_session_snapshot()
    if not snap.get("ok"):
        return False
    start_ts = snap.get("start_time") or 0
    end_ts = snap.get("end_time") or 0
    if start_ts <= 0:
        return False
    if end_ts <= 0:
        return True
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

        def _session_recency_key(s: dict) -> tuple:
            et = s.get("end_time") or 0
            st = s.get("start_time") or 0
            is_active = 1 if et == 0 and st > 0 else 0
            return (is_active, et, st)

        latest = max(sessions, key=_session_recency_key)
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
    except Exception as exc:
        print(
            structured_log_line(
                runtime="app",
                action="tiktok.state.persist_failed",
                success=False,
                context="shared._persist_tiktok_state",
                error=str(exc)[:400],
            )
        )  # best-effort; don't crash the request


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
    except Exception as exc:
        print(
            structured_log_line(
                runtime="app",
                action="tiktok.state.load_failed",
                success=False,
                context="shared._load_tiktok_state_from_db",
                error=str(exc)[:400],
            )
        )  # best-effort; proceed with empty state


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


def _tiktok_webhook_enrich_error_is_retryable_auth(exc: BaseException) -> bool:
    """HTTP 401 or TikTok business-layer token errors (JSON body, HTTP 200)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response is not None and exc.response.status_code == 401
    if isinstance(exc, RuntimeError):
        text = str(exc).lower()
        return any(
            phrase in text
            for phrase in (
                "invalid access",
                "access token",
                "access_token",
                "token expired",
                "expired token",
            )
        )
    return False


def _enrich_tiktok_order_from_api(order_id: str) -> None:
    """Fetch full order details from TikTok API and update the DB record."""
    if _fetch_tiktok_order_details is None or _order_record_from_payload is None:
        return
    runtime_name = f"{settings.runtime_name}_tiktok_webhook_enrich"
    try:
        with managed_session() as session:
            auth_row = ensure_tiktok_auth_row(session)
            if auth_row is None and not ((settings.tiktok_shop_id or "").strip() and (settings.tiktok_access_token or "").strip()):
                return

            refresh_result = _refresh_tiktok_auth_if_needed(session, runtime_name=runtime_name)
            if refresh_result is not None:
                auth_row = ensure_tiktok_auth_row(session)

            shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
            if not access_token or (not shop_id and not shop_cipher):
                return

            def _fetch_details_with_token(
                sid: str, scipher: str, current_access_token: str
            ) -> list[Any]:
                with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                    return _fetch_tiktok_order_details(
                        client,
                        base_url=resolve_tiktok_shop_pull_base_url(),
                        app_key=(settings.tiktok_app_key or "").strip(),
                        app_secret=(settings.tiktok_app_secret or "").strip(),
                        access_token=current_access_token,
                        shop_id=sid,
                        shop_cipher=scipher,
                        order_ids=[order_id],
                    )

            try:
                details = _fetch_details_with_token(shop_id, shop_cipher, access_token)
            except httpx.HTTPStatusError as exc:
                if exc.response is None or exc.response.status_code != 401:
                    raise
                print(
                    structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.webhook.order_enrich_auth_retry",
                        success=False,
                        tiktok_order_id=order_id,
                        reason="http_401_refresh_and_retry",
                        error=str(exc),
                    )
                )
                refresh_result = _refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name=f"{runtime_name}_401_refresh",
                    force=True,
                )
                if refresh_result is None:
                    raise
                auth_row = ensure_tiktok_auth_row(session)
                shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
                if not access_token or (not shop_id and not shop_cipher):
                    raise
                details = _fetch_details_with_token(shop_id, shop_cipher, access_token)
            except RuntimeError as exc:
                if not _tiktok_webhook_enrich_error_is_retryable_auth(exc):
                    raise
                print(
                    structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.webhook.order_enrich_auth_retry",
                        success=False,
                        tiktok_order_id=order_id,
                        reason="tiktok_auth_error_refresh_and_retry",
                        error=str(exc),
                    )
                )
                refresh_result = _refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name=f"{runtime_name}_401_refresh",
                    force=True,
                )
                if refresh_result is None:
                    raise
                auth_row = ensure_tiktok_auth_row(session)
                shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
                if not access_token or (not shop_id and not shop_cipher):
                    raise
                details = _fetch_details_with_token(shop_id, shop_cipher, access_token)

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
        "parse_disagreement": _safe_json_load(row.parse_disagreement_json),
        "ai_resolver_reasoning": _safe_json_load(row.ai_resolver_reasoning_json),
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
        "parse_disagreement": _safe_json_load(row.parse_disagreement_json),
        "ai_resolver_reasoning": _safe_json_load(row.ai_resolver_reasoning_json),
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


def _get_app_setting(session: Session, key: str, default: str = "") -> str:
    row = session.get(AppSetting, key)
    return row.value if row and row.value else default


def _set_app_setting(session: Session, key: str, value: str) -> None:
    existing = session.get(AppSetting, key)
    if existing:
        existing.value = value
        session.add(existing)
    else:
        session.add(AppSetting(key=key, value=value))
    session.commit()


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
    except OperationalError as exc:
        print(
            structured_log_line(
                runtime="app",
                action="dashboard.shopify_today_query_failed",
                success=False,
                context="shared.build_dashboard_snapshot",
                error=str(exc)[:400],
            )
        )
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


# ---------------------------------------------------------------------------
# Stream Schedule helpers (used by both main.py and routers)
# ---------------------------------------------------------------------------

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
    return datetime.now(ZoneInfo("America/Los_Angeles"))


def get_current_streamer(session: Session, stream_account_id: Optional[int] = None) -> Optional[str]:
    """Return the streamer name for whoever is scheduled right now (Pacific time), or None.

    Handles overnight shifts (e.g. 18:00-06:00) by also checking yesterday's schedules
    that extend past midnight into today.
    """
    now = _now_pacific()
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")

    today_q = select(StreamSchedule).where(StreamSchedule.date == today)
    yesterday_q = select(StreamSchedule).where(
        StreamSchedule.date == yesterday, StreamSchedule.is_overnight == True
    )
    if stream_account_id is not None:
        today_q = today_q.where(StreamSchedule.stream_account_id == stream_account_id)
        yesterday_q = yesterday_q.where(StreamSchedule.stream_account_id == stream_account_id)

    for sched in session.exec(today_q).all():
        if sched.is_overnight:
            if current_time >= sched.start_time:
                streamer = session.get(Streamer, sched.streamer_id)
                if streamer and streamer.is_active:
                    return streamer.display_name or streamer.name
        else:
            if sched.start_time <= current_time <= sched.end_time:
                streamer = session.get(Streamer, sched.streamer_id)
                if streamer and streamer.is_active:
                    return streamer.display_name or streamer.name

    for sched in session.exec(yesterday_q).all():
        if current_time <= sched.end_time:
            streamer = session.get(Streamer, sched.streamer_id)
            if streamer and streamer.is_active:
                return streamer.display_name or streamer.name

    return None


def _get_default_streamer_for_tiktok(session: Session) -> Optional[str]:
    """Return the streamer on shift for the default (main) stream account."""
    default_acct = session.exec(
        select(StreamAccount).where(StreamAccount.is_default == True, StreamAccount.is_active == True)
    ).first()
    acct_id = default_acct.id if default_acct else None
    return get_current_streamer(session, stream_account_id=acct_id)
