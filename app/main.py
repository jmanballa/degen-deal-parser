import asyncio
import csv
import hashlib
import json
import os
import socket
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.exc import OperationalError
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, select

from .auth import authenticate_user, has_role, seed_default_users
from .attachment_storage import attachment_cache_path, write_attachment_cache_file
from .bookkeeping import (
    refresh_bookkeeping_import_from_source,
    import_bookkeeping_file,
    get_bookkeeping_status_by_message_ids,
    list_bookkeeping_imports,
    list_detected_bookkeeping_posts,
    reconcile_bookkeeping_import,
)
from .backfill_requests import (
    backfill_request_loop,
    cancel_backfill_request,
    enqueue_backfill_request,
    list_recent_backfill_requests,
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
    AttachmentAsset,
    BackfillRequest,
    BookkeepingImport,
    DiscordMessage,
    expand_parse_status_filter_values,
    OperationsLog,
    ParseAttempt,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    ReparseRun,
    ShopifyOrder,
    User,
    WatchedChannel,
    normalize_parse_status,
    utcnow,
)
from .ops_log import list_operations_logs, list_operations_logs_for_backfill_request, parse_operations_log_details
from .reparse_runs import list_recent_reparse_runs, safe_create_reparse_run, safe_finalize_reparse_run_queue
from .reparse import reparse_message_row, reparse_message_rows
from .reporting import (
    build_financial_summary,
    build_reporting_periods,
    build_shopify_reporting_summary,
    get_financial_rows,
    get_shopify_reporting_rows,
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
from .worker import (
    STALE_PROCESSING_AFTER,
    clear_parsed_fields,
    parser_loop,
    periodic_stitch_audit_loop,
    queue_auto_reprocess_candidates,
    queue_reparse_range,
)


settings = get_settings()
setup_runtime_file_logging("app.log")

REPORT_SOURCE_ALL = "all"
REPORT_SOURCE_DISCORD = "discord"
REPORT_SOURCE_SHOPIFY = "shopify"
REPORT_SOURCE_OPTIONS = {REPORT_SOURCE_ALL, REPORT_SOURCE_DISCORD, REPORT_SOURCE_SHOPIFY}


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


def normalize_filesystem_path(path: Path) -> str:
    normalized = os.path.normpath(str(path))
    if normalized.startswith("\\\\?\\"):
        return normalized[4:]
    return normalized


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=normalize_filesystem_path(BASE_DIR / "templates"))
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")

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
        discord_net = round(float(discord_summary["totals"].get("net", 0.0) or 0.0), 2)
        shopify_gross = round(float(shopify_summary["gross_revenue"] or 0.0), 2)
        shopify_tax = round(float(shopify_summary["total_tax"] or 0.0), 2)
        shopify_net = round(float(shopify_summary["net_revenue"] or 0.0), 2)
        rows.append(
            {
                "key": period.get("key") or "",
                "label": period.get("label") or "Period",
                "discord_net": discord_net,
                "shopify_gross": shopify_gross,
                "shopify_tax": shopify_tax,
                "shopify_net": shopify_net,
                "combined_net": round(discord_net + shopify_net, 2),
                "shopify_tax_unknown_orders": int(shopify_summary["tax_unknown_orders"] or 0),
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


def local_runtime_details() -> dict:
    return {
        "discord_status": discord_runtime_state.get("status"),
        "discord_error": discord_runtime_state.get("error"),
        "parser_worker_enabled": settings.parser_worker_enabled,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
        "periodic_attachment_repair_enabled": settings.periodic_attachment_repair_enabled,
        "periodic_attachment_repair_interval_minutes": settings.periodic_attachment_repair_interval_minutes,
        "periodic_attachment_repair_lookback_hours": settings.periodic_attachment_repair_lookback_hours,
        "periodic_attachment_repair_limit": settings.periodic_attachment_repair_limit,
        "periodic_attachment_repair_min_age_minutes": settings.periodic_attachment_repair_min_age_minutes,
        "periodic_stitch_audit_enabled": settings.periodic_stitch_audit_enabled,
        "periodic_stitch_audit_interval_minutes": settings.periodic_stitch_audit_interval_minutes,
        "backfill_queue_expected": settings.discord_ingest_enabled,
        "last_recent_audit_at": discord_runtime_state.get("last_recent_audit_at"),
        "last_recent_audit_summary": discord_runtime_state.get("last_recent_audit_summary"),
        "last_attachment_repair_at": discord_runtime_state.get("last_attachment_repair_at"),
        "last_attachment_repair_summary": discord_runtime_state.get("last_attachment_repair_summary"),
    }


def app_runtime_details() -> dict:
    return {
        "service_mode": "web-app",
        "parser_worker_enabled": settings.parser_worker_enabled,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
        "periodic_stitch_audit_enabled": settings.periodic_stitch_audit_enabled,
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
) -> dict:
    message_id = item.get("id")
    if message_id is None:
        return item

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
        return item

    item["attachment_urls"] = base_attachment_urls
    item["image_urls"] = base_image_urls
    item["first_image_url"] = base_image_urls[0] if base_image_urls else None
    item["has_images"] = bool(base_image_urls)
    return item


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    with managed_session() as session:
        seed_default_users(session)
    seed_channels_from_env()

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

    discord_task = asyncio.create_task(run_discord_bot(stop_event), name="discord-ingest")
    background_tasks.append(discord_task)
    app.state.discord_task = discord_task

    if settings.discord_ingest_enabled:
        backfill_task = asyncio.create_task(
            backfill_request_loop(stop_event, get_discord_client),
            name="backfill-queue",
        )
        background_tasks.append(backfill_task)
        app.state.backfill_task = backfill_task
        recent_audit_task = asyncio.create_task(
            recent_message_audit_loop(stop_event, get_discord_client),
            name="recent-message-audit",
        )
        background_tasks.append(recent_audit_task)
        app.state.recent_audit_task = recent_audit_task
        if settings.periodic_attachment_repair_enabled:
            attachment_repair_task = asyncio.create_task(
                periodic_attachment_repair_loop(stop_event, get_discord_client),
                name="attachment-repair-audit",
            )
            background_tasks.append(attachment_repair_task)
            app.state.attachment_repair_task = attachment_repair_task
        else:
            app.state.attachment_repair_task = None
    else:
        app.state.backfill_task = None
        app.state.recent_audit_task = None
        app.state.attachment_repair_task = None

    if settings.discord_ingest_enabled and settings.parser_worker_enabled:
        stitch_audit_task = asyncio.create_task(
            periodic_stitch_audit_loop(stop_event),
            name="stitch-audit",
        )
        background_tasks.append(stitch_audit_task)
        app.state.stitch_audit_task = stitch_audit_task
    else:
        app.state.stitch_audit_task = None

    if settings.parser_worker_enabled:
        worker_task = asyncio.create_task(parser_loop(stop_event), name="parser-worker")
        background_tasks.append(worker_task)
        app.state.worker_task = worker_task
    else:
        app.state.worker_task = None
        print("[worker] parser worker disabled by configuration")

    yield

    stop_event.set()
    heartbeat_stop_event.set()

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
    domain=settings.session_domain or None,
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
            "<h1>SQLite temporarily busy</h1>"
            "<p>The local database is handling another write right now. Please retry in a few seconds.</p>"
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
def attachment_asset(asset_id: int, session: Session = Depends(get_session)):
    asset_meta = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.filename, AttachmentAsset.content_type)
        .where(AttachmentAsset.id == asset_id)
    ).first()
    if not asset_meta:
        raise HTTPException(status_code=404, detail="Attachment not found")

    _, filename, content_type = asset_meta
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
    }
    if filename:
        headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return FileResponse(path=file_path, media_type=media_type, headers=headers)


@app.get("/messages/{message_id}/attachments/{attachment_index}")
async def message_attachment_fallback(
    message_id: int,
    attachment_index: int,
    session: Session = Depends(get_session),
):
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
)


def user_role_for_path(path: str) -> Optional[str]:
    if path.startswith("/table") or path.startswith("/review-table") or path.startswith("/bookkeeping") or path.startswith("/admin"):
        return "admin"
    if path.startswith("/api/review"):
        return "reviewer"
    if path.startswith("/review") or path.startswith("/messages") or path.startswith("/channels"):
        return "reviewer"
    if path.startswith("/reports") or path.startswith("/shopify-orders") or path.startswith("/shopify/orders"):
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
        return "/admin"
    if role == "reviewer":
        return "/review"
    return "/dashboard"


def require_role_response(request: Request, minimum_role: str) -> Optional[Response]:
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
            "total": 0.0,
            "total_display": format_dashboard_money(0.0),
            "discord_total_display": format_dashboard_money(0.0),
            "discord_sales_display": format_dashboard_money(0.0),
            "discord_trade_in_display": format_dashboard_money(0.0),
            "shopify_total_display": format_dashboard_money(0.0),
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
            "parser_progress": parser_progress,
        },
    )


@app.get("/partner", response_class=HTMLResponse)
def partner_page(
    request: Request,
    session: Session = Depends(get_session),
):
    return RedirectResponse(url="/dashboard", status_code=301)


@app.get("/status", response_class=HTMLResponse)
def status_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    status_snapshot = build_status_snapshot(session)
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
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    return templates.TemplateResponse(
        request,
        "ops_log.html",
        {
            "request": request,
            "title": "Operations Log",
            "current_user": getattr(request.state, "current_user", None),
            "logs": serialize_operations_logs(list_operations_logs(session)),
            "snapshot": build_status_snapshot(session),
        },
    )


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


@app.get("/admin/debug", response_class=HTMLResponse)
def admin_debug_page(
    request: Request,
    session: Session = Depends(get_session),
):
    return RedirectResponse(url="/status", status_code=301)


@app.get("/admin/health", response_class=HTMLResponse)
def admin_health_page(
    request: Request,
    session: Session = Depends(get_session),
):
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
    redirect_target = next or app_home_for_role(user.role)
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
    rows = session.exec(select(BackfillRequest)).all()
    queued = sum(1 for row in rows if row.status == "queued")
    processing = sum(1 for row in rows if row.status == "processing")
    completed = sum(1 for row in rows if row.status == "completed")
    failed = sum(1 for row in rows if row.status == "failed")
    cancelled = sum(1 for row in rows if row.status == "cancelled")
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
    for raw_status in session.exec(select(DiscordMessage.parse_status)).all():
        normalized = normalize_parse_status(raw_status)
        if normalized in queue_counts:
            queue_counts[normalized] += 1

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
def list_channels(session: Session = Depends(get_session)):
    return get_channel_filter_choices(session)


@app.get("/messages")
def list_messages(
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
def get_message(message_id: int, session: Session = Depends(get_session)):
    row = session.get(DiscordMessage, message_id)
    target = f"/deals/{message_id}" if row else "/deals"
    return RedirectResponse(url=target, status_code=301)


@app.get("/admin/parser-progress")
def admin_parser_progress(
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
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
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    session: Session = Depends(get_session),
):
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
    normalized_text: str = Form(...),
    return_to: str = Form(default="/table"),
    session: Session = Depends(get_session),
):
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
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
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
def shopify_orders_redirect():
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
    summary_rows = get_shopify_order_rows(
        session,
        start=start_dt,
        end=end_dt,
        financial_status=financial_status,
        source=source,
        search=search,
    )
    summary = build_shopify_order_summary(summary_rows)
    status_rows = session.exec(select(ShopifyOrder.financial_status)).all()
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
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    if entry_kind:
        rows = [row for row in rows if row.entry_kind == entry_kind]
    return build_message_list_items(session, rows)


@app.get("/reports/export.csv")
def report_transactions_csv(
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
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
    transactions = get_transactions(
        session,
        start=start_dt,
        end=end_dt,
        channel_id=channel_id,
        entry_kind=entry_kind,
    )
    discord_summary = build_transaction_summary(transactions)
    shopify_rows = get_shopify_reporting_rows(session, start=start_dt, end=end_dt)
    shopify_summary = build_shopify_reporting_summary(shopify_rows)
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
    summary = discord_summary
    channels = get_channel_filter_choices(session)
    period_rows = build_report_period_comparison_rows(
        session,
        periods=build_reporting_periods(selected_start=start_dt, selected_end=end_dt),
        channel_id=channel_id,
        entry_kind=entry_kind,
    )
    report_totals = {
        "discord_net": round(float(discord_summary["totals"].get("net", 0.0) or 0.0), 2),
        "shopify_gross": round(float(shopify_summary["gross_revenue"] or 0.0), 2),
        "shopify_tax": round(float(shopify_summary["total_tax"] or 0.0), 2),
        "shopify_net": round(float(shopify_summary["net_revenue"] or 0.0), 2),
        "combined_net": round(
            float(discord_summary["totals"].get("net", 0.0) or 0.0)
            + float(shopify_summary["net_revenue"] or 0.0),
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
            "report_totals": report_totals,
            "period_rows": period_rows,
            "show_discord_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_DISCORD},
            "show_shopify_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_SHOPIFY},
            "reports_url": build_reports_url,
            "expense_chart": build_bar_chart_rows(summary["expense_categories"]),
            "channel_chart": build_bar_chart_rows(summary["channel_net"]),
            "transactions": transactions,
            "shopify_daily_totals": shopify_daily_totals,
        },
    )


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
def retry_message(message_id: int, session: Session = Depends(get_session)):
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
def approve_message(message_id: int, session: Session = Depends(get_session)):
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
def clear_all_messages():
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
def clear_all_messages_form():
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
def admin_recompute_financials(session: Session = Depends(get_session)):
    updated = recompute_financial_fields(session)
    return {"ok": True, "updated": updated}


@app.post("/admin/recompute-financials/form")
def admin_recompute_financials_form(session: Session = Depends(get_session)):
    updated = recompute_financial_fields(session)
    return RedirectResponse(
        url=f"/table?success=Recomputed+financial+fields+for+{updated}+messages",
        status_code=303,
    )


@app.post("/admin/rebuild-transactions")
def admin_rebuild_transactions(session: Session = Depends(get_session)):
    rebuilt = rebuild_transactions(session)
    return {"ok": True, "rebuilt": rebuilt}


@app.post("/admin/rebuild-transactions/form")
def admin_rebuild_transactions_form(session: Session = Depends(get_session)):
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
def clear_channel_messages(channel_id: str):
    with managed_session() as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)

        for row in rows:
            attempts = session.exec(
                select(ParseAttempt).where(ParseAttempt.message_id == row.id)
            ).all()
            for attempt in attempts:
                session.delete(attempt)

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
    channel_id: str = Form(...),
):
    with managed_session() as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)
        channel_name = rows[0].channel_name if rows else channel_id

        for row in rows:
            attempts = session.exec(
                select(ParseAttempt).where(ParseAttempt.message_id == row.id)
            ).all()
            for attempt in attempts:
                session.delete(attempt)

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
def admin_list_channels(session: Session = Depends(get_session)):
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
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
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
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
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
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
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
def admin_list_discord_channels():
    channels = list_available_discord_channels()
    return channels
