import asyncio
import csv
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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, select

from .auth import authenticate_user, has_role, seed_default_users
from .bookkeeping import (
    refresh_bookkeeping_import_from_source,
    import_bookkeeping_file,
    get_bookkeeping_status_by_message_ids,
    list_bookkeeping_imports,
    list_detected_bookkeeping_posts,
    reconcile_bookkeeping_import,
)
from .backfill_requests import enqueue_backfill_request, list_recent_backfill_requests
from .channels import (
    get_available_channel_choices,
    get_channel_filter_choices,
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
)
from .db import engine, get_session, init_db, managed_session, recent_db_failure
from .discord_ingest import (
    discord_runtime_state,
    get_discord_client,
    list_available_discord_channels,
    parse_iso_datetime,
    run_discord_bot,
    seed_channels_from_env,
)
from .financials import compute_financials
from .models import AttachmentAsset, BookkeepingImport, DiscordMessage, ParseAttempt, User, WatchedChannel, utcnow
from .ops_log import list_operations_logs
from .reporting import build_financial_summary, get_financial_rows, parse_report_datetime
from .runtime_monitor import get_runtime_heartbeat_status, runtime_heartbeat_loop
from .schemas import HealthOut
from .transactions import build_transaction_summary, get_transactions, rebuild_transactions, sync_transaction_from_message
from .worker import STALE_PROCESSING_AFTER, parser_loop


settings = get_settings()


def count_rows(session: Session, stmt) -> int:
    count_stmt = select(func.count()).select_from(stmt.order_by(None).subquery())
    return int(session.exec(count_stmt).one())


def normalize_filesystem_path(path: Path) -> str:
    normalized = os.path.normpath(str(path))
    if normalized.startswith("\\\\?\\"):
        return normalized[4:]
    return normalized


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=normalize_filesystem_path(BASE_DIR / "templates"))
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")

PARSE_STATUS_OPTIONS = ["parsed", "needs_review", "failed", "queued", "ignored"]
DEAL_TYPE_OPTIONS = ["", "sell", "buy", "trade", "unknown"]
ENTRY_KIND_OPTIONS = ["", "sale", "buy", "trade", "expense", "unknown"]
PAYMENT_METHOD_OPTIONS = ["", "cash", "zelle", "venmo", "paypal", "card", "mixed", "trade", "unknown"]
CASH_DIRECTION_OPTIONS = ["", "to_store", "from_store", "none", "unknown"]
CATEGORY_OPTIONS = ["", "slabs", "singles", "sealed", "packs", "mixed", "accessories", "unknown"]
IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"]
NEARBY_IMAGE_AUDIT_WINDOW_SECONDS = 30
LOCAL_HEARTBEAT_RUNTIME_NAME = settings.runtime_name


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


def extract_image_urls(attachment_urls: list[str]) -> list[str]:
    return [
        url for url in attachment_urls
        if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
    ]


def get_cached_attachment_map(session: Session, message_ids: list[int]) -> dict[int, dict]:
    valid_ids = [message_id for message_id in message_ids if message_id is not None]
    if not valid_ids:
        return {}

    assets = session.exec(
        select(AttachmentAsset)
        .where(AttachmentAsset.message_id.in_(valid_ids))
        .order_by(AttachmentAsset.message_id.asc(), AttachmentAsset.id.asc())
    ).all()

    results: dict[int, dict] = {}
    for asset in assets:
        if asset.id is None:
            continue
        bucket = results.setdefault(
            asset.message_id,
            {"all_urls": [], "image_urls": []},
        )
        asset_url = f"/attachments/{asset.id}"
        bucket["all_urls"].append(asset_url)
        if asset.is_image:
            bucket["image_urls"].append(asset_url)

    return results


def local_runtime_details() -> dict:
    return {
        "discord_status": discord_runtime_state.get("status"),
        "discord_error": discord_runtime_state.get("error"),
        "parser_worker_enabled": settings.parser_worker_enabled,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
    }


def row_looks_transactional(row: DiscordMessage) -> bool:
    if row.amount is not None:
        return True
    if row.deal_type in {"sell", "buy", "trade"}:
        return True
    content = (row.content or "").lower()
    return any(token in content for token in ["sold", "sell", "bought", "buy", "cash", "zelle", "venmo", "paypal", "trade", "$"])


def find_nearby_image_candidates(session: Session, rows: list[DiscordMessage]) -> dict[int, dict]:
    targets = [
        row for row in rows
        if row.id is not None
        and not row.is_deleted
        and not row.stitched_group_id
        and row.channel_id
        and row.author_name
        and not extract_image_urls(json.loads(row.attachment_urls_json or "[]"))
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
            candidate_images = (
                cached_assets["image_urls"]
                if cached_assets and cached_assets["image_urls"]
                else extract_image_urls(json.loads(candidate.attachment_urls_json or "[]"))
            )
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
    after: Optional[str] = None,
    before: Optional[str] = None,
):
    after_dt = parse_report_datetime(after)
    before_dt = parse_report_datetime(before, end_of_day=True)
    stmt = select(DiscordMessage)

    if status:
        if status == "review_queue":
            stmt = stmt.where(DiscordMessage.parse_status.in_(["needs_review", "failed"]))
        else:
            stmt = stmt.where(DiscordMessage.parse_status == status)
    else:
        stmt = stmt.where(DiscordMessage.parse_status != "ignored")

    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)

    if entry_kind:
        stmt = stmt.where(DiscordMessage.entry_kind == entry_kind)

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
        "edited_at": format_pacific_datetime(row.edited_at),
        "is_deleted": row.is_deleted,
        "channel": row.channel_name,
        "channel_id": row.channel_id,
        "author": row.author_name,
        "message": row.content,
        "status": row.parse_status,
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
        "has_images": len(image_urls) > 0,
        "image_urls": image_urls,
        "first_image_url": image_urls[0] if image_urls else None,
        "parse_attempts": row.parse_attempts,
        "stitched_group_id": row.stitched_group_id,
        "stitched_primary": row.stitched_primary,
        "stitched_message_ids": stitched_ids,
        "stitched_count": len(stitched_ids),
    }


def build_message_list_items(session: Session, rows: list[DiscordMessage]) -> list[dict]:
    items = [message_list_item(row) for row in rows]
    cached_assets_by_message_id = get_cached_attachment_map(
        session,
        [row.id for row in rows if row.id is not None],
    )
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

    for item in items:
        grouped_messages = []
        for grouped_id in item["stitched_message_ids"]:
            grouped_row = grouped_rows_by_id.get(grouped_id)
            if not grouped_row:
                continue
            grouped_messages.append(
                {
                    "id": grouped_row.id,
                    "time": format_pacific_datetime(grouped_row.created_at),
                    "author": grouped_row.author_name or "",
                    "message": (grouped_row.content or "").strip(),
                    "is_self": grouped_row.id == item["id"],
                    "has_image": bool(json.loads(grouped_row.attachment_urls_json or "[]")),
                }
            )
        item["grouped_messages"] = grouped_messages
        cached_assets = cached_assets_by_message_id.get(item["id"])
        if cached_assets:
            item["attachment_urls"] = cached_assets["all_urls"]
            item["image_urls"] = cached_assets["image_urls"]
            item["first_image_url"] = cached_assets["image_urls"][0] if cached_assets["image_urls"] else None
            item["has_images"] = bool(cached_assets["image_urls"])
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
        "parse_status": row.parse_status,
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
        "entry_kind": row.entry_kind,
        "money_in": row.money_in,
        "money_out": row.money_out,
        "expense_category": row.expense_category,
    }

def build_return_url(
    return_path: str,
    *,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
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
    else:
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
    after: Optional[str] = None,
    before: Optional[str] = None,
    sort_by: str = "time",
    sort_dir: str = "desc",
) -> list[int]:
    rows, _ = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
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
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> dict:
    stmt = build_message_stmt(
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        after=after,
        before=before,
    )
    summary_subquery = stmt.order_by(None).subquery()

    status_counts = {
        row[0]: int(row[1])
        for row in session.exec(
            select(summary_subquery.c.parse_status, func.count())
            .group_by(summary_subquery.c.parse_status)
        ).all()
    }
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
        "parsed": status_counts.get("parsed", 0),
        "processing": status_counts.get("processing", 0),
        "queued": status_counts.get("queued", 0),
        "failed": status_counts.get("failed", 0),
        "needs_review": status_counts.get("needs_review", 0),
        "ignored": status_counts.get("ignored", 0),
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
        .where(DiscordMessage.parse_status == "parsed")
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

    background_tasks: list[asyncio.Task] = []

    if settings.discord_ingest_enabled or settings.parser_worker_enabled:
        heartbeat_thread = threading.Thread(
            target=runtime_heartbeat_loop,
            kwargs={
                "stop_event": heartbeat_stop_event,
                "runtime_name": LOCAL_HEARTBEAT_RUNTIME_NAME,
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
    payload = {
        "ok": False,
        "error": "Database connection is temporarily unavailable.",
        "detail": "The shared database did not accept the connection cleanly. Please retry in a few seconds.",
    }
    headers = {"Retry-After": "5"}
    wants_json = (
        request.url.path.startswith("/admin/parser-progress")
        or request.url.path.startswith("/health")
        or "application/json" in request.headers.get("accept", "")
    )
    if wants_json:
        return JSONResponse(status_code=503, content=payload, headers=headers)
    return HTMLResponse(
        "<h1>Database temporarily unavailable</h1>"
        "<p>The shared database connection dropped unexpectedly. Please retry in a few seconds.</p>",
        status_code=503,
        headers=headers,
    )


@app.get("/attachments/{asset_id}")
def attachment_asset(asset_id: int, session: Session = Depends(get_session)):
    asset = session.get(AttachmentAsset, asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Attachment not found")

    media_type = asset.content_type or "application/octet-stream"
    headers = {}
    if asset.filename:
        headers["Content-Disposition"] = f'inline; filename="{asset.filename}"'
    return Response(content=asset.data, media_type=media_type, headers=headers)


PUBLIC_PATH_PREFIXES = (
    "/static",
    "/health",
    "/login",
)


def user_role_for_path(path: str) -> Optional[str]:
    if path.startswith("/table") or path.startswith("/review-table") or path.startswith("/bookkeeping") or path.startswith("/admin"):
        return "admin"
    if path.startswith("/api/review"):
        return "reviewer"
    if path.startswith("/review") or path.startswith("/messages") or path.startswith("/channels"):
        return "reviewer"
    if path.startswith("/reports"):
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
            local_runtime = get_runtime_heartbeat_status(
                session,
                LOCAL_HEARTBEAT_RUNTIME_NAME,
                runtime_label=settings.runtime_label,
                updated_at_formatter=format_pacific_datetime,
            )
        return HealthOut(
            ok=True,
            db_ok=True,
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
    parser_progress = get_parser_progress(session)
    recent_reviewed = build_message_list_items(
        session,
        session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.reviewed_at != None)  # noqa: E711
            .order_by(DiscordMessage.reviewed_at.desc())
            .limit(8)
        ).all(),
    )

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
            "parser_progress": parser_progress,
        },
    )


@app.get("/status", response_class=HTMLResponse)
def status_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    return templates.TemplateResponse(
        request,
        "status.html",
        {
            "request": request,
            "title": "System Status",
            "current_user": getattr(request.state, "current_user", None),
            "snapshot": build_status_snapshot(session),
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
        if row.channel_id in watched_channel_ids and row.parse_status == "parsed" and not row.is_deleted
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

    watched_channel_ids = {
        row.channel_id
        for row in get_watched_channels(session)
        if row.is_enabled
    }
    row = session.get(DiscordMessage, message_id)
    if not row or row.channel_id not in watched_channel_ids or row.parse_status != "parsed":
        raise HTTPException(status_code=404, detail="Deal not found")

    item = build_message_list_items(session, [row])[0]
    attachment_urls = list(item.get("attachment_urls") or [])
    image_urls = list(item.get("image_urls") or [])
    if row.stitched_group_id:
        grouped_rows = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.stitched_group_id == row.stitched_group_id)
            .order_by(DiscordMessage.created_at.asc(), DiscordMessage.id.asc())
        ).all()
        cached_assets_by_message_id = get_cached_attachment_map(
            session,
            [grouped_row.id for grouped_row in grouped_rows if grouped_row.id is not None],
        )
        for grouped_row in grouped_rows:
            cached_assets = cached_assets_by_message_id.get(grouped_row.id)
            grouped_attachment_urls = (
                cached_assets["all_urls"]
                if cached_assets
                else json.loads(grouped_row.attachment_urls_json or "[]")
            )
            grouped_image_urls = (
                cached_assets["image_urls"]
                if cached_assets
                else extract_image_urls(grouped_attachment_urls)
            )
            for url in grouped_attachment_urls:
                if url not in attachment_urls:
                    attachment_urls.append(url)
            for url in grouped_image_urls:
                if url not in image_urls:
                    image_urls.append(url)
    item["attachment_urls"] = attachment_urls
    item["image_urls"] = image_urls
    item["first_image_url"] = image_urls[0] if image_urls else None
    item["trade_summary"] = row.trade_summary
    item["notes"] = row.notes
    item["image_summary"] = row.image_summary
    item["reviewed_by"] = row.reviewed_by
    item["reviewed_at"] = format_pacific_datetime(row.reviewed_at)
    back_url = build_return_url(
        "/deals",
        channel_id=channel_id,
        after=after,
        before=before,
        page=page,
        limit=limit,
    )
    if entry_kind:
        separator = "&" if "?" in back_url else "?"
        back_url = f"{back_url}{separator}entry_kind={entry_kind}"

    return templates.TemplateResponse(
        request,
        "deal_detail.html",
        {
            "request": request,
            "title": f"Deal {message_id}",
            "deal": item,
            "back_url": back_url,
            "current_user": getattr(request.state, "current_user", None),
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
        "+The+worker+will+run+it+when+Discord+is+ready+and+will+requeue+it+if+a+deploy+restart+interrupts+the+run."
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
            "created_at": format_pacific_datetime(row.created_at),
            "started_at": format_pacific_datetime(row.started_at) if row.started_at else "",
            "finished_at": format_pacific_datetime(row.finished_at) if row.finished_at else "",
            "error_message": row.error_message or "",
        }
        for row in rows
    ]


def serialize_operations_logs(rows: list) -> list[dict]:
    return [
        {
            "id": row.id,
            "event_type": row.event_type,
            "level": row.level,
            "source": row.source,
            "message": row.message,
            "created_at": format_pacific_datetime(row.created_at),
        }
        for row in rows
    ]


def recompute_financial_fields(session: Session) -> int:
    rows = session.exec(
        select(DiscordMessage).where(
            DiscordMessage.parse_status.in_(["parsed", "needs_review"])
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
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> dict:
    stmt = build_message_stmt(
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        after=after,
        before=before,
    )
    summary_subquery = stmt.order_by(None).subquery()

    status_counts = {
        row[0]: int(row[1])
        for row in session.exec(
            select(summary_subquery.c.parse_status, func.count())
            .group_by(summary_subquery.c.parse_status)
        ).all()
    }
    total = sum(status_counts.values())
    parsed = status_counts.get("parsed", 0)
    processing = status_counts.get("processing", 0)
    queued = status_counts.get("queued", 0)
    failed = status_counts.get("failed", 0)
    needs_review = status_counts.get("needs_review", 0)
    ignored = status_counts.get("ignored", 0)
    completed = parsed + needs_review + failed + ignored
    pending = queued + processing
    percent_complete = round((completed / total) * 100, 1) if total else 100.0
    processing_ids = [
        row_id
        for row_id in session.exec(
            select(summary_subquery.c.id).where(summary_subquery.c.parse_status == "processing")
        ).all()
        if row_id is not None
    ]
    processing_with_images = int(
        session.exec(
            select(func.count())
            .select_from(summary_subquery)
            .where(
                summary_subquery.c.parse_status == "processing",
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
            LOCAL_HEARTBEAT_RUNTIME_NAME,
            runtime_label=settings.runtime_label,
            updated_at_formatter=format_pacific_datetime,
        ),
    }


def build_status_snapshot(session: Session) -> dict:
    local_runtime = get_runtime_heartbeat_status(
        session,
        LOCAL_HEARTBEAT_RUNTIME_NAME,
        runtime_label=settings.runtime_label,
        updated_at_formatter=format_pacific_datetime,
    )
    parser_progress = get_parser_progress(session)
    latest_ingested_at = session.exec(
        select(DiscordMessage.ingested_at)
        .order_by(DiscordMessage.ingested_at.desc())
        .limit(1)
    ).first()
    latest_reviewed_at = session.exec(
        select(DiscordMessage.reviewed_at)
        .where(DiscordMessage.reviewed_at != None)  # noqa: E711
        .order_by(DiscordMessage.reviewed_at.desc())
        .limit(1)
    ).first()
    latest_parse_finished_at = session.exec(
        select(ParseAttempt.finished_at)
        .where(ParseAttempt.finished_at != None)  # noqa: E711
        .order_by(ParseAttempt.finished_at.desc())
        .limit(1)
    ).first()

    return {
        "db_ok": True,
        "local_runtime": local_runtime,
        "parser_progress": parser_progress,
        "queue_backlog": parser_progress["queued"] + parser_progress["processing"],
        "queue_is_moving": parser_progress["processing"] > 0 or parser_progress["queued"] == 0,
        "recent_activity": {
            "latest_ingested_label": format_pacific_datetime(latest_ingested_at),
            "latest_reviewed_label": format_pacific_datetime(latest_reviewed_at),
            "latest_parse_finished_label": format_pacific_datetime(latest_parse_finished_at),
        },
        "runtime_flags": {
            "discord_ingest_enabled": settings.discord_ingest_enabled,
            "parser_worker_enabled": settings.parser_worker_enabled,
        },
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
        .where(DiscordMessage.parse_status.in_(["parsed", "needs_review"]))
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
            "net_display": format_dashboard_money(today_totals.get("net", 0.0)),
            "trade_in_display": format_dashboard_money(today_totals.get("trade_cash_in", 0.0)),
            "trade_out_display": format_dashboard_money(today_totals.get("trade_cash_out", 0.0)),
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
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    return message_detail_item(row)


@app.get("/admin/parser-progress")
def admin_parser_progress(
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    return get_parser_progress(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        after=after,
        before=before,
    )


@app.get("/table/messages/{message_id}", response_class=HTMLResponse)
def message_detail_page(
    message_id: int,
    request: Request,
    return_path: str = Query(default="/table"),
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
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
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    item = message_detail_item(row)
    cached_assets = get_cached_attachment_map(
        session,
        [row.id] if row.id is not None else [],
    ).get(row.id)
    if cached_assets:
        item["attachment_urls"] = cached_assets["all_urls"]
        item["image_urls"] = cached_assets["image_urls"]
    item["time"] = format_pacific_datetime(row.created_at)
    item["edited_at"] = format_pacific_datetime(row.edited_at)
    item["is_deleted"] = row.is_deleted
    item["nearby_image"] = find_nearby_image_candidates(session, [row]).get(row.id)
    item["possible_missing_image"] = item["nearby_image"] is not None
    learning_signal = get_learning_signal(session, row.content or "")

    return templates.TemplateResponse(
        request,
        "message_detail.html",
        {
            "request": request,
            "title": f"Message {message_id}",
            "message": item,
            "success": success,
            "error": error,
            "return_path": return_path,
            "back_url": build_return_url(
                return_path,
                status=status,
                channel_id=channel_id,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ),
            "selected_status": status or "",
            "selected_channel_id": channel_id or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by or "",
            "selected_sort_dir": sort_dir or "",
            "selected_page": page or 1,
            "selected_limit": limit or 100,
            "parse_status_options": PARSE_STATUS_OPTIONS,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "correction_patterns": get_correction_pattern_counts(),
            "learning_signal": learning_signal,
        },
    )


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
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    parse_status: str = Form(default="parsed"),
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

    try:
        parsed_amount = parse_optional_float(amount)
        parsed_confidence = parse_optional_float(confidence)
    except ValueError:
        detail_url = build_return_url(
            f"/table/messages/{message_id}",
            status=status,
            channel_id=channel_id,
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

    row.parse_status = parse_status or "parsed"
    row.needs_review = bool(needs_review)
    if row.parse_status == "needs_review" or row.needs_review:
        row.parse_status = "needs_review"
        row.needs_review = True
    elif row.parse_status == "parsed":
        row.needs_review = False

    normalized_deal_type = (deal_type or "").strip() or None
    normalized_payment_method = (payment_method or "").strip() or None
    normalized_cash_direction = (cash_direction or "").strip() or None
    normalized_category = (category or "").strip() or None
    normalized_entry_kind = (entry_kind or "").strip() or None
    normalized_expense_category = (expense_category or "").strip() or None

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
        row.parse_status = "parsed"
        row.needs_review = False
    if row.parse_status == "parsed" and not row.needs_review:
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
    elif row.parse_status != "parsed" or row.needs_review:
        row.reviewed_by = None
        row.reviewed_at = None
    row.last_error = None if row.parse_status in {"parsed", "needs_review"} else row.last_error

    session.add(row)
    save_review_correction(session, row)
    sync_transaction_from_message(session, row)
    session.commit()

    redirect_target = build_return_url(
        f"/table/messages/{message_id}" if stay_on_detail else return_path,
        status=status,
        channel_id=channel_id,
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
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=1,
        limit=50000,
    )
    items = build_message_list_items(session, rows)
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
    summary = build_transaction_summary(transactions)
    channels = get_channel_filter_choices(session)

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
            "summary": summary,
            "expense_chart": build_bar_chart_rows(summary["expense_categories"]),
            "channel_chart": build_bar_chart_rows(summary["channel_net"]),
            "transactions": transactions,
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
    if import_id:
        selected_import = session.get(BookkeepingImport, import_id)
        if selected_import:
            reconciliation = reconcile_bookkeeping_import(session, import_id)

    return templates.TemplateResponse(
        request,
        "bookkeeping.html",
        {
            "request": request,
            "title": "Bookkeeping Reconciliation",
            "imports": imports,
            "selected_import": selected_import,
            "reconciliation": reconciliation,
            "detected_posts": list_detected_bookkeeping_posts(session),
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

    row.parse_status = "queued"
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
    row.parse_status = "parsed"
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
        row.parse_status = "parsed"
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
        session.add(row)
        sync_transaction_from_message(session, row)
        session.commit()

    redirect_url = f"{return_path}?success=Approved+message+{message_id}&limit={limit}"
    if status:
        redirect_url += f"&status={status}"
    if channel_id:
        redirect_url += f"&channel_id={channel_id}"
    if after:
        redirect_url += f"&after={after}"
    if before:
        redirect_url += f"&before={before}"
    if sort_by:
        redirect_url += f"&sort_by={sort_by}"
    if sort_dir:
        redirect_url += f"&sort_dir={sort_dir}"
    if page > 1:
        redirect_url += f"&page={page}"

    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/messages/bulk/approve-form")
def bulk_approve_messages_form(
    request: Request,
    message_ids: list[int] = Form(default=[]),
    return_path: str = Form(default="/review-table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
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
        row.parse_status = "parsed"
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
        session.add(row)
        sync_transaction_from_message(session, row)
        updated += 1
    session.commit()

    redirect_url = f"{return_path}?success=Approved+{updated}+messages&limit={limit}"
    if status:
        redirect_url += f"&status={status}"
    if channel_id:
        redirect_url += f"&channel_id={channel_id}"
    if after:
        redirect_url += f"&after={after}"
    if before:
        redirect_url += f"&before={before}"
    if sort_by:
        redirect_url += f"&sort_by={sort_by}"
    if sort_dir:
        redirect_url += f"&sort_dir={sort_dir}"
    if page > 1:
        redirect_url += f"&page={page}"

    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/messages/bulk/retry-form")
def bulk_retry_messages_form(
    request: Request,
    message_ids: list[int] = Form(default=[]),
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
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
    updated = 0
    for message_id in message_ids:
        row = session.get(DiscordMessage, message_id)
        if not row:
            continue
        row.parse_status = "queued"
        row.last_error = None
        row.reviewed_by = None
        row.reviewed_at = None
        session.add(row)
        sync_transaction_from_message(session, row)
        updated += 1
    session.commit()

    redirect_url = f"{return_path}?success=Re-queued+{updated}+messages&limit={limit}"
    if status:
        redirect_url += f"&status={status}"
    if channel_id:
        redirect_url += f"&channel_id={channel_id}"
    if after:
        redirect_url += f"&after={after}"
    if before:
        redirect_url += f"&before={before}"
    if sort_by:
        redirect_url += f"&sort_by={sort_by}"
    if sort_dir:
        redirect_url += f"&sort_dir={sort_dir}"
    if page > 1:
        redirect_url += f"&page={page}"

    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/messages/{message_id}/retry-form")
def retry_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
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
    if row:
        row.parse_status = "queued"
        row.last_error = None
        row.reviewed_by = None
        row.reviewed_at = None
        session.add(row)
        sync_transaction_from_message(session, row)
        session.commit()

    redirect_url = f"{return_path}?success=Re-queued+message+{message_id}&limit={limit}"
    if status:
        redirect_url += f"&status={status}"
    if channel_id:
        redirect_url += f"&channel_id={channel_id}"
    if after:
        redirect_url += f"&after={after}"
    if before:
        redirect_url += f"&before={before}"
    if sort_by:
        redirect_url += f"&sort_by={sort_by}"
    if sort_dir:
        redirect_url += f"&sort_dir={sort_dir}"
    if page > 1:
        redirect_url += f"&page={page}"

    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/messages/{message_id}/mark-incorrect-form")
def mark_incorrect_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
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
    row.parse_status = "needs_review"
    row.last_error = "Manually marked incorrect for review."
    row.reviewed_by = None
    row.reviewed_at = None
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    detail_url = build_return_url(
        f"/table/messages/{message_id}",
        status="review_queue" if return_path == "/review-table" else status,
        channel_id=channel_id,
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
    client = get_discord_client()
    if client is None or not client.is_ready():
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
        return {"ok": True, "queued": True, "message": queued_message.replace("+", " ")}

    if channel_id:
        return await client.backfill_channel(
            channel_id=int(channel_id),
            limit=limit,
            oldest_first=oldest_first,
            after=after_dt,
            before=before_dt,
        )

    return await client.backfill_enabled_channels(
        limit_per_channel=limit,
        oldest_first=oldest_first,
        after=after_dt,
        before=before_dt,
    )


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
        client = get_discord_client()
        if client is None or not client.is_ready():
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
            return RedirectResponse(url=f"/table?success={queued_message}", status_code=303)
        if channel_id:
            result = await client.backfill_channel(
                channel_id=int(channel_id),
                limit=limit,
                oldest_first=oldest_first,
                after=after_dt,
                before=before_dt,
            )
            if result.get("ok"):
                persist_backfill_window_for_targets(
                    session,
                    channel_ids=target_channel_ids,
                    after_dt=after_dt,
                    before_dt=before_dt,
                )
                channel_name = result.get("channel_name") or result.get("channel_id")
                msg = f"Backfill complete for {channel_name}: inserted={result.get('inserted', 0)}, skipped={result.get('skipped', 0)}"
                return RedirectResponse(url=f"/table?success={msg}", status_code=303)

            return RedirectResponse(url=f"/table?error={result.get('error', 'Backfill failed')}", status_code=303)

        result = await client.backfill_enabled_channels(
            limit_per_channel=limit,
            oldest_first=oldest_first,
            after=after_dt,
            before=before_dt,
        )
        if not result.get("ok"):
            return RedirectResponse(url="/table?error=Backfill+failed+for+one+or+more+channels", status_code=303)
        persist_backfill_window_for_targets(
            session,
            channel_ids=target_channel_ids,
            after_dt=after_dt,
            before_dt=before_dt,
        )
        msg = f"Backfill complete: inserted={result.get('total_inserted', 0)}, skipped={result.get('total_skipped', 0)}"
        return RedirectResponse(url=f"/table?success={msg}", status_code=303)

    except Exception as e:
        return RedirectResponse(url=f"/table?error={str(e)}", status_code=303)


@app.get("/table", response_class=HTMLResponse)
def messages_table(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
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
        status=status,
        channel_id=channel_id,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows)
    channels = get_channel_filter_choices(session)
    summary = get_summary(
        session,
        status=status,
        channel_id=channel_id,
        after=after,
        before=before,
    )
    watched_channels = get_watched_channels(session)
    available_discord_channels, has_live_available_discord_channels = get_available_channel_choices(session)
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    financial_rows = get_financial_rows(
        session,
        start=parse_report_datetime(after),
        end=parse_report_datetime(before, end_of_day=True),
        channel_id=channel_id,
    )
    financial_summary = build_financial_summary(financial_rows)
    recent_backfill_requests = serialize_backfill_requests(list_recent_backfill_requests(session))
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    parser_progress = get_parser_progress(
        session,
        status=status,
        channel_id=channel_id,
        after=after,
        before=before,
    )

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
            "selected_channel_id": channel_id or "",
            "selected_status": status or "",
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
        },
    )


@app.get("/review-table", response_class=HTMLResponse)
def review_table(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
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
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows)
    channels = get_channel_filter_choices(session)
    summary = get_summary(
        session,
        status="review_queue",
        channel_id=channel_id,
        after=after,
        before=before,
    )
    financial_summary = build_financial_summary(get_financial_rows(session))
    watched_channels = get_watched_channels(session)
    available_discord_channels, has_live_available_discord_channels = get_available_channel_choices(session)
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    recent_backfill_requests = serialize_backfill_requests(list_recent_backfill_requests(session))
    parser_progress = get_parser_progress(
        session,
        status="review_queue",
        channel_id=channel_id,
        after=after,
        before=before,
    )
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)

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
            "selected_channel_id": channel_id or "",
            "selected_status": "review_queue",
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
        },
    )


@app.get("/review", response_class=HTMLResponse)
def reviewer_queue_page(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
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
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows)
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    channels = get_channel_filter_choices(session)
    summary = get_summary(
        session,
        status="review_queue",
        channel_id=channel_id,
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
            "summary": summary,
            "pagination": pagination,
            "selected_channel_id": channel_id or "",
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
    message_id: int,
    request: Request,
    channel_id: Optional[str] = Query(default=None),
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

    item = message_list_item(row)
    item["attachment_urls"] = json.loads(row.attachment_urls_json or "[]")
    item["grouped_messages"] = build_message_list_items(session, [row])[0]["grouped_messages"]
    ordered_ids = get_ordered_message_ids(
        session,
        status="review_queue",
        channel_id=channel_id,
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
