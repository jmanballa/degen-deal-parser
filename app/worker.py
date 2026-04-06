import asyncio
import base64
import mimetypes
import json
import logging
import uuid
import re
from collections.abc import Iterable
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import func, or_
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from .config import get_settings
from .db import dispose_engine, is_sqlite_lock_error, managed_session
from .discord_ingest import get_discord_client, recover_attachment_assets_for_message, sync_attachment_assets
from .display_media import extract_image_urls, parse_attachment_urls_json
from .financials import compute_financials
from .models import (
    AttachmentAsset,
    DiscordMessage,
    OperationsLog,
    ParseAttempt,
    Transaction,
    WatchedChannel,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    expand_parse_status_filter_values,
    normalize_parse_status,
)
from .parser import parse_message, TimedOutRowError
from .reparse_runs import safe_record_reparse_run_outcome
from .runtime_logging import structured_log_line
from .transactions import sync_transaction_from_message

settings = get_settings()
STALE_PROCESSING_AFTER = timedelta(minutes=10)
STALE_RECOVERY_ERROR = "Recovered from stale processing state after worker interruption."
MAX_ATTEMPTS_ERROR = "Maximum parse attempts reached; requeue with attempt reset to retry."
OFFLINE_EDIT_REPARSE_ERROR = "Recovered refreshed message after offline audit."
logger = logging.getLogger(__name__)


def utcnow():
    return datetime.now(timezone.utc)


def canonical_status(row: DiscordMessage) -> str:
    return normalize_parse_status(
        row.parse_status,
        is_deleted=bool(row.is_deleted),
        needs_review=bool(row.needs_review),
    )


def set_row_status(
    row: DiscordMessage,
    status: str,
    *,
    error: str | None = None,
    clear_error: bool = False,
) -> None:
    row.parse_status = normalize_parse_status(status)
    row.needs_review = row.parse_status == PARSE_REVIEW_REQUIRED
    if clear_error:
        row.last_error = None
    elif error is not None:
        row.last_error = error


def worker_log(
    *,
    action: str,
    row: DiscordMessage | None = None,
    level: str = "info",
    success: bool | None = None,
    error: str | None = None,
    session: Session | None = None,
    **details,
) -> None:
    payload = {
        "message_id": getattr(row, "id", None),
        "discord_message_id": getattr(row, "discord_message_id", None),
        "channel": getattr(row, "channel_name", None),
        "channel_id": getattr(row, "channel_id", None),
        "current_state": canonical_status(row) if row is not None else None,
    }
    payload.update(details)
    message = structured_log_line(
        runtime="worker",
        action=action,
        success=success,
        error=error,
        **payload,
    )
    getattr(logger, level if level in {"debug", "info", "warning", "error"} else "info")(message)

    if session is None:
        return
    session.add(
        OperationsLog(
            event_type=f"queue.{action}",
            level="error" if level == "error" else level,
            source="worker",
            message=action,
            details_json=json.dumps(payload, default=str),
        )
    )


def reset_for_reprocess(
    row: DiscordMessage,
    *,
    reason: str,
    reset_attempts: bool = False,
) -> None:
    set_row_status(row, PARSE_PENDING, error=reason)
    row.reviewed_by = None
    row.reviewed_at = None
    if reset_attempts:
        row.parse_attempts = 0


def exhausted_retry_error(existing_error: str | None, *, reason: str) -> str:
    existing = (existing_error or "").strip()
    if not existing:
        return reason
    if existing.startswith(reason):
        return existing
    return f"{reason} Previous error: {existing}"


def row_retry_limit_already_exhausted(row: DiscordMessage, *, reason: str) -> bool:
    return (
        canonical_status(row) == PARSE_FAILED
        and (row.parse_attempts or 0) >= settings.parser_max_attempts
        and ((row.last_error or "").strip().startswith(reason))
    )


def exhaust_retry_limit(session: Session, row: DiscordMessage, *, reason: str) -> None:
    if row_retry_limit_already_exhausted(row, reason=reason):
        session.add(row)
        return

    exhausted_error = exhausted_retry_error(row.last_error, reason=reason)
    set_row_status(row, PARSE_FAILED, error=exhausted_error)
    session.add(row)
    worker_log(
        action="max_attempts_reached",
        row=row,
        level="warning",
        success=False,
        error=exhausted_error,
        session=session,
        parse_attempts=row.parse_attempts,
        max_attempts=settings.parser_max_attempts,
    )


def schedule_next_reprocess_run() -> datetime:
    return utcnow() + timedelta(hours=max(settings.parser_reprocess_interval_hours, 0.25))


def schedule_next_offline_audit_run() -> datetime:
    return utcnow() + timedelta(minutes=max(settings.periodic_offline_audit_interval_minutes, 1.0))


def normalize_legacy_queue_states(session: Session) -> int:
    rows = session.exec(
        select(DiscordMessage).where(
            DiscordMessage.parse_status.in_(["queued", "needs_review", "deleted"])
        )
    ).all()
    changed = 0
    for row in rows:
        normalized_status = canonical_status(row)
        if row.parse_status == normalized_status:
            continue
        row.parse_status = normalized_status
        if normalized_status == PARSE_IGNORED and not row.last_error and row.is_deleted:
            row.last_error = "message deleted"
        session.add(row)
        changed += 1
    if changed:
        session.commit()
    return changed


def _attempt_timestamp(attempt: ParseAttempt) -> datetime | None:
    timestamp = attempt.finished_at or attempt.started_at
    if timestamp is not None and timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp


def latest_attempt_timestamp(session: Session, row_id: int) -> datetime | None:
    latest_attempt = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.message_id == row_id)
        .order_by(ParseAttempt.started_at.desc(), ParseAttempt.id.desc())
    ).first()
    if not latest_attempt:
        return None
    return _attempt_timestamp(latest_attempt)


def row_has_nearby_siblings(session: Session, row: DiscordMessage) -> bool:
    if row.id is None or row.is_deleted:
        return False
    if not row.channel_id:
        return False
    if not row.author_name:
        return False
    if row.reviewed_at is not None:
        return False
    if canonical_status(row) not in {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED}:
        return False
    return len(
        build_stitch_group(
            session=session,
            row=row,
            window_seconds=settings.stitch_window_seconds,
            max_messages=settings.stitch_max_messages,
        )
    ) > 1


def _requeue_refreshed_message(row: DiscordMessage) -> None:
    reset_for_reprocess(row, reason=OFFLINE_EDIT_REPARSE_ERROR, reset_attempts=True)
    row.active_reparse_run_id = None


def reconcile_deleted_message(session: Session, row: DiscordMessage) -> None:
    sync_transaction_from_message(session, row)
    set_row_status(row, PARSE_IGNORED, error="message deleted")
    row.active_reparse_run_id = None
    session.add(row)


def reconcile_offline_audit_rows(session: Session, *, batch_size: int | None = None) -> int:
    effective_batch_size = batch_size or settings.parser_batch_size
    lookback_cutoff = utcnow() - timedelta(hours=max(settings.periodic_offline_audit_lookback_hours, 1.0))
    watched_channel_ids = [
        channel_id
        for channel_id in session.exec(
            select(WatchedChannel.channel_id).where(WatchedChannel.is_enabled == True)  # noqa: E712
        ).all()
        if channel_id
    ]

    changed = 0
    recently_touched = or_(
        DiscordMessage.created_at >= lookback_cutoff,
        DiscordMessage.edited_at >= lookback_cutoff,
        DiscordMessage.deleted_at >= lookback_cutoff,
        DiscordMessage.last_seen_at >= lookback_cutoff,
    )

    deleted_stmt = (
        select(DiscordMessage)
        .outerjoin(Transaction, Transaction.source_message_id == DiscordMessage.id)
        .where(recently_touched)
        .where(
            or_(
                DiscordMessage.is_deleted == True,  # noqa: E712
                DiscordMessage.deleted_at != None,  # noqa: E711
            )
        )
        .where(
            (DiscordMessage.parse_status != PARSE_IGNORED)
            | (DiscordMessage.last_error != "message deleted")
            | (Transaction.id != None)  # noqa: E711
        )
        .order_by(DiscordMessage.deleted_at.desc(), DiscordMessage.edited_at.desc(), DiscordMessage.created_at.desc())
        .limit(effective_batch_size)
    )
    if watched_channel_ids:
        deleted_stmt = deleted_stmt.where(DiscordMessage.channel_id.in_(watched_channel_ids))

    deleted_rows = session.exec(deleted_stmt).all()
    for row in deleted_rows:
        if not row.is_deleted:
            row.is_deleted = True
            row.deleted_at = row.deleted_at or utcnow()
        reconcile_deleted_message(session, row)
        worker_log(
            action="deleted_row_reconciled",
            row=row,
            success=True,
            session=session,
        )
        changed += 1

    edited_stmt = (
        select(DiscordMessage)
        .where(recently_touched)
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(DiscordMessage.edited_at != None)  # noqa: E711
        .where(
            DiscordMessage.parse_status.in_(
                sorted(
                    expand_parse_status_filter_values(
                        [PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED]
                    )
                )
            )
        )
        .order_by(DiscordMessage.edited_at.desc(), DiscordMessage.deleted_at.desc(), DiscordMessage.created_at.desc())
        .limit(effective_batch_size)
    )
    if watched_channel_ids:
        edited_stmt = edited_stmt.where(DiscordMessage.channel_id.in_(watched_channel_ids))

    edited_rows = session.exec(edited_stmt).all()
    for row in edited_rows:
        latest_attempt = session.exec(
            select(ParseAttempt)
            .where(ParseAttempt.message_id == row.id)
            .order_by(ParseAttempt.started_at.desc(), ParseAttempt.id.desc())
        ).first()
        last_attempt_timestamp = _attempt_timestamp(latest_attempt) if latest_attempt else None
        edited_at = row.edited_at
        if edited_at is not None and edited_at.tzinfo is None:
            edited_at = edited_at.replace(tzinfo=timezone.utc)

        if last_attempt_timestamp is not None and edited_at is not None and last_attempt_timestamp >= edited_at:
            continue

        _requeue_refreshed_message(row)
        session.add(row)
        worker_log(
            action="offline_edit_requeued",
            row=row,
            success=True,
            session=session,
            edited_at=edited_at,
            last_attempt_at=last_attempt_timestamp,
        )
        changed += 1

    if changed:
        session.commit()

    return changed

def queue_recent_stitch_audit_candidates(session: Session, *, batch_size: int | None = None) -> int:
    effective_batch_size = batch_size or settings.periodic_stitch_audit_limit
    audit_cutoff = utcnow() - timedelta(hours=max(settings.periodic_stitch_audit_lookback_hours, 0.25))
    min_age_cutoff = utcnow() - timedelta(minutes=max(settings.periodic_stitch_audit_min_age_minutes, 1))
    review_cutoff = utcnow() - timedelta(hours=max(settings.parser_reprocess_interval_hours, 0.25))
    watched_channel_ids = [
        channel_id
        for channel_id in session.exec(
            select(WatchedChannel.channel_id).where(WatchedChannel.is_enabled == True)  # noqa: E712
        ).all()
        if channel_id
    ]
    if not watched_channel_ids:
        return 0

    candidate_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id.in_(watched_channel_ids))
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED]))
            )
        )
        .where(DiscordMessage.reviewed_at == None)  # noqa: E711
        .where(
            or_(
                DiscordMessage.created_at >= audit_cutoff,
                DiscordMessage.edited_at >= audit_cutoff,
                DiscordMessage.deleted_at >= audit_cutoff,
                DiscordMessage.last_seen_at >= audit_cutoff,
            )
        )
        .where(DiscordMessage.created_at <= min_age_cutoff)
        .order_by(
            DiscordMessage.needs_review.desc(),
            DiscordMessage.edited_at.desc(),
            DiscordMessage.created_at.asc(),
            DiscordMessage.id.asc(),
        )
        .limit(max(effective_batch_size * 5, effective_batch_size))
    ).all()

    queued_count = 0
    for row in candidate_rows:
        if queued_count >= effective_batch_size:
            break
        if row.id is None:
            continue
        if canonical_status(row) not in {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED}:
            continue
        if row.parse_attempts >= settings.parser_max_attempts:
            continue

        last_attempt_timestamp = latest_attempt_timestamp(session, row.id)
        edited_at = row.edited_at
        if edited_at is not None and edited_at.tzinfo is None:
            edited_at = edited_at.replace(tzinfo=timezone.utc)

        requeue_reason: str | None = None
        if edited_at is not None and (last_attempt_timestamp is None or edited_at > last_attempt_timestamp):
            requeue_reason = "recent stitch audit: edited after last parse"
        elif row_recently_attempted(session, row.id, review_cutoff):
            continue
        elif row_may_benefit_from_auto_reprocess(row):
            requeue_reason = "recent stitch audit: fragment-like"
        elif row_has_nearby_siblings(session, row):
            requeue_reason = "recent stitch audit: nearby siblings"

        if not requeue_reason:
            continue

        reset_for_reprocess(row, reason=requeue_reason, reset_attempts=False)
        row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
        session.add(row)
        worker_log(
            action="recent_stitch_audit_requeued",
            row=row,
            success=True,
            session=session,
            reason=requeue_reason,
            edited_at=edited_at,
            last_attempt_at=last_attempt_timestamp,
            parse_attempts=row.parse_attempts,
        )
        queued_count += 1

    if queued_count:
        session.commit()

    return queued_count


def close_or_recover_unfinished_attempts(session: Session) -> None:
    recovery_now = utcnow()
    cutoff = recovery_now - STALE_PROCESSING_AFTER
    attempts = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.finished_at == None)  # noqa: E711
        .order_by(ParseAttempt.started_at)
        .limit(5000)
    ).all()
    unfinished_attempt_message_ids = {attempt.message_id for attempt in attempts}

    changed = False
    for attempt in attempts:
        row = session.get(DiscordMessage, attempt.message_id)
        if not row:
            attempt.finished_at = utcnow()
            attempt.success = False
            attempt.error = attempt.error or "message missing during recovery"
            session.add(attempt)
            worker_log(
                action="attempt_recovered_missing_row",
                level="warning",
                success=False,
                error=attempt.error,
                session=session,
                parse_attempt_id=attempt.id,
                message_id=attempt.message_id,
            )
            changed = True
            continue

        normalized_status = canonical_status(row)
        if row.parse_status != normalized_status:
            row.parse_status = normalized_status
            session.add(row)
            changed = True

        if normalized_status in {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_IGNORED}:
            attempt.finished_at = utcnow()
            attempt.success = True
            attempt.error = None
            session.add(attempt)
            worker_log(
                action="attempt_closed_after_terminal_state",
                row=row,
                success=True,
                session=session,
                parse_attempt_id=attempt.id,
            )
            changed = True
            continue

        attempt_started_at = attempt.started_at
        if attempt_started_at is not None and attempt_started_at.tzinfo is None:
            attempt_started_at = attempt_started_at.replace(tzinfo=timezone.utc)

        if normalized_status == PARSE_PROCESSING and attempt_started_at and attempt_started_at < cutoff:
            set_row_status(row, PARSE_PENDING, error=STALE_RECOVERY_ERROR)
            row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
            attempt.finished_at = recovery_now
            attempt.success = False
            attempt.error = "recovered stale processing attempt"
            session.add(row)
            session.add(attempt)
            worker_log(
                action="stale_processing_recovered",
                row=row,
                level="warning",
                success=False,
                error=attempt.error,
                session=session,
                parse_attempt_id=attempt.id,
                parse_attempts=row.parse_attempts,
            )
            changed = True

    recovered_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.parse_status.in_(sorted(expand_parse_status_filter_values([PARSE_PENDING]))))
        .where(DiscordMessage.parse_attempts >= settings.parser_max_attempts)
        .where(DiscordMessage.last_error == STALE_RECOVERY_ERROR)
    ).all()
    for row in recovered_rows:
        row.parse_attempts = max(settings.parser_max_attempts - 1, 0)
        session.add(row)
        changed = True

    exhausted_rows = session.exec(
        select(DiscordMessage)
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PENDING, PARSE_PROCESSING]))
            )
        )
        .where(DiscordMessage.parse_attempts >= settings.parser_max_attempts)
    ).all()
    for row in exhausted_rows:
        exhaust_retry_limit(session, row, reason=MAX_ATTEMPTS_ERROR)
        changed = True

    orphaned_processing_rows = session.exec(
        select(DiscordMessage).where(DiscordMessage.parse_status == PARSE_PROCESSING)
    ).all()
    for row in orphaned_processing_rows:
        if row.id in unfinished_attempt_message_ids:
            continue
        set_row_status(row, PARSE_PENDING, error="Recovered processing row without an active parse attempt.")
        row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
        session.add(row)
        worker_log(
            action="orphaned_processing_recovered",
            row=row,
            level="warning",
            success=False,
            error=row.last_error,
            session=session,
            parse_attempts=row.parse_attempts,
        )
        changed = True

    if changed:
        session.commit()


def clear_stitch_fields(row: DiscordMessage) -> None:
    row.stitched_group_id = None
    row.stitched_primary = False
    row.stitched_message_ids_json = "[]"


def clear_parsed_fields(row: DiscordMessage) -> None:
    row.deal_type = None
    row.amount = None
    row.payment_method = None
    row.cash_direction = None
    row.category = None
    row.item_names_json = "[]"
    row.items_in_json = "[]"
    row.items_out_json = "[]"
    row.trade_summary = None
    row.notes = None
    row.confidence = None
    row.needs_review = False
    row.image_summary = None
    row.entry_kind = None
    row.money_in = None
    row.money_out = None
    row.expense_category = None


def mark_grouped_child_ignored(row: DiscordMessage) -> None:
    clear_parsed_fields(row)
    set_row_status(row, PARSE_IGNORED, clear_error=True)


def clear_stale_group_members(
    session: Session,
    group_rows: list[DiscordMessage],
    primary_row: DiscordMessage,
) -> list[DiscordMessage]:
    row_ids = [grouped_row.id for grouped_row in group_rows if grouped_row.id is not None]
    stale_rows: list[DiscordMessage] = []

    for grouped_row in group_rows:
        prior_group_id = grouped_row.stitched_group_id
        if not prior_group_id:
            continue

        existing_group_rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.stitched_group_id == prior_group_id)
        ).all()

        for existing_row in existing_group_rows:
            if existing_row.id in row_ids:
                continue
            clear_stitch_fields(existing_row)
            clear_parsed_fields(existing_row)
            if not existing_row.is_deleted:
                reset_for_reprocess(existing_row, reason="re-queued after stitch group changed")
            stale_rows.append(existing_row)

    if len(group_rows) == 1:
        clear_stitch_fields(primary_row)

    return stale_rows


def _attachment_asset_content_type(asset: AttachmentAsset) -> str:
    if asset.content_type:
        return asset.content_type.split(";", 1)[0].strip() or "application/octet-stream"
    guessed_type, _ = mimetypes.guess_type(asset.filename or "")
    return guessed_type or "application/octet-stream"


def _attachment_asset_data_url(asset: AttachmentAsset) -> str | None:
    if not asset.data:
        return None
    content_type = _attachment_asset_content_type(asset)
    encoded = base64.b64encode(asset.data).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


def _cached_parser_image_inputs(session: Session, group_rows: list[DiscordMessage]) -> list[str]:
    row_ids = [row.id for row in group_rows if row.id is not None]
    if not row_ids:
        return []

    image_assets = session.exec(
        select(AttachmentAsset)
        .where(AttachmentAsset.message_id.in_(row_ids))
        .where(AttachmentAsset.is_image == True)  # noqa: E712
        .order_by(AttachmentAsset.message_id.asc(), AttachmentAsset.id.asc())
    ).all()
    if not image_assets:
        return []

    assets_by_message_id: dict[int, list[AttachmentAsset]] = {}
    for asset in image_assets:
        assets_by_message_id.setdefault(asset.message_id, []).append(asset)

    parser_inputs: list[str] = []
    for row in group_rows:
        if row.id is None:
            continue
        for asset in assets_by_message_id.get(row.id, []):
            data_url = _attachment_asset_data_url(asset)
            if data_url:
                parser_inputs.append(data_url)
    return parser_inputs


async def build_parser_attachment_inputs(
    session: Session,
    group_rows: list[DiscordMessage],
    fallback_attachment_urls: list[str],
) -> list[str]:
    parser_inputs = _cached_parser_image_inputs(session, group_rows)
    if parser_inputs:
        return parser_inputs

    recovered_any = False
    for grouped_row in group_rows:
        if grouped_row.id is None:
            continue
        if not extract_image_urls(parse_attachment_urls_json(grouped_row.attachment_urls_json)):
            continue
        if not grouped_row.channel_id or not grouped_row.discord_message_id:
            continue
        try:
            recovered = await recover_attachment_assets_for_message(
                channel_id=grouped_row.channel_id,
                discord_message_id=grouped_row.discord_message_id,
                message_row_id=grouped_row.id,
            )
            recovered_any = recovered_any or recovered
        except OperationalError as exc:
            if is_sqlite_lock_error(exc):
                worker_log(
                    action="attachment_recovery_sqlite_busy",
                    level="warning",
                    success=False,
                    error="SQLite busy during attachment recovery; falling back to URLs",
                    message_id=grouped_row.id,
                )
            else:
                raise

    if recovered_any:
        session.expire_all()
        parser_inputs = _cached_parser_image_inputs(session, group_rows)
        if parser_inputs:
            return parser_inputs

    return fallback_attachment_urls


async def parser_loop(stop_event: asyncio.Event):
    next_reprocess_at = schedule_next_reprocess_run()
    next_offline_audit_at = utcnow()
    while not stop_event.is_set():
        try:
            await process_once()
            if settings.periodic_offline_audit_enabled and utcnow() >= next_offline_audit_at:
                await offline_audit_once()
                next_offline_audit_at = schedule_next_offline_audit_run()
            if settings.parser_reprocess_enabled and utcnow() >= next_reprocess_at:
                await auto_reprocess_once()
                next_reprocess_at = schedule_next_reprocess_run()
        except OperationalError as e:
            worker_log(action="loop_database_error", level="error", success=False, error=str(e))
            dispose_engine()
        except Exception as e:
            worker_log(action="loop_error", level="error", success=False, error=str(e))
        await asyncio.sleep(settings.parser_poll_seconds)


async def process_once():
    row_ids: list[int] = []

    with managed_session() as session:
        normalize_legacy_queue_states(session)
        close_or_recover_unfinished_attempts(session)

        rows = session.exec(
            select(DiscordMessage)
            .where(
                DiscordMessage.parse_status.in_(
                    sorted(expand_parse_status_filter_values([PARSE_PENDING, PARSE_FAILED]))
                )
            )
            .where(DiscordMessage.parse_attempts < settings.parser_max_attempts)
            .order_by(DiscordMessage.created_at)
            .limit(settings.parser_batch_size)
        ).all()

        skipped_rows = session.exec(
            select(DiscordMessage)
            .where(
                DiscordMessage.parse_status.in_(
                    sorted(expand_parse_status_filter_values([PARSE_PENDING, PARSE_FAILED]))
                )
            )
            .where(DiscordMessage.parse_attempts >= settings.parser_max_attempts)
            .order_by(DiscordMessage.created_at)
            .limit(settings.parser_batch_size)
        ).all()
        for row in skipped_rows:
            exhaust_retry_limit(session, row, reason=MAX_ATTEMPTS_ERROR)

        for row in rows:
            set_row_status(row, PARSE_PROCESSING)
            row.parse_attempts += 1
            session.add(row)

            session.add(
                ParseAttempt(
                    message_id=row.id,
                    attempt_number=row.parse_attempts,
                    model_used="gpt-5-nano",
                )
            )

            row_ids.append(row.id)
            worker_log(
                action="processing_started",
                row=row,
                success=True,
                session=session,
                parse_attempts=row.parse_attempts,
            )

        session.commit()

    for row_id in row_ids:
        await process_row(row_id)


def row_may_benefit_from_auto_reprocess(row: DiscordMessage) -> bool:
    if row.is_deleted or row.reviewed_at is not None:
        return False

    text = normalize_text(row.content)

    if canonical_status(row) == PARSE_REVIEW_REQUIRED:
        return True
    if row.confidence is None or float(row.confidence) < 0.9:
        return True
    if not row.stitched_group_id and (looks_like_fragment(row) or is_payment_method_only_text(text)):
        return True
    if not row.stitched_group_id and has_images(row) and len(text) <= 30:
        return True

    return False


def row_recently_attempted(session: Session, row_id: int, cutoff: datetime) -> bool:
    latest_attempt = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.message_id == row_id)
        .order_by(ParseAttempt.started_at.desc())
    ).first()
    if not latest_attempt or latest_attempt.started_at is None:
        return False

    started_at = latest_attempt.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    return started_at >= cutoff


def queue_auto_reprocess_candidates(
    session: Session,
    *,
    batch_size: int | None = None,
    force: bool = False,
) -> int:
    queued_count = 0
    effective_batch_size = batch_size or settings.parser_reprocess_batch_size
    review_cutoff = utcnow() - timedelta(hours=max(settings.parser_reprocess_interval_hours, 0.25))
    min_age_cutoff = utcnow() - timedelta(minutes=max(settings.parser_reprocess_min_age_minutes, 1))
    lookback_cutoff = utcnow() - timedelta(days=max(settings.parser_reprocess_lookback_days, 1))

    candidate_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.is_deleted == False)
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED]))
            )
        )
        .where(DiscordMessage.reviewed_at == None)  # noqa: E711
        .where(DiscordMessage.created_at >= lookback_cutoff)
        .where(DiscordMessage.created_at <= min_age_cutoff)
        .order_by(DiscordMessage.needs_review.desc(), DiscordMessage.created_at.asc())
        .limit(max(effective_batch_size * 5, effective_batch_size))
    ).all()

    for row in candidate_rows:
        if queued_count >= effective_batch_size:
            break
        if row.id is None:
            continue
        if not force and not row_may_benefit_from_auto_reprocess(row):
            continue
        if not force and row_recently_attempted(session, row.id, review_cutoff):
            continue

        reset_for_reprocess(
            row,
            reason="manual reprocess" if force else "auto reprocess",
            reset_attempts=force,
        )
        if not force:
            row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
        session.add(row)
        worker_log(
            action="reprocess_queued",
            row=row,
            success=True,
            session=session,
            force=force,
            parse_attempts=row.parse_attempts,
        )
        queued_count += 1

    if queued_count:
        session.commit()

    return queued_count


def queue_reparse_range(
    session: Session,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    channel_id: str | None = None,
    include_statuses: Iterable[str] | None = None,
    include_reviewed: bool = False,
    reset_attempts: bool = True,
    reason: str = "manual range reparse",
    reparse_run_id: str | None = None,
) -> dict[str, int]:
    requested_statuses = {
        normalize_parse_status(status)
        for status in (include_statuses or (PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED))
        if status and normalize_parse_status(status) != PARSE_PROCESSING
    }
    if not requested_statuses:
        requested_statuses = {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED}
    raw_filter_statuses = expand_parse_status_filter_values(requested_statuses)

    stmt = (
        select(DiscordMessage)
        .where(DiscordMessage.is_deleted == False)
        .where(DiscordMessage.parse_status.in_(sorted(raw_filter_statuses)))
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
    )

    if start is not None:
        stmt = stmt.where(DiscordMessage.created_at >= start)
    if end is not None:
        stmt = stmt.where(DiscordMessage.created_at <= end)
    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)
    skipped_reviewed_count = 0
    if not include_reviewed:
        review_scope = stmt.subquery()
        skipped_reviewed_count = int(
            session.exec(
                select(func.count())
                .select_from(review_scope)
                .where(review_scope.c.reviewed_at != None)  # noqa: E711
            ).one()
        )
        stmt = stmt.where(DiscordMessage.reviewed_at == None)  # noqa: E711

    chunk_size = 500
    offset = 0
    result = {
        "matched": 0,
        "queued": 0,
        "already_queued": 0,
        "skipped_reviewed": skipped_reviewed_count,
        "first_message_id": None,
        "last_message_id": None,
        "first_message_created_at": None,
        "last_message_created_at": None,
    }

    while True:
        batch = session.exec(stmt.offset(offset).limit(chunk_size)).all()
        if not batch:
            break
        result["matched"] += len(batch)
        if result["first_message_id"] is None:
            result["first_message_id"] = batch[0].id
            result["first_message_created_at"] = batch[0].created_at
        result["last_message_id"] = batch[-1].id
        result["last_message_created_at"] = batch[-1].created_at

        chunk_touched = False
        for row in batch:
            if canonical_status(row) == PARSE_PENDING:
                set_row_status(row, PARSE_PENDING, error=reason)
                row.active_reparse_run_id = reparse_run_id
                session.add(row)
                result["already_queued"] += 1
                chunk_touched = True
                continue

            reset_for_reprocess(row, reason=reason, reset_attempts=reset_attempts)
            row.active_reparse_run_id = reparse_run_id
            session.add(row)
            result["queued"] += 1
            chunk_touched = True

        if chunk_touched:
            session.commit()

        offset += chunk_size
        if len(batch) < chunk_size:
            break

    return result


async def auto_reprocess_once():
    with managed_session() as session:
        queue_auto_reprocess_candidates(session)


async def offline_audit_once():
    with managed_session() as session:
        worker_log(
            action="offline_audit_started",
            success=True,
            session=session,
            periodic_limit=settings.periodic_offline_audit_limit_per_channel,
            lookback_hours=settings.periodic_offline_audit_lookback_hours,
        )
        deleted_or_edited = reconcile_offline_audit_rows(
            session,
            batch_size=settings.periodic_offline_audit_limit_per_channel,
        )
        worker_log(
            action="offline_audit_completed",
            success=True,
            session=session,
            deleted_or_edited=deleted_or_edited,
        )


async def run_periodic_stitch_audit_once() -> dict | None:
    if not settings.periodic_stitch_audit_enabled:
        return None

    lookback_hours = max(settings.periodic_stitch_audit_lookback_hours, 0.25)
    min_age_minutes = max(settings.periodic_stitch_audit_min_age_minutes, 1)
    batch_limit = max(settings.periodic_stitch_audit_limit, 1)

    with managed_session() as session:
        queued = queue_recent_stitch_audit_candidates(session, batch_size=batch_limit)
        worker_log(
            action="recent_stitch_audit_completed",
            success=True,
            session=session,
            lookback_hours=lookback_hours,
            min_age_minutes=min_age_minutes,
            batch_limit=batch_limit,
            queued=queued,
        )

    return {
        "ok": True,
        "lookback_hours": lookback_hours,
        "min_age_minutes": min_age_minutes,
        "batch_limit": batch_limit,
        "queued": queued,
    }


async def periodic_stitch_audit_loop(stop_event: asyncio.Event) -> None:
    if not settings.periodic_stitch_audit_enabled or not settings.parser_worker_enabled:
        return

    interval_minutes = max(settings.periodic_stitch_audit_interval_minutes, 5.0)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_minutes * 60)
            break
        except asyncio.TimeoutError:
            pass

        try:
            await run_periodic_stitch_audit_once()
        except OperationalError as exc:
            worker_log(
                action="recent_stitch_audit_database_error",
                level="error",
                success=False,
                error=str(exc),
            )
            dispose_engine()
        except Exception as exc:
            worker_log(
                action="recent_stitch_audit_failed",
                level="error",
                success=False,
                error=str(exc),
            )


async def process_row(row_id: int):
    with managed_session() as session:
        row = session.get(DiscordMessage, row_id)
        if not row:
            worker_log(
                action="processing_skipped_missing_row",
                level="warning",
                success=False,
                error="message missing",
                session=session,
                message_id=row_id,
            )
            session.commit()
            return

        current_status = canonical_status(row)
        active_reparse_run_id = row.active_reparse_run_id

        if row.is_deleted:
            reconcile_deleted_message(session, row)
            worker_log(
                action="processing_skipped_deleted",
                row=row,
                level="warning",
                success=False,
                error=row.last_error,
                session=session,
            )
            attempt = session.exec(
                select(ParseAttempt)
                .where(ParseAttempt.message_id == row.id)
                .order_by(ParseAttempt.id.desc())
            ).first()
            if attempt and attempt.finished_at is None:
                attempt.success = False
                attempt.error = row.last_error
                attempt.finished_at = utcnow()
                session.add(attempt)
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=row.last_error,
            )
            return

        if current_status not in [PARSE_PROCESSING, PARSE_PENDING, PARSE_FAILED]:
            worker_log(
                action="processing_skipped_state",
                row=row,
                level="warning",
                success=False,
                error=f"skip because state is {current_status}",
                session=session,
            )
            attempt = session.exec(
                select(ParseAttempt)
                .where(ParseAttempt.message_id == row.id)
                .order_by(ParseAttempt.id.desc())
            ).first()
            if attempt and attempt.finished_at is None:
                attempt.success = False
                attempt.error = f"skip because state is {current_status}"
                attempt.finished_at = utcnow()
                session.add(attempt)
            row.active_reparse_run_id = None
            session.add(row)
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=f"skip because state is {current_status}",
            )
            return

        group_rows = [row]
        if settings.stitch_enabled:
            group_rows = build_stitch_group(
                session=session,
                row=row,
                window_seconds=settings.stitch_window_seconds,
                max_messages=settings.stitch_max_messages,
            )

        group_rows = sorted(group_rows, key=lambda r: r.created_at)
        primary_row = group_rows[0]
        stale_rows = clear_stale_group_members(session, group_rows, primary_row)

        combined_text, combined_attachments, grouped_row_ids = combine_group_payload(group_rows)
        parser_attachment_inputs = await build_parser_attachment_inputs(
            session,
            group_rows,
            combined_attachments,
        )

        group_id = str(uuid.uuid4()) if len(group_rows) > 1 else None
        stitched_at = utcnow() if group_id else None

        attempt = session.exec(
            select(ParseAttempt)
            .where(ParseAttempt.message_id == row.id)
            .order_by(ParseAttempt.id.desc())
        ).first()
        worker_log(
            action="parse_started",
            row=row,
            success=True,
            session=session,
            grouped_message_ids=grouped_row_ids,
            attachment_count=len(combined_attachments),
        )

        try:
            result = await parse_message(
                content=combined_text,
                attachment_urls=parser_attachment_inputs,
                author_name=row.author_name or "",
                channel_name=row.channel_name or "",
            )
            normalized_cash_direction = result.get("parsed_cash_direction") if result.get("parsed_type") == "trade" else None
            learned_rule_event = result.pop("_learned_rule_event", None)
            usage = result.pop("_openai_usage", None) or {}
            model_used = result.pop("_openai_model", None)
            if learned_rule_event:
                learned_rule_status = learned_rule_event.get("status") or "unknown"
                learned_rule_reason = learned_rule_event.get("reason")
                worker_log(
                    action=f"learned_rule_{learned_rule_status}",
                    row=row,
                    success=learned_rule_status == "applied",
                    level="warning" if learned_rule_status == "rejected" else "info",
                    error=None if learned_rule_status == "applied" else learned_rule_reason,
                    session=session,
                    grouped_message_ids=grouped_row_ids,
                    **learned_rule_event,
                )
            financials = compute_financials(
                parsed_type=result.get("parsed_type"),
                parsed_category=result.get("parsed_category"),
                amount=result.get("parsed_amount"),
                cash_direction=normalized_cash_direction,
                message_text=combined_text,
            )

            for grouped_row in group_rows:
                grouped_row.stitched_group_id = group_id
                grouped_row.stitched_primary = (grouped_row.id == primary_row.id)
                grouped_row.stitched_message_ids_json = json.dumps(grouped_row_ids)
                grouped_row.last_stitched_at = stitched_at

            primary_row.deal_type = result.get("parsed_type")
            primary_row.amount = result.get("parsed_amount")
            primary_row.payment_method = result.get("parsed_payment_method")
            primary_row.cash_direction = normalized_cash_direction
            primary_row.category = result.get("parsed_category")
            primary_row.item_names_json = json.dumps(result.get("parsed_items", []))
            primary_row.items_in_json = json.dumps(result.get("parsed_items_in", []))
            primary_row.items_out_json = json.dumps(result.get("parsed_items_out", []))
            primary_row.trade_summary = result.get("parsed_trade_summary")
            primary_row.notes = result.get("parsed_notes")
            primary_row.confidence = result.get("confidence")
            primary_row.needs_review = bool(result.get("needs_review", False))
            primary_row.image_summary = result.get("image_summary")
            primary_row.entry_kind = financials.entry_kind
            primary_row.money_in = financials.money_in
            primary_row.money_out = financials.money_out
            primary_row.expense_category = financials.expense_category
            if result.get("ignore_message"):
                clear_parsed_fields(primary_row)
                set_row_status(primary_row, PARSE_IGNORED, clear_error=True)
            else:
                set_row_status(
                    primary_row,
                    PARSE_REVIEW_REQUIRED if primary_row.needs_review else PARSE_PARSED,
                    clear_error=True,
                )

            for grouped_row in group_rows:
                if grouped_row.id != primary_row.id:
                    mark_grouped_child_ignored(grouped_row)

            if attempt:
                attempt.success = True
                attempt.error = None
                attempt.finished_at = utcnow()
                attempt.model_used = model_used or attempt.model_used
                attempt.input_tokens = usage.get("input_tokens")
                attempt.cached_input_tokens = usage.get("cached_input_tokens")
                attempt.output_tokens = usage.get("output_tokens")
                attempt.total_tokens = usage.get("total_tokens")
                attempt.estimated_cost_usd = usage.get("estimated_cost_usd")
                session.add(attempt)

            for grouped_row in group_rows:
                session.add(grouped_row)
            for stale_row in stale_rows:
                session.add(stale_row)

            for grouped_row in group_rows:
                worker_log(
                    action="transaction_sync_started",
                    row=grouped_row,
                    success=True,
                    session=session,
                )
                try:
                    sync_transaction_from_message(session, grouped_row)
                except Exception as exc:
                    worker_log(
                        action="transaction_sync_failed",
                        row=grouped_row,
                        level="error",
                        success=False,
                        error=str(exc),
                        session=session,
                    )
                    raise
                worker_log(
                    action="transaction_sync_succeeded",
                    row=grouped_row,
                    success=True,
                    session=session,
                )
            for stale_row in stale_rows:
                worker_log(
                    action="transaction_sync_started",
                    row=stale_row,
                    success=True,
                    session=session,
                )
                try:
                    sync_transaction_from_message(session, stale_row)
                except Exception as exc:
                    worker_log(
                        action="transaction_sync_failed",
                        row=stale_row,
                        level="error",
                        success=False,
                        error=str(exc),
                        session=session,
                    )
                    raise
                worker_log(
                    action="transaction_sync_succeeded",
                    row=stale_row,
                    success=True,
                    session=session,
                )
            worker_log(
                action="parse_succeeded",
                row=primary_row,
                success=True,
                session=session,
                grouped_message_ids=grouped_row_ids,
                final_state=canonical_status(primary_row),
                needs_review=primary_row.needs_review,
            )
            row.active_reparse_run_id = None
            session.add(row)
            session.commit()
            safe_record_reparse_run_outcome(run_id=active_reparse_run_id, success=True)

        except TimedOutRowError as e:
            set_row_status(row, PARSE_FAILED, error=f"timeout: {e}")
            row.active_reparse_run_id = None

            if attempt:
                attempt.success = False
                attempt.error = f"timeout: {e}"
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            worker_log(
                action="parse_failed",
                row=row,
                level="error",
                success=False,
                  error=row.last_error,
                  session=session,
              )
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=row.last_error,
            )

        except Exception as e:
            set_row_status(row, PARSE_FAILED, error=str(e))
            row.active_reparse_run_id = None

            if attempt:
                attempt.success = False
                attempt.error = str(e)
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            worker_log(
                action="parse_failed",
                row=row,
                level="error",
                success=False,
                  error=row.last_error,
                  session=session,
              )
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=row.last_error,
            )
def looks_like_fragment(row: DiscordMessage) -> bool:
    text = (row.content or "").strip().lower()
    has_images = bool(json.loads(row.attachment_urls_json or "[]"))

    if has_images and len(text) <= 30:
        return True

    fragment_patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card)\s*\$?\d+",
        r"^\+?\s*\$?\d+\s*(zelle|venmo|paypal|cash|tap|card)$",
        r"^(top|bottom|left|right).*\b(in|out)\b",
        r"^\+?\s*\d+\s*(zelle|venmo|paypal|cash|tap|card)$",
    ]

    return any(re.search(p, text, re.I) for p in fragment_patterns)
def build_stitch_group(
    session: Session,
    row: DiscordMessage,
    window_seconds: int,
    max_messages: int,
) -> list[DiscordMessage]:
    if row.is_deleted:
        return [row]

    start_time = row.created_at - timedelta(seconds=window_seconds)
    end_time = row.created_at + timedelta(seconds=window_seconds)

    candidate_cap = max(200, max_messages * 30)
    candidates = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id == row.channel_id)
        .where(DiscordMessage.is_deleted == False)
        .where(DiscordMessage.created_at >= start_time)
        .where(DiscordMessage.created_at <= end_time)
        .order_by(DiscordMessage.created_at)
        .limit(candidate_cap)
    ).all()

    if not candidates:
        return [row]

    candidates = [c for c in candidates if not c.is_deleted]
    candidates = [candidate for candidate in candidates if same_author(candidate, row)]
    candidates = [
        candidate for candidate in candidates
        if abs((candidate.created_at - row.created_at).total_seconds()) <= window_seconds
    ]

    if row not in candidates:
        candidates.append(row)

    group_rows = [row]
    nearby_candidates = sorted(
        [candidate for candidate in candidates if candidate.id != row.id],
        key=lambda candidate: (
            abs((candidate.created_at - row.created_at).total_seconds()),
            candidate.created_at,
        ),
    )

    for candidate in nearby_candidates:
        if len(group_rows) >= max_messages:
            break
        if not stitch_group_needs_more_context(group_rows):
            break
        if not candidate_improves_group(group_rows, candidate):
            continue

        tentative_group = sorted(group_rows + [candidate], key=lambda grouped_row: grouped_row.created_at)
        if should_stitch_rows(row, tentative_group):
            group_rows = tentative_group

    if len(group_rows) <= 1 or not should_stitch_rows(row, group_rows):
        return [row]

    return group_rows

def combine_group_payload(rows: list[DiscordMessage]) -> tuple[str, list[str], list[int]]:
    combined_parts = []
    combined_attachments = []
    row_ids = []

    for i, r in enumerate(rows, start=1):
        text = (r.content or "").strip()
        if text:
            combined_parts.append(f"Message {i}: {text}")
        else:
            combined_parts.append(f"Message {i}: [no text]")

        combined_attachments.extend(json.loads(r.attachment_urls_json or "[]"))
        row_ids.append(r.id)

    combined_text = "\n\n".join(combined_parts)
    return combined_text, combined_attachments, row_ids
def normalize_text(text: str) -> str:
    return (text or "").strip().lower()


def has_images(row: DiscordMessage) -> bool:
    return bool(json.loads(row.attachment_urls_json or "[]"))


def same_author(left: DiscordMessage, right: DiscordMessage) -> bool:
    left_author_id = (left.author_id or "").strip()
    right_author_id = (right.author_id or "").strip()
    if left_author_id and right_author_id:
        return left_author_id == right_author_id

    left_author_name = (left.author_name or "").strip().lower()
    right_author_name = (right.author_name or "").strip().lower()
    return bool(left_author_name and right_author_name and left_author_name == right_author_name)


def is_payment_only_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card|cc|dc)\s*\$?\d+(?:\.\d{1,2})?$",
        r"^\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)$",
        r"^\+\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)?$",
        r"^(plus|\+)\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)?$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_payment_method_only_text(text: str) -> bool:
    text = normalize_text(text)
    return bool(re.fullmatch(r"(zelle|venmo|paypal|cash|tap|card|cc|dc)", text, re.I))


def is_trade_fragment_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r".*\b(in|out)\b.*",
        r"^(top|bottom|left|right).*$",
        r"^.*\bplus\b.*$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_explicit_buy_sell_text(text: str) -> bool:
    text = normalize_text(text)
    has_explicit_verb = bool(re.search(r"\b(sold|sell|bought|buy|paid)\b", text, re.I))
    has_payment_amount = bool(
        re.search(r"\b(zelle|venmo|paypal|cash|tap|card|cc|dc)\s*\$?\d+(?:\.\d{1,2})?\b", text, re.I)
        or re.search(r"\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)\b", text, re.I)
    )
    has_non_quantity_number = bool(
        re.search(r"\b(sold|sell|bought|buy|paid)\b.*\b\d+(?:\.\d{1,2})?\b(?!\s*(box|boxes|pack|packs|slab|slabs|case|cases|card|cards|binder|binders|lot|lots)\b)", text, re.I)
    )
    return has_payment_amount or (has_explicit_verb and has_non_quantity_number)


def is_short_fragment(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if has_images(row) and len(text) <= 20:
        return True
    if is_payment_only_text(text):
        return True
    if is_payment_method_only_text(text):
        return True
    if is_trade_fragment_text(text) and len(text) <= 50:
        return True
    return False


def looks_like_complete_deal(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    complete = is_explicit_buy_sell_text(text)

    # image + substantial text can also be a complete standalone log
    if has_images(row) and len(text) >= 25:
        return True

    return complete


def contains_amount(text: str) -> bool:
    return bool(re.search(r"\$?\d+(?:\.\d{1,2})?", normalize_text(text)))


def has_descriptive_text(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if not text or is_payment_only_text(text):
        return False
    return len(text) >= 8


def stitch_profile(rows: list[DiscordMessage]) -> dict[str, int]:
    profile = {
        "images": 0,
        "payment_fragments": 0,
        "descriptions": 0,
        "trade_fragments": 0,
    }

    for row in rows:
        text = normalize_text(row.content)
        if has_images(row):
            profile["images"] += 1
        if is_payment_only_text(text) or is_payment_method_only_text(text):
            profile["payment_fragments"] += 1
        if has_descriptive_text(row):
            profile["descriptions"] += 1
        if is_trade_fragment_text(text):
            profile["trade_fragments"] += 1

    return profile


def stitch_group_needs_more_context(rows: list[DiscordMessage]) -> bool:
    profile = stitch_profile(rows)

    if profile["descriptions"] >= 1 and profile["payment_fragments"] >= 1:
        return False
    if (
        profile["trade_fragments"] >= 1
        and profile["payment_fragments"] >= 1
        and (profile["images"] >= 1 or profile["descriptions"] >= 1)
    ):
        return False
    if (
        profile["images"] >= 1
        and profile["descriptions"] >= 1
        and profile["payment_fragments"] >= 1
    ):
        return False

    return True


def candidate_improves_group(group_rows: list[DiscordMessage], candidate: DiscordMessage) -> bool:
    before = stitch_profile(group_rows)
    after = stitch_profile(group_rows + [candidate])

    if after == before:
        return False

    return (
        (before["images"] == 0 and after["images"] > before["images"])
        or (before["payment_fragments"] == 0 and after["payment_fragments"] > before["payment_fragments"])
        or (before["descriptions"] == 0 and after["descriptions"] > before["descriptions"])
        or (
            before["trade_fragments"] == 0
            and after["trade_fragments"] > before["trade_fragments"]
            and stitch_group_needs_more_context(group_rows)
        )
    )


def should_force_stitch(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) != 2:
        return False

    sorted_rows = sorted(candidate_rows, key=lambda candidate: candidate.created_at)
    first_row, second_row = sorted_rows
    first_text = normalize_text(first_row.content)
    second_text = normalize_text(second_row.content)

    if has_large_gap(sorted_rows, max_gap_seconds=8):
        return False

    if has_images(first_row) and len(first_text) <= 20 and is_explicit_buy_sell_text(second_text):
        return True

    return False


def has_large_gap(candidate_rows: list[DiscordMessage], max_gap_seconds: int = 12) -> bool:
    if len(candidate_rows) <= 1:
        return False

    sorted_rows = sorted(candidate_rows, key=lambda row: row.created_at)
    for previous, current in zip(sorted_rows, sorted_rows[1:]):
        gap = abs((current.created_at - previous.created_at).total_seconds())
        if gap > max_gap_seconds:
            return True
    return False


def should_stitch_rows(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) <= 1:
        return False

    if should_force_stitch(base_row, candidate_rows):
        return True

    payment_fragments = 0
    short_fragments = 0
    complete_deals = 0
    rows_with_images = 0
    amount_mentions = 0

    for r in candidate_rows:
        text = normalize_text(r.content)
        if is_payment_only_text(text):
            payment_fragments += 1
        if is_short_fragment(r):
            short_fragments += 1
        if looks_like_complete_deal(r):
            complete_deals += 1
        if has_images(r):
            rows_with_images += 1
        if contains_amount(text):
            amount_mentions += 1

    if has_large_gap(candidate_rows):
        return False

    # Too many full standalone deals close together -> do not stitch
    if complete_deals >= 2:
        return False

    # Two image posts close together are often separate showroom deals
    if rows_with_images >= 2 and short_fragments < len(candidate_rows):
        return False

    # Multiple amount mentions usually mean multiple separate deals unless one row is clearly just a payment fragment.
    if amount_mentions >= 2 and payment_fragments == 0:
        return False

    # More than one payment fragment usually means multiple separate deals
    if payment_fragments >= 2:
        return False

    # If the base row itself already looks complete, only stitch when the neighbor is clearly a short fragment.
    if looks_like_complete_deal(base_row) and short_fragments <= 1:
        return False

    # Stitch only if there is at least one short/incomplete fragment
    if short_fragments == 0:
        return False

    # Good common case:
    # one image/incomplete row + one payment/direction fragment
    return True
