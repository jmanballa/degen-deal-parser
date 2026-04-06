from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Iterable, Optional

from sqlmodel import Session, select

from .db import managed_session
from .models import ReparseRun, utcnow


def _duration_ms(started_at: datetime, finished_at: datetime) -> int:
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    if finished_at.tzinfo is None:
        finished_at = finished_at.replace(tzinfo=timezone.utc)
    return max(int((finished_at - started_at).total_seconds() * 1000), 0)


def create_reparse_run_record(
    session: Session,
    *,
    source: str,
    reason: str,
    range_after: datetime | None,
    range_before: datetime | None,
    channel_id: str | None,
    include_reviewed: bool,
    force_reviewed: bool,
    requested_statuses: Iterable[str],
) -> ReparseRun:
    row = ReparseRun(
        run_id=str(uuid.uuid4()),
        source=source,
        reason=reason,
        range_after=range_after,
        range_before=range_before,
        channel_id=channel_id,
        include_reviewed=include_reviewed,
        force_reviewed=force_reviewed,
        requested_statuses_json=json.dumps(list(requested_statuses)),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def finalize_reparse_run_queue_record(
    session: Session,
    *,
    run_id: str,
    selected_count: int,
    queued_count: int,
    already_queued_count: int,
    skipped_reviewed_count: int,
    first_message_id: int | None,
    last_message_id: int | None,
    first_message_created_at: datetime | None,
    last_message_created_at: datetime | None,
) -> ReparseRun | None:
    row = session.exec(select(ReparseRun).where(ReparseRun.run_id == run_id)).first()
    if not row:
        return None

    row.selected_count = selected_count
    row.queued_count = queued_count
    row.already_queued_count = already_queued_count
    row.skipped_reviewed_count = skipped_reviewed_count
    row.first_message_id = first_message_id
    row.last_message_id = last_message_id
    row.first_message_created_at = first_message_created_at
    row.last_message_created_at = last_message_created_at

    total_in_run = queued_count + already_queued_count
    if total_in_run == 0:
        finished_at = utcnow()
        row.finished_at = finished_at
        row.duration_ms = _duration_ms(row.requested_at, finished_at)
        row.status = "completed"
    else:
        row.status = "queued"

    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def record_reparse_run_outcome(
    session: Session,
    *,
    run_id: str,
    success: bool,
    error_message: str | None = None,
) -> ReparseRun | None:
    row = session.exec(select(ReparseRun).where(ReparseRun.run_id == run_id)).first()
    if not row:
        return None

    if success:
        row.succeeded_count += 1
    else:
        row.failed_count += 1
        if error_message:
            row.error_message = error_message

    total_in_run = row.queued_count + row.already_queued_count
    processed_count = row.succeeded_count + row.failed_count
    if total_in_run > 0 and processed_count >= total_in_run:
        finished_at = utcnow()
        row.finished_at = finished_at
        row.duration_ms = _duration_ms(row.requested_at, finished_at)
        row.status = "completed"

    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def list_recent_reparse_runs(session: Session, *, limit: int = 20) -> list[ReparseRun]:
    return session.exec(
        select(ReparseRun)
        .order_by(ReparseRun.requested_at.desc(), ReparseRun.id.desc())
        .limit(limit)
    ).all()


def safe_create_reparse_run(**kwargs) -> str | None:
    try:
        with managed_session() as session:
            row = create_reparse_run_record(session, **kwargs)
            return row.run_id
    except Exception as exc:
        print(f"[reparse_runs] safe_create_reparse_run failed: {exc}")
        return None


def safe_finalize_reparse_run_queue(**kwargs) -> None:
    try:
        with managed_session() as session:
            finalize_reparse_run_queue_record(session, **kwargs)
    except Exception as exc:
        print(f"[reparse_runs] safe_finalize_reparse_run_queue failed: {exc}")
        return None


def safe_record_reparse_run_outcome(*, run_id: str | None, success: bool, error_message: Optional[str] = None) -> None:
    if not run_id:
        return
    try:
        with managed_session() as session:
            record_reparse_run_outcome(
                session,
                run_id=run_id,
                success=success,
                error_message=error_message,
            )
    except Exception as exc:
        print(f"[reparse_runs] safe_record_reparse_run_outcome failed: {exc}")
        return None
