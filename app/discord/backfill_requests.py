import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlmodel import Session, select

from ..db import managed_session
from ..models import (
    BACKFILL_CANCELLED,
    BACKFILL_COMPLETED,
    BACKFILL_FAILED,
    BACKFILL_PROCESSING,
    BACKFILL_QUEUED,
    BACKFILL_TERMINAL_STATUSES,
    BackfillRequest,
)
from .ops_log import write_operations_log

BACKFILL_POLL_SECONDS = 5.0
BACKFILL_REQUEST_TIMEOUT_SECONDS = 300
STALE_BACKFILL_AFTER = timedelta(minutes=15)
BACKFILL_WAITING_LOG_INTERVAL_SECONDS = 60.0

_last_waiting_log_at = 0.0
_last_waiting_log_key: tuple[Optional[int], str] | None = None
_UNSET = object()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def load_backfill_result(row: BackfillRequest) -> dict:
    try:
        return json.loads(row.result_json or "{}")
    except json.JSONDecodeError:
        return {}


def default_backfill_progress(*, stage: str, last_progress_at: Optional[str] = None) -> dict:
    return build_backfill_progress_snapshot(
        {
            "stage": stage,
            "last_progress_at": last_progress_at or utcnow().isoformat(),
            "channels": {},
            "total_messages_discovered": 0,
            "inserted": 0,
            "skipped": 0,
            "queued_for_parsing": 0,
            "completed": 0,
            "failed": 0,
        }
    )


def status_to_progress_stage(status: str) -> str:
    if status == BACKFILL_QUEUED:
        return BACKFILL_QUEUED
    if status == BACKFILL_PROCESSING:
        return "waiting_for_discovery"
    return status


def normalize_backfill_progress(
    row: BackfillRequest,
    *,
    progress: Optional[dict] = None,
    stage: Optional[str] = None,
    waiting_reason: object = _UNSET,
    queued_count: object = _UNSET,
    last_progress_at: Optional[str] = None,
) -> dict:
    normalized = dict(progress or default_backfill_progress(stage=status_to_progress_stage(row.status)))
    normalized["channels"] = dict(normalized.get("channels") or {})
    normalized["stage"] = stage or normalized.get("stage") or status_to_progress_stage(row.status)
    normalized["last_progress_at"] = last_progress_at or normalized.get("last_progress_at") or utcnow().isoformat()

    if waiting_reason is not _UNSET:
        if waiting_reason:
            normalized["waiting_reason"] = str(waiting_reason)
        else:
            normalized.pop("waiting_reason", None)

    if queued_count is not _UNSET:
        if queued_count is None:
            normalized.pop("queued_count", None)
        else:
            normalized["queued_count"] = int(queued_count)

    return build_backfill_progress_snapshot(normalized)


def apply_backfill_request_state(
    row: BackfillRequest,
    *,
    status: str,
    progress: Optional[dict] = None,
    progress_stage: Optional[str] = None,
    started_at: object = _UNSET,
    finished_at: object = _UNSET,
    error_message: object = _UNSET,
    final_result: object = _UNSET,
    waiting_reason: object = _UNSET,
    queued_count: object = _UNSET,
) -> None:
    payload = load_backfill_result(row)
    normalized_progress = normalize_backfill_progress(
        row,
        progress=progress or dict(payload.get("progress") or {}),
        stage=progress_stage,
        waiting_reason=waiting_reason,
        queued_count=queued_count,
    )
    payload["progress"] = normalized_progress

    if final_result is not _UNSET:
        if final_result is None:
            payload.pop("final_result", None)
        else:
            payload["final_result"] = final_result
    elif status in BACKFILL_TERMINAL_STATUSES and "final_result" not in payload:
        payload["final_result"] = {
            "ok": status == BACKFILL_COMPLETED,
            "error": row.error_message if error_message is _UNSET else error_message,
            "status": status,
        }

    row.status = status
    if started_at is not _UNSET:
        row.started_at = started_at
    if finished_at is not _UNSET:
        row.finished_at = finished_at
    if error_message is not _UNSET:
        row.error_message = error_message
    row.result_json = json.dumps(payload)
    row.inserted_count = int(normalized_progress.get("inserted") or 0)
    row.skipped_count = int(normalized_progress.get("skipped") or 0)


def persist_backfill_request_state(session: Session, row: BackfillRequest, **kwargs) -> None:
    apply_backfill_request_state(row, **kwargs)
    session.add(row)


def repair_backfill_request_state_rows(session: Session) -> int:
    rows = session.exec(select(BackfillRequest).order_by(BackfillRequest.id)).all()
    repaired = 0
    for row in rows:
        original = {
            "status": row.status,
            "started_at": row.started_at,
            "finished_at": row.finished_at,
            "error_message": row.error_message,
            "result_json": row.result_json,
            "inserted_count": row.inserted_count,
            "skipped_count": row.skipped_count,
        }

        status = row.status or BACKFILL_QUEUED
        started_at = row.started_at
        finished_at = row.finished_at
        progress = dict(load_backfill_result(row).get("progress") or {})
        final_result = load_backfill_result(row).get("final_result", _UNSET)
        progress_stage = progress.get("stage") or status_to_progress_stage(status)

        if status in BACKFILL_TERMINAL_STATUSES and finished_at is None:
            finished_at = row.started_at or row.created_at
        if status == BACKFILL_PROCESSING and started_at is None:
            started_at = row.created_at
        if status == BACKFILL_QUEUED:
            started_at = None
            if finished_at and row.error_message and "requeued" in row.error_message.lower():
                finished_at = None

        persist_backfill_request_state(
            session,
            row,
            status=status,
            progress=progress,
            progress_stage=progress_stage,
            started_at=started_at,
            finished_at=finished_at,
            error_message=row.error_message,
            final_result=None if status not in BACKFILL_TERMINAL_STATUSES else (
                final_result if final_result is not _UNSET else {
                    "ok": status == BACKFILL_COMPLETED,
                    "error": row.error_message,
                    "status": status,
                    "repaired": True,
                }
            ),
            waiting_reason=progress.get("waiting_reason", _UNSET),
            queued_count=progress.get("queued_count", _UNSET),
        )
        updated = {
            "status": row.status,
            "started_at": row.started_at,
            "finished_at": row.finished_at,
            "error_message": row.error_message,
            "result_json": row.result_json,
            "inserted_count": row.inserted_count,
            "skipped_count": row.skipped_count,
        }
        if updated != original:
            repaired += 1

    if repaired:
        session.commit()
    return repaired


def build_backfill_progress_snapshot(progress: dict) -> dict:
    channels = progress.get("channels") or {}
    totals = {
        "total_messages_discovered": 0,
        "inserted": 0,
        "skipped": 0,
        "queued_for_parsing": 0,
        "completed": 0,
        "failed": 0,
    }
    for channel in channels.values():
        totals["total_messages_discovered"] += int(channel.get("processed_count") or 0)
        totals["inserted"] += int(channel.get("inserted_count") or 0)
        totals["skipped"] += int(channel.get("skipped_count") or 0)
        if channel.get("stage") == "completed":
            totals["completed"] += 1
        if channel.get("stage") == "failed":
            totals["failed"] += 1
    totals["queued_for_parsing"] = totals["inserted"]
    return {
        **progress,
        **totals,
    }


def update_backfill_request_progress(
    session: Session,
    request_id: int,
    *,
    event_type: str,
    details: Optional[dict] = None,
) -> None:
    row = session.get(BackfillRequest, request_id)
    if not row or row.status == BACKFILL_CANCELLED:
        return

    details = details or {}
    payload = load_backfill_result(row)
    progress = dict(payload.get("progress") or {})
    channels = dict(progress.get("channels") or {})

    channel_key = str(details.get("channel_id") or row.channel_id or "__all__")
    channel_progress = dict(channels.get(channel_key) or {})
    channel_progress["channel_id"] = details.get("channel_id") or row.channel_id
    channel_progress["channel_name"] = details.get("channel_name") or channel_progress.get("channel_name")
    for key in ("processed_count", "inserted_count", "updated_count", "skipped_count"):
        if details.get(key) is not None:
            channel_progress[key] = int(details.get(key) or 0)

    if event_type == "backfill_channel_completed":
        channel_progress["stage"] = "completed"
    elif event_type == "backfill_channel_failed":
        channel_progress["stage"] = "failed"
        channel_progress["error"] = details.get("error")
    elif event_type == "backfill_channel_skipped":
        channel_progress["stage"] = "skipped"
    elif event_type == "backfill_channel_started":
        channel_progress["stage"] = "discovering"
    elif event_type == "backfill_channel_progress":
        channel_progress["stage"] = channel_progress.get("stage") or "discovering"

    channel_progress["last_event_type"] = event_type
    channel_progress["last_progress_at"] = utcnow().isoformat()
    channels[channel_key] = channel_progress

    progress["channels"] = channels
    persist_backfill_request_state(
        session,
        row,
        status=row.status,
        progress=progress,
        progress_stage="discovering_messages" if row.status == BACKFILL_PROCESSING else row.status,
        error_message=row.error_message,
        waiting_reason=None if row.status == BACKFILL_PROCESSING else _UNSET,
        queued_count=None if row.status == BACKFILL_PROCESSING else _UNSET,
    )
    session.commit()


class BackfillCancelledError(Exception):
    pass


def enqueue_backfill_request(
    session: Session,
    *,
    channel_id: Optional[str],
    after: Optional[datetime],
    before: Optional[datetime],
    limit_per_channel: Optional[int],
    oldest_first: bool,
    requested_by: Optional[str],
) -> BackfillRequest:
    request = BackfillRequest(
        channel_id=channel_id,
        after=after,
        before=before,
        limit_per_channel=limit_per_channel,
        oldest_first=oldest_first,
        requested_by=requested_by,
        status=BACKFILL_QUEUED,
        created_at=utcnow(),
    )
    apply_backfill_request_state(
        request,
        status=BACKFILL_QUEUED,
        progress_stage=BACKFILL_QUEUED,
        started_at=None,
        finished_at=None,
        error_message=None,
        final_result=None,
        queued_count=None,
        waiting_reason=None,
    )
    session.add(request)
    session.commit()
    session.refresh(request)
    write_operations_log(
        session,
        event_type="backfill_queued",
        source="web",
        message=f"Queued backfill request {request.id} for {channel_id or 'all backfill-enabled watched channels'}",
        details={
            "request_id": request.id,
            "channel_id": channel_id,
            "after": after.isoformat() if after else None,
            "before": before.isoformat() if before else None,
            "limit_per_channel": limit_per_channel,
            "oldest_first": oldest_first,
            "requested_by": requested_by,
        },
    )
    return request


def list_recent_backfill_requests(session: Session, limit: int = 10) -> list[BackfillRequest]:
    return session.exec(
        select(BackfillRequest)
        .order_by(BackfillRequest.created_at.desc(), BackfillRequest.id.desc())
        .limit(limit)
    ).all()


def describe_backfill_client_state(client) -> str:
    if client is None:
        return "discord client is unavailable"
    if client.is_closed():
        return "discord client is closed"
    if not client.is_ready():
        return "discord client is not ready"
    return "discord client is ready"


def note_backfill_waiting_for_discord(session: Session, *, reason: str) -> None:
    global _last_waiting_log_at, _last_waiting_log_key

    oldest_queued = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == BACKFILL_QUEUED)
        .order_by(BackfillRequest.created_at, BackfillRequest.id)
    ).first()
    if not oldest_queued:
        return

    queue_count = len(
        session.exec(
            select(BackfillRequest.id).where(BackfillRequest.status == BACKFILL_QUEUED)
        ).all()
    )

    persist_backfill_request_state(
        session,
        oldest_queued,
        status=BACKFILL_QUEUED,
        progress=load_backfill_result(oldest_queued).get("progress") or {},
        progress_stage="waiting_for_discord",
        error_message=reason,
        waiting_reason=reason,
        queued_count=queue_count,
    )
    session.commit()

    log_key = (oldest_queued.id, reason)
    now = time.monotonic()
    if (
        _last_waiting_log_key == log_key
        and now - _last_waiting_log_at < BACKFILL_WAITING_LOG_INTERVAL_SECONDS
    ):
        return

    write_operations_log(
        session,
        event_type="backfill_waiting_for_discord",
        level="warning",
        source="worker",
        message=(
            f"Backfill request {oldest_queued.id} is queued while waiting for Discord readiness: {reason}"
        ),
        details={
            "request_id": oldest_queued.id,
            "channel_id": oldest_queued.channel_id,
            "status": oldest_queued.status,
            "queue_count": queue_count,
            "reason": reason,
        },
    )
    _last_waiting_log_key = log_key
    _last_waiting_log_at = now


def is_backfill_request_cancelled(session: Session, request_id: int) -> bool:
    row = session.get(BackfillRequest, request_id)
    if not row:
        return True
    return row.status == BACKFILL_CANCELLED


def cancel_backfill_request(
    session: Session,
    request_id: int,
    *,
    requested_by: Optional[str] = None,
) -> tuple[bool, str]:
    row = session.get(BackfillRequest, request_id)
    if not row:
        return False, "Backfill request not found."

    if row.status == BACKFILL_CANCELLED:
        return False, f"Backfill request {request_id} is already cancelled."

    if row.status in {BACKFILL_COMPLETED, BACKFILL_FAILED}:
        return False, f"Backfill request {request_id} is already {row.status}."

    prior_status = row.status
    message = (
        f"Cancelled by {requested_by}."
        if requested_by else
        "Cancelled by user."
    )
    persist_backfill_request_state(
        session,
        row,
        status=BACKFILL_CANCELLED,
        progress=load_backfill_result(row).get("progress") or {},
        progress_stage=BACKFILL_CANCELLED,
        finished_at=utcnow(),
        error_message=message,
        final_result={"ok": False, "cancelled": True, "error": message},
        waiting_reason=None,
        queued_count=None,
    )
    session.commit()
    write_operations_log(
        session,
        event_type="backfill_cancelled",
        level="warning",
        source="web",
        message=f"Cancelled backfill request {row.id}",
        details={
            "request_id": row.id,
            "channel_id": row.channel_id,
            "requested_by": requested_by,
            "prior_status": prior_status,
        },
    )
    return True, f"Cancelled backfill request {request_id}."


def recover_stale_backfill_requests(session: Session) -> int:
    cutoff = utcnow() - STALE_BACKFILL_AFTER
    rows = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == BACKFILL_PROCESSING)
        .order_by(BackfillRequest.started_at, BackfillRequest.id)
    ).all()

    recovered = 0
    for row in rows:
        started_at = row.started_at or row.created_at
        if started_at is None:
            continue
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        if started_at >= cutoff:
            continue

        error_message = "Recovered stale backfill request after worker interruption or timeout."
        persist_backfill_request_state(
            session,
            row,
            status=BACKFILL_FAILED,
            progress=load_backfill_result(row).get("progress") or {},
            progress_stage=BACKFILL_FAILED,
            finished_at=utcnow(),
            error_message=error_message,
            final_result={"ok": False, "error": error_message, "recovered_stale": True},
            waiting_reason=None,
            queued_count=None,
        )
        session.commit()
        write_operations_log(
            session,
            event_type="backfill_recovered_stale",
            level="error",
            source="worker",
            message=f"Recovered stale backfill request {row.id}",
            details={
                "request_id": row.id,
                "channel_id": row.channel_id,
                "started_at": started_at.isoformat(),
                "cutoff": cutoff.isoformat(),
            },
        )
        recovered += 1

    return recovered


def requeue_interrupted_backfill_requests(session: Session) -> int:
    rows = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == BACKFILL_PROCESSING)
        .order_by(BackfillRequest.started_at, BackfillRequest.id)
    ).all()

    requeued = 0
    for row in rows:
        previous_started_at = row.started_at
        error_message = "Requeued after worker restart interrupted the previous run."
        persist_backfill_request_state(
            session,
            row,
            status=BACKFILL_QUEUED,
            progress=load_backfill_result(row).get("progress") or {},
            progress_stage=BACKFILL_QUEUED,
            started_at=None,
            finished_at=None,
            error_message=error_message,
            final_result=None,
            waiting_reason=None,
            queued_count=None,
        )
        session.commit()
        write_operations_log(
            session,
            event_type="backfill_requeued_interrupted",
            level="warning",
            source="worker",
            message=f"Requeued interrupted backfill request {row.id}",
            details={
                "request_id": row.id,
                "channel_id": row.channel_id,
                "previous_started_at": previous_started_at.isoformat() if previous_started_at else None,
            },
        )
        requeued += 1

    return requeued


def claim_next_backfill_request(session: Session) -> Optional[dict]:
    request = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == BACKFILL_QUEUED)
        .order_by(BackfillRequest.created_at, BackfillRequest.id)
    ).first()
    if not request:
        return None

    persist_backfill_request_state(
        session,
        request,
        status=BACKFILL_PROCESSING,
        progress=load_backfill_result(request).get("progress") or {},
        progress_stage="waiting_for_discovery",
        started_at=utcnow(),
        finished_at=None,
        error_message=None,
        final_result=None,
        waiting_reason=None,
        queued_count=None,
    )
    session.commit()
    session.refresh(request)
    write_operations_log(
        session,
        event_type="backfill_started",
        source="worker",
        message=f"Started backfill request {request.id} for {request.channel_id or 'all backfill-enabled watched channels'}",
        details={
            "request_id": request.id,
            "channel_id": request.channel_id,
            "after": request.after.isoformat() if request.after else None,
            "before": request.before.isoformat() if request.before else None,
            "limit_per_channel": request.limit_per_channel,
            "oldest_first": request.oldest_first,
            "requested_by": request.requested_by,
        },
    )
    session.refresh(request)
    payload = {
        "id": request.id,
        "channel_id": request.channel_id,
        "after": request.after,
        "before": request.before,
        "limit_per_channel": request.limit_per_channel,
        "oldest_first": request.oldest_first,
    }
    session.expunge(request)
    return payload


def log_backfill_request_progress(
    request_id: int,
    *,
    event_type: str,
    message: str,
    level: str = "info",
    source: str = "worker",
    details: Optional[dict] = None,
) -> None:
    payload = {"request_id": request_id, **(details or {})}
    with managed_session() as session:
        update_backfill_request_progress(
            session,
            request_id,
            event_type=event_type,
            details=details,
        )
        write_operations_log(
            session,
            event_type=event_type,
            level=level,
            source=source,
            message=message,
            details=payload,
        )


def trigger_backfill_claim_attempt(client) -> bool:
    if client is None or client.is_closed() or not client.is_ready():
        return False

    loop = asyncio.get_running_loop()
    loop.create_task(
        process_backfill_request_once(client),
        name="backfill-queue-kick",
    )
    return True


def mark_backfill_request_complete(
    session: Session,
    request_id: int,
    *,
    ok: bool,
    result: dict,
) -> None:
    request = session.get(BackfillRequest, request_id)
    if not request:
        return
    if request.status == BACKFILL_CANCELLED:
        return

    existing_payload = load_backfill_result(request)
    progress = dict(existing_payload.get("progress") or {})
    if not progress:
        progress = default_backfill_progress(stage="completed" if ok else "failed")
    if int(progress.get("inserted") or 0) == 0 and result.get("inserted", result.get("total_inserted")) is not None:
        progress["inserted"] = int(result.get("inserted", result.get("total_inserted", 0)) or 0)
    if int(progress.get("skipped") or 0) == 0 and result.get("skipped", result.get("total_skipped")) is not None:
        progress["skipped"] = int(result.get("skipped", result.get("total_skipped", 0)) or 0)

    persist_backfill_request_state(
        session,
        request,
        status=BACKFILL_COMPLETED if ok else BACKFILL_FAILED,
        progress=progress,
        progress_stage=BACKFILL_COMPLETED if ok else BACKFILL_FAILED,
        finished_at=utcnow(),
        error_message=result.get("error"),
        final_result=result,
        waiting_reason=None,
        queued_count=None,
    )
    session.commit()
    write_operations_log(
        session,
        event_type="backfill_completed" if ok else "backfill_failed",
        level="info" if ok else "error",
        source="worker",
        message=(
            f"Completed backfill request {request.id}: inserted={request.inserted_count}, skipped={request.skipped_count}"
            if ok else
            f"Backfill request {request.id} failed: {request.error_message or 'unknown error'}"
        ),
        details={
            "request_id": request.id,
            "channel_id": request.channel_id,
            "status": request.status,
            "inserted_count": request.inserted_count,
            "skipped_count": request.skipped_count,
            "error_message": request.error_message,
            "result": result,
        },
    )


async def process_backfill_request_once(client) -> bool:
    if client is None or client.is_closed() or not client.is_ready():
        with managed_session() as session:
            # Keep queued requests queued, but record why the runner cannot
            # claim them yet so "stuck" backfills are visible in logs and UI.
            note_backfill_waiting_for_discord(
                session,
                reason=describe_backfill_client_state(client),
            )
        return False

    with managed_session() as session:
        request = claim_next_backfill_request(session)

    if not request or request.get("id") is None:
        return False

    async def progress_callback(payload: dict) -> None:
        with managed_session() as session:
            # Backfill cancellation currently takes effect on periodic progress
            # updates from the ingest loop rather than as an interrupt signal.
            if is_backfill_request_cancelled(session, request["id"]):
                raise BackfillCancelledError(f"Backfill request {request['id']} was cancelled")
        event_type = str(payload.get("event_type") or "backfill_progress")
        message = str(payload.get("message") or f"Backfill request {request['id']} progress update")
        level = str(payload.get("level") or "info")
        details = dict(payload.get("details") or {})
        log_backfill_request_progress(
            request["id"],
            event_type=event_type,
            message=message,
            level=level,
            details=details,
        )

    try:
        if request["channel_id"]:
            result = await asyncio.wait_for(
                client.backfill_channel(
                    channel_id=int(request["channel_id"]),
                    after=request["after"],
                    before=request["before"],
                    limit=request["limit_per_channel"],
                    oldest_first=request["oldest_first"],
                    progress_callback=progress_callback,
                ),
                timeout=BACKFILL_REQUEST_TIMEOUT_SECONDS,
            )
        else:
            result = await asyncio.wait_for(
                client.backfill_enabled_channels(
                    after=request["after"],
                    before=request["before"],
                    limit_per_channel=request["limit_per_channel"],
                    oldest_first=request["oldest_first"],
                    progress_callback=progress_callback,
                ),
                timeout=BACKFILL_REQUEST_TIMEOUT_SECONDS,
            )
    except BackfillCancelledError as exc:
        result = {"ok": False, "error": str(exc), "cancelled": True}
    except asyncio.TimeoutError:
        result = {
            "ok": False,
            "error": f"Backfill request timed out after {BACKFILL_REQUEST_TIMEOUT_SECONDS} seconds",
        }
    except Exception as exc:
        result = {"ok": False, "error": str(exc)}

    with managed_session() as session:
        mark_backfill_request_complete(
            session,
            request["id"],
            ok=bool(result.get("ok")),
            result=result,
        )

    return True


async def backfill_request_loop(stop_event: asyncio.Event, get_client) -> None:
    while not stop_event.is_set():
        try:
            with managed_session() as session:
                recover_stale_backfill_requests(session)
            processed = await process_backfill_request_once(get_client())
        except Exception as exc:
            print(f"[backfill] queue loop error: {exc}")
            with managed_session() as session:
                write_operations_log(
                    session,
                    event_type="backfill_loop_error",
                    level="error",
                    source="worker",
                    message=f"Backfill queue loop error: {exc}",
                    details={"error": str(exc)},
                )
            processed = False

        if processed:
            continue

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=BACKFILL_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
