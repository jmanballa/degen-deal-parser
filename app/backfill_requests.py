import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlmodel import Session, select

from .db import managed_session
from .models import BackfillRequest
from .ops_log import write_operations_log

BACKFILL_POLL_SECONDS = 5.0
BACKFILL_REQUEST_TIMEOUT_SECONDS = 300
STALE_BACKFILL_AFTER = timedelta(minutes=15)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
        status="queued",
        created_at=utcnow(),
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


def recover_stale_backfill_requests(session: Session) -> int:
    cutoff = utcnow() - STALE_BACKFILL_AFTER
    rows = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == "processing")
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

        row.status = "failed"
        row.finished_at = utcnow()
        row.error_message = "Recovered stale backfill request after worker interruption or timeout."
        session.add(row)
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


def claim_next_backfill_request(session: Session) -> Optional[BackfillRequest]:
    request = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == "queued")
        .order_by(BackfillRequest.created_at, BackfillRequest.id)
    ).first()
    if not request:
        return None

    request.status = "processing"
    request.started_at = utcnow()
    request.finished_at = None
    request.error_message = None
    session.add(request)
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
    return request


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

    request.status = "completed" if ok else "failed"
    request.finished_at = utcnow()
    request.result_json = json.dumps(result)
    request.error_message = result.get("error")
    request.inserted_count = int(
        result.get("inserted", result.get("total_inserted", 0)) or 0
    )
    request.skipped_count = int(
        result.get("skipped", result.get("total_skipped", 0)) or 0
    )
    session.add(request)
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
        return False

    with managed_session() as session:
        request = claim_next_backfill_request(session)

    if not request or request.id is None:
        return False

    try:
        if request.channel_id:
            result = await asyncio.wait_for(
                client.backfill_channel(
                    channel_id=int(request.channel_id),
                    after=request.after,
                    before=request.before,
                    limit=request.limit_per_channel,
                    oldest_first=request.oldest_first,
                ),
                timeout=BACKFILL_REQUEST_TIMEOUT_SECONDS,
            )
        else:
            result = await asyncio.wait_for(
                client.backfill_enabled_channels(
                    after=request.after,
                    before=request.before,
                    limit_per_channel=request.limit_per_channel,
                    oldest_first=request.oldest_first,
                ),
                timeout=BACKFILL_REQUEST_TIMEOUT_SECONDS,
            )
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
            request.id,
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
