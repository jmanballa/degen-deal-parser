import json
from typing import Optional

from sqlmodel import Session, select

from .models import OperationsLog


def write_operations_log(
    session: Session,
    *,
    event_type: str,
    message: str,
    level: str = "info",
    source: str = "system",
    details: Optional[dict] = None,
) -> OperationsLog:
    row = OperationsLog(
        event_type=event_type,
        level=level,
        source=source,
        message=message,
        details_json=json.dumps(details or {}),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def list_operations_logs(
    session: Session,
    *,
    limit: int = 200,
) -> list[OperationsLog]:
    return session.exec(
        select(OperationsLog)
        .order_by(OperationsLog.created_at.desc(), OperationsLog.id.desc())
        .limit(limit)
    ).all()


def parse_operations_log_details(row: OperationsLog) -> dict:
    try:
        return json.loads(row.details_json or "{}")
    except json.JSONDecodeError:
        return {}


def list_operations_logs_for_backfill_request(
    session: Session,
    *,
    request_id: int,
    limit: int = 300,
) -> list[OperationsLog]:
    rows = list_operations_logs(session, limit=max(limit * 4, 500))
    filtered: list[OperationsLog] = []
    for row in rows:
        details = parse_operations_log_details(row)
        if details.get("request_id") == request_id:
            filtered.append(row)
        if len(filtered) >= limit:
            break
    return filtered
