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
