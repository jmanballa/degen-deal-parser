import json
import threading
from datetime import timezone
from typing import Callable

from sqlmodel import Session, select

from .db import managed_session
from .models import RuntimeHeartbeat, utcnow

RUNTIME_HEARTBEAT_INTERVAL_SECONDS = 30
RUNTIME_HEARTBEAT_STALE_AFTER_SECONDS = 120


def upsert_runtime_heartbeat(
    session: Session,
    *,
    runtime_name: str,
    host_name: str,
    status: str,
    details: dict,
) -> None:
    heartbeat = session.exec(
        select(RuntimeHeartbeat).where(RuntimeHeartbeat.runtime_name == runtime_name)
    ).first()
    if not heartbeat:
        heartbeat = RuntimeHeartbeat(runtime_name=runtime_name)
    heartbeat.host_name = host_name
    heartbeat.status = status
    heartbeat.details_json = json.dumps(details)
    heartbeat.updated_at = utcnow()
    session.add(heartbeat)
    session.commit()


def get_runtime_heartbeat_status(
    session: Session,
    runtime_name: str,
    *,
    runtime_label: str,
    updated_at_formatter: Callable[[object], str],
) -> dict:
    heartbeat = session.exec(
        select(RuntimeHeartbeat).where(RuntimeHeartbeat.runtime_name == runtime_name)
    ).first()
    if not heartbeat:
        return {
            "status": "missing",
            "label": "Offline",
            "is_running": False,
            "needs_attention": True,
            "alert_message": f"{runtime_label} has not reported a heartbeat yet. The hosted UI can load, but Discord ingest and parser work may be offline.",
            "host_name": "",
            "updated_at": None,
            "updated_at_label": "never",
            "stale_after_seconds": RUNTIME_HEARTBEAT_STALE_AFTER_SECONDS,
            "details": {},
        }

    updated_at = heartbeat.updated_at
    if updated_at and updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age_seconds = None
    if updated_at:
        age_seconds = max(0, int((utcnow() - updated_at).total_seconds()))

    effective_status = heartbeat.status
    label = "Running"
    is_running = True
    needs_attention = False
    alert_message = ""
    if age_seconds is not None and age_seconds > RUNTIME_HEARTBEAT_STALE_AFTER_SECONDS:
        effective_status = "stale"
        label = "Stale"
        is_running = False
        needs_attention = True
        alert_message = f"{runtime_label} heartbeat is stale. Hosted pages may still load, but new Discord messages might not be ingesting."
    elif heartbeat.status not in {"running", "ready"}:
        label = heartbeat.status.replace("_", " ").title()
        is_running = heartbeat.status in {"degraded"}
        if heartbeat.status in {"rate_limited", "error"}:
            needs_attention = True
            alert_message = f"{runtime_label} is reporting {label.lower()}. Parser and Discord ingest may be delayed."

    details = {}
    if heartbeat.details_json:
        try:
            details = json.loads(heartbeat.details_json)
        except json.JSONDecodeError:
            details = {}

    return {
        "status": effective_status,
        "label": label,
        "is_running": is_running,
        "needs_attention": needs_attention,
        "alert_message": alert_message,
        "host_name": heartbeat.host_name or "",
        "updated_at": updated_at.isoformat() if updated_at else None,
        "updated_at_label": updated_at_formatter(updated_at) if updated_at else "never",
        "age_seconds": age_seconds,
        "stale_after_seconds": RUNTIME_HEARTBEAT_STALE_AFTER_SECONDS,
        "details": details,
    }


def runtime_heartbeat_loop(
    stop_event: threading.Event,
    *,
    runtime_name: str,
    host_name: str,
    details_provider: Callable[[], dict],
) -> None:
    while not stop_event.is_set():
        details = details_provider()
        status = "running"
        if details.get("discord_status") in {"rate_limited", "degraded", "error"}:
            status = details["discord_status"]
        try:
            with managed_session() as session:
                upsert_runtime_heartbeat(
                    session,
                    runtime_name=runtime_name,
                    host_name=host_name,
                    status=status,
                    details=details,
                )
        except Exception as exc:
            print(f"[heartbeat] failed to update runtime heartbeat: {exc}")
        stop_event.wait(timeout=RUNTIME_HEARTBEAT_INTERVAL_SECONDS)
