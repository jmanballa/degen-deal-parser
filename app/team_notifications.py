"""Employee portal notification helpers.

Notifications are stored in AuditLog rows so we do not need a schema migration.
SMS delivery uses the same provider adapter as invite links.
"""
from __future__ import annotations

import json
from typing import Iterable, Optional

from fastapi import Request
from sqlmodel import Session, select

from .config import get_settings
from .models import AuditLog, EmployeeProfile, User
from .pii import PIIDecryptError, decrypt_pii
from .sms import (
    mask_sms_phone,
    normalize_sms_phone,
    send_sms,
    sms_phone_fingerprint,
)

EMPLOYEE_NOTIFICATION_ACTION = "employee.notification"


def _client_ip(request: Optional[Request]) -> Optional[str]:
    if request is None or request.client is None:
        return None
    return request.client.host


def _notification_url(path: str) -> str:
    cleaned = (path or "/team/").strip() or "/team/"
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return cleaned
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    base = (get_settings().public_base_url or "http://127.0.0.1:8000").rstrip("/")
    return f"{base}{cleaned}"


def _employee_phone(session: Session, user_id: int) -> tuple[Optional[str], str]:
    profile = session.get(EmployeeProfile, user_id)
    if profile is None or not profile.phone_enc:
        return None, "no_phone"
    try:
        raw_phone = decrypt_pii(profile.phone_enc) or ""
    except (PIIDecryptError, ValueError):
        return None, "phone_unreadable"
    normalized = normalize_sms_phone(raw_phone)
    if not normalized:
        return None, "phone_invalid"
    return normalized, "ok"


def notify_employee(
    session: Session,
    *,
    user_id: int,
    actor_user_id: Optional[int],
    kind: str,
    title: str,
    body: str,
    link_path: str = "/team/",
    request: Optional[Request] = None,
    send_text: bool = True,
) -> AuditLog:
    """Queue an employee notification and optionally send an SMS.

    The caller owns commit/rollback. This keeps admin actions transactional:
    the notification log is committed with the schedule/time-off/announcement
    change that caused it.
    """
    details: dict[str, object] = {
        "kind": (kind or "general").strip() or "general",
        "title": (title or "Team update").strip()[:160],
        "body": (body or "").strip()[:700],
        "link_path": (link_path or "/team/").strip() or "/team/",
        "sms": {"status": "not_requested"},
    }

    if send_text:
        phone, phone_status = _employee_phone(session, user_id)
        sms_details: dict[str, object] = {"status": phone_status}
        if phone:
            sms_details["phone"] = mask_sms_phone(phone)
            sms_details["phone_fingerprint"] = sms_phone_fingerprint(phone)
            message = (
                f"{details['title']}\n"
                f"{details['body']}\n"
                f"{_notification_url(str(details['link_path']))}"
            ).strip()[:1500]
            result = send_sms(to_phone=phone, body=message)
            sms_details.update(
                {
                    "provider": result.provider,
                    "delivery_status": result.status,
                    "message_id": result.message_id,
                    "dry_run": result.dry_run,
                }
            )
            if result.error:
                sms_details["error"] = result.error[:240]
        details["sms"] = sms_details

    row = AuditLog(
        actor_user_id=actor_user_id,
        target_user_id=user_id,
        action=EMPLOYEE_NOTIFICATION_ACTION,
        resource_key=f"employee.notification.{details['kind']}",
        details_json=json.dumps(details),
        ip_address=_client_ip(request),
    )
    session.add(row)
    return row


def notify_active_employees(
    session: Session,
    *,
    actor_user_id: Optional[int],
    kind: str,
    title: str,
    body: str,
    link_path: str = "/team/",
    request: Optional[Request] = None,
    exclude_user_ids: Optional[Iterable[int]] = None,
) -> int:
    excluded = set(exclude_user_ids or [])
    users = session.exec(select(User).where(User.is_active == True)).all()  # noqa: E712
    count = 0
    for user in users:
        if user.id is None or user.id in excluded:
            continue
        if user.role not in {"employee", "viewer", "manager", "reviewer", "admin"}:
            continue
        notify_employee(
            session,
            user_id=user.id,
            actor_user_id=actor_user_id,
            kind=kind,
            title=title,
            body=body,
            link_path=link_path,
            request=request,
            send_text=True,
        )
        count += 1
    return count
