"""/team/admin/clockify - Clockify setup and employee mapping tools."""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..clockify import (
    ClockifyClient,
    ClockifyApiError,
    ClockifyConfigError,
    clockify_client_from_settings,
    clockify_is_configured,
    format_hours,
)
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, EmployeeProfile, User, utcnow
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate

router = APIRouter()


def _mask_id(value: str) -> str:
    value = (value or "").strip()
    if len(value) <= 8:
        return value or "-"
    return f"{value[:4]}...{value[-4:]}"


def _clockify_user_id(row: dict[str, Any]) -> str:
    return str(row.get("id") or "").strip()


def _clockify_user_name(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    if name:
        return name
    email = str(row.get("email") or "").strip()
    if email:
        return email
    return _mask_id(_clockify_user_id(row))


def _clockify_user_email(row: dict[str, Any]) -> str:
    return str(row.get("email") or "").strip()


def _employee_clockify_counts(session: Session) -> dict[str, int]:
    profiles = list(session.exec(select(EmployeeProfile)).all())
    users = {
        row.id: row
        for row in session.exec(select(User)).all()
        if row.id is not None
    }
    active_profiles = [
        profile
        for profile in profiles
        if (users.get(profile.user_id) is not None and users[profile.user_id].is_active)
    ]
    mapped = sum(1 for profile in active_profiles if profile.clockify_user_id)
    with_email = sum(1 for profile in active_profiles if profile.email_ciphertext)
    return {
        "active_profiles": len(active_profiles),
        "mapped": mapped,
        "unmapped": max(0, len(active_profiles) - mapped),
        "with_email": with_email,
    }


def _employee_rows(session: Session) -> list[dict[str, Any]]:
    users = list(
        session.exec(
            select(User)
            .where(User.is_active == True)  # noqa: E712
            .order_by(User.display_name, User.username)
        ).all()
    )
    profiles = {
        row.user_id: row
        for row in session.exec(
            select(EmployeeProfile).where(
                EmployeeProfile.user_id.in_([user.id for user in users if user.id])
            )
        ).all()
    } if users else {}
    out: list[dict[str, Any]] = []
    for employee in users:
        profile = profiles.get(employee.id)
        out.append(
            {
                "user": employee,
                "profile": profile,
                "clockify_user_id": (profile.clockify_user_id or "").strip()
                if profile
                else "",
            }
        )
    return out


def _employee_link_map(employee_rows: list[dict[str, Any]]) -> dict[str, User]:
    linked: dict[str, User] = {}
    for row in employee_rows:
        clockify_id = (row.get("clockify_user_id") or "").strip()
        user = row.get("user")
        if clockify_id and isinstance(user, User):
            linked[clockify_id] = user
    return linked


def _clockify_users_by_email(clockify_users: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_email: dict[str, dict[str, Any]] = {}
    for row in clockify_users:
        email = str(row.get("email") or "").strip().lower()
        user_id = str(row.get("id") or "").strip()
        if email and user_id:
            by_email.setdefault(email, row)
    return by_email


def build_clockify_roster_preview(
    clockify_users: list[dict[str, Any]],
    *,
    client: Optional[ClockifyClient] = None,
    settings=None,
    include_hours: bool = True,
    today: Optional[date] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Build admin-safe rows for Clockify people and optional hour previews."""
    rows: list[dict[str, Any]] = []
    today = today or date.today()
    for row in clockify_users[: max(0, limit)]:
        clockify_id = _clockify_user_id(row)
        preview = {
            "id": clockify_id,
            "id_masked": _mask_id(clockify_id),
            "name": _clockify_user_name(row),
            "email": _clockify_user_email(row),
            "status": str(row.get("status") or "").strip() or "-",
            "raw": row,
            "has_data": False,
            "hours_label": "-",
            "entry_count": 0,
            "running_count": 0,
            "data_error": "",
        }
        if include_hours and client is not None and clockify_id:
            try:
                summary = client.user_week_summary(
                    clockify_id,
                    today=today,
                    settings=settings,
                )
                preview["has_data"] = bool(summary.entries or summary.total_seconds)
                preview["hours_label"] = format_hours(summary.total_seconds)
                preview["entry_count"] = len(summary.entries)
                preview["running_count"] = summary.running_count
            except (ClockifyApiError, ClockifyConfigError) as exc:
                preview["data_error"] = str(exc)
        rows.append(preview)
    return rows


def set_employee_clockify_user_id(
    session: Session,
    *,
    current_user: User,
    user_id: int,
    clockify_user_id: str,
    ip_address: Optional[str] = None,
) -> tuple[bool, str]:
    employee = session.get(User, user_id)
    if employee is None:
        raise ValueError("employee_not_found")

    clockify_user_id = (clockify_user_id or "").strip()
    if clockify_user_id:
        existing = session.exec(
            select(EmployeeProfile).where(
                EmployeeProfile.clockify_user_id == clockify_user_id,
                EmployeeProfile.user_id != user_id,
            )
        ).first()
        if existing is not None:
            other_user = session.get(User, existing.user_id)
            other_name = (
                other_user.display_name or other_user.username
                if other_user is not None
                else f"employee {existing.user_id}"
            )
            return False, f"That Clockify user is already linked to {other_name}."

    profile = session.get(EmployeeProfile, user_id)
    if profile is None:
        profile = EmployeeProfile(user_id=user_id)
        session.add(profile)
        session.flush()

    old_value = (profile.clockify_user_id or "").strip()
    if old_value == clockify_user_id:
        return True, "No change."

    profile.clockify_user_id = clockify_user_id or None
    profile.updated_at = utcnow()
    session.add(profile)
    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            target_user_id=user_id,
            action="admin.clockify.manual_link",
            resource_key="admin.employees.edit",
            details_json=json.dumps(
                {
                    "old_clockify_user_id": old_value,
                    "new_clockify_user_id": clockify_user_id,
                },
                sort_keys=True,
            ),
            ip_address=ip_address,
        )
    )
    session.commit()
    if clockify_user_id:
        return True, "Clockify user linked."
    return True, "Clockify user unlinked."


def sync_clockify_user_ids_by_email(
    session: Session,
    *,
    current_user: User,
    clockify_users: list[dict[str, Any]],
    ip_address: Optional[str] = None,
) -> dict[str, int]:
    """Link local employee profiles to Clockify ids by exact email match.

    Existing conflicting Clockify ids are left untouched. Counts are audited;
    raw email addresses are never written to AuditLog or returned to the UI.
    """
    by_email = _clockify_users_by_email(clockify_users)
    profiles = list(session.exec(select(EmployeeProfile)).all())
    users = {
        row.id: row
        for row in session.exec(select(User)).all()
        if row.id is not None
    }
    now = utcnow()
    counts = {
        "checked": 0,
        "mapped": 0,
        "already_mapped": 0,
        "conflicts": 0,
        "missing_email": 0,
        "email_decrypt_failed": 0,
        "no_clockify_match": 0,
    }

    for profile in profiles:
        user = users.get(profile.user_id)
        if user is None or not user.is_active:
            continue
        counts["checked"] += 1
        if not profile.email_ciphertext:
            counts["missing_email"] += 1
            continue
        try:
            email = (decrypt_pii(profile.email_ciphertext) or "").strip().lower()
        except ValueError:
            counts["email_decrypt_failed"] += 1
            continue
        if not email:
            counts["missing_email"] += 1
            continue
        match = by_email.get(email)
        if match is None:
            counts["no_clockify_match"] += 1
            continue
        match_id = str(match.get("id") or "").strip()
        if not match_id:
            counts["no_clockify_match"] += 1
            continue
        existing = (profile.clockify_user_id or "").strip()
        if existing == match_id:
            counts["already_mapped"] += 1
            continue
        if existing and existing != match_id:
            counts["conflicts"] += 1
            continue
        profile.clockify_user_id = match_id
        profile.updated_at = now
        session.add(profile)
        counts["mapped"] += 1

    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            action="admin.clockify.sync_users",
            resource_key="admin.employees.edit",
            details_json=json.dumps(counts, sort_keys=True),
            ip_address=ip_address,
        )
    )
    session.commit()
    return counts


@router.get("/team/admin/clockify", response_class=HTMLResponse)
def admin_clockify_page(
    request: Request,
    flash: Optional[str] = None,
    error: Optional[str] = None,
    include_hours: str = Query(default="1"),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    settings = get_settings()
    configured = clockify_is_configured(settings)
    workspace = None
    status_error = None
    clockify_users: list[dict[str, Any]] = []
    roster_preview: list[dict[str, Any]] = []
    clockify_user_map: dict[str, dict[str, Any]] = {}
    preview_capped = False
    if configured:
        try:
            client = clockify_client_from_settings(settings)
            workspace = client.workspace_info()
            clockify_users = client.list_workspace_users(status="ALL")
            clockify_user_map = {
                _clockify_user_id(row): row
                for row in clockify_users
                if _clockify_user_id(row)
            }
            roster_preview = build_clockify_roster_preview(
                clockify_users,
                client=client,
                settings=settings,
                include_hours=include_hours not in ("0", "false", "no", "off"),
            )
            preview_capped = len(clockify_users) > len(roster_preview)
        except (ClockifyApiError, ClockifyConfigError) as exc:
            status_error = str(exc)
    employees = _employee_rows(session)
    linked_by_clockify = _employee_link_map(employees)
    return templates.TemplateResponse(
        request,
        "team/admin/clockify.html",
        {
            "request": request,
            "title": "Clockify",
            "current_user": user,
            "configured": configured,
            "workspace": workspace,
            "workspace_id_masked": _mask_id(settings.clockify_workspace_id),
            "status_error": status_error,
            "clockify_users": clockify_users,
            "clockify_user_map": clockify_user_map,
            "roster_preview": roster_preview,
            "preview_capped": preview_capped,
            "include_hours": include_hours not in ("0", "false", "no", "off"),
            "employees": employees,
            "linked_by_clockify": linked_by_clockify,
            "counts": _employee_clockify_counts(session),
            "can_sync": user.role == "admin",
            "mask_id": _mask_id,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/clockify/sync-users",
    dependencies=[Depends(require_csrf)],
)
async def admin_clockify_sync_users(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    settings = get_settings()
    if not clockify_is_configured(settings):
        return RedirectResponse(
            "/team/admin/clockify?error=CLOCKIFY_API_KEY+and+CLOCKIFY_WORKSPACE_ID+are+required.",
            status_code=303,
        )
    try:
        clockify_users = clockify_client_from_settings(settings).list_workspace_users(
            status="ALL"
        )
        counts = sync_clockify_user_ids_by_email(
            session,
            current_user=user,
            clockify_users=clockify_users,
            ip_address=(request.client.host if request.client else None),
        )
    except (ClockifyApiError, ClockifyConfigError) as exc:
        return RedirectResponse(
            "/team/admin/clockify?" + urlencode({"error": str(exc)}),
            status_code=303,
        )
    flash = (
        f"Mapped {counts['mapped']} employee(s). "
        f"{counts['already_mapped']} already linked, "
        f"{counts['conflicts']} conflict(s), "
        f"{counts['no_clockify_match']} without a Clockify email match."
    )
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({"flash": flash}),
        status_code=303,
    )


@router.post(
    "/team/admin/clockify/manual-link",
    dependencies=[Depends(require_csrf)],
)
async def admin_clockify_manual_link(
    request: Request,
    user_id: int = Form(...),
    clockify_user_id: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    try:
        ok, message = set_employee_clockify_user_id(
            session,
            current_user=user,
            user_id=user_id,
            clockify_user_id=clockify_user_id,
            ip_address=(request.client.host if request.client else None),
        )
    except ValueError:
        return RedirectResponse(
            "/team/admin/clockify?" + urlencode({"error": "Employee not found."}),
            status_code=303,
        )
    key = "flash" if ok else "error"
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({key: message}),
        status_code=303,
    )
