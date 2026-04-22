"""
/team/admin/employees/* — employee management (Wave 4).

All PII-decrypting paths write an AuditLog row BEFORE attempting decryption.
If the audit write fails, the decrypt does not happen (fail-closed).
"""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..auth import generate_password_reset_token, has_permission
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, EmployeeProfile, User, utcnow
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate

router = APIRouter()


ROLES = ("employee", "viewer", "manager", "reviewer", "admin")


def _mask_phone(value: Optional[str]) -> str:
    if not value:
        return "—"
    return "(•••) ••• ••••"


class PIIDecryptError(Exception):
    """Raised when ciphertext cannot be decrypted with any configured key."""


def _safe_decrypt(blob: Optional[bytes]) -> Optional[str]:
    """decrypt_pii wrapper that raises PIIDecryptError on failure.

    decrypt_pii already rewraps InvalidToken as ValueError; normalize to
    our sentinel so callers don't care about cryptography internals.
    """
    if blob is None:
        return None
    try:
        return decrypt_pii(blob)
    except ValueError as exc:
        raise PIIDecryptError(str(exc)) from exc


def _decode_address(blob: Optional[bytes]) -> dict:
    if not blob:
        return {}
    try:
        raw = _safe_decrypt(blob) or ""
        if not raw:
            return {}
        return json.loads(raw)
    except (PIIDecryptError, json.JSONDecodeError):
        return {}


def _audit_then_commit(session: Session, row: AuditLog) -> None:
    """Flush audit row first so a DB failure aborts the caller."""
    session.add(row)
    session.flush()


def _base_url(request: Request) -> str:
    scheme = request.url.scheme
    netloc = request.url.netloc
    return f"{scheme}://{netloc}"


def _detail_context(
    request: Request,
    session: Session,
    current: User,
    employee: User,
    profile: EmployeeProfile,
) -> dict:
    """Context for employee_detail.html, including per-action permission flags
    so the template renders only the buttons this user can actually submit."""
    return {
        "request": request,
        "title": f"Employee · {employee.username}",
        "current_user": current,
        "employee": employee,
        "profile": profile,
        "roles": ROLES,
        "reveal_field": None,
        "reveal_value": None,
        "reveal_error": None,
        "flash": None,
        "csrf_token": issue_token(request),
        "can_reveal_pii": has_permission(
            session, current, "admin.employees.reveal_pii"
        ),
        "can_reset_password": has_permission(
            session, current, "admin.employees.reset_password"
        ),
        "can_terminate": has_permission(
            session, current, "admin.employees.terminate"
        ),
        "can_purge": has_permission(
            session, current, "admin.employees.purge"
        ),
        "can_edit_profile": has_permission(
            session, current, "admin.employees.edit"
        ),
    }


@router.get("/team/admin/employees", response_class=HTMLResponse)
def admin_employees_list(
    request: Request,
    q: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    stmt = select(User).order_by(User.username)
    if q:
        like = f"%{q.strip().lower()}%"
        stmt = select(User).where(User.username.like(like)).order_by(User.username)
    rows = list(session.exec(stmt).all())[:200]
    profiles: dict[int, EmployeeProfile] = {}
    if rows:
        ids = [r.id for r in rows if r.id is not None]
        if ids:
            profiles = {
                p.user_id: p
                for p in session.exec(
                    select(EmployeeProfile).where(
                        EmployeeProfile.user_id.in_(ids)
                    )
                ).all()
            }
    return templates.TemplateResponse(
        request,
        "team/admin/employees_list.html",
        {
            "request": request,
            "title": "Employees",
            "current_user": user,
            "users": rows,
            "profiles": profiles,
            "q": q or "",
            "csrf_token": issue_token(request),
        },
    )


@router.get("/team/admin/employees/{user_id}", response_class=HTMLResponse)
def admin_employee_detail(
    request: Request,
    user_id: int,
    reveal_field: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    profile = session.get(EmployeeProfile, user_id) or EmployeeProfile(user_id=user_id)
    ctx = _detail_context(request, session, current, employee, profile)
    ctx["flash"] = flash
    return templates.TemplateResponse(
        request, "team/admin/employee_detail.html", ctx
    )


@router.post(
    "/team/admin/employees/{user_id}/reveal",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_reveal(
    request: Request,
    user_id: int,
    field: str = Form(...),
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.reveal_pii")
    if denial:
        return denial
    if field not in ("phone", "address", "legal_name", "email", "emergency_contact_name", "emergency_contact_phone"):
        return HTMLResponse("Unknown field", status_code=400)
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    profile = session.get(EmployeeProfile, user_id)

    # Phase 1: persist audit row in its own transaction so that a subsequent
    # decrypt failure cannot roll it back.
    ip = request.client.host if request.client else None
    session.add(
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="pii.reveal",
            resource_key="admin.employees.reveal_pii",
            details_json=json.dumps({"field": field}),
            ip_address=ip,
        )
    )
    session.commit()

    # Phase 2: attempt decrypt. Any failure writes a SECOND audit row
    # ("pii.reveal_failed") and surfaces a sanitized error to the user —
    # never a 500 with Fernet internals.
    value: Optional[str] = None
    decrypt_failed = False
    if profile is not None:
        try:
            if field == "phone":
                value = _safe_decrypt(profile.phone_enc)
            elif field == "legal_name":
                value = _safe_decrypt(profile.legal_name_enc)
            elif field == "email":
                value = _safe_decrypt(profile.email_ciphertext)
            elif field == "emergency_contact_name":
                value = _safe_decrypt(profile.emergency_contact_name_enc)
            elif field == "emergency_contact_phone":
                value = _safe_decrypt(profile.emergency_contact_phone_enc)
            elif field == "address":
                parts = _decode_address(profile.address_enc)
                if parts:
                    value = ", ".join(
                        p for p in (
                            parts.get("street"),
                            parts.get("city"),
                            parts.get("state"),
                            parts.get("zip"),
                        ) if p
                    )
        except PIIDecryptError:
            decrypt_failed = True
            session.add(
                AuditLog(
                    actor_user_id=current.id,
                    target_user_id=user_id,
                    action="pii.reveal_failed",
                    resource_key="admin.employees.reveal_pii",
                    details_json=json.dumps(
                        {"field": field, "reason": "invalid_token"}
                    ),
                    ip_address=ip,
                )
            )
            session.commit()

    profile_for_template = profile or EmployeeProfile(user_id=user_id)
    ctx = _detail_context(
        request, session, current, employee, profile_for_template
    )
    ctx.update({
        "reveal_field": field,
        "reveal_value": None if decrypt_failed else (value or "(empty)"),
        "reveal_error": field if decrypt_failed else None,
    })
    return templates.TemplateResponse(
        request, "team/admin/employee_detail.html", ctx
    )


def _parse_date(value: str) -> Optional[date]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


@router.post(
    "/team/admin/employees/{user_id}/profile-update",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_profile_update(
    request: Request,
    user_id: int,
    role: str = Form(default=""),
    display_name: str = Form(default=""),
    hourly_rate_cents: str = Form(default=""),
    hire_date: str = Form(default=""),
    termination_date: str = Form(default=""),
    clockify_user_id: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    profile = session.get(EmployeeProfile, user_id)
    if profile is None:
        profile = EmployeeProfile(user_id=user_id)
        session.add(profile)
        session.flush()

    changed: list[str] = []
    now = utcnow()

    new_role = (role or "").strip().lower()
    if new_role in ROLES and new_role != employee.role:
        employee.role = new_role
        employee.updated_at = now
        session.add(employee)
        changed.append("role")

    new_display = (display_name or "").strip()
    if new_display and new_display != (employee.display_name or ""):
        employee.display_name = new_display
        employee.updated_at = now
        session.add(employee)
        changed.append("display_name")

    rate_raw = (hourly_rate_cents or "").strip()
    if rate_raw:
        try:
            rate_int = int(rate_raw)
        except ValueError:
            rate_int = None
        if rate_int is not None:
            from ..pii import encrypt_pii
            profile.hourly_rate_cents_enc = encrypt_pii(str(rate_int))
            changed.append("hourly_rate_cents")

    for form_val, attr, label in (
        (hire_date, "hire_date", "hire_date"),
        (termination_date, "termination_date", "termination_date"),
    ):
        raw = (form_val or "").strip()
        if raw:
            parsed = _parse_date(raw)
            if parsed is not None and parsed != getattr(profile, attr):
                setattr(profile, attr, parsed)
                changed.append(label)

    clk_raw = (clockify_user_id or "").strip()
    if clk_raw != (profile.clockify_user_id or ""):
        profile.clockify_user_id = clk_raw or None
        changed.append("clockify_user_id")

    if changed:
        profile.updated_at = now
        session.add(profile)
        _audit_then_commit(
            session,
            AuditLog(
                actor_user_id=current.id,
                target_user_id=user_id,
                action="admin.profile_update",
                resource_key="admin.employees.edit",
                details_json=json.dumps({"fields": changed}),
                ip_address=(request.client.host if request.client else None),
            ),
        )
        session.commit()
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=Saved.", status_code=303
    )


@router.post(
    "/team/admin/employees/{user_id}/reset-password",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_reset_password(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.reset_password")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    raw = generate_password_reset_token(
        session, user_id=employee.id, issued_by_user_id=current.id
    )
    reset_url = f"{_base_url(request)}/team/password/reset/{raw}"
    return templates.TemplateResponse(
        request,
        "team/admin/employee_reset.html",
        {
            "request": request,
            "title": f"Password reset · {employee.username}",
            "current_user": current,
            "employee": employee,
            "reset_url": reset_url,
            "csrf_token": issue_token(request),
        },
    )


@router.get(
    "/team/admin/employees/{user_id}/terminate",
    response_class=HTMLResponse,
)
def admin_employee_terminate_page(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.terminate")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    return templates.TemplateResponse(
        request,
        "team/admin/employee_terminate.html",
        {
            "request": request,
            "title": f"Terminate · {employee.username}",
            "current_user": current,
            "employee": employee,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/employees/{user_id}/terminate",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_terminate_post(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.terminate")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    now = utcnow()
    employee.is_active = False
    employee.updated_at = now
    profile = session.get(EmployeeProfile, user_id)
    if profile is not None:
        profile.termination_date = now.date()
        profile.updated_at = now
        session.add(profile)
    session.add(employee)
    _audit_then_commit(
        session,
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="account.terminated",
            resource_key="admin.employees.terminate",
            details_json=json.dumps({"username": employee.username}),
            ip_address=(request.client.host if request.client else None),
        ),
    )
    session.commit()
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=Terminated.", status_code=303
    )


@router.get(
    "/team/admin/employees/{user_id}/purge",
    response_class=HTMLResponse,
)
def admin_employee_purge_page(
    request: Request,
    user_id: int,
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.purge")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    return templates.TemplateResponse(
        request,
        "team/admin/employee_purge.html",
        {
            "request": request,
            "title": f"Purge · {employee.username}",
            "current_user": current,
            "employee": employee,
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/employees/{user_id}/purge",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_purge_post(
    request: Request,
    user_id: int,
    confirm_username: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.purge")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    if (confirm_username or "").strip().lower() != (employee.username or "").lower():
        return HTMLResponse(
            "Confirmation did not match the employee username.",
            status_code=400,
        )
    profile = session.get(EmployeeProfile, user_id)
    now = utcnow()
    if profile is not None:
        for attr in (
            "legal_name_enc",
            "phone_enc",
            "address_enc",
            "emergency_contact_name_enc",
            "emergency_contact_phone_enc",
            "email_ciphertext",
            "hourly_rate_cents_enc",
        ):
            setattr(profile, attr, None)
        profile.email_lookup_hash = None
        profile.clockify_user_id = None
        profile.updated_at = now
        session.add(profile)
    employee.is_active = False
    employee.updated_at = now
    session.add(employee)
    _audit_then_commit(
        session,
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="account.purged",
            resource_key="admin.employees.purge",
            details_json=json.dumps({"username": employee.username}),
            ip_address=(request.client.host if request.client else None),
        ),
    )
    session.commit()
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=PII+purged.", status_code=303
    )
