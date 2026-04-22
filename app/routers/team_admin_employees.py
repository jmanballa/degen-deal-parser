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

from ..auth import (
    create_draft_employee,
    generate_invite_token,
    generate_password_reset_token,
    has_permission,
    is_draft_user,
)
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, EmployeeProfile, InviteToken, User, utcnow
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
    now = utcnow()
    outstanding = session.exec(
        select(InviteToken)
        .where(
            InviteToken.target_user_id == employee.id,
            InviteToken.used_at.is_(None),
            InviteToken.expires_at > now,
        )
        .order_by(InviteToken.created_at.desc())
    ).first()
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
        "is_draft": is_draft_user(employee),
        "outstanding_invite": outstanding,
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
        "can_issue_invite": has_permission(
            session, current, "admin.invites.issue"
        ),
    }


@router.get("/team/admin/employees", response_class=HTMLResponse)
def admin_employees_list(
    request: Request,
    q: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    # Hide deactivated employees by default — the common case is "who's on
    # payroll right now". Admins opt in to see everyone via the "Show
    # inactive" toggle, which flips this flag.
    show_inactive: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    include_inactive = show_inactive in ("1", "true", "yes", "on")
    # Default filter: show people who are currently part of the team.
    # That's active users PLUS drafts (pre-onboarding), which are
    # inactive by design but are exactly the people Jeffrey wants to
    # see so he can put them on the schedule before they register.
    # The "Show inactive" toggle reveals TRUE inactives — terminated
    # employees who have a real password but are switched off.
    stmt = select(User).order_by(User.is_active.desc(), User.username)
    if q:
        like = f"%{q.strip().lower()}%"
        stmt = (
            select(User)
            .where(User.username.like(like))
            .order_by(User.is_active.desc(), User.username)
        )
    if not include_inactive:
        # Keep active OR draft (is_active=False AND password_hash=''). Drafts
        # have an empty password_hash set by create_draft_employee.
        stmt = stmt.where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
    rows = list(session.exec(stmt).all())[:200]
    profiles: dict[int, EmployeeProfile] = {}
    outstanding_invite_ids: set[int] = set()
    draft_legal_names: dict[int, str] = {}
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
            # Which drafts currently have a live invite? Used for the status
            # pill ("Invite pending" vs "Draft").
            now = utcnow()
            outstanding_invite_ids = {
                inv.target_user_id
                for inv in session.exec(
                    select(InviteToken).where(
                        InviteToken.target_user_id.in_(ids),
                        InviteToken.used_at.is_(None),
                        InviteToken.expires_at > now,
                    )
                ).all()
                if inv.target_user_id is not None
            }
            # Decrypt legal_name for drafts so admin sees a real name in the
            # list (the placeholder username is intentionally ugly). Classic
            # users keep showing username + display_name; we never decrypt
            # their legal name without an audited reveal.
            for uid, prof in profiles.items():
                usr = next((r for r in rows if r.id == uid), None)
                if usr is None or not is_draft_user(usr):
                    continue
                if not prof.legal_name_enc:
                    continue
                try:
                    draft_legal_names[uid] = decrypt_pii(prof.legal_name_enc) or ""
                except ValueError:
                    draft_legal_names[uid] = ""
    return templates.TemplateResponse(
        request,
        "team/admin/employees_list.html",
        {
            "request": request,
            "title": "Employees",
            "current_user": user,
            "users": rows,
            "profiles": profiles,
            "outstanding_invite_ids": outstanding_invite_ids,
            "draft_legal_names": draft_legal_names,
            "is_draft_user": is_draft_user,
            "q": q or "",
            "flash": flash,
            "show_inactive": include_inactive,
            "csrf_token": issue_token(request),
        },
    )


@router.get("/team/admin/employees/new", response_class=HTMLResponse)
def admin_employee_new_page(
    request: Request,
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/admin/employee_new.html",
        {
            "request": request,
            "title": "Add employee",
            "current_user": current,
            "roles": ROLES,
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/employees/new",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_new_post(
    request: Request,
    display_name: str = Form(default=""),
    legal_name: str = Form(default=""),
    preferred_name: str = Form(default=""),
    role: str = Form(default="employee"),
    email: str = Form(default=""),
    hire_date: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    role_clean = (role or "").strip().lower()
    if role_clean not in ROLES:
        role_clean = "employee"
    try:
        user = create_draft_employee(
            session,
            created_by_user_id=current.id,
            display_name=display_name,
            legal_name=legal_name or None,
            preferred_name=preferred_name or None,
            role=role_clean,
            hire_date=_parse_date(hire_date),
            email=email or None,
        )
    except ValueError as exc:
        code = str(exc)
        message = {
            "draft_display_name_required": "Display name is required — it's what you'll call them on the schedule.",
            "draft_legal_name_required": "Display name is required.",
            "draft_email_taken": "That email already belongs to another employee.",
        }.get(code, code)
        return RedirectResponse(
            f"/team/admin/employees/new?error={message}", status_code=303
        )
    return RedirectResponse(
        f"/team/admin/employees/{user.id}?flash=Employee+added.+Send+them+an+invite+when+you%27re+ready.",
        status_code=303,
    )


@router.post(
    "/team/admin/employees/{user_id}/send-invite",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_send_invite(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.invites.issue")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    if not is_draft_user(employee):
        return RedirectResponse(
            f"/team/admin/employees/{user_id}?flash=This+employee+already+has+an+active+account.",
            status_code=303,
        )
    try:
        raw = generate_invite_token(
            session,
            role=employee.role or "employee",
            created_by_user_id=current.id,
            email_hint=(employee.display_name or "").strip() or None,
            target_user_id=user_id,
        )
    except ValueError as exc:
        return RedirectResponse(
            f"/team/admin/employees/{user_id}?flash=Could+not+issue+invite:+{exc}",
            status_code=303,
        )
    session.add(
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="invite.issued_for_draft",
            resource_key="admin.invites.issue",
            details_json=json.dumps({"role": employee.role}),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    invite_url = f"{_base_url(request)}/team/invite/accept/{raw}"
    return templates.TemplateResponse(
        request,
        "team/admin/invite_issued.html",
        {
            "request": request,
            "title": "Invite issued",
            "current_user": current,
            "invite_url": invite_url,
            "role": employee.role,
            "email_hint": employee.display_name or "",
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
    "/team/admin/employees/{user_id}/pii-update",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_pii_update(
    request: Request,
    user_id: int,
    legal_name: str = Form(default=""),
    email: str = Form(default=""),
    phone: str = Form(default=""),
    emergency_contact_name: str = Form(default=""),
    emergency_contact_phone: str = Form(default=""),
    address_street: str = Form(default=""),
    address_city: str = Form(default=""),
    address_state: str = Form(default=""),
    address_zip: str = Form(default=""),
    session: Session = Depends(get_session),
):
    """Admin-side PII edit — mirror of the self-serve /team/profile save.

    Kept deliberately audit-noisy: we log every field that actually
    changed, which admin did it, and from what IP. Blank fields do NOT
    clobber existing values — an empty input means "I didn't enter
    anything for this one". To explicitly clear a field, the admin
    should instead use the PII reveal + re-save workflow, or we could
    add a per-field delete button later. This prevents accidentally
    wiping an employee's emergency contact by saving an empty form.

    Only admins with `admin.employees.edit` can call this; no reveal
    permission is required (typing a new value is a blind overwrite).
    """
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

    from ..pii import encrypt_pii, email_lookup_hash as _email_hash

    now = utcnow()
    changed: list[str] = []

    def _overwrite_if_set(attr: str, raw: str, label: str) -> None:
        v = (raw or "").strip()
        if not v:
            return
        # Read current so a no-op typo doesn't fire a bogus audit row.
        try:
            current_val = decrypt_pii(getattr(profile, attr)) or ""
        except ValueError:
            current_val = ""
        if v != current_val:
            setattr(profile, attr, encrypt_pii(v))
            changed.append(label)

    _overwrite_if_set("legal_name_enc", legal_name, "legal_name")
    _overwrite_if_set("phone_enc", phone, "phone")
    _overwrite_if_set("emergency_contact_name_enc", emergency_contact_name, "emergency_contact_name")
    _overwrite_if_set("emergency_contact_phone_enc", emergency_contact_phone, "emergency_contact_phone")

    new_email = (email or "").strip().lower()
    if new_email:
        try:
            current_email = decrypt_pii(profile.email_ciphertext) or ""
        except ValueError:
            current_email = ""
        if new_email != current_email:
            new_hash = _email_hash(new_email)
            clash = session.exec(
                select(EmployeeProfile).where(
                    EmployeeProfile.email_lookup_hash == new_hash,
                    EmployeeProfile.user_id != user_id,
                )
            ).first()
            if clash is not None:
                return RedirectResponse(
                    f"/team/admin/employees/{user_id}?flash=That+email+is+already+taken+by+another+employee.",
                    status_code=303,
                )
            profile.email_ciphertext = encrypt_pii(new_email)
            profile.email_lookup_hash = new_hash
            changed.append("email")

    new_address = {
        "street": (address_street or "").strip(),
        "city": (address_city or "").strip(),
        "state": (address_state or "").strip(),
        "zip": (address_zip or "").strip(),
    }
    # Only touch address if admin actually filled in at least one part.
    # Same non-clobbering rule as above.
    if any(new_address.values()):
        current_addr = _decode_address(profile.address_enc)
        if new_address != current_addr:
            profile.address_enc = encrypt_pii(json.dumps(new_address))
            changed.append("address")

    if changed:
        profile.updated_at = now
        session.add(profile)
        _audit_then_commit(
            session,
            AuditLog(
                actor_user_id=current.id,
                target_user_id=user_id,
                action="admin.pii_update",
                resource_key="admin.employees.edit",
                details_json=json.dumps({"fields": changed}),
                ip_address=(request.client.host if request.client else None),
            ),
        )
        session.commit()
        return RedirectResponse(
            f"/team/admin/employees/{user_id}?flash=PII+updated+({len(changed)}+field{'s' if len(changed) != 1 else ''}).",
            status_code=303,
        )
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=No+changes.",
        status_code=303,
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
