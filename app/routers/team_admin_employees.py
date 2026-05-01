"""
/team/admin/employees/* — employee management (Wave 4).

All PII-decrypting paths write an AuditLog row BEFORE attempting decryption.
If the audit write fails, the decrypt does not happen (fail-closed).
"""
from __future__ import annotations

import base64
import hashlib
import json
from calendar import monthrange
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import or_, update
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from ..clockify import clockify_today
from ..auth import (
    create_draft_employee,
    generate_invite_token,
    generate_password_reset_token,
    has_permission,
    is_draft_user,
)
from ..csrf import issue_token, require_csrf
from ..config import get_settings
from ..db import get_session
from ..models import (
    AuditLog,
    EmployeeCompensationHistory,
    EmployeeProfile,
    EmployeePurgeTombstone,
    InviteToken,
    PasswordResetToken,
    SHIFT_KIND_ALL,
    SHIFT_KIND_WORK,
    STAFF_KINDS,
    ShiftEntry,
    User,
    utcnow,
)
from ..pii import decrypt_pii, encrypt_pii
from ..rate_limit import rate_limited_or_429
from ..shared import templates
from ..sms import mask_sms_phone, normalize_sms_phone, send_sms, sms_phone_fingerprint
from .team_admin import _admin_denied_response, _admin_gate, _permission_gate

router = APIRouter()


ROLES = ("employee", "viewer", "manager", "reviewer", "admin")
COMPENSATION_TYPE_UNPAID = "unpaid"
COMPENSATION_TYPE_HOURLY = "hourly"
COMPENSATION_TYPE_MONTHLY = "monthly_salary"
COMPENSATION_TYPES = (
    COMPENSATION_TYPE_UNPAID,
    COMPENSATION_TYPE_HOURLY,
    COMPENSATION_TYPE_MONTHLY,
)
COMPENSATION_TYPE_LABELS = {
    COMPENSATION_TYPE_UNPAID: "Not paid",
    COMPENSATION_TYPE_HOURLY: "Hourly",
    COMPENSATION_TYPE_MONTHLY: "Monthly salary",
}
PAYMENT_METHODS = ("cash", "check")
PAYMENT_METHOD_LABELS = {
    "cash": "Cash",
    "check": "Check",
}
COMPENSATION_HISTORY_BASELINE_DATE = date(1970, 1, 1)
PAY_RATE_PAGE_LIMIT = 500
PURGE_RESTORE_WINDOW = timedelta(hours=24)
COMPENSATION_HISTORY_FIELDS = {
    "compensation_type",
    "hourly_rate_cents",
    "monthly_salary_cents",
    "monthly_salary_pay_day",
    "payment_method",
}


class CompensationDecryptError(ValueError):
    """Raised when encrypted compensation cannot be decrypted or parsed."""


def _normalize_compensation_type(value: str) -> str:
    value = (value or "").strip().lower()
    return value if value in COMPENSATION_TYPES else COMPENSATION_TYPE_HOURLY


def _normalize_payment_method(value: str) -> str:
    value = (value or "").strip().lower()
    return value if value in PAYMENT_METHODS else "cash"


def _parse_compensation_effective_date(value: str, today: date) -> date:
    raw = (value or "").strip()
    if not raw:
        return today
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return today


def _decrypt_money_cents(blob: Optional[bytes]) -> Optional[int]:
    if not blob:
        return None
    try:
        raw = decrypt_pii(blob) or ""
        return max(0, int(raw))
    except Exception as exc:
        raise CompensationDecryptError("Compensation value could not be decrypted") from exc


def _decrypt_hourly_rate_cents(profile: Optional[EmployeeProfile]) -> Optional[int]:
    if profile is None:
        return None
    return _decrypt_money_cents(profile.hourly_rate_cents_enc)


def _decrypt_monthly_salary_cents(profile: Optional[EmployeeProfile]) -> Optional[int]:
    if profile is None:
        return None
    return _decrypt_money_cents(profile.monthly_salary_cents_enc)


def _decrypt_history_hourly_rate_cents(
    row: Optional[EmployeeCompensationHistory],
) -> Optional[int]:
    if row is None:
        return None
    return _decrypt_money_cents(row.hourly_rate_cents_enc)


def _decrypt_history_monthly_salary_cents(
    row: Optional[EmployeeCompensationHistory],
) -> Optional[int]:
    if row is None:
        return None
    return _decrypt_money_cents(row.monthly_salary_cents_enc)


def _money_cents_enc(value: Optional[int]) -> Optional[bytes]:
    if value is None:
        return None
    return encrypt_pii(str(max(0, int(value))))


def _compensation_signature_from_profile(
    profile: Optional[EmployeeProfile],
) -> tuple[str, Optional[int], Optional[int], Optional[int], str]:
    compensation_type = _normalize_compensation_type(
        profile.compensation_type if profile is not None else ""
    )
    return (
        compensation_type,
        _decrypt_hourly_rate_cents(profile),
        _decrypt_monthly_salary_cents(profile),
        profile.monthly_salary_pay_day if profile is not None else None,
        _normalize_payment_method(profile.payment_method if profile is not None else ""),
    )


def _compensation_signature_from_history(
    row: EmployeeCompensationHistory,
) -> tuple[str, Optional[int], Optional[int], Optional[int], str]:
    return (
        _normalize_compensation_type(row.compensation_type or ""),
        _decrypt_history_hourly_rate_cents(row),
        _decrypt_history_monthly_salary_cents(row),
        row.monthly_salary_pay_day,
        _normalize_payment_method(row.payment_method or ""),
    )


def _history_snapshot_from_signature(
    signature: tuple[str, Optional[int], Optional[int], Optional[int], str],
    *,
    effective_date: Optional[date],
    source: str,
) -> dict:
    compensation_type, hourly_cents, salary_cents, pay_day, payment_method = signature
    return {
        "compensation_type": compensation_type,
        "hourly_rate_cents": hourly_cents,
        "monthly_salary_cents": salary_cents,
        "monthly_salary_pay_day": pay_day,
        "payment_method": payment_method,
        "effective_date": effective_date,
        "source": source,
    }


def _apply_compensation_signature_to_history(
    row: EmployeeCompensationHistory,
    signature: tuple[str, Optional[int], Optional[int], Optional[int], str],
) -> None:
    compensation_type, hourly_cents, salary_cents, pay_day, payment_method = signature
    row.compensation_type = compensation_type
    row.hourly_rate_cents_enc = _money_cents_enc(hourly_cents)
    row.monthly_salary_cents_enc = _money_cents_enc(salary_cents)
    row.monthly_salary_pay_day = pay_day
    row.payment_method = payment_method


def _create_compensation_history_row(
    *,
    user_id: int,
    effective_date: date,
    signature: tuple[str, Optional[int], Optional[int], Optional[int], str],
    current_user: Optional[User],
    source: str,
    note: str = "",
) -> EmployeeCompensationHistory:
    row = EmployeeCompensationHistory(
        user_id=user_id,
        effective_date=effective_date,
        source=source,
        note=note,
        created_by_user_id=current_user.id if current_user is not None else None,
    )
    _apply_compensation_signature_to_history(row, signature)
    return row


def _ensure_compensation_baseline(
    session: Session,
    *,
    profile: EmployeeProfile,
    before_signature: tuple[str, Optional[int], Optional[int], Optional[int], str],
    effective_date: date,
    current_user: Optional[User],
) -> None:
    user_id = profile.user_id
    existing_before = session.exec(
        select(EmployeeCompensationHistory)
        .where(
            EmployeeCompensationHistory.user_id == user_id,
            EmployeeCompensationHistory.effective_date < effective_date,
        )
        .limit(1)
    ).first()
    if existing_before is not None:
        return
    baseline_date = profile.hire_date or COMPENSATION_HISTORY_BASELINE_DATE
    if baseline_date >= effective_date:
        return
    session.add(
        _create_compensation_history_row(
            user_id=user_id,
            effective_date=baseline_date,
            signature=before_signature,
            current_user=current_user,
            source="baseline",
            note="Auto-created from profile before first dated change",
        )
    )


def _upsert_compensation_history(
    session: Session,
    *,
    profile: EmployeeProfile,
    effective_date: date,
    signature: tuple[str, Optional[int], Optional[int], Optional[int], str],
    current_user: Optional[User],
    source: str,
    note: str = "",
) -> EmployeeCompensationHistory:
    row = session.exec(
        select(EmployeeCompensationHistory).where(
            EmployeeCompensationHistory.user_id == profile.user_id,
            EmployeeCompensationHistory.effective_date == effective_date,
        )
    ).first()
    now = utcnow()
    if row is None:
        row = _create_compensation_history_row(
            user_id=profile.user_id,
            effective_date=effective_date,
            signature=signature,
            current_user=current_user,
            source=source,
            note=note,
        )
    else:
        _apply_compensation_signature_to_history(row, signature)
        row.source = source
        row.note = note
        row.updated_at = now
        if current_user is not None:
            row.created_by_user_id = current_user.id
    session.add(row)
    return row


def record_compensation_history_if_changed(
    session: Session,
    *,
    profile: EmployeeProfile,
    before_signature: tuple[str, Optional[int], Optional[int], Optional[int], str],
    effective_date: date,
    current_user: Optional[User],
    source: str,
) -> bool:
    after_signature = _compensation_signature_from_profile(profile)
    if after_signature == before_signature:
        return False
    _ensure_compensation_baseline(
        session,
        profile=profile,
        before_signature=before_signature,
        effective_date=effective_date,
        current_user=current_user,
    )
    _upsert_compensation_history(
        session,
        profile=profile,
        effective_date=effective_date,
        signature=after_signature,
        current_user=current_user,
        source=source,
        note="Created from current profile compensation",
    )
    return True


def compensation_history_rows_for_users(
    session: Session,
    user_ids: list[int],
    *,
    end_day: Optional[date] = None,
) -> dict[int, list[EmployeeCompensationHistory]]:
    if not user_ids:
        return {}
    stmt = select(EmployeeCompensationHistory).where(
        EmployeeCompensationHistory.user_id.in_(user_ids)
    )
    if end_day is not None:
        stmt = stmt.where(EmployeeCompensationHistory.effective_date <= end_day)
    rows = session.exec(
        stmt.order_by(
            EmployeeCompensationHistory.user_id,
            EmployeeCompensationHistory.effective_date,
            EmployeeCompensationHistory.id,
        )
    ).all()
    out: dict[int, list[EmployeeCompensationHistory]] = {}
    for row in rows:
        out.setdefault(row.user_id, []).append(row)
    return out


def compensation_snapshot_for_day(
    profile: Optional[EmployeeProfile],
    day: date,
    *,
    history_rows: Optional[list[EmployeeCompensationHistory]] = None,
) -> dict:
    selected: Optional[EmployeeCompensationHistory] = None
    for row in history_rows or []:
        if row.effective_date <= day:
            selected = row
        else:
            break
    if selected is not None:
        return _history_snapshot_from_signature(
            _compensation_signature_from_history(selected),
            effective_date=selected.effective_date,
            source=selected.source or "history",
        )
    return _history_snapshot_from_signature(
        _compensation_signature_from_profile(profile),
        effective_date=None,
        source="profile",
    )


def _format_money_dollars(cents: Optional[int]) -> str:
    if cents is None:
        return ""
    return f"{Decimal(cents) / Decimal(100):.2f}"


def _parse_money_dollars(
    value: str, *, max_cents: int
) -> tuple[Optional[int], bool]:
    raw = (value or "").strip()
    if not raw:
        return None, False
    raw = raw.replace("$", "").replace(",", "").strip()
    try:
        amount = Decimal(raw)
    except (InvalidOperation, ValueError):
        return None, True
    if amount < 0:
        return None, True
    cents = int((amount * Decimal(100)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return min(max(cents, 0), max_cents), False


def _parse_hourly_rate_dollars(value: str) -> tuple[Optional[int], bool]:
    return _parse_money_dollars(value, max_cents=1_000_000)


def _parse_monthly_salary_dollars(value: str) -> tuple[Optional[int], bool]:
    return _parse_money_dollars(value, max_cents=100_000_000)


def _parse_monthly_pay_day(value: str) -> tuple[Optional[int], bool]:
    raw = (value or "").strip()
    if not raw:
        return None, False
    if not raw.isdigit():
        return None, True
    day = int(raw)
    if day < 1 or day > 31:
        return None, True
    return day, False


def _monthly_pay_date(year: int, month: int, pay_day: Optional[int]) -> Optional[date]:
    if pay_day is None:
        return None
    last_day = monthrange(year, month)[1]
    return date(year, month, min(pay_day, last_day))


def _next_monthly_pay_date(
    today: date, pay_day: Optional[int]
) -> Optional[date]:
    this_month = _monthly_pay_date(today.year, today.month, pay_day)
    if this_month is None:
        return None
    if this_month >= today:
        return this_month
    next_month = today.month + 1
    next_year = today.year
    if next_month == 13:
        next_month = 1
        next_year += 1
    return _monthly_pay_date(next_year, next_month, pay_day)


def _format_date_label(value: Optional[date]) -> str:
    if value is None:
        return "Not set"
    return f"{value.strftime('%b')} {value.day}, {value.year}"


def _format_dollars(cents: int) -> str:
    return f"${Decimal(cents) / Decimal(100):,.2f}"


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
    session.commit()


def _pii_fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _revoke_employee_tokens(
    session: Session,
    user_id: int,
    now: datetime,
) -> tuple[int, int]:
    invite_result = session.exec(
        update(InviteToken)
        .where(
            InviteToken.target_user_id == user_id,
            InviteToken.used_at.is_(None),
        )
        .values(used_at=now)
    )
    reset_result = session.exec(
        update(PasswordResetToken)
        .where(
            PasswordResetToken.user_id == user_id,
            PasswordResetToken.used_at.is_(None),
        )
        .values(used_at=now)
    )
    return int(invite_result.rowcount or 0), int(reset_result.rowcount or 0)


def _base_url(request: Request) -> str:
    scheme = request.url.scheme
    netloc = request.url.netloc
    return f"{scheme}://{netloc}"


def _public_base_url(request: Request) -> str:
    configured = (get_settings().public_base_url or "").strip().rstrip("/")
    return configured or _base_url(request)


def _invite_accept_url(request: Request, raw_token: str) -> str:
    return f"{_public_base_url(request)}/team/invite/accept/{raw_token}"


def _employee_detail_redirect(user_id: int, flash: str) -> RedirectResponse:
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?{urlencode({'flash': flash})}",
        status_code=303,
    )


def _admin_self_action_rejected(action: str) -> HTMLResponse:
    return HTMLResponse(
        f"You cannot {action} your own admin account.",
        status_code=400,
    )


def _invite_sms_body(invite_url: str) -> str:
    return (
        "Degen Team invite: "
        f"{invite_url}\n"
        "Expires in 24 hours. Reply STOP to opt out."
    )


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
    can_reveal_pii = has_permission(session, current, "admin.employees.reveal_pii")
    can_issue_invite = has_permission(session, current, "admin.invites.issue")
    can_edit_profile = has_permission(session, current, "admin.employees.edit")
    can_edit_schedule_roster = has_permission(
        session, current, "admin.employee_roster.edit"
    )
    can_view_labor_financials = has_permission(
        session,
        current,
        "admin.labor_financials.view",
    )
    hourly_rate_cents = (
        _decrypt_hourly_rate_cents(profile) if can_view_labor_financials else None
    )
    monthly_salary_cents = (
        _decrypt_monthly_salary_cents(profile) if can_view_labor_financials else None
    )
    return {
        "request": request,
        "title": f"Employee · {employee.username}",
        "current_user": current,
        "employee": employee,
        "profile": profile,
        "roles": ROLES,
        "compensation_types": COMPENSATION_TYPES,
        "compensation_type_labels": COMPENSATION_TYPE_LABELS,
        "current_compensation_type": _normalize_compensation_type(
            profile.compensation_type if profile is not None else ""
        ),
        "hourly_rate_value": _format_money_dollars(hourly_rate_cents),
        "hourly_rate_display": _format_money_dollars(hourly_rate_cents),
        "monthly_salary_value": _format_money_dollars(monthly_salary_cents),
        "monthly_salary_pay_day_value": profile.monthly_salary_pay_day or "",
        "monthly_salary_pay_date_label": _format_date_label(
            _next_monthly_pay_date(clockify_today(), profile.monthly_salary_pay_day)
        ),
        "today_iso": clockify_today().isoformat(),
        "payment_methods": PAYMENT_METHODS,
        "payment_method_labels": PAYMENT_METHOD_LABELS,
        "reveal_field": None,
        "reveal_value": None,
        "reveal_error": None,
        "flash": None,
        "csrf_token": issue_token(request),
        "is_draft": is_draft_user(employee),
        "outstanding_invite": outstanding,
        "has_phone_on_file": bool(profile and profile.phone_enc),
        "can_reveal_pii": can_reveal_pii,
        "can_reset_password": has_permission(
            session, current, "admin.employees.reset_password"
        ),
        "can_terminate": has_permission(
            session, current, "admin.employees.terminate"
        ),
        "can_purge": has_permission(
            session, current, "admin.employees.purge"
        ),
        "can_edit_profile": can_edit_profile,
        "can_edit_schedule_roster": can_edit_schedule_roster,
        "can_view_labor_financials": can_view_labor_financials,
        "can_manage_compensation": can_edit_profile and can_view_labor_financials,
        "can_issue_invite": can_issue_invite,
        "can_text_invite": can_issue_invite and can_reveal_pii,
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
    stmt = (
        select(User)
        .join(EmployeeProfile, EmployeeProfile.user_id == User.id, isouter=True)
        .order_by(User.is_active.desc(), User.username)
    )
    q_clean = (q or "").strip()
    if not include_inactive:
        # Keep active OR draft (is_active=False AND password_hash=''). Drafts
        # have an empty password_hash set by create_draft_employee.
        stmt = stmt.where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
    if q_clean:
        q_like = f"%{q_clean}%"
        stmt = stmt.where(
            or_(
                User.username.ilike(q_like),
                User.display_name.ilike(q_like),
                EmployeeProfile.email_lookup_hash.ilike(q_like),
            )
        )
    rows = list(session.exec(stmt.limit(200)).all())
    profiles: dict[int, EmployeeProfile] = {}
    outstanding_invite_ids: set[int] = set()
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
    can_reveal_pii = has_permission(session, user, "admin.employees.reveal_pii")
    can_issue_invite = has_permission(session, user, "admin.invites.issue")
    can_edit_employee_profile = has_permission(session, user, "admin.employees.edit")
    can_edit_schedule_roster = has_permission(
        session, user, "admin.employee_roster.edit"
    )
    can_view_labor_financials = has_permission(
        session,
        user,
        "admin.labor_financials.view",
    )
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
            "is_draft_user": is_draft_user,
            "can_reveal_pii": can_reveal_pii,
            "can_issue_invite": can_issue_invite,
            "can_edit_employee_profile": can_edit_employee_profile,
            "can_edit_schedule_roster": can_edit_schedule_roster,
            "can_view_labor_financials": can_view_labor_financials,
            "can_text_invite": can_issue_invite and can_reveal_pii,
            "q": q or "",
            "flash": flash,
            "show_inactive": include_inactive,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/employees/{user_id}/schedulable-toggle",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_schedulable_toggle(
    request: Request,
    user_id: int,
    is_schedulable: str = Form(default=""),
    return_show_inactive: str = Form(default=""),
    return_q: str = Form(default=""),
    session: Session = Depends(get_session),
):
    """Flip the per-user 'can this person be put on the schedule' flag.

    The checkbox submits its value (`1`) only when checked, so an
    absent field means "unchecked". This is the same pattern the rest
    of the portal uses for toggles.
    """
    denial, current = _permission_gate(request, session, "admin.employee_roster.edit")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)

    new_value = is_schedulable in ("1", "true", "yes", "on")
    if employee.is_schedulable != new_value:
        employee.is_schedulable = new_value
        employee.updated_at = utcnow()
        session.add(employee)
        _audit_then_commit(
            session,
            AuditLog(
                actor_user_id=current.id,
                target_user_id=user_id,
                action="admin.employee.schedulable_toggle",
                resource_key="admin.employee_roster.edit",
                details_json=json.dumps({"is_schedulable": new_value}),
                ip_address=(request.client.host if request.client else None),
            ),
        )

    from urllib.parse import urlencode

    qs = {}
    if return_q:
        qs["q"] = return_q
    if return_show_inactive in ("1", "true", "yes", "on"):
        qs["show_inactive"] = "1"
    tail = f"?{urlencode(qs)}" if qs else ""
    return RedirectResponse(f"/team/admin/employees{tail}", status_code=303)


@router.get("/team/admin/employees/new", response_class=HTMLResponse)
def admin_employee_new_page(
    request: Request,
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(
        request,
        session,
        "admin.labor_financials.view",
    )
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
        return _employee_detail_redirect(
            user_id, "This employee already has an active account."
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
        return _employee_detail_redirect(user_id, f"Could not issue invite: {exc}")
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
    invite_url = _invite_accept_url(request, raw)
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
            "sms_result": None,
            "sms_phone_label": "",
        },
    )


@router.post(
    "/team/admin/employees/{user_id}/text-invite",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_text_invite(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.invites.issue")
    if denial:
        return denial
    pii_denial, _ = _admin_gate(request, session, "admin.employees.reveal_pii")
    if pii_denial:
        return pii_denial
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"text_invite:{current.id}",
        max_requests=20,
        window_seconds=900,
    ):
        return limited

    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    if not is_draft_user(employee):
        return _employee_detail_redirect(
            user_id, "This employee already has an active account."
        )

    profile = session.get(EmployeeProfile, user_id)
    if profile is None or not profile.phone_enc:
        return _employee_detail_redirect(
            user_id, "Add a phone number before texting an invite."
        )

    ip_address = request.client.host if request.client else None
    _audit_then_commit(
        session,
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="pii.use_for_invite_sms",
            resource_key="admin.employees.reveal_pii",
            details_json=json.dumps({"field": "phone", "purpose": "invite_sms"}),
            ip_address=ip_address,
        ),
    )

    try:
        phone_plain = _safe_decrypt(profile.phone_enc) or ""
    except PIIDecryptError:
        session.add(
            AuditLog(
                actor_user_id=current.id,
                target_user_id=user_id,
                action="invite.text_failed",
                resource_key="admin.invites.issue",
                details_json=json.dumps(
                    {"reason": "phone_decrypt_failed"}, sort_keys=True
                ),
                ip_address=ip_address,
            )
        )
        session.commit()
        return _employee_detail_redirect(
            user_id, "Could not text invite because the saved phone could not be decrypted."
        )

    to_phone = normalize_sms_phone(phone_plain)
    if not to_phone:
        session.add(
            AuditLog(
                actor_user_id=current.id,
                target_user_id=user_id,
                action="invite.text_failed",
                resource_key="admin.invites.issue",
                details_json=json.dumps({"reason": "invalid_phone"}, sort_keys=True),
                ip_address=ip_address,
            )
        )
        session.commit()
        return _employee_detail_redirect(
            user_id, "Saved phone number is not a valid SMS number."
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
        return _employee_detail_redirect(user_id, f"Could not issue invite: {exc}")

    invite_url = _invite_accept_url(request, raw)
    body = _invite_sms_body(invite_url)
    sms_result = send_sms(
        to_phone=to_phone,
        body=body,
        settings=get_settings(),
    )
    invite_row = session.exec(
        select(InviteToken)
        .where(InviteToken.target_user_id == user_id)
        .order_by(InviteToken.created_at.desc())
    ).first()
    invite_id = invite_row.id if invite_row is not None else None
    phone_label = mask_sms_phone(to_phone)
    safe_details = {
        "provider": sms_result.provider,
        "status": sms_result.status,
        "dry_run": sms_result.dry_run,
        "success": sms_result.success,
        "invite_id": invite_id,
        "phone": phone_label,
        "phone_fingerprint": sms_phone_fingerprint(to_phone),
    }
    if sms_result.message_id:
        safe_details["message_id"] = sms_result.message_id
    if sms_result.error:
        safe_details["error"] = sms_result.error[:240]
    session.add(
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="invite.issued_for_draft",
            resource_key="admin.invites.issue",
            details_json=json.dumps(
                {"role": employee.role, "delivery": "sms", "invite_id": invite_id},
                sort_keys=True,
            ),
            ip_address=ip_address,
        )
    )
    session.add(
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action=(
                "invite.text_dry_run"
                if sms_result.success and sms_result.dry_run
                else "invite.text_sent"
                if sms_result.success
                else "invite.text_failed"
            ),
            resource_key="admin.invites.issue",
            details_json=json.dumps(safe_details, sort_keys=True),
            ip_address=ip_address,
        )
    )
    session.commit()

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
            "sms_result": sms_result,
            "sms_phone_label": phone_label,
        },
    )


def _pay_rate_rows(session: Session, *, include_inactive: bool = False) -> list[dict]:
    stmt = select(User).order_by(User.is_active.desc(), User.display_name, User.username)
    if not include_inactive:
        stmt = stmt.where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
    stmt = stmt.limit(PAY_RATE_PAGE_LIMIT)
    users = list(session.exec(stmt).all())
    ids = [row.id for row in users if row.id is not None]
    profiles: dict[int, EmployeeProfile] = {}
    if ids:
        profiles = {
            profile.user_id: profile
            for profile in session.exec(
                select(EmployeeProfile).where(EmployeeProfile.user_id.in_(ids))
            ).all()
        }
    rows: list[dict] = []
    for employee in users:
        profile = profiles.get(employee.id or 0)
        compensation_type = _normalize_compensation_type(
            profile.compensation_type if profile is not None else ""
        )
        rate_cents = _decrypt_hourly_rate_cents(profile)
        salary_cents = _decrypt_monthly_salary_cents(profile)
        pay_day = profile.monthly_salary_pay_day if profile is not None else None
        payment_method = _normalize_payment_method(
            profile.payment_method if profile is not None else "cash"
        )
        rows.append(
            {
                "user": employee,
                "profile": profile,
                "is_draft": is_draft_user(employee),
                "compensation_type": compensation_type,
                "is_paid": compensation_type != COMPENSATION_TYPE_UNPAID,
                "can_edit_hourly": compensation_type == COMPENSATION_TYPE_HOURLY,
                "can_edit_salary": compensation_type == COMPENSATION_TYPE_MONTHLY,
                "rate_value": _format_money_dollars(rate_cents),
                "has_rate": rate_cents is not None,
                "salary_value": _format_money_dollars(salary_cents),
                "has_salary": salary_cents is not None,
                "monthly_pay_day": pay_day or "",
                "monthly_pay_date_label": _format_date_label(
                    _next_monthly_pay_date(clockify_today(), pay_day)
                ),
                "payment_method": payment_method,
            }
        )
    rows.sort(key=lambda row: row["compensation_type"] == COMPENSATION_TYPE_UNPAID)
    return rows


def _pay_rate_scope_user_ids(session: Session, *, include_inactive: bool = False) -> set[int]:
    stmt = select(User.id)
    if not include_inactive:
        stmt = stmt.where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
    return {int(user_id) for user_id in session.exec(stmt).all() if user_id is not None}


def _employee_in_payroll_scope(user: User) -> bool:
    return bool(user.is_active or is_draft_user(user))


def _employee_active_on(user: User, profile: Optional[EmployeeProfile], day: date) -> bool:
    if not _employee_in_payroll_scope(user):
        return False
    if profile is not None:
        if profile.hire_date and day < profile.hire_date:
            return False
        if profile.termination_date and day > profile.termination_date:
            return False
    return True


def _salary_cost_for_period(
    *,
    salary_cents: int,
    user: User,
    profile: EmployeeProfile,
    start_day: date,
    end_day: date,
) -> int:
    if salary_cents <= 0 or end_day < start_day:
        return 0
    total = Decimal("0")
    cursor = start_day
    while cursor <= end_day:
        if _employee_active_on(user, profile, cursor):
            days_in_month = monthrange(cursor.year, cursor.month)[1]
            total += Decimal(salary_cents) / Decimal(days_in_month)
        cursor += timedelta(days=1)
    return int(total.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _cents_for_minutes(minutes: int, rate_cents: int) -> int:
    if minutes <= 0 or rate_cents <= 0:
        return 0
    amount = (Decimal(minutes) / Decimal(60)) * Decimal(rate_cents)
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _payroll_cost_summary(session: Session, *, today: Optional[date] = None) -> dict:
    today = today or clockify_today()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    periods = {
        "today": {"label": "Today", "start": today, "end": today},
        "week_to_date": {"label": "Week to date", "start": week_start, "end": today},
        "month_to_date": {"label": "Month to date", "start": month_start, "end": today},
    }

    users = [
        row
        for row in session.exec(select(User).order_by(User.display_name, User.username)).all()
        if row.id is not None and _employee_in_payroll_scope(row)
    ]
    user_ids = [row.id for row in users if row.id is not None]
    profiles: dict[int, EmployeeProfile] = {}
    if user_ids:
        profiles = {
            row.user_id: row
            for row in session.exec(
                select(EmployeeProfile).where(EmployeeProfile.user_id.in_(user_ids))
            ).all()
        }

    totals = {
        key: {"salary_cents": 0, "hourly_cents": 0, "total_cents": 0}
        for key in periods
    }
    monthly_salary_commitment_cents = 0
    salaried_count = 0
    hourly_count = 0
    unpaid_count = 0
    missing_hourly_rate_ids: set[int] = set()
    missing_salary_ids: set[int] = set()

    for user in users:
        profile = profiles.get(user.id or 0)
        compensation_type = _normalize_compensation_type(
            profile.compensation_type if profile is not None else ""
        )
        if compensation_type == COMPENSATION_TYPE_UNPAID:
            unpaid_count += 1
            continue
        if compensation_type == COMPENSATION_TYPE_MONTHLY:
            salaried_count += 1
            salary_cents = _decrypt_monthly_salary_cents(profile)
            if salary_cents is None:
                missing_salary_ids.add(user.id or 0)
                continue
            monthly_salary_commitment_cents += salary_cents
            for key, period in periods.items():
                totals[key]["salary_cents"] += _salary_cost_for_period(
                    salary_cents=salary_cents,
                    user=user,
                    profile=profile,
                    start_day=period["start"],
                    end_day=period["end"],
                )
        else:
            hourly_count += 1
            if _decrypt_hourly_rate_cents(profile) is None:
                missing_hourly_rate_ids.add(user.id or 0)

    hourly_rates: dict[int, int] = {}
    for user_id, profile in profiles.items():
        if _normalize_compensation_type(profile.compensation_type or "") != COMPENSATION_TYPE_HOURLY:
            continue
        rate_cents = _decrypt_hourly_rate_cents(profile)
        if rate_cents is not None:
            hourly_rates[user_id] = rate_cents

    shift_rows: list[ShiftEntry] = []
    if user_ids:
        shift_rows = list(
            session.exec(
                select(ShiftEntry)
                .where(ShiftEntry.user_id.in_(user_ids))
                .where(ShiftEntry.shift_date >= month_start)
                .where(ShiftEntry.shift_date <= today)
            ).all()
        )

    from .team_admin_employees_timecards import _parse_shift_ranges

    labor_kinds = {SHIFT_KIND_WORK, SHIFT_KIND_ALL}
    for shift in shift_rows:
        rate_cents = hourly_rates.get(shift.user_id)
        user = next((row for row in users if row.id == shift.user_id), None)
        profile = profiles.get(shift.user_id)
        if (
            rate_cents is None
            or user is None
            or not _employee_active_on(user, profile, shift.shift_date)
            or (shift.kind or "") not in labor_kinds
        ):
            continue
        ranges = _parse_shift_ranges(shift.label or "")
        minutes = sum(max(0, end - start) for start, end in ranges)
        cost_cents = _cents_for_minutes(minutes, rate_cents)
        if cost_cents <= 0:
            continue
        for key, period in periods.items():
            if period["start"] <= shift.shift_date <= period["end"]:
                totals[key]["hourly_cents"] += cost_cents

    period_rows = []
    for key, period in periods.items():
        salary_cents = totals[key]["salary_cents"]
        hourly_cents = totals[key]["hourly_cents"]
        total_cents = salary_cents + hourly_cents
        totals[key]["total_cents"] = total_cents
        period_rows.append(
            {
                "key": key,
                "label": period["label"],
                "range_label": (
                    _format_date_label(period["start"])
                    if period["start"] == period["end"]
                    else f"{_format_date_label(period['start'])} - {_format_date_label(period['end'])}"
                ),
                "total_label": _format_dollars(total_cents),
                "salary_label": _format_dollars(salary_cents),
                "hourly_label": _format_dollars(hourly_cents),
            }
        )

    return {
        "periods": period_rows,
        "monthly_salary_commitment_label": _format_dollars(
            monthly_salary_commitment_cents
        ),
        "salaried_count": salaried_count,
        "hourly_count": hourly_count,
        "unpaid_count": unpaid_count,
        "missing_hourly_rate_count": len(missing_hourly_rate_ids),
        "missing_salary_count": len(missing_salary_ids),
        "basis_label": "Salary prorated by calendar day + scheduled hourly shifts",
    }


@router.get("/team/admin/employees/pay-rates", response_class=HTMLResponse)
def admin_employee_pay_rates_page(
    request: Request,
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    show_inactive: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    include_inactive = show_inactive in ("1", "true", "yes", "on")
    rows = _pay_rate_rows(session, include_inactive=include_inactive)
    return templates.TemplateResponse(
        request,
        "team/admin/employee_pay_rates.html",
        {
            "request": request,
            "title": "Compensation",
            "current_user": current,
            "rows": rows,
            "compensation_types": COMPENSATION_TYPES,
            "compensation_type_labels": COMPENSATION_TYPE_LABELS,
            "payment_methods": PAYMENT_METHODS,
            "payment_method_labels": PAYMENT_METHOD_LABELS,
            "today_iso": clockify_today().isoformat(),
            "show_inactive": include_inactive,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/employees/pay-rates",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_pay_rates_post(
    request: Request,
    show_inactive: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employee_roster.edit")
    if denial:
        return denial
    if not has_permission(session, current, "admin.labor_financials.view"):
        return HTMLResponse(
            "You do not have permission to view compensation.",
            status_code=403,
        )

    form = await request.form()
    now = utcnow()
    effective_date = _parse_compensation_effective_date(
        str(form.get("effective_date") or ""),
        now.date(),
    )
    user_ids: set[int] = set()
    for key in form.keys():
        if (
            key.startswith("comp_")
            or key.startswith("rate_")
            or key.startswith("salary_")
            or key.startswith("pay_day_")
            or key.startswith("payment_")
        ):
            try:
                suffix = key[8:] if key.startswith("pay_day_") else key.split("_", 1)[1]
                user_ids.add(int(suffix))
            except (IndexError, ValueError):
                continue

    include_inactive = show_inactive in ("1", "true", "yes", "on")
    allowed_user_ids = _pay_rate_scope_user_ids(
        session,
        include_inactive=include_inactive,
    )
    submitted_out_of_scope = sorted(user_ids - allowed_user_ids)
    if submitted_out_of_scope:
        return HTMLResponse(
            "Submitted employee is outside your editable payroll scope.",
            status_code=400,
        )

    changed_user_ids: set[int] = set()
    history_updates = 0
    compensation_changes = 0
    rate_changes = 0
    salary_changes = 0
    cleared_rates = 0
    cleared_salaries = 0
    pay_day_changes = 0
    payment_changes = 0
    invalid_rates = 0
    invalid_salaries = 0
    invalid_pay_days = 0

    for user_id in sorted(user_ids):
        employee = session.get(User, user_id)
        if employee is None:
            continue
        profile = session.get(EmployeeProfile, user_id)
        if profile is None:
            profile = EmployeeProfile(user_id=user_id)
            session.add(profile)
            session.flush()
        before_signature = _compensation_signature_from_profile(profile)

        comp_key = f"comp_{user_id}"
        if comp_key in form:
            comp_raw = str(form.get(comp_key) or "")
            new_compensation = _normalize_compensation_type(comp_raw)
            if new_compensation != _normalize_compensation_type(
                profile.compensation_type or ""
            ):
                profile.compensation_type = new_compensation
                compensation_changes += 1
                changed_user_ids.add(user_id)

        effective_compensation = _normalize_compensation_type(
            profile.compensation_type or ""
        )

        rate_key = f"rate_{user_id}"
        if rate_key in form and effective_compensation == COMPENSATION_TYPE_HOURLY:
            rate_raw = str(form.get(rate_key) or "").strip()
            parsed_rate, rate_invalid = _parse_hourly_rate_dollars(rate_raw)
            if rate_invalid:
                invalid_rates += 1
            else:
                current_rate = _decrypt_hourly_rate_cents(profile)
                if parsed_rate is None:
                    if profile.hourly_rate_cents_enc:
                        profile.hourly_rate_cents_enc = None
                        cleared_rates += 1
                        rate_changes += 1
                        changed_user_ids.add(user_id)
                elif parsed_rate != current_rate:
                    profile.hourly_rate_cents_enc = encrypt_pii(str(parsed_rate))
                    rate_changes += 1
                    changed_user_ids.add(user_id)

        salary_key = f"salary_{user_id}"
        if salary_key in form and effective_compensation == COMPENSATION_TYPE_MONTHLY:
            salary_raw = str(form.get(salary_key) or "").strip()
            parsed_salary, salary_invalid = _parse_monthly_salary_dollars(salary_raw)
            if salary_invalid:
                invalid_salaries += 1
            else:
                current_salary = _decrypt_monthly_salary_cents(profile)
                if parsed_salary is None:
                    if profile.monthly_salary_cents_enc:
                        profile.monthly_salary_cents_enc = None
                        cleared_salaries += 1
                        salary_changes += 1
                        changed_user_ids.add(user_id)
                elif parsed_salary != current_salary:
                    profile.monthly_salary_cents_enc = encrypt_pii(str(parsed_salary))
                    salary_changes += 1
                    changed_user_ids.add(user_id)

        pay_day_key = f"pay_day_{user_id}"
        if pay_day_key in form and effective_compensation == COMPENSATION_TYPE_MONTHLY:
            pay_day_raw = str(form.get(pay_day_key) or "").strip()
            parsed_pay_day, pay_day_invalid = _parse_monthly_pay_day(pay_day_raw)
            if pay_day_invalid:
                invalid_pay_days += 1
            elif parsed_pay_day != profile.monthly_salary_pay_day:
                profile.monthly_salary_pay_day = parsed_pay_day
                pay_day_changes += 1
                changed_user_ids.add(user_id)

        payment_key = f"payment_{user_id}"
        if payment_key in form and effective_compensation != COMPENSATION_TYPE_UNPAID:
            method_raw = str(form.get(payment_key) or "")
            new_method = _normalize_payment_method(method_raw)
            if new_method != _normalize_payment_method(profile.payment_method or ""):
                profile.payment_method = new_method
                payment_changes += 1
                changed_user_ids.add(user_id)

        if user_id in changed_user_ids:
            profile.updated_at = now
            session.add(profile)
            if record_compensation_history_if_changed(
                session,
                profile=profile,
                before_signature=before_signature,
                effective_date=effective_date,
                current_user=current,
                source="bulk_pay_rates",
            ):
                history_updates += 1

    if changed_user_ids:
        _audit_then_commit(
            session,
            AuditLog(
                actor_user_id=current.id,
                action="admin.pay_rates.bulk_update",
                resource_key="admin.employee_roster.edit",
                details_json=json.dumps(
                    {
                        "updated_count": len(changed_user_ids),
                        "compensation_type_changes": compensation_changes,
                        "rate_changes": rate_changes,
                        "monthly_salary_changes": salary_changes,
                        "cleared_rates": cleared_rates,
                        "cleared_monthly_salaries": cleared_salaries,
                        "monthly_pay_day_changes": pay_day_changes,
                        "payment_method_changes": payment_changes,
                        "compensation_history_updates": history_updates,
                        "effective_date": effective_date.isoformat(),
                        "invalid_rates": invalid_rates,
                        "invalid_monthly_salaries": invalid_salaries,
                        "invalid_monthly_pay_days": invalid_pay_days,
                        "user_ids": sorted(changed_user_ids),
                    },
                    sort_keys=True,
                ),
                ip_address=(request.client.host if request.client else None),
            ),
        )

    from urllib.parse import urlencode

    qs = {
        "flash": (
            f"Saved {len(changed_user_ids)} employee(s). "
            f"{compensation_changes} pay type change(s), "
            f"{rate_changes + salary_changes} amount change(s), "
            f"{pay_day_changes} pay date change(s), "
            f"{payment_changes} payment method change(s). "
            f"Effective {effective_date.isoformat()}."
        )
    }
    if invalid_rates or invalid_salaries or invalid_pay_days:
        qs["error"] = (
            f"{invalid_rates + invalid_salaries + invalid_pay_days} invalid compensation value(s) "
            "were ignored."
        )
    if include_inactive:
        qs["show_inactive"] = "1"
    return RedirectResponse(
        "/team/admin/employees/pay-rates?" + urlencode(qs),
        status_code=303,
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
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"reveal:{current.id}",
        max_requests=30,
        window_seconds=900,
    ):
        return limited
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


def _clamp_hourly_rate_cents(value: str) -> tuple[Optional[int], bool]:
    value = (value or "").strip()
    if not value:
        return None, False
    if not value.isdigit():
        return None, True
    parsed = int(value)
    return min(max(parsed, 0), 1_000_000), False


def _parse_profile_hourly_rate(
    *,
    hourly_rate_dollars: str,
    hourly_rate_cents: str,
) -> tuple[Optional[int], bool]:
    if (hourly_rate_dollars or "").strip():
        return _parse_hourly_rate_dollars(hourly_rate_dollars)
    # Backward compatibility for older tests/tools that still submit the
    # internal cents field. The manager UI no longer renders this name.
    return _clamp_hourly_rate_cents(hourly_rate_cents)


@router.post(
    "/team/admin/employees/{user_id}/profile-update",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_profile_update(
    request: Request,
    user_id: int,
    role: str = Form(default=""),
    display_name: str = Form(default=""),
    staff_kind: str = Form(default=""),
    is_schedulable: str = Form(default=""),
    compensation_type: str = Form(default=""),
    compensation_effective_date: str = Form(default=""),
    hourly_rate_dollars: str = Form(default=""),
    hourly_rate_cents: str = Form(default=""),
    monthly_salary_dollars: str = Form(default=""),
    monthly_salary_pay_day: str = Form(default=""),
    payment_method: str = Form(default=""),
    hire_date: str = Form(default=""),
    termination_date: str = Form(default=""),
    clockify_user_id: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    can_edit_profile = has_permission(session, current, "admin.employees.edit")
    can_edit_schedule_roster = has_permission(
        session,
        current,
        "admin.employee_roster.edit",
    )
    if not can_edit_profile:
        return _admin_denied_response(request, session, current)
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
    can_manage_compensation = can_edit_profile and has_permission(
        session,
        current,
        "admin.labor_financials.view",
    )
    before_compensation_signature = (
        _compensation_signature_from_profile(profile)
        if can_manage_compensation
        else None
    )
    effective_date = _parse_compensation_effective_date(
        compensation_effective_date,
        now.date(),
    ) if can_manage_compensation else now.date()

    if can_edit_profile:
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

    if can_edit_profile or can_edit_schedule_roster:
        new_kind = (staff_kind or "").strip().lower()
        if new_kind in STAFF_KINDS and new_kind != (employee.staff_kind or ""):
            employee.staff_kind = new_kind
            employee.updated_at = now
            session.add(employee)
            changed.append("staff_kind")
        new_schedulable = is_schedulable in ("1", "true", "yes", "on")
        if employee.is_schedulable != new_schedulable:
            employee.is_schedulable = new_schedulable
            employee.updated_at = now
            session.add(employee)
            changed.append("is_schedulable")

    rate_invalid = False
    salary_invalid = False
    pay_day_invalid = False
    if can_manage_compensation:
        if (compensation_type or "").strip():
            new_compensation = _normalize_compensation_type(compensation_type)
            if new_compensation != _normalize_compensation_type(
                profile.compensation_type or ""
            ):
                profile.compensation_type = new_compensation
                changed.append("compensation_type")

        effective_compensation = _normalize_compensation_type(
            profile.compensation_type or ""
        )

        if effective_compensation == COMPENSATION_TYPE_HOURLY:
            rate_int, rate_invalid = _parse_profile_hourly_rate(
                hourly_rate_dollars=hourly_rate_dollars,
                hourly_rate_cents=hourly_rate_cents,
            )
        else:
            rate_int = None
        if rate_int is not None:
            from ..pii import decrypt_pii, encrypt_pii
            try:
                current_rate = decrypt_pii(profile.hourly_rate_cents_enc) or ""
            except ValueError:
                current_rate = ""
            if str(rate_int) != current_rate:
                profile.hourly_rate_cents_enc = encrypt_pii(str(rate_int))
                changed.append("hourly_rate_cents")

        if effective_compensation == COMPENSATION_TYPE_MONTHLY:
            salary_int, salary_invalid = _parse_monthly_salary_dollars(
                monthly_salary_dollars
            )
            if salary_int is not None:
                from ..pii import encrypt_pii

                current_salary = _decrypt_monthly_salary_cents(profile)
                if salary_int != current_salary:
                    profile.monthly_salary_cents_enc = encrypt_pii(str(salary_int))
                    changed.append("monthly_salary_cents")

            pay_day_int, pay_day_invalid = _parse_monthly_pay_day(
                monthly_salary_pay_day
            )
            if (monthly_salary_pay_day or "").strip() and not pay_day_invalid:
                if pay_day_int != profile.monthly_salary_pay_day:
                    profile.monthly_salary_pay_day = pay_day_int
                    changed.append("monthly_salary_pay_day")

        if (
            (payment_method or "").strip()
            and effective_compensation != COMPENSATION_TYPE_UNPAID
        ):
            new_payment_method = _normalize_payment_method(payment_method)
            if new_payment_method != _normalize_payment_method(
                profile.payment_method or ""
            ):
                profile.payment_method = new_payment_method
                changed.append("payment_method")

    if can_edit_profile:
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
        history_recorded = False
        if any(field in COMPENSATION_HISTORY_FIELDS for field in changed):
            history_recorded = record_compensation_history_if_changed(
                session,
                profile=profile,
                before_signature=before_compensation_signature,
                effective_date=effective_date,
                current_user=current,
                source="profile_update",
            )
        profile.updated_at = now
        session.add(profile)
        details = {"fields": changed}
        if history_recorded:
            details["compensation_effective_date"] = effective_date.isoformat()
        _audit_then_commit(
            session,
            AuditLog(
                actor_user_id=current.id,
                target_user_id=user_id,
                action="admin.profile_update",
                resource_key=(
                    "admin.employees.edit"
                    if can_edit_profile
                    else "admin.employee_roster.edit"
                ),
                details_json=json.dumps(details, sort_keys=True),
                ip_address=(request.client.host if request.client else None),
            ),

        )
    flash = "Saved."
    if rate_invalid and not salary_invalid and not pay_day_invalid:
        flash = "Saved.+Invalid+hourly+rate+ignored."
    elif salary_invalid or rate_invalid or pay_day_invalid:
        flash = "Saved.+Invalid+compensation+value+ignored."
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash={flash}", status_code=303
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

    Non-empty sensitive writes also require reveal authority. That keeps
    "can edit general employee metadata" distinct from "may handle live
    PII at all" — blank submissions still behave as no-ops and do not
    require reveal authority.
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
    sensitive_write_requested = any(
        (
            (legal_name or "").strip(),
            (email or "").strip(),
            (phone or "").strip(),
            (emergency_contact_name or "").strip(),
            (emergency_contact_phone or "").strip(),
            (address_street or "").strip(),
            (address_city or "").strip(),
            (address_state or "").strip(),
            (address_zip or "").strip(),
        )
    )
    if sensitive_write_requested:
        reveal_denial, _ = _admin_gate(request, session, "admin.employees.reveal_pii")
        if reveal_denial:
            return reveal_denial

    def _field_fingerprint(label: str, value: str) -> dict[str, object]:
        cleaned = (value or "").strip().lower()
        digest = hashlib.sha256(f"{label}:{cleaned}".encode("utf-8")).hexdigest()
        return {"present": bool(cleaned), "len": len(cleaned), "sha256_12": digest[:12]}

    field_fingerprints: dict[str, str] = {}

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
            field_fingerprints[label] = _field_fingerprint(label, v)["sha256_12"]

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
            field_fingerprints["email"] = _field_fingerprint("email", new_email)["sha256_12"]

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
            field_fingerprints["address"] = _field_fingerprint(
                "address", json.dumps(new_address, sort_keys=True)
            )["sha256_12"]

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
                details_json=json.dumps(
                    {
                        "fields": changed,
                        "fingerprints": {
                            "legal_name": _field_fingerprint("legal_name", legal_name),
                            "email": _field_fingerprint("email", new_email),
                            "phone": _field_fingerprint("phone", phone),
                            "emergency_contact_name": _field_fingerprint(
                                "emergency_contact_name", emergency_contact_name
                            ),
                            "emergency_contact_phone": _field_fingerprint(
                                "emergency_contact_phone", emergency_contact_phone
                            ),
                            "address": _field_fingerprint(
                                "address",
                                "|".join(
                                    [
                                        new_address["street"],
                                        new_address["city"],
                                        new_address["state"],
                                        new_address["zip"],
                                    ]
                                ),
                            ),
                        },
                    }
                ),
                ip_address=(request.client.host if request.client else None),
            ),
        )
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
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"admin_reset:{current.id}",
        max_requests=20,
        window_seconds=900,
    ):
        return limited
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
    if current.id == user_id:
        return _admin_self_action_rejected("terminate")
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
    if current.id == user_id:
        return _admin_self_action_rejected("terminate")
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    now = utcnow()
    invite_revoked, reset_revoked = _revoke_employee_tokens(session, user_id, now)
    employee.is_active = False
    employee.session_invalidated_at = now
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
            details_json=json.dumps(
                {
                    "username": employee.username,
                    "invite_tokens_revoked": invite_revoked,
                    "reset_tokens_revoked": reset_revoked,
                },
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        ),
    )
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=Terminated.", status_code=303
    )


_PURGE_PROFILE_BLOB_FIELDS = (
    "legal_name_enc",
    "phone_enc",
    "address_enc",
    "emergency_contact_name_enc",
    "emergency_contact_phone_enc",
    "email_ciphertext",
    "hourly_rate_cents_enc",
    "monthly_salary_cents_enc",
)


def _encode_blob(value: Optional[bytes]) -> Optional[str]:
    if not value:
        return None
    return base64.b64encode(value).decode("ascii")


def _decode_blob(value: Optional[str]) -> Optional[bytes]:
    if not value:
        return None
    return base64.b64decode(value.encode("ascii"))


def _iso_datetime(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _iso_date(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _employee_purge_snapshot(
    employee: User, profile: Optional[EmployeeProfile]
) -> dict:
    profile_snapshot: Optional[dict] = None
    if profile is not None:
        profile_snapshot = {
            "email_lookup_hash": profile.email_lookup_hash,
            "hire_date": _iso_date(profile.hire_date),
            "termination_date": _iso_date(profile.termination_date),
            "compensation_type": profile.compensation_type,
            "monthly_salary_pay_day": profile.monthly_salary_pay_day,
            "payment_method": profile.payment_method,
            "clockify_user_id": profile.clockify_user_id,
            "onboarding_completed_at": _iso_datetime(profile.onboarding_completed_at),
            "policies_acknowledged_at": _iso_datetime(profile.policies_acknowledged_at),
            "created_at": _iso_datetime(profile.created_at),
            "updated_at": _iso_datetime(profile.updated_at),
        }
        for field_name in _PURGE_PROFILE_BLOB_FIELDS:
            profile_snapshot[field_name] = _encode_blob(getattr(profile, field_name))

    return {
        "version": 1,
        "user": {
            "username": employee.username,
            "password_hash": employee.password_hash,
            "password_salt": employee.password_salt,
            "display_name": employee.display_name,
            "role": employee.role,
            "is_active": employee.is_active,
            "is_schedulable": employee.is_schedulable,
            "staff_kind": employee.staff_kind,
            "password_changed_at": _iso_datetime(employee.password_changed_at),
            "session_invalidated_at": _iso_datetime(employee.session_invalidated_at),
            "created_at": _iso_datetime(employee.created_at),
            "updated_at": _iso_datetime(employee.updated_at),
        },
        "profile": profile_snapshot,
    }


def _active_purge_tombstone(
    session: Session, user_id: int, now: datetime
) -> Optional[EmployeePurgeTombstone]:
    return session.exec(
        select(EmployeePurgeTombstone)
        .where(
            EmployeePurgeTombstone.user_id == user_id,
            EmployeePurgeTombstone.restored_at.is_(None),
            EmployeePurgeTombstone.restore_until >= now,
        )
        .order_by(EmployeePurgeTombstone.created_at.desc())
    ).first()


def _ensure_purge_tombstone(
    session: Session,
    *,
    employee: User,
    profile: Optional[EmployeeProfile],
    purged_by_user_id: Optional[int],
    now: datetime,
) -> EmployeePurgeTombstone:
    existing = _active_purge_tombstone(session, employee.id or 0, now)
    if existing is not None:
        return existing
    tombstone = EmployeePurgeTombstone(
        user_id=employee.id or 0,
        purged_by_user_id=purged_by_user_id,
        restore_until=now + PURGE_RESTORE_WINDOW,
        snapshot_json=json.dumps(
            _employee_purge_snapshot(employee, profile),
            sort_keys=True,
        ),
        created_at=now,
    )
    session.add(tombstone)
    session.flush()
    return tombstone


def restore_employee_purge_tombstone(
    session: Session,
    user_id: int,
    *,
    actor_user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> User:
    now = utcnow()
    tombstone = _active_purge_tombstone(session, user_id, now)
    if tombstone is None:
        raise ValueError("purge_tombstone_unavailable")
    employee = session.get(User, user_id)
    if employee is None:
        raise ValueError("employee_not_found")
    try:
        payload = json.loads(tombstone.snapshot_json or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError("purge_tombstone_invalid") from exc

    user_snapshot = payload.get("user") or {}
    employee.username = str(user_snapshot.get("username") or employee.username)
    employee.password_hash = str(user_snapshot.get("password_hash") or "")
    employee.password_salt = str(user_snapshot.get("password_salt") or "")
    employee.display_name = str(user_snapshot.get("display_name") or "")
    employee.role = str(user_snapshot.get("role") or "employee")
    employee.is_active = bool(user_snapshot.get("is_active", False))
    employee.is_schedulable = bool(user_snapshot.get("is_schedulable", False))
    employee.staff_kind = str(user_snapshot.get("staff_kind") or "storefront")
    employee.password_changed_at = _parse_iso_datetime(
        user_snapshot.get("password_changed_at")
    )
    employee.session_invalidated_at = now
    employee.created_at = (
        _parse_iso_datetime(user_snapshot.get("created_at")) or employee.created_at
    )
    employee.updated_at = now
    session.add(employee)

    profile_snapshot = payload.get("profile")
    if profile_snapshot:
        profile = session.get(EmployeeProfile, user_id) or EmployeeProfile(
            user_id=user_id
        )
        profile.email_lookup_hash = profile_snapshot.get("email_lookup_hash")
        profile.hire_date = _parse_iso_date(profile_snapshot.get("hire_date"))
        profile.termination_date = _parse_iso_date(
            profile_snapshot.get("termination_date")
        )
        profile.compensation_type = str(
            profile_snapshot.get("compensation_type") or "hourly"
        )
        profile.monthly_salary_pay_day = profile_snapshot.get("monthly_salary_pay_day")
        profile.payment_method = str(profile_snapshot.get("payment_method") or "cash")
        profile.clockify_user_id = profile_snapshot.get("clockify_user_id")
        profile.onboarding_completed_at = _parse_iso_datetime(
            profile_snapshot.get("onboarding_completed_at")
        )
        profile.policies_acknowledged_at = _parse_iso_datetime(
            profile_snapshot.get("policies_acknowledged_at")
        )
        profile.created_at = (
            _parse_iso_datetime(profile_snapshot.get("created_at"))
            or profile.created_at
        )
        profile.updated_at = now
        for field_name in _PURGE_PROFILE_BLOB_FIELDS:
            setattr(profile, field_name, _decode_blob(profile_snapshot.get(field_name)))
        session.add(profile)

    tombstone.restored_at = now
    session.add(tombstone)
    session.add(
        AuditLog(
            actor_user_id=actor_user_id,
            target_user_id=user_id,
            action="account.purge_restored",
            resource_key="admin.employees.purge",
            details_json=json.dumps({"tombstone_id": tombstone.id}, sort_keys=True),
            ip_address=ip_address,
        )
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise ValueError("purge_restore_conflict") from exc
    session.refresh(employee)
    return employee


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
    if current.id == user_id:
        return _admin_self_action_rejected("purge")
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
    if current.id == user_id:
        return _admin_self_action_rejected("purge")
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    expected_username = employee.username
    if (confirm_username or "").strip() != expected_username:
        return HTMLResponse(
            "Confirmation did not match the employee username.",
            status_code=400,
        )
    profile = session.get(EmployeeProfile, user_id)
    now = utcnow()
    invite_revoked, reset_revoked = _revoke_employee_tokens(session, user_id, now)
    tombstone = _ensure_purge_tombstone(
        session,
        employee=employee,
        profile=profile,
        purged_by_user_id=current.id,
        now=now,
    )
    if profile is not None:
        for attr in (
            "legal_name_enc",
            "phone_enc",
            "address_enc",
            "emergency_contact_name_enc",
            "emergency_contact_phone_enc",
            "email_ciphertext",
            "hourly_rate_cents_enc",
            "monthly_salary_cents_enc",
        ):
            setattr(profile, attr, None)
        profile.email_lookup_hash = None
        profile.compensation_type = "hourly"
        profile.monthly_salary_pay_day = None
        profile.clockify_user_id = None
        profile.updated_at = now
        session.add(profile)
    employee.is_active = False
    employee.session_invalidated_at = now
    employee.username = f"purged+{employee.id}@anonymized.local"
    employee.password_hash = "__purged_password_hash__"
    employee.password_salt = "__purged_password_salt__"
    employee.display_name = ""
    employee.updated_at = now
    session.add(employee)
    _audit_then_commit(
        session,
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="account.purged",
            resource_key="admin.employees.purge",
            details_json=json.dumps(
                {
                    "username": employee.username,
                    "invite_tokens_revoked": invite_revoked,
                    "reset_tokens_revoked": reset_revoked,
                    "tombstone_id": tombstone.id,
                    "restore_until": tombstone.restore_until.isoformat(),
                },
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        ),
    )
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=PII+purged.", status_code=303
    )


@router.post(
    "/team/admin/employees/{user_id}/purge/undo",
    dependencies=[Depends(require_csrf)],
)
async def admin_employee_purge_undo_post(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.employees.purge")
    if denial:
        return denial
    try:
        restore_employee_purge_tombstone(
            session,
            user_id,
            actor_user_id=current.id,
            ip_address=(request.client.host if request.client else None),
        )
    except ValueError as exc:
        message = {
            "purge_tombstone_unavailable": "Purge undo window has expired or was already used.",
            "employee_not_found": "Employee not found.",
            "purge_tombstone_invalid": "Purge undo snapshot could not be read.",
            "purge_restore_conflict": (
                "Purge undo could not restore this account because a unique value "
                "is already in use."
            ),
        }.get(str(exc), "Purge undo failed.")
        return HTMLResponse(message, status_code=400)
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=Purge+undone.", status_code=303
    )
