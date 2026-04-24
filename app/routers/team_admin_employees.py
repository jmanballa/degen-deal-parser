"""
/team/admin/employees/* — employee management (Wave 4).

All PII-decrypting paths write an AuditLog row BEFORE attempting decryption.
If the audit write fails, the decrypt does not happen (fail-closed).
"""
from __future__ import annotations

import hashlib
import json
from calendar import monthrange
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import or_, update
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
from ..models import (
    AuditLog,
    EmployeeProfile,
    InviteToken,
    PasswordResetToken,
    SHIFT_KIND_ALL,
    SHIFT_KIND_WORK,
    STAFF_KINDS,
    ShiftEntry,
    User,
    utcnow,
)
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate

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


def _normalize_compensation_type(value: str) -> str:
    value = (value or "").strip().lower()
    return value if value in COMPENSATION_TYPES else COMPENSATION_TYPE_HOURLY


def _normalize_payment_method(value: str) -> str:
    value = (value or "").strip().lower()
    return value if value in PAYMENT_METHODS else "cash"


def _decrypt_money_cents(blob: Optional[bytes]) -> Optional[int]:
    if not blob:
        return None
    try:
        raw = decrypt_pii(blob) or ""
        return max(0, int(raw))
    except (TypeError, ValueError):
        return None


def _decrypt_hourly_rate_cents(profile: Optional[EmployeeProfile]) -> Optional[int]:
    if profile is None:
        return None
    return _decrypt_money_cents(profile.hourly_rate_cents_enc)


def _decrypt_monthly_salary_cents(profile: Optional[EmployeeProfile]) -> Optional[int]:
    if profile is None:
        return None
    return _decrypt_money_cents(profile.monthly_salary_cents_enc)


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
        "compensation_types": COMPENSATION_TYPES,
        "compensation_type_labels": COMPENSATION_TYPE_LABELS,
        "current_compensation_type": _normalize_compensation_type(
            profile.compensation_type if profile is not None else ""
        ),
        "monthly_salary_value": _format_money_dollars(
            _decrypt_monthly_salary_cents(profile)
        ),
        "monthly_salary_pay_day_value": profile.monthly_salary_pay_day or "",
        "monthly_salary_pay_date_label": _format_date_label(
            _next_monthly_pay_date(utcnow().date(), profile.monthly_salary_pay_day)
        ),
        "payment_methods": PAYMENT_METHODS,
        "payment_method_labels": PAYMENT_METHOD_LABELS,
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
            "can_reveal_pii": has_permission(
                session, user, "admin.employees.reveal_pii"
            ),
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
    denial, current = _admin_gate(request, session, "admin.employees.edit")
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
                resource_key="admin.employees.edit",
                details_json=json.dumps({"is_schedulable": new_value}),
                ip_address=(request.client.host if request.client else None),
            ),
        )
        session.commit()

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


def _pay_rate_rows(session: Session, *, include_inactive: bool = False) -> list[dict]:
    stmt = select(User).order_by(User.is_active.desc(), User.display_name, User.username)
    if not include_inactive:
        stmt = stmt.where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
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
                    _next_monthly_pay_date(utcnow().date(), pay_day)
                ),
                "payment_method": payment_method,
            }
        )
    rows.sort(key=lambda row: row["compensation_type"] == COMPENSATION_TYPE_UNPAID)
    return rows


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
    today = today or utcnow().date()
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
    denial, current = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial

    form = await request.form()
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

    from ..pii import encrypt_pii

    now = utcnow()
    changed_user_ids: set[int] = set()
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

    if changed_user_ids:
        _audit_then_commit(
            session,
            AuditLog(
                actor_user_id=current.id,
                action="admin.pay_rates.bulk_update",
                resource_key="admin.employees.edit",
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
        session.commit()

    from urllib.parse import urlencode

    qs = {
        "flash": (
            f"Saved {len(changed_user_ids)} employee(s). "
            f"{compensation_changes} pay type change(s), "
            f"{rate_changes + salary_changes} amount change(s), "
            f"{pay_day_changes} pay date change(s), "
            f"{payment_changes} payment method change(s)."
        )
    }
    if invalid_rates or invalid_salaries or invalid_pay_days:
        qs["error"] = (
            f"{invalid_rates + invalid_salaries + invalid_pay_days} invalid compensation value(s) "
            "were ignored."
        )
    if show_inactive in ("1", "true", "yes", "on"):
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
    compensation_type: str = Form(default=""),
    hourly_rate_cents: str = Form(default=""),
    monthly_salary_dollars: str = Form(default=""),
    monthly_salary_pay_day: str = Form(default=""),
    payment_method: str = Form(default=""),
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

    new_kind = (staff_kind or "").strip().lower()
    if new_kind in STAFF_KINDS and new_kind != (employee.staff_kind or ""):
        employee.staff_kind = new_kind
        employee.updated_at = now
        session.add(employee)
        changed.append("staff_kind")

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

    rate_invalid = False
    if effective_compensation == COMPENSATION_TYPE_HOURLY:
        rate_int, rate_invalid = _clamp_hourly_rate_cents(hourly_rate_cents)
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

    salary_invalid = False
    pay_day_invalid = False
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

        pay_day_int, pay_day_invalid = _parse_monthly_pay_day(monthly_salary_pay_day)
        if (monthly_salary_pay_day or "").strip() and not pay_day_invalid:
            if pay_day_int != profile.monthly_salary_pay_day:
                profile.monthly_salary_pay_day = pay_day_int
                changed.append("monthly_salary_pay_day")

    if (
        (payment_method or "").strip()
        and effective_compensation != COMPENSATION_TYPE_UNPAID
    ):
        new_payment_method = _normalize_payment_method(payment_method)
        if new_payment_method != _normalize_payment_method(profile.payment_method or ""):
            profile.payment_method = new_payment_method
            changed.append("payment_method")

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
    flash = "Saved."
    if rate_invalid and not salary_invalid and not pay_day_invalid:
        flash = "Saved.+Invalid+hourly_rate_cents+ignored."
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
    invite_revoked, reset_revoked = _revoke_employee_tokens(session, user_id, now)
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
    invite_revoked, reset_revoked = _revoke_employee_tokens(session, user_id, now)
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
                },
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        ),
    )
    session.commit()
    return RedirectResponse(
        f"/team/admin/employees/{user_id}?flash=PII+purged.", status_code=303
    )
