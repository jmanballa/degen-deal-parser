"""/team/admin/employees/{id}/timecards - per-employee weekly timecard view.

Combines the scheduled ShiftEntry rows for a week with the employee's
Clockify time entries, computes per-day anomalies (late/early/no-show/
unscheduled/running), and shows an aggregate labor total. Salaried
employees use their fixed monthly amount for the labor tile; Clockify
hours are optional for them.

Wage privacy: individual hourly rates are never placed in the template
context, logs, or response body. Only the aggregate labor total and a
boolean "missing rate" flag leave this module.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..clockify import (
    ClockifyApiError,
    ClockifyConfigError,
    ClockifyEntryView,
    ClockifyWeekSummary,
    build_week_summary,
    clockify_client_from_settings,
    clockify_is_configured,
    format_hours,
)
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import (
    AuditLog,
    EmployeeProfile,
    SHIFT_KIND_ALL,
    SHIFT_KIND_WORK,
    ShiftEntry,
    TimecardApproval,
    User,
    utcnow,
)
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _permission_gate
from .team_admin_employees import (
    compensation_history_rows_for_users,
    compensation_snapshot_for_day,
)


router = APIRouter()


DEFAULT_TZ = "America/Los_Angeles"
LATE_THRESHOLD_MIN = 15
EARLY_LEAVE_THRESHOLD_MIN = 15
VARIANCE_TOLERANCE_MIN = 10
DAILY_OVERTIME_SECONDS = 8 * 3600
WEEKLY_OVERTIME_SECONDS = 40 * 3600
_LABOR_KINDS = {SHIFT_KIND_WORK, SHIFT_KIND_ALL}
_COMPENSATION_MONTHLY_SALARY = "monthly_salary"
_BREAK_KEYWORDS = ("break", "lunch", "meal", "rest")
_TIMECARD_AUDIT_ACTIONS = (
    "admin.timecard.status_update",
    "admin.timecard.bulk_approve",
)
TIMECARD_STATUS_PENDING = "pending"
TIMECARD_STATUS_APPROVED = "approved"
TIMECARD_STATUS_REJECTED = "rejected"
TIMECARD_STATUS_LOCKED = "locked"
TIMECARD_STATUS_VALUES = (
    TIMECARD_STATUS_PENDING,
    TIMECARD_STATUS_APPROVED,
    TIMECARD_STATUS_REJECTED,
    TIMECARD_STATUS_LOCKED,
)
TIMECARD_STATUS_OPTIONS = [
    {"value": TIMECARD_STATUS_PENDING, "label": "Pending"},
    {"value": TIMECARD_STATUS_APPROVED, "label": "Approved"},
    {"value": TIMECARD_STATUS_REJECTED, "label": "Needs fix"},
    {"value": TIMECARD_STATUS_LOCKED, "label": "Locked"},
]
_TIMECARD_STATUS_LABELS = {
    TIMECARD_STATUS_PENDING: "Pending",
    TIMECARD_STATUS_APPROVED: "Approved",
    TIMECARD_STATUS_REJECTED: "Needs fix",
    TIMECARD_STATUS_LOCKED: "Locked",
}
_TIMECARD_STATUS_TONES = {
    TIMECARD_STATUS_PENDING: "info",
    TIMECARD_STATUS_APPROVED: "ok",
    TIMECARD_STATUS_REJECTED: "danger",
    TIMECARD_STATUS_LOCKED: "warn",
}


def _format_month_day(value: date, *, include_year: bool = False) -> str:
    label = f"{value.strftime('%b')} {value.day}"
    if include_year:
        label = f"{label}, {value.year}"
    return label


def _tz(settings=None) -> ZoneInfo:
    settings = settings or get_settings()
    name = (getattr(settings, "clockify_timezone", None) or DEFAULT_TZ).strip() or DEFAULT_TZ
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)


def _week_start_for(value: Optional[str], today: date) -> date:
    if value:
        try:
            parsed = datetime.strptime(value.strip(), "%Y-%m-%d").date()
        except ValueError:
            parsed = today
    else:
        parsed = today
    return parsed - timedelta(days=parsed.weekday())


_TIME_RE = re.compile(
    r"^\s*(?P<h>\d{1,2})(?::(?P<m>\d{2}))?\s*(?P<ap>[ap](?:\.?m\.?)?)?\s*$",
    re.IGNORECASE,
)
_RANGE_SPLIT_RE = re.compile(r"\s*[/,&]\s*")
_NON_SHIFT_TOKENS = {"OFF", "SHOW", "REQUEST", "IF NEEDED", "STREAM"}


def _parse_time_to_minutes(s: str) -> Optional[int]:
    m = _TIME_RE.match(s)
    if not m:
        return None
    h = int(m.group("h"))
    mm = int(m.group("m") or 0)
    ap = (m.group("ap") or "").lower().replace(".", "").replace("m", "")
    if mm > 59 or h > 23:
        return None
    if ap == "p" and h < 12:
        h += 12
    elif ap == "a" and h == 12:
        h = 0
    return h * 60 + mm


def _parse_shift_ranges(label: str) -> list[tuple[int, int]]:
    """Return list of (start_min, end_min) for a shift label.

    End may exceed 1440 to indicate overnight wrap. Returns [] for
    non-shift tokens or anything unparseable.
    """
    if not label:
        return []
    if label.strip().upper() in _NON_SHIFT_TOKENS:
        return []
    out: list[tuple[int, int]] = []
    for part in _RANGE_SPLIT_RE.split(label):
        m = re.match(
            r"^\s*(?P<a>[0-9:.apm\s]+?)\s*[-–—]\s*(?P<b>[0-9:.apm\s]+?)\s*$",
            part,
            re.IGNORECASE,
        )
        if not m:
            continue
        a_raw = m.group("a").strip()
        b_raw = m.group("b").strip()
        a_has_ap = bool(re.search(r"[ap]\.?m?\.?$", a_raw, re.I))
        b_has_ap = bool(re.search(r"[ap]\.?m?\.?$", b_raw, re.I))
        a = _parse_time_to_minutes(a_raw)
        b = _parse_time_to_minutes(b_raw)
        if a is None or b is None:
            continue
        if not a_has_ap and not b_has_ap:
            a_h, b_h = a // 60, b // 60
            if b_h < a_h and a_h <= 11:
                b += 12 * 60
        if b <= a:
            b += 24 * 60
        out.append((a, b))
    return out


def _shift_total_hours(ranges: list[tuple[int, int]]) -> float:
    return round(sum((b - a) for a, b in ranges) / 60.0, 2)


def _fmt_time(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    try:
        return dt.strftime("%-I:%M %p")
    except ValueError:
        return dt.strftime("%I:%M %p").lstrip("0")


def _normalize_compensation_type(profile: Optional[EmployeeProfile]) -> str:
    value = (profile.compensation_type if profile is not None else "") or ""
    value = value.strip().lower()
    if value == _COMPENSATION_MONTHLY_SALARY:
        return _COMPENSATION_MONTHLY_SALARY
    return "hourly"


def _encrypted_cents(blob: Optional[bytes]) -> tuple[int, bool]:
    """Return (cents, missing). Never logs or renders plaintext."""
    if not blob:
        return 0, True
    try:
        raw = decrypt_pii(blob)
    except Exception:
        return 0, True
    raw = (raw or "").strip()
    if not raw:
        return 0, True
    try:
        cents = int(raw)
    except (TypeError, ValueError):
        return 0, True
    return (cents if cents >= 0 else 0), False


def _hourly_rate_cents(profile: Optional[EmployeeProfile]) -> tuple[int, bool]:
    if profile is None:
        return 0, True
    return _encrypted_cents(profile.hourly_rate_cents_enc)


def _monthly_salary_cents(profile: Optional[EmployeeProfile]) -> tuple[int, bool]:
    if profile is None:
        return 0, True
    return _encrypted_cents(profile.monthly_salary_cents_enc)


def _labor_cents(total_seconds: int, rate_cents: int) -> int:
    if rate_cents <= 0 or total_seconds <= 0:
        return 0
    hours = Decimal(total_seconds) / Decimal(3600)
    return int(
        (hours * Decimal(rate_cents)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )


def _format_dollars(cents: int) -> str:
    return f"${cents / 100:,.2f}"


def _entry_is_break(entry: ClockifyEntryView) -> bool:
    text = (entry.description or "").strip().lower()
    if not text:
        return False
    return any(re.search(rf"\b{re.escape(word)}\b", text) for word in _BREAK_KEYWORDS)


def _format_signed_minutes(minutes: int) -> str:
    sign = "+" if minutes > 0 else "-" if minutes < 0 else ""
    total = abs(minutes)
    return f"{sign}{total // 60}:{total % 60:02d}"


def _variance_tone(minutes: Optional[int]) -> str:
    if minutes is None:
        return "info"
    if abs(minutes) <= VARIANCE_TOLERANCE_MIN:
        return "ok"
    if minutes < 0:
        return "warn"
    return "info"


def _approval_to_view(approval: Optional[TimecardApproval]) -> dict:
    raw_status = (approval.status if approval else TIMECARD_STATUS_PENDING) or ""
    status = raw_status if raw_status in TIMECARD_STATUS_VALUES else TIMECARD_STATUS_PENDING
    decided_label = ""
    if approval and approval.decided_at:
        decided_label = f"{_format_month_day(approval.decided_at.date())}, {_fmt_time(approval.decided_at)}"
    return {
        "status": status,
        "label": _TIMECARD_STATUS_LABELS[status],
        "tone": _TIMECARD_STATUS_TONES[status],
        "note": (approval.note or "") if approval else "",
        "decided_label": decided_label,
    }


def _audit_log_to_view(row: AuditLog, tz: ZoneInfo) -> dict:
    try:
        details = json.loads(row.details_json or "{}")
    except (TypeError, ValueError):
        details = {}
    created_at = row.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    created_local = created_at.astimezone(tz)
    action = "Bulk approved" if row.action == "admin.timecard.bulk_approve" else "Status updated"
    return {
        "created_label": f"{_format_month_day(created_local.date())}, {_fmt_time(created_local)}",
        "action": action,
        "work_date": details.get("work_date") or details.get("week_start") or "",
        "status": _TIMECARD_STATUS_LABELS.get(str(details.get("status") or ""), ""),
        "count": details.get("count"),
        "note": details.get("note") or "",
    }


@dataclass
class _DayRow:
    day: date
    weekday_label: str
    date_label: str
    shifts: list[dict]
    scheduled_hours: float
    entries: list[ClockifyEntryView]
    entry_rows: list[dict]
    actual_seconds: int
    actual_label: str
    break_seconds: int
    break_label: str
    first_clock_in_label: str
    last_clock_out_label: str
    variance_minutes: Optional[int]
    variance_label: str
    variance_tone: str
    daily_overtime_seconds: int
    daily_overtime_label: str
    approval: dict
    has_activity: bool
    pills: list[dict]
    has_running: bool
    is_today: bool


def _shift_to_view(entry: ShiftEntry) -> dict:
    ranges = _parse_shift_ranges(entry.label or "")
    return {
        "kind": entry.kind or "",
        "label": entry.label or "",
        "is_labor": (entry.kind or "") in _LABOR_KINDS and bool(ranges),
        "ranges": ranges,
        "hours": _shift_total_hours(ranges),
    }


def _entry_to_view(entry: ClockifyEntryView) -> dict:
    is_break = _entry_is_break(entry)
    if entry.running:
        end_label = "now"
        note = "Running"
    elif entry.end_local is None:
        end_label = "—"
        note = "No clock-out"
    else:
        end_label = _fmt_time(entry.end_local)
        note = ""
    return {
        "id": entry.id,
        "description": entry.description,
        "start_label": _fmt_time(entry.start_local),
        "end_label": end_label,
        "duration_label": format_hours(entry.duration_seconds),
        "duration_seconds": entry.duration_seconds,
        "running": entry.running,
        "is_break": is_break,
        "note": note,
    }


def _anomaly_pills(
    shifts: list[dict],
    entries: list[ClockifyEntryView],
    day: date,
    tz: ZoneInfo,
    today: date,
) -> tuple[list[dict], bool]:
    pills: list[dict] = []
    scheduled_labor_ranges: list[tuple[int, int]] = []
    for s in shifts:
        if s["is_labor"]:
            scheduled_labor_ranges.extend(s["ranges"])
    has_schedule = bool(scheduled_labor_ranges)
    work_entries = [e for e in entries if not _entry_is_break(e)]
    has_actual = bool(work_entries)
    running = any(e.running for e in entries)

    if running:
        pills.append({"label": "Running", "tone": "info"})

    if has_schedule and not has_actual:
        if day <= today:
            pills.append({"label": "No-show", "tone": "danger"})
        return pills, running

    if has_actual and not has_schedule:
        pills.append({"label": "Unscheduled", "tone": "warn"})
        return pills, running

    if has_schedule and has_actual:
        day_start = datetime.combine(day, time.min, tzinfo=tz)
        sched_start_min = min(a for a, _ in scheduled_labor_ranges)
        sched_end_min = max(b for _, b in scheduled_labor_ranges)
        sched_start_dt = day_start + timedelta(minutes=sched_start_min)
        sched_end_dt = day_start + timedelta(minutes=sched_end_min)
        actual_starts = [e.start_local for e in work_entries if e.start_local]
        actual_ends = [e.end_local for e in work_entries if e.end_local]
        late = False
        left_early = False
        if actual_starts:
            first_in = min(actual_starts)
            if first_in > sched_start_dt + timedelta(minutes=LATE_THRESHOLD_MIN):
                late = True
        if not running and actual_ends:
            last_out = max(actual_ends)
            if last_out < sched_end_dt - timedelta(minutes=EARLY_LEAVE_THRESHOLD_MIN):
                left_early = True
        if late:
            pills.append({"label": "Late arrival", "tone": "danger"})
        if left_early:
            pills.append({"label": "Left early", "tone": "warn"})
        if not late and not left_early and not running:
            pills.append({"label": "On time", "tone": "ok"})
    return pills, running


def _build_day_rows(
    *,
    week_start: date,
    shifts_by_day: dict[date, list[ShiftEntry]],
    summary: Optional[ClockifyWeekSummary],
    approvals_by_day: dict[date, TimecardApproval],
    tz: ZoneInfo,
    today: date,
) -> list[_DayRow]:
    entries_by_day: dict[date, list[ClockifyEntryView]] = {}
    if summary:
        for entry in summary.entries:
            if entry.start_local is None:
                continue
            entries_by_day.setdefault(entry.start_local.date(), []).append(entry)
    daily_totals: dict[date, int] = {}
    if summary:
        daily_totals = {row.day: row.duration_seconds for row in summary.daily_totals}

    rows: list[_DayRow] = []
    for offset in range(7):
        day = week_start + timedelta(days=offset)
        day_shifts = [_shift_to_view(s) for s in shifts_by_day.get(day, [])]
        scheduled_hours = round(
            sum(s["hours"] for s in day_shifts if s["is_labor"]), 2
        )
        day_entries = entries_by_day.get(day, [])
        day_entry_rows = [_entry_to_view(e) for e in day_entries]
        break_seconds = sum(
            e.duration_seconds for e in day_entries if _entry_is_break(e)
        )
        actual_seconds = sum(
            e.duration_seconds for e in day_entries if not _entry_is_break(e)
        )
        if (
            actual_seconds == 0
            and day_entries
            and not any(_entry_is_break(e) for e in day_entries)
        ):
            actual_seconds = daily_totals.get(day, 0)
        work_entries = [e for e in day_entries if not _entry_is_break(e)]
        first_clock_in_label = _fmt_time(
            min((e.start_local for e in work_entries if e.start_local), default=None)
        )
        last_clock_out_label = _fmt_time(
            max((e.end_local for e in work_entries if e.end_local), default=None)
        )
        scheduled_seconds = int(round(scheduled_hours * 3600))
        if scheduled_seconds > 0:
            variance_minutes: Optional[int] = int(
                round((actual_seconds - scheduled_seconds) / 60)
            )
            variance_label = _format_signed_minutes(variance_minutes)
        elif actual_seconds > 0:
            variance_minutes = None
            variance_label = "Unscheduled"
        else:
            variance_minutes = None
            variance_label = "--"
        daily_overtime_seconds = max(0, actual_seconds - DAILY_OVERTIME_SECONDS)
        pills, running = _anomaly_pills(day_shifts, day_entries, day, tz, today)
        if break_seconds > 0:
            pills.append({"label": f"Break {format_hours(break_seconds)}", "tone": "info"})
        if daily_overtime_seconds > 0:
            pills.append({"label": f"OT {format_hours(daily_overtime_seconds)}", "tone": "warn"})
        date_label = _format_month_day(day)
        rows.append(
            _DayRow(
                day=day,
                weekday_label=day.strftime("%a"),
                date_label=date_label,
                shifts=day_shifts,
                scheduled_hours=scheduled_hours,
                entries=day_entries,
                entry_rows=day_entry_rows,
                actual_seconds=actual_seconds,
                actual_label=format_hours(actual_seconds),
                break_seconds=break_seconds,
                break_label=format_hours(break_seconds),
                first_clock_in_label=first_clock_in_label,
                last_clock_out_label=last_clock_out_label,
                variance_minutes=variance_minutes,
                variance_label=variance_label,
                variance_tone=_variance_tone(variance_minutes),
                daily_overtime_seconds=daily_overtime_seconds,
                daily_overtime_label=format_hours(daily_overtime_seconds),
                approval=_approval_to_view(approvals_by_day.get(day)),
                has_activity=bool(day_shifts or day_entries or actual_seconds or break_seconds),
                pills=pills,
                has_running=running,
                is_today=(day == today),
            )
        )
    return rows


def _employee_display_name(user: User) -> str:
    name = (user.display_name or "").strip()
    return name or (user.username or "").strip() or f"User {user.id}"


def _timecard_redirect_url(
    user_id: int,
    week_start: date,
    *,
    flash: Optional[str] = None,
    error: Optional[str] = None,
) -> str:
    query: dict[str, str] = {"week": week_start.isoformat()}
    if flash:
        query["flash"] = flash
    if error:
        query["error"] = error
    return f"/team/admin/employees/{user_id}/timecards?{urlencode(query)}"


def _parse_iso_date(value: str) -> date:
    return datetime.strptime((value or "").strip(), "%Y-%m-%d").date()


def set_timecard_day_status(
    session: Session,
    *,
    current_user: User,
    user_id: int,
    work_date: date,
    status: str,
    note: str = "",
    ip_address: Optional[str] = None,
    action: str = "admin.timecard.status_update",
) -> TimecardApproval:
    status = (status or "").strip().lower()
    if status not in TIMECARD_STATUS_VALUES:
        raise ValueError("Unsupported timecard status")
    note = (note or "").strip()[:1000]
    approval = session.exec(
        select(TimecardApproval).where(
            TimecardApproval.user_id == user_id,
            TimecardApproval.work_date == work_date,
        )
    ).first()
    old_status = approval.status if approval else None
    now = utcnow()
    if approval is None:
        approval = TimecardApproval(user_id=user_id, work_date=work_date)
        approval.created_at = now
    approval.status = status
    approval.note = note
    approval.decided_by_user_id = current_user.id
    approval.decided_at = now
    approval.updated_at = now
    session.add(approval)
    details: dict[str, object] = {
        "work_date": work_date.isoformat(),
        "status": status,
        "old_status": old_status,
    }
    if note:
        details["note"] = note[:240]
    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            target_user_id=user_id,
            action=action,
            resource_key=f"timecard:{user_id}:{work_date.isoformat()}",
            details_json=json.dumps(details, sort_keys=True),
            ip_address=ip_address,
        )
    )
    session.commit()
    session.refresh(approval)
    return approval


@router.post(
    "/team/admin/employees/{user_id}/timecards/day-status",
    dependencies=[Depends(require_csrf)],
)
def admin_employee_timecard_day_status(
    request: Request,
    user_id: int,
    work_date: str = Form(...),
    status: str = Form(...),
    note: str = Form(default=""),
    week: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    try:
        parsed_date = _parse_iso_date(work_date)
    except ValueError:
        parsed_date = datetime.now(_tz()).date()
        week_start = _week_start_for(week, parsed_date)
        return RedirectResponse(
            _timecard_redirect_url(user_id, week_start, error="Invalid work date"),
            status_code=303,
        )
    week_start = _week_start_for(week, parsed_date)
    try:
        set_timecard_day_status(
            session,
            current_user=current,
            user_id=user_id,
            work_date=parsed_date,
            status=status,
            note=note,
            ip_address=request.client.host if request.client else None,
        )
    except ValueError as exc:
        return RedirectResponse(
            _timecard_redirect_url(user_id, week_start, error=str(exc)),
            status_code=303,
        )
    return RedirectResponse(
        _timecard_redirect_url(user_id, week_start, flash="Timecard updated"),
        status_code=303,
    )


@router.post(
    "/team/admin/employees/{user_id}/timecards/bulk-approve",
    dependencies=[Depends(require_csrf)],
)
def admin_employee_timecard_bulk_approve(
    request: Request,
    user_id: int,
    work_dates: list[str] = Form(default=[]),
    week: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    today = datetime.now(_tz()).date()
    week_start = _week_start_for(week, today)
    parsed_dates: list[date] = []
    for raw in work_dates:
        try:
            parsed_dates.append(_parse_iso_date(raw))
        except ValueError:
            continue
    if not parsed_dates:
        return RedirectResponse(
            _timecard_redirect_url(user_id, week_start, error="No active days to approve"),
            status_code=303,
        )

    approved_count = 0
    for work_day in sorted(set(parsed_dates)):
        existing = session.exec(
            select(TimecardApproval).where(
                TimecardApproval.user_id == user_id,
                TimecardApproval.work_date == work_day,
            )
        ).first()
        if existing and existing.status == TIMECARD_STATUS_LOCKED:
            continue
        set_timecard_day_status(
            session,
            current_user=current,
            user_id=user_id,
            work_date=work_day,
            status=TIMECARD_STATUS_APPROVED,
            note="",
            ip_address=request.client.host if request.client else None,
            action="admin.timecard.bulk_approve",
        )
        approved_count += 1

    session.add(
        AuditLog(
            actor_user_id=current.id,
            target_user_id=user_id,
            action="admin.timecard.bulk_approve",
            resource_key=f"timecard:{user_id}:{week_start.isoformat()}",
            details_json=json.dumps(
                {
                    "week_start": week_start.isoformat(),
                    "status": TIMECARD_STATUS_APPROVED,
                    "count": approved_count,
                },
                sort_keys=True,
            ),
            ip_address=request.client.host if request.client else None,
        )
    )
    session.commit()
    return RedirectResponse(
        _timecard_redirect_url(
            user_id,
            week_start,
            flash=f"Approved {approved_count} day{'s' if approved_count != 1 else ''}",
        ),
        status_code=303,
    )


@router.get(
    "/team/admin/employees/{user_id}/timecards",
    response_class=HTMLResponse,
)
def admin_employee_timecards(
    request: Request,
    user_id: int,
    week: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial

    employee = session.get(User, user_id)
    if employee is None:
        return HTMLResponse("Employee not found", status_code=404)
    profile = session.get(EmployeeProfile, user_id)

    settings = get_settings()
    tz = _tz(settings)
    today = datetime.now(tz).date()
    week_start = _week_start_for(week, today)
    week_end_inclusive = week_start + timedelta(days=6)

    prev_week = (week_start - timedelta(days=7)).isoformat()
    next_week = (week_start + timedelta(days=7)).isoformat()
    this_week_start = today - timedelta(days=today.weekday())

    shift_rows = list(
        session.exec(
            select(ShiftEntry)
            .where(
                ShiftEntry.user_id == user_id,
                ShiftEntry.shift_date >= week_start,
                ShiftEntry.shift_date <= week_end_inclusive,
            )
            .order_by(ShiftEntry.shift_date, ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )
    shifts_by_day: dict[date, list[ShiftEntry]] = {}
    for row in shift_rows:
        shifts_by_day.setdefault(row.shift_date, []).append(row)

    approval_rows = list(
        session.exec(
            select(TimecardApproval).where(
                TimecardApproval.user_id == user_id,
                TimecardApproval.work_date >= week_start,
                TimecardApproval.work_date <= week_end_inclusive,
            )
        ).all()
    )
    approvals_by_day = {row.work_date: row for row in approval_rows}

    configured = clockify_is_configured(settings)
    mapped = bool(profile and (profile.clockify_user_id or "").strip())
    summary: Optional[ClockifyWeekSummary] = None
    api_error: Optional[str] = None

    if configured and mapped:
        start_local = datetime.combine(week_start, time.min, tzinfo=tz)
        end_local = start_local + timedelta(days=7)
        try:
            client = clockify_client_from_settings(settings)
            raw_entries = client.get_user_time_entries(
                (profile.clockify_user_id or "").strip(),
                start_utc=start_local.astimezone(timezone.utc),
                end_utc=end_local.astimezone(timezone.utc),
            )
            summary = build_week_summary(
                raw_entries,
                week_start_local=start_local,
                week_end_local=end_local,
                settings=settings,
            )
        except (ClockifyApiError, ClockifyConfigError) as exc:
            api_error = str(exc)

    day_rows = _build_day_rows(
        week_start=week_start,
        shifts_by_day=shifts_by_day,
        summary=summary,
        approvals_by_day=approvals_by_day,
        tz=tz,
        today=today,
    )

    scheduled_total_hours = round(sum(row.scheduled_hours for row in day_rows), 2)
    actual_total_seconds = sum(row.actual_seconds for row in day_rows)
    actual_total_label = format_hours(actual_total_seconds)
    break_total_seconds = sum(row.break_seconds for row in day_rows)
    break_total_label = format_hours(break_total_seconds)
    variance_total_minutes = sum(
        row.variance_minutes or 0 for row in day_rows if row.scheduled_hours
    )
    variance_total_label = _format_signed_minutes(variance_total_minutes)
    weekly_overtime_seconds = max(0, actual_total_seconds - WEEKLY_OVERTIME_SECONDS)
    daily_overtime_seconds = sum(row.daily_overtime_seconds for row in day_rows)
    overtime_total_seconds = max(weekly_overtime_seconds, daily_overtime_seconds)
    overtime_total_label = format_hours(overtime_total_seconds)
    shifts_worked = sum(1 for row in day_rows if row.actual_seconds > 0)
    running_count = summary.running_count if summary else 0
    active_day_rows = [row for row in day_rows if row.has_activity]
    approval_counts = {
        status: sum(1 for row in active_day_rows if row.approval["status"] == status)
        for status in TIMECARD_STATUS_VALUES
    }

    history_rows = compensation_history_rows_for_users(
        session,
        [user_id],
        end_day=week_end_inclusive,
    ).get(user_id, [])
    week_snapshot = compensation_snapshot_for_day(
        profile,
        week_end_inclusive,
        history_rows=history_rows,
    )
    compensation_type = str(week_snapshot.get("compensation_type") or "hourly")
    salary_employee = compensation_type == _COMPENSATION_MONTHLY_SALARY
    if salary_employee:
        salary_cents = int(week_snapshot.get("monthly_salary_cents") or 0)
        pay_missing = week_snapshot.get("monthly_salary_cents") is None
        labor_cents = salary_cents
        labor_tile_label = "Monthly pay"
        labor_basis_label = "Fixed monthly salary effective this week"
        labor_missing_label = "salary not set"
        labor_missing_help = "Add a monthly salary on the profile"
        # Drop the plaintext amount before building the context to keep it out
        # of both the template namespace and any future locals() dumps.
        del salary_cents
    elif compensation_type == "unpaid":
        labor_cents = 0
        pay_missing = False
        labor_tile_label = "Labor cost"
        labor_basis_label = "Employee marked unpaid for this week"
        labor_missing_label = "unpaid"
        labor_missing_help = "Change the pay type on the profile"
    else:
        labor_cents = 0
        missing_rate = False
        has_hourly_seconds = False
        for row in day_rows:
            seconds = int(row.actual_seconds or 0)
            if seconds <= 0:
                continue
            day_snapshot = compensation_snapshot_for_day(
                profile,
                row.day,
                history_rows=history_rows,
            )
            if day_snapshot.get("compensation_type") != "hourly":
                continue
            has_hourly_seconds = True
            rate_cents = day_snapshot.get("hourly_rate_cents")
            if rate_cents is None:
                missing_rate = True
                continue
            labor_cents += _labor_cents(seconds, int(rate_cents))
        pay_missing = (
            missing_rate
            if has_hourly_seconds
            else week_snapshot.get("hourly_rate_cents") is None
        )
        labor_tile_label = "Labor cost"
        labor_basis_label = "Paid Clockify hours x effective hourly rate"
        labor_missing_label = "rate not set"
        labor_missing_help = "Add an hourly rate on the profile"

    has_any_scheduled = bool(shift_rows)
    has_any_actual = any(row.actual_seconds or row.break_seconds for row in day_rows)
    audit_history = [
        _audit_log_to_view(row, tz)
        for row in session.exec(
            select(AuditLog)
            .where(
                AuditLog.target_user_id == user_id,
                AuditLog.action.in_(_TIMECARD_AUDIT_ACTIONS),
            )
            .order_by(AuditLog.created_at.desc())
            .limit(12)
        ).all()
    ]

    week_label = (
        f"{_format_month_day(week_start)} - "
        f"{_format_month_day(week_end_inclusive, include_year=True)}"
    )

    context = {
        "request": request,
        "title": f"Timecards · {_employee_display_name(employee)}",
        "current_user": current,
        "employee": employee,
        "employee_name": _employee_display_name(employee),
        "profile_mapped": mapped,
        "clockify_configured": configured,
        "api_error": api_error,
        "flash": flash,
        "error": error,
        "week_start": week_start,
        "week_end_inclusive": week_end_inclusive,
        "week_label": week_label,
        "week_start_iso": week_start.isoformat(),
        "prev_week_iso": prev_week,
        "next_week_iso": next_week,
        "this_week_iso": this_week_start.isoformat(),
        "is_this_week": week_start == this_week_start,
        "timezone_label": str(tz.key),
        "day_rows": day_rows,
        "scheduled_total_hours": scheduled_total_hours,
        "actual_total_label": actual_total_label,
        "actual_total_seconds": actual_total_seconds,
        "break_total_label": break_total_label,
        "break_total_seconds": break_total_seconds,
        "variance_total_label": variance_total_label,
        "variance_total_tone": _variance_tone(variance_total_minutes),
        "overtime_total_label": overtime_total_label,
        "overtime_total_seconds": overtime_total_seconds,
        "shifts_worked": shifts_worked,
        "running_count": running_count,
        "approval_counts": approval_counts,
        "timecard_status_options": TIMECARD_STATUS_OPTIONS,
        "active_day_count": len(active_day_rows),
        "audit_history": audit_history,
        "labor_total_label": _format_dollars(labor_cents) if not pay_missing else None,
        "labor_rate_missing": pay_missing,
        "labor_tile_label": labor_tile_label,
        "labor_missing_label": labor_missing_label,
        "labor_missing_help": labor_missing_help,
        "labor_basis_label": labor_basis_label,
        "salary_employee": salary_employee,
        "has_any_scheduled": has_any_scheduled,
        "has_any_actual": has_any_actual,
        "csrf_token": issue_token(request),
    }
    return templates.TemplateResponse(
        request, "team/admin/employee_timecards.html", context
    )
