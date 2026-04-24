"""/team/admin/employees/{id}/timecards - per-employee weekly timecard view.

Combines the scheduled ShiftEntry rows for a week with the employee's
Clockify time entries, computes per-day anomalies (late/early/no-show/
unscheduled/running), and shows an aggregate labor total.

Wage privacy: individual hourly rates are never placed in the template
context, logs, or response body. Only the aggregate labor total and a
boolean "missing rate" flag leave this module.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
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
from ..csrf import issue_token
from ..db import get_session
from ..models import (
    EmployeeProfile,
    SHIFT_KIND_ALL,
    SHIFT_KIND_WORK,
    ShiftEntry,
    User,
)
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _permission_gate


router = APIRouter()


DEFAULT_TZ = "America/Los_Angeles"
LATE_THRESHOLD_MIN = 15
EARLY_LEAVE_THRESHOLD_MIN = 15
_LABOR_KINDS = {SHIFT_KIND_WORK, SHIFT_KIND_ALL}


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


def _hourly_rate_cents(profile: Optional[EmployeeProfile]) -> tuple[int, bool]:
    """Return (rate_cents, missing). Never logs or renders plaintext."""
    if profile is None or not profile.hourly_rate_cents_enc:
        return 0, True
    try:
        raw = decrypt_pii(profile.hourly_rate_cents_enc)
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


def _labor_cents(total_seconds: int, rate_cents: int) -> int:
    if rate_cents <= 0 or total_seconds <= 0:
        return 0
    hours = Decimal(total_seconds) / Decimal(3600)
    return int(
        (hours * Decimal(rate_cents)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )


def _format_dollars(cents: int) -> str:
    return f"${cents / 100:,.2f}"


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
    has_actual = bool(entries)
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
        actual_starts = [e.start_local for e in entries if e.start_local]
        actual_ends = [e.end_local for e in entries if e.end_local]
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
        actual_seconds = daily_totals.get(
            day, sum(e.duration_seconds for e in day_entries)
        )
        pills, running = _anomaly_pills(day_shifts, day_entries, day, tz, today)
        try:
            date_label = day.strftime("%b %-d")
        except ValueError:
            date_label = day.strftime("%b %d").replace(" 0", " ")
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
                pills=pills,
                has_running=running,
                is_today=(day == today),
            )
        )
    return rows


def _employee_display_name(user: User) -> str:
    name = (user.display_name or "").strip()
    return name or (user.username or "").strip() or f"User {user.id}"


@router.get(
    "/team/admin/employees/{user_id}/timecards",
    response_class=HTMLResponse,
)
def admin_employee_timecards(
    request: Request,
    user_id: int,
    week: Optional[str] = Query(default=None),
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
        tz=tz,
        today=today,
    )

    scheduled_total_hours = round(sum(row.scheduled_hours for row in day_rows), 2)
    actual_total_seconds = summary.total_seconds if summary else 0
    actual_total_label = format_hours(actual_total_seconds)
    shifts_worked = sum(1 for row in day_rows if row.actual_seconds > 0)
    running_count = summary.running_count if summary else 0

    rate_cents, rate_missing = _hourly_rate_cents(profile)
    labor_cents = _labor_cents(actual_total_seconds, rate_cents)
    # Drop the plaintext rate before building the context to keep it out
    # of both the template namespace and any future locals() dumps.
    del rate_cents

    has_any_scheduled = bool(shift_rows)
    has_any_actual = bool(summary and summary.entries)

    week_label = f"{week_start.strftime('%b %-d')} – {week_end_inclusive.strftime('%b %-d, %Y')}"

    context = {
        "request": request,
        "title": f"Timecards · {_employee_display_name(employee)}",
        "current_user": current,
        "employee": employee,
        "employee_name": _employee_display_name(employee),
        "profile_mapped": mapped,
        "clockify_configured": configured,
        "api_error": api_error,
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
        "shifts_worked": shifts_worked,
        "running_count": running_count,
        "labor_total_label": _format_dollars(labor_cents) if not rate_missing else None,
        "labor_rate_missing": rate_missing,
        "has_any_scheduled": has_any_scheduled,
        "has_any_actual": has_any_actual,
        "csrf_token": issue_token(request),
    }
    return templates.TemplateResponse(
        request, "team/admin/employee_timecards.html", context
    )
