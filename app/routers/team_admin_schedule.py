"""
/team/admin/schedule — weekly grid editor.

Rows are active employees; columns are the 7 days of a chosen Mon-Sun week.
Each (employee, day) cell stores a free-text label ("10:30 AM - 6:30 PM",
"OFF", "Stream", "REQUEST", etc.) plus a derived `kind` that drives the
cell color. Admins type directly into each cell; hitting Save posts the
whole grid in one shot so the common case — tweaking a few shifts — never
re-creates dozens of rows.

The matching employee read-only view at /team/schedule reuses the same
visual so there's zero translation between what admins see and what the
floor sees.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..auth import has_permission
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import (
    AuditLog,
    STAFF_KIND_STOREFRONT,
    STAFF_KIND_STREAM,
    STAFF_KINDS,
    ScheduleDayNote,
    ScheduleRosterMember,
    SHIFT_KIND_BLANK,
    ShiftEntry,
    StoreClosure,
    StreamAccount,
    Streamer,
    StreamSchedule,
    User,
    classify_shift_label,
    utcnow,
)
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate

router = APIRouter()


def _monday_of(d: date) -> date:
    """Return the Monday of the given date's ISO week."""
    return d - timedelta(days=d.weekday())


def _parse_week_start(raw: Optional[str]) -> date:
    """Accept ?week=YYYY-MM-DD (any day); snap to Monday. Default = this week."""
    if raw:
        try:
            return _monday_of(datetime.strptime(raw, "%Y-%m-%d").date())
        except ValueError:
            pass
    return _monday_of(date.today())


def _week_dates(start_monday: date) -> list[date]:
    return [start_monday + timedelta(days=i) for i in range(7)]


def _build_cell_key(user_id: int, d: date) -> str:
    """Form-input name for a given (user, date) cell."""
    return f"cell__{user_id}__{d.isoformat()}"


def _build_day_loc_key(d: date) -> str:
    return f"dayloc__{d.isoformat()}"


# ---------------------------------------------------------------------------
# US legal holidays
#
# Hand-rolled so we don't take on a new dependency. Covers the 11 US federal
# holidays plus Christmas Eve / NYE / Easter Sunday which commonly close
# retail storefronts. Each entry is (stable_key, display_label, date) so
# re-checking the same holiday next year upserts on (holiday_key, year).
# ---------------------------------------------------------------------------


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the nth occurrence of `weekday` (Mon=0..Sun=6) in year/month."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of `weekday` in year/month."""
    import calendar as _cal

    last_day = _cal.monthrange(year, month)[1]
    last = date(year, month, last_day)
    offset = (last.weekday() - weekday) % 7
    return last - timedelta(days=offset)


def _easter_sunday(year: int) -> date:
    """Anonymous Gregorian algorithm for Easter Sunday (western)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d_ = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d_ - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _us_legal_holidays(year: int) -> list[tuple[str, str, date]]:
    """Return a list of (key, label, date) tuples for the given year.

    Keys are stable across years so the holidays modal can treat
    "christmas 2026" and "christmas 2027" as the same line with a
    different date.
    """
    return [
        ("new_years_day", "New Year's Day", date(year, 1, 1)),
        ("mlk_day", "Martin Luther King Jr. Day", _nth_weekday(year, 1, 0, 3)),
        ("presidents_day", "Presidents' Day", _nth_weekday(year, 2, 0, 3)),
        ("easter_sunday", "Easter Sunday", _easter_sunday(year)),
        ("memorial_day", "Memorial Day", _last_weekday(year, 5, 0)),
        ("juneteenth", "Juneteenth", date(year, 6, 19)),
        ("independence_day", "Independence Day", date(year, 7, 4)),
        ("labor_day", "Labor Day", _nth_weekday(year, 9, 0, 1)),
        ("columbus_day", "Columbus Day / Indigenous Peoples' Day", _nth_weekday(year, 10, 0, 2)),
        ("veterans_day", "Veterans Day", date(year, 11, 11)),
        ("thanksgiving", "Thanksgiving Day", _nth_weekday(year, 11, 3, 4)),
        ("day_after_thanksgiving", "Day after Thanksgiving", _nth_weekday(year, 11, 3, 4) + timedelta(days=1)),
        ("christmas_eve", "Christmas Eve", date(year, 12, 24)),
        ("christmas_day", "Christmas Day", date(year, 12, 25)),
        ("new_years_eve", "New Year's Eve", date(year, 12, 31)),
    ]


def _closure_map_for_range(
    session: Session, first_day: date, last_day: date
) -> dict[str, StoreClosure]:
    """ISO date -> StoreClosure for every closure in the given range."""
    rows = list(
        session.exec(
            select(StoreClosure).where(
                StoreClosure.day_date >= first_day,
                StoreClosure.day_date <= last_day,
            )
        ).all()
    )
    return {r.day_date.isoformat(): r for r in rows}


def _fmt_time_12h(t24: str) -> str:
    """Format an 'HH:MM' 24-hour time string as '4:00 PM'."""
    try:
        h_s, m_s = t24.split(":")[0], t24.split(":")[1]
        h = int(h_s)
        suffix = "AM" if h < 12 else "PM"
        h12 = h % 12 or 12
        return f"{h12}:{m_s} {suffix}"
    except Exception:
        return t24


# ---------------------------------------------------------------------------
# Shift-hour parsing
#
# Admins type shift labels as free text ("10:30 AM - 6:30 PM"). To power the
# 7shifts-style daily/weekly hour totals, we need to turn that text back into
# a number of hours. The parser is deliberately forgiving: if we can't make
# sense of a label, we return 0 hours rather than raising, so one weird cell
# never hides the rest of the week's totals.
# ---------------------------------------------------------------------------

_RANGE_SPLIT_RE = re.compile(r"\s*[/,&]\s*")
_TIME_RE = re.compile(
    r"^\s*(?P<h>\d{1,2})(?::(?P<m>\d{2}))?\s*(?P<ap>[ap](?:\.?m\.?)?)?\s*$",
    re.IGNORECASE,
)

_NON_SHIFT_TOKENS = {"OFF", "SHOW", "REQUEST", "IF NEEDED", "STREAM"}


def _parse_time_to_minutes(s: str) -> Optional[int]:
    m = _TIME_RE.match(s)
    if not m:
        return None
    h = int(m.group("h"))
    mins = int(m.group("m") or 0)
    ap = (m.group("ap") or "").lower().replace(".", "").replace("m", "")
    if mins < 0 or mins > 59 or h < 0 or h > 23:
        return None
    if ap == "p" and h < 12:
        h += 12
    elif ap == "a" and h == 12:
        h = 0
    return h * 60 + mins


def _parse_shift_hours(label: str) -> float:
    """Return total hours in the label, or 0.0 if unparseable.

    Supports:
      - "10:30 AM - 6:30 PM", "10:30am-6:30pm", "10-6pm"
      - Bare "9-5" → assumed 9 AM to 5 PM (business-day heuristic)
      - Multiple ranges separated by '/', ',', or '&' (summed)
      - Overnight ranges (end <= start) wrap to next day
      - Labels like "OFF", "SHOW", "REQUEST" → 0 hours (they aren't shifts)
    """
    if not label:
        return 0.0
    upper = label.strip().upper()
    if upper in _NON_SHIFT_TOKENS:
        return 0.0

    total = 0.0
    for part in _RANGE_SPLIT_RE.split(label):
        m = re.match(
            r"^\s*(?P<a>[0-9:.apm\s]+?)\s*[-\u2013\u2014]\s*(?P<b>[0-9:.apm\s]+?)\s*$",
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
        # "9-5" business-day heuristic: no AM/PM on either side, bump
        # the smaller end into PM so the total comes out to 8h not 20h.
        if not a_has_ap and not b_has_ap:
            a_h, b_h = a // 60, b // 60
            if b_h < a_h and a_h <= 11:
                b += 12 * 60
        if b <= a:
            b += 24 * 60  # overnight wrap
        total += (b - a) / 60.0
    return round(total, 2)


# Palette used to give each StreamAccount a stable, distinct background
# color on the weekly Stream grid. Deliberately picked to NOT collide with
# the existing work/off/show/request/stream shift-kind colors. Account
# IDs index into this list (wrapped) so the same account keeps the same
# color across weeks.
_STREAM_ACCOUNT_COLOR_PALETTE = [
    "#a78bfa",  # violet
    "#fbbf24",  # amber
    "#f472b6",  # pink
    "#22d3ee",  # cyan
    "#fb923c",  # orange
    "#84cc16",  # lime
    "#e879f9",  # fuchsia
    "#14b8a6",  # teal
]


def _stream_account_color(account_id: Optional[int]) -> str:
    if account_id is None:
        return "#bbf7d0"  # neutral mint fallback (matches the old Stream color)
    return _STREAM_ACCOUNT_COLOR_PALETTE[
        account_id % len(_STREAM_ACCOUNT_COLOR_PALETTE)
    ]


def _stream_schedule_hint_map(
    session: Session,
    week_days: list[date],
    user_ids: set[int],
) -> tuple[dict[tuple[int, str], dict], list[dict]]:
    """Build the week's Stream grid contents from StreamSchedule rows.

    Returns a tuple ``(hint_map, legend)`` where:

    * ``hint_map[(user_id, 'YYYY-MM-DD')]`` is a dict with ``label``
      (``'4:00 PM - 6:00 AM (next day)'``), ``account_id``,
      ``account_name``, and ``color`` keys.
    * ``legend`` is a deduplicated list of ``{"name", "color"}`` dicts
      for every StreamAccount that appears on the week, so the template
      can render a per-account legend right next to the grid.

    Streamers with no linked user are skipped (they have no row on the
    grid to pre-fill). If multiple shifts land on the same day for one
    user, we keep only the first and append a '+N more' note so nothing
    silently disappears.
    """
    if not user_ids or not week_days:
        return {}, []
    streamers = list(
        session.exec(
            select(Streamer).where(Streamer.user_id.in_(user_ids))  # type: ignore[attr-defined]
        ).all()
    )
    streamer_to_user: dict[int, int] = {
        s.id: s.user_id for s in streamers if s.id is not None and s.user_id is not None
    }
    if not streamer_to_user:
        return {}, []
    iso_days = [d.isoformat() for d in week_days]
    scheds = list(
        session.exec(
            select(StreamSchedule).where(
                StreamSchedule.streamer_id.in_(streamer_to_user.keys()),  # type: ignore[attr-defined]
                StreamSchedule.date.in_(iso_days),  # type: ignore[attr-defined]
            )
        ).all()
    )
    if not scheds:
        return {}, []

    account_ids = {s.stream_account_id for s in scheds if s.stream_account_id}
    account_map: dict[int, StreamAccount] = {}
    if account_ids:
        for acct in session.exec(
            select(StreamAccount).where(StreamAccount.id.in_(account_ids))  # type: ignore[attr-defined]
        ).all():
            if acct.id is not None:
                account_map[acct.id] = acct

    def _acct_name(aid: Optional[int]) -> str:
        if aid is None:
            return "Other"
        acct = account_map.get(aid)
        if acct is None:
            return "Other"
        return acct.name or acct.handle or f"Account {aid}"

    hint_map: dict[tuple[int, str], dict] = {}
    # Track account colors for the legend chips.
    seen_accounts: dict[Optional[int], str] = {}

    for s in sorted(scheds, key=lambda r: (r.date, r.start_time)):
        uid = streamer_to_user.get(s.streamer_id)
        if uid is None:
            continue
        color = _stream_account_color(s.stream_account_id)
        seen_accounts[s.stream_account_id] = color
        key = (uid, s.date)
        if key in hint_map:
            # Second shift on same (user, date). Tag the existing cell
            # rather than dropping the data entirely. `extra_count` tells
            # the admin UI "this cell has N more shifts — edit in /stream-manager".
            hint_map[key]["label"] += "  +1"
            hint_map[key]["extra_count"] = hint_map[key].get("extra_count", 0) + 1
            continue
        label = f"{_fmt_time_12h(s.start_time)} - {_fmt_time_12h(s.end_time)}"
        if s.is_overnight:
            label += " (next day)"
        hint_map[key] = {
            "label": label,
            "account_id": s.stream_account_id,
            "account_name": _acct_name(s.stream_account_id),
            "color": color,
            # Raw fields so the click-to-edit modal can pre-fill without
            # parsing the display label back out.
            "schedule_id": s.id,
            "start_time": s.start_time,
            "end_time": s.end_time,
            "title": s.title or "",
            "notes": s.notes or "",
            "is_overnight": s.is_overnight,
            "extra_count": 0,
        }

    legend: list[dict] = []
    seen_names: set[str] = set()
    for aid, color in seen_accounts.items():
        name = _acct_name(aid)
        if name in seen_names:
            continue
        seen_names.add(name)
        legend.append({"name": name, "color": color})
    # Stable, human-friendly legend order: accounts sorted by name.
    legend.sort(key=lambda row: row["name"].lower())
    return hint_map, legend


def _grid_context(
    session: Session,
    week_start: date,
    *,
    staff_kind: Optional[str] = None,
    flash: Optional[str] = None,
) -> dict:
    """Collect all the data the schedule grid template needs.

    Shared between the admin view (editable) and the employee view
    (read-only) so the two render identically.

    The list of people on the grid is a union of:
      1. The per-week roster (`ScheduleRosterMember`) — admins opt people
         in explicitly for each week.
      2. Anyone who already has a `ShiftEntry` for this week — so a week
         with data can never "hide" that data just because we cleared
         the roster. This also preserves backward compat for existing
         schedules created before the roster existed.

    Terminated users (is_active=False AND password_hash set) are still
    excluded; drafts (is_active=False AND password_hash empty) are
    eligible to be rostered so a new hire can be put on the schedule
    before they finish onboarding.
    """
    week_days = _week_dates(week_start)
    first_day = week_days[0]
    last_day = week_days[-1]

    # The Stream grid has a different population contract: it's the
    # read-only projection of the Stream Manager schedule. Every
    # schedulable Stream-role employee automatically gets a row (no
    # per-week roster), cells are filled from StreamSchedule, and the
    # page never writes into ShiftEntry. We handle that case first so
    # the Storefront branch below can stay focused on its own logic.
    if staff_kind == STAFF_KIND_STREAM:
        return _stream_grid_context(session, week_start, week_days, flash=flash)

    # Roster membership for this week.
    roster_user_ids: set[int] = set(
        session.exec(
            select(ScheduleRosterMember.user_id).where(
                ScheduleRosterMember.week_start == week_start
            )
        ).all()
    )

    # Load shift entries first so we can surface anyone with saved data,
    # even if they were removed from the roster.
    entries = list(
        session.exec(
            select(ShiftEntry).where(
                ShiftEntry.shift_date >= first_day,
                ShiftEntry.shift_date <= last_day,
            )
        ).all()
    )
    entry_map: dict[tuple[int, str], ShiftEntry] = {
        (e.user_id, e.shift_date.isoformat()): e for e in entries
    }
    shifted_user_ids = {e.user_id for e in entries}

    grid_user_ids = roster_user_ids | shifted_user_ids

    users: list[User] = []
    if grid_user_ids:
        users = list(
            session.exec(
                select(User)
                .where(User.id.in_(grid_user_ids))  # type: ignore[attr-defined]
                .where(_not_terminated_clause())
                .order_by(User.display_name, User.username)
            ).all()
        )

    # For the "add employee" picker: all schedulable users (active OR
    # draft) who are NOT already on this week's grid. Sorted like the
    # rest of the portal for consistency.
    schedulable: list[User] = list(
        session.exec(
            select(User)
            .where(_schedulable_clause())
            .order_by(User.display_name, User.username)
        ).all()
    )
    already_on_grid = {u.id for u in users}
    addable_users = [u for u in schedulable if u.id not in already_on_grid]

    # Optional staff_kind filter splits one roster+shift pool into two
    # grids: Storefront floor staff vs Stream room staff. Users default
    # to "storefront" if unset so existing rows render on the
    # Storefront grid as before.
    if staff_kind in STAFF_KINDS:
        def _kind(u: User) -> str:
            return (u.staff_kind or STAFF_KIND_STOREFRONT)
        users = [u for u in users if _kind(u) == staff_kind]
        addable_users = [u for u in addable_users if _kind(u) == staff_kind]

    day_notes = list(
        session.exec(
            select(ScheduleDayNote).where(
                ScheduleDayNote.day_date >= first_day,
                ScheduleDayNote.day_date <= last_day,
            )
        ).all()
    )
    day_note_map: dict[str, ScheduleDayNote] = {
        n.day_date.isoformat(): n for n in day_notes
    }

    prev_week_date = week_start - timedelta(days=7)
    prev_week = prev_week_date.isoformat()
    next_week = (week_start + timedelta(days=7)).isoformat()
    this_week = _monday_of(date.today()).isoformat()

    # Is there a previous-week roster we could copy in one click?
    # When a staff_kind filter is active, only count users of that
    # kind so the button label ("Copy last week (3)") isn't misleading.
    prev_roster_ids = list(
        session.exec(
            select(ScheduleRosterMember.user_id).where(
                ScheduleRosterMember.week_start == prev_week_date
            )
        ).all()
    )
    if staff_kind in STAFF_KINDS and prev_roster_ids:
        prev_users = session.exec(
            select(User).where(User.id.in_(prev_roster_ids))  # type: ignore[attr-defined]
        ).all()
        prev_roster_count = sum(
            1
            for u in prev_users
            if (u.staff_kind or STAFF_KIND_STOREFRONT) == staff_kind
        )
    else:
        prev_roster_count = len(prev_roster_ids)

    # 7shifts-style totals: per-user weekly hours, per-day column totals,
    # grand weekly total, and a raw "how many shift cells were scheduled".
    # We only tally entries for users actually on the grid so rows hidden
    # by a staff_kind filter don't pollute the totals.
    user_ids_on_grid = {u.id for u in users if u.id is not None}
    user_hours: dict[int, float] = {uid: 0.0 for uid in user_ids_on_grid}
    day_hours: dict[str, float] = {d.isoformat(): 0.0 for d in week_days}
    total_shifts = 0
    for (uid, iso), e in entry_map.items():
        if uid not in user_ids_on_grid:
            continue
        hrs = _parse_shift_hours(e.label or "")
        if hrs > 0:
            user_hours[uid] = user_hours.get(uid, 0.0) + hrs
            day_hours[iso] = day_hours.get(iso, 0.0) + hrs
        if (e.kind or "") in ("work", "all_day"):
            total_shifts += 1
    grand_hours = round(sum(user_hours.values()), 2)
    # Count distinct employees who have at least one worked cell.
    people_with_shifts = sum(
        1
        for uid in user_ids_on_grid
        if any(
            (entry_map.get((uid, d.isoformat())) and (entry_map[(uid, d.isoformat())].kind or "") in ("work", "all_day"))
            for d in week_days
        )
    )

    closure_map = _closure_map_for_range(session, first_day, last_day)

    return {
        "week_start": week_start,
        "week_start_iso": week_start.isoformat(),
        "week_days": week_days,
        "users": users,
        "entry_map": entry_map,
        "stream_hint_map": {},
        "stream_legend": [],
        "day_note_map": day_note_map,
        "closure_map": closure_map,
        "roster_user_ids": roster_user_ids,
        "addable_users": addable_users,
        "prev_week": prev_week,
        "next_week": next_week,
        "this_week": this_week,
        "prev_roster_count": prev_roster_count,
        "is_current_week": week_start == _monday_of(date.today()),
        "staff_kind": staff_kind or "",
        "flash": flash,
        "user_hours": user_hours,
        "day_hours": day_hours,
        "grand_hours": grand_hours,
        "total_shifts": total_shifts,
        "people_with_shifts": people_with_shifts,
    }


def _stream_grid_context(
    session: Session,
    week_start: date,
    week_days: list[date],
    *,
    flash: Optional[str] = None,
) -> dict:
    """Read-only context for the Stream grid.

    Auto-rosters every schedulable Stream-role employee, and fills each
    day cell from StreamSchedule rows. There is no edit, roster-add, or
    remove path here — that all lives at /stream-manager.
    """
    # Auto-roster: every schedulable Stream-role user, active or draft.
    users: list[User] = list(
        session.exec(
            select(User)
            .where(_schedulable_clause())
            .where(User.staff_kind == STAFF_KIND_STREAM)
            .order_by(User.display_name, User.username)
        ).all()
    )

    hint_map, legend = _stream_schedule_hint_map(
        session, week_days, {u.id for u in users if u.id is not None}
    )

    prev_week = (week_start - timedelta(days=7)).isoformat()
    next_week = (week_start + timedelta(days=7)).isoformat()
    this_week = _monday_of(date.today()).isoformat()

    closure_map = _closure_map_for_range(session, week_days[0], week_days[-1])

    return {
        "week_start": week_start,
        "week_start_iso": week_start.isoformat(),
        "week_days": week_days,
        "users": users,
        # Stream grid never uses ShiftEntry data. Kept as an empty map
        # so the shared macro's `ctx.entry_map.get(...)` calls stay safe.
        "entry_map": {},
        "stream_hint_map": hint_map,
        "stream_legend": legend,
        # Per-day location headers are a Storefront-only concept.
        "day_note_map": {},
        "closure_map": closure_map,
        "roster_user_ids": set(),
        "addable_users": [],
        "prev_week": prev_week,
        "next_week": next_week,
        "this_week": this_week,
        "prev_roster_count": 0,
        "is_current_week": week_start == _monday_of(date.today()),
        "staff_kind": STAFF_KIND_STREAM,
        "flash": flash,
        # Stream grid is a mirror of StreamSchedule, not tracked as
        # ShiftEntry hours. Kept at zero so the shared totals row in the
        # Jinja macro still renders without special-casing.
        "user_hours": {u.id: 0.0 for u in users if u.id is not None},
        "day_hours": {d.isoformat(): 0.0 for d in week_days},
        "grand_hours": 0.0,
        "total_shifts": 0,
        "people_with_shifts": 0,
    }


def _schedulable_clause():
    """Users eligible to appear in the 'add to schedule' picker.

    Active OR draft employees (is_active=False AND empty password_hash)
    AND explicitly opted-in via User.is_schedulable. The latter gate
    was added in Wave 4.8 so admins can keep non-scheduling roles
    (office admin, owner, etc.) out of the picker without having to
    terminate them. Terminated employees (is_active=False AND
    password_hash set) are excluded regardless.
    """
    from sqlalchemy import and_, or_

    return and_(
        or_(
            User.is_active == True,  # noqa: E712
            User.password_hash == "",
        ),
        User.is_schedulable == True,  # noqa: E712
    )


def _not_terminated_clause():
    """Predicate for filtering ALREADY-SELECTED grid users.

    Intentionally looser than `_schedulable_clause`: we still want to
    render rows for someone who was rostered/shifted while
    `is_schedulable` was True but got toggled off later, so historical
    data isn't silently hidden. Only terminated employees are culled.
    """
    from sqlalchemy import or_

    return or_(
        User.is_active == True,  # noqa: E712
        User.password_hash == "",
    )


@router.get("/team/admin/schedule", response_class=HTMLResponse)
def admin_schedule_view(
    request: Request,
    week: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    edit: int = Query(default=0),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.schedule.view")
    if denial:
        return denial
    week_start = _parse_week_start(week)
    storefront_ctx = _grid_context(
        session, week_start, staff_kind=STAFF_KIND_STOREFRONT, flash=flash
    )
    stream_ctx = _grid_context(
        session, week_start, staff_kind=STAFF_KIND_STREAM
    )
    can_edit = has_permission(session, user, "admin.schedule.edit")
    # Edit mode is opt-in per visit. The default is a clean read-only
    # view for everyone (including admins) — safer on mobile and avoids
    # accidental cell edits. Admins flip it on with ?edit=1.
    edit_mode = bool(can_edit and edit)
    # Stream accounts for the edit modal's account selector. We only
    # need them when edit mode is on; skip the query otherwise.
    stream_accounts: list[StreamAccount] = []
    if edit_mode:
        stream_accounts = list(
            session.exec(
                select(StreamAccount)
                .where(StreamAccount.is_active == True)  # noqa: E712
                .order_by(StreamAccount.sort_order, StreamAccount.name)
            ).all()
        )

    # Holidays modal data. Only computed in edit mode to keep the
    # read-only view cheap. We show the current year + the following
    # year so admins can plan the closure list ahead (eg. check
    # Christmas 2027 in November 2026).
    holiday_options: list[dict] = []
    custom_closures: list[StoreClosure] = []
    if edit_mode:
        today = date.today()
        first_year = min(today.year, week_start.year)
        years_to_show = [first_year, first_year + 1]
        # All existing closures across both modal years — used to
        # pre-check legal boxes and to list custom closures.
        range_first = date(years_to_show[0], 1, 1)
        range_last = date(years_to_show[-1], 12, 31)
        existing_closures = list(
            session.exec(
                select(StoreClosure).where(
                    StoreClosure.day_date >= range_first,
                    StoreClosure.day_date <= range_last,
                ).order_by(StoreClosure.day_date)
            ).all()
        )
        closed_iso = {c.day_date.isoformat(): c for c in existing_closures}
        week_last = week_start + timedelta(days=6)
        for year in years_to_show:
            for key, label, d in _us_legal_holidays(year):
                iso = d.isoformat()
                pretty = f"{d.strftime('%a, %b')} {d.day}"
                holiday_options.append({
                    "key": key,
                    "label": label,
                    "year": year,
                    "date": d,
                    "date_iso": iso,
                    "pretty_date": pretty,
                    "checked": iso in closed_iso,
                    "in_week": week_start <= d <= week_last,
                })
        custom_closures = [c for c in existing_closures if (c.source or "custom") == "custom"]
    # Top-level nav context (week_start / prev_week / etc.) mirrors the
    # storefront grid so the week buttons still work.
    return templates.TemplateResponse(
        request,
        "team/admin/schedule.html",
        {
            "request": request,
            "title": "Schedule",
            "active": "schedule",
            "current_user": user,
            "can_edit": can_edit,
            "edit_mode": edit_mode,
            "stream_accounts": stream_accounts,
            "holiday_options": holiday_options,
            "custom_closures": custom_closures,
            "csrf_token": issue_token(request),
            "build_cell_key": _build_cell_key,
            "build_day_loc_key": _build_day_loc_key,
            "storefront": storefront_ctx,
            "stream": stream_ctx,
            # Shared nav/week state (same across both grids).
            "week_start": storefront_ctx["week_start"],
            "week_start_iso": storefront_ctx["week_start_iso"],
            "week_days": storefront_ctx["week_days"],
            "day_note_map": storefront_ctx["day_note_map"],
            "prev_week": storefront_ctx["prev_week"],
            "next_week": storefront_ctx["next_week"],
            "this_week": storefront_ctx["this_week"],
            "is_current_week": storefront_ctx["is_current_week"],
            "today": date.today(),
            "flash": flash,
        },
    )


@router.post(
    "/team/admin/schedule",
    dependencies=[Depends(require_csrf)],
)
async def admin_schedule_save(
    request: Request,
    session: Session = Depends(get_session),
):
    """Save the entire week grid in one form submission.

    We iterate form fields rather than defining 80+ Form(...) args
    because the grid is dynamic (N employees × 7 days + 7 day-notes).
    Only dirty rows get touched; identical-to-stored values are a no-op
    so the audit log stays clean.
    """
    denial, current = _admin_gate(request, session, "admin.schedule.edit")
    if denial:
        return denial
    form = await request.form()

    week_raw = form.get("week") or ""
    week_start = _parse_week_start(week_raw)
    week_days = _week_dates(week_start)
    first_day, last_day = week_days[0], week_days[-1]

    # Stream grid writes into StreamSchedule (same table /stream-manager
    # uses) so edits are bidirectional — whatever an admin edits here
    # shows up in the Stream Manager and vice versa.
    if (form.get("staff_kind") or "").strip() == STAFF_KIND_STREAM:
        return await _save_stream_shifts(
            request, session, current, form, week_start, week_days
        )

    # Only touch cells for users who are actually on this week's grid
    # — the roster plus anyone with an existing shift that week. We
    # never accept cell edits for arbitrary users via raw form keys;
    # a savvy client could otherwise write to employees they shouldn't.
    roster_user_ids: set[int] = set(
        session.exec(
            select(ScheduleRosterMember.user_id).where(
                ScheduleRosterMember.week_start == week_start
            )
        ).all()
    )

    existing_entries = list(
        session.exec(
            select(ShiftEntry).where(
                ShiftEntry.shift_date >= first_day,
                ShiftEntry.shift_date <= last_day,
            )
        ).all()
    )
    entry_map: dict[tuple[int, str], ShiftEntry] = {
        (e.user_id, e.shift_date.isoformat()): e for e in existing_entries
    }
    shifted_user_ids = {e.user_id for e in existing_entries}
    user_ids = roster_user_ids | shifted_user_ids

    now = utcnow()
    touched: int = 0
    emptied: int = 0
    added: int = 0
    recurred: int = 0

    # Collect (uid, iso_date, label, kind, recur_weeks) we applied so
    # recurrence can project the same shift forward on later passes.
    applied_labels: list[tuple[int, date, str, str, int]] = []

    for uid in user_ids:
        for d in week_days:
            key = _build_cell_key(uid, d)
            if key not in form:
                continue
            raw = (form.get(key) or "").strip()
            kind = classify_shift_label(raw)
            entry = entry_map.get((uid, d.isoformat()))
            if entry is None:
                if kind == SHIFT_KIND_BLANK:
                    continue  # nothing to save
                session.add(
                    ShiftEntry(
                        user_id=uid,
                        shift_date=d,
                        label=raw,
                        kind=kind,
                        created_by_user_id=current.id,
                        created_at=now,
                        updated_at=now,
                    )
                )
                added += 1
            else:
                if entry.label == raw and entry.kind == kind:
                    # No-change, but still project recurrence forward.
                    pass
                elif kind == SHIFT_KIND_BLANK and not raw:
                    session.delete(entry)
                    emptied += 1
                else:
                    entry.label = raw
                    entry.kind = kind
                    entry.updated_at = now
                    session.add(entry)
                    touched += 1

            # Capture recurrence intent. A value of N means "this shift
            # plus the next N-1 weeks". Empty cells never recur.
            try:
                recur_n = int(form.get(f"recur__{uid}__{d.isoformat()}") or "1")
            except (TypeError, ValueError):
                recur_n = 1
            recur_n = max(1, min(recur_n, 12))
            if recur_n > 1 and kind != SHIFT_KIND_BLANK and raw:
                applied_labels.append((uid, d, raw, kind, recur_n))

    # Apply recurrence by upserting the same label into the same weekday
    # on each of the next recur_n-1 weeks. We always OVERWRITE the
    # destination cell so "Repeat for 4 weeks" is idempotent — re-saving
    # the same modal won't create drift with an old pre-existing entry.
    if applied_labels:
        max_offset = max(n for _, _, _, _, n in applied_labels)
        future_start = first_day + timedelta(days=7)
        future_end = first_day + timedelta(days=7 * (max_offset - 1)) + timedelta(days=6)
        future_entries = list(
            session.exec(
                select(ShiftEntry).where(
                    ShiftEntry.shift_date >= future_start,
                    ShiftEntry.shift_date <= future_end,
                )
            ).all()
        )
        future_map: dict[tuple[int, date], ShiftEntry] = {
            (e.user_id, e.shift_date): e for e in future_entries
        }
        for uid, base_date, raw, kind, recur_n in applied_labels:
            for i in range(1, recur_n):
                fut = base_date + timedelta(days=7 * i)
                existing = future_map.get((uid, fut))
                if existing is None:
                    session.add(
                        ShiftEntry(
                            user_id=uid,
                            shift_date=fut,
                            label=raw,
                            kind=kind,
                            created_by_user_id=current.id,
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    recurred += 1
                else:
                    if existing.label == raw and existing.kind == kind:
                        continue
                    existing.label = raw
                    existing.kind = kind
                    existing.updated_at = now
                    session.add(existing)
                    recurred += 1

    # Per-day location header (e.g. "East Bay Santa Clara" for the weekend).
    # Same non-clobbering rule: empty input on a day with no existing note
    # is a no-op. Emptying a pre-existing note deletes it.
    existing_notes = list(
        session.exec(
            select(ScheduleDayNote).where(
                ScheduleDayNote.day_date >= first_day,
                ScheduleDayNote.day_date <= last_day,
            )
        ).all()
    )
    note_map: dict[str, ScheduleDayNote] = {
        n.day_date.isoformat(): n for n in existing_notes
    }
    day_note_changes = 0
    for d in week_days:
        key = _build_day_loc_key(d)
        if key not in form:
            continue
        raw_loc = (form.get(key) or "").strip()
        existing = note_map.get(d.isoformat())
        if existing is None:
            if not raw_loc:
                continue
            session.add(
                ScheduleDayNote(
                    day_date=d,
                    location_label=raw_loc,
                    updated_by_user_id=current.id,
                    created_at=now,
                    updated_at=now,
                )
            )
            day_note_changes += 1
        else:
            if existing.location_label == raw_loc:
                continue
            if not raw_loc:
                session.delete(existing)
            else:
                existing.location_label = raw_loc
                existing.updated_by_user_id = current.id
                existing.updated_at = now
                session.add(existing)
            day_note_changes += 1

    total_changes = touched + emptied + added + day_note_changes + recurred
    if total_changes:
        session.add(
            AuditLog(
                actor_user_id=current.id,
                action="admin.schedule.save",
                resource_key="admin.schedule.edit",
                details_json=json.dumps(
                    {
                        "week_start": week_start.isoformat(),
                        "cells_added": added,
                        "cells_updated": touched,
                        "cells_cleared": emptied,
                        "cells_recurred": recurred,
                        "day_headers_changed": day_note_changes,
                    }
                ),
                ip_address=(request.client.host if request.client else None),
            )
        )
        session.commit()
        flash = (
            f"Saved · {added} added · {touched} updated · {emptied} cleared"
            + (f" · {recurred} future week{'s' if recurred != 1 else ''}" if recurred else "")
            + (f" · {day_note_changes} header{'s' if day_note_changes != 1 else ''}" if day_note_changes else "")
        )
    else:
        flash = "No changes."

    from urllib.parse import quote_plus
    return RedirectResponse(
        f"/team/admin/schedule?week={week_start.isoformat()}&flash={quote_plus(flash)}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Stream shift save — called from admin_schedule_save when staff_kind=stream.
#
# Stream shifts live in StreamSchedule (same table /stream-manager uses), so
# anything saved here appears in the Stream Manager and vice versa. Each
# cell hidden-input `stream_cell__{uid}__{date}` carries a JSON payload:
#
#   { "start": "14:00", "end": "18:00", "account_id": 2,
#     "title": "", "notes": "", "recur": 1, "clear": false }
#
# - clear=true deletes any existing StreamSchedule rows for (streamer, date).
# - otherwise: one-shift-per-(streamer, date) semantics — existing rows are
#   wiped and a single new row is inserted. For multi-shift days, admins
#   still use /stream-manager.
# - recur N applies the same upsert to the same weekday in the next N-1
#   weeks (max 12).
# ---------------------------------------------------------------------------


async def _save_stream_shifts(
    request: Request,
    session: Session,
    current: User,
    form,
    week_start: date,
    week_days: list[date],
) -> RedirectResponse:
    from urllib.parse import quote_plus

    iso_dates_this_week = {d.isoformat() for d in week_days}

    # Only edit streamers for users that belong on the Stream grid.
    stream_users: list[User] = list(
        session.exec(
            select(User)
            .where(_schedulable_clause())
            .where(User.staff_kind == STAFF_KIND_STREAM)
        ).all()
    )
    valid_uids = {u.id for u in stream_users if u.id is not None}
    user_by_id = {u.id: u for u in stream_users if u.id is not None}

    # Map user_id -> Streamer row; create on the fly if missing so admins
    # can schedule a newly-flipped Stream-role employee without first
    # visiting /stream-manager to register them.
    existing_streamers = list(
        session.exec(
            select(Streamer).where(Streamer.user_id.in_(valid_uids))  # type: ignore[attr-defined]
        ).all()
    ) if valid_uids else []
    streamer_by_uid: dict[int, Streamer] = {
        s.user_id: s for s in existing_streamers if s.user_id is not None
    }

    def _ensure_streamer(uid: int) -> Optional[Streamer]:
        s = streamer_by_uid.get(uid)
        if s is not None:
            return s
        user = user_by_id.get(uid)
        if user is None:
            return None
        s = Streamer(
            name=(user.display_name or user.username or f"User {uid}"),
            user_id=uid,
            is_active=True,
        )
        session.add(s)
        session.flush()  # get the id
        streamer_by_uid[uid] = s
        return s

    # Default StreamAccount — used when a cell doesn't pick one.
    default_acct = session.exec(
        select(StreamAccount).where(StreamAccount.is_default == True)  # noqa: E712
    ).first()
    default_acct_id = default_acct.id if default_acct else None

    now = utcnow()
    added = 0
    updated = 0
    cleared = 0
    recurred = 0

    # Track what we applied so recurrence can project forward.
    applied: list[tuple[int, date, dict, int]] = []  # (uid, date, payload, recur_n)

    for key in list(form.keys()):
        if not key.startswith("stream_cell__"):
            continue
        parts = key.split("__")
        if len(parts) != 3:
            continue
        try:
            uid = int(parts[1])
        except (TypeError, ValueError):
            continue
        iso = parts[2]
        if uid not in valid_uids or iso not in iso_dates_this_week:
            continue
        raw = (form.get(key) or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue

        streamer = _ensure_streamer(uid)
        if streamer is None or streamer.id is None:
            continue

        # Nuke whatever currently exists for (streamer, date). Keeps the
        # invariant that admin-schedule edits are single-shift-per-day.
        existing_rows = list(
            session.exec(
                select(StreamSchedule).where(
                    StreamSchedule.streamer_id == streamer.id,
                    StreamSchedule.date == iso,
                )
            ).all()
        )

        if payload.get("clear"):
            for row in existing_rows:
                session.delete(row)
                cleared += 1
            continue

        start_t = (payload.get("start") or "").strip()
        end_t = (payload.get("end") or "").strip()
        if not (start_t and end_t):
            continue  # nothing to save without times
        acct_id_raw = payload.get("account_id")
        try:
            acct_id: Optional[int] = int(acct_id_raw) if acct_id_raw else None
        except (TypeError, ValueError):
            acct_id = None
        if acct_id is None:
            acct_id = default_acct_id

        title = (payload.get("title") or "").strip() or None
        notes = (payload.get("notes") or "").strip() or None
        is_overnight = end_t < start_t

        for row in existing_rows:
            session.delete(row)
        session.add(
            StreamSchedule(
                streamer_id=streamer.id,
                stream_account_id=acct_id,
                date=iso,
                start_time=start_t,
                end_time=end_t,
                is_overnight=is_overnight,
                title=title,
                notes=notes,
                created_at=now,
                updated_at=now,
            )
        )
        if existing_rows:
            updated += 1
        else:
            added += 1

        try:
            recur_n = int(payload.get("recur") or 1)
        except (TypeError, ValueError):
            recur_n = 1
        recur_n = max(1, min(recur_n, 12))
        if recur_n > 1:
            try:
                base_date = datetime.strptime(iso, "%Y-%m-%d").date()
            except ValueError:
                base_date = None
            if base_date is not None:
                applied.append((uid, base_date, {
                    "start": start_t, "end": end_t, "account_id": acct_id,
                    "title": title, "notes": notes, "is_overnight": is_overnight,
                }, recur_n))

    # Project recurrence forward.
    for uid, base_date, payload, recur_n in applied:
        streamer = streamer_by_uid.get(uid)
        if streamer is None or streamer.id is None:
            continue
        for i in range(1, recur_n):
            fut = base_date + timedelta(days=7 * i)
            iso_fut = fut.isoformat()
            existing_rows = list(
                session.exec(
                    select(StreamSchedule).where(
                        StreamSchedule.streamer_id == streamer.id,
                        StreamSchedule.date == iso_fut,
                    )
                ).all()
            )
            for row in existing_rows:
                session.delete(row)
            session.add(
                StreamSchedule(
                    streamer_id=streamer.id,
                    stream_account_id=payload["account_id"],
                    date=iso_fut,
                    start_time=payload["start"],
                    end_time=payload["end"],
                    is_overnight=payload["is_overnight"],
                    title=payload["title"],
                    notes=payload["notes"],
                    created_at=now,
                    updated_at=now,
                )
            )
            recurred += 1

    total = added + updated + cleared + recurred
    if total:
        session.add(
            AuditLog(
                actor_user_id=current.id,
                action="admin.schedule.stream_save",
                resource_key="admin.schedule.edit",
                details_json=json.dumps({
                    "week_start": week_start.isoformat(),
                    "added": added,
                    "updated": updated,
                    "cleared": cleared,
                    "recurred": recurred,
                }),
                ip_address=(request.client.host if request.client else None),
            )
        )
        session.commit()
        flash = (
            f"Stream saved · {added} added · {updated} updated · {cleared} cleared"
            + (f" · {recurred} future week{'s' if recurred != 1 else ''}" if recurred else "")
        )
    else:
        flash = "No stream changes."

    return RedirectResponse(
        f"/team/admin/schedule?week={week_start.isoformat()}&flash={quote_plus(flash)}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Generate from previous week — copies shifts from the immediately prior
# week onto the target week for a blank/partial week. Handles both grids:
#   staff_kind=storefront -> clones ShiftEntry rows
#   staff_kind=stream     -> clones StreamSchedule rows
# Never touches cells that already have a value on the target week.
# ---------------------------------------------------------------------------


@router.post(
    "/team/admin/schedule/generate-from-previous",
    dependencies=[Depends(require_csrf)],
)
async def admin_schedule_generate_from_previous(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.schedule.edit")
    if denial:
        return denial
    form = await request.form()
    week_start = _parse_week_start(form.get("week") or "")
    week_days = _week_dates(week_start)
    first_day, last_day = week_days[0], week_days[-1]
    prev_start = week_start - timedelta(days=7)
    prev_first = prev_start
    prev_last = prev_start + timedelta(days=6)

    form_kind = (form.get("staff_kind") or "").strip().lower()
    staff_kind = form_kind if form_kind in STAFF_KINDS else STAFF_KIND_STOREFRONT

    now = utcnow()
    added = 0

    if staff_kind == STAFF_KIND_STREAM:
        stream_users: list[User] = list(
            session.exec(
                select(User)
                .where(_schedulable_clause())
                .where(User.staff_kind == STAFF_KIND_STREAM)
            ).all()
        )
        uids = {u.id for u in stream_users if u.id is not None}
        streamers = list(
            session.exec(
                select(Streamer).where(Streamer.user_id.in_(uids))  # type: ignore[attr-defined]
            ).all()
        ) if uids else []
        streamer_ids = {s.id for s in streamers if s.id is not None}
        if not streamer_ids:
            return _redirect_back(week_start, "No stream employees to copy shifts for.")
        prev_rows = list(
            session.exec(
                select(StreamSchedule).where(
                    StreamSchedule.streamer_id.in_(streamer_ids),  # type: ignore[attr-defined]
                    StreamSchedule.date >= prev_first.isoformat(),
                    StreamSchedule.date <= prev_last.isoformat(),
                )
            ).all()
        )
        if not prev_rows:
            return _redirect_back(week_start, "No stream shifts on the previous week to copy.")

        existing_this_week = list(
            session.exec(
                select(StreamSchedule).where(
                    StreamSchedule.streamer_id.in_(streamer_ids),  # type: ignore[attr-defined]
                    StreamSchedule.date >= first_day.isoformat(),
                    StreamSchedule.date <= last_day.isoformat(),
                )
            ).all()
        )
        occupied = {(r.streamer_id, r.date) for r in existing_this_week}
        for row in prev_rows:
            try:
                prev_d = datetime.strptime(row.date, "%Y-%m-%d").date()
            except ValueError:
                continue
            fut = prev_d + timedelta(days=7)
            iso_fut = fut.isoformat()
            if (row.streamer_id, iso_fut) in occupied:
                continue  # don't clobber existing shift on the target week
            session.add(
                StreamSchedule(
                    streamer_id=row.streamer_id,
                    stream_account_id=row.stream_account_id,
                    date=iso_fut,
                    start_time=row.start_time,
                    end_time=row.end_time,
                    is_overnight=row.is_overnight,
                    title=row.title,
                    notes=row.notes,
                    created_at=now,
                    updated_at=now,
                )
            )
            added += 1
    else:
        # Storefront: clone ShiftEntry rows for the roster/shifted set
        # (same "who's on this week" rules admin_schedule_save uses).
        roster_ids: set[int] = set(
            session.exec(
                select(ScheduleRosterMember.user_id).where(
                    ScheduleRosterMember.week_start == week_start
                )
            ).all()
        )
        prev_entries = list(
            session.exec(
                select(ShiftEntry).where(
                    ShiftEntry.shift_date >= prev_first,
                    ShiftEntry.shift_date <= prev_last,
                )
            ).all()
        )
        if not prev_entries:
            return _redirect_back(week_start, "No storefront shifts on the previous week to copy.")

        existing_this_week = list(
            session.exec(
                select(ShiftEntry).where(
                    ShiftEntry.shift_date >= first_day,
                    ShiftEntry.shift_date <= last_day,
                )
            ).all()
        )
        occupied = {(e.user_id, e.shift_date) for e in existing_this_week}

        # Filter to storefront-kind users (users whose staff_kind is
        # either 'storefront' or NULL — the legacy default pre-dating
        # the Stream/Storefront split).
        from sqlalchemy import or_ as _or
        kind_user_ids = set(
            session.exec(
                select(User.id).where(
                    _or(
                        User.staff_kind == STAFF_KIND_STOREFRONT,
                        User.staff_kind.is_(None),  # type: ignore[attr-defined]
                    )
                )
            ).all()
        )
        for entry in prev_entries:
            if entry.user_id not in kind_user_ids and entry.user_id not in roster_ids:
                continue
            fut = entry.shift_date + timedelta(days=7)
            if (entry.user_id, fut) in occupied:
                continue
            session.add(
                ShiftEntry(
                    user_id=entry.user_id,
                    shift_date=fut,
                    label=entry.label,
                    kind=entry.kind,
                    created_by_user_id=current.id,
                    created_at=now,
                    updated_at=now,
                )
            )
            added += 1

    if added == 0:
        return _redirect_back(
            week_start,
            "Nothing to copy — every targeted cell already has a shift this week.",
        )
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="admin.schedule.generate_from_previous",
            resource_key="admin.schedule.edit",
            details_json=json.dumps({
                "week_start": week_start.isoformat(),
                "from_week": prev_start.isoformat(),
                "staff_kind": staff_kind,
                "shifts_added": added,
            }),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _redirect_back(
        week_start,
        f"Generated {added} shift{'s' if added != 1 else ''} from last week.",
    )


# ---------------------------------------------------------------------------
# Holidays / store-closed days
#
# Admins open a modal listing US legal holidays (+ common retail days like
# Christmas Eve) with checkboxes. Whatever is checked on save becomes the
# authoritative set of closures for that year range. Admins can also add
# freehand custom closures (any date + label) and delete them.
# ---------------------------------------------------------------------------


_LEGAL_HOLIDAY_KEYS: set[str] = {
    key for year in (2020,) for (key, _l, _d) in _us_legal_holidays(year)
}


@router.post(
    "/team/admin/schedule/closures/save",
    dependencies=[Depends(require_csrf)],
)
async def admin_schedule_closures_save(
    request: Request,
    session: Session = Depends(get_session),
):
    """Replace the closure set for the years covered by the modal.

    Form conventions:
      - week=YYYY-MM-DD            → where to redirect after save
      - legal_year[]=2026          → years whose legal checklist is in
                                    the payload. Any legal StoreClosure
                                    in those years that is not listed
                                    under `closure_iso[]` is deleted.
      - closure_iso[]=2026-12-25   → ISO dates to keep / create.
      - closure_key[]=christmas_day → parallel list aligned to
                                    closure_iso; "" for custom entries.
      - closure_label[]=Christmas Day → parallel list of display labels.
      - custom_new_date[]=2026-03-15  → any fresh custom rows to add
      - custom_new_label[]=Inventory day → parallel to custom_new_date
      - custom_delete_id[]=7         → custom rows to delete
    """
    denial, current = _admin_gate(request, session, "admin.schedule.edit")
    if denial:
        return denial
    form = await request.form()
    week_start = _parse_week_start(form.get("week") or "")

    # 1) Build the incoming set of (iso, key, label) the admin wants.
    incoming_iso = form.getlist("closure_iso")
    incoming_key = form.getlist("closure_key")
    incoming_label = form.getlist("closure_label")
    incoming: list[tuple[str, str, str]] = []
    for i in range(len(incoming_iso)):
        iso = (incoming_iso[i] or "").strip()
        if not iso:
            continue
        try:
            datetime.strptime(iso, "%Y-%m-%d")
        except ValueError:
            continue
        key = incoming_key[i] if i < len(incoming_key) else ""
        label = (incoming_label[i] if i < len(incoming_label) else "").strip() or iso
        incoming.append((iso, key, label))

    # 2) The years we're fully overwriting — any legal row in these
    #    years that isn't in `incoming` should be deleted so unchecked
    #    boxes persist across the save.
    legal_years: set[int] = set()
    for raw in form.getlist("legal_year"):
        try:
            legal_years.add(int(raw))
        except (TypeError, ValueError):
            pass

    # 3) Pull existing rows for the relevant year range + any extra
    #    dates referenced by incoming (to handle upserts correctly).
    year_first = date(min(legal_years), 1, 1) if legal_years else date(week_start.year, 1, 1)
    year_last = date(max(legal_years), 12, 31) if legal_years else date(week_start.year, 12, 31)
    existing = list(
        session.exec(
            select(StoreClosure).where(
                StoreClosure.day_date >= year_first,
                StoreClosure.day_date <= year_last,
            )
        ).all()
    )
    existing_by_iso: dict[str, StoreClosure] = {r.day_date.isoformat(): r for r in existing}

    now = utcnow()
    added = 0
    updated = 0
    deleted = 0

    # 3a) Upsert each incoming entry.
    incoming_iso_set: set[str] = set()
    for iso, key, label in incoming:
        incoming_iso_set.add(iso)
        row = existing_by_iso.get(iso)
        if row is None:
            # Upsert-by-ISO may miss rows outside the year window; look up again.
            row = session.exec(
                select(StoreClosure).where(
                    StoreClosure.day_date == datetime.strptime(iso, "%Y-%m-%d").date()
                )
            ).first()
        source = "legal" if key in _LEGAL_HOLIDAY_KEYS else "custom"
        if row is None:
            session.add(
                StoreClosure(
                    day_date=datetime.strptime(iso, "%Y-%m-%d").date(),
                    label=label,
                    source=source,
                    holiday_key=key if source == "legal" else "",
                    created_by_user_id=current.id,
                    created_at=now,
                    updated_at=now,
                )
            )
            added += 1
        else:
            changed = False
            if row.label != label:
                row.label = label
                changed = True
            if row.source != source:
                row.source = source
                changed = True
            new_key = key if source == "legal" else ""
            if row.holiday_key != new_key:
                row.holiday_key = new_key
                changed = True
            if changed:
                row.updated_at = now
                session.add(row)
                updated += 1

    # 3b) Legal rows in overwritten years that aren't in incoming → delete.
    for row in existing:
        if (row.source or "custom") != "legal":
            continue
        if row.day_date.year not in legal_years:
            continue
        if row.day_date.isoformat() in incoming_iso_set:
            continue
        session.delete(row)
        deleted += 1

    # 4) Custom-delete list — explicit deletions of custom rows the
    #    admin hit "×" on.
    for raw in form.getlist("custom_delete_id"):
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            continue
        target = session.get(StoreClosure, cid)
        if target is None:
            continue
        if (target.source or "custom") != "custom":
            continue
        session.delete(target)
        deleted += 1

    # 5) New custom rows added inline.
    new_dates = form.getlist("custom_new_date")
    new_labels = form.getlist("custom_new_label")
    for i, raw_date in enumerate(new_dates):
        raw_date = (raw_date or "").strip()
        if not raw_date:
            continue
        try:
            d_new = datetime.strptime(raw_date, "%Y-%m-%d").date()
        except ValueError:
            continue
        raw_label = (new_labels[i] if i < len(new_labels) else "").strip() or "Closed"
        # Don't duplicate an existing date — upsert-ish behaviour: if
        # there's already a row (legal or custom) for this date, bump
        # its label instead of adding a second row.
        existing_same = session.exec(
            select(StoreClosure).where(StoreClosure.day_date == d_new)
        ).first()
        if existing_same is not None:
            if existing_same.label != raw_label or (existing_same.source or "custom") != "custom":
                existing_same.label = raw_label
                existing_same.source = "custom"
                existing_same.holiday_key = ""
                existing_same.updated_at = now
                session.add(existing_same)
                updated += 1
            continue
        session.add(
            StoreClosure(
                day_date=d_new,
                label=raw_label,
                source="custom",
                holiday_key="",
                created_by_user_id=current.id,
                created_at=now,
                updated_at=now,
            )
        )
        added += 1

    total = added + updated + deleted
    if total:
        session.add(
            AuditLog(
                actor_user_id=current.id,
                action="admin.schedule.closures_save",
                resource_key="admin.schedule.edit",
                details_json=json.dumps({
                    "added": added,
                    "updated": updated,
                    "deleted": deleted,
                    "legal_years": sorted(legal_years),
                }),
                ip_address=(request.client.host if request.client else None),
            )
        )
        session.commit()
        parts = []
        if added:
            parts.append(f"{added} added")
        if updated:
            parts.append(f"{updated} updated")
        if deleted:
            parts.append(f"{deleted} removed")
        flash = "Holidays saved · " + " · ".join(parts)
    else:
        flash = "No holiday changes."

    from urllib.parse import quote_plus
    return RedirectResponse(
        f"/team/admin/schedule?week={week_start.isoformat()}&edit=1&flash={quote_plus(flash)}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Per-week roster management
#
# The grid no longer auto-populates with every active employee. Admins add
# people to each week explicitly. These endpoints cover the common ops:
#   - add one employee to this week
#   - remove one employee from this week (optionally clearing their saved
#     shifts for that week)
#   - copy the entire previous week's roster forward in one click
# ---------------------------------------------------------------------------


def _redirect_back(week_start: date, flash: str) -> RedirectResponse:
    from urllib.parse import quote_plus
    return RedirectResponse(
        f"/team/admin/schedule?week={week_start.isoformat()}&flash={quote_plus(flash)}",
        status_code=303,
    )


@router.post(
    "/team/admin/schedule/roster/add",
    dependencies=[Depends(require_csrf)],
)
async def admin_schedule_roster_add(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.schedule.edit")
    if denial:
        return denial
    form = await request.form()
    week_start = _parse_week_start(form.get("week") or "")
    if (form.get("staff_kind") or "").strip().lower() == STAFF_KIND_STREAM:
        return _redirect_back(
            week_start, "Stream schedule is managed in the Stream Manager."
        )
    try:
        user_id = int(form.get("user_id") or 0)
    except (TypeError, ValueError):
        user_id = 0

    if user_id <= 0:
        return _redirect_back(week_start, "Pick an employee to add.")

    target = session.get(User, user_id)
    if target is None:
        return _redirect_back(week_start, "Employee not found.")
    # Schedulable = active OR draft, AND explicitly flagged is_schedulable.
    # Terminated employees can't be scheduled; neither can employees an
    # admin has opted out of the schedule picker.
    is_draft = (not target.is_active) and (target.password_hash or "") == ""
    if not (target.is_active or is_draft):
        return _redirect_back(week_start, "That employee is not schedulable.")
    if not target.is_schedulable:
        return _redirect_back(
            week_start,
            "That employee isn't marked 'on the schedule'. Turn it on from the Employees page first.",
        )

    # Enforce staff_kind match when a grid specifies it. This prevents
    # a crafted request from adding a Stream user to the Storefront
    # grid (or vice versa), and gives a clear error if the admin
    # picks somebody whose Type has since changed.
    form_kind = (form.get("staff_kind") or "").strip().lower()
    if form_kind in STAFF_KINDS:
        user_kind = (target.staff_kind or STAFF_KIND_STOREFRONT)
        if user_kind != form_kind:
            return _redirect_back(
                week_start,
                f"{target.display_name or target.username} is marked as {user_kind}, not {form_kind}. Change their Type first.",
            )

    existing = session.exec(
        select(ScheduleRosterMember).where(
            ScheduleRosterMember.week_start == week_start,
            ScheduleRosterMember.user_id == user_id,
        )
    ).first()
    if existing is not None:
        return _redirect_back(
            week_start, f"{target.display_name or target.username} is already on this week."
        )

    session.add(
        ScheduleRosterMember(
            week_start=week_start,
            user_id=user_id,
            added_by_user_id=current.id,
            created_at=utcnow(),
        )
    )
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="admin.schedule.roster_add",
            resource_key="admin.schedule.edit",
            details_json=json.dumps(
                {
                    "week_start": week_start.isoformat(),
                    "user_id": user_id,
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _redirect_back(
        week_start, f"Added {target.display_name or target.username} to this week."
    )


@router.post(
    "/team/admin/schedule/roster/remove",
    dependencies=[Depends(require_csrf)],
)
async def admin_schedule_roster_remove(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.schedule.edit")
    if denial:
        return denial
    form = await request.form()
    week_start = _parse_week_start(form.get("week") or "")
    if (form.get("staff_kind") or "").strip().lower() == STAFF_KIND_STREAM:
        return _redirect_back(
            week_start, "Stream schedule is managed in the Stream Manager."
        )
    try:
        user_id = int(form.get("user_id") or 0)
    except (TypeError, ValueError):
        user_id = 0

    if user_id <= 0:
        return _redirect_back(week_start, "Missing employee.")

    target = session.get(User, user_id)

    membership = session.exec(
        select(ScheduleRosterMember).where(
            ScheduleRosterMember.week_start == week_start,
            ScheduleRosterMember.user_id == user_id,
        )
    ).first()
    if membership is not None:
        session.delete(membership)

    # Also clear their saved shifts for this week so they actually drop
    # off the grid (otherwise they'd re-appear via the "has shifts this
    # week" union). We DO want this to be deliberate — it's the whole
    # point of removing someone from a week.
    week_days = _week_dates(week_start)
    cleared = 0
    entries = list(
        session.exec(
            select(ShiftEntry).where(
                ShiftEntry.user_id == user_id,
                ShiftEntry.shift_date >= week_days[0],
                ShiftEntry.shift_date <= week_days[-1],
            )
        ).all()
    )
    for e in entries:
        session.delete(e)
        cleared += 1

    if membership is None and cleared == 0:
        return _redirect_back(week_start, "Not on this week's roster.")

    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="admin.schedule.roster_remove",
            resource_key="admin.schedule.edit",
            details_json=json.dumps(
                {
                    "week_start": week_start.isoformat(),
                    "user_id": user_id,
                    "cells_cleared": cleared,
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    name = (target.display_name or target.username) if target else f"#{user_id}"
    tail = f" ({cleared} shift{'s' if cleared != 1 else ''} cleared)" if cleared else ""
    return _redirect_back(week_start, f"Removed {name} from this week{tail}.")


@router.post(
    "/team/admin/schedule/roster/copy-previous",
    dependencies=[Depends(require_csrf)],
)
async def admin_schedule_roster_copy_previous(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, current = _admin_gate(request, session, "admin.schedule.edit")
    if denial:
        return denial
    form = await request.form()
    week_start = _parse_week_start(form.get("week") or "")
    if (form.get("staff_kind") or "").strip().lower() == STAFF_KIND_STREAM:
        return _redirect_back(
            week_start, "Stream schedule is managed in the Stream Manager."
        )
    prev_week = week_start - timedelta(days=7)

    form_kind = (form.get("staff_kind") or "").strip().lower()
    kind_filter = form_kind if form_kind in STAFF_KINDS else None

    existing_ids: set[int] = set(
        session.exec(
            select(ScheduleRosterMember.user_id).where(
                ScheduleRosterMember.week_start == week_start
            )
        ).all()
    )
    prev_ids: list[int] = list(
        session.exec(
            select(ScheduleRosterMember.user_id).where(
                ScheduleRosterMember.week_start == prev_week
            )
        ).all()
    )
    if not prev_ids:
        return _redirect_back(week_start, "No roster on the previous week to copy.")

    added = 0
    now = utcnow()
    for uid in prev_ids:
        if uid in existing_ids:
            continue
        # Skip users who got terminated since last week. Keep drafts in
        # because they're still schedulable. Also skip users whom an
        # admin has since removed from the schedule picker.
        target = session.get(User, uid)
        if target is None:
            continue
        is_draft = (not target.is_active) and (target.password_hash or "") == ""
        if not (target.is_active or is_draft):
            continue
        if not target.is_schedulable:
            continue
        # When the copy button was pressed on the Stream grid, only
        # bring over stream-kind users (and vice versa).
        if kind_filter is not None:
            user_kind = (target.staff_kind or STAFF_KIND_STOREFRONT)
            if user_kind != kind_filter:
                continue
        session.add(
            ScheduleRosterMember(
                week_start=week_start,
                user_id=uid,
                added_by_user_id=current.id,
                created_at=now,
            )
        )
        added += 1

    if added == 0:
        return _redirect_back(week_start, "Everyone from last week is already on this week.")

    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="admin.schedule.roster_copy_previous",
            resource_key="admin.schedule.edit",
            details_json=json.dumps(
                {
                    "week_start": week_start.isoformat(),
                    "from_week": prev_week.isoformat(),
                    "users_added": added,
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _redirect_back(
        week_start,
        f"Copied {added} employee{'s' if added != 1 else ''} from last week.",
    )
