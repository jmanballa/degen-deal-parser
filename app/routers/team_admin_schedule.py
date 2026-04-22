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

    return {
        "week_start": week_start,
        "week_start_iso": week_start.isoformat(),
        "week_days": week_days,
        "users": users,
        "entry_map": entry_map,
        "day_note_map": day_note_map,
        "roster_user_ids": roster_user_ids,
        "addable_users": addable_users,
        "prev_week": prev_week,
        "next_week": next_week,
        "this_week": this_week,
        "prev_roster_count": prev_roster_count,
        "is_current_week": week_start == _monday_of(date.today()),
        "staff_kind": staff_kind or "",
        "flash": flash,
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
                    continue
                if kind == SHIFT_KIND_BLANK and not raw:
                    session.delete(entry)
                    emptied += 1
                else:
                    entry.label = raw
                    entry.kind = kind
                    entry.updated_at = now
                    session.add(entry)
                    touched += 1

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

    total_changes = touched + emptied + added + day_note_changes
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
                        "day_headers_changed": day_note_changes,
                    }
                ),
                ip_address=(request.client.host if request.client else None),
            )
        )
        session.commit()
        flash = (
            f"Saved · {added} added · {touched} updated · {emptied} cleared"
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
