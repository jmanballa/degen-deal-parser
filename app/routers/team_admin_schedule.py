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
    ScheduleDayNote,
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
    flash: Optional[str] = None,
) -> dict:
    """Collect all the data the schedule grid template needs.

    Shared between the admin view (editable) and the employee view
    (read-only) so the two render identically. Only active employees
    are included — terminated / drafted employees are hidden from the
    grid. We sort by display_name so it reads like the screenshot.
    """
    week_days = _week_dates(week_start)
    first_day = week_days[0]
    last_day = week_days[-1]

    # Active, onboarded employees only. Drafts (is_active=False) don't
    # belong on a published grid yet.
    users: list[User] = list(
        session.exec(
            select(User)
            .where(User.is_active == True)  # noqa: E712
            .order_by(User.display_name, User.username)
        ).all()
    )

    entries = list(
        session.exec(
            select(ShiftEntry).where(
                ShiftEntry.shift_date >= first_day,
                ShiftEntry.shift_date <= last_day,
            )
        ).all()
    )
    # (user_id, iso_date) -> ShiftEntry
    entry_map: dict[tuple[int, str], ShiftEntry] = {
        (e.user_id, e.shift_date.isoformat()): e for e in entries
    }

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

    prev_week = (week_start - timedelta(days=7)).isoformat()
    next_week = (week_start + timedelta(days=7)).isoformat()
    this_week = _monday_of(date.today()).isoformat()

    return {
        "week_start": week_start,
        "week_start_iso": week_start.isoformat(),
        "week_days": week_days,
        "users": users,
        "entry_map": entry_map,
        "day_note_map": day_note_map,
        "prev_week": prev_week,
        "next_week": next_week,
        "this_week": this_week,
        "is_current_week": week_start == _monday_of(date.today()),
        "flash": flash,
    }


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
    ctx = _grid_context(session, _parse_week_start(week), flash=flash)
    can_edit = has_permission(session, user, "admin.schedule.edit")
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
            **ctx,
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

    users = list(
        session.exec(
            select(User)
            .where(User.is_active == True)  # noqa: E712
        ).all()
    )
    user_ids = {u.id for u in users if u.id is not None}

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
