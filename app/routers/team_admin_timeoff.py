"""
/team/admin/timeoff — manager/admin time-off approval queue.

Approval is intentionally non-destructive: it only appends request-kind
ShiftEntry rows and never edits or deletes existing schedule entries.
"""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import update
from sqlmodel import Session, func, select

from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import (
    AuditLog,
    SHIFT_KIND_REQUEST,
    ShiftEntry,
    TimeOffRequest,
    User,
    utcnow,
)
from ..shared import templates
from ..team_notifications import notify_employee
from .team_admin import _permission_gate

router = APIRouter()

VALID_STATUSES = ("submitted", "approved", "denied")


def _queue_redirect(
    message: str,
    *,
    error: bool = False,
) -> RedirectResponse:
    key = "error" if error else "flash"
    return RedirectResponse(
        f"/team/admin/timeoff?{key}={quote_plus(message)}",
        status_code=303,
    )


@router.get("/team/admin/requests")
def admin_requests_alias():
    return RedirectResponse("/team/admin/timeoff", status_code=303)


@router.get("/team/admin/timeoff", response_class=HTMLResponse)
def admin_timeoff_list(
    request: Request,
    status: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.timeoff.view")
    if denial:
        return denial

    filter_status = status if status in VALID_STATUSES else None
    stmt = select(TimeOffRequest).order_by(TimeOffRequest.created_at.asc())
    if filter_status:
        stmt = stmt.where(TimeOffRequest.status == filter_status)
    rows = list(session.exec(stmt).all())
    if filter_status is None:
        rows.sort(
            key=lambda row: (
                0 if row.status == "submitted" else 1,
                row.created_at,
                row.id or 0,
            )
        )

    submitter_ids = {row.submitted_by_user_id for row in rows}
    submitters: dict[int, User] = {}
    if submitter_ids:
        submitters = {
            user.id: user
            for user in session.exec(
                select(User).where(User.id.in_(submitter_ids))
            ).all()
            if user.id is not None
        }

    count_rows = session.exec(
        select(TimeOffRequest.status, func.count()).group_by(TimeOffRequest.status)
    ).all()
    counts = {status_name: int(count) for status_name, count in count_rows}
    for status_name in VALID_STATUSES:
        counts.setdefault(status_name, 0)

    return templates.TemplateResponse(
        request,
        "team/admin/timeoff.html",
        {
            "request": request,
            "title": "Time off queue",
            "active": "time-off",
            "current_user": current,
            "requests": rows,
            "submitters": submitters,
            "filter_status": filter_status,
            "statuses": VALID_STATUSES,
            "counts": counts,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
            "can_view_admin_timeoff": True,
        },
    )


def _ensure_timeoff_shift_entries(
    session: Session,
    *,
    row: TimeOffRequest,
    actor: User,
) -> int:
    note = f"Approved time-off request #{row.id}"
    created = 0
    day = row.start_date
    while day <= row.end_date:
        existing_request_entry = session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == row.submitted_by_user_id)
            .where(ShiftEntry.shift_date == day)
            .where(ShiftEntry.kind == SHIFT_KIND_REQUEST)
            .where(ShiftEntry.notes == note)
        ).first()
        if existing_request_entry is not None:
            day += timedelta(days=1)
            continue

        entries_for_day = list(
            session.exec(
                select(ShiftEntry)
                .where(ShiftEntry.user_id == row.submitted_by_user_id)
                .where(ShiftEntry.shift_date == day)
            ).all()
        )
        if entries_for_day:
            sort_order = max(entry.sort_order for entry in entries_for_day) + 1
        else:
            sort_order = 0
        session.add(
            ShiftEntry(
                user_id=row.submitted_by_user_id,
                shift_date=day,
                label="Time off",
                kind=SHIFT_KIND_REQUEST,
                notes=note,
                created_by_user_id=actor.id,
                sort_order=sort_order,
            )
        )
        created += 1
        day += timedelta(days=1)
    return created


def _transition_timeoff(
    session: Session,
    *,
    request_id: int,
    actor: User,
    new_status: str,
    action: str,
    decision_notes: str = "",
    request: Optional[Request] = None,
) -> Optional[HTMLResponse]:
    row = session.get(TimeOffRequest, request_id)
    if row is None:
        return HTMLResponse("Time-off request not found", status_code=404)

    clean_notes = (decision_notes or "").strip()[:2000]
    if row.status == new_status:
        if new_status == "approved":
            created = _ensure_timeoff_shift_entries(session, row=row, actor=actor)
            if created:
                session.commit()
        return None

    if row.status != "submitted":
        return HTMLResponse(
            "Time-off request has already been decided.",
            status_code=409,
        )

    now = utcnow()
    transition = session.exec(
        update(TimeOffRequest)
        .where(
            TimeOffRequest.id == request_id,
            TimeOffRequest.status == "submitted",
        )
        .values(
            status=new_status,
            approved_by_user_id=actor.id,
            status_changed_at=now,
            decision_notes=clean_notes,
            updated_at=now,
        )
        .execution_options(synchronize_session=False)
    )
    if int(transition.rowcount or 0) != 1:
        session.rollback()
        return HTMLResponse(
            "Time-off request has already been decided.",
            status_code=409,
        )
    session.refresh(row)

    shift_entries_created = 0
    if new_status == "approved":
        shift_entries_created = _ensure_timeoff_shift_entries(
            session,
            row=row,
            actor=actor,
        )

    session.add(
        AuditLog(
            actor_user_id=actor.id,
            target_user_id=row.submitted_by_user_id,
            action=action,
            resource_key="admin.timeoff.approve",
            details_json=json.dumps(
                {
                    "time_off_request_id": request_id,
                    "status": new_status,
                    "shift_entries_created": shift_entries_created,
                }
            ),
            ip_address=(request.client.host if request and request.client else None),
        )
    )
    notify_employee(
        session,
        user_id=row.submitted_by_user_id,
        actor_user_id=actor.id,
        kind=f"timeoff_{new_status}",
        title=f"Time off {new_status}",
        body=(
            f"{row.start_date.strftime('%b %d')} - {row.end_date.strftime('%b %d')}"
            + (f": {clean_notes}" if clean_notes else "")
        ),
        link_path="/team/timeoff",
        request=request,
    )
    session.commit()
    return None


@router.post(
    "/team/admin/timeoff/{request_id}/approve",
    dependencies=[Depends(require_csrf)],
)
async def admin_timeoff_approve(
    request: Request,
    request_id: int,
    decision_notes: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.timeoff.approve")
    if denial:
        return denial
    err = _transition_timeoff(
        session,
        request_id=request_id,
        actor=current,
        new_status="approved",
        action="timeoff.approved",
        decision_notes=decision_notes,
        request=request,
    )
    if err:
        if err.status_code == 409:
            return _queue_redirect("Time-off request has already been decided.", error=True)
        return err
    return _queue_redirect("Approved.")


@router.post(
    "/team/admin/timeoff/{request_id}/deny",
    dependencies=[Depends(require_csrf)],
)
async def admin_timeoff_deny(
    request: Request,
    request_id: int,
    decision_notes: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.timeoff.approve")
    if denial:
        return denial
    err = _transition_timeoff(
        session,
        request_id=request_id,
        actor=current,
        new_status="denied",
        action="timeoff.denied",
        decision_notes=decision_notes,
        request=request,
    )
    if err:
        if err.status_code == 409:
            return _queue_redirect("Time-off request has already been decided.", error=True)
        return err
    return _queue_redirect("Denied.")
