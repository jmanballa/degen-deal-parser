"""
/team/timeoff — employee-facing time-off requests.

Patterned after the supply request flow: authenticated employees can submit
and review their own requests, while approval is handled in the admin router.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, TimeOffRequest
from ..rate_limit import rate_limited_or_429
from ..shared import templates
from .team import _nav_context, _require_employee

router = APIRouter()


def _timeoff_redirect(message: str, *, error: bool = False) -> RedirectResponse:
    key = "error" if error else "flash"
    return RedirectResponse(
        f"/team/timeoff?{key}={quote_plus(message)}",
        status_code=303,
    )


def _parse_iso_date(value: str) -> Optional[date]:
    try:
        return date.fromisoformat((value or "").strip())
    except ValueError:
        return None


@router.get("/team/requests")
def team_requests_alias():
    return RedirectResponse("/team/timeoff", status_code=303)


@router.get("/team/timeoff", response_class=HTMLResponse)
def team_timeoff(
    request: Request,
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.timeoff")
    if denial:
        return denial

    rows = session.exec(
        select(TimeOffRequest)
        .where(TimeOffRequest.submitted_by_user_id == user.id)
        .order_by(TimeOffRequest.created_at.desc())
    ).all()
    return templates.TemplateResponse(
        request,
        "team/timeoff.html",
        {
            "request": request,
            "title": "Time off",
            "active": "time-off",
            "current_user": user,
            "requests": list(rows),
            "flash": flash,
            "error": error,
            "today": date.today().isoformat(),
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post("/team/timeoff", dependencies=[Depends(require_csrf)])
async def team_timeoff_post(
    request: Request,
    start_date: str = Form(default=""),
    end_date: str = Form(default=""),
    reason: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.timeoff.submit"
    )
    if denial:
        return denial

    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:timeoff:{user.id}",
        max_requests=10,
        window_seconds=3600.0,
    ):
        return limited

    parsed_start = _parse_iso_date(start_date)
    parsed_end = _parse_iso_date(end_date)
    if parsed_start is None or parsed_end is None:
        return _timeoff_redirect("Start and end dates must be valid.", error=True)
    if parsed_start > parsed_end:
        return _timeoff_redirect("End date must be on or after start date.", error=True)
    if parsed_start < date.today():
        return _timeoff_redirect("Start date cannot be in the past.", error=True)
    if (parsed_end - parsed_start).days > 90:
        return _timeoff_redirect("Time-off requests cannot span more than 90 days.", error=True)

    overlapping = session.exec(
        select(TimeOffRequest)
        .where(TimeOffRequest.submitted_by_user_id == user.id)
        .where(TimeOffRequest.status.in_(("submitted", "approved")))
        .where(TimeOffRequest.start_date <= parsed_end)
        .where(TimeOffRequest.end_date >= parsed_start)
    ).first()
    if overlapping is not None:
        return _timeoff_redirect(
            "You already have a pending request for those dates.",
            error=True,
        )

    row = TimeOffRequest(
        submitted_by_user_id=user.id,
        start_date=parsed_start,
        end_date=parsed_end,
        reason=(reason or "").strip()[:2000],
        status="submitted",
    )
    session.add(row)
    session.flush()
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="timeoff.submitted",
            resource_key="action.timeoff.submit",
            details_json=json.dumps(
                {
                    "time_off_request_id": row.id,
                    "start_date": parsed_start.isoformat(),
                    "end_date": parsed_end.isoformat(),
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _timeoff_redirect("Time-off request submitted.")
