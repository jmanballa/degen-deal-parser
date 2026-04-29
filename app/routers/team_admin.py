"""
/team/admin/* — admin-only portal routes (Wave 2).

Scope: landing page + permissions matrix UI. Employee-management pages and
supply-queue content live in Wave 4.
"""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from .. import permissions as perms
from ..auth import has_permission
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import (
    AuditLog,
    EmployeeProfile,
    InviteToken,
    ShiftEntry,
    SupplyRequest,
    TeamAnnouncement,
    TimeOffRequest,
    TimecardApproval,
    User,
    utcnow,
)
from ..shared import templates
from sqlmodel import select

router = APIRouter()


TEAM_ADMIN_NAV_SECTIONS: tuple[tuple[str, tuple[tuple[str, str, str, str], ...]], ...] = (
    (
        "",
        (
            ("overview", "Overview", "/team/admin", "admin.permissions.view"),
            ("permissions", "Permissions", "/team/admin/permissions", "admin.permissions.view"),
        ),
    ),
    (
        "People",
        (
            ("employees", "Employees", "/team/admin/employees", "admin.employees.view"),
            (
                "password-resets",
                "Reset requests",
                "/team/admin/password-reset-requests",
                "admin.employees.reset_password",
            ),
            ("pay-rates", "Compensation", "/team/admin/employees/pay-rates", "admin.labor_financials.view"),
            ("clockify", "Clockify", "/team/admin/clockify", "admin.employees.view"),
            ("shift-tracker", "Shift Tracker", "/team/admin/shift-tracker", "admin.employees.view"),
            ("labor-stats", "Labor Stats", "/team/admin/labor-stats", "admin.employees.view"),
            ("payroll", "Payroll Export", "/team/admin/payroll", "admin.payroll.view"),
            ("exceptions", "Exceptions", "/team/admin/exceptions", "admin.employees.view"),
            ("schedule", "Schedule", "/team/admin/schedule", "admin.schedule.view"),
            ("announcements", "Announcements", "/team/admin/announcements", "admin.announcements.view"),
            ("policies", "Policies", "/team/admin/policies", "admin.policies.view"),
            ("invites", "Invites", "/team/admin/invites", "admin.invites.view"),
            ("supply", "Supply queue", "/team/admin/supply", "admin.supply.view"),
            ("time-off", "Time off", "/team/admin/timeoff", "admin.timeoff.view"),
        ),
    ),
)


def _build_team_admin_nav(
    session: Session,
    user: User,
) -> list[dict[str, Any]]:
    cache: dict[tuple[str, str], bool] = {}
    sections: list[dict[str, Any]] = []
    for group, items in TEAM_ADMIN_NAV_SECTIONS:
        visible_items = []
        for active, label, href, resource_key in items:
            if has_permission(session, user, resource_key, cache=cache):
                visible_items.append(
                    {
                        "active": active,
                        "label": label,
                        "href": href,
                        "resource_key": resource_key,
                    }
                )
        if visible_items:
            sections.append({"group": group, "items": visible_items})
    return sections


def _set_team_admin_state(request: Request, session: Session, user: User) -> None:
    request.state.can_view_admin_announcements = has_permission(
        session, user, "admin.announcements.view"
    )
    request.state.can_view_admin_timeoff = has_permission(
        session, user, "admin.timeoff.view"
    )
    request.state.team_admin_nav_sections = _build_team_admin_nav(session, user)


def _admin_denied_response(
    request: Request,
    session: Session,
    user: Optional[User],
    *,
    message: str = "You do not have permission to view this admin page.",
    status_code: int = 403,
):
    if user is not None:
        request.state.current_user = user
        _set_team_admin_state(request, session, user)
    if not hasattr(request, "url"):
        return HTMLResponse(message, status_code=status_code)
    return templates.TemplateResponse(
        request,
        "team/admin/access_denied.html",
        {
            "request": request,
            "title": "Access denied",
            "current_user": user,
            "message": message,
            "back_href": _first_allowed_admin_href(request) if user is not None else "/team/",
        },
        status_code=status_code,
    )


def _portal_gate(request: Request) -> Optional[RedirectResponse]:
    """Hide the entire /team/admin surface when the portal is disabled."""
    if not get_settings().employee_portal_enabled:
        return RedirectResponse("/", status_code=303)
    return None


def _admin_gate(request: Request, session: Session, resource_key: str):
    """Run portal + admin-role + per-resource checks. Returns a Response on denial."""
    if denial := _portal_gate(request):
        return denial, None
    user = getattr(request.state, "current_user", None)
    if user is None:
        from ..shared import get_request_user, redirect_to_login

        user = get_request_user(request)
        if user is None:
            return redirect_to_login(request), None
        request.state.current_user = user
    if getattr(user, "role", None) != "admin":
        return _admin_denied_response(request, session, user), None
    if not has_permission(session, user, resource_key):
        return _admin_denied_response(request, session, user), None
    _set_team_admin_state(request, session, user)
    return None, user


def _permission_gate(request: Request, session: Session, resource_key: str):
    """Portal + authenticated + per-resource permission with an admin-surface
    role floor. Only admin/manager/reviewer may enter permission-gated
    /team/admin pages, even if a lower role somehow holds the permission.
    """
    if denial := _portal_gate(request):
        return denial, None
    user = getattr(request.state, "current_user", None)
    if user is None:
        from ..shared import get_request_user, redirect_to_login

        user = get_request_user(request)
        if user is None:
            return redirect_to_login(request), None
        request.state.current_user = user
    if getattr(user, "role", None) not in {"admin", "manager", "reviewer"}:
        return _admin_denied_response(request, session, user), None
    if not has_permission(session, user, resource_key):
        return _admin_denied_response(request, session, user), None
    _set_team_admin_state(request, session, user)
    return None, user


def _team_admin_surface_gate(request: Request, session: Session):
    """Allow any privileged portal role into /team/admin, then send them to
    the first admin page they can actually use.
    """
    if denial := _portal_gate(request):
        return denial, None
    user = getattr(request.state, "current_user", None)
    if user is None:
        from ..shared import get_request_user, redirect_to_login

        user = get_request_user(request)
        if user is None:
            return redirect_to_login(request), None
        request.state.current_user = user
    if getattr(user, "role", None) not in {"admin", "manager", "reviewer"}:
        return _admin_denied_response(request, session, user), None
    _set_team_admin_state(request, session, user)
    return None, user


def _first_allowed_admin_href(request: Request) -> str:
    sections = getattr(request.state, "team_admin_nav_sections", [])
    visible = [
        item["href"]
        for section in sections
        for item in section.get("items", [])
    ]
    for preferred in (
        "/team/admin/schedule",
        "/team/admin/supply",
        "/team/admin/timeoff",
        "/team/admin/announcements",
        "/team/admin/employees",
    ):
        if preferred in visible:
            return preferred
    return visible[0] if visible else "/team/"


def _admin_page_guide_rows(request: Request) -> list[dict[str, str | bool]]:
    visible_hrefs = {
        item["href"]
        for section in getattr(request.state, "team_admin_nav_sections", [])
        for item in section.get("items", [])
    }
    rows: list[dict[str, str | bool]] = [
        {
            "label": "Overview",
            "href": "/team/admin",
            "job": "Start here when you are not sure what needs attention.",
            "details": "Shows request queues, setup health, active staff, Clockify setup, upcoming shifts, and shortcuts into common workflows.",
        },
        {
            "label": "Employees",
            "href": "/team/admin/employees",
            "job": "Find or update a staff profile.",
            "details": "Use it for roles, contact/profile cleanup, schedule eligibility, staff type, invite status, and employee detail pages.",
        },
        {
            "label": "Reset requests",
            "href": "/team/admin/password-reset-requests",
            "job": "Help employees get back into the portal.",
            "details": "Review password reset requests and issue reset links without hunting through employee profiles.",
        },
        {
            "label": "Compensation",
            "href": "/team/admin/employees/pay-rates",
            "job": "Manage hourly and salary data.",
            "details": "Financial access only. This is where pay rates and compensation history live.",
        },
        {
            "label": "Clockify",
            "href": "/team/admin/clockify",
            "job": "Connect portal employees to Clockify users.",
            "details": "Estimated pay, labor stats, and shift tracking need these mappings to be correct.",
        },
        {
            "label": "Shift Tracker",
            "href": "/team/admin/shift-tracker",
            "job": "See who is clocked in, on break, or missing a clock event.",
            "details": "Uses actual Clockify data. Manual refresh reconciles today's timers when the cache needs a nudge.",
        },
        {
            "label": "Labor Stats",
            "href": "/team/admin/labor-stats",
            "job": "Review hours, labor windows, and pay estimates.",
            "details": "Uses clocked hours and salary/hourly setup. Financial amounts are hidden unless the role is allowed to see them.",
        },
        {
            "label": "Payroll Export",
            "href": "/team/admin/payroll",
            "job": "Prepare payroll output.",
            "details": "Admin/financial workflow for exporting pay-period data after Clockify and compensation are clean.",
        },
        {
            "label": "Exceptions",
            "href": "/team/admin/exceptions",
            "job": "Catch problems before payroll.",
            "details": "Review clock/timecard issues, missing mappings, and other data that may need manager cleanup.",
        },
        {
            "label": "Schedule",
            "href": "/team/admin/schedule",
            "job": "Build and publish the weekly team schedule.",
            "details": "Pick a week, edit cells, add roster people, copy/generate from last week, and save storefront or stream shifts.",
        },
        {
            "label": "Announcements",
            "href": "/team/admin/announcements",
            "job": "Post updates employees will see in the portal.",
            "details": "Create pinned or expiring announcements for schedule changes, shop notes, and team reminders.",
        },
        {
            "label": "Policies",
            "href": "/team/admin/policies",
            "job": "Publish documents employees need to read or sign.",
            "details": "Add policy content, require acknowledgement, and keep employee policy visibility inside the portal.",
        },
        {
            "label": "Invites",
            "href": "/team/admin/invites",
            "job": "Send signup links and track unfinished onboarding.",
            "details": "Use this when creating accounts or resending invite links by SMS.",
        },
        {
            "label": "Supply queue",
            "href": "/team/admin/supply",
            "job": "Handle employee supply requests.",
            "details": "Review requested items, mark work in progress, complete requests, or close requests that are not needed.",
        },
        {
            "label": "Time off",
            "href": "/team/admin/timeoff",
            "job": "Approve or deny time-off requests.",
            "details": "Use before editing the schedule so REQUEST days and staffing gaps do not sneak up on you.",
        },
        {
            "label": "Permissions",
            "href": "/team/admin/permissions",
            "job": "Control what roles can see and do.",
            "details": "Admin-only. This is where financial visibility and manager scope are controlled.",
        },
    ]
    for row in rows:
        row["visible"] = row["href"] in visible_hrefs
    return rows


def _pending_password_reset_request_rows(
    session: Session,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    relevant_actions = (
        "password.reset_manager_request",
        "password.reset_issued",
        "password.reset_consumed",
        "password.reset_sms_sent",
    )
    logs = list(
        session.exec(
            select(AuditLog)
            .where(AuditLog.action.in_(relevant_actions))
            .where(AuditLog.target_user_id.is_not(None))
            .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
            .limit(300)
        ).all()
    )
    latest_by_user: dict[int, AuditLog] = {}
    for row in logs:
        if row.target_user_id is None:
            continue
        latest_by_user.setdefault(int(row.target_user_id), row)
    open_logs = [
        row
        for row in latest_by_user.values()
        if row.action == "password.reset_manager_request"
    ][:limit]
    user_ids = [
        int(row.target_user_id)
        for row in open_logs
        if row.target_user_id is not None
    ]
    users = (
        {
            user.id: user
            for user in session.exec(select(User).where(User.id.in_(user_ids))).all()
            if user.id is not None
        }
        if user_ids
        else {}
    )
    out: list[dict[str, Any]] = []
    for row in open_logs:
        user = users.get(int(row.target_user_id or 0))
        if user is None:
            continue
        try:
            details = json.loads(row.details_json or "{}")
        except json.JSONDecodeError:
            details = {}
        out.append(
            {
                "log": row,
                "user": user,
                "reason": str(details.get("reason") or "manager_action_required"),
                "requested_at": row.created_at,
            }
        )
    return out


@router.get("/team/admin", response_class=HTMLResponse)
def team_admin_home(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _team_admin_surface_gate(request, session)
    if denial:
        return denial
    if not has_permission(session, user, "admin.permissions.view"):
        return RedirectResponse(_first_allowed_admin_href(request), status_code=303)
    now = utcnow()
    all_users = list(session.exec(select(User)).all())
    active_users = [row for row in all_users if row.is_active]
    staff_users = [
        row for row in active_users if row.role in {"employee", "manager", "viewer"}
    ]
    employee_count = len(all_users)
    active_employee_count = len(staff_users)
    outstanding_invites = len(
        list(
            session.exec(
                select(InviteToken).where(
                    InviteToken.used_at.is_(None), InviteToken.expires_at > now
                )
            ).all()
        )
    )
    draft_employee_count = len(
        [
            row
            for row in all_users
            if not row.is_active and row.username.startswith("__draft_")
        ]
    )
    pending_supply = len(
        list(
            session.exec(
                select(SupplyRequest).where(SupplyRequest.status == "submitted")
            ).all()
        )
    )
    pending_timeoff = len(
        list(
            session.exec(
                select(TimeOffRequest).where(TimeOffRequest.status == "submitted")
            ).all()
        )
    )
    pending_timecards = len(
        list(
            session.exec(
                select(TimecardApproval).where(TimecardApproval.status == "pending")
            ).all()
        )
    )
    pending_password_resets = len(_pending_password_reset_request_rows(session))
    active_announcements = len(
        list(
            session.exec(
                select(TeamAnnouncement).where(TeamAnnouncement.is_active == True)  # noqa: E712
            ).all()
        )
    )
    profile_rows = {
        row.user_id: row for row in session.exec(select(EmployeeProfile)).all()
    }
    clockify_mapped = sum(
        1
        for row in staff_users
        if (profile_rows.get(row.id).clockify_user_id if row.id in profile_rows else "")
    )
    clockify_unmapped = max(active_employee_count - clockify_mapped, 0)
    today = now.date()
    upcoming_shift_count = len(
        list(
            session.exec(
                select(ShiftEntry).where(
                    ShiftEntry.shift_date >= today,
                    ShiftEntry.shift_date <= today + timedelta(days=6),
                )
            ).all()
        )
    )
    needs_attention_count = (
        pending_supply + pending_timeoff + pending_timecards + pending_password_resets
    )
    return templates.TemplateResponse(
        request,
        "team/admin/index.html",
        {
            "request": request,
            "title": "Team Admin",
            "current_user": user,
            "employee_count": employee_count,
            "active_employee_count": active_employee_count,
            "outstanding_invites": outstanding_invites,
            "draft_employee_count": draft_employee_count,
            "pending_supply": pending_supply,
            "pending_timeoff": pending_timeoff,
            "pending_timecards": pending_timecards,
            "pending_password_resets": pending_password_resets,
            "active_announcements": active_announcements,
            "clockify_mapped": clockify_mapped,
            "clockify_unmapped": clockify_unmapped,
            "upcoming_shift_count": upcoming_shift_count,
            "needs_attention_count": needs_attention_count,
            "csrf_token": issue_token(request),
        },
    )


@router.get("/team/admin/tutorial", response_class=HTMLResponse)
def team_admin_tutorial(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _team_admin_surface_gate(request, session)
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/admin/tutorial.html",
        {
            "request": request,
            "title": "Admin Guide",
            "active": "tutorial",
            "current_user": user,
            "admin_page_guide": _admin_page_guide_rows(request),
        },
    )


@router.get("/team/admin/password-reset-requests", response_class=HTMLResponse)
def team_admin_password_reset_requests(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.reset_password")
    if denial:
        return denial
    rows = _pending_password_reset_request_rows(session)
    return templates.TemplateResponse(
        request,
        "team/admin/password_reset_requests.html",
        {
            "request": request,
            "title": "Password Reset Requests",
            "current_user": user,
            "rows": rows,
            "csrf_token": issue_token(request),
        },
    )


@router.get("/team/admin/permissions", response_class=HTMLResponse)
def team_admin_permissions(
    request: Request,
    success: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.permissions.view")
    if denial:
        return denial

    matrix = perms.permissions_matrix(session)
    groups = perms.grouped_resource_keys()

    # Advisory: any role whose True-count is zero is effectively locked out.
    locked_out_roles = [
        role for role in perms.ROLES if not any(matrix[role].get(k) for k in perms.RESOURCE_KEYS)
    ]

    labels = {k: perms.resource_label(k) for k in perms.RESOURCE_KEYS}

    return templates.TemplateResponse(
        request,
        "team/admin/permissions.html",
        {
            "request": request,
            "title": "Permissions matrix",
            "current_user": user,
            "csrf_token": issue_token(request),
            "roles": list(perms.ROLES),
            "groups": groups,
            "labels": labels,
            "matrix": matrix,
            "locked_out_roles": locked_out_roles,
            "success": success,
        },
    )


@router.post("/team/admin/permissions/set", dependencies=[Depends(require_csrf)])
async def team_admin_permissions_set(
    request: Request,
    role: str = Form(...),
    resource_key: str = Form(...),
    is_allowed: str = Form(default="0"),
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.permissions.edit")
    if denial:
        return denial
    if resource_key not in perms.RESOURCE_KEYS:
        return HTMLResponse("Unknown resource_key", status_code=400)
    perms.set_permission(
        session,
        role=role,
        resource_key=resource_key,
        is_allowed=is_allowed in ("1", "true", "on", "yes"),
        actor_user_id=getattr(user, "id", None),
    )
    return RedirectResponse(
        f"/team/admin/permissions?success=saved", status_code=303
    )


@router.post("/team/admin/permissions/reset", dependencies=[Depends(require_csrf)])
async def team_admin_permissions_reset(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.permissions.edit")
    if denial:
        return denial
    perms.reset_to_defaults(session, actor_user_id=getattr(user, "id", None))
    return RedirectResponse(
        "/team/admin/permissions?success=reset", status_code=303
    )
