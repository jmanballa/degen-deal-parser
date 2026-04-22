"""
/team/admin/* — admin-only portal routes (Wave 2).

Scope: landing page + permissions matrix UI. Employee-management pages and
supply-queue content live in Wave 4.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from .. import permissions as perms
from ..auth import has_permission
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..shared import require_role_response, templates

router = APIRouter()


def _portal_gate(request: Request) -> Optional[RedirectResponse]:
    """Hide the entire /team/admin surface when the portal is disabled."""
    if not get_settings().employee_portal_enabled:
        return RedirectResponse("/", status_code=303)
    return None


def _admin_gate(request: Request, session: Session, resource_key: str):
    """Run portal + role + per-resource checks. Returns a Response on denial."""
    if denial := _portal_gate(request):
        return denial, None
    if denial := require_role_response(request, "admin"):
        return denial, None
    user = getattr(request.state, "current_user", None)
    if not has_permission(session, user, resource_key):
        return HTMLResponse(
            "You do not have permission to view this page.", status_code=403
        ), None
    return None, user


@router.get("/team/admin", response_class=HTMLResponse)
def team_admin_home(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.permissions.view")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/admin/index.html",
        {
            "request": request,
            "title": "Team Admin",
            "current_user": user,
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
