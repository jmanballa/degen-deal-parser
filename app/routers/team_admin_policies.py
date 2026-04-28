"""/team/admin/policies - policy publishing for the employee portal."""
from __future__ import annotations

import json
import re
from typing import Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..auth import has_permission
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, TeamPolicy, User, utcnow
from ..shared import templates
from ..team_notifications import notify_active_employees
from .team_admin import _permission_gate

router = APIRouter()

MAX_TITLE_CHARS = 200
MAX_BODY_CHARS = 12000
MAX_VERSION_CHARS = 40
POLICY_KINDS = {
    "policy": "Policy",
    "sop": "SOP",
    "checklist": "Checklist",
    "notice": "Notice",
}
LEGACY_POLICY_IDS = {"code-of-conduct", "safety-handling"}


def _redirect(message: str) -> RedirectResponse:
    return RedirectResponse(
        f"/team/admin/policies?flash={quote_plus(message)}",
        status_code=303,
    )


def _clean_text(
    value: object,
    *,
    field_label: str,
    max_chars: int,
    required: bool = True,
) -> tuple[str, Optional[str]]:
    if isinstance(value, bytes):
        try:
            raw = value.decode("utf-8")
        except UnicodeDecodeError:
            return "", f"{field_label} contains unsupported characters."
    elif value is None:
        raw = ""
    else:
        raw = str(value)

    if "\x00" in raw:
        return "", f"{field_label} contains unsupported characters."

    clean = raw.strip()
    if required and not clean:
        return "", f"{field_label} is required."
    if len(clean) > max_chars:
        return "", f"{field_label} must be {max_chars} characters or fewer."
    return clean, None


def _bool_from_form(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "on", "yes"}


def _slugify_title(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (title or "").lower()).strip("-")
    return (slug or "policy")[:80].strip("-") or "policy"


def _unique_public_id(session: Session, title: str) -> str:
    base = _slugify_title(title)
    candidate = base
    suffix = 2
    while candidate in LEGACY_POLICY_IDS or session.exec(
        select(TeamPolicy).where(TeamPolicy.public_id == candidate)
    ).first():
        suffix_text = f"-{suffix}"
        candidate = f"{base[:80 - len(suffix_text)].strip('-')}{suffix_text}"
        suffix += 1
    return candidate


@router.get("/team/admin/policies", response_class=HTMLResponse)
def admin_policies_list(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.policies.view")
    if denial:
        return denial

    policies = list(
        session.exec(
            select(TeamPolicy).order_by(
                TeamPolicy.published_at.desc(),
                TeamPolicy.id.desc(),
            )
        ).all()
    )
    creator_ids = {
        row.created_by_user_id
        for row in policies
        if row.created_by_user_id is not None
    }
    authors: dict[int, User] = {}
    if creator_ids:
        authors = {
            user.id: user
            for user in session.exec(select(User).where(User.id.in_(creator_ids))).all()
            if user.id is not None
        }

    return templates.TemplateResponse(
        request,
        "team/admin/policies.html",
        {
            "request": request,
            "title": "Policies",
            "active": "policies",
            "current_user": current,
            "policies": policies,
            "authors": authors,
            "kind_labels": POLICY_KINDS,
            "can_create": has_permission(session, current, "admin.policies.create"),
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/policies",
    dependencies=[Depends(require_csrf)],
)
async def admin_policies_create(
    request: Request,
    title: str = Form(default=""),
    body: str = Form(default=""),
    version: str = Form(default="v1"),
    kind: str = Form(default="policy"),
    requires_acknowledgement: Optional[str] = Form(default=None),
    notify_employees: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.policies.create")
    if denial:
        return denial
    if current.id is None:
        return HTMLResponse("Current user is missing an id.", status_code=400)

    clean_title, title_error = _clean_text(
        title,
        field_label="Title",
        max_chars=MAX_TITLE_CHARS,
    )
    if title_error:
        return _redirect(title_error)
    clean_body, body_error = _clean_text(
        body,
        field_label="Body",
        max_chars=MAX_BODY_CHARS,
    )
    if body_error:
        return _redirect(body_error)
    clean_version, version_error = _clean_text(
        version,
        field_label="Version",
        max_chars=MAX_VERSION_CHARS,
        required=False,
    )
    if version_error:
        return _redirect(version_error)
    clean_kind = (kind or "policy").strip().lower()
    if clean_kind not in POLICY_KINDS:
        clean_kind = "policy"

    now = utcnow()
    row = TeamPolicy(
        public_id=_unique_public_id(session, clean_title),
        title=clean_title,
        body=clean_body,
        version=clean_version or "v1",
        kind=clean_kind,
        requires_acknowledgement=_bool_from_form(requires_acknowledgement),
        created_by_user_id=current.id,
        published_at=now,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.flush()
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="policy.created",
            resource_key="admin.policies.create",
            details_json=json.dumps(
                {
                    "policy_id": row.public_id,
                    "row_id": row.id,
                    "title": row.title,
                    "version": row.version,
                    "kind": row.kind,
                    "requires_acknowledgement": bool(row.requires_acknowledgement),
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    notified = 0
    if _bool_from_form(notify_employees):
        notified = notify_active_employees(
            session,
            actor_user_id=current.id,
            kind="policy",
            title=f"New policy: {row.title}",
            body=(
                "Please open Policies in the employee portal and acknowledge it."
                if row.requires_acknowledgement
                else "A new team document is available in the employee portal."
            ),
            link_path="/team/policies",
            request=request,
        )
    session.commit()
    suffix = f" Notified {notified} employee(s)." if notified else ""
    return _redirect(f"Policy published.{suffix}")


@router.post(
    "/team/admin/policies/{policy_id}/archive",
    dependencies=[Depends(require_csrf)],
)
async def admin_policies_archive(
    request: Request,
    policy_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(request, session, "admin.policies.create")
    if denial:
        return denial

    row = session.get(TeamPolicy, policy_id)
    if row is None:
        return HTMLResponse("Policy not found", status_code=404)

    now = utcnow()
    was_active = bool(row.is_active)
    row.is_active = False
    row.archived_at = now
    row.updated_at = now
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="policy.archived",
            resource_key="admin.policies.create",
            details_json=json.dumps(
                {
                    "policy_id": row.public_id,
                    "row_id": row.id,
                    "was_active": was_active,
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _redirect("Policy archived.")
