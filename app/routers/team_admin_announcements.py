"""
/team/admin/announcements — team announcements management (Wave C).

Managers and admins can create/archive announcements; reviewers can view the
management page but cannot write. Employee visibility is handled in
app.routers.team.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..auth import has_permission
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import AuditLog, TeamAnnouncement, User, utcnow
from ..shared import templates
from .team_admin import _permission_gate

router = APIRouter()

MAX_TITLE_CHARS = 200
MAX_BODY_CHARS = 4000


def _flash_redirect(message: str) -> RedirectResponse:
    return RedirectResponse(
        f"/team/admin/announcements?flash={quote_plus(message)}",
        status_code=303,
    )


def _clean_text(
    value: object,
    *,
    field_label: str,
    max_chars: int,
) -> tuple[Optional[str], Optional[str]]:
    if isinstance(value, bytes):
        try:
            raw = value.decode("utf-8")
        except UnicodeDecodeError:
            return None, f"{field_label} contains unsupported characters."
    elif value is None:
        raw = ""
    else:
        raw = str(value)

    if "\x00" in raw:
        return None, f"{field_label} contains unsupported characters."

    clean = raw.strip()
    if not clean:
        return None, f"{field_label} is required."
    if len(clean) > max_chars:
        return None, f"{field_label} must be {max_chars} characters or fewer."
    return clean, None


def _parse_expires_at(
    value: object,
    tz_offset_minutes: Optional[str] = None,
) -> tuple[Optional[datetime], Optional[str]]:
    if isinstance(value, bytes):
        try:
            raw = value.decode("utf-8")
        except UnicodeDecodeError:
            return None, "Expiration contains unsupported characters."
    elif value is None:
        raw = ""
    else:
        raw = str(value)

    if "\x00" in raw:
        return None, "Expiration contains unsupported characters."

    raw = raw.strip()
    if not raw:
        return None, None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None, "Expiration must be a valid ISO date and time."
    if parsed.tzinfo is None:
        try:
            offset_minutes = int(str(tz_offset_minutes or "").strip() or "0")
        except ValueError:
            offset_minutes = 0
        parsed = (parsed + timedelta(minutes=offset_minutes)).replace(
            tzinfo=timezone.utc
        )
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed, None


def _bool_from_form(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "on", "yes"}


def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _announcement_status(row: TeamAnnouncement, now: datetime) -> str:
    if not row.is_active:
        return "archived"
    expires = _as_utc(row.expires_at)
    if expires is not None and expires <= now:
        return "expired"
    return "active"


@router.get("/team/admin/announcements", response_class=HTMLResponse)
def admin_announcements_list(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(
        request, session, "admin.announcements.view"
    )
    if denial:
        return denial

    announcements = list(
        session.exec(
            select(TeamAnnouncement)
            .order_by(
                TeamAnnouncement.published_at.desc(),
                TeamAnnouncement.id.desc(),
            )
        ).all()
    )
    creator_ids = {
        row.created_by_user_id
        for row in announcements
        if row.created_by_user_id is not None
    }
    authors: dict[int, User] = {}
    if creator_ids:
        authors = {
            user.id: user
            for user in session.exec(
                select(User).where(User.id.in_(creator_ids))
            ).all()
            if user.id is not None
        }

    now = utcnow()
    return templates.TemplateResponse(
        request,
        "team/admin/announcements.html",
        {
            "request": request,
            "title": "Announcements",
            "active": "announcements",
            "current_user": current,
            "announcements": announcements,
            "authors": authors,
            "statuses": {
                row.id: _announcement_status(row, now)
                for row in announcements
                if row.id is not None
            },
            "can_view_admin_announcements": True,
            "can_create": has_permission(
                session, current, "admin.announcements.create"
            ),
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post(
    "/team/admin/announcements",
    dependencies=[Depends(require_csrf)],
)
async def admin_announcements_create(
    request: Request,
    title: str = Form(default=""),
    body: str = Form(default=""),
    pinned: Optional[str] = Form(default=None),
    expires_at: str = Form(default=""),
    tz_offset_minutes: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(
        request, session, "admin.announcements.create"
    )
    if denial:
        return denial

    clean_title, title_error = _clean_text(
        title,
        field_label="Title",
        max_chars=MAX_TITLE_CHARS,
    )
    if title_error:
        return _flash_redirect(title_error)
    clean_body, body_error = _clean_text(
        body,
        field_label="Body",
        max_chars=MAX_BODY_CHARS,
    )
    if body_error:
        return _flash_redirect(body_error)
    parsed_expires_at, expires_error = _parse_expires_at(
        expires_at,
        tz_offset_minutes,
    )
    if expires_error:
        return _flash_redirect(expires_error)

    now = utcnow()
    row = TeamAnnouncement(
        title=clean_title or "",
        body=clean_body or "",
        created_by_user_id=current.id,
        pinned=_bool_from_form(pinned),
        published_at=now,
        expires_at=parsed_expires_at,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.flush()
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="announcement.created",
            resource_key="admin.announcements.create",
            details_json=json.dumps(
                {
                    "announcement_id": row.id,
                    "title": row.title,
                    "pinned": bool(row.pinned),
                    "expires_at": (
                        parsed_expires_at.isoformat()
                        if parsed_expires_at is not None
                        else None
                    ),
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _flash_redirect("Announcement published.")


@router.post(
    "/team/admin/announcements/{announcement_id}/archive",
    dependencies=[Depends(require_csrf)],
)
async def admin_announcements_archive(
    request: Request,
    announcement_id: int,
    session: Session = Depends(get_session),
):
    denial, current = _permission_gate(
        request, session, "admin.announcements.create"
    )
    if denial:
        return denial

    row = session.get(TeamAnnouncement, announcement_id)
    if row is None:
        return HTMLResponse("Announcement not found", status_code=404)

    now = utcnow()
    was_active = bool(row.is_active)
    row.is_active = False
    row.updated_at = now
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=current.id,
            action="announcement.archived",
            resource_key="admin.announcements.view",
            details_json=json.dumps(
                {
                    "announcement_id": announcement_id,
                    "was_active": was_active,
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return _flash_redirect("Announcement archived.")
