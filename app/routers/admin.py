"""
Admin view routes: home, users, debug, logs, health.

Extracted from app/main.py.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..csrf import CSRFProtectedRoute
from ..shared import *  # noqa: F401,F403 — shared helpers, constants, state
from ..db import get_session

router = APIRouter(route_class=CSRFProtectedRoute)


@router.get("/admin", response_class=HTMLResponse)
def admin_home_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    watched_channels = get_watched_channels(session)
    enabled_channels = [row for row in watched_channels if row.is_enabled]
    backfill_channels = [row for row in watched_channels if row.backfill_enabled]
    review_summary = get_summary(session, status="review_queue")
    overall_summary = get_summary(session)

    return templates.TemplateResponse(
        request,
        "admin_home.html",
        {
            "request": request,
            "title": "Admin Hub",
            "current_user": getattr(request.state, "current_user", None),
            "review_summary": review_summary,
            "overall_summary": overall_summary,
            "parser_progress": get_parser_progress(session),
            "watched_channels": watched_channels,
            "enabled_channel_count": len(enabled_channels),
            "backfill_channel_count": len(backfill_channels),
            "employee_portal_enabled": settings.employee_portal_enabled,
        },
    )

@router.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(
    request: Request,
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    users = session.exec(select(User).order_by(User.created_at.asc())).all()
    return templates.TemplateResponse(
        request,
        "admin_users.html",
        {
            "request": request,
            "title": "User Management",
            "current_user": getattr(request.state, "current_user", None),
            "users": users,
            "current_admin_username": settings.admin_username.strip().lower(),
            "success": success,
            "error": error,
        },
    )

@router.post("/admin/users/create")
def admin_create_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    display_name: str = Form(default=""),
    role: str = Form(default="viewer"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    normalized = (username or "").strip().lower()
    if not normalized or not password:
        return RedirectResponse(url="/admin/users?error=Username+and+password+are+required", status_code=303)
    if role not in ("viewer", "reviewer", "admin"):
        return RedirectResponse(url="/admin/users?error=Invalid+role", status_code=303)
    existing = session.exec(select(User).where(User.username == normalized)).first()
    if existing:
        return RedirectResponse(url=f"/admin/users?error=User+{normalized}+already+exists", status_code=303)
    from ..auth import hash_password
    pwd_hash, pwd_salt = hash_password(password)
    session.add(User(
        username=normalized,
        password_hash=pwd_hash,
        password_salt=pwd_salt,
        display_name=(display_name or "").strip() or normalized,
        role=role,
        is_active=True,
        created_at=utcnow(),
        updated_at=utcnow(),
    ))
    session.commit()
    return RedirectResponse(url=f"/admin/users?success=Created+user+{normalized}", status_code=303)

@router.post("/admin/users/{user_id}/toggle")
def admin_toggle_user(
    request: Request,
    user_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    user = session.get(User, user_id)
    if not user:
        return RedirectResponse(url="/admin/users?error=User+not+found", status_code=303)
    if user.username == settings.admin_username.strip().lower():
        return RedirectResponse(url="/admin/users?error=Cannot+disable+the+primary+admin+account", status_code=303)
    user.is_active = not user.is_active
    now = utcnow()
    if not user.is_active:
        user.session_invalidated_at = now
    user.updated_at = now
    session.add(user)
    session.commit()
    action = "enabled" if user.is_active else "disabled"
    return RedirectResponse(url=f"/admin/users?success=User+{user.username}+{action}", status_code=303)

@router.get("/admin/debug", response_class=HTMLResponse)
def admin_debug_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return RedirectResponse(url="/status", status_code=301)

@router.get("/admin/logs", response_class=HTMLResponse)
def admin_logs_page(
    request: Request,
    file: str = Query(default="app"),
    lines: int = Query(default=200, ge=10, le=2000),
):
    role_response = require_role_response(request, "admin")
    if role_response:
        return role_response

    allowed_files = {"app": "app.log", "worker": "worker.log"}
    log_filename = allowed_files.get(file, "app.log")
    log_path = resolve_runtime_log_path(log_filename)

    tail_lines: list[str] = []
    if log_path.exists():
        try:
            raw = log_path.read_text(encoding="utf-8", errors="replace")
            all_lines = raw.splitlines()
            tail_lines = all_lines[-lines:]
        except OSError:
            tail_lines = ["(unable to read log file)"]
    else:
        tail_lines = [f"(log file not found: {log_path})"]

    log_content = "\n".join(tail_lines)
    nav_links = " | ".join(
        f'<a href="/admin/logs?file={k}&lines={lines}" style="{"font-weight:bold" if k == file else ""}">{k}.log</a>'
        for k in allowed_files
    )

    return HTMLResponse(
        f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Logs — {log_filename}</title>
<style>
body {{ margin:0; padding:20px; background:#0a0c10; color:#c8d0da; font-family:monospace; font-size:13px; }}
nav {{ margin-bottom:16px; font-size:14px; }}
nav a {{ color:#ff8844; text-decoration:none; margin-right:12px; }}
pre {{ white-space:pre-wrap; word-break:break-all; line-height:1.6; }}
h1 {{ font-size:18px; color:#eee; margin:0 0 8px; }}
.controls {{ margin-bottom:12px; color:#888; font-size:12px; }}
.controls a {{ color:#ff8844; }}
</style></head><body>
<h1>{log_filename}</h1>
<nav>{nav_links}</nav>
<div class="controls">
Showing last {len(tail_lines)} lines &mdash;
<a href="/admin/logs?file={file}&lines=50">50</a> |
<a href="/admin/logs?file={file}&lines=200">200</a> |
<a href="/admin/logs?file={file}&lines=500">500</a> |
<a href="/admin/logs?file={file}&lines=1000">1000</a>
&mdash; <a href="/status">&larr; Status</a>
</div>
<pre>{log_content}</pre>
<script>window.scrollTo(0, document.body.scrollHeight);</script>
</body></html>"""
    )

@router.get("/admin/health", response_class=HTMLResponse)
def admin_health_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return RedirectResponse(url="/status", status_code=301)
