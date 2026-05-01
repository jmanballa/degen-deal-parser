"""
/team/* — employee-facing portal (Wave 3).

Scope:
  * Auth flows: login / logout / invite-accept / password reset (public).
  * Authenticated surface: dashboard (widget-driven), profile (self-edit
    non-critical PII), policies (placeholders + ack via AuditLog), hours
    (Clockify stub), schedule (placeholder), supply (submit + list own).

Admin employee-management pages live under /team/admin/* (Wave 2 + Wave 4).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional, Tuple
from urllib.parse import unquote, urlparse

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from .. import permissions as perms
from ..auth import (
    BadCurrentPasswordError,
    LoginRateLimitedError,
    WeakPasswordError,
    authenticate_user,
    change_user_password,
    consume_invite_token,
    consume_password_reset_token,
    generate_password_reset_token,
    has_permission,
    _find_token_row,
    _token_hmac_key,
    validate_password_strength,
)
from ..clockify import (
    ClockifyApiError,
    ClockifyConfigError,
    build_week_summary,
    clockify_client_from_settings,
    clockify_is_configured,
    clockify_week_bounds,
    format_hours,
)
from ..config import get_settings
from ..csrf import issue_token, require_csrf, rotate_token
from ..db import get_session
from ..models import (
    AuditLog,
    EmployeeProfile,
    InviteToken,
    SHIFT_KIND_ALL,
    SHIFT_KIND_BLANK,
    SHIFT_KIND_OFF,
    SHIFT_KIND_REQUEST,
    SHIFT_KIND_WORK,
    ScheduleDayNote,
    ShiftEntry,
    SupplyRequest,
    TeamAnnouncement,
    TeamPolicy,
    TimeOffRequest,
    User,
    utcnow,
)
from ..pii import PIIDecryptError, decrypt_pii, encrypt_pii
from ..rate_limit import rate_limited_or_429
from ..shared import app_home_for_role, templates
from ..sms import mask_sms_phone, normalize_sms_phone, send_sms, sms_phone_fingerprint
from ..team_notifications import EMPLOYEE_NOTIFICATION_ACTION

router = APIRouter()


LEGACY_POLICIES: tuple[dict, ...] = (
    {
        "id": "code-of-conduct",
        "title": "Code of Conduct",
        "version": "v1",
        "kind": "policy",
        "requires_ack": True,
        "body_md": (
            "Treat teammates, customers, and contractors with respect. "
            "Report safety or conduct concerns to Jeffrey directly. "
            "No harassment, theft, or discrimination will be tolerated."
        ),
    },
    {
        "id": "safety-handling",
        "title": "Safety & Handling",
        "version": "v1",
        "kind": "policy",
        "requires_ack": True,
        "body_md": (
            "Wash hands before handling cards. Sleeve slabs before storage. "
            "Never leave inventory unattended in common areas. "
            "Power tools require PPE; stop and ask if unsure."
        ),
    },
)
LEGACY_POLICY_BY_ID = {p["id"]: p for p in LEGACY_POLICIES}


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

def _portal_or_404() -> None:
    if not get_settings().employee_portal_enabled:
        raise HTTPException(status_code=404)


def _require_employee(
    request: Request,
    session: Session,
    *,
    resource_key: Optional[str] = None,
) -> Tuple[Optional[Response], Optional[User]]:
    """Portal on + session present + optional resource check.

    Access is governed entirely by `has_permission` against the matrix; any
    role (employee, manager, reviewer, admin) that holds the required
    resource flag may view the page. Anonymous users are redirected to login.
    """
    _portal_or_404()
    user: Optional[User] = getattr(request.state, "current_user", None)
    if user is None:
        return RedirectResponse("/team/login", status_code=303), None
    if resource_key is not None and not has_permission(session, user, resource_key):
        return HTMLResponse(
            "You do not have permission to view this page.", status_code=403
        ), None
    return None, user


# ---------------------------------------------------------------------------
# Public auth flows
# ---------------------------------------------------------------------------

def _safe_next(value: Optional[str]) -> str:
    """Only forward local paths to prevent open-redirects through `next`."""
    value = (value or "").strip()
    if not value:
        return ""
    decoded = unquote(value).strip()
    if decoded.startswith("\\"):
        return ""
    parsed = urlparse(decoded)
    if parsed.netloc or parsed.scheme:
        return ""
    if decoded.startswith("/.") or decoded.startswith("/%2e"):
        return ""
    if decoded.startswith("/") and not (
        decoded.startswith("//") or (len(decoded) > 1 and decoded[1] == "\\")
    ):
        return decoded
    return ""


def _password_changed_session_value(user: User) -> Optional[str]:
    changed_at = getattr(user, "password_changed_at", None)
    return changed_at.isoformat() if changed_at is not None else None


def _session_invalidated_session_value(user: User) -> Optional[str]:
    invalidated_at = getattr(user, "session_invalidated_at", None)
    return invalidated_at.isoformat() if invalidated_at is not None else None


def _public_base_url(request: Request) -> str:
    configured = (get_settings().public_base_url or "").strip().rstrip("/")
    if configured:
        return configured
    return f"{request.url.scheme}://{request.url.netloc}"


def _password_reset_url(request: Request, raw_token: str) -> str:
    return f"{_public_base_url(request)}/team/password/reset/{raw_token}"


def _password_reset_sms_body(reset_url: str) -> str:
    return (
        "Degen Team password reset: "
        f"{reset_url}\n"
        "Expires in 60 minutes. Ignore this if you did not request it."
    )


def _sms_provider_can_deliver() -> bool:
    provider = (get_settings().sms_provider or "dry_run").strip().lower()
    return provider not in {
        "",
        "dryrun",
        "dry_run",
        "log",
        "console",
        "disabled",
        "off",
        "none",
    }


def _find_password_reset_user(session: Session, identifier: str) -> Optional[User]:
    probe = (identifier or "").strip()
    if not probe:
        return None
    normalized = probe.lower()
    user = session.exec(
        select(User).where(func.lower(User.username) == normalized)
    ).first()
    if user is not None and user.is_active:
        return user
    if "@" in normalized:
        from ..pii import email_lookup_hash

        digest = email_lookup_hash(normalized)
        profile = session.exec(
            select(EmployeeProfile).where(EmployeeProfile.email_lookup_hash == digest)
        ).first()
        if profile is not None:
            user = session.get(User, profile.user_id)
            if user is not None and user.is_active:
                return user
    return None


def _password_reset_identifier_hash(identifier: str) -> str:
    probe = (identifier or "").strip().lower()
    if not probe:
        return ""
    return hmac.new(
        _token_hmac_key(),
        probe.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _queue_password_reset_request(
    session: Session,
    *,
    request: Request,
    user: User,
    probe_hash: str,
    reason: str,
) -> None:
    session.add(
        AuditLog(
            target_user_id=user.id,
            action="password.reset_manager_request",
            resource_key="admin.employees.reset_password",
            details_json=json.dumps(
                {
                    "source": "http_forgot",
                    "identifier_hash": probe_hash,
                    "reason": reason,
                },
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )


def _try_send_password_reset_sms(
    session: Session,
    *,
    request: Request,
    user: User,
    probe_hash: str,
) -> bool:
    if not _sms_provider_can_deliver():
        return False
    profile = session.get(EmployeeProfile, user.id)
    if profile is None or not profile.phone_enc:
        return False
    try:
        phone_plain = decrypt_pii(profile.phone_enc) or ""
    except (PIIDecryptError, ValueError):
        return False
    to_phone = normalize_sms_phone(phone_plain)
    if not to_phone:
        return False
    raw_token = generate_password_reset_token(
        session,
        user_id=user.id,
        issued_by_user_id=user.id,
    )
    result = send_sms(
        to_phone=to_phone,
        body=_password_reset_sms_body(_password_reset_url(request, raw_token)),
        settings=get_settings(),
    )
    details = {
        "provider": result.provider,
        "status": result.status,
        "dry_run": result.dry_run,
        "success": result.success and not result.dry_run,
        "phone": mask_sms_phone(to_phone),
        "phone_fingerprint": sms_phone_fingerprint(to_phone),
        "identifier_hash": probe_hash,
    }
    if result.message_id:
        details["message_id"] = result.message_id
    if result.error:
        details["error"] = result.error[:240]
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action=(
                "password.reset_sms_sent"
                if result.success and not result.dry_run
                else "password.reset_sms_failed"
            ),
            details_json=json.dumps(details, sort_keys=True),
            ip_address=(request.client.host if request.client else None),
        )
    )
    if result.success and not result.dry_run:
        return True
    return False


@router.get("/team/login", response_class=HTMLResponse)
def team_login_page(
    request: Request,
    next: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
):
    _portal_or_404()
    from ..shared import app_home_for_role
    next_url = _safe_next(next)
    user = getattr(request.state, "current_user", None)
    if user is not None:
        if next_url:
            return RedirectResponse(next_url, status_code=303)
        return RedirectResponse(app_home_for_role(user.role), status_code=303)
    return templates.TemplateResponse(
        request,
        "team/login.html",
        {
            "request": request,
            "title": "Team Sign In",
            "error": error,
            "flash": flash,
            "next_url": next_url,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/login")
async def team_login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(default=""),
    next: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    from ..shared import app_home_for_role
    from urllib.parse import urlencode as _urlencode
    ip = request.client.host if request.client else None
    next_url = _safe_next(next)
    next_qs = f"&next={_urlencode({'next': next_url})[5:]}" if next_url else ""

    if limited := rate_limited_or_429(
        request, key_prefix="team:login", max_requests=5, window_seconds=900.0
    ):
        session.add(
            AuditLog(
                action="login.rate_limited",
                details_json=json.dumps({"ip": ip}),
                ip_address=ip,
            )
        )
        session.commit()
        return limited
    # CSRF is enforced manually here so we can also render the login form
    # with a fresh token on a failure without breaking the flow.
    from ..csrf import verify_token

    if not verify_token(request, csrf_token):
        return RedirectResponse(
            f"/team/login?error=Session+expired.+Please+try+again.{next_qs}",
            status_code=303,
        )

    try:
        user = authenticate_user(
            session, username, password, request=request, ip_address=ip
        )
    except LoginRateLimitedError as exc:
        return exc.response
    if not user:
        return RedirectResponse(
            f"/team/login?error=Invalid+username+or+password{next_qs}",
            status_code=303,
        )

    request.session["user_id"] = user.id
    request.session["password_changed_at"] = _password_changed_session_value(user)
    request.session["session_invalidated_at"] = _session_invalidated_session_value(user)
    rotate_token(request)  # m1 — bind a fresh CSRF to the authenticated session
    if next_url:
        return RedirectResponse(next_url, status_code=303)
    return RedirectResponse(app_home_for_role(user.role), status_code=303)


@router.post("/team/logout", dependencies=[Depends(require_csrf)])
def team_logout(request: Request):
    _portal_or_404()
    request.session.clear()
    return RedirectResponse(
        "/team/login?flash=You+have+been+signed+out.", status_code=303
    )


@router.get("/team/invite/accept/{token}", response_class=HTMLResponse)
def team_invite_accept_page(
    request: Request,
    token: str,
    error: Optional[str] = Query(default=None),
    problems: Optional[str] = Query(default=None),
    username: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    invite_role = "employee"
    token_row = _find_token_row(session, InviteToken, token)
    if token_row is not None and token_row.role:
        invite_role = token_row.role.strip().lower() or "employee"
    else:
        claimed_token_row = _find_token_row(
            session,
            InviteToken,
            token,
            include_used=True,
            include_expired=True,
        )
        if claimed_token_row is not None and claimed_token_row.used_at is not None:
            claimed_user_id = (
                claimed_token_row.used_by_user_id or claimed_token_row.target_user_id
            )
            claimed_user = session.get(User, claimed_user_id) if claimed_user_id else None
            if (
                claimed_user is not None
                and claimed_user.is_active
                and claimed_user.password_hash
            ):
                current_user: Optional[User] = getattr(
                    request.state, "current_user", None
                )
                if current_user is None:
                    return RedirectResponse("/team/login", status_code=303)
                if current_user.id == claimed_user.id:
                    return RedirectResponse("/team/", status_code=303)
                return RedirectResponse(
                    app_home_for_role(current_user.role), status_code=303
                )
    show_employee_tutorial = invite_role == "employee"
    show_manager_tutorial = invite_role == "manager"
    show_portal_tutorial = show_employee_tutorial or show_manager_tutorial
    setup_step_total = 7 if show_portal_tutorial else 6
    return templates.TemplateResponse(
        request,
        "team/invite_accept.html",
        {
            "request": request,
            "title": "Accept Invite",
            "token": token,
            "invite_role": invite_role,
            "show_employee_tutorial": show_employee_tutorial,
            "show_manager_tutorial": show_manager_tutorial,
            "show_portal_tutorial": show_portal_tutorial,
            "setup_step_total": setup_step_total,
            "progress_dot_count": setup_step_total + 1,
            "error": error,
            "problems": (problems or "").split("|") if problems else [],
            "username": username or "",
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/invite/accept/{token}", dependencies=[Depends(require_csrf)])
async def team_invite_accept_post(
    request: Request,
    token: str,
    new_username: str = Form(...),
    new_password: str = Form(...),
    preferred_name: str = Form(default=""),
    legal_name: str = Form(default=""),
    email: str = Form(default=""),
    phone: str = Form(default=""),
    address_street: str = Form(default=""),
    address_city: str = Form(default=""),
    address_state: str = Form(default=""),
    address_zip: str = Form(default=""),
    emergency_contact_name: str = Form(default=""),
    emergency_contact_phone: str = Form(default=""),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    if limited := rate_limited_or_429(
        request, key_prefix="team:invite", max_requests=3, window_seconds=900.0
    ):
        return limited
    address_payload = {
        "street": (address_street or "").strip(),
        "city": (address_city or "").strip(),
        "state": (address_state or "").strip(),
        "zip": (address_zip or "").strip(),
    }
    try:
        user = consume_invite_token(
            session,
            token,
            new_username=new_username,
            new_password=new_password,
            preferred_name=preferred_name,
            legal_name=legal_name,
            email=email,
            phone=phone,
            address=address_payload if any(address_payload.values()) else None,
            emergency_contact_name=emergency_contact_name,
            emergency_contact_phone=emergency_contact_phone,
        )
    except WeakPasswordError as exc:
        qs = "problems=" + "|".join(p.replace(" ", "+") for p in exc.problems)
        qs += f"&username={new_username}"
        return RedirectResponse(
            f"/team/invite/accept/{token}?{qs}", status_code=303
        )
    except ValueError as exc:
        return RedirectResponse(
            f"/team/invite/accept/{token}?error={str(exc)}", status_code=303
        )
    request.session["user_id"] = user.id
    request.session["password_changed_at"] = _password_changed_session_value(user)
    request.session["session_invalidated_at"] = _session_invalidated_session_value(user)
    rotate_token(request)
    redirect_url = "/team/?flash=Welcome+to+the+team!"
    if session.info.pop("invite_email_skipped_due_to_clash", False):
        redirect_url += "&banner=Email+not+saved.+That+address+is+already+on+file+for+another+employee."
    return RedirectResponse(redirect_url, status_code=303)


@router.get("/team/password/forgot", response_class=HTMLResponse)
def team_password_forgot_page(
    request: Request,
    flash: Optional[str] = Query(default=None),
):
    _portal_or_404()
    return templates.TemplateResponse(
        request,
        "team/password_forgot.html",
        {
            "request": request,
            "title": "Reset password",
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/password/forgot", dependencies=[Depends(require_csrf)])
async def team_password_forgot_post(
    request: Request,
    identifier: str = Form(default=""),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    if limited := rate_limited_or_429(
        request, key_prefix="team:forgot", max_requests=3, window_seconds=900.0
    ):
        return limited
    probe = (identifier or "").strip().lower()
    probe_hash = _password_reset_identifier_hash(probe)
    if probe_hash:
        if limited := rate_limited_or_429(
            request,
            key_prefix=f"team:forgot:{probe_hash[:16]}",
            max_requests=3,
            window_seconds=900.0,
        ):
            return limited
    matched_user = _find_password_reset_user(session, probe)
    delivered = False
    if matched_user is not None:
        delivered = _try_send_password_reset_sms(
            session,
            request=request,
            user=matched_user,
            probe_hash=probe_hash,
        )
        if not delivered:
            _queue_password_reset_request(
                session,
                request=request,
                user=matched_user,
                probe_hash=probe_hash,
                reason="manager_action_required",
            )
    session.add(
        AuditLog(
            action="password.reset_requested",
            target_user_id=None,
            details_json=json.dumps(
                {
                    "identifier_hash": probe_hash,
                    "source": "http_forgot",
                    "status": "accepted",
                },
                sort_keys=True,
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse(
        "/team/password/forgot?flash=If+that+account+exists%2C+we%27ll+send+a+reset+link+or+put+it+in+the+admin+reset+queue.",
        status_code=303,
    )


@router.get("/team/password/reset/{token}", response_class=HTMLResponse)
def team_password_reset_page(
    request: Request,
    token: str,
    problems: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    _portal_or_404()
    return templates.TemplateResponse(
        request,
        "team/password_reset.html",
        {
            "request": request,
            "title": "Choose a new password",
            "token": token,
            "problems": (problems or "").split("|") if problems else [],
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/password/reset/{token}", dependencies=[Depends(require_csrf)])
async def team_password_reset_post(
    request: Request,
    token: str,
    new_password: str = Form(...),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    if limited := rate_limited_or_429(
        request, key_prefix="team:reset", max_requests=5, window_seconds=900.0
    ):
        return limited
    try:
        consume_password_reset_token(session, token, new_password=new_password)
    except WeakPasswordError as exc:
        qs = "problems=" + "|".join(p.replace(" ", "+") for p in exc.problems)
        return RedirectResponse(
            f"/team/password/reset/{token}?{qs}", status_code=303
        )
    except ValueError as exc:
        return RedirectResponse(
            f"/team/password/reset/{token}?error={str(exc)}", status_code=303
        )
    return RedirectResponse(
        "/team/login?flash=Password+updated.+Please+sign+in.",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Authenticated employee surface
# ---------------------------------------------------------------------------

def _nav_context(session: Session, user: User) -> dict:
    cache: dict = {}
    # Keep the regular portal Schedule link employee-facing for every role.
    # Managers/admins get a separate Team Admin schedule link so they can
    # choose between checking the published view and editing the team grid.
    can_edit_schedule = has_permission(
        session, user, "admin.schedule.edit", cache=cache
    )
    schedule_href = "/team/schedule"
    keys = (
        ("dashboard", "Dashboard", "page.dashboard", "/team/"),
        ("hours", "Hours", "page.hours", "/team/hours"),
        ("announcements", "Announcements", "page.announcements", "/team/announcements"),
        ("notifications", "Notifications", "page.announcements", "/team/notifications"),
        ("schedule", "Schedule", "page.schedule", schedule_href),
        ("time-off", "Time off", "page.timeoff", "/team/timeoff"),
        ("policies", "Policies", "page.policies", "/team/policies"),
        ("supply", "Supply", "page.supply_requests", "/team/supply"),
        ("profile", "Profile", "page.profile", "/team/profile"),
    )
    nav = []
    for name, label, key, href in keys:
        if has_permission(session, user, key, cache=cache):
            nav.append({"name": name, "label": label, "href": href})

    # Admin-only section. Rendered as a separate group in the sidebar when
    # at least one entry is visible. Gated per-key against the perms matrix
    # so managers/reviewers only see the admin links they actually have.
    admin_keys = (
        ("employees", "Employees", "page.admin.employees", "/team/admin/employees"),
        ("invites", "Invites", "page.admin.invites", "/team/admin/invites"),
        ("permissions", "Permissions", "page.admin.permissions", "/team/admin/permissions"),
        ("team-schedule", "Team schedule", "admin.schedule.view", "/team/admin/schedule"),
        ("supply-queue", "Supply queue", "page.admin.supply", "/team/admin/supply"),
        ("time-off-queue", "Time off queue", "admin.timeoff.view", "/team/admin/timeoff"),
        (
            "announcements-admin",
            "Announcements admin",
            "admin.announcements.view",
            "/team/admin/announcements",
        ),
    )
    admin_nav = []
    for name, label, key, href in admin_keys:
        if has_permission(session, user, key, cache=cache):
            admin_nav.append({"name": name, "label": label, "href": href})

    # Ops shortcuts are safe employee-facing tools and should sit apart from
    # HR/self-service links.
    ops_nav = [
        {"name": "inventory", "href": "/inventory/scan?team_shell=1"},
        {"name": "degen-eye", "href": "/degen_eye?team_shell=1"},
        {"name": "live-stream", "href": "/tiktok/streamer?team_shell=1"},
    ]

    return {
        "nav_items": nav,
        "admin_nav_items": admin_nav,
        "tools_nav_items": ops_nav,
        "schedule_href": schedule_href,
        "can_edit_schedule": can_edit_schedule,
    }


def _portal_now(*, settings=None, now: Optional[datetime] = None) -> datetime:
    """Return the current time in the configured business/Clockify timezone."""
    settings = settings or get_settings()
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    week_start_local, _ = clockify_week_bounds(now_utc.date(), settings=settings)
    return now_utc.astimezone(week_start_local.tzinfo)


def _portal_today(*, settings=None, now: Optional[datetime] = None) -> date:
    """Return the business-local date used by Clockify/team scheduling.

    The app server runs in UTC, but Degen's staff schedule and Clockify day
    are Pacific time. Employee-facing "today" widgets must not roll over at
    5 PM PT just because UTC is already tomorrow.
    """
    return _portal_now(settings=settings, now=now).date()


@router.get("/team/dashboard")
def team_dashboard_alias():
    # Unauthenticated-safe: this only redirects to /team/, which is auth-gated.
    return RedirectResponse("/team/", status_code=303)


@router.get("/team/", response_class=HTMLResponse)
def team_dashboard(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    widgets = perms.allowed_widgets_for(session, user)
    settings = get_settings()
    clockify_ready = clockify_is_configured(settings)
    show_supply_queue_count = has_permission(session, user, "admin.supply.view")
    dashboard_context: dict[str, Any] = {
        "request": request,
        "title": "Dashboard",
        "active": "dashboard",
        "current_user": user,
        "widgets": widgets,
        "clockify_ready": clockify_ready,
        "show_supply_queue_count": show_supply_queue_count,
    }
    if show_supply_queue_count:
        dashboard_context["supply_queue_count"] = int(
            session.exec(
                select(func.count())
                .select_from(SupplyRequest)
                .where(SupplyRequest.status.in_(("pending", "submitted")))
            ).one()
        )
    today = _portal_today(settings=settings)
    today_shifts = _today_shifts_for(session, user, today=today)
    upcoming_shifts = _upcoming_shifts_for(session, user, today=today, limit=5)
    next_shift = next(
        (shift for shift in upcoming_shifts if shift["shift_date"] > today),
        upcoming_shifts[0] if upcoming_shifts else None,
    )
    pay_summary = _employee_dashboard_pay_summary(session, user, today=today)
    active_announcements = _active_announcements_for(session, limit=3)
    profile_completion = _profile_completion_for(
        session,
        user,
        clockify_ready=clockify_ready,
    )
    nav_ctx = _nav_context(session, user)
    dashboard_context.update(
        {
            "dashboard_pay": pay_summary,
            "today_shifts": today_shifts,
            "next_shift": next_shift,
            "upcoming_shifts": upcoming_shifts,
            "today_staffing": _today_staffing_for(session, today=today),
            "active_announcements": active_announcements,
            "profile_completion": profile_completion,
            "today_focus": _today_focus_for(
                today_shifts=today_shifts,
                pay_summary=pay_summary,
                announcements=active_announcements,
                profile_completion=profile_completion,
                schedule_href=nav_ctx["schedule_href"],
            ),
            "today_date": today,
            "now_hour": _portal_now(settings=settings).hour,
            "csrf_token": issue_token(request),
            **nav_ctx,
        }
    )
    return templates.TemplateResponse(
        request,
        "team/dashboard.html",
        dashboard_context,
    )


def _format_money_label(cents: int) -> str:
    return f"${Decimal(cents) / Decimal(100):,.2f}"


def _cents_for_seconds(seconds: int, rate_cents: int) -> int:
    if seconds <= 0 or rate_cents <= 0:
        return 0
    amount = (Decimal(seconds) / Decimal(3600)) * Decimal(rate_cents)
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _format_time_label(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.strftime("%I:%M %p").lstrip("0")


def _entry_overlap_seconds(entry: Any, start_local: datetime, end_local: datetime) -> int:
    entry_start = getattr(entry, "start_local", None)
    entry_end = getattr(entry, "end_local", None)
    if entry_start is None:
        return 0
    if entry_end is None:
        duration = int(getattr(entry, "duration_seconds", 0) or 0)
        entry_end = entry_start + timedelta(seconds=duration)
    overlap_start = max(entry_start, start_local)
    overlap_end = min(entry_end, end_local)
    if overlap_end <= overlap_start:
        return 0
    return int((overlap_end - overlap_start).total_seconds())


def _employee_dashboard_pay_summary(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
) -> dict[str, Any]:
    today = today or _portal_today()
    profile = session.get(EmployeeProfile, user.id)
    clockify_user_id = (profile.clockify_user_id or "").strip() if profile else ""
    base: dict[str, Any] = {
        "hours_label": "0m",
        "hours_this_week_sub": "Since Monday 12:00 AM",
        "clocked_in_today_label": "Not clocked in",
        "hours_today_label": "0m",
        "break_today_label": "Not yet",
        "estimated_pay_label": "$0.00",
        "pay_basis": "Hours not connected yet",
        "clockify_user_id": clockify_user_id,
        "has_clockify": bool(clockify_user_id),
        "has_rate": False,
        "error": "",
    }
    if not clockify_user_id:
        return base

    settings = get_settings()
    start_local, end_local = clockify_week_bounds(today, settings=settings)
    today_start_local = datetime.combine(today, time.min, tzinfo=start_local.tzinfo)
    today_end_local = today_start_local + timedelta(days=1)
    work_seconds = 0
    source_label = "Clockify cache"
    try:
        from .team_admin_clockify import (
            _apply_missed_break_deduction,
            _cached_clockify_entries_by_user,
            _clockify_entry_is_break,
        )

        cached = _cached_clockify_entries_by_user(
            session,
            [clockify_user_id],
            start_local=start_local,
            end_local=end_local,
        )
        raw_entries = cached.get(clockify_user_id, [])
        if not raw_entries and clockify_is_configured(settings):
            raw_entries = clockify_client_from_settings(settings).get_user_time_entries(
                clockify_user_id,
                start_utc=start_local.astimezone(timezone.utc),
                end_utc=end_local.astimezone(timezone.utc),
            )
            source_label = "Clockify live"
        summary = build_week_summary(
            raw_entries,
            week_start_local=start_local,
            week_end_local=end_local,
            settings=settings,
            now=datetime.now(timezone.utc),
        )
        daily_work_seconds: dict[date, int] = {}
        daily_break_seconds: dict[date, int] = {}
        range_day = start_local.date()
        last_range_day = (end_local - timedelta(seconds=1)).date()
        while range_day <= last_range_day:
            day_start = datetime.combine(range_day, time.min, tzinfo=start_local.tzinfo)
            day_end = day_start + timedelta(days=1)
            for row in summary.entries:
                seconds = _entry_overlap_seconds(row, day_start, day_end)
                if seconds <= 0:
                    continue
                if _clockify_entry_is_break(row):
                    daily_break_seconds[range_day] = (
                        daily_break_seconds.get(range_day, 0) + seconds
                    )
                else:
                    daily_work_seconds[range_day] = (
                        daily_work_seconds.get(range_day, 0) + seconds
                    )
            range_day += timedelta(days=1)
        adjusted_by_day: dict[date, tuple[int, int, int]] = {}
        for day_key in set(daily_work_seconds) | set(daily_break_seconds):
            adjusted_by_day[day_key] = _apply_missed_break_deduction(
                daily_work_seconds.get(day_key, 0),
                daily_break_seconds.get(day_key, 0),
            )
        work_seconds = sum(row[0] for row in adjusted_by_day.values())
        today_work_entries = [
            row
            for row in summary.entries
            if not _clockify_entry_is_break(row)
            and _entry_overlap_seconds(row, today_start_local, today_end_local) > 0
        ]
        today_break_entries = [
            row
            for row in summary.entries
            if _clockify_entry_is_break(row)
            and _entry_overlap_seconds(row, today_start_local, today_end_local) > 0
        ]
        today_work_seconds, today_break_seconds, today_missed_break_seconds = (
            adjusted_by_day.get(today, (0, 0, 0))
        )
        running_break = any(row.running for row in today_break_entries)
        base["clocked_in_today_label"] = (
            _format_time_label(today_work_entries[0].start_local)
            if today_work_entries
            else "Not clocked in"
        )
        base["hours_today_label"] = format_hours(today_work_seconds)
        if today_missed_break_seconds > 0:
            base["break_today_label"] = (
                f"Auto-deducted {format_hours(today_missed_break_seconds)}"
            )
        elif today_break_seconds > 0:
            prefix = "On break" if running_break else "Taken"
            base["break_today_label"] = f"{prefix} ({format_hours(today_break_seconds)})"
        base["hours_label"] = format_hours(work_seconds)
    except (ClockifyApiError, ClockifyConfigError) as exc:
        base["error"] = str(exc)

    try:
        from .team_admin_employees import (
            COMPENSATION_TYPE_HOURLY,
            COMPENSATION_TYPE_LABELS,
            COMPENSATION_TYPE_MONTHLY,
            compensation_history_rows_for_users,
            compensation_snapshot_for_day,
            _salary_cost_for_period,
        )

        history_rows = compensation_history_rows_for_users(
            session,
            [user.id] if user.id is not None else [],
            end_day=today,
        )
        snapshot = compensation_snapshot_for_day(
            profile,
            today,
            history_rows=history_rows.get(user.id or 0, []),
        )
        compensation_type = snapshot["compensation_type"]
        base["pay_basis"] = COMPENSATION_TYPE_LABELS.get(compensation_type, "Pay")
        if compensation_type == COMPENSATION_TYPE_HOURLY:
            rate_cents = snapshot["hourly_rate_cents"]
            base["has_rate"] = rate_cents is not None
            if rate_cents is not None:
                base["estimated_pay_label"] = _format_money_label(
                    _cents_for_seconds(work_seconds, rate_cents)
                )
                base["pay_basis"] = f"This week at {_format_money_label(rate_cents)}/hr"
            else:
                base["pay_basis"] = "Hourly rate missing"
        elif compensation_type == COMPENSATION_TYPE_MONTHLY:
            salary_cents = snapshot["monthly_salary_cents"]
            base["has_rate"] = salary_cents is not None
            if salary_cents is not None and isinstance(profile, EmployeeProfile):
                base["estimated_pay_label"] = _format_money_label(
                    _salary_cost_for_period(
                        salary_cents=salary_cents,
                        user=user,
                        profile=profile,
                        start_day=start_local.date(),
                        end_day=today,
                    )
                )
                base["pay_basis"] = "This week's salary accrual"
            else:
                base["pay_basis"] = "Salary missing"
    except Exception as exc:
        base["error"] = str(exc)
        base["pay_basis"] = "Pay setup unavailable"

    return base


def _active_announcements_for(
    session: Session,
    *,
    limit: Optional[int] = None,
) -> list[TeamAnnouncement]:
    now = utcnow()
    stmt = (
        select(TeamAnnouncement)
        .where(TeamAnnouncement.is_active == True)  # noqa: E712
        .where(
            or_(
                TeamAnnouncement.expires_at.is_(None),
                TeamAnnouncement.expires_at > now,
            )
        )
        .order_by(
            TeamAnnouncement.pinned.desc(),
            TeamAnnouncement.published_at.desc(),
            TeamAnnouncement.id.desc(),
        )
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(session.exec(stmt).all())


def _policy_from_row(row: TeamPolicy) -> dict[str, Any]:
    return {
        "id": row.public_id,
        "title": row.title,
        "version": row.version or "v1",
        "kind": row.kind or "policy",
        "requires_ack": bool(row.requires_acknowledgement),
        "body_md": row.body or "",
        "published_at": row.published_at,
        "is_dynamic": True,
    }


def _published_policies(session: Session) -> list[dict[str, Any]]:
    rows = session.exec(
        select(TeamPolicy)
        .where(TeamPolicy.is_active == True)  # noqa: E712
        .order_by(TeamPolicy.published_at.desc(), TeamPolicy.id.desc())
    ).all()
    policies = [_policy_from_row(row) for row in rows]
    policies.extend(dict(policy, is_dynamic=False) for policy in LEGACY_POLICIES)
    return policies


def _policy_by_id(session: Session, policy_id: str) -> Optional[dict[str, Any]]:
    policy_id = (policy_id or "").strip()
    if not policy_id:
        return None
    row = session.exec(
        select(TeamPolicy).where(
            TeamPolicy.public_id == policy_id,
            TeamPolicy.is_active == True,  # noqa: E712
        )
    ).first()
    if row is not None:
        return _policy_from_row(row)
    legacy = LEGACY_POLICY_BY_ID.get(policy_id)
    return dict(legacy, is_dynamic=False) if legacy is not None else None


def _policy_acknowledgements_for(session: Session, user: User) -> set[str]:
    rows = session.exec(
        select(AuditLog).where(
            AuditLog.actor_user_id == user.id,
            AuditLog.action == "policy.acknowledge",
        )
    ).all()
    acknowledged: set[str] = set()
    for row in rows:
        try:
            payload = json.loads(row.details_json or "{}")
        except json.JSONDecodeError:
            continue
        policy_id = payload.get("policy_id")
        if isinstance(policy_id, str):
            acknowledged.add(policy_id)
    return acknowledged


def _decrypt_optional(blob: Optional[bytes]) -> str:
    if not blob:
        return ""
    try:
        return (decrypt_pii(blob) or "").strip()
    except (PIIDecryptError, ValueError):
        return ""


def _profile_completion_for(
    session: Session,
    user: User,
    *,
    profile: Optional[EmployeeProfile] = None,
    clockify_ready: Optional[bool] = None,
) -> dict[str, Any]:
    profile = profile or session.get(EmployeeProfile, user.id)
    clockify_ready = clockify_is_configured() if clockify_ready is None else clockify_ready
    acknowledged = _policy_acknowledgements_for(session, user)
    missing_policies = [
        p
        for p in _published_policies(session)
        if p.get("requires_ack", True) and p["id"] not in acknowledged
    ]
    phone_done = bool(_decrypt_optional(profile.phone_enc if profile else None))
    emergency_done = bool(
        _decrypt_optional(profile.emergency_contact_name_enc if profile else None)
        and _decrypt_optional(profile.emergency_contact_phone_enc if profile else None)
    )
    clockify_done = bool(
        clockify_ready and profile and (profile.clockify_user_id or "").strip()
    )
    items = [
        {
            "key": "phone",
            "label": "Phone number",
            "done": phone_done,
            "href": "/team/profile",
            "hint": "Needed for schedule and time-off texts.",
        },
        {
            "key": "emergency",
            "label": "Emergency contact",
            "done": emergency_done,
            "href": "/team/profile",
            "hint": "Name and phone number.",
        },
        {
            "key": "policies",
            "label": "Policies signed",
            "done": not missing_policies,
            "href": "/team/policies",
            "hint": (
                "All caught up."
                if not missing_policies
                else f"{len(missing_policies)} left to sign."
            ),
        },
        {
            "key": "clockify",
            "label": "Clockify connected",
            "done": clockify_done,
            "href": "/team/hours",
            "hint": "Ask a manager to connect this if hours look blank.",
        },
    ]
    complete_count = sum(1 for item in items if item["done"])
    return {
        "items": items,
        "complete_count": complete_count,
        "total_count": len(items),
        "percent": int((complete_count / len(items)) * 100) if items else 100,
        "missing_policies": missing_policies,
        "is_complete": complete_count == len(items),
        "phone_ready": phone_done,
        "clockify_ready": clockify_done,
    }


def _today_focus_for(
    *,
    today_shifts: list[dict[str, Any]],
    pay_summary: dict[str, Any],
    announcements: list[TeamAnnouncement],
    profile_completion: dict[str, Any],
    schedule_href: str,
) -> dict[str, Any]:
    shift_text = (
        "; ".join(
            (row.get("label") or "Shift").strip()
            for row in today_shifts
            if (row.get("label") or "").strip()
        )
        or "You are scheduled today."
        if today_shifts
        else "No shift today."
    )
    clocked_in = (pay_summary.get("clocked_in_today_label") or "").strip()
    break_label = (pay_summary.get("break_today_label") or "Not yet").strip()
    missing_policies = profile_completion.get("missing_policies") or []
    items = [
        {
            "label": "Shift",
            "value": shift_text,
            "href": schedule_href,
            "state": "ok" if today_shifts else "neutral",
        },
        {
            "label": "Clock status",
            "value": (
                f"Clocked in at {clocked_in}."
                if clocked_in and clocked_in != "Not clocked in"
                else "Clock in from the shop iPad when you arrive."
            ),
            "href": "/team/hours",
            "state": (
                "ok"
                if clocked_in and clocked_in != "Not clocked in"
                else ("todo" if today_shifts else "neutral")
            ),
        },
        {
            "label": "Break",
            "value": (
                "Break recorded."
                if break_label.startswith(("Taken", "On break"))
                else (
                    "No break clocked yet. If a 5+ hour shift misses a break, 30 minutes is deducted."
                    if today_shifts
                    else "No break needed unless you work today."
                )
            ),
            "href": "/team/hours",
            "state": (
                "ok"
                if break_label.startswith(("Taken", "On break"))
                else ("todo" if today_shifts else "neutral")
            ),
        },
        {
            "label": "Announcements",
            "value": (
                f"{len(announcements)} current update{'s' if len(announcements) != 1 else ''}."
                if announcements
                else "Nothing new right now."
            ),
            "href": "/team/announcements",
            "state": "todo" if announcements else "ok",
        },
        {
            "label": "Policies",
            "value": (
                f"{len(missing_policies)} unsigned polic{'ies' if len(missing_policies) != 1 else 'y'}."
                if missing_policies
                else "All signed."
            ),
            "href": "/team/policies",
            "state": "todo" if missing_policies else "ok",
        },
    ]
    todo_count = sum(1 for item in items if item["state"] == "todo")
    return {"items": items, "todo_count": todo_count}


def _employee_notifications_for(
    session: Session,
    user: User,
    *,
    limit: int = 20,
    since_id: int = 0,
    newest_first: bool = True,
) -> list[dict[str, Any]]:
    stmt = (
        select(AuditLog)
        .where(AuditLog.target_user_id == user.id)
        .where(AuditLog.action == EMPLOYEE_NOTIFICATION_ACTION)
    )
    if since_id > 0:
        stmt = stmt.where(AuditLog.id > since_id)
    if newest_first:
        stmt = stmt.order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
    else:
        stmt = stmt.order_by(AuditLog.created_at.asc(), AuditLog.id.asc())
    rows = session.exec(stmt.limit(limit)).all()
    notifications: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row.details_json or "{}")
        except json.JSONDecodeError:
            payload = {}
        notifications.append(
            {
                "id": row.id,
                "kind": str(payload.get("kind") or "general"),
                "title": str(payload.get("title") or "Team update"),
                "body": str(payload.get("body") or ""),
                "link_path": str(payload.get("link_path") or "/team/"),
                "created_at": row.created_at,
                "sms": payload.get("sms") if isinstance(payload.get("sms"), dict) else {},
            }
        )
    return notifications


@router.get("/team/notifications/poll")
def team_notifications_poll(
    request: Request,
    since_id: int = Query(default=0),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="page.announcements"
    )
    if denial:
        return denial
    notifications = _employee_notifications_for(
        session,
        user,
        limit=10,
        since_id=max(0, since_id),
        newest_first=False,
    )
    latest_seen = max(
        [since_id]
        + [int(note["id"]) for note in notifications if note.get("id") is not None]
    )
    return {
        "latest_id": latest_seen,
        "notifications": [
            {
                "id": note["id"],
                "kind": note["kind"],
                "title": note["title"],
                "body": note["body"],
                "link_path": note["link_path"],
                "created_at": note["created_at"].isoformat()
                if note.get("created_at")
                else None,
            }
            for note in notifications
        ],
    }


@router.get("/team/announcements", response_class=HTMLResponse)
def team_announcements(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="page.announcements"
    )
    if denial:
        return denial

    announcements = _active_announcements_for(session)
    creator_ids = {
        row.created_by_user_id
        for row in announcements
        if row.created_by_user_id is not None
    }
    authors: dict[int, User] = {}
    if creator_ids:
        authors = {
            author.id: author
            for author in session.exec(
                select(User).where(User.id.in_(creator_ids))
            ).all()
            if author.id is not None
        }

    return templates.TemplateResponse(
        request,
        "team/announcements.html",
        {
            "request": request,
            "title": "Announcements",
            "active": "announcements",
            "current_user": user,
            "announcements": announcements,
            "authors": authors,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/notifications", response_class=HTMLResponse)
def team_notifications(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="page.announcements"
    )
    if denial:
        return denial
    profile_completion = _profile_completion_for(session, user)
    timeoff_rows = session.exec(
        select(TimeOffRequest)
        .where(TimeOffRequest.submitted_by_user_id == user.id)
        .where(TimeOffRequest.status.in_(("approved", "denied")))
        .order_by(TimeOffRequest.updated_at.desc(), TimeOffRequest.id.desc())
        .limit(5)
    ).all()
    return templates.TemplateResponse(
        request,
        "team/notifications.html",
        {
            "request": request,
            "title": "Notifications",
            "active": "notifications",
            "current_user": user,
            "notifications": _employee_notifications_for(session, user),
            "active_announcements": _active_announcements_for(session, limit=5),
            "timeoff_rows": list(timeoff_rows),
            "profile_completion": profile_completion,
            "sms_enabled": bool(profile_completion.get("phone_ready")),
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/help", response_class=HTMLResponse)
def team_help(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/help.html",
        {
            "request": request,
            "title": "Ask for Help",
            "active": "",
            "current_user": user,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/help/tutorial", response_class=HTMLResponse)
def team_help_tutorial(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/help_tutorial.html",
        {
            "request": request,
            "title": "Portal Tour",
            "active": "",
            "current_user": user,
            "csrf_token": issue_token(request),
            "tutorial_links": True,
            "show_manager_tutorial": user.role == "manager",
            **_nav_context(session, user),
        },
    )


@router.get("/team/tools/inventory")
def team_tool_inventory(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return RedirectResponse("/inventory/scan?team_shell=1", status_code=303)


@router.get("/team/tools/degen-eye")
def team_tool_degen_eye(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return RedirectResponse("/degen_eye?team_shell=1", status_code=303)


@router.get("/team/tools/live-stream")
def team_tool_live_stream(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    return RedirectResponse("/tiktok/streamer?team_shell=1", status_code=303)


_SHIFT_START_RE = re.compile(
    r"^\s*(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>a|am|p|pm)?\b",
    re.IGNORECASE,
)


def _parse_shift_start_minutes(label: str) -> Optional[int]:
    """Best-effort sort key for schedule labels like "10:30 AM - 6 PM"."""
    match = _SHIFT_START_RE.search((label or "").strip())
    if not match:
        return None
    hour = int(match.group("hour"))
    minute = int(match.group("minute") or "0")
    if minute > 59:
        return None
    ampm = (match.group("ampm") or "").lower()
    if ampm.startswith("p"):
        if hour != 12:
            hour += 12
    elif ampm.startswith("a"):
        if hour == 12:
            hour = 0
    elif 1 <= hour <= 5:
        # Store shifts written as "3-7" usually mean afternoon.
        hour += 12
    if hour > 23:
        return None
    return hour * 60 + minute


def _today_shifts_for(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
) -> list[dict[str, Any]]:
    today = today or _portal_today()
    shifts = list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == user.id)
            .where(ShiftEntry.shift_date == today)
            .where(
                ~ShiftEntry.kind.in_(
                    (SHIFT_KIND_REQUEST, SHIFT_KIND_OFF, SHIFT_KIND_BLANK)
                )
            )
            .order_by(ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )
    if not shifts:
        return []

    day_note_row = session.exec(
        select(ScheduleDayNote).where(ScheduleDayNote.day_date == today)
    ).first()
    day_note = None
    if day_note_row is not None:
        day_note = (
            (day_note_row.location_label or "").strip()
            or (day_note_row.notes or "").strip()
            or None
        )
    return [
        {
            "shift_date": shift.shift_date,
            "label": shift.label,
            "kind": shift.kind,
            "day_note": day_note,
        }
        for shift in shifts
    ]


def _upcoming_shifts_for(
    session: Session,
    user: User,
    *,
    today: Optional[date] = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    today = today or _portal_today()
    shifts = list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == user.id)
            .where(ShiftEntry.shift_date >= today)
            .where(
                ~ShiftEntry.kind.in_(
                    (SHIFT_KIND_REQUEST, SHIFT_KIND_OFF, SHIFT_KIND_BLANK)
                )
            )
            .order_by(ShiftEntry.shift_date, ShiftEntry.sort_order, ShiftEntry.id)
            .limit(limit)
        ).all()
    )
    if not shifts:
        return []

    dates = sorted({shift.shift_date for shift in shifts})
    notes = {
        note.day_date: note
        for note in session.exec(
            select(ScheduleDayNote).where(ScheduleDayNote.day_date.in_(dates))
        ).all()
    }
    out: list[dict[str, Any]] = []
    for shift in shifts:
        day_note_row = notes.get(shift.shift_date)
        day_note = None
        if day_note_row is not None:
            day_note = (
                (day_note_row.location_label or "").strip()
                or (day_note_row.notes or "").strip()
                or None
            )
        out.append(
            {
                "shift_date": shift.shift_date,
                "label": shift.label,
                "kind": shift.kind,
                "day_note": day_note,
            }
        )
    return out


def _today_staffing_for(
    session: Session,
    *,
    today: Optional[date] = None,
) -> list[dict[str, Any]]:
    today = today or _portal_today()
    shifts = list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.shift_date == today)
            .where(ShiftEntry.kind.in_((SHIFT_KIND_WORK, SHIFT_KIND_ALL)))
            .order_by(ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )
    if not shifts:
        return []

    user_ids = sorted({shift.user_id for shift in shifts})
    users = {
        user.id: user
        for user in session.exec(select(User).where(User.id.in_(user_ids))).all()
    }
    grouped: dict[int, dict[str, Any]] = {}
    first_start: dict[int, int] = {}
    for shift in shifts:
        scheduled_user = users.get(shift.user_id)
        display_name = (
            (scheduled_user.display_name or scheduled_user.username)
            if scheduled_user is not None
            else f"User {shift.user_id}"
        )
        row = grouped.setdefault(
            shift.user_id,
            {"display_name": display_name, "shifts": []},
        )
        row["shifts"].append((shift.label or "").strip() or "Shift")
        start = _parse_shift_start_minutes(shift.label)
        if start is not None:
            first_start[shift.user_id] = min(start, first_start.get(shift.user_id, start))

    def sort_key(item: tuple[int, dict[str, Any]]) -> tuple[bool, int, str]:
        user_id, row = item
        start = first_start.get(user_id)
        return (
            start is None,
            start if start is not None else 0,
            str(row["display_name"]).casefold(),
        )

    return [row for _, row in sorted(grouped.items(), key=sort_key)]


def _profile_for(session: Session, user_id: int) -> EmployeeProfile:
    row = session.get(EmployeeProfile, user_id)
    if row is None:
        row = EmployeeProfile(user_id=user_id)
        session.add(row)
        session.commit()
        session.refresh(row)
    return row


def _decode_address(blob: Optional[bytes]) -> dict[str, str]:
    if not blob:
        return {}
    try:
        raw = decrypt_pii(blob) or ""
        if not raw:
            return {}
        return json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return {}


@router.get("/team/profile", response_class=HTMLResponse)
def team_profile(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    profile = _profile_for(session, user.id)
    # Self-view of own PII is not audited per spec.
    phone = decrypt_pii(profile.phone_enc) or ""
    email = decrypt_pii(profile.email_ciphertext) or ""
    legal_name = decrypt_pii(profile.legal_name_enc) or ""
    emergency_contact_name = decrypt_pii(profile.emergency_contact_name_enc) or ""
    emergency_contact_phone = decrypt_pii(profile.emergency_contact_phone_enc) or ""
    address = _decode_address(profile.address_enc)
    profile_completion = _profile_completion_for(session, user, profile=profile)
    return templates.TemplateResponse(
        request,
        "team/profile.html",
        {
            "request": request,
            "title": "My Profile",
            "active": "profile",
            "current_user": user,
            "profile": profile,
            "preferred_name": user.display_name or "",
            "legal_name": legal_name,
            "email": email,
            "phone": phone,
            "emergency_contact_name": emergency_contact_name,
            "emergency_contact_phone": emergency_contact_phone,
            "address": address,
            "profile_completion": profile_completion,
            "flash": flash,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post("/team/profile", dependencies=[Depends(require_csrf)])
async def team_profile_post(
    request: Request,
    preferred_name: str = Form(default=""),
    legal_name: str = Form(default=""),
    email: str = Form(default=""),
    phone: str = Form(default=""),
    emergency_contact_name: str = Form(default=""),
    emergency_contact_phone: str = Form(default=""),
    address_street: str = Form(default=""),
    address_city: str = Form(default=""),
    address_state: str = Form(default=""),
    address_zip: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:profile:{user.id}",
        max_requests=20,
        window_seconds=900,
    ):
        return limited
    profile = _profile_for(session, user.id)
    now = utcnow()
    changed: list[str] = []

    # Re-fetch into the router session; the middleware-supplied `user` is
    # detached from any session and cannot be safely mutated here.
    db_user = session.get(User, user.id)
    new_display = (preferred_name or "").strip()
    if db_user is not None and new_display and new_display != (db_user.display_name or ""):
        db_user.display_name = new_display
        db_user.updated_at = now
        session.add(db_user)
        changed.append("preferred_name")

    def _maybe_set_enc(attr: str, raw: str, label: str) -> None:
        try:
            current = decrypt_pii(getattr(profile, attr)) or ""
        except (PIIDecryptError, ValueError):
            current = ""
        raw_s = (raw or "").strip()
        if raw_s != current:
            setattr(profile, attr, encrypt_pii(raw_s) if raw_s else None)
            changed.append(label)

    _maybe_set_enc("legal_name_enc", legal_name, "legal_name")
    _maybe_set_enc("phone_enc", phone, "phone")
    _maybe_set_enc(
        "emergency_contact_name_enc",
        emergency_contact_name,
        "emergency_contact_name",
    )
    _maybe_set_enc(
        "emergency_contact_phone_enc",
        emergency_contact_phone,
        "emergency_contact_phone",
    )

    # Email needs both the ciphertext AND the lookup hash kept in sync.
    from ..pii import email_lookup_hash as _email_hash
    new_email = (email or "").strip().lower()
    try:
        current_email = decrypt_pii(profile.email_ciphertext) or ""
    except (PIIDecryptError, ValueError):
        current_email = ""
    if new_email != current_email:
        if new_email:
            new_hash = _email_hash(new_email)
            clash = session.exec(
                select(EmployeeProfile).where(
                    EmployeeProfile.email_lookup_hash == new_hash,
                    EmployeeProfile.user_id != user.id,
                )
            ).first()
            if clash is not None:
                return RedirectResponse(
                    "/team/profile?flash=That+email+is+already+taken.", status_code=303
                )
            profile.email_ciphertext = encrypt_pii(new_email)
            profile.email_lookup_hash = new_hash
        else:
            profile.email_ciphertext = None
            profile.email_lookup_hash = None
        changed.append("email")

    address_payload = {
        "street": (address_street or "").strip(),
        "city": (address_city or "").strip(),
        "state": (address_state or "").strip(),
        "zip": (address_zip or "").strip(),
    }
    current_address = _decode_address(profile.address_enc)
    if address_payload != current_address:
        if any(address_payload.values()):
            profile.address_enc = encrypt_pii(json.dumps(address_payload))
        else:
            profile.address_enc = None
        changed.append("address")

    if changed:
        profile.updated_at = now
        session.add(profile)
        session.add(
            AuditLog(
                actor_user_id=user.id,
                target_user_id=user.id,
                action="profile.self_update",
                details_json=json.dumps({"fields": changed}),
                ip_address=(request.client.host if request.client else None),
            )
        )
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            return RedirectResponse("/team/profile?flash=email_taken", status_code=303)
    return RedirectResponse("/team/profile?flash=Saved.", status_code=303)


# ---------------------------------------------------------------------------
# Self-serve password change (authenticated)
# ---------------------------------------------------------------------------
# Sibling of /team/password/reset/<token>. That one is for people who forgot
# their password (admin issues reset link). This one is for people who know
# their current password and just want to rotate it — no admin in the loop,
# but auditable. Lives here (not in the auth reset module) because it's
# authenticated and nav-integrated.

@router.get("/team/password/change", response_class=HTMLResponse)
def team_password_change_page(
    request: Request,
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    problems: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/password_change.html",
        {
            "request": request,
            "title": "Change password",
            "active": "profile",
            "current_user": user,
            "flash": flash,
            "error": error,
            "problems": (problems or "").split("|") if problems else [],
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post("/team/password/change", dependencies=[Depends(require_csrf)])
async def team_password_change_post(
    request: Request,
    current_password: str = Form(default=""),
    new_password: str = Form(default=""),
    confirm_password: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.profile")
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request, key_prefix=f"team:pwchange:{user.id}", max_requests=8, window_seconds=900.0
    ):
        return limited
    if new_password != confirm_password:
        return RedirectResponse(
            "/team/password/change?error=New+password+and+confirmation+don%27t+match.",
            status_code=303,
        )
    try:
        user = change_user_password(
            session,
            user,
            current_password=current_password,
            new_password=new_password,
            ip_address=(request.client.host if request.client else None),
        )
    except BadCurrentPasswordError as exc:
        code = str(exc)
        message = {
            "current_password_required": "Enter your current password.",
            "current_password_wrong": "That's not your current password.",
        }.get(code, "Could not verify your current password.")
        from urllib.parse import quote_plus
        return RedirectResponse(
            f"/team/password/change?error={quote_plus(message)}",
            status_code=303,
        )
    except WeakPasswordError as exc:
        qs = "problems=" + "|".join(p.replace(" ", "+") for p in exc.problems)
        return RedirectResponse(
            f"/team/password/change?{qs}",
            status_code=303,
        )
    except ValueError as exc:
        code = str(exc)
        message = {
            "new_password_required": "Choose a new password.",
            "new_password_same_as_current": "Your new password has to be different from the current one.",
        }.get(code, "Could not update password.")
        from urllib.parse import quote_plus
        return RedirectResponse(
            f"/team/password/change?error={quote_plus(message)}",
            status_code=303,
        )
    request.session["password_changed_at"] = _password_changed_session_value(user)
    request.session["session_invalidated_at"] = _session_invalidated_session_value(user)
    rotate_token(request)
    return RedirectResponse(
        "/team/profile?flash=Password+updated.",
        status_code=303,
    )


@router.get("/team/policies", response_class=HTMLResponse)
def team_policies(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.policies")
    if denial:
        return denial
    policies = _published_policies(session)
    acknowledged = _policy_acknowledgements_for(session, user)
    return templates.TemplateResponse(
        request,
        "team/policies.html",
        {
            "request": request,
            "title": "Policies",
            "active": "policies",
            "current_user": user,
            "policies": policies,
            "acknowledged": acknowledged,
            "flash": flash,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post(
    "/team/policies/acknowledge/{policy_id}",
    dependencies=[Depends(require_csrf)],
)
async def team_policies_acknowledge(
    request: Request,
    policy_id: str,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.policies")
    if denial:
        return denial
    policy = _policy_by_id(session, policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="policy_not_found")
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="policy.acknowledge",
            resource_key=f"policy.{policy_id}",
            details_json=json.dumps(
                {
                    "policy_id": policy_id,
                    "policy_version": policy["version"],
                    "policy_title": policy["title"],
                    "policy_kind": policy.get("kind", "policy"),
                }
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse("/team/policies?flash=Acknowledged.", status_code=303)


@router.get("/team/hours", response_class=HTMLResponse)
def team_hours(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.hours")
    if denial:
        return denial
    settings = get_settings()
    clockify_ready = clockify_is_configured(settings)
    profile = session.get(EmployeeProfile, user.id)
    clockify_user_id = (profile.clockify_user_id or "").strip() if profile else ""
    clockify_summary = None
    clockify_error = None
    if clockify_ready and clockify_user_id:
        try:
            clockify_summary = clockify_client_from_settings(settings).user_week_summary(
                clockify_user_id,
                today=_portal_today(settings=settings),
                settings=settings,
            )
        except (ClockifyApiError, ClockifyConfigError) as exc:
            clockify_error = str(exc)
    return templates.TemplateResponse(
        request,
        "team/hours.html",
        {
            "request": request,
            "title": "My Hours",
            "active": "hours",
            "current_user": user,
            "clockify_ready": clockify_ready,
            "clockify_user_id": clockify_user_id,
            "clockify_summary": clockify_summary,
            "clockify_error": clockify_error,
            "format_hours": format_hours,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/schedule", response_class=HTMLResponse)
def team_schedule(
    request: Request,
    week: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.schedule")
    if denial:
        return denial
    # Reuse the admin grid builder so the employee view is literally the
    # same visual — no translation layer, no "my shifts" fork. Everyone
    # sees the published grid the same way; only the top-level wrapper
    # differs (admin has inputs, employee has static cells).
    from .team_admin_schedule import (
        _build_cell_key,
        _build_day_loc_key,
        _grid_context,
        _parse_week_start,
    )
    from ..models import STAFF_KIND_STOREFRONT, STAFF_KIND_STREAM

    week_start = _parse_week_start(week)
    storefront_ctx = _grid_context(
        session, week_start, staff_kind=STAFF_KIND_STOREFRONT
    )
    stream_ctx = _grid_context(
        session, week_start, staff_kind=STAFF_KIND_STREAM
    )
    return templates.TemplateResponse(
        request,
        "team/schedule.html",
        {
            "request": request,
            "title": "Schedule",
            "active": "schedule",
            "current_user": user,
            "csrf_token": issue_token(request),
            "build_cell_key": _build_cell_key,
            "build_day_loc_key": _build_day_loc_key,
            "storefront": storefront_ctx,
            "stream": stream_ctx,
            "week_start": storefront_ctx["week_start"],
            "week_days": storefront_ctx["week_days"],
            "day_note_map": storefront_ctx["day_note_map"],
            "prev_week": storefront_ctx["prev_week"],
            "next_week": storefront_ctx["next_week"],
            "this_week": storefront_ctx["this_week"],
            "is_current_week": storefront_ctx["is_current_week"],
            "today": _portal_today(),
            **_nav_context(session, user),
        },
    )


@router.get("/team/supply", response_class=HTMLResponse)
def team_supply(
    request: Request,
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="page.supply_requests"
    )
    if denial:
        return denial
    rows = session.exec(
        select(SupplyRequest)
        .where(SupplyRequest.submitted_by_user_id == user.id)
        .order_by(SupplyRequest.created_at.desc())
    ).all()
    return templates.TemplateResponse(
        request,
        "team/supply.html",
        {
            "request": request,
            "title": "Supply Requests",
            "active": "supply",
            "current_user": user,
            "requests": list(rows),
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.post("/team/supply", dependencies=[Depends(require_csrf)])
async def team_supply_post(
    request: Request,
    title: str = Form(default=""),
    description: str = Form(default=""),
    urgency: str = Form(default="normal"),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(
        request, session, resource_key="action.supply_request.submit"
    )
    if denial:
        return denial
    if limited := rate_limited_or_429(
        request,
        key_prefix=f"team:supply:{user.id}",
        max_requests=10,
        window_seconds=3600.0,
    ):
        return limited
    clean_title = (title or "").strip()
    if not clean_title:
        return RedirectResponse(
            "/team/supply?error=Title+is+required.", status_code=303
        )
    if urgency not in ("low", "normal", "high"):
        urgency = "normal"
    row = SupplyRequest(
        submitted_by_user_id=user.id,
        title=clean_title[:200],
        description=(description or "")[:4000],
        urgency=urgency,
        status="submitted",
    )
    session.add(row)
    session.commit()
    return RedirectResponse("/team/supply?flash=Request+submitted.", status_code=303)
