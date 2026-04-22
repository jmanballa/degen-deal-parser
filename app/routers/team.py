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
import json
from typing import Any, Optional, Tuple

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlmodel import Session, select

from .. import permissions as perms
from ..auth import (
    WeakPasswordError,
    authenticate_user,
    consume_invite_token,
    consume_password_reset_token,
    has_permission,
    validate_password_strength,
)
from ..config import get_settings
from ..csrf import issue_token, require_csrf, rotate_token
from ..db import get_session
from ..models import AuditLog, EmployeeProfile, SupplyRequest, User, utcnow
from ..pii import decrypt_pii, encrypt_pii
from ..rate_limit import rate_limited_or_429
from ..shared import templates

router = APIRouter()


POLICIES: tuple[dict, ...] = (
    {
        "id": "code-of-conduct",
        "title": "Code of Conduct",
        "version": "v1",
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
        "body_md": (
            "Wash hands before handling cards. Sleeve slabs before storage. "
            "Never leave inventory unattended in common areas. "
            "Power tools require PPE; stop and ask if unsure."
        ),
    },
)
POLICY_BY_ID = {p["id"]: p for p in POLICIES}


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

@router.get("/team/login", response_class=HTMLResponse)
def team_login_page(
    request: Request,
    error: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
):
    _portal_or_404()
    user = getattr(request.state, "current_user", None)
    if user is not None:
        if user.role == "employee":
            return RedirectResponse("/team/", status_code=303)
        return RedirectResponse("/dashboard", status_code=303)
    return templates.TemplateResponse(
        request,
        "team/login.html",
        {
            "request": request,
            "title": "Team Sign In",
            "error": error,
            "flash": flash,
            "csrf_token": issue_token(request),
        },
    )


@router.post("/team/login")
async def team_login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(default=""),
    session: Session = Depends(get_session),
):
    _portal_or_404()
    ip = request.client.host if request.client else None
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
            "/team/login?error=Session+expired.+Please+try+again.",
            status_code=303,
        )

    user = authenticate_user(session, username, password, ip_address=ip)
    if not user:
        return RedirectResponse(
            "/team/login?error=Invalid+username+or+password", status_code=303
        )

    request.session["user_id"] = user.id
    rotate_token(request)  # m1 — bind a fresh CSRF to the authenticated session
    if user.role == "employee":
        return RedirectResponse("/team/", status_code=303)
    return RedirectResponse("/dashboard", status_code=303)


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
):
    _portal_or_404()
    return templates.TemplateResponse(
        request,
        "team/invite_accept.html",
        {
            "request": request,
            "title": "Accept Invite",
            "token": token,
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
    rotate_token(request)
    return RedirectResponse("/team/?flash=Welcome+to+the+team!", status_code=303)


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
    # No enumeration: target_user_id is always None so admins reading the
    # audit log cannot trivially distinguish exists-vs-not. A hashed probe
    # goes into details_json for investigation without revealing structure.
    probe = (identifier or "").strip().lower()
    matched = False
    if probe:
        existing = session.exec(select(User).where(User.username == probe)).first()
        if existing is not None and existing.is_active:
            matched = True
    probe_hash = (
        hashlib.sha256(probe.encode("utf-8")).hexdigest() if probe else ""
    )
    session.add(
        AuditLog(
            action="password.reset_requested",
            target_user_id=None,
            details_json=json.dumps(
                {"username_hash": probe_hash, "matched": matched, "source": "http_forgot"}
            ),
            ip_address=(request.client.host if request.client else None),
        )
    )
    session.commit()
    return RedirectResponse(
        "/team/password/forgot?flash=If+your+account+exists%2C+ask+an+admin+for+a+reset+link.",
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
    keys = (
        ("dashboard", "page.dashboard", "/team/"),
        ("hours", "page.hours", "/team/hours"),
        ("schedule", "page.schedule", "/team/schedule"),
        ("policies", "page.policies", "/team/policies"),
        ("supply", "page.supply_requests", "/team/supply"),
        ("profile", "page.profile", "/team/profile"),
    )
    nav = []
    for name, key, href in keys:
        if has_permission(session, user, key, cache=cache):
            nav.append({"name": name, "href": href})

    # Admin-only section. Rendered as a separate group in the sidebar when
    # at least one entry is visible. Gated per-key against the perms matrix
    # so managers/reviewers only see the admin links they actually have.
    admin_keys = (
        ("employees", "page.admin.employees", "/team/admin/employees"),
        ("invites", "page.admin.invites", "/team/admin/invites"),
        ("permissions", "page.admin.permissions", "/team/admin/permissions"),
        ("supply-queue", "page.admin.supply", "/team/admin/supply"),
    )
    admin_nav = []
    for name, key, href in admin_keys:
        if has_permission(session, user, key, cache=cache):
            admin_nav.append({"name": name, "href": href})

    # "Tools" section — ops pages selectively exposed to rank-and-file staff.
    # These are pages where TikTok numbers / public market prices are OK but
    # internal cost basis / margins / P&L are NOT present. Gated against the
    # user's role rather than the perms matrix so every authenticated employee
    # gets them out of the box (matches the role drops in inventory.py +
    # tiktok_streamer.py).
    tools_nav = []
    role = (user.role or "").lower()
    if role in ("employee", "viewer", "manager", "reviewer", "admin"):
        tools_nav.append({"name": "live-stream", "href": "/tiktok/streamer"})
        tools_nav.append({"name": "degen-eye", "href": "/degen_eye"})

    return {
        "nav_items": nav,
        "admin_nav_items": admin_nav,
        "tools_nav_items": tools_nav,
    }


@router.get("/team/", response_class=HTMLResponse)
def team_dashboard(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.dashboard")
    if denial:
        return denial
    widgets = perms.allowed_widgets_for(session, user)
    clockify_ready = bool((get_settings().clockify_api_key or "").strip())
    return templates.TemplateResponse(
        request,
        "team/dashboard.html",
        {
            "request": request,
            "title": "Dashboard",
            "active": "dashboard",
            "current_user": user,
            "widgets": widgets,
            "clockify_ready": clockify_ready,
            "now_hour": utcnow().hour,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


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
        current = decrypt_pii(getattr(profile, attr)) or ""
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
    current_email = decrypt_pii(profile.email_ciphertext) or ""
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
        session.commit()
    return RedirectResponse("/team/profile?flash=Saved.", status_code=303)


@router.get("/team/policies", response_class=HTMLResponse)
def team_policies(
    request: Request,
    flash: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.policies")
    if denial:
        return denial
    # Which policies have I acknowledged? Look at my own AuditLog rows.
    ack_rows = session.exec(
        select(AuditLog).where(
            AuditLog.actor_user_id == user.id,
            AuditLog.action == "policy.acknowledge",
        )
    ).all()
    acknowledged: set[str] = set()
    for row in ack_rows:
        try:
            d = json.loads(row.details_json or "{}")
            pid = d.get("policy_id")
            if isinstance(pid, str):
                acknowledged.add(pid)
        except json.JSONDecodeError:
            continue
    return templates.TemplateResponse(
        request,
        "team/policies.html",
        {
            "request": request,
            "title": "Policies",
            "active": "policies",
            "current_user": user,
            "policies": POLICIES,
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
    policy = POLICY_BY_ID.get(policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="policy_not_found")
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="policy.acknowledge",
            resource_key=f"policy.{policy_id}",
            details_json=json.dumps(
                {"policy_id": policy_id, "policy_version": policy["version"]}
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
    clockify_ready = bool((get_settings().clockify_api_key or "").strip())
    # TODO(Wave 5): fetch time entries from Clockify here.
    return templates.TemplateResponse(
        request,
        "team/hours.html",
        {
            "request": request,
            "title": "My Hours",
            "active": "hours",
            "current_user": user,
            "clockify_ready": clockify_ready,
            "csrf_token": issue_token(request),
            **_nav_context(session, user),
        },
    )


@router.get("/team/schedule", response_class=HTMLResponse)
def team_schedule(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _require_employee(request, session, resource_key="page.schedule")
    if denial:
        return denial
    return templates.TemplateResponse(
        request,
        "team/schedule.html",
        {
            "request": request,
            "title": "My Schedule",
            "active": "schedule",
            "current_user": user,
            "csrf_token": issue_token(request),
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
