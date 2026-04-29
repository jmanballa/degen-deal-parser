"""Walk-through simulation of the new-employee journey.

This isn't a pytest test — it's a runnable script to exercise the full
onboarding → dashboard → supply → profile → schedule → policies path end
to end, exactly like a new hire on a phone. It prints a summary of what
happened at each step plus a couple of phone-specific sanity checks so we
can eyeball the flow without having to click through the UI by hand.

Run with:  .\\.venv\\Scripts\\python.exe -m tests._walkthrough_portal
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from html import unescape
from urllib.parse import urlparse, parse_qs

# IMPORTANT: set all env BEFORE importing anything from `app.*` so that
# config.get_settings() reads the dev/test values instead of whatever is
# in .env (prod Postgres, session-https-only, prod session domain).
_tmp_db = os.path.join(tempfile.gettempdir(), "degen_walkthrough.db")
if os.path.exists(_tmp_db):
    os.remove(_tmp_db)
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp_db}"
os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true"
os.environ["SESSION_HTTPS_ONLY"] = "false"
os.environ["SESSION_DOMAIN"] = ""
os.environ["SESSION_SECRET"] = "walkthrough-session-secret-long-enough"

from cryptography.fernet import Fernet  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402
from sqlmodel import Session, create_engine  # noqa: E402

os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "walkthrough-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "walkthrough-hmac")


def banner(msg: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {msg}")
    print("=" * 72)


def check(label: str, cond: bool, detail: str = "") -> None:
    mark = "OK " if cond else "!! "
    print(f"  [{mark}] {label}" + (f"  -- {detail}" if detail else ""))


def csrf(html: str) -> str:
    m = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    if not m:
        raise RuntimeError("no csrf token found on page")
    return unescape(m.group(1))


def main() -> int:
    from app.models import SQLModel, User, utcnow
    from app import auth as auth_mod
    from app import config as cfg
    from app.db import seed_employee_portal_defaults

    # File-backed SQLite so the dependency_override + app use the same DB.
    engine = create_engine(
        os.environ["DATABASE_URL"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)

    # Swap the module-level app.db engine to this temporary one so the
    # middleware (which uses managed_session() from app.db) sees our data.
    import app.db as app_db
    app_db.engine = engine

    with Session(engine) as s:
        seed_employee_portal_defaults(s)
        admin = User(
            username="owner",
            password_hash="x",
            password_salt="x",
            display_name="Owner",
            role="admin",
            is_active=True,
        )
        s.add(admin)
        s.commit()
        s.refresh(admin)

        raw_token = auth_mod.generate_invite_token(
            s,
            role="employee",
            created_by_user_id=admin.id,
            ttl_hours=48,
        )

    cfg.get_settings.cache_clear()
    import importlib
    import app.main as app_main
    importlib.reload(app_main)

    from app.db import get_session as real_get_session

    def _ov():
        with Session(engine) as ses:
            yield ses

    app_main.app.dependency_overrides[real_get_session] = _ov

    from fastapi.testclient import TestClient
    client = TestClient(app_main.app, follow_redirects=False)

    # ---------- Step 1: Tap the invite link ----------
    banner("Step 1 — tap the invite link on my phone")
    r = client.get(f"/team/invite/accept/{raw_token}")
    check("invite page loads", r.status_code == 200, f"status={r.status_code}")
    page = r.text
    check("mobile viewport set", 'name="viewport"' in page)
    check("shows friendly 'Welcome' copy", "Welcome" in page)
    check("has Start button", "Start" in page)
    check(
        "renders 8 progress dots",
        page.count('class="onb-progress-dot') == 8,
    )
    check("username input present", 'name="new_username"' in page)
    check("password input present", 'name="new_password"' in page)
    check(
        "show/hide password affordance present",
        ('data-toggle="password"' in page) or ('aria-label="Show password"' in page),
        "phone users REALLY need this",
    )
    check(
        "address fields present",
        all(f'name="{n}"' in page for n in
            ("address_street", "address_city", "address_state", "address_zip")),
    )
    check(
        "emergency contact fields present",
        all(f'name="{n}"' in page for n in
            ("emergency_contact_name", "emergency_contact_phone")),
    )
    check("phone web-app guidance present", "Add this to your phone." in page)
    check("employee portal tour present", "Know where everything lives." in page)
    check("skip tutorial action present", "Skip tutorial" in page)
    check("Degen logo present", "degen-collectibles-180.png" in page)
    check("team portal manifest present", "/static/team.webmanifest" in page)
    check(
        "css cache-buster matches base.html",
        "/static/portal.css?v=2026042607" in page,
        "onboarding page is on v=2026042607",
    )
    tok = csrf(page)

    # ---------- Step 2: submit the onboarding form ----------
    banner("Step 2 — fill out the onboarding wizard and submit")
    form = {
        "csrf_token": tok,
        "new_username": "pikachu.pete",
        "new_password": "Thunderbolt!42zzz",
        "preferred_name": "Pete",
        "legal_name": "Peter Parker",
        "email": "pete@example.com",
        "phone": "(555) 222-3333",
        "address_street": "123 Route St",
        "address_city": "Pallet",
        "address_state": "CA",
        "address_zip": "95110",
        "emergency_contact_name": "Professor Oak",
        "emergency_contact_phone": "(555) 111-0000",
    }
    r = client.post(f"/team/invite/accept/{raw_token}", data=form)
    check("form submit accepted", r.status_code == 303, f"status={r.status_code}")
    check(
        "redirected to /team/ after onboarding",
        urlparse(r.headers.get("location", "")).path == "/team/",
        f"location={r.headers.get('location')}",
    )
    check(
        "welcome flash included",
        "Welcome" in (parse_qs(urlparse(r.headers.get("location", "")).query).get("flash") or [""])[0],
    )

    # Cookie jar should now contain a valid session
    sess_cookie = next(
        (c for c in client.cookies.jar if c.name in ("degen_session", "session")),
        None,
    )
    check("session cookie dropped", sess_cookie is not None)

    # ---------- Step 3: land on dashboard ----------
    banner("Step 3 — land on the team dashboard")
    r = client.get("/team/")
    check("dashboard 200s", r.status_code == 200, f"status={r.status_code}")
    page = r.text
    check("greets the employee by name", "Pete" in page)
    check(
        "time-aware greeting renders",
        any(g in page for g in ("Good morning", "Good afternoon", "Good evening")),
    )
    check("hero card present", "pt-hero" in page)
    check("Tools group visible", '<div class="pt-side-group">Tools</div>' in page)
    check("NO Admin group for employee", '<div class="pt-side-group">Admin</div>' not in page)
    check("mobile topbar rendered", 'id="pt-mobile-topbar"' in page)
    check("bottom nav rendered", 'class="pt-mobile-bottom-nav"' in page)
    check("hamburger button rendered", 'id="pt-hamburger"' in page)
    check("drawer close button rendered", 'id="pt-drawer-close"' in page)
    check("sidebar has #pt-sidebar id", 'id="pt-sidebar"' in page)
    check("quick-action tile: live stream", 'href="/tiktok/streamer"' in page)
    check("quick-action tile: degen eye",   'href="/degen_eye"' in page)
    check("sign-out form present",          'action="/team/logout"' in page)

    # ---------- Step 4: tap "Request supplies" ----------
    banner("Step 4 — tap 'Request supplies'")
    r = client.get("/team/supply")
    check("supply page 200s", r.status_code == 200, f"status={r.status_code}")
    page = r.text
    check("supply form present", 'action="/team/supply"' in page)
    check("urgency select present", 'name="urgency"' in page)
    check("empty state for 'my recent requests'", "No requests yet" in page)
    # PHONE-SPECIFIC: the page uses a 2-col grid. If the inline style is
    # still present, content will squeeze on a 375px viewport.
    check(
        "supply page should stack columns on mobile (class-based, not inline grid)",
        "grid-template-columns: 1fr 1.2fr" not in page,
        "inline grid doesn't collapse — move to a .pt-two-col class",
    )

    sup_tok = csrf(page)
    r = client.post(
        "/team/supply",
        data={
            "csrf_token": sup_tok,
            "title": "100 penny sleeves",
            "description": "Running low up front",
            "urgency": "normal",
        },
    )
    check("supply submit redirects", r.status_code == 303)

    r = client.get("/team/supply")
    check("submitted request shows on list", "100 penny sleeves" in r.text)

    # ---------- Step 5: check profile ----------
    banner("Step 5 — open profile, check what got saved")
    r = client.get("/team/profile")
    check("profile page 200s", r.status_code == 200)
    page = r.text
    check("preferred name pre-filled", 'value="Pete"' in page)
    check("email pre-filled", 'value="pete@example.com"' in page)
    check("address street pre-filled", "123 Route St" in page)
    check("emergency contact pre-filled", "Professor Oak" in page)
    check(
        "profile page stacks on mobile (class-based, not inline grid)",
        "grid-template-columns: 2fr 1fr" not in page,
        "inline grid doesn't collapse — move to a .pt-two-col class",
    )

    # ---------- Step 6: schedule + hours + policies ----------
    banner("Step 6 — tour the other tabs")
    for path, label in (
        ("/team/schedule", "schedule"),
        ("/team/hours", "hours"),
        ("/team/policies", "policies"),
    ):
        r = client.get(path)
        check(f"{label} 200s", r.status_code == 200, f"status={r.status_code}")
        check(f"{label}: topbar present", 'id="pt-mobile-topbar"' in r.text)
        check(f"{label}: bottom nav present", 'class="pt-mobile-bottom-nav"' in r.text)

    # ---------- Step 7: open tools ----------
    banner("Step 7 — try the Tools tiles (Stream + Eye)")
    for path in ("/tiktok/streamer", "/degen_eye"):
        r = client.get(path)
        check(f"{path} 200s for employee", r.status_code == 200, f"status={r.status_code}")

    # ---------- Step 8: login roundtrip after logout ----------
    banner("Step 8 — sign out and sign back in")
    r = client.get("/team/")
    form_tok = csrf(r.text)
    r = client.post("/team/logout", data={"csrf_token": form_tok})
    check("logout 303s", r.status_code == 303)

    r = client.get("/team/login")
    check("login page 200s", r.status_code == 200)
    check(
        "login has show/hide password toggle",
        ('data-toggle="password"' in r.text) or ('aria-label="Show password"' in r.text),
    )
    login_tok = csrf(r.text)
    r = client.post(
        "/team/login",
        data={
            "csrf_token": login_tok,
            "username": "pikachu.pete",
            "password": "Thunderbolt!42zzz",
        },
    )
    check("login redirects", r.status_code == 303, f"status={r.status_code}")
    check(
        "login lands on dashboard",
        urlparse(r.headers.get("location", "")).path in ("/team/", "/team"),
        f"location={r.headers.get('location')}",
    )

    banner("DONE — walkthrough finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
