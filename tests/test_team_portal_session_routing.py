"""End-to-end regression: real session cookie grants access to /team/.

Bug reproduced: admin clicks "Team" in the ops sidebar, gets redirected to
/team/login, submits creds, gets bounced back to /dashboard, clicks Team
again → infinite loop.

Root cause: attach_current_user middleware was registered as the OUTERMOST
middleware (above SessionMiddleware), so `request.scope["session"]` had not
been decoded yet when it tried to resolve the logged-in user from the
cookie. The ops side limped along via a fallback in `require_role_response`
that re-reads the session from inside the route handler (where scope is
fully populated), but the team portal's `_require_employee` gate reads
`request.state.current_user` directly, with no fallback, so authenticated
admins were bounced to /team/login.

This test drives a real POST to /team/login (so the TestClient receives a
real encrypted session cookie from Starlette's SessionMiddleware), then
GETs /team/ and asserts we don't get a redirect to /team/login.
"""
from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-key")


def _fresh_engine():
    from app.models import SQLModel

    # Shared in-memory DB across threads: StaticPool + check_same_thread=False.
    # The FastAPI test client hops threads when dispatching middleware +
    # dependency overrides, so the default per-thread sqlite connection
    # breaks mid-request.
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class TeamPortalSessionRoutingTests(unittest.TestCase):
    """Drive a real cookie-based flow so attach_current_user runs for real."""

    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults

        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        # Reload main so the middleware stack is freshly wired.
        os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true"
        from app import config as cfg
        cfg.get_settings.cache_clear()

        import app.main as app_main
        importlib.reload(app_main)
        self.app_main = app_main

        # Point every `get_session` Depends() at our in-memory DB.
        from app.db import get_session as real_get_session

        def _session_override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _session_override

        # Seed an admin user with a known password.
        from app.auth import hash_password
        from app.models import User

        ph, salt = hash_password("TestAdmin!234")
        self.admin = User(
            username="testadmin",
            password_hash=ph,
            password_salt=salt,
            display_name="Test Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()
        self.session.refresh(self.admin)

        from fastapi.testclient import TestClient
        self.client = TestClient(self.app_main.app)

    def tearDown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        try:
            self.client.close()
        except Exception:
            pass

    def _csrf(self) -> str:
        r = self.client.get("/team/login")
        marker = 'name="csrf_token" value="'
        idx = r.text.index(marker) + len(marker)
        end = r.text.index('"', idx)
        return r.text[idx:end]

    def test_admin_with_valid_session_does_not_bounce_to_team_login(self):
        """Regression: middleware order bug made this redirect loop."""
        # Patch get_request_user to use OUR in-memory DB. The real function
        # opens a fresh managed_session which points at the (conftest-set)
        # sqlite file — but we want it to honor our in-memory User so the
        # test is self-contained.
        from app import shared
        import app.main as app_main
        from app.models import User

        def _fake_get_request_user(request):
            sess = request.scope.get("session") or {}
            user_id = sess.get("user_id")
            if not user_id:
                return None
            with Session(self.engine) as s:
                u = s.get(User, user_id)
                if u and u.is_active:
                    # Detach from the short-lived session so the caller
                    # can access attributes after this context closes.
                    s.expunge(u)
                    return u
            return None

        with patch.object(shared, "get_request_user", side_effect=_fake_get_request_user), \
             patch.object(app_main, "get_request_user", side_effect=_fake_get_request_user):

            csrf = self._csrf()
            r = self.client.post(
                "/team/login",
                data={
                    "username": "testadmin",
                    "password": "TestAdmin!234",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            self.assertEqual(r.status_code, 303, f"login failed: {r.text[:300]}")
            # Admin via /team/login is routed to /dashboard, that's fine.
            self.assertEqual(r.headers["location"], "/dashboard")

            # THE CRITICAL STEP — click "Team" in the sidebar.
            r2 = self.client.get("/team/", follow_redirects=False)

            # If attach_current_user runs BEFORE SessionMiddleware, the
            # session cookie hasn't been decoded yet, request.state.current_user
            # ends up None, and _require_employee bounces us to /team/login.
            if r2.status_code in (301, 302, 303, 307, 308):
                loc = r2.headers.get("location", "")
                self.assertNotIn(
                    "/team/login",
                    loc,
                    "REGRESSION: authenticated admin bounced to /team/login. "
                    "Likely cause: attach_current_user is registered OUTSIDE of "
                    "SessionMiddleware, so scope['session'] is empty when it runs.",
                )
            else:
                self.assertEqual(
                    r2.status_code, 200,
                    f"expected 200 on /team/, got {r2.status_code}: {r2.text[:300]}",
                )


if __name__ == "__main__":
    unittest.main()
