"""Login-alias + employee-redirect regression tests.

Two behaviors under test:

1. `/login` and `/team/login` are the same thing — a client that hits
   `/login` is seamlessly forwarded to the canonical `/team/login` page
   (with `?next=` preserved).

2. Employees logging in land on `/team/` (the employee portal home),
   NOT `/dashboard` (which they don't have permission to view and would
   just 403 at). This was the live bug — employees bounced straight
   into a permission wall after sign-in.
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
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-login")
os.environ.setdefault("SESSION_SECRET", "test-secret-login-" + "x" * 32)


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class AppHomeForRoleTests(unittest.TestCase):
    """Pure unit test — the helper must route employees to /team/."""

    def test_employee_goes_to_team_portal(self):
        from app.shared import app_home_for_role
        self.assertEqual(app_home_for_role("employee"), "/team/")

    def test_admin_still_goes_to_dashboard(self):
        from app.shared import app_home_for_role
        self.assertEqual(app_home_for_role("admin"), "/dashboard")

    def test_reviewer_still_goes_to_review(self):
        from app.shared import app_home_for_role
        self.assertEqual(app_home_for_role("reviewer"), "/review")


class LoginAliasTests(unittest.TestCase):
    """HTTP-level: /login is a thin alias for /team/login."""

    def setUp(self):
        from app import rate_limit
        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true"
        from app import config as cfg
        cfg.get_settings.cache_clear()

        import app.main as app_main
        importlib.reload(app_main)
        self.app_main = app_main

        from app.db import get_session as real_get_session

        def _override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _override

        from fastapi.testclient import TestClient
        self.client = TestClient(self.app_main.app)

        # Stub get_request_user so /login doesn't try to resolve a real
        # user out of the conftest DB (which isn't ours).
        from app import shared
        self._patcher_shared = patch.object(shared, "get_request_user", return_value=None)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=None)
        self._patcher_main.start()

    def tearDown(self):
        self._patcher_shared.stop()
        self._patcher_main.stop()
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        try:
            self.client.close()
        except Exception:
            pass

    def test_login_get_redirects_to_team_login(self):
        r = self.client.get("/login", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/team/login")

    def test_login_get_preserves_safe_next(self):
        r = self.client.get("/login?next=/reports", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertIn("/team/login?next=", r.headers["location"])
        self.assertIn("%2Freports", r.headers["location"])

    def test_login_get_strips_unsafe_next(self):
        """An open-redirect attempt via `next=//evil.com` must be dropped,
        not forwarded."""
        r = self.client.get("/login?next=//evil.com/x", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/team/login")

    def test_login_post_redirects_to_team_login(self):
        # Stray form posts (old bookmarks, etc.) shouldn't 404 or crash.
        r = self.client.post(
            "/login",
            data={"username": "x", "password": "y"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/team/login")

    def test_logout_lands_on_team_login(self):
        r = self.client.post("/logout", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertIn("/team/login", r.headers["location"])


class EmployeeLoginRedirectTests(unittest.TestCase):
    """The live bug: a real employee logging in at /team/login landed at
    /dashboard and 403'd. After fix, they land at /team/."""

    def setUp(self):
        from app import rate_limit
        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true"
        from app import config as cfg
        cfg.get_settings.cache_clear()

        import app.main as app_main
        importlib.reload(app_main)
        self.app_main = app_main

        from app.db import get_session as real_get_session

        def _override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _override

        from app.auth import hash_password
        from app.models import User

        ph, salt = hash_password("EmployeePass9!")
        self.employee = User(
            username="worker",
            password_hash=ph,
            password_salt=salt,
            display_name="Worker",
            role="employee",
            is_active=True,
        )
        self.session.add(self.employee)
        self.session.commit()
        self.session.refresh(self.employee)

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

    def test_employee_login_lands_on_team_home(self):
        csrf = self._csrf()
        r = self.client.post(
            "/team/login",
            data={
                "username": "worker",
                "password": "EmployeePass9!",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303, r.text[:300])
        self.assertEqual(
            r.headers["location"],
            "/team/",
            "employees must land on /team/, not /dashboard (which they can't access)",
        )

    def test_employee_login_honors_safe_next(self):
        """Deep-links into the portal should survive the sign-in bounce."""
        csrf = self._csrf()
        r = self.client.post(
            "/team/login",
            data={
                "username": "worker",
                "password": "EmployeePass9!",
                "csrf_token": csrf,
                "next": "/team/profile",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/team/profile")

    def test_employee_login_drops_unsafe_next(self):
        csrf = self._csrf()
        r = self.client.post(
            "/team/login",
            data={
                "username": "worker",
                "password": "EmployeePass9!",
                "csrf_token": csrf,
                "next": "//evil.com/x",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertEqual(
            r.headers["location"],
            "/team/",
            "open-redirect via `next` must be neutralized — fall back to role home",
        )


if __name__ == "__main__":
    unittest.main()
