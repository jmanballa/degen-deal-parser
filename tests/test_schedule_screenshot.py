"""Phone-screenshot-friendly schedule view (/team/admin/schedule/screenshot).

Renders a narrow read-only week view Jeffrey can grab a clean phone
screenshot of. These tests verify the route is gated by admin
permission, includes the seeded shift labels, and that the editor page
exposes a discoverable button to it.
"""
from __future__ import annotations

import importlib
import os
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "ss-shot-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "ss-shot-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "ss-shot-secret-" + "x" * 32)


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _load_app_with_portal(enabled: bool):
    os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true" if enabled else "false"
    from app import config as cfg

    cfg.get_settings.cache_clear()
    import app.main as app_main

    importlib.reload(app_main)
    return app_main


def _monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


class ScheduleScreenshotViewTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit

        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults

        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        self.app_main = _load_app_with_portal(True)
        from app.db import get_session as real_get_session

        def _session_override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _session_override
        from fastapi.testclient import TestClient

        self.client = TestClient(self.app_main.app)

        self.week_start = _monday(date(2026, 5, 4))  # Monday May 4 2026

    def tearDown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in ("_user_patcher", "_user_patcher_main"):
            patcher = getattr(self, attr, None)
            if patcher:
                patcher.stop()
                setattr(self, attr, None)

    def _login(self, role: str, user_id: int = 100, username: str = "ss_admin"):
        from app import shared
        from app.models import User

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        import app.main as app_main

        self._user_patcher = patch.object(shared, "get_request_user", return_value=u)
        self._user_patcher.start()
        self._user_patcher_main = patch.object(
            app_main, "get_request_user", return_value=u
        )
        self._user_patcher_main.start()
        return u

    def _seed_shift(self, *, user_id: int, display_name: str, day_offset: int,
                    label: str, kind: str = "work"):
        from app.auth import hash_password
        from app.models import ShiftEntry, User

        ph, salt = hash_password("xx")
        existing = self.session.get(User, user_id)
        if existing is None:
            self.session.add(
                User(
                    id=user_id,
                    username=f"u{user_id}",
                    password_hash=ph,
                    password_salt=salt,
                    display_name=display_name,
                    role="employee",
                    is_active=True,
                    is_schedulable=True,
                )
            )
        self.session.add(
            ShiftEntry(
                user_id=user_id,
                shift_date=self.week_start + timedelta(days=day_offset),
                label=label,
                kind=kind,
                created_by_user_id=1,
            )
        )
        self.session.commit()

    def test_screenshot_view_requires_admin_perm(self):
        # Plain employee (no admin.schedule.view) should be denied.
        self._login("employee", user_id=200, username="emp_noperm")
        r = self.client.get(
            "/team/admin/schedule/screenshot?week=2026-05-04",
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303, 401, 403, 404))
        self.assertNotIn("Storefront", r.text or "")

    def test_screenshot_view_renders_seeded_shifts(self):
        self._login("admin", user_id=201, username="ss_admin_view")
        self._seed_shift(
            user_id=301,
            display_name="Alice Storefront",
            day_offset=0,
            label="10:00 AM - 6:00 PM · counter",
            kind="work",
        )
        self._seed_shift(
            user_id=302,
            display_name="Bob Backroom",
            day_offset=2,
            label="OFF",
            kind="off",
        )

        r = self.client.get("/team/admin/schedule/screenshot?week=2026-05-04")
        self.assertEqual(r.status_code, 200)
        body = r.text

        # Standalone — no portal nav, but week heading and section labels.
        self.assertIn("Week of May 4, 2026", body)
        self.assertIn("Storefront", body)
        self.assertIn("Stream", body)

        # Seeded shifts surface with names + labels.
        self.assertIn("Alice Storefront", body)
        self.assertIn("10:00 AM - 6:00 PM", body)
        self.assertIn("counter", body)
        self.assertIn("Bob Backroom", body)

        # Mobile viewport meta is present so iOS doesn't shrink the page.
        self.assertIn("width=device-width", body)

        # Week navigation links use the screenshot route, not the editor.
        self.assertIn("/team/admin/schedule/screenshot?week=", body)

    def test_screenshot_view_is_week_aware(self):
        self._login("admin", user_id=202, username="ss_admin_week")
        # Shift on the requested week.
        self._seed_shift(
            user_id=311,
            display_name="Cara CurrentWeek",
            day_offset=1,
            label="9 AM - 1 PM",
            kind="work",
        )
        # Shift in a far-future week (should NOT appear in May 4 view).
        from app.models import ShiftEntry, User
        from app.auth import hash_password

        ph, salt = hash_password("xx")
        self.session.add(
            User(
                id=312,
                username="u312",
                password_hash=ph,
                password_salt=salt,
                display_name="Dora Future",
                role="employee",
                is_active=True,
                is_schedulable=True,
            )
        )
        self.session.add(
            ShiftEntry(
                user_id=312,
                shift_date=self.week_start + timedelta(days=21),  # 3 weeks later
                label="3 PM - 9 PM",
                kind="work",
                created_by_user_id=1,
            )
        )
        self.session.commit()

        r = self.client.get("/team/admin/schedule/screenshot?week=2026-05-04")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Cara CurrentWeek", r.text)
        self.assertNotIn("Dora Future", r.text)

    def test_editor_page_links_to_screenshot_view(self):
        self._login("admin", user_id=203, username="ss_admin_btn")
        r = self.client.get("/team/admin/schedule")
        self.assertEqual(r.status_code, 200)
        self.assertIn("/team/admin/schedule/screenshot", r.text)
        self.assertIn("Screenshot view", r.text)


if __name__ == "__main__":
    unittest.main()
