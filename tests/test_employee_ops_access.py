"""Rank-and-file employee access to selected ops pages.

Employees should be able to:
  - Use Degen Eye (`/degen_eye`) and the camera scanner (`/inventory/scan*`).
  - Open the TikTok live-stream dashboard (`/tiktok/streamer`) so they can
    chase GMV goals during a live. TikTok numbers are explicitly visible.

Employees must NOT be able to:
  - Hit the inventory list (`/inventory`) or item detail (shows cost basis).
  - Hit the ops dashboard, reports, bookkeeping, or admin surfaces.

The portal sidebar should expose a "Tools" group with Live Stream + Degen Eye
for every authenticated user (rank employees included).

The TikTok streamer template's hamburger nav should hide ops / admin links
for anyone below role=viewer so employees aren't tempted into 403s.
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
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-opsaccess")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-opsaccess")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class EmployeeOpsAccessTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        from app import config as cfg
        cfg.get_settings.cache_clear()
        import app.main as app_main
        importlib.reload(app_main)
        self.app_main = app_main

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

    def tearDown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in ("_patcher_shared", "_patcher_main"):
            p = getattr(self, attr, None)
            if p:
                p.stop()
                setattr(self, attr, None)

    def _login_as(self, role: str, user_id: int = 200, username: str = "u"):
        from app import shared
        import app.main as app_main
        from app.models import User

        # Persist a real User row so anything that hits the DB (e.g. perms
        # lookups on /team/) works. We then expunge it from the session so
        # attribute access (`.role`) never triggers a lazy refresh against a
        # session that might be in an inconsistent state — lazy refreshes
        # were the root cause of a flaky "role reads back as default
        # 'viewer'" bug when asserting against the streamer template.
        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()
            self.session.refresh(u)
            self.session.expunge(u)

        self._patcher_shared = patch.object(shared, "get_request_user", return_value=u)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=u)
        self._patcher_main.start()
        return u

    # ---------- Sidebar "Tools" group ----------

    def test_employee_sees_tools_group_in_portal_sidebar(self):
        self._login_as("employee", user_id=201, username="emp1")
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        html = r.text
        self.assertIn('<div class="pt-side-group">Tools</div>', html)
        self.assertIn('href="/tiktok/streamer"', html)
        self.assertIn('href="/degen_eye"', html)

    def test_admin_also_sees_tools_group(self):
        self._login_as("admin", user_id=202, username="adm1")
        html = self.client.get("/team/", follow_redirects=False).text
        self.assertIn('<div class="pt-side-group">Tools</div>', html)
        self.assertIn('href="/tiktok/streamer"', html)
        self.assertIn('href="/degen_eye"', html)

    # ---------- Degen Eye + scanner access ----------

    def test_employee_can_open_degen_eye(self):
        self._login_as("employee", user_id=203, username="emp2")
        r = self.client.get("/degen_eye", follow_redirects=False)
        self.assertEqual(r.status_code, 200, f"degen_eye denied: {r.status_code}")
        self.assertIn("Degen Eye", r.text)

    def test_employee_can_open_scanner_singles(self):
        self._login_as("employee", user_id=204, username="emp3")
        r = self.client.get("/inventory/scan/singles", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    def test_employee_can_open_scanner_slabs(self):
        self._login_as("employee", user_id=205, username="emp4")
        r = self.client.get("/inventory/scan/slabs", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    def test_employee_can_open_scan_root(self):
        self._login_as("employee", user_id=206, username="emp5")
        r = self.client.get("/inventory/scan", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    # ---------- Pages that should STAY gated above employee ----------

    def test_employee_blocked_from_inventory_list(self):
        self._login_as("employee", user_id=207, username="emp6")
        r = self.client.get("/inventory", follow_redirects=False)
        # 403 (forbidden HTML) is the explicit deny path; 303 (redirect to
        # login) would mean the login guard kicked in first. Either of those
        # is fine as long as it is NOT a 200 (i.e. employees never see the
        # cost-basis list).
        self.assertNotEqual(r.status_code, 200,
                            "employee must not see the inventory list (cost basis visible)")

    # ---------- TikTok streamer access ----------

    def test_employee_can_open_tiktok_streamer_dashboard(self):
        self._login_as("employee", user_id=208, username="emp7")
        r = self.client.get("/tiktok/streamer", follow_redirects=False)
        self.assertEqual(r.status_code, 200, f"streamer denied: {r.status_code}")

    def test_streamer_dashboard_hides_ops_links_for_employees(self):
        self._login_as("employee", user_id=209, username="emp8")
        html = self.client.get("/tiktok/streamer", follow_redirects=False).text
        # Employee-safe tiles: the Team Portal + Degen Eye must be there.
        self.assertIn('href="/team/">Team Portal</a>', html)
        self.assertIn('href="/degen_eye">Degen Eye</a>', html)
        # Ops-only subgroup labels only render inside {% if _is_ops %}. Their
        # absence is the clean signal that the whole ops block was skipped.
        self.assertNotIn(
            '<div class="nav-dropdown-label">Operators</div>',
            html,
            "ops subgroup leaked into employee streamer view",
        )
        self.assertNotIn(
            '<div class="nav-dropdown-label">TikTok</div>',
            html,
            "internal TikTok subgroup leaked into employee streamer view",
        )
        # Specific dashboard / admin / bookkeeping anchors must also be gone.
        self.assertNotIn('<a href="/dashboard">', html)
        self.assertNotIn('<a href="/admin">', html)
        self.assertNotIn('<a href="/bookkeeping">', html)

    def test_streamer_dashboard_shows_ops_links_for_admin(self):
        self._login_as("admin", user_id=210, username="adm2")
        html = self.client.get("/tiktok/streamer", follow_redirects=False).text
        self.assertIn('<a href="/dashboard">', html)
        self.assertIn('<a href="/admin">', html)
        self.assertIn('<a href="/bookkeeping">', html)
        self.assertIn(
            '<div class="nav-dropdown-label">Operators</div>', html,
        )

    # ---------- Unauthenticated requests still redirect ----------

    def test_anonymous_redirected_from_degen_eye(self):
        # No _login_as(); stub get_request_user to return None so middleware
        # doesn't try to hit the (real) configured DB.
        from app import shared
        import app.main as app_main
        self._patcher_shared = patch.object(shared, "get_request_user", return_value=None)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=None)
        self._patcher_main.start()
        r = self.client.get("/degen_eye", follow_redirects=False)
        self.assertIn(r.status_code, (302, 303, 307))


if __name__ == "__main__":
    unittest.main()
