"""Mobile nav regression tests for the employee portal.

The portal's mobile layout has three moving parts that all need to render
on every authenticated portal page:

  1. A sticky topbar with a hamburger (`#pt-hamburger`) that opens the
     sidebar drawer on phones.
  2. The sidebar itself must have the drawer id (`#pt-sidebar`) and a
     close button (`#pt-drawer-close`) so the JS can wire up tap-to-close.
  3. A bottom nav (`.pt-mobile-bottom-nav`) with a primary center FAB.

We also verify the drawer JS is loaded and that the bottom nav adapts by
role: employees get the employee schedule and no tools, while privileged
portal roles get Stream + Eye and the editable admin schedule.
"""
from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-mobilenav")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-mobilenav")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-mobilenav")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class MobileNavTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _login_as(self, role: str, user_id: int = 500, username: str = "u"):
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
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()
        return u

    def _dashboard_html(self, path: str = "/team/") -> str:
        from app import permissions as perms
        from app.routers.team import _nav_context
        from app.shared import templates

        user = self._current_user
        request = SimpleNamespace(url=SimpleNamespace(path=path))
        context = {
            "request": request,
            "title": "Dashboard",
            "active": "dashboard",
            "current_user": user,
            "widgets": perms.allowed_widgets_for(self.session, user),
            "clockify_ready": False,
            "supply_queue_count": 0,
            "now_hour": 12,
            "csrf_token": "test-token",
            **_nav_context(self.session, user),
        }
        return templates.env.get_template("team/dashboard.html").render(context)

    def test_portal_dashboard_renders_mobile_topbar_and_hamburger(self):
        self._current_user = self._login_as("employee", user_id=501, username="emp1")
        html = self._dashboard_html()
        self.assertIn('id="pt-mobile-topbar"', html)
        self.assertIn('id="pt-hamburger"', html)
        self.assertIn('aria-controls="pt-sidebar"', html)

    def test_portal_sidebar_has_drawer_hooks(self):
        self._current_user = self._login_as("employee", user_id=502, username="emp2")
        html = self._dashboard_html()
        self.assertIn('id="pt-sidebar"', html)
        self.assertIn('id="pt-drawer-close"', html)
        self.assertIn('id="pt-drawer-backdrop"', html)
        self.assertIn("/static/portal-drawer.js", html)

    def test_bottom_nav_hides_tools_for_employee(self):
        self._current_user = self._login_as("employee", user_id=503, username="emp3")
        html = self._dashboard_html()
        self.assertIn('class="pt-mobile-bottom-nav"', html)
        # Five expected bottom-nav destinations for a plain employee:
        for needle in (
            'href="/team/"',
            'href="/team/policies"',
            'href="/team/supply"',
            'href="/team/schedule"',
            'href="/team/profile"',
        ):
            self.assertIn(needle, html, f"missing bottom-nav link: {needle}")
        self.assertNotIn('href="/team/admin/schedule"', html)
        self.assertNotIn('href="/tiktok/streamer"', html)
        self.assertNotIn('href="/degen_eye"', html)
        # Center FAB still exists, but it points to employee supply requests.
        self.assertIn('class="pt-mbn-fab"', html)
        self.assertIn('pt-mbn-item-center', html)

    def test_bottom_nav_shows_tools_and_admin_schedule_for_manager(self):
        self._current_user = self._login_as("manager", user_id=506, username="mgr1")
        html = self._dashboard_html()
        for needle in (
            'href="/team/"',
            'href="/tiktok/streamer"',
            'href="/degen_eye"',
            'href="/team/admin/schedule"',
            'href="/team/profile"',
        ):
            self.assertIn(needle, html, f"missing bottom-nav link: {needle}")
        self.assertIn('class="pt-mbn-fab"', html)
        self.assertIn('pt-mbn-item-center', html)

    def test_bottom_nav_renders_on_non_home_pages_too(self):
        self._current_user = self._login_as("employee", user_id=504, username="emp5")
        for path in ("/team/schedule", "/team/policies", "/team/profile"):
            html = self._dashboard_html(path=path)
            self.assertIn('class="pt-mobile-bottom-nav"', html,
                          f"bottom nav missing on {path}")
            self.assertIn('id="pt-hamburger"', html,
                          f"hamburger missing on {path}")

    def test_active_state_marks_current_bottom_nav_item(self):
        self._current_user = self._login_as("employee", user_id=505, username="emp4")
        html = self._dashboard_html(path="/team/schedule")
        # Look for the schedule anchor carrying the active class.
        self.assertRegex(
            html,
            r'href="/team/schedule"[^>]*class="pt-mbn-item[^"]* is-active"',
        )


if __name__ == "__main__":
    unittest.main()
