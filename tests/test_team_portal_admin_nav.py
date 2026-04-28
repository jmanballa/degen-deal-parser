"""Sidebar admin section — visibility by role.

The "Admin" group in the portal sidebar (`base.html`) is populated by
`_nav_context` in `app/routers/team.py`, which filters admin links through
`has_permission()`. This test locks in:

- Admins see all privileged sidebar links.
- Employees see ZERO admin/tools links and no privileged dividers.
- Managers see schedule plus their permitted queue links.
- Reviewers see their permitted queue links.
"""
from __future__ import annotations

import os
import unittest
from html import unescape
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-adminnav")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-adminnav")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-adminnav")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class AdminSidebarVisibilityTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _login_as(self, role: str, user_id: int = 100, username: str = "u"):
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
        # Also persist so /team/ handler can load perms against this row.
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()
        return u

    def _dashboard_html(self) -> str:
        from app import permissions as perms
        from app.routers.team import _nav_context
        from app.shared import templates

        user = self._current_user
        request = SimpleNamespace(url=SimpleNamespace(path="/team/"))
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

    def test_team_dashboard_alias_redirects_to_home(self):
        from app.routers.team import router, team_dashboard_alias

        self.assertTrue(
            any(
                getattr(route, "path", None) == "/team/dashboard"
                and "GET" in getattr(route, "methods", set())
                for route in router.routes
            )
        )
        response = team_dashboard_alias()
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers.get("location"), "/team/")

    def test_admin_sees_all_privileged_sidebar_links(self):
        self._current_user = self._login_as("admin", user_id=101, username="adm")
        html = self._dashboard_html()
        self.assertIn("What do I need to do today?", html)
        self.assertIn('href="/team/admin/schedule"', html)
        self.assertIn('href="/team/admin/employees"', html)
        self.assertIn('href="/team/admin/invites"', html)
        self.assertIn('href="/team/admin/permissions"', html)
        self.assertIn('href="/team/admin/supply"', html)
        self.assertIn('href="/team/admin/timeoff"', html)
        self.assertIn('href="/team/tools/live-stream"', html)
        self.assertIn('href="/team/tools/degen-eye"', html)
        self.assertIn('href="/dashboard"', html)
        self.assertIn(">Admin<", html, "admin group divider should render")

    def test_employee_sees_no_admin_or_tools_links(self):
        self._current_user = self._login_as("employee", user_id=102, username="emp")
        html = self._dashboard_html()
        self.assertIn("What do I need to do today?", html)
        self.assertIn('href="/team/profile"', html)
        self.assertIn('href="/team/supply"', html)
        self.assertIn('href="/team/schedule"', html)
        self.assertNotIn('href="/team/admin/schedule"', html)
        self.assertNotIn('href="/team/admin/employees"', html)
        self.assertNotIn('href="/team/admin/invites"', html)
        self.assertNotIn('href="/team/admin/permissions"', html)
        self.assertNotIn('href="/team/admin/supply"', html)
        self.assertIn('href="/team/tools/inventory"', html)
        self.assertIn('href="/team/tools/live-stream"', html)
        self.assertIn('href="/team/tools/degen-eye"', html)
        self.assertNotIn('href="/dashboard"', html)
        self.assertNotIn('href="/admin"', html)
        self.assertNotIn("Back to Ops", html)
        # The "Admin" group header must not render for plain employees.
        self.assertNotIn(
            '<div class="pt-side-group">Admin</div>',
            html,
            "admin divider leaked into an employee's sidebar",
        )
        self.assertNotIn(
            '<div class="pt-side-group">Tools</div>',
            html,
            "legacy tools divider leaked into an employee's sidebar",
        )

    def test_manager_sees_queue_links(self):
        self._current_user = self._login_as("manager", user_id=103, username="mgr")
        html = self._dashboard_html()
        self.assertIn('href="/team/admin/schedule"', html)
        # page.admin.supply is manager=True in DEFAULT_ROLE_PERMISSIONS.
        self.assertIn('href="/team/admin/supply"', html)
        self.assertIn('href="/team/admin/timeoff"', html)
        self.assertIn('href="/team/tools/live-stream"', html)
        self.assertIn('href="/team/tools/degen-eye"', html)
        self.assertNotIn('href="/dashboard"', html)
        self.assertNotIn("Back to Ops", html)
        # These employee-management pages stay admin-only.
        self.assertNotIn('href="/team/admin/employees"', html)
        self.assertNotIn('href="/team/admin/invites"', html)
        self.assertNotIn('href="/team/admin/permissions"', html)

    def test_reviewer_sees_queue_links(self):
        self._current_user = self._login_as("reviewer", user_id=104, username="rev")
        html = self._dashboard_html()
        self.assertIn('href="/team/admin/supply"', html)
        self.assertIn('href="/team/admin/timeoff"', html)
        self.assertIn('href="/team/tools/live-stream"', html)
        self.assertIn('href="/team/tools/degen-eye"', html)
        self.assertNotIn('href="/team/admin/employees"', html)
        self.assertNotIn('href="/team/admin/invites"', html)
        self.assertNotIn('href="/team/admin/permissions"', html)

    def test_dashboard_placeholder_cards_are_hidden_until_wired(self):
        self._current_user = self._login_as("employee", user_id=107, username="copy")
        html = self._dashboard_html()
        self.assertNotIn("Not connected", html)
        self.assertNotIn("isn't hooked up", html)
        self.assertNotIn("lands soon", html)
        upper = unescape(html).upper()
        self.assertIn("HOURS THIS WEEK", upper)
        self.assertIn("ESTIMATED PAY", upper)
        self.assertIn("WHAT DO I NEED TO DO TODAY?", upper)
        self.assertNotIn("Coming with payroll integration", html)
        self.assertNotIn("Task assignments pending", html)

    def test_viewer_cannot_enter_permission_gated_admin_page(self):
        from app.routers.team_admin import _permission_gate

        user = self._login_as("viewer", user_id=105, username="viewer")
        request = SimpleNamespace(state=SimpleNamespace(current_user=user))
        denial, _ = _permission_gate(request, self.session, "admin.supply.view")
        self.assertIsNotNone(denial)
        self.assertEqual(denial.status_code, 403)


if __name__ == "__main__":
    unittest.main()
