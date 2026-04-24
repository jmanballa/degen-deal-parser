"""Homepage data and copy regression tests for the employee portal.

These render the dashboard template directly instead of using TestClient,
matching the Wave A portal tests and avoiding sandbox hangs on app routes.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, timedelta
from html import unescape
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-homepage")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-homepage")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class EmployeePortalHomepageTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        from app.db import seed_employee_portal_defaults

        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _seed_user(
        self,
        user_id: int,
        *,
        username: str | None = None,
        display_name: str | None = None,
        role: str = "employee",
    ):
        from app.models import User

        user = User(
            id=user_id,
            username=username or f"user{user_id}",
            password_hash="x",
            password_salt="x",
            display_name=display_name or username or f"User {user_id}",
            role=role,
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def _login_as(self, role: str, user_id: int = 100, username: str = "u"):
        from app.models import User

        existing = self.session.get(User, user_id)
        if existing is not None:
            self._current_user = existing
            return existing
        self._current_user = self._seed_user(
            user_id,
            username=username,
            display_name=username,
            role=role,
        )
        return self._current_user

    def _seed_shift(
        self,
        user_id: int,
        shift_date: date,
        label: str,
        *,
        sort_order: int = 0,
    ):
        from app.models import ShiftEntry, classify_shift_label

        row = ShiftEntry(
            user_id=user_id,
            shift_date=shift_date,
            label=label,
            kind=classify_shift_label(label),
            sort_order=sort_order,
            created_by_user_id=user_id,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _seed_day_note(self, shift_date: date, location_label: str):
        from app.models import ScheduleDayNote

        row = ScheduleDayNote(day_date=shift_date, location_label=location_label)
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _dashboard_html(self) -> str:
        from app import permissions as perms
        from app.routers.team import (
            _nav_context,
            _today_staffing_for,
            _upcoming_shifts_for,
        )
        from app.shared import templates

        today = date.today()
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
            "upcoming_shifts": _upcoming_shifts_for(
                self.session,
                user,
                today=today,
            ),
            "today_staffing": _today_staffing_for(self.session, today=today),
            "today_date": today,
            "now_hour": 12,
            "csrf_token": "test-token",
            **_nav_context(self.session, user),
        }
        return templates.env.get_template("team/dashboard.html").render(context)

    def test_upcoming_shifts_shows_users_next_three(self):
        user = self._login_as("employee", user_id=101, username="emp")
        today = date.today()
        labels = [
            "9:00 AM - 1:00 PM",
            "10:00 AM - 2:00 PM",
            "11:00 AM - 3:00 PM",
            "12:00 PM - 4:00 PM",
            "1:00 PM - 5:00 PM",
        ]
        for offset, label in enumerate(labels, start=1):
            self._seed_shift(user.id, today + timedelta(days=offset), label)
        self._seed_day_note(today + timedelta(days=2), "Storefront")

        html = self._dashboard_html()

        self.assertEqual(html.count('class="pt-upcoming-shift"'), 3)
        for label in labels[:3]:
            self.assertIn(label, html)
        for label in labels[3:]:
            self.assertNotIn(label, html)
        self.assertLess(html.index(labels[0]), html.index(labels[1]))
        self.assertLess(html.index(labels[1]), html.index(labels[2]))
        self.assertIn("Storefront", html)

    def test_upcoming_shifts_excludes_other_users_shifts(self):
        user_a = self._login_as("employee", user_id=201, username="alice")
        user_b = self._seed_user(202, username="bob", display_name="Bob")
        today = date.today()
        for offset in range(1, 4):
            self._seed_shift(
                user_a.id,
                today + timedelta(days=offset),
                f"A shift {offset}",
            )
            self._seed_shift(
                user_b.id,
                today + timedelta(days=offset),
                f"B shift {offset}",
            )

        html = self._dashboard_html()

        for offset in range(1, 4):
            self.assertIn(f"A shift {offset}", html)
            self.assertNotIn(f"B shift {offset}", html)

    def test_upcoming_shifts_excludes_past_shifts(self):
        user = self._login_as("employee", user_id=301, username="past")
        today = date.today()
        self._seed_shift(user.id, today - timedelta(days=2), "Past shift one")
        self._seed_shift(user.id, today - timedelta(days=1), "Past shift two")
        self._seed_shift(user.id, today, "Today shift")
        self._seed_shift(user.id, today + timedelta(days=1), "Future shift")

        html = self._dashboard_html()

        self.assertNotIn("Past shift one", html)
        self.assertNotIn("Past shift two", html)
        self.assertIn("Today shift", html)
        self.assertIn("Future shift", html)

    def test_upcoming_shifts_empty_state(self):
        self._login_as("employee", user_id=401, username="empty")

        html = self._dashboard_html()

        self.assertIn("No upcoming shifts", html)
        self.assertNotIn("coming soon", html.lower())
        self.assertNotIn("lands soon", html.lower())
        self.assertNotIn("TBA", html)

    def test_today_staffing_renders_for_admin(self):
        admin = self._login_as("admin", user_id=501, username="admin")
        worker_a = self._seed_user(502, username="amy", display_name="Amy")
        worker_b = self._seed_user(503, username="ben", display_name="Ben")
        today = date.today()
        self._seed_shift(worker_b.id, today, "12:00 PM - 4:00 PM")
        self._seed_shift(worker_a.id, today, "9:00 AM - 1:00 PM")

        html = self._dashboard_html()

        self.assertEqual(admin.role, "admin")
        self.assertIn("Who's on today", html)
        self.assertIn("Amy", html)
        self.assertIn("Ben", html)
        self.assertLess(html.index("Amy"), html.index("Ben"))

    def test_today_staffing_hidden_for_employee(self):
        employee = self._login_as("employee", user_id=601, username="employee")
        coworker = self._seed_user(602, username="coworker", display_name="Coworker")
        today = date.today()
        self._seed_shift(employee.id, today, "9:00 AM - 1:00 PM")
        self._seed_shift(coworker.id, today, "10:00 AM - 2:00 PM")

        html = self._dashboard_html()

        self.assertNotIn("Who's on today", html)

    def test_today_staffing_empty_message(self):
        self._login_as("manager", user_id=701, username="manager")

        html = self._dashboard_html()

        self.assertIn("Who's on today", html)
        self.assertIn("Nobody scheduled today", html)

    def test_placeholder_cards_still_present(self):
        self._login_as("employee", user_id=801, username="cards")

        html = self._dashboard_html()
        upper = unescape(html).upper()

        self.assertIn("HOURS THIS WEEK", upper)
        self.assertIn("ESTIMATED PAY", upper)
        self.assertIn("TODAY'S TASKS", upper)

    def test_placeholder_copy_hygiene(self):
        self._login_as("employee", user_id=901, username="copy")

        html = self._dashboard_html()

        for forbidden in (
            "Not connected",
            "isn't hooked up",
            "lands soon",
            "All clear",
            "No tasks assigned right now",
        ):
            self.assertNotIn(forbidden, html)


if __name__ == "__main__":
    unittest.main()
