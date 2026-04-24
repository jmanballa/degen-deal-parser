"""Wave C announcement MVP regression tests.

These follow the portal's direct-render/router-call pattern instead of using
TestClient, which avoids route hangs in this sandbox while still exercising the
handlers, permission checks, model table creation, and templates.
"""
from __future__ import annotations

import asyncio
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-announcements")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-announcements")
os.environ.setdefault("SESSION_SECRET", "unit-test-session-announcements")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-announcements")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class _FakeRequest:
    def __init__(self, current_user, *, path: str = "/team/"):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(
            path=path,
            scheme="http",
            netloc="testserver",
        )


class TeamAnnouncementTests(unittest.TestCase):
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
        role: str = "employee",
        username: str | None = None,
        display_name: str | None = None,
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

    def _seed_announcement(
        self,
        title: str,
        *,
        body: str = "Details",
        creator=None,
        is_active: bool = True,
        pinned: bool = False,
        published_at=None,
        expires_at=None,
    ):
        from app.models import TeamAnnouncement, utcnow

        creator = creator or self._seed_user(900, role="admin", username="creator")
        now = utcnow()
        row = TeamAnnouncement(
            title=title,
            body=body,
            created_by_user_id=creator.id,
            is_active=is_active,
            pinned=pinned,
            published_at=published_at or now,
            expires_at=expires_at,
            created_at=now,
            updated_at=now,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _create(
        self,
        user,
        *,
        title: object,
        body: object,
        pinned=None,
        expires_at="",
        tz_offset_minutes: str | None = None,
    ):
        from app.routers import team_admin_announcements as admin_announcements

        request = _FakeRequest(user, path="/team/admin/announcements")
        return asyncio.run(
            admin_announcements.admin_announcements_create(
                request,
                title=title,
                body=body,
                pinned=pinned,
                expires_at=expires_at,
                tz_offset_minutes=tz_offset_minutes,
                session=self.session,
            )
        )

    def _archive(self, user, announcement_id: int):
        from app.routers import team_admin_announcements as admin_announcements

        request = _FakeRequest(
            user,
            path=f"/team/admin/announcements/{announcement_id}/archive",
        )
        return asyncio.run(
            admin_announcements.admin_announcements_archive(
                request,
                announcement_id,
                self.session,
            )
        )

    def _dashboard_html(self, user) -> str:
        from app import permissions as perms
        from app.routers.team import (
            _active_announcements_for,
            _nav_context,
            _today_staffing_for,
            _upcoming_shifts_for,
        )
        from app.shared import templates

        today = date.today()
        request = _FakeRequest(user, path="/team/")
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
            "active_announcements": _active_announcements_for(
                self.session,
                limit=3,
            ),
            "today_date": today,
            "now_hour": 12,
            "csrf_token": "test-token",
            **_nav_context(self.session, user),
        }
        return templates.env.get_template("team/dashboard.html").render(context)

    def _announcements_html(self, user) -> str:
        from app.routers.team import _active_announcements_for, _nav_context
        from app.models import User
        from app.shared import templates

        announcements = _active_announcements_for(self.session)
        author_ids = {row.created_by_user_id for row in announcements}
        authors = {
            row.id: row
            for row in self.session.exec(
                select(User).where(User.id.in_(author_ids))
            ).all()
            if row.id is not None
        }
        request = _FakeRequest(user, path="/team/announcements")
        context = {
            "request": request,
            "title": "Announcements",
            "active": "announcements",
            "current_user": user,
            "announcements": announcements,
            "authors": authors,
            "csrf_token": "test-token",
            **_nav_context(self.session, user),
        }
        return templates.env.get_template("team/announcements.html").render(context)

    def _admin_announcements_html(self, user) -> str:
        from app.shared import templates

        request = _FakeRequest(user, path="/team/admin/announcements")
        context = {
            "request": request,
            "title": "Announcements",
            "active": "announcements",
            "current_user": user,
            "announcements": [],
            "authors": {},
            "statuses": {},
            "can_view_admin_announcements": True,
            "can_create": True,
            "csrf_token": "test-token",
            "flash": None,
        }
        return templates.env.get_template(
            "team/admin/announcements.html"
        ).render(context)

    def test_admin_can_create_announcement(self):
        from app.models import AuditLog, TeamAnnouncement

        admin = self._seed_user(1, role="admin", username="admin")
        response = self._create(
            admin,
            title="Schedule update",
            body="Please check the new weekend coverage.",
            pinned="1",
        )

        self.assertEqual(response.status_code, 303)
        row = self.session.exec(select(TeamAnnouncement)).one()
        self.assertEqual(row.title, "Schedule update")
        self.assertEqual(row.body, "Please check the new weekend coverage.")
        self.assertTrue(row.pinned)
        audit = self.session.exec(
            select(AuditLog).where(AuditLog.action == "announcement.created")
        ).one()
        self.assertEqual(audit.actor_user_id, admin.id)
        self.assertEqual(audit.resource_key, "admin.announcements.create")

    def test_expires_at_respects_client_timezone_offset(self):
        from app.models import TeamAnnouncement

        admin = self._seed_user(18, role="admin", username="tzadmin")

        response = self._create(
            admin,
            title="Closing early",
            body="Heads up.",
            expires_at="2026-04-30T17:00",
            tz_offset_minutes="420",
        )

        self.assertEqual(response.status_code, 303)
        row = self.session.exec(select(TeamAnnouncement)).one()
        self.assertEqual(
            _utc(row.expires_at),
            datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        )

    def test_expires_at_backward_compat_when_no_tz_offset(self):
        from app.models import TeamAnnouncement

        admin = self._seed_user(19, role="admin", username="utctzadmin")

        response = self._create(
            admin,
            title="UTC expiration",
            body="Heads up.",
            expires_at="2026-04-30T17:00",
        )

        self.assertEqual(response.status_code, 303)
        row = self.session.exec(select(TeamAnnouncement)).one()
        self.assertEqual(
            _utc(row.expires_at),
            datetime(2026, 4, 30, 17, 0, tzinfo=timezone.utc),
        )

    def test_create_requires_title_and_body(self):
        from app.models import TeamAnnouncement

        admin = self._seed_user(2, role="admin", username="admin2")
        self._create(admin, title=" ", body="Body")
        self._create(admin, title="Title", body="\n ")

        rows = self.session.exec(select(TeamAnnouncement)).all()
        self.assertEqual(rows, [])

    def test_title_length_enforced(self):
        from app.models import TeamAnnouncement

        admin = self._seed_user(3, role="admin", username="admin3")
        response = self._create(admin, title="x" * 201, body="Body")

        self.assertEqual(response.status_code, 303)
        self.assertIn("flash=Title+must+be+200", response.headers["location"])
        self.assertEqual(self.session.exec(select(TeamAnnouncement)).all(), [])

    def test_employee_cannot_post(self):
        from app.models import TeamAnnouncement

        employee = self._seed_user(4, role="employee", username="emp")
        response = self._create(employee, title="Nope", body="Not allowed")

        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.session.exec(select(TeamAnnouncement)).all(), [])

    def test_employee_sees_active_announcement_on_dashboard(self):
        admin = self._seed_user(5, role="admin", username="poster")
        employee = self._seed_user(6, role="employee", username="reader")
        self._seed_announcement("Store meeting", creator=admin)

        html = self._dashboard_html(employee)

        self.assertIn("Store meeting", html)
        self.assertIn("pt-announcements-preview", html)

    def test_archived_announcement_hidden_from_employee_dashboard(self):
        admin = self._seed_user(7, role="admin", username="poster2")
        employee = self._seed_user(8, role="employee", username="reader2")
        row = self._seed_announcement("Archived notice", creator=admin)

        self._archive(admin, row.id)
        html = self._dashboard_html(employee)

        self.assertNotIn("Archived notice", html)

    def test_expired_announcement_hidden_from_employee(self):
        from app.models import utcnow

        admin = self._seed_user(9, role="admin", username="poster3")
        employee = self._seed_user(10, role="employee", username="reader3")
        self._seed_announcement(
            "Expired notice",
            creator=admin,
            expires_at=utcnow() - timedelta(days=1),
        )

        html = self._dashboard_html(employee)

        self.assertNotIn("Expired notice", html)

    def test_pinned_announcement_sorts_first(self):
        from app.models import utcnow

        admin = self._seed_user(11, role="admin", username="poster4")
        employee = self._seed_user(12, role="employee", username="reader4")
        now = utcnow()
        self._seed_announcement(
            "Normal update",
            creator=admin,
            published_at=now + timedelta(minutes=5),
        )
        self._seed_announcement(
            "Pinned update",
            creator=admin,
            pinned=True,
            published_at=now,
        )

        html = self._announcements_html(employee)

        self.assertLess(html.index("Pinned update"), html.index("Normal update"))

    def test_admin_can_archive(self):
        from app.models import TeamAnnouncement

        admin = self._seed_user(13, role="admin", username="poster5")
        row = self._seed_announcement("Archive me", creator=admin)

        response = self._archive(admin, row.id)

        self.assertEqual(response.status_code, 303)
        archived = self.session.get(TeamAnnouncement, row.id)
        self.assertFalse(archived.is_active)

    def test_audit_log_on_archive(self):
        from app.models import AuditLog

        admin = self._seed_user(14, role="admin", username="poster6")
        row = self._seed_announcement("Audit archive", creator=admin)

        self._archive(admin, row.id)

        audit = self.session.exec(
            select(AuditLog).where(AuditLog.action == "announcement.archived")
        ).one()
        self.assertEqual(audit.actor_user_id, admin.id)
        self.assertEqual(audit.resource_key, "admin.announcements.view")

    def test_dashboard_with_no_announcements_does_not_render_empty_card(self):
        employee = self._seed_user(15, role="employee", username="reader5")

        html = self._dashboard_html(employee)

        self.assertNotIn("pt-announcements-preview", html)

    def test_sidebar_shows_announcements_for_employee(self):
        employee = self._seed_user(16, role="employee", username="reader6")

        html = self._dashboard_html(employee)

        self.assertIn('href="/team/announcements"', html)

    def test_admin_sidebar_shows_management_link(self):
        admin = self._seed_user(17, role="admin", username="admin17")

        html = self._admin_announcements_html(admin)

        self.assertIn('href="/team/admin/announcements"', html)


if __name__ == "__main__":
    unittest.main()
