"""Wave D time-off request queue regression tests.

Direct router/template calls mirror the existing portal tests and avoid
TestClient route hangs in this sandbox.
"""
from __future__ import annotations

import asyncio
import os
import unittest
from datetime import date, timedelta
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy import update
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-timeoff")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-timeoff")
os.environ.setdefault("SESSION_SECRET", "unit-test-session-timeoff")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-timeoff")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class _FakeRequest:
    def __init__(self, current_user, *, path: str = "/team/timeoff"):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(
            path=path,
            scheme="http",
            netloc="testserver",
        )


class TeamTimeOffTests(unittest.TestCase):
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

    def _seed_request(
        self,
        user,
        *,
        start: date | None = None,
        end: date | None = None,
        status: str = "submitted",
        reason: str = "Family trip",
    ):
        from app.models import TimeOffRequest

        start = start or (date.today() + timedelta(days=14))
        end = end or start
        row = TimeOffRequest(
            submitted_by_user_id=user.id,
            start_date=start,
            end_date=end,
            reason=reason,
            status=status,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _submit(self, user, *, start: date, end: date, reason: str = "Trip"):
        from app.routers import team_timeoff

        request = _FakeRequest(user, path="/team/timeoff")
        return asyncio.run(
            team_timeoff.team_timeoff_post(
                request,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                reason=reason,
                session=self.session,
            )
        )

    def _approve(self, actor, request_id: int, *, notes: str = ""):
        from app.routers import team_admin_timeoff

        request = _FakeRequest(
            actor,
            path=f"/team/admin/timeoff/{request_id}/approve",
        )
        return asyncio.run(
            team_admin_timeoff.admin_timeoff_approve(
                request,
                request_id,
                notes,
                self.session,
            )
        )

    def _deny(self, actor, request_id: int, *, notes: str = ""):
        from app.routers import team_admin_timeoff

        request = _FakeRequest(
            actor,
            path=f"/team/admin/timeoff/{request_id}/deny",
        )
        return asyncio.run(
            team_admin_timeoff.admin_timeoff_deny(
                request,
                request_id,
                notes,
                self.session,
            )
        )

    def _admin_list_context(self, admin, *, status: str | None = None):
        from app.routers import team_admin_timeoff

        request = _FakeRequest(admin, path="/team/admin/timeoff")
        response = team_admin_timeoff.admin_timeoff_list(
            request,
            status=status,
            flash=None,
            error=None,
            session=self.session,
        )
        return response.context

    def _dashboard_html(self, user) -> str:
        from app import permissions as perms
        from app.routers.team import _nav_context
        from app.shared import templates

        request = _FakeRequest(user, path="/team/")
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

    def test_submit_valid_request(self):
        from app.models import AuditLog, TimeOffRequest

        user = self._seed_user(1, username="emp1")
        start = date.today() + timedelta(days=10)
        end = start + timedelta(days=2)

        response = self._submit(user, start=start, end=end, reason="  vacation  ")

        self.assertEqual(response.status_code, 303)
        row = self.session.exec(select(TimeOffRequest)).one()
        self.assertEqual(row.submitted_by_user_id, user.id)
        self.assertEqual(row.start_date, start)
        self.assertEqual(row.end_date, end)
        self.assertEqual(row.reason, "vacation")
        self.assertEqual(row.status, "submitted")
        audit = self.session.exec(
            select(AuditLog).where(AuditLog.action == "timeoff.submitted")
        ).one()
        self.assertEqual(audit.actor_user_id, user.id)

    def test_submit_rejects_end_before_start(self):
        from app.models import TimeOffRequest

        user = self._seed_user(2)
        start = date.today() + timedelta(days=10)

        response = self._submit(user, start=start, end=start - timedelta(days=1))

        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        self.assertEqual(self.session.exec(select(TimeOffRequest)).all(), [])

    def test_submit_rejects_past_start(self):
        from app.models import TimeOffRequest

        user = self._seed_user(3)
        start = date.today() - timedelta(days=1)

        self._submit(user, start=start, end=date.today() + timedelta(days=1))

        self.assertEqual(self.session.exec(select(TimeOffRequest)).all(), [])

    def test_submit_rejects_range_too_long(self):
        from app.models import TimeOffRequest

        user = self._seed_user(4)
        start = date.today() + timedelta(days=5)

        self._submit(user, start=start, end=start + timedelta(days=91))

        self.assertEqual(self.session.exec(select(TimeOffRequest)).all(), [])

    def test_submit_rejects_overlapping_pending(self):
        from app.models import TimeOffRequest

        user = self._seed_user(5)
        start = date.today() + timedelta(days=20)
        self._seed_request(user, start=start, end=start + timedelta(days=2))

        response = self._submit(
            user,
            start=start + timedelta(days=1),
            end=start + timedelta(days=3),
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("pending+request", response.headers["location"])
        self.assertEqual(len(self.session.exec(select(TimeOffRequest)).all()), 1)

    def test_submit_blocked_by_existing_approved_overlap(self):
        from app.models import TimeOffRequest

        user = self._seed_user(24)
        start = date.today() + timedelta(days=20)
        self._seed_request(
            user,
            start=start,
            end=start + timedelta(days=2),
            status="approved",
        )

        response = self._submit(
            user,
            start=start + timedelta(days=1),
            end=start + timedelta(days=3),
        )

        self.assertEqual(response.status_code, 303)
        self.assertIn("pending+request", response.headers["location"])
        self.assertEqual(len(self.session.exec(select(TimeOffRequest)).all()), 1)

    def test_submit_requires_csrf(self):
        from app.csrf import require_csrf
        from app.routers import team_timeoff

        route = next(
            route
            for route in team_timeoff.router.routes
            if getattr(route, "path", None) == "/team/timeoff"
            and "POST" in getattr(route, "methods", set())
        )

        self.assertTrue(
            any(dep.call is require_csrf for dep in route.dependant.dependencies)
        )

    def test_admin_sees_all_requests_default(self):
        employee = self._seed_user(6)
        admin = self._seed_user(7, role="admin", username="admin7")
        base = date.today() + timedelta(days=30)
        approved = self._seed_request(employee, start=base, status="approved")
        submitted = self._seed_request(
            employee,
            start=base + timedelta(days=1),
            status="submitted",
        )

        context = self._admin_list_context(admin)

        rows = context["requests"]
        self.assertEqual(rows[0].id, submitted.id)
        self.assertIn(approved.id, {row.id for row in rows})
        self.assertEqual(context["counts"]["submitted"], 1)

    def test_admin_status_filter(self):
        employee = self._seed_user(8)
        admin = self._seed_user(9, role="admin", username="admin9")
        base = date.today() + timedelta(days=30)
        approved = self._seed_request(employee, start=base, status="approved")
        self._seed_request(employee, start=base + timedelta(days=1), status="denied")

        context = self._admin_list_context(admin, status="approved")

        self.assertEqual([row.id for row in context["requests"]], [approved.id])
        self.assertEqual(context["filter_status"], "approved")

    def test_approve_changes_status_and_audits(self):
        from app.models import AuditLog, TimeOffRequest

        employee = self._seed_user(10)
        admin = self._seed_user(11, role="admin", username="admin11")
        row = self._seed_request(employee)

        response = self._approve(admin, row.id, notes="ok")

        self.assertEqual(response.status_code, 303)
        refreshed = self.session.get(TimeOffRequest, row.id)
        self.assertEqual(refreshed.status, "approved")
        self.assertEqual(refreshed.approved_by_user_id, admin.id)
        self.assertIsNotNone(refreshed.status_changed_at)
        self.assertEqual(refreshed.decision_notes, "ok")
        audit = self.session.exec(
            select(AuditLog).where(AuditLog.action == "timeoff.approved")
        ).one()
        self.assertEqual(audit.actor_user_id, admin.id)

    def test_deny_changes_status_and_audits(self):
        from app.models import AuditLog, TimeOffRequest

        employee = self._seed_user(12)
        admin = self._seed_user(13, role="admin", username="admin13")
        row = self._seed_request(employee)

        response = self._deny(admin, row.id, notes="coverage conflict")

        self.assertEqual(response.status_code, 303)
        refreshed = self.session.get(TimeOffRequest, row.id)
        self.assertEqual(refreshed.status, "denied")
        self.assertEqual(refreshed.approved_by_user_id, admin.id)
        self.assertIsNotNone(refreshed.status_changed_at)
        self.assertEqual(refreshed.decision_notes, "coverage conflict")
        audit = self.session.exec(
            select(AuditLog).where(AuditLog.action == "timeoff.denied")
        ).one()
        self.assertEqual(audit.actor_user_id, admin.id)

    def test_approve_is_idempotent(self):
        from app.models import AuditLog, SHIFT_KIND_REQUEST, ShiftEntry

        employee = self._seed_user(14)
        admin = self._seed_user(15, role="admin", username="admin15")
        start = date.today() + timedelta(days=30)
        row = self._seed_request(employee, start=start, end=start + timedelta(days=1))

        self._approve(admin, row.id)
        self._approve(admin, row.id)

        audits = self.session.exec(
            select(AuditLog).where(AuditLog.action == "timeoff.approved")
        ).all()
        shifts = self.session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == employee.id)
            .where(ShiftEntry.kind == SHIFT_KIND_REQUEST)
        ).all()
        self.assertEqual(len(audits), 1)
        self.assertEqual(len(shifts), 2)

    def test_stale_approval_loses_atomic_status_race(self):
        from app.models import AuditLog, SHIFT_KIND_REQUEST, ShiftEntry, TimeOffRequest, utcnow
        from app.routers import team_admin_timeoff

        employee = self._seed_user(240)
        admin = self._seed_user(241, role="admin", username="admin241")
        winner = self._seed_user(242, role="admin", username="admin242")
        row = self._seed_request(employee)

        stale_session = Session(self.engine)
        try:
            stale_row = stale_session.get(TimeOffRequest, row.id)
            self.assertEqual(stale_row.status, "submitted")
            now = utcnow()
            self.session.exec(
                update(TimeOffRequest)
                .where(TimeOffRequest.id == row.id)
                .values(
                    status="approved",
                    approved_by_user_id=winner.id,
                    status_changed_at=now,
                    updated_at=now,
                )
            )
            self.session.commit()

            response = team_admin_timeoff._transition_timeoff(
                stale_session,
                request_id=row.id,
                actor=admin,
                new_status="approved",
                action="timeoff.approved",
            )
            self.assertEqual(response.status_code, 409)
        finally:
            stale_session.close()

        self.session.expire_all()
        refreshed = self.session.get(TimeOffRequest, row.id)
        self.assertEqual(refreshed.status, "approved")
        self.assertEqual(refreshed.approved_by_user_id, winner.id)
        audits = self.session.exec(
            select(AuditLog).where(AuditLog.action == "timeoff.approved")
        ).all()
        shifts = self.session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == employee.id)
            .where(ShiftEntry.kind == SHIFT_KIND_REQUEST)
        ).all()
        self.assertEqual(audits, [])
        self.assertEqual(shifts, [])

    def test_cannot_approve_denied_request(self):
        from app.models import AuditLog, TimeOffRequest

        employee = self._seed_user(16)
        admin = self._seed_user(17, role="admin", username="admin17")
        row = self._seed_request(employee, status="denied")

        response = self._approve(admin, row.id)

        self.assertEqual(response.status_code, 303)
        self.assertIn("error=", response.headers["location"])
        refreshed = self.session.get(TimeOffRequest, row.id)
        self.assertEqual(refreshed.status, "denied")
        audits = self.session.exec(
            select(AuditLog).where(AuditLog.action == "timeoff.approved")
        ).all()
        self.assertEqual(audits, [])

    def test_approval_creates_request_shifts_without_overwriting_existing(self):
        from app.models import (
            SHIFT_KIND_REQUEST,
            SHIFT_KIND_WORK,
            ShiftEntry,
        )

        employee = self._seed_user(18)
        admin = self._seed_user(19, role="admin", username="admin19")
        start = date.today() + timedelta(days=30)
        existing = ShiftEntry(
            user_id=employee.id,
            shift_date=start,
            label="10-6",
            kind=SHIFT_KIND_WORK,
            notes="original shift",
            created_by_user_id=admin.id,
            sort_order=0,
        )
        self.session.add(existing)
        self.session.commit()
        self.session.refresh(existing)
        row = self._seed_request(employee, start=start, end=start + timedelta(days=2))

        self._approve(admin, row.id)

        original = self.session.get(ShiftEntry, existing.id)
        self.assertEqual(original.label, "10-6")
        self.assertEqual(original.kind, SHIFT_KIND_WORK)
        request_shifts = self.session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == employee.id)
            .where(ShiftEntry.kind == SHIFT_KIND_REQUEST)
        ).all()
        self.assertEqual(len(request_shifts), 3)
        first_day_request = [
            shift for shift in request_shifts if shift.shift_date == start
        ][0]
        self.assertGreater(first_day_request.sort_order, existing.sort_order)

    def test_approval_populates_only_affected_dates(self):
        from app.models import SHIFT_KIND_REQUEST, ShiftEntry

        employee = self._seed_user(20)
        admin = self._seed_user(21, role="admin", username="admin21")
        start = date.today() + timedelta(days=40)
        row = self._seed_request(employee, start=start, end=start + timedelta(days=2))

        self._approve(admin, row.id)

        shift_dates = {
            shift.shift_date
            for shift in self.session.exec(
                select(ShiftEntry)
                .where(ShiftEntry.user_id == employee.id)
                .where(ShiftEntry.kind == SHIFT_KIND_REQUEST)
            ).all()
        }
        self.assertEqual(
            shift_dates,
            {start, start + timedelta(days=1), start + timedelta(days=2)},
        )
        self.assertNotIn(start - timedelta(days=1), shift_dates)
        self.assertNotIn(start + timedelta(days=3), shift_dates)

    def test_employee_cannot_approve(self):
        from app.models import TimeOffRequest

        employee = self._seed_user(22)
        row = self._seed_request(employee)

        response = self._approve(employee, row.id)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.session.get(TimeOffRequest, row.id).status, "submitted")

    def test_team_requests_alias_redirects_to_timeoff(self):
        from app.routers.team_timeoff import team_requests_alias

        response = team_requests_alias()

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/team/timeoff")

    def test_admin_requests_alias_redirects(self):
        from app.routers.team_admin_timeoff import admin_requests_alias

        response = admin_requests_alias()

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/team/admin/timeoff")

    def test_sidebar_shows_timeoff_for_employee(self):
        employee = self._seed_user(23, username="emp23")

        html = self._dashboard_html(employee)

        self.assertIn('href="/team/timeoff"', html)


if __name__ == "__main__":
    unittest.main()
