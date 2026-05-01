from __future__ import annotations

import inspect
import os
import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "audit-top5-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "audit-top5-hmac-key")
os.environ.setdefault("SESSION_SECRET", "audit-top5-session-secret-xxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "audit-top5-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class PortalAuditTop5Tests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app.db import seed_employee_portal_defaults

        cfg.get_settings.cache_clear()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _user(
        self,
        user_id: int,
        *,
        role: str = "employee",
        username: str | None = None,
        is_active: bool = True,
    ):
        from app.models import EmployeeProfile, User

        user = User(
            id=user_id,
            username=username or f"user{user_id}",
            password_hash="x",
            password_salt="x",
            display_name=username or f"User {user_id}",
            role=role,
            is_active=is_active,
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.add(EmployeeProfile(user_id=user_id))
        self.session.commit()
        self.session.refresh(user)
        return user

    def _request(self, user, *, path: str = "/team/"):
        return SimpleNamespace(
            state=SimpleNamespace(current_user=user),
            session={},
            scope={"session": {}},
            headers={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(
                path=path,
                query="",
                scheme="http",
                netloc="testserver",
            ),
        )

    @staticmethod
    def _template_response(_request, template_name, context, **_kwargs):
        return SimpleNamespace(
            status_code=200,
            template_name=template_name,
            context=context,
        )

    def _dashboard_context(self, user):
        from app.routers import team

        request = self._request(user)
        with patch.object(
            team.templates,
            "TemplateResponse",
            side_effect=self._template_response,
        ):
            response = team.team_dashboard(request, session=self.session)
        self.assertEqual(response.status_code, 200)
        return response.context

    def test_dashboard_hides_supply_queue_count_without_permission(self):
        from app.models import SupplyRequest
        from app.shared import templates

        employee = self._user(10, role="employee", username="employee")
        self.session.add(
            SupplyRequest(submitted_by_user_id=employee.id, title="Top loaders")
        )
        self.session.commit()

        context = self._dashboard_context(employee)

        self.assertFalse(context["show_supply_queue_count"])
        self.assertNotIn("supply_queue_count", context)
        html = templates.env.get_template("team/dashboard.html").render(context)
        self.assertNotIn("Pending requests", html)

    def test_dashboard_shows_supply_queue_count_with_permission(self):
        from app.models import SupplyRequest
        from app.shared import templates

        admin = self._user(20, role="admin", username="admin")
        self.session.add_all(
            [
                SupplyRequest(submitted_by_user_id=admin.id, title="Sleeves"),
                SupplyRequest(
                    submitted_by_user_id=admin.id,
                    title="Boxes",
                    status="pending",
                ),
                SupplyRequest(
                    submitted_by_user_id=admin.id,
                    title="Already handled",
                    status="approved",
                ),
            ]
        )
        self.session.commit()

        context = self._dashboard_context(admin)

        self.assertTrue(context["show_supply_queue_count"])
        self.assertEqual(context["supply_queue_count"], 2)
        html = templates.env.get_template("team/dashboard.html").render(context)
        self.assertIn("Supply queue", html)
        self.assertIn(">2<", html)
        self.assertIn("Pending requests", html)

    def test_team_admin_home_counts_use_aggregate_queries(self):
        from app.routers import team_admin

        source = inspect.getsource(team_admin.team_admin_home)
        self.assertIn("select(func.count())", source)
        self.assertNotIn(".all()", source)

    def test_team_admin_home_count_context(self):
        from app.models import (
            EmployeeProfile,
            InviteToken,
            ShiftEntry,
            SupplyRequest,
            TeamAnnouncement,
            TimeOffRequest,
            TimecardApproval,
            utcnow,
        )
        from app.routers import team_admin

        now = utcnow()
        today = now.date()
        admin = self._user(30, role="admin", username="admin-overview")
        employee = self._user(31, role="employee", username="employee-overview")
        manager = self._user(32, role="manager", username="manager-overview")
        inactive = self._user(
            33,
            role="employee",
            username="inactive-overview",
            is_active=False,
        )
        self._user(
            34,
            role="employee",
            username="__draft_employee",
            is_active=False,
        )
        profile = self.session.get(EmployeeProfile, employee.id)
        profile.clockify_user_id = "clk-employee"
        self.session.add(profile)
        self.session.add_all(
            [
                InviteToken(
                    token_hash="open-invite",
                    role="employee",
                    created_by_user_id=admin.id,
                    expires_at=now + timedelta(days=1),
                ),
                InviteToken(
                    token_hash="used-invite",
                    role="employee",
                    created_by_user_id=admin.id,
                    expires_at=now + timedelta(days=1),
                    used_at=now,
                ),
                SupplyRequest(
                    submitted_by_user_id=employee.id,
                    title="Pending supply",
                    status="submitted",
                ),
                SupplyRequest(
                    submitted_by_user_id=employee.id,
                    title="Approved supply",
                    status="approved",
                ),
                TimeOffRequest(
                    submitted_by_user_id=employee.id,
                    start_date=today + timedelta(days=10),
                    end_date=today + timedelta(days=10),
                    status="submitted",
                ),
                TimeOffRequest(
                    submitted_by_user_id=employee.id,
                    start_date=today + timedelta(days=11),
                    end_date=today + timedelta(days=11),
                    status="approved",
                ),
                TimecardApproval(
                    user_id=employee.id,
                    work_date=today,
                    status="pending",
                ),
                TimecardApproval(
                    user_id=manager.id,
                    work_date=today,
                    status="approved",
                ),
                TeamAnnouncement(
                    title="Active",
                    body="Visible",
                    created_by_user_id=admin.id,
                    is_active=True,
                ),
                TeamAnnouncement(
                    title="Inactive",
                    body="Hidden",
                    created_by_user_id=admin.id,
                    is_active=False,
                ),
                ShiftEntry(
                    user_id=employee.id,
                    shift_date=today,
                    label="9 AM - 1 PM",
                    created_by_user_id=admin.id,
                ),
                ShiftEntry(
                    user_id=manager.id,
                    shift_date=today + timedelta(days=6),
                    label="10 AM - 2 PM",
                    created_by_user_id=admin.id,
                ),
                ShiftEntry(
                    user_id=inactive.id,
                    shift_date=today + timedelta(days=8),
                    label="11 AM - 3 PM",
                    created_by_user_id=admin.id,
                ),
            ]
        )
        self.session.commit()

        request = self._request(admin, path="/team/admin")
        with patch.object(
            team_admin.templates,
            "TemplateResponse",
            side_effect=self._template_response,
        ):
            response = team_admin.team_admin_home(request, session=self.session)

        self.assertEqual(response.status_code, 200)
        context = response.context
        self.assertEqual(context["employee_count"], 5)
        self.assertEqual(context["active_employee_count"], 2)
        self.assertEqual(context["outstanding_invites"], 1)
        self.assertEqual(context["draft_employee_count"], 1)
        self.assertEqual(context["pending_supply"], 1)
        self.assertEqual(context["pending_timeoff"], 1)
        self.assertEqual(context["pending_timecards"], 1)
        self.assertEqual(context["active_announcements"], 1)
        self.assertEqual(context["clockify_mapped"], 1)
        self.assertEqual(context["clockify_unmapped"], 1)
        self.assertEqual(context["upcoming_shift_count"], 2)
        self.assertEqual(context["needs_attention_count"], 3)
