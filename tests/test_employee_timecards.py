"""Per-employee weekly timecard page regression tests.

Uses the direct-render pattern so these tests avoid the TestClient hang
issues seen in the sandbox. Mocks the Clockify client at the module level.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-timecards")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-timecards")
os.environ.setdefault("SESSION_SECRET", "unit-test-session-timecards-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-timecards")

LA = ZoneInfo("America/Los_Angeles")


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
    def __init__(self, current_user, *, path: str = "/team/admin/employees/1/timecards"):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(path=path, scheme="http", netloc="testserver")


class _FakeClockifyClient:
    def __init__(self, *, entries=None, exc=None):
        self._entries = entries or []
        self._exc = exc
        self.calls: list[dict] = []

    def get_user_time_entries(self, user_id, *, start_utc, end_utc, **_kw):
        self.calls.append({"user_id": user_id, "start": start_utc, "end": end_utc})
        if self._exc is not None:
            raise self._exc
        return list(self._entries)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_entry(*, start_local: datetime, end_local=None, running=False, desc="Shift", eid="e1"):
    interval = {"start": _iso_utc(start_local)}
    if end_local is not None and not running:
        interval["end"] = _iso_utc(end_local)
    else:
        interval["end"] = None
    return {"id": eid, "description": desc, "timeInterval": interval}


class EmployeeTimecardsTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        from app.db import seed_employee_portal_defaults

        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = self._seed_user(1, role="admin", username="admin1")
        self.employee = self._seed_user(2, role="employee", username="worker")

    def tearDown(self):
        self.session.close()

    def _seed_user(self, uid, *, role, username):
        from app.models import User

        user = User(
            id=uid,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username.title(),
            role=role,
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def _seed_profile(
        self,
        *,
        clockify_user_id=None,
        hourly_rate_cents=None,
        compensation_type="hourly",
        monthly_salary_cents=None,
    ):
        from app.models import EmployeeProfile
        from app.pii import encrypt_pii

        profile = self.session.get(EmployeeProfile, self.employee.id) or EmployeeProfile(
            user_id=self.employee.id
        )
        profile.compensation_type = compensation_type
        if clockify_user_id is not None:
            profile.clockify_user_id = clockify_user_id
        if hourly_rate_cents is not None:
            profile.hourly_rate_cents_enc = encrypt_pii(str(hourly_rate_cents))
        if monthly_salary_cents is not None:
            profile.monthly_salary_cents_enc = encrypt_pii(str(monthly_salary_cents))
        self.session.add(profile)
        self.session.commit()
        return profile

    def _seed_shift(self, *, day: date, label: str, kind: str = "work", sort_order: int = 0):
        from app.models import ShiftEntry

        shift = ShiftEntry(
            user_id=self.employee.id,
            shift_date=day,
            label=label,
            kind=kind,
            sort_order=sort_order,
            created_by_user_id=self.admin.id,
        )
        self.session.add(shift)
        self.session.commit()
        return shift

    def _settings(self, *, configured=True):
        from app.config import get_settings

        base = get_settings()
        return SimpleNamespace(
            employee_portal_enabled=True,
            clockify_api_key="k" if configured else "",
            clockify_workspace_id="w" if configured else "",
            clockify_timezone="America/Los_Angeles",
            clockify_base_url=base.clockify_base_url,
            clockify_timeout_seconds=base.clockify_timeout_seconds,
        )

    def _render(
        self,
        *,
        actor=None,
        week="2026-04-20",
        entries=None,
        api_exc=None,
        configured=True,
    ):
        from app.routers import team_admin_employees_timecards as mod

        actor = actor or self.admin
        fake_client = _FakeClockifyClient(entries=entries, exc=api_exc)
        request = _FakeRequest(actor)

        settings = self._settings(configured=configured)

        with patch.object(mod, "get_settings", return_value=settings), \
             patch.object(mod, "clockify_client_from_settings", return_value=fake_client):
            response = mod.admin_employee_timecards(
                request,
                self.employee.id,
                week=week,
                session=self.session,
            )
        return response, fake_client

    # ------------------------------------------------------------------

    def test_timecards_page_renders_with_mapped_employee(self):
        self._seed_profile(clockify_user_id="ck-1")
        start = datetime(2026, 4, 20, 10, 0, tzinfo=LA)  # Mon 10am PT
        end = datetime(2026, 4, 20, 18, 30, tzinfo=LA)   # 6:30pm PT
        entries = [_make_entry(start_local=start, end_local=end, desc="Open store")]

        response, client = self._render(entries=entries)
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Open store", body)
        self.assertIn("10:00 AM", body)
        self.assertIn("6:30 PM", body)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["user_id"], "ck-1")

    def test_timecards_page_renders_without_clockify_configured(self):
        self._seed_profile(clockify_user_id="ck-1")
        response, client = self._render(configured=False)
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Clockify is not configured", body)
        self.assertEqual(client.calls, [])

    def test_timecards_page_renders_without_employee_mapping(self):
        self._seed_profile(clockify_user_id=None)
        response, client = self._render()
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("isn't mapped to a Clockify user", body)
        self.assertEqual(client.calls, [])

    def test_timecards_page_handles_api_error(self):
        from app.clockify import ClockifyApiError

        self._seed_profile(clockify_user_id="ck-1")
        response, _client = self._render(api_exc=ClockifyApiError("boom: 503"))
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("Couldn't load hours from Clockify", body)
        self.assertIn("boom: 503", body)

    def test_timecards_page_shows_late_arrival_pill(self):
        self._seed_profile(clockify_user_id="ck-1")
        self._seed_shift(day=date(2026, 4, 20), label="10 AM - 7 PM", kind="work")
        entries = [
            _make_entry(
                start_local=datetime(2026, 4, 20, 10, 30, tzinfo=LA),
                end_local=datetime(2026, 4, 20, 18, 45, tzinfo=LA),
            )
        ]
        response, _ = self._render(entries=entries)
        body = response.body.decode("utf-8")
        self.assertIn("Late arrival", body)

    def test_timecards_page_shows_no_show_pill(self):
        self._seed_profile(clockify_user_id="ck-1")
        # Past Monday in the rendered week — should be flagged.
        self._seed_shift(day=date(2026, 4, 20), label="10 AM - 6 PM", kind="work")
        with patch(
            "app.routers.team_admin_employees_timecards.datetime"
        ) as dt_mock:
            dt_mock.now.return_value = datetime(2026, 4, 26, 12, 0, tzinfo=LA)
            dt_mock.combine = datetime.combine
            dt_mock.strptime = datetime.strptime
            dt_mock.min = datetime.min
            response, _ = self._render(entries=[])
        body = response.body.decode("utf-8")
        self.assertIn("No-show", body)

    def test_timecards_labor_total_respects_wage_privacy(self):
        # $25/hr => 2500 cents. 7.5 clocked hours with no break => 7 paid hours.
        self._seed_profile(clockify_user_id="ck-1", hourly_rate_cents=2500)
        entries = [
            _make_entry(
                start_local=datetime(2026, 4, 20, 10, 0, tzinfo=LA),
                end_local=datetime(2026, 4, 20, 17, 30, tzinfo=LA),
            )
        ]
        response, _ = self._render(entries=entries)
        body = response.body.decode("utf-8")
        self.assertIn("$175.00", body)
        self.assertIn("break 30m (30m auto)", body)
        self.assertNotIn("2500", body)
        self.assertNotIn("$25.00", body)
        self.assertNotIn("25.00/", body)

    def test_timecards_salary_employee_uses_fixed_monthly_amount(self):
        self._seed_profile(
            compensation_type="monthly_salary",
            monthly_salary_cents=450000,
        )

        response, client = self._render(configured=False)
        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("$4,500.00", body)
        self.assertIn("Monthly pay", body)
        self.assertIn("Fixed monthly salary", body)
        self.assertNotIn("Clockify is not configured", body)
        self.assertNotIn("isn't mapped to a Clockify user", body)
        self.assertEqual(client.calls, [])

    def test_timecards_paid_hours_exclude_break_entries(self):
        self._seed_profile(clockify_user_id="ck-1", hourly_rate_cents=2500)
        self._seed_shift(day=date(2026, 4, 20), label="10 AM - 6 PM", kind="work")
        entries = [
            _make_entry(
                start_local=datetime(2026, 4, 20, 10, 0, tzinfo=LA),
                end_local=datetime(2026, 4, 20, 14, 0, tzinfo=LA),
                desc="Floor",
                eid="work-a",
            ),
            _make_entry(
                start_local=datetime(2026, 4, 20, 14, 0, tzinfo=LA),
                end_local=datetime(2026, 4, 20, 14, 30, tzinfo=LA),
                desc="Lunch break",
                eid="break-a",
            ),
            _make_entry(
                start_local=datetime(2026, 4, 20, 14, 30, tzinfo=LA),
                end_local=datetime(2026, 4, 20, 18, 0, tzinfo=LA),
                desc="Floor",
                eid="work-b",
            ),
        ]

        response, _ = self._render(entries=entries)
        body = response.body.decode("utf-8")
        self.assertIn("Paid hours", body)
        self.assertIn("Lunch break", body)
        self.assertIn("Break", body)
        self.assertIn("30m", body)
        self.assertIn("variance -0:30", body)
        self.assertIn("$187.50", body)

    def test_timecards_day_status_persists_with_audit_note(self):
        from app.models import AuditLog, TimecardApproval
        from app.routers import team_admin_employees_timecards as mod

        self._seed_profile(clockify_user_id="ck-1")
        approval = mod.set_timecard_day_status(
            self.session,
            current_user=self.admin,
            user_id=self.employee.id,
            work_date=date(2026, 4, 20),
            status=mod.TIMECARD_STATUS_APPROVED,
            note="checked with payroll",
            ip_address="127.0.0.1",
        )
        self.assertEqual(approval.status, mod.TIMECARD_STATUS_APPROVED)
        persisted = self.session.exec(select(TimecardApproval)).first()
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted.note, "checked with payroll")
        audit_row = self.session.exec(select(AuditLog)).first()
        self.assertIsNotNone(audit_row)
        self.assertIn("checked with payroll", audit_row.details_json)

        response, _ = self._render(entries=[])
        body = response.body.decode("utf-8")
        self.assertIn("Approved", body)
        self.assertIn("checked with payroll", body)

    def test_timecards_permission_gate_denies_employee(self):
        # Employee role does NOT have admin.employees.view permission.
        response, _ = self._render(actor=self.employee, configured=False)
        self.assertEqual(response.status_code, 403)

    def test_timecards_week_navigation_renders_requested_week(self):
        self._seed_profile(clockify_user_id="ck-1")
        response, _ = self._render(week="2026-03-30", entries=[])
        body = response.body.decode("utf-8")
        # Week-range header should contain the Monday the user asked for
        # (Mar 30, 2026) and the Sunday 6 days later (Apr 5, 2026).
        self.assertIn("Mar 30", body)
        self.assertIn("Apr 5", body)
        # Prev/next nav links should point at ISO-8601 Mondays ±7 days.
        self.assertIn("week=2026-03-23", body)
        self.assertIn("week=2026-04-06", body)


if __name__ == "__main__":
    unittest.main()
