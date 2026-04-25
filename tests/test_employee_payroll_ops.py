"""Payroll export and exceptions inbox helper tests."""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-payroll")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "unit-test-hmac-payroll")
os.environ.setdefault("SESSION_SECRET", "unit-test-session-payroll-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "unit-test-admin-password-payroll")

LA = ZoneInfo("America/Los_Angeles")


class _FakeRequest:
    def __init__(self, current_user):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(path="/team/admin/payroll", scheme="http", netloc="testserver")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class PayrollOpsTests(unittest.TestCase):
    def setUp(self):
        from app.db import seed_employee_portal_defaults

        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = self._seed_user(1, role="admin", username="admin")
        self.employee = self._seed_user(2, role="employee", username="worker")

    def tearDown(self):
        self.session.close()

    def _settings(self):
        return SimpleNamespace(clockify_timezone="America/Los_Angeles")

    def _seed_user(self, uid, *, role, username):
        from app.models import User

        user = User(
            id=uid,
            username=username,
            display_name=username.title(),
            password_hash="x",
            password_salt="x",
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
        user_id: int,
        *,
        clockify_user_id=None,
        compensation_type="hourly",
        hourly_rate_cents=None,
        monthly_salary_cents=None,
    ):
        from app.models import EmployeeProfile
        from app.pii import encrypt_pii

        profile = EmployeeProfile(
            user_id=user_id,
            clockify_user_id=clockify_user_id,
            compensation_type=compensation_type,
        )
        if hourly_rate_cents is not None:
            profile.hourly_rate_cents_enc = encrypt_pii(str(hourly_rate_cents))
        if monthly_salary_cents is not None:
            profile.monthly_salary_cents_enc = encrypt_pii(str(monthly_salary_cents))
        self.session.add(profile)
        self.session.commit()
        return profile

    def _seed_clockify_entry(
        self,
        *,
        clockify_user_id="ck-1",
        start_local: datetime,
        end_local: datetime | None,
        description="Shift",
        is_running=False,
        entry_id="entry-1",
    ):
        from app.models import ClockifyTimeEntry

        start_utc = start_local.astimezone(timezone.utc)
        end_utc = end_local.astimezone(timezone.utc) if end_local else None
        duration_seconds = (
            int((end_utc - start_utc).total_seconds())
            if end_utc is not None
            else 0
        )
        row = ClockifyTimeEntry(
            clockify_entry_id=entry_id,
            clockify_user_id=clockify_user_id,
            user_id=self.employee.id,
            workspace_id="workspace",
            description=description,
            start_at=start_utc,
            end_at=end_utc,
            duration_seconds=duration_seconds,
            is_running=is_running,
        )
        self.session.add(row)
        self.session.commit()
        return row

    def test_payroll_summary_exports_and_locks_active_days(self):
        from app.models import TimecardApproval
        from app.routers import team_admin_clockify as mod

        self._seed_profile(
            self.employee.id,
            clockify_user_id="ck-1",
            hourly_rate_cents=2000,
        )
        self._seed_clockify_entry(
            start_local=datetime(2026, 4, 20, 10, 0, tzinfo=LA),
            end_local=datetime(2026, 4, 20, 18, 0, tzinfo=LA),
        )

        summary = mod.build_payroll_export_summary(
            self.session,
            start_day=date(2026, 4, 20),
            end_day=date(2026, 4, 26),
            settings=self._settings(),
            now=datetime(2026, 4, 26, 12, 0, tzinfo=LA),
        )
        self.assertEqual(summary["rows"][0]["total_label"], "$160.00")
        self.assertEqual(summary["rows"][0]["pending_day_count"], 1)
        csv_text = mod.payroll_summary_to_csv(summary)
        self.assertIn("Worker", csv_text)
        self.assertIn("$160.00", csv_text)

        result = mod.lock_payroll_window(
            self.session,
            current_user=self.admin,
            start_day=date(2026, 4, 20),
            end_day=date(2026, 4, 26),
            settings=self._settings(),
            ip_address="127.0.0.1",
        )
        self.assertEqual(result["locked"], 1)
        approval = self.session.exec(select(TimecardApproval)).one()
        self.assertEqual(approval.status, "locked")

    def test_exceptions_catches_mapping_rates_no_shows_and_rejections(self):
        from app.models import ShiftEntry, TimecardApproval
        from app.routers import team_admin_clockify as mod

        missing = self._seed_user(3, role="employee", username="missing")
        self._seed_profile(missing.id, compensation_type="hourly")
        self._seed_profile(
            self.employee.id,
            clockify_user_id="ck-1",
            hourly_rate_cents=2000,
        )
        self._seed_clockify_entry(
            start_local=datetime(2026, 4, 21, 10, 0, tzinfo=LA),
            end_local=datetime(2026, 4, 21, 14, 0, tzinfo=LA),
            entry_id="unscheduled",
        )
        self.session.add(
            ShiftEntry(
                user_id=self.employee.id,
                shift_date=date(2026, 4, 20),
                label="10 AM - 6 PM",
                kind="work",
                created_by_user_id=self.admin.id,
            )
        )
        self.session.add(
            TimecardApproval(
                user_id=self.employee.id,
                work_date=date(2026, 4, 21),
                status="rejected",
                note="wrong project",
            )
        )
        self.session.commit()

        inbox = mod.build_timecard_exceptions(
            self.session,
            week_start=date(2026, 4, 20),
            settings=self._settings(),
            now=datetime(2026, 4, 26, 12, 0, tzinfo=LA),
        )
        categories = {row["category"] for row in inbox["rows"]}
        self.assertIn("Missing Clockify mapping", categories)
        self.assertIn("Missing pay rate", categories)
        self.assertIn("Rejected timecard", categories)
        self.assertIn("Unscheduled clock-in", categories)
        self.assertIn("Possible no-show", categories)

    def test_new_admin_pages_render(self):
        from app.routers import team_admin_clockify as mod

        self._seed_profile(
            self.employee.id,
            clockify_user_id="ck-1",
            hourly_rate_cents=2000,
        )
        request = _FakeRequest(self.admin)

        payroll_response = mod.admin_payroll_page(
            request,
            range_key="custom",
            start="2026-04-20",
            end="2026-04-26",
            session=self.session,
        )
        self.assertEqual(payroll_response.status_code, 200)
        self.assertIn("Payroll Export", payroll_response.body.decode("utf-8"))

        exceptions_response = mod.admin_exceptions_page(
            request,
            week="2026-04-20",
            session=self.session,
        )
        self.assertEqual(exceptions_response.status_code, 200)
        self.assertIn("Exceptions Inbox", exceptions_response.body.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
