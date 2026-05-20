"""Wave E manager schedule tooling and wage-privacy tests."""
from __future__ import annotations

import logging
import os
import re
import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "manager-tooling-salt")
os.environ.setdefault("SESSION_SECRET", "manager-tooling-secret-" + "x" * 32)


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class TeamManagerToolingTests(unittest.TestCase):
    WEEK = date(2026, 4, 20)

    def setUp(self):
        from app import rate_limit

        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        from app.models import User

        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = User(
            id=8500,
            username="mgr_admin",
            password_hash="x",
            password_salt="x",
            display_name="Manager Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def _employee(
        self,
        user_id: int,
        name: str,
        *,
        rate_plaintext: str | None = "2500",
        encrypted_blob: bytes | None = None,
    ):
        from app.models import EmployeeProfile, ScheduleRosterMember, User
        from app.team.pii import encrypt_pii

        u = User(
            id=user_id,
            username=name.lower().replace(" ", "_"),
            password_hash="x",
            password_salt="x",
            display_name=name,
            role="employee",
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(u)
        self.session.add(
            ScheduleRosterMember(
                week_start=self.WEEK,
                user_id=user_id,
                added_by_user_id=self.admin.id,
            )
        )
        if encrypted_blob is not None:
            rate_blob = encrypted_blob
        elif rate_plaintext is None:
            rate_blob = None
        else:
            rate_blob = encrypt_pii(rate_plaintext)
        self.session.add(
            EmployeeProfile(
                user_id=user_id,
                hourly_rate_cents_enc=rate_blob,
            )
        )
        self.session.commit()
        return u

    def _shift(
        self,
        user_id: int,
        *,
        label: str = "10-6",
        kind: str | None = None,
        day_offset: int = 0,
    ) -> None:
        from app.models import ShiftEntry, classify_shift_label

        self.session.add(
            ShiftEntry(
                user_id=user_id,
                shift_date=self.WEEK + timedelta(days=day_offset),
                label=label,
                kind=kind or classify_shift_label(label),
                created_by_user_id=self.admin.id,
            )
        )
        self.session.commit()

    def _ctx(self):
        from app.models import STAFF_KIND_STOREFRONT
        from app.routers.team_admin_schedule import _grid_context

        return _grid_context(
            self.session,
            self.WEEK,
            staff_kind=STAFF_KIND_STOREFRONT,
        )

    def _render(self) -> str:
        from app import shared
        from app.models import STAFF_KIND_STOREFRONT, STAFF_KIND_STREAM
        from app.routers.team_admin_schedule import (
            _build_cell_key,
            _build_day_loc_key,
            _grid_context,
        )

        storefront = _grid_context(
            self.session,
            self.WEEK,
            staff_kind=STAFF_KIND_STOREFRONT,
        )
        stream = _grid_context(
            self.session,
            self.WEEK,
            staff_kind=STAFF_KIND_STREAM,
        )
        request = SimpleNamespace(
            state=SimpleNamespace(
                can_view_admin_announcements=False,
                can_view_admin_timeoff=False,
            ),
            url=SimpleNamespace(path="/team/admin/schedule"),
        )
        template = shared.templates.env.get_template("team/admin/schedule.html")
        return template.render(
            request=request,
            title="Schedule",
            active="schedule",
            current_user=self.admin,
            can_edit=True,
            edit_mode=False,
            stream_accounts=[],
            stream_account_colors={},
            holiday_options=[],
            custom_closures=[],
            csrf_token="csrf-token",
            build_cell_key=_build_cell_key,
            build_day_loc_key=_build_day_loc_key,
            storefront=storefront,
            stream=stream,
            week_start=storefront["week_start"],
            week_start_iso=storefront["week_start_iso"],
            week_days=storefront["week_days"],
            day_note_map=storefront["day_note_map"],
            prev_week=storefront["prev_week"],
            next_week=storefront["next_week"],
            this_week=storefront["this_week"],
            is_current_week=storefront["is_current_week"],
            today=self.WEEK,
            flash=None,
        )

    def test_encrypted_rate_decrypts_server_side(self):
        employee = self._employee(8601, "Encrypted Rate", rate_plaintext="2500")
        self._shift(employee.id)

        ctx = self._ctx()
        self.assertEqual(ctx["labor_total_cents"], 20000)
        self.assertEqual(ctx["labor_total_display"], "$200.00")

    def test_missing_rate_treated_as_zero(self):
        employee = self._employee(8602, "Missing Rate", rate_plaintext=None)
        self._shift(employee.id)

        ctx = self._ctx()
        self.assertEqual(ctx["labor_total_cents"], 0)
        self.assertEqual(ctx["missing_rate_count"], 1)

    def test_bad_ciphertext_does_not_500(self):
        employee = self._employee(8603, "Bad Ciphertext", encrypted_blob=b"garbage")
        self._shift(employee.id)

        from starlette.requests import Request
        from app.routers import team_admin_schedule

        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/team/admin/schedule",
                "query_string": f"week={self.WEEK.isoformat()}".encode("ascii"),
                "headers": [],
                "session": {},
            }
        )
        with patch.object(
            team_admin_schedule,
            "_permission_gate",
            return_value=(None, self.admin),
        ), patch.object(team_admin_schedule, "has_permission", return_value=True):
            response = team_admin_schedule.admin_schedule_view(
                request,
                week=self.WEEK.isoformat(),
                session=self.session,
            )
        self.assertEqual(response.status_code, 200)
        html = self._render()
        ctx = self._ctx()
        self.assertEqual(ctx["labor_total_cents"], 0)
        self.assertEqual(ctx["missing_rate_count"], 1)
        self.assertIn("missing rates", html)

    def test_non_integer_plaintext_treated_as_missing(self):
        employee = self._employee(8604, "Text Rate", rate_plaintext="hello")
        self._shift(employee.id)

        ctx = self._ctx()
        self.assertEqual(ctx["labor_total_cents"], 0)
        self.assertEqual(ctx["missing_rate_count"], 1)

    def test_rate_cents_never_appear_in_rendered_html(self):
        employee = self._employee(8605, "Private Rate", rate_plaintext="2537")
        self._shift(employee.id)

        html = self._render()
        for forbidden in ("2537", "25.37", "$25.37"):
            self.assertNotIn(forbidden, html)
        self.assertIn("Labor this week", html)

    def test_rate_cents_never_appear_in_json_context(self):
        employee = self._employee(8606, "Private Json", rate_plaintext="2537")
        self._shift(employee.id)

        html = self._render()
        scripts = "\n".join(
            re.findall(r"<script\b[^>]*>(.*?)</script>", html, flags=re.I | re.S)
        )
        haystack = scripts or html
        for forbidden in (
            "2537",
            "25.37",
            "$25.37",
            "hourly_rate_cents",
            "rate_cents",
            "hourlyRate",
        ):
            self.assertNotIn(forbidden, haystack)

    def test_rate_cents_never_logged(self):
        employee = self._employee(8607, "Log Privacy", encrypted_blob=b"garbage")
        self._shift(employee.id)
        captured: list[str] = []

        def _capture(_logger, msg, *args, **_kwargs):
            try:
                rendered = str(msg) % args if args else str(msg)
            except Exception:
                rendered = str(msg)
            captured.append(rendered)

        with patch.object(logging.Logger, "info", new=_capture), patch.object(
            logging.Logger, "warning", new=_capture
        ), patch.object(logging.Logger, "error", new=_capture), patch.object(
            logging.Logger, "exception", new=_capture
        ):
            self._render()

        logs = "\n".join(captured)
        for forbidden in (
            "2537",
            "25.37",
            "$25.37",
            "garbage",
            "hourly_rate",
            os.environ["EMPLOYEE_PII_KEY"],
        ):
            self.assertNotIn(forbidden, logs)

    def test_labor_total_format(self):
        employee = self._employee(8608, "Big Total", rate_plaintext="15432")
        self._shift(employee.id)

        ctx = self._ctx()
        self.assertEqual(ctx["labor_total_display"], "$1,234.56")
        self.assertIn("$1,234.56", self._render())

    def test_zero_labor_with_missing_rates_shows_em_dash(self):
        employee = self._employee(8609, "Dash Missing", rate_plaintext=None)
        self._shift(employee.id)

        html = self._render()
        self.assertIn("— (missing rates)", html)
        self.assertNotIn("$0.00", html)

    def test_labor_total_excludes_shift_kind_request(self):
        from app.models import SHIFT_KIND_REQUEST

        employee = self._employee(8610, "Request Excluded", rate_plaintext="2000")
        self._shift(employee.id, label="10-6", kind=SHIFT_KIND_REQUEST)

        ctx = self._ctx()
        self.assertEqual(ctx["grand_hours"], 0.0)
        self.assertEqual(ctx["labor_total_cents"], 0)

    def test_coverage_gap_day_rendered_with_warning_class(self):
        employee = self._employee(8611, "Gap Rendered", rate_plaintext="2500")
        self._shift(employee.id, day_offset=0)

        html = self._render()
        self.assertRegex(
            html,
            re.compile(
                r'<th class="[^"]*sched-day-gap[^"]*"[^>]*>\s*'
                r'<div[^>]*>21 Apr</div>.*?No coverage',
                re.S,
            ),
        )
        self.assertNotRegex(
            html,
            re.compile(
                r'<th class="[^"]*sched-day-gap[^"]*"[^>]*>\s*'
                r'<div[^>]*>20 Apr</div>',
                re.S,
            ),
        )


if __name__ == "__main__":
    unittest.main()
