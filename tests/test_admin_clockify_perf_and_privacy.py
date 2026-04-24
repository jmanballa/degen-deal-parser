from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave-g-clockify-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "wave-g-clockify-hmac-key")
os.environ.setdefault("SESSION_SECRET", "wave-g-clockify-session-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "wave-g-clockify-admin-password")


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
    def __init__(self, current_user, path="/team/admin/clockify"):
        self.state = SimpleNamespace(current_user=current_user)
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(path=path, scheme="http", netloc="testserver")


class _FakeClockifyClient:
    def __init__(self, entries=None):
        self.entries = entries or []
        self.entry_calls = 0

    def workspace_info(self):
        return {"id": "workspace", "name": "Test Workspace"}

    def list_workspace_users(self, status="ALL"):
        return [{"id": "clock-1", "name": "Alice", "email": "alice@example.com", "status": status}]

    def get_user_time_entries(self, user_id, *, start_utc, end_utc, **_kw):
        self.entry_calls += 1
        return list(self.entries)

    def user_week_summary(self, user_id, *, today=None, settings=None):
        from app.clockify import build_week_summary, clockify_week_bounds

        start, end = clockify_week_bounds(today, settings=settings)
        return build_week_summary(
            self.get_user_time_entries(
                user_id,
                start_utc=start.astimezone(timezone.utc),
                end_utc=end.astimezone(timezone.utc),
            ),
            week_start_local=start,
            week_end_local=end,
            settings=settings,
        )


class _CountingSummaryClient:
    def __init__(self):
        self.calls = 0

    def user_week_summary(self, user_id, *, today=None, settings=None):
        self.calls += 1
        return SimpleNamespace(user_id=user_id, call_number=self.calls)


class ClockifyAdminPerfPrivacyTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app.db import seed_employee_portal_defaults
        from app.models import User
        from app.routers import team_admin_clockify as mod

        cfg.get_settings.cache_clear()
        mod._CLOCKIFY_WEEK_CACHE.clear()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = User(
            id=1,
            username="clockify-admin",
            password_hash="x",
            password_salt="x",
            display_name="Clockify Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()

    def tearDown(self):
        from app.routers import team_admin_clockify as mod

        mod._CLOCKIFY_WEEK_CACHE.clear()
        self.session.close()

    def _settings(self):
        return SimpleNamespace(
            employee_portal_enabled=True,
            clockify_api_key="key",
            clockify_workspace_id="workspace",
            clockify_timezone="America/Los_Angeles",
        )

    def _render(self, *, include_hours="0", entries=None):
        from app.routers import team_admin_clockify as mod

        fake_client = _FakeClockifyClient(entries=entries)
        with patch.object(mod, "get_settings", return_value=self._settings()), \
             patch.object(mod, "clockify_client_from_settings", return_value=fake_client):
            response = mod.admin_clockify_page(
                _FakeRequest(self.admin),
                include_hours=include_hours,
                session=self.session,
            )
        return response.body.decode("utf-8"), fake_client

    def _render_shift_tracker(self):
        from app.routers import team_admin_clockify as mod

        fake_client = _FakeClockifyClient()
        with patch.object(mod, "get_settings", return_value=self._settings()), \
             patch.object(mod, "clockify_client_from_settings", return_value=fake_client):
            response = mod.admin_shift_tracker_page(
                _FakeRequest(self.admin, path="/team/admin/shift-tracker"),
                session=self.session,
            )
        return response.body.decode("utf-8"), fake_client

    def _seed_linked_employee(self):
        from app.models import EmployeeProfile, User

        employee = User(
            id=20,
            username="alice",
            password_hash="x",
            password_salt="x",
            display_name="Alice",
            role="employee",
            is_active=True,
        )
        profile = EmployeeProfile(user_id=20, clockify_user_id="clock-1")
        self.session.add(employee)
        self.session.add(profile)
        self.session.commit()
        return employee

    def test_page_does_not_fetch_hours_by_default(self):
        html, client = self._render(include_hours="0")
        self.assertIn("Not loaded", html)
        self.assertEqual(client.entry_calls, 0)

    def test_page_does_not_fetch_live_status_by_default(self):
        self._seed_linked_employee()

        html, client = self._render(include_hours="0")

        self.assertNotIn("Live clock-ins", html)
        self.assertIn("Manual employee matching", html)
        self.assertEqual(client.entry_calls, 0)

    def test_page_fetches_hours_when_explicitly_requested(self):
        entries = [
            {
                "id": "entry-1",
                "description": "Open",
                "timeInterval": {
                    "start": "2026-04-20T16:00:00Z",
                    "end": "2026-04-20T18:00:00Z",
                },
            }
        ]
        html, client = self._render(include_hours="1", entries=entries)
        self.assertIn("2h", html)
        self.assertEqual(client.entry_calls, 1)

    def test_week_summary_is_cached_within_60_seconds(self):
        from app.routers import team_admin_clockify as mod

        client = _CountingSummaryClient()
        first = mod._cached_user_week_summary(
            client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
        )
        second = mod._cached_user_week_summary(
            client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
        )
        self.assertIs(first, second)
        self.assertEqual(client.calls, 1)

    def test_week_summary_cache_expires_after_60_seconds(self):
        from app.routers import team_admin_clockify as mod

        client = _CountingSummaryClient()
        first = mod._cached_user_week_summary(
            client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
        )
        key = next(iter(mod._CLOCKIFY_WEEK_CACHE))
        mod._CLOCKIFY_WEEK_CACHE[key] = (0.0, first)
        with patch.object(mod.time, "time", return_value=120.0):
            second = mod._cached_user_week_summary(
                client, "clock-1", today=date(2026, 4, 24), settings=self._settings()
            )
        self.assertIsNot(first, second)
        self.assertEqual(client.calls, 2)

    def test_displayed_emails_are_masked(self):
        html, _client = self._render(include_hours="0")
        self.assertNotIn("alice@example.com", html)
        self.assertIn("ali***@example.com", html)

    def test_live_status_shows_running_timer_and_break(self):
        from app.routers import team_admin_clockify as mod

        self._seed_linked_employee()
        entries = [
            {
                "id": "break-1",
                "description": "Lunch break",
                "timeInterval": {
                    "start": "2026-04-24T19:00:00Z",
                    "end": "2026-04-24T19:30:00Z",
                },
            },
            {
                "id": "work-1",
                "description": "Shipping orders",
                "timeInterval": {
                    "start": "2026-04-24T20:00:00Z",
                    "end": None,
                },
            },
        ]

        live = mod.build_clockify_live_status(
            self.session,
            _FakeClockifyClient(entries=entries),
            settings=self._settings(),
            today=date(2026, 4, 24),
            now=datetime(2026, 4, 24, 22, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(live["mapped_count"], 1)
        row = live["rows"][0]
        self.assertEqual(row["status"], "Clocked in")
        self.assertEqual(row["current_start_label"], "1:00 PM")
        self.assertEqual(row["running_duration_label"], "2h")
        self.assertEqual(row["today_total_label"], "2h")
        self.assertEqual(row["break_label"], "Taken (30m)")

    def test_live_status_marks_current_break(self):
        from app.routers import team_admin_clockify as mod

        self._seed_linked_employee()
        entries = [
            {
                "id": "break-now",
                "description": "Break",
                "timeInterval": {
                    "start": "2026-04-24T21:45:00Z",
                    "end": None,
                },
            },
        ]

        live = mod.build_clockify_live_status(
            self.session,
            _FakeClockifyClient(entries=entries),
            settings=self._settings(),
            today=date(2026, 4, 24),
            now=datetime(2026, 4, 24, 22, 0, tzinfo=timezone.utc),
        )

        row = live["rows"][0]
        self.assertEqual(row["status"], "On break")
        self.assertEqual(row["current_start_label"], "2:45 PM")
        self.assertEqual(row["running_duration_label"], "15m")
        self.assertEqual(row["today_total_label"], "0m")
        self.assertEqual(row["break_label"], "On break now (15m)")

    def test_live_status_uses_cached_webhook_entries_before_api(self):
        from app.models import ClockifyTimeEntry

        self._seed_linked_employee()
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id="cached-running",
                clockify_user_id="clock-1",
                user_id=20,
                description="Cached work",
                start_at=datetime(2026, 4, 24, 20, 0, tzinfo=timezone.utc),
                end_at=None,
                is_running=True,
            )
        )
        self.session.commit()

        with patch("app.routers.team_admin_clockify.datetime") as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 4, 24, 22, 0, tzinfo=timezone.utc)
            fake_datetime.combine.side_effect = datetime.combine
            fake_datetime.min = datetime.min
            html, client = self._render_shift_tracker()

        self.assertIn("Clocked in", html)
        self.assertIn("2h", html)
        self.assertEqual(client.entry_calls, 0)

    def test_shift_tracker_adds_hourly_labor_from_cached_entries(self):
        from app.models import ClockifyTimeEntry, EmployeeProfile
        from app.pii import encrypt_pii

        self._seed_linked_employee()
        profile = self.session.get(EmployeeProfile, 20)
        profile.compensation_type = "hourly"
        profile.hourly_rate_cents_enc = encrypt_pii("2500")
        self.session.add(profile)
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id="cached-closed",
                clockify_user_id="clock-1",
                user_id=20,
                description="Cached work",
                start_at=datetime(2026, 4, 24, 18, 0, tzinfo=timezone.utc),
                end_at=datetime(2026, 4, 24, 20, 0, tzinfo=timezone.utc),
                duration_seconds=7200,
                is_running=False,
            )
        )
        self.session.add(
            ClockifyTimeEntry(
                clockify_entry_id="cached-break",
                clockify_user_id="clock-1",
                user_id=20,
                description="Lunch break",
                start_at=datetime(2026, 4, 24, 20, 0, tzinfo=timezone.utc),
                end_at=datetime(2026, 4, 24, 20, 30, tzinfo=timezone.utc),
                duration_seconds=1800,
                is_running=False,
            )
        )
        self.session.commit()

        with patch("app.routers.team_admin_clockify.date") as fake_date, \
             patch("app.routers.team_admin_clockify.datetime") as fake_datetime:
            fake_date.today.return_value = date(2026, 4, 24)
            fake_datetime.now.return_value = datetime(2026, 4, 24, 22, 0, tzinfo=timezone.utc)
            fake_datetime.combine.side_effect = datetime.combine
            fake_datetime.min = datetime.min
            html, client = self._render_shift_tracker()

        self.assertIn("$50.00", html)
        self.assertIn("Taken (30m)", html)
        self.assertIn("Hourly today", html)
        self.assertEqual(client.entry_calls, 0)

    def test_manual_refresh_caches_entries_without_user_id_in_payload(self):
        from app.models import ClockifyTimeEntry
        from app.routers import team_admin_clockify as mod

        self._seed_linked_employee()
        client = _FakeClockifyClient(
            entries=[
                {
                    "id": "refresh-1",
                    "description": "Packing",
                    "timeInterval": {
                        "start": "2026-04-24T18:00:00Z",
                        "end": "2026-04-24T19:00:00Z",
                    },
                }
            ]
        )

        result = mod.refresh_clockify_shift_tracker_cache(
            self.session,
            client,
            settings=self._settings(),
            today=date(2026, 4, 24),
        )
        cached = self.session.exec(
            mod.select(ClockifyTimeEntry).where(
                ClockifyTimeEntry.clockify_entry_id == "refresh-1"
            )
        ).first()

        self.assertEqual(result["cached_entries"], 1)
        self.assertIsNotNone(cached)
        self.assertEqual(cached.clockify_user_id, "clock-1")
        self.assertEqual(cached.user_id, 20)
