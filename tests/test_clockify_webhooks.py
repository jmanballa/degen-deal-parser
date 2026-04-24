from __future__ import annotations

import importlib
import os
import unittest

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "clockify-webhook-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "clockify-webhook-hmac")
os.environ.setdefault("SESSION_SECRET", "clockify-webhook-session-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "clockify-webhook-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _load_app():
    from app import config as cfg

    cfg.get_settings.cache_clear()
    import app.main as app_main

    importlib.reload(app_main)
    return app_main


class ClockifyWebhookTests(unittest.TestCase):
    def setUp(self):
        os.environ["CLOCKIFY_WEBHOOK_SECRET"] = "test-clockify-secret"
        os.environ["CLOCKIFY_WEBHOOK_SIGNING_SECRET"] = "clockify-signing-secret"
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        self.app_main = _load_app()
        from app.db import get_session as real_get_session

        def _override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _override
        from fastapi.testclient import TestClient

        self.client = TestClient(self.app_main.app)

    def tearDown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        os.environ.pop("CLOCKIFY_WEBHOOK_SECRET", None)
        os.environ.pop("CLOCKIFY_WEBHOOK_SIGNING_SECRET", None)

    def _seed_linked_employee(self):
        from app.models import EmployeeProfile, User

        user = User(
            id=44,
            username="alice",
            password_hash="x",
            password_salt="x",
            display_name="Alice",
            role="employee",
            is_active=True,
        )
        profile = EmployeeProfile(user_id=44, clockify_user_id="clock-user-1")
        self.session.add(user)
        self.session.add(profile)
        self.session.commit()
        return user

    def test_clockify_webhook_rejects_bad_secret(self):
        response = self.client.post(
            "/webhooks/clockify?secret=wrong",
            json={"event": "NEW_TIMER_STARTED"},
        )

        self.assertEqual(response.status_code, 403)

    def test_clockify_webhook_accepts_clockify_signing_secret_header(self):
        response = self.client.post(
            "/webhooks/clockify",
            headers={"X-Clockify-Webhook-Token": "clockify-signing-secret"},
            json={"id": "event-signed", "event": "NEW_TIMER_STARTED"},
        )

        self.assertEqual(response.status_code, 200)

    def test_clockify_webhook_rejects_bad_signing_secret_even_with_url_secret(self):
        response = self.client.post(
            "/webhooks/clockify?secret=test-clockify-secret",
            headers={"X-Clockify-Webhook-Token": "wrong-signing-secret"},
            json={"event": "NEW_TIMER_STARTED"},
        )

        self.assertEqual(response.status_code, 403)

    def test_clockify_webhook_logs_event_and_caches_time_entry(self):
        from app.models import ClockifyTimeEntry, ClockifyWebhookEvent

        self._seed_linked_employee()
        payload = {
            "id": "event-1",
            "event": "NEW_TIMER_STARTED",
            "workspaceId": "workspace",
            "timeEntry": {
                "id": "entry-1",
                "userId": "clock-user-1",
                "description": "Shipping orders",
                "type": "REGULAR",
                "timeInterval": {
                    "start": "2026-04-24T20:00:00Z",
                    "end": None,
                },
            },
        }

        response = self.client.post(
            "/webhooks/clockify?secret=test-clockify-secret",
            json=payload,
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["cached"])
        event = self.session.exec(select(ClockifyWebhookEvent)).one()
        self.assertEqual(event.event_type, "NEW_TIMER_STARTED")
        entry = self.session.exec(select(ClockifyTimeEntry)).one()
        self.assertEqual(entry.clockify_entry_id, "entry-1")
        self.assertEqual(entry.clockify_user_id, "clock-user-1")
        self.assertEqual(entry.user_id, 44)
        self.assertTrue(entry.is_running)
        self.assertFalse(entry.is_deleted)

    def test_clockify_webhook_dedupes_retries(self):
        from app.models import ClockifyWebhookEvent

        payload = {
            "id": "event-retry",
            "event": "NEW_TIME_ENTRY",
            "timeEntry": {
                "id": "entry-retry",
                "userId": "clock-user-1",
                "timeInterval": {
                    "start": "2026-04-24T20:00:00Z",
                    "end": "2026-04-24T21:00:00Z",
                },
            },
        }

        first = self.client.post(
            "/webhooks/clockify?secret=test-clockify-secret",
            json=payload,
        )
        second = self.client.post(
            "/webhooks/clockify?secret=test-clockify-secret",
            json=payload,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.json()["duplicate"])
        events = self.session.exec(select(ClockifyWebhookEvent)).all()
        self.assertEqual(len(events), 1)

    def test_clockify_delete_webhook_marks_cached_entry_deleted(self):
        from app.models import ClockifyTimeEntry

        start_payload = {
            "id": "event-create",
            "event": "NEW_TIME_ENTRY",
            "timeEntry": {
                "id": "entry-delete",
                "userId": "clock-user-1",
                "timeInterval": {
                    "start": "2026-04-24T20:00:00Z",
                    "end": "2026-04-24T21:00:00Z",
                },
            },
        }
        delete_payload = {
            "id": "event-delete",
            "event": "TIME_ENTRY_DELETED",
            "timeEntryId": "entry-delete",
        }

        self.client.post(
            "/webhooks/clockify?secret=test-clockify-secret",
            json=start_payload,
        )
        response = self.client.post(
            "/webhooks/clockify?secret=test-clockify-secret",
            json=delete_payload,
        )

        self.assertEqual(response.status_code, 200)
        entry = self.session.exec(select(ClockifyTimeEntry)).one()
        self.assertTrue(entry.is_deleted)
        self.assertFalse(entry.is_running)
