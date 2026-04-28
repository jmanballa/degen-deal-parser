from __future__ import annotations

import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "invite-sms-email-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "invite-sms-token-hmac")
os.environ.setdefault("SESSION_SECRET", "invite-sms-session-secret-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "invite-sms-admin-password")


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
    def __init__(self, current_user):
        self.state = SimpleNamespace(current_user=current_user)
        self.scope = {"session": {}}
        self.session: dict[str, str] = {}
        self.headers: dict[str, str] = {}
        self.client = SimpleNamespace(host="testclient")
        self.url = SimpleNamespace(
            path="/team/admin/employees/2",
            scheme="http",
            netloc="testserver",
        )


class EmployeeInviteSmsTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app import rate_limit
        from app.db import seed_employee_portal_defaults
        from app.models import User

        cfg.get_settings.cache_clear()
        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = User(
            id=1,
            username="sms-admin",
            password_hash="x",
            password_salt="x",
            display_name="SMS Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def _request(self):
        return _FakeRequest(self.admin)

    def _settings(self):
        return SimpleNamespace(
            public_base_url="https://team.example.test",
            sms_provider="dry_run",
            sms_from_number="",
            sms_twilio_account_sid="",
            sms_twilio_auth_token="",
            sms_twilio_messaging_service_sid="",
            sms_timeout_seconds=1,
        )

    def _draft_with_phone(self, phone: str):
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile
        from app.pii import encrypt_pii

        draft = create_draft_employee(
            self.session,
            created_by_user_id=self.admin.id,
            display_name="Invite SMS Person",
        )
        profile = self.session.get(EmployeeProfile, draft.id)
        profile.phone_enc = encrypt_pii(phone)
        self.session.add(profile)
        self.session.commit()
        return draft

    def test_text_invite_creates_unique_token_and_sends_safe_sms(self):
        from app.models import AuditLog, InviteToken
        from app.routers import team_admin_employees as mod
        from app.sms import SmsSendResult

        draft = self._draft_with_phone("(555) 867-5309")
        sent: dict[str, str] = {}

        def fake_send_sms(*, to_phone, body, settings=None):
            sent["to_phone"] = to_phone
            sent["body"] = body
            return SmsSendResult(provider="dry_run", status="dry_run", dry_run=True)

        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_sms", side_effect=fake_send_sms
        ):
            response = asyncio.run(
                mod.admin_employee_text_invite(
                    self._request(),
                    draft.id,
                    session=self.session,
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(sent["to_phone"], "+15558675309")
        self.assertIn("https://team.example.test/team/invite/accept/", sent["body"])

        invites = list(
            self.session.exec(
                select(InviteToken).where(InviteToken.target_user_id == draft.id)
            ).all()
        )
        self.assertEqual(len(invites), 1)

        audit_rows = list(
            self.session.exec(
                select(AuditLog).where(AuditLog.target_user_id == draft.id)
            ).all()
        )
        actions = {row.action for row in audit_rows}
        self.assertIn("pii.use_for_invite_sms", actions)
        self.assertIn("invite.issued_for_draft", actions)
        self.assertIn("invite.text_dry_run", actions)
        details_blob = "\n".join(row.details_json for row in audit_rows)
        self.assertNotIn("https://team.example.test/team/invite/accept/", details_blob)
        self.assertNotIn("5558675309", details_blob)
        self.assertIn("***-***-5309", details_blob)

        text_audit = next(row for row in audit_rows if row.action == "invite.text_dry_run")
        details = json.loads(text_audit.details_json)
        self.assertTrue(details["dry_run"])
        self.assertTrue(details["success"])
        self.assertEqual(details["phone"], "***-***-5309")
        self.assertIn("phone_fingerprint", details)

    def test_text_invite_rejects_invalid_phone_without_issuing_token(self):
        from app.models import AuditLog, InviteToken
        from app.routers import team_admin_employees as mod

        draft = self._draft_with_phone("not a phone")
        with patch.object(mod, "get_settings", return_value=self._settings()), patch.object(
            mod, "send_sms"
        ) as send_sms:
            response = asyncio.run(
                mod.admin_employee_text_invite(
                    self._request(),
                    draft.id,
                    session=self.session,
                )
            )

        self.assertEqual(response.status_code, 303)
        send_sms.assert_not_called()
        invites = list(
            self.session.exec(
                select(InviteToken).where(InviteToken.target_user_id == draft.id)
            ).all()
        )
        self.assertEqual(invites, [])
        failure = self.session.exec(
            select(AuditLog).where(
                AuditLog.target_user_id == draft.id,
                AuditLog.action == "invite.text_failed",
            )
        ).first()
        self.assertIsNotNone(failure)
        self.assertEqual(json.loads(failure.details_json)["reason"], "invalid_phone")

    def test_employee_list_exposes_text_invite_for_drafts_with_phone(self):
        from app.routers.team_admin_employees import admin_employees_list

        draft = self._draft_with_phone("555-867-5309")
        response = admin_employees_list(
            self._request(),
            q=None,
            flash=None,
            show_inactive=None,
            session=self.session,
        )

        self.assertEqual(response.status_code, 200)
        html = response.body.decode("utf-8")
        self.assertIn(f"/team/admin/employees/{draft.id}/text-invite", html)
        self.assertIn("Text invite", html)


if __name__ == "__main__":
    unittest.main()
