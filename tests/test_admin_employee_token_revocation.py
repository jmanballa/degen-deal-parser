from __future__ import annotations

import asyncio
import json
import os
import re
import unittest
from contextlib import contextmanager
from datetime import timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave-g-token-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "wave-g-token-hmac-key")
os.environ.setdefault("SESSION_SECRET", "wave-g-session-secret-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "wave-g-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class EmployeeTokenRevocationTests(unittest.TestCase):
    def setUp(self):
        from app import config as cfg
        from app import rate_limit
        from app.db import seed_employee_portal_defaults
        from app.models import EmployeeProfile, User

        cfg.get_settings.cache_clear()
        rate_limit.reset()
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = User(
            id=1,
            username="admin-token",
            password_hash="x",
            password_salt="x",
            role="admin",
            display_name="Admin",
            is_active=True,
        )
        self.employee = User(
            id=2,
            username="employee-token",
            password_hash="hash",
            password_salt="salt",
            role="employee",
            display_name="Employee",
            is_active=True,
        )
        self.session.add_all([self.admin, self.employee])
        self.session.add(EmployeeProfile(user_id=2))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    @contextmanager
    def _managed_session_for_request_user(self):
        session = Session(self.engine)
        try:
            yield session
        finally:
            session.close()

    def _request(self):
        return SimpleNamespace(
            state=SimpleNamespace(current_user=self.admin),
            scope={"session": {}},
            session={},
            headers={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path=f"/team/admin/employees/{self.employee.id}", query=""),
        )

    def _invite(self, token_hash: str, *, used_at=None):
        from app.models import InviteToken, utcnow

        row = InviteToken(
            token_hash=token_hash,
            role="employee",
            created_by_user_id=self.admin.id,
            target_user_id=self.employee.id,
            expires_at=utcnow() + timedelta(hours=1),
            used_at=used_at,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _reset(self, token_hash: str, *, used_at=None):
        from app.models import PasswordResetToken, utcnow

        row = PasswordResetToken(
            token_hash=token_hash,
            user_id=self.employee.id,
            issued_by_user_id=self.admin.id,
            expires_at=utcnow() + timedelta(hours=1),
            used_at=used_at,
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row

    def _terminate(self):
        from app.routers.team_admin_employees import admin_employee_terminate_post

        return asyncio.run(
            admin_employee_terminate_post(
                self._request(),
                self.employee.id,
                session=self.session,
            )
        )

    def _purge(self):
        from app.routers.team_admin_employees import admin_employee_purge_post

        return asyncio.run(
            admin_employee_purge_post(
                self._request(),
                self.employee.id,
                confirm_username="PURGE",
                session=self.session,
            )
        )

    def test_terminate_revokes_outstanding_invite_tokens(self):
        from app.models import InviteToken

        first = self._invite("invite-a")
        second = self._invite("invite-b")
        response = self._terminate()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        self.assertIsNotNone(self.session.get(InviteToken, first.id).used_at)
        self.assertIsNotNone(self.session.get(InviteToken, second.id).used_at)

    def test_terminate_revokes_outstanding_reset_tokens(self):
        from app.models import PasswordResetToken

        first = self._reset("reset-a")
        second = self._reset("reset-b")
        response = self._terminate()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        self.assertIsNotNone(self.session.get(PasswordResetToken, first.id).used_at)
        self.assertIsNotNone(self.session.get(PasswordResetToken, second.id).used_at)

    def test_session_invalidation_timestamp_rejects_stale_active_cookie(self):
        from app.models import User, utcnow
        from app.shared import get_request_user

        self.employee.session_invalidated_at = utcnow()
        self.session.add(self.employee)
        self.session.commit()
        stale_cookie = {
            "user_id": self.employee.id,
            "password_changed_at": None,
            "session_invalidated_at": None,
        }

        with patch("app.shared.managed_session", self._managed_session_for_request_user):
            found = get_request_user(SimpleNamespace(scope={"session": stale_cookie}))

        self.assertIsNone(found)
        self.assertEqual(stale_cookie, {})
        self.assertIsNotNone(self.session.get(User, self.employee.id).session_invalidated_at)

    def test_password_reset_sets_session_invalidation_and_rejects_stale_cookie(self):
        from app.auth import consume_password_reset_token, generate_password_reset_token
        from app.models import User
        from app.shared import get_request_user

        stale_cookie = {
            "user_id": self.employee.id,
            "password_changed_at": None,
            "session_invalidated_at": None,
        }
        raw = generate_password_reset_token(self.session, user_id=self.employee.id)
        consume_password_reset_token(
            self.session,
            raw,
            new_password="NewResetPassword5678!",
        )

        self.session.expire_all()
        user = self.session.get(User, self.employee.id)
        self.assertIsNotNone(user.session_invalidated_at)
        with patch("app.shared.managed_session", self._managed_session_for_request_user):
            found = get_request_user(SimpleNamespace(scope={"session": stale_cookie}))

        self.assertIsNone(found)
        self.assertEqual(stale_cookie, {})

    def test_terminate_sets_session_invalidation_timestamp(self):
        from app.models import User

        response = self._terminate()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        user = self.session.get(User, self.employee.id)
        self.assertFalse(user.is_active)
        self.assertIsNotNone(user.session_invalidated_at)

    def test_terminate_does_not_touch_already_used_tokens(self):
        from app.models import InviteToken, utcnow

        original_used_at = utcnow() - timedelta(minutes=5)
        used = self._invite("invite-used", used_at=original_used_at)
        unused = self._invite("invite-unused")
        response = self._terminate()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        refreshed_used_at = self.session.get(InviteToken, used.id).used_at
        self.assertEqual(refreshed_used_at.replace(tzinfo=timezone.utc), original_used_at)
        self.assertIsNotNone(self.session.get(InviteToken, unused.id).used_at)

    def test_purge_anonymizes_user_email_and_password_hash(self):
        from app.models import User

        response = self._purge()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        user = self.session.get(User, self.employee.id)
        self.assertRegex(user.username, re.compile(r"^purged\+\d+@anonymized\.local$"))
        self.assertEqual(user.password_hash, "__purged_password_hash__")

    def test_purge_revokes_outstanding_tokens(self):
        from app.models import InviteToken, PasswordResetToken

        invite = self._invite("purge-invite")
        reset = self._reset("purge-reset")
        response = self._purge()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()
        self.assertIsNotNone(self.session.get(InviteToken, invite.id).used_at)
        self.assertIsNotNone(self.session.get(PasswordResetToken, reset.id).used_at)

    def test_purge_writes_audit_log_with_token_counts(self):
        from app.models import AuditLog

        self._invite("audit-invite")
        self._reset("audit-reset")
        response = self._purge()
        self.assertEqual(response.status_code, 303)
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purged")
        ).first()
        self.assertIsNotNone(row)
        details = json.loads(row.details_json)
        self.assertEqual(details["invite_tokens_revoked"], 1)
        self.assertEqual(details["reset_tokens_revoked"], 1)

    def test_purge_creates_restorable_tombstone_before_wiping_pii(self):
        from app.models import AuditLog, EmployeeProfile, EmployeePurgeTombstone, User
        from app.pii import decrypt_pii, email_lookup_hash, encrypt_pii
        from app.routers.team_admin_employees import restore_employee_purge_tombstone

        profile = self.session.get(EmployeeProfile, self.employee.id)
        profile.phone_enc = encrypt_pii("555-123-4567")
        profile.email_ciphertext = encrypt_pii("restore@example.com")
        profile.email_lookup_hash = email_lookup_hash("restore@example.com")
        self.employee.display_name = "Restore Target"
        self.session.add_all([profile, self.employee])
        self.session.commit()

        response = self._purge()
        self.assertEqual(response.status_code, 303)
        self.session.expire_all()

        tombstone = self.session.exec(select(EmployeePurgeTombstone)).one()
        self.assertGreater(tombstone.restore_until, tombstone.created_at)
        purged_profile = self.session.get(EmployeeProfile, self.employee.id)
        self.assertIsNone(purged_profile.phone_enc)
        self.assertIsNone(purged_profile.email_ciphertext)

        restore_employee_purge_tombstone(
            self.session,
            self.employee.id,
            actor_user_id=self.admin.id,
            ip_address="testclient",
        )
        self.session.expire_all()

        restored_user = self.session.get(User, self.employee.id)
        restored_profile = self.session.get(EmployeeProfile, self.employee.id)
        self.assertEqual(restored_user.username, "employee-token")
        self.assertEqual(restored_user.display_name, "Restore Target")
        self.assertTrue(restored_user.is_active)
        self.assertIsNotNone(restored_user.session_invalidated_at)
        self.assertEqual(decrypt_pii(restored_profile.phone_enc), "555-123-4567")
        self.assertEqual(
            restored_profile.email_lookup_hash,
            email_lookup_hash("restore@example.com"),
        )
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purge_restored")
        ).first()
        self.assertIsNotNone(row)
