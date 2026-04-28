"""Wave 3.5 — hardening: audit logs, logout POST, admin short-circuit, password."""
from __future__ import annotations

import json
import os
import unittest
import asyncio
import hashlib
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlmodel import select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")
os.environ.setdefault("SESSION_SECRET", "test-secret-wave35-" + "x" * 32)
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "test-token-hmac-wave35-" + "x" * 24)

from tests.test_employee_portal_wave3 import _PortalHarness  # noqa: E402


class AuditLogOnAuthEventsTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def _count(self, action: str) -> int:
        from app.models import AuditLog

        return len(
            self.session.exec(select(AuditLog).where(AuditLog.action == action)).all()
        )

    def test_authenticate_failure_writes_login_failed(self):
        from app.auth import authenticate_user

        result = authenticate_user(self.session, "nobody", "x")
        self.assertIsNone(result)
        self.assertGreaterEqual(self._count("login.failed"), 1)

    def test_authenticate_success_writes_login_succeeded(self):
        from app.auth import authenticate_user, hash_password
        from app.models import User

        ph, salt = hash_password("SuperSecret9!")
        self.session.add(User(
            username="audit_ok", password_hash=ph, password_salt=salt,
            display_name="A", role="employee", is_active=True,
        ))
        self.session.commit()
        u = authenticate_user(self.session, "audit_ok", "SuperSecret9!")
        self.assertIsNotNone(u)
        self.assertGreaterEqual(self._count("login.succeeded"), 1)

    def test_failed_login_does_not_leak_password(self):
        from app.auth import authenticate_user
        from app.models import AuditLog

        authenticate_user(self.session, "whoever", "SuperSecretValue123!")
        rows = self.session.exec(
            select(AuditLog).where(AuditLog.action == "login.failed")
        ).all()
        for row in rows:
            self.assertNotIn("SuperSecretValue123!", row.details_json or "")

    def test_invite_accept_writes_audit(self):
        from app.auth import generate_invite_token, hash_password
        from app.models import User

        ph, salt = hash_password("AdminPass1!")
        admin = User(username="adm2", password_hash=ph, password_salt=salt,
                     display_name="A", role="admin", is_active=True)
        self.session.add(admin)
        self.session.commit()
        self.session.refresh(admin)
        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=admin.id,
            email_hint="h@example.com",
        )
        csrf = self._csrf()
        r = self.client.post(
            f"/team/invite/accept/{raw}",
            data={
                "new_username": "auditee",
                "new_password": "StrongPass9#xy",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertGreaterEqual(self._count("account.invite_accepted"), 1)

    def test_password_reset_consume_writes_audit(self):
        from app.auth import generate_password_reset_token, hash_password
        from app.models import User

        ph, salt = hash_password("OldPass9!!!!")
        u = User(username="resetaud", password_hash=ph, password_salt=salt,
                 display_name="R", role="employee", is_active=True)
        self.session.add(u)
        self.session.commit()
        self.session.refresh(u)
        raw = generate_password_reset_token(self.session, user_id=u.id)
        csrf = self._csrf()
        r = self.client.post(
            f"/team/password/reset/{raw}",
            data={"new_password": "NewStrong#Pass9", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertGreaterEqual(self._count("password.reset_consumed"), 1)

    def test_forgot_confirmation_is_neutral_and_queues_matched_accounts(self):
        from app.auth import hash_password
        from app.models import AuditLog, User

        ph, salt = hash_password("xxxxx")
        real = User(
            username="realuser", password_hash=ph, password_salt=salt,
            display_name="R", role="employee", is_active=True,
        )
        self.session.add(real)
        self.session.commit()
        self.session.refresh(real)
        from app.routers.team import team_password_forgot_post

        request = SimpleNamespace(
            client=SimpleNamespace(host="testclient"),
            headers={},
            url=SimpleNamespace(scheme="http", netloc="testserver"),
        )
        for probe in ("realuser", "doesnotexist"):
            r = asyncio.run(
                team_password_forgot_post(
                    request, identifier=probe, session=self.session
                )
            )
            self.assertEqual(r.status_code, 303)
            self.assertIn("If+that+account+exists", r.headers["location"])
        rows = self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_requested")
        ).all()
        http_rows = [
            r for r in rows
            if "http_forgot" in (r.details_json or "")
        ]
        self.assertGreaterEqual(len(http_rows), 2)
        identifier_hashes = []
        for row in http_rows:
            self.assertIsNone(row.target_user_id)
            details = json.loads(row.details_json)
            self.assertEqual(details["status"], "accepted")
            self.assertNotIn("matched", details)
            self.assertNotIn("delivery", details)
            self.assertRegex(details["identifier_hash"], r"^[0-9a-f]{64}$")
            identifier_hashes.append(details["identifier_hash"])
            self.assertNotIn("realuser", row.details_json or "")
        self.assertNotIn(hashlib.sha256(b"realuser").hexdigest(), identifier_hashes)
        manager_rows = self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_manager_request")
        ).all()
        self.assertEqual(len(manager_rows), 1)
        self.assertEqual(manager_rows[0].target_user_id, real.id)
        self.assertNotIn("realuser", manager_rows[0].details_json or "")


class LogoutPostTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def test_get_logout_returns_405(self):
        r = self.client.get("/team/logout", follow_redirects=False)
        self.assertEqual(r.status_code, 405)

    def test_post_logout_without_csrf_is_403(self):
        r = self.client.post("/team/logout", follow_redirects=False)
        self.assertEqual(r.status_code, 403)

    def test_post_logout_with_csrf_clears_session(self):
        csrf = self._csrf()
        r = self.client.post(
            "/team/logout", data={"csrf_token": csrf}, follow_redirects=False
        )
        self.assertEqual(r.status_code, 303)
        self.assertIn("/team/login", r.headers["location"])


class AdminShortCircuitTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def _admin(self):
        from app.models import User

        u = User(id=500, username="adm_sc", password_hash="x", password_salt="x",
                 display_name="A", role="admin", is_active=True)
        self.session.add(u)
        self.session.commit()
        return u

    def test_admin_no_row_is_allowed(self):
        from app.auth import has_permission
        from app.models import RolePermission

        admin = self._admin()
        # Delete any seeded row for a synthetic key to emulate "no row".
        self.session.exec(select(RolePermission))  # noop
        self.assertTrue(has_permission(self.session, admin, "synthetic.key.none"))

    def test_admin_explicit_false_is_denied(self):
        from app.auth import has_permission
        from app.models import RolePermission

        admin = self._admin()
        self.session.add(RolePermission(
            role="admin", resource_key="synthetic.deny", is_allowed=False,
        ))
        self.session.commit()
        self.assertFalse(has_permission(self.session, admin, "synthetic.deny"))


class MatrixDrivenAccessTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def test_manager_with_dashboard_lands_on_team_root(self):
        # Seeded matrix has manager.page.dashboard=True.
        self._login_as("manager", user_id=60, username="mgr_t")
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 200)

    def test_viewer_denied_dashboard_gets_403(self):
        from app.models import RolePermission

        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "viewer",
                RolePermission.resource_key == "page.dashboard",
            )
        ).first()
        if row is not None:
            row.is_allowed = False
            self.session.add(row)
            self.session.commit()
        self._login_as("viewer", user_id=61, username="v_t2")
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 403)


class PasswordValidationTightenedTests(unittest.TestCase):
    def test_whitespace_rejected(self):
        from app.auth import validate_password_strength

        problems = validate_password_strength("Strong Pass9#xy")
        self.assertTrue(any("whitespace" in p.lower() or "space" in p.lower()
                            for p in problems))

    def test_only_alnum_no_symbol_rejected(self):
        from app.auth import validate_password_strength

        # 12 chars, upper+lower+digit only (no symbol) → classes=3. This was
        # accepted before; the old bug was that e.g. whitespace counted as
        # "symbol". Verify whitespace no longer counts.
        problems = validate_password_strength("Abcdefghij 9")  # includes space
        self.assertTrue(problems)

    def test_strong_password_accepted(self):
        from app.auth import validate_password_strength

        self.assertEqual(validate_password_strength("StrongPass9#xy"), [])


if __name__ == "__main__":
    unittest.main()
