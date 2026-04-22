import base64
import importlib
import os
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlmodel import Session, create_engine, select

# Configure portal env before importing app modules.
os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")


def _reload_settings_and_pii():
    from app import config as cfg
    cfg.get_settings.cache_clear()
    import app.pii as pii
    importlib.reload(pii)
    return pii


class PIIRoundtripTests(unittest.TestCase):
    def test_encrypt_decrypt_roundtrip(self):
        pii = _reload_settings_and_pii()
        blob = pii.encrypt_pii("hello world")
        self.assertIsInstance(blob, bytes)
        self.assertNotEqual(blob, b"hello world")
        self.assertEqual(pii.decrypt_pii(blob), "hello world")

    def test_email_hash_is_deterministic_and_case_insensitive(self):
        pii = _reload_settings_and_pii()
        a = pii.email_lookup_hash("Jane@Example.com")
        b = pii.email_lookup_hash("jane@example.com  ")
        self.assertEqual(a, b)
        self.assertEqual(len(a), 64)

    def test_email_hash_salt_dependent(self):
        pii = _reload_settings_and_pii()
        h1 = pii.email_lookup_hash("x@y.com")
        with patch.object(pii, "_settings") as mock_settings:
            mock_settings.employee_email_hash_salt = "other-salt"
            mock_settings.employee_portal_enabled = True
            h2 = pii.email_lookup_hash("x@y.com")
        self.assertNotEqual(h1, h2)


class RoleRankTests(unittest.TestCase):
    def test_five_tier_ordering(self):
        from app.auth import role_rank
        ranks = [role_rank(r) for r in ("employee", "viewer", "manager", "reviewer", "admin")]
        self.assertEqual(ranks, [1, 2, 3, 4, 5])

    def test_existing_callers_still_work(self):
        from app.auth import has_role
        from app.models import User

        admin = User(username="a", password_hash="x", role="admin")
        reviewer = User(username="r", password_hash="x", role="reviewer")
        viewer = User(username="v", password_hash="x", role="viewer")
        self.assertTrue(has_role(admin, "admin"))
        self.assertTrue(has_role(reviewer, "reviewer"))
        self.assertFalse(has_role(reviewer, "admin"))
        self.assertTrue(has_role(viewer, "viewer"))
        self.assertFalse(has_role(viewer, "reviewer"))


class AuthTokenAndPermissionTests(unittest.TestCase):
    def setUp(self):
        from app.models import SQLModel

        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        # Seed an admin user who issues invites.
        from app.auth import hash_password
        from app.models import User
        h, s = hash_password("adminpw")
        self.admin = User(username="admin", password_hash=h, password_salt=s, role="admin")
        self.session.add(self.admin)
        self.session.commit()
        self.session.refresh(self.admin)

    def tearDown(self):
        self.session.close()

    def test_invite_token_stored_as_bcrypt_not_plaintext(self):
        from app.auth import generate_invite_token
        from app.models import InviteToken

        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        self.assertGreater(len(raw), 32)
        row = self.session.exec(select(InviteToken)).first()
        self.assertIsNotNone(row)
        self.assertNotIn(raw, row.token_hash)
        self.assertTrue(row.token_hash.startswith("$2"))

    def test_consume_invite_happy_path(self):
        from app.auth import consume_invite_token, generate_invite_token
        from app.models import EmployeeProfile, InviteToken

        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        user = consume_invite_token(
            self.session, raw, new_username="jane", new_password="PasswordABC12!"
        )
        self.assertEqual(user.role, "employee")
        profile = self.session.get(EmployeeProfile, user.id)
        self.assertIsNotNone(profile)
        row = self.session.exec(select(InviteToken)).first()
        self.assertIsNotNone(row.used_at)
        self.assertEqual(row.used_by_user_id, user.id)

    def test_consume_invite_rejects_reuse(self):
        from app.auth import consume_invite_token, generate_invite_token

        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        consume_invite_token(
            self.session, raw, new_username="jane", new_password="PasswordABC12!"
        )
        with self.assertRaises(ValueError):
            consume_invite_token(
                self.session, raw, new_username="jane2", new_password="PasswordABC12!"
            )

    def test_consume_invite_rejects_unknown(self):
        from app.auth import consume_invite_token

        with self.assertRaises(ValueError):
            consume_invite_token(
                self.session, "not-a-real-token", new_username="x", new_password="y"
            )

    def test_consume_invite_rejects_expired(self):
        from datetime import timedelta

        from app.auth import consume_invite_token, generate_invite_token
        from app.models import InviteToken, utcnow

        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        row = self.session.exec(select(InviteToken)).first()
        row.expires_at = utcnow() - timedelta(minutes=1)
        self.session.add(row)
        self.session.commit()
        with self.assertRaises(ValueError):
            consume_invite_token(
                self.session, raw, new_username="x", new_password="PasswordABC12!"
            )

    def test_password_reset_roundtrip(self):
        from app.auth import (
            consume_password_reset_token,
            generate_password_reset_token,
            verify_password,
        )

        raw = generate_password_reset_token(self.session, user_id=self.admin.id)
        user = consume_password_reset_token(
            self.session, raw, new_password="BrandNewPass99!"
        )
        self.assertTrue(
            verify_password("BrandNewPass99!", user.password_hash, salt=user.password_salt)
        )

    def test_has_permission_allows_when_row_says_true(self):
        from app.auth import has_permission
        from app.models import RolePermission, User

        emp = User(username="e", password_hash="x", role="employee", is_active=True)
        self.session.add(emp)
        self.session.add(RolePermission(role="employee", resource_key="page.dashboard", is_allowed=True))
        self.session.add(RolePermission(role="employee", resource_key="action.pii.reveal", is_allowed=False))
        self.session.commit()

        self.assertTrue(has_permission(self.session, emp, "page.dashboard"))
        self.assertFalse(has_permission(self.session, emp, "action.pii.reveal"))

    def test_has_permission_falls_back_to_admin_when_no_row(self):
        from app.auth import has_permission
        from app.models import User

        emp = User(username="e2", password_hash="x", role="employee", is_active=True)
        admin = User(username="a2", password_hash="x", role="admin", is_active=True)
        self.session.add(emp)
        self.session.add(admin)
        self.session.commit()
        self.assertFalse(has_permission(self.session, emp, "page.nonexistent"))
        self.assertTrue(has_permission(self.session, admin, "page.nonexistent"))

    def test_has_permission_none_user(self):
        from app.auth import has_permission
        self.assertFalse(has_permission(self.session, None, "page.anything"))


class RateLimiterTests(unittest.TestCase):
    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

    def test_blocks_after_max_requests_in_window(self):
        from app.rate_limit import check

        key = "login:1.2.3.4"
        for _ in range(3):
            self.assertTrue(check(key, max_requests=3, window_seconds=60))
        self.assertFalse(check(key, max_requests=3, window_seconds=60))

    def test_window_expiry_allows_new_requests(self):
        from app.rate_limit import check

        key = "login:1.2.3.5"
        self.assertTrue(check(key, max_requests=1, window_seconds=0.05))
        self.assertFalse(check(key, max_requests=1, window_seconds=0.05))
        time.sleep(0.1)
        self.assertTrue(check(key, max_requests=1, window_seconds=0.05))


class SeedDefaultsTests(unittest.TestCase):
    def test_seed_is_idempotent(self):
        from app.db import seed_employee_portal_defaults
        from app.models import RolePermission, SQLModel

        engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(engine)
        with Session(engine) as session:
            seed_employee_portal_defaults(session)
            count_after_first = len(session.exec(select(RolePermission)).all())
            seed_employee_portal_defaults(session)
            count_after_second = len(session.exec(select(RolePermission)).all())
        self.assertEqual(count_after_first, count_after_second)
        self.assertEqual(count_after_first, 5 * 23)


class AppBootTests(unittest.TestCase):
    def test_refuses_to_boot_without_pii_key(self):
        """Subprocess boot with portal enabled but no key should exit non-zero."""
        env = os.environ.copy()
        env["EMPLOYEE_PORTAL_ENABLED"] = "true"
        env["EMPLOYEE_PII_KEY"] = ""
        env["EMPLOYEE_EMAIL_HASH_SALT"] = "x"
        # Prevent real DB / network side-effects — just import the module.
        code = "import app.pii"
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)

    def test_boots_without_clockify_key(self):
        """Import-level smoke test: no CLOCKIFY_API_KEY required in Wave 1."""
        env = os.environ.copy()
        env["EMPLOYEE_PORTAL_ENABLED"] = "false"
        env.pop("CLOCKIFY_API_KEY", None)
        code = "from app.config import get_settings; get_settings()"
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)


if __name__ == "__main__":
    unittest.main()
