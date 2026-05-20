"""Wave 1.5 hardening tests — M1 (HMAC indexed token lookup) + M2 (boot-time salt)."""
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")


class TokenHmacLookupTests(unittest.TestCase):
    def setUp(self):
        from app.models import SQLModel, User
        from app.auth import hash_password

        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        h, s = hash_password("adminpw")
        self.admin = User(username="admin", password_hash=h, password_salt=s, role="admin")
        self.session.add(self.admin)
        self.session.commit()
        self.session.refresh(self.admin)

    def tearDown(self):
        self.session.close()

    def test_generate_invite_populates_both_hash_and_hmac(self):
        from app.auth import generate_invite_token
        from app.models import InviteToken

        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        row = self.session.exec(select(InviteToken)).first()
        self.assertTrue(row.token_hash.startswith("$2"))
        self.assertIsInstance(row.token_lookup_hmac, (bytes, bytearray, memoryview))
        self.assertEqual(len(bytes(row.token_lookup_hmac)), 32)  # SHA-256
        # HMAC must be derived from raw token (deterministic).
        from app.auth import _token_lookup_hmac
        self.assertEqual(bytes(row.token_lookup_hmac), _token_lookup_hmac(raw))

    def test_consume_invite_wrong_token_does_zero_bcrypt(self):
        from app import auth
        from app.auth import consume_invite_token, generate_invite_token

        generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        with patch.object(auth, "_verify_token", wraps=auth._verify_token) as spy:
            with self.assertRaises(ValueError):
                consume_invite_token(
                    self.session, "nope-not-real", new_username="x", new_password="y"
                )
            self.assertEqual(spy.call_count, 0)

    def test_consume_invite_correct_token_does_one_bcrypt(self):
        from app import auth
        from app.auth import consume_invite_token, generate_invite_token

        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=self.admin.id
        )
        with patch.object(auth, "_verify_token", wraps=auth._verify_token) as spy:
            consume_invite_token(
                self.session, raw, new_username="jane", new_password="PasswordABC12!"
            )
            self.assertEqual(spy.call_count, 1)

    def test_consume_reset_wrong_token_does_zero_bcrypt(self):
        from app import auth
        from app.auth import consume_password_reset_token, generate_password_reset_token

        generate_password_reset_token(self.session, user_id=self.admin.id)
        with patch.object(auth, "_verify_token", wraps=auth._verify_token) as spy:
            with self.assertRaises(ValueError):
                consume_password_reset_token(self.session, "nope", new_password="x")
            self.assertEqual(spy.call_count, 0)

    def test_null_hmac_row_cannot_be_consumed(self):
        """Legacy / back-filled rows with NULL token_lookup_hmac are unreachable."""
        from datetime import timedelta

        from app.auth import consume_password_reset_token
        from app.models import PasswordResetToken, utcnow

        # Insert a row with a known raw token but NULL HMAC (simulating legacy row).
        import bcrypt
        raw = "legacy-raw-token"
        row = PasswordResetToken(
            token_hash=bcrypt.hashpw(raw.encode(), bcrypt.gensalt(rounds=4)).decode(),
            token_lookup_hmac=None,
            user_id=self.admin.id,
            expires_at=utcnow() + timedelta(hours=1),
        )
        self.session.add(row)
        self.session.commit()

        with self.assertRaises(ValueError):
            consume_password_reset_token(self.session, raw, new_password="NewPass12!")

    def test_prior_reset_tokens_invalidated_on_new_issuance(self):
        from app.auth import generate_password_reset_token
        from app.models import PasswordResetToken

        generate_password_reset_token(self.session, user_id=self.admin.id)
        generate_password_reset_token(self.session, user_id=self.admin.id)
        rows = self.session.exec(
            select(PasswordResetToken).where(PasswordResetToken.user_id == self.admin.id)
        ).all()
        self.assertEqual(len(rows), 2)
        unused = [r for r in rows if r.used_at is None]
        self.assertEqual(len(unused), 1)


class BootTimeSaltValidationTests(unittest.TestCase):
    def test_boot_fails_when_portal_enabled_without_salt(self):
        env = os.environ.copy()
        env["EMPLOYEE_PORTAL_ENABLED"] = "true"
        env["EMPLOYEE_PII_KEY"] = Fernet.generate_key().decode("ascii")
        env["EMPLOYEE_EMAIL_HASH_SALT"] = ""
        code = "import app.team.pii"
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("EMPLOYEE_EMAIL_HASH_SALT", (result.stderr or "") + (result.stdout or ""))

    def test_boot_succeeds_when_both_set(self):
        env = os.environ.copy()
        env["EMPLOYEE_PORTAL_ENABLED"] = "true"
        env["EMPLOYEE_PII_KEY"] = Fernet.generate_key().decode("ascii")
        env["EMPLOYEE_EMAIL_HASH_SALT"] = "boot-test-salt"
        code = "import app.team.pii"
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)


if __name__ == "__main__":
    unittest.main()
