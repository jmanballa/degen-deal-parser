"""Draft-employee flow tests (wave 4.6).

Covers the "admin creates employee profile first, then sends invite later"
flow so Jeffrey can schedule people before they've registered.

The draft flow adds:
- `create_draft_employee` — inactive User + seeded EmployeeProfile
- `InviteToken.target_user_id` — binds an invite to an existing draft row
- `consume_invite_token` hydrates the existing user/profile instead of
  creating new rows when `target_user_id` is set

These tests reuse the same harness style as
`test_employee_portal_pii_capture.py` so CSRF + session handling behave
consistently with the rest of the portal suite.
"""
from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt-draft")
os.environ.setdefault("SESSION_SECRET", "test-secret-draft-" + "x" * 32)


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


class _DraftHarness:
    def _setup(self):
        from app import rate_limit
        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
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

        from app import shared
        import app.main as app_main

        self._default_user_patcher_shared = patch.object(
            shared, "get_request_user", return_value=None
        )
        self._default_user_patcher_shared.start()
        self._default_user_patcher_main = patch.object(
            app_main, "get_request_user", return_value=None
        )
        self._default_user_patcher_main.start()

    def _teardown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in (
            "_patcher_shared",
            "_patcher_main",
            "_default_user_patcher_shared",
            "_default_user_patcher_main",
        ):
            patcher = getattr(self, attr, None)
            if patcher:
                patcher.stop()
                setattr(self, attr, None)

    def _login_as(self, role: str, *, user_id: int = 500, username: str = "adm_draft"):
        from app import shared
        from app.models import User
        import app.main as app_main

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        for attr in ("_default_user_patcher_shared", "_default_user_patcher_main"):
            p = getattr(self, attr, None)
            if p:
                p.stop()
                setattr(self, attr, None)
        self._patcher_shared = patch.object(shared, "get_request_user", return_value=u)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=u)
        self._patcher_main.start()
        if self.session.get(User, user_id) is None:
            self.session.add(u)
            self.session.commit()
        return u

    def _csrf(self) -> str:
        marker = 'name="csrf_token" value="'
        for path in ("/team/login", "/team/password/forgot"):
            r = self.client.get(path, follow_redirects=False)
            if marker in r.text:
                idx = r.text.index(marker) + len(marker)
                end = r.text.index('"', idx)
                return r.text[idx:end]
        raise AssertionError("no csrf token rendered")


class CreateDraftEmployeeTests(unittest.TestCase, _DraftHarness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_create_draft_encrypts_legal_name_and_marks_inactive(self):
        from app.auth import create_draft_employee, is_draft_user
        from app.models import EmployeeProfile, User
        from app.pii import decrypt_pii

        admin = self._login_as("admin")
        user = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Jane Elizabeth Doe",
            preferred_name="Jane",
            role="employee",
        )
        self.assertIsNotNone(user.id)
        self.assertFalse(user.is_active, "draft must start inactive")
        self.assertEqual(user.password_hash, "", "draft has no password yet")
        self.assertEqual(user.display_name, "Jane")
        self.assertTrue(
            user.username.startswith("__draft_"),
            f"placeholder username should be synthetic, got {user.username!r}",
        )
        self.assertTrue(is_draft_user(user))

        profile = self.session.get(EmployeeProfile, user.id)
        self.assertIsNotNone(profile, "profile must be created alongside draft user")
        self.assertIsNotNone(profile.legal_name_enc, "legal_name must be stored")
        self.assertNotEqual(
            profile.legal_name_enc,
            b"Jane Elizabeth Doe",
            "legal_name must be encrypted, not plaintext",
        )
        self.assertEqual(decrypt_pii(profile.legal_name_enc), "Jane Elizabeth Doe")

    def test_create_draft_requires_name(self):
        # Wave 4.7: display_name is now the canonical required field.
        # legal_name is optional and can be added later by the employee
        # during onboarding. We still accept legal_name as a fallback if
        # no display_name is given, so passing just a legal_name works.
        from app.auth import create_draft_employee

        admin = self._login_as("admin")
        with self.assertRaises(ValueError) as cm:
            create_draft_employee(
                self.session,
                created_by_user_id=admin.id,
                display_name="",
                legal_name="   ",
            )
        self.assertEqual(str(cm.exception), "draft_display_name_required")

    def test_draft_cannot_log_in(self):
        from app.auth import authenticate_user, create_draft_employee

        admin = self._login_as("admin")
        user = create_draft_employee(
            self.session, created_by_user_id=admin.id, legal_name="Can't Log In"
        )
        # Placeholder username + empty password → must fail cleanly.
        self.assertIsNone(
            authenticate_user(self.session, user.username, ""),
            "draft with empty password must not authenticate",
        )
        self.assertIsNone(
            authenticate_user(self.session, user.username, "anything"),
            "draft with any password must not authenticate (is_active=False)",
        )


class IssueInviteForDraftTests(unittest.TestCase, _DraftHarness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_invite_persists_target_user_id_and_revokes_prior(self):
        from app.auth import create_draft_employee, generate_invite_token
        from app.models import InviteToken

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session, created_by_user_id=admin.id, legal_name="Target Person"
        )

        raw1 = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=admin.id,
            target_user_id=draft.id,
        )
        raw2 = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=admin.id,
            target_user_id=draft.id,
        )
        self.assertNotEqual(raw1, raw2)

        rows = list(
            self.session.exec(
                select(InviteToken).where(InviteToken.target_user_id == draft.id)
            ).all()
        )
        self.assertEqual(len(rows), 2)
        live = [r for r in rows if r.used_at is None]
        self.assertEqual(len(live), 1, "issuing a second invite must invalidate the first")

    def test_cannot_issue_invite_for_active_user(self):
        from app.auth import generate_invite_token, hash_password
        from app.models import User

        admin = self._login_as("admin")
        ph, salt = hash_password("AlreadyHere1!")
        real = User(
            username="already_here",
            password_hash=ph,
            password_salt=salt,
            display_name="Already",
            role="employee",
            is_active=True,
        )
        self.session.add(real)
        self.session.commit()
        self.session.refresh(real)

        with self.assertRaises(ValueError) as cm:
            generate_invite_token(
                self.session,
                role="employee",
                created_by_user_id=admin.id,
                target_user_id=real.id,
            )
        self.assertEqual(str(cm.exception), "invite_target_already_registered")


class ConsumeInviteForDraftTests(unittest.TestCase, _DraftHarness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_consume_hydrates_same_user_row(self):
        """The draft's user_id must survive. This is the whole point —
        scheduling code can reference the ID right now and have it still
        be valid after onboarding."""
        from app.auth import (
            authenticate_user,
            consume_invite_token,
            create_draft_employee,
            generate_invite_token,
        )
        from app.models import User

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Stable Id Person",
            role="employee",
        )
        draft_id = draft.id
        placeholder_username = draft.username

        raw = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=admin.id,
            target_user_id=draft_id,
        )

        user = consume_invite_token(
            self.session,
            raw,
            new_username="stable_id",
            new_password="StrongPass9#xy",
            preferred_name="Stable",
        )
        self.assertEqual(user.id, draft_id, "user_id must be stable across onboarding")
        self.assertEqual(user.username, "stable_id")
        self.assertTrue(user.is_active)
        self.assertNotEqual(user.username, placeholder_username)

        # No duplicate User rows for the placeholder username.
        still_there = self.session.exec(
            select(User).where(User.username == placeholder_username)
        ).first()
        self.assertIsNone(
            still_there,
            "placeholder username must have been replaced, not left behind",
        )

        # They can now log in.
        logged_in = authenticate_user(self.session, "stable_id", "StrongPass9#xy")
        self.assertIsNotNone(logged_in)
        self.assertEqual(logged_in.id, draft_id)

    def test_admin_seeded_legal_name_survives_when_employee_leaves_it_blank(self):
        from app.auth import (
            consume_invite_token,
            create_draft_employee,
            generate_invite_token,
        )
        from app.models import EmployeeProfile
        from app.pii import decrypt_pii

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Official Legal Name",
        )
        raw = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=admin.id,
            target_user_id=draft.id,
        )

        user = consume_invite_token(
            self.session,
            raw,
            new_username="legal_survives",
            new_password="StrongPass9#xy",
            # NOTE: legal_name intentionally omitted.
        )
        profile = self.session.get(EmployeeProfile, user.id)
        self.assertIsNotNone(profile)
        self.assertEqual(
            decrypt_pii(profile.legal_name_enc),
            "Official Legal Name",
            "admin-seeded legal_name must survive when employee leaves it blank",
        )

    def test_employee_can_overwrite_legal_name_during_onboarding(self):
        from app.auth import (
            consume_invite_token,
            create_draft_employee,
            generate_invite_token,
        )
        from app.models import EmployeeProfile
        from app.pii import decrypt_pii

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Admin Typed This",
        )
        raw = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=admin.id,
            target_user_id=draft.id,
        )

        user = consume_invite_token(
            self.session,
            raw,
            new_username="overwriter",
            new_password="StrongPass9#xy",
            legal_name="Employee Typed This Correct Spelling",
        )
        profile = self.session.get(EmployeeProfile, user.id)
        self.assertEqual(
            decrypt_pii(profile.legal_name_enc),
            "Employee Typed This Correct Spelling",
        )

    def test_draft_email_does_not_clash_with_itself(self):
        """Admin may seed an email at draft creation. The SAME email
        re-entered during onboarding must NOT be treated as a clash with
        the draft's own existing profile."""
        from app.auth import (
            consume_invite_token,
            create_draft_employee,
            generate_invite_token,
        )

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Email Person",
            email="person@example.com",
        )
        raw = generate_invite_token(
            self.session,
            role="employee",
            created_by_user_id=admin.id,
            target_user_id=draft.id,
        )
        user = consume_invite_token(
            self.session,
            raw,
            new_username="emailperson",
            new_password="StrongPass9#xy",
            email="person@example.com",
        )
        self.assertEqual(user.id, draft.id)


class AdminDraftUITests(unittest.TestCase, _DraftHarness):
    """HTTP-level: admin can create a draft and send an invite through
    the UI without 500-ing."""

    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_admin_can_create_draft_via_http(self):
        from app.models import User, EmployeeProfile
        from app.pii import decrypt_pii

        self._login_as("admin")
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/new",
            data={
                "legal_name": "Http Created Person",
                "preferred_name": "Http",
                "role": "employee",
                "email": "",
                "hire_date": "",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303, r.text)
        self.assertIn("/team/admin/employees/", r.headers["location"])

        created = list(
            self.session.exec(
                select(User).where(User.display_name == "Http")
            ).all()
        )
        self.assertEqual(len(created), 1)
        prof = self.session.get(EmployeeProfile, created[0].id)
        self.assertEqual(decrypt_pii(prof.legal_name_enc), "Http Created Person")

    def test_admin_can_send_invite_for_draft_via_http(self):
        from app.auth import create_draft_employee
        from app.models import InviteToken

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Send Invite Person",
        )
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{draft.id}/send-invite",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertIn("/team/invite/accept/", r.text)

        rows = list(
            self.session.exec(
                select(InviteToken).where(InviteToken.target_user_id == draft.id)
            ).all()
        )
        self.assertEqual(len(rows), 1)

    def test_admin_employees_list_shows_draft_pill(self):
        from app.auth import create_draft_employee

        admin = self._login_as("admin")
        create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Pill Person",
        )
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        # Legal name is rendered for drafts (decrypted server-side), and
        # the status pill says "draft" (not "active").
        self.assertIn("Pill Person", r.text)
        self.assertIn(">draft<", r.text)


if __name__ == "__main__":
    unittest.main()
