"""Wave 3 — employee portal: auth flows, dashboard, profile, policies, supply."""
from __future__ import annotations

import importlib
import json
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")
os.environ.setdefault("SESSION_SECRET", "test-secret-wave3-" + "x" * 32)


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _load_app_with_portal(enabled: bool):
    prev = os.environ.get("EMPLOYEE_PORTAL_ENABLED")
    os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true" if enabled else "false"
    from app import config as cfg
    cfg.get_settings.cache_clear()
    import app.main as app_main
    importlib.reload(app_main)
    if prev is None:
        os.environ.pop("EMPLOYEE_PORTAL_ENABLED", None)
    else:
        os.environ["EMPLOYEE_PORTAL_ENABLED"] = prev
    return app_main


class _PortalHarness:
    """Shared setup: fresh in-memory DB + TestClient with dep override."""

    def _setup_portal(self):
        from app import rate_limit
        rate_limit.reset()
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults

        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        self.app_main = _load_app_with_portal(True)
        from app.db import get_session as real_get_session

        def _session_override():
            s = Session(self.engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _session_override
        from fastapi.testclient import TestClient

        self.client = TestClient(self.app_main.app)

    def _teardown_portal(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in ("_user_patcher", "_user_patcher_main"):
            patcher = getattr(self, attr, None)
            if patcher:
                patcher.stop()
                setattr(self, attr, None)

    def _login_as(self, role: str, user_id: int = 10, username: str = "emp_t"):
        from app import shared
        from app.models import User

        user = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        # Patch BOTH shared and main — the middleware pulls via `from .shared import *`
        # which creates a bound name in app.main.
        import app.main as app_main

        self._user_patcher = patch.object(shared, "get_request_user", return_value=user)
        self._user_patcher.start()
        self._user_patcher_main = patch.object(app_main, "get_request_user", return_value=user)
        self._user_patcher_main.start()
        return user

    def _csrf(self) -> str:
        # Hit a page that issues a CSRF token, then extract it from the
        # starlette session cookie is not straightforward — instead, grab
        # the token from the rendered HTML.
        r = self.client.get("/team/login")
        marker = 'name="csrf_token" value="'
        if marker not in r.text:
            # Page may have redirected (already logged in). Fall back to
            # forgot page which also renders a fresh token.
            r = self.client.get("/team/password/forgot")
        idx = r.text.index(marker) + len(marker)
        end = r.text.index('"', idx)
        return r.text[idx:end]


class TeamLoginTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def test_login_page_renders(self):
        r = self.client.get("/team/login")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Sign in", r.text)

    def test_login_wrong_creds_redirects_with_error(self):
        csrf = self._csrf()
        r = self.client.post(
            "/team/login",
            data={"username": "nobody", "password": "nopass", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertIn("error", r.headers["location"])

    def test_login_success_routes_employee_to_team(self):
        from app.auth import hash_password
        from app.models import User

        ph, salt = hash_password("SuperSecret9!")
        self.session.add(User(
            username="emp1", password_hash=ph, password_salt=salt,
            display_name="E1", role="employee", is_active=True,
        ))
        self.session.commit()
        csrf = self._csrf()
        r = self.client.post(
            "/team/login",
            data={"username": "emp1", "password": "SuperSecret9!", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/team/")

    def test_login_rate_limit_after_5_attempts(self):
        csrf = self._csrf()
        for _ in range(5):
            r = self.client.post(
                "/team/login",
                data={"username": "x", "password": "y", "csrf_token": csrf},
                follow_redirects=False,
            )
            self.assertEqual(r.status_code, 303)
        r = self.client.post(
            "/team/login",
            data={"username": "x", "password": "y", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 429)


class AuthGatingTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def test_team_root_without_session_redirects_to_login(self):
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertTrue(r.headers["location"].endswith("/team/login"))

    def test_team_root_with_viewer_no_permission_returns_403(self):
        # Revoke viewer's page.dashboard so the matrix denies the page.
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
        self._login_as("viewer", user_id=20, username="v_t")
        r = self.client.get("/team/", follow_redirects=False)
        self.assertEqual(r.status_code, 403)

    def test_team_root_with_employee_renders_dashboard(self):
        self._login_as("employee", user_id=30, username="e_t")
        r = self.client.get("/team/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Dashboard", r.text)

    def test_portal_disabled_returns_404(self):
        self._teardown_portal()
        self.app_main = _load_app_with_portal(False)
        from fastapi.testclient import TestClient

        client = TestClient(self.app_main.app)
        r = client.get("/team/login", follow_redirects=False)
        self.assertEqual(r.status_code, 404)
        # Restore for tearDown safety.
        self._setup_portal()


class PasswordResetTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def _issue_reset(self) -> tuple[int, str]:
        from app.auth import generate_password_reset_token, hash_password
        from app.models import User

        ph, salt = hash_password("OldPassword1!")
        u = User(username="resetme", password_hash=ph, password_salt=salt,
                 display_name="R", role="employee", is_active=True)
        self.session.add(u)
        self.session.commit()
        self.session.refresh(u)
        raw = generate_password_reset_token(self.session, user_id=u.id)
        return u.id, raw

    def test_weak_password_re_renders_with_problems(self):
        _, raw = self._issue_reset()
        csrf = self._csrf()
        r = self.client.post(
            f"/team/password/reset/{raw}",
            data={"new_password": "short", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertIn("problems=", r.headers["location"])

    def test_strong_password_redirects_to_login(self):
        _, raw = self._issue_reset()
        csrf = self._csrf()
        r = self.client.post(
            f"/team/password/reset/{raw}",
            data={"new_password": "VeryStrong#Pass9", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertTrue(r.headers["location"].startswith("/team/login"))


class InviteAcceptTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def test_invite_accept_creates_user_and_profile(self):
        from app.auth import generate_invite_token, hash_password
        from app.models import EmployeeProfile, User

        ph, salt = hash_password("AdminPass1!")
        admin = User(username="adm", password_hash=ph, password_salt=salt,
                     display_name="A", role="admin", is_active=True)
        self.session.add(admin)
        self.session.commit()
        self.session.refresh(admin)
        raw = generate_invite_token(self.session, role="employee", created_by_user_id=admin.id)
        csrf = self._csrf()
        r = self.client.post(
            f"/team/invite/accept/{raw}",
            data={
                "new_username": "neweemp",
                "new_password": "StrongPass9#xy",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        # Post-onboarding redirect may include a welcome flash query string.
        self.assertEqual(
            r.headers["location"].split("?", 1)[0], "/team/", r.headers["location"]
        )
        # Verify user + profile
        u = self.session.exec(select(User).where(User.username == "neweemp")).first()
        self.assertIsNotNone(u)
        self.assertEqual(u.role, "employee")
        prof = self.session.get(EmployeeProfile, u.id)
        self.assertIsNotNone(prof)

    def test_invite_accept_email_collision_keeps_onboarding_successful_with_flash(self):
        from app.auth import generate_invite_token, hash_password
        from app.models import AuditLog, EmployeeProfile, User
        from app.pii import email_lookup_hash, encrypt_pii

        ph, salt = hash_password("AdminPass1!")
        admin = User(username="adm2", password_hash=ph, password_salt=salt,
                     display_name="A2", role="admin", is_active=True)
        self.session.add(admin)
        self.session.commit()
        self.session.refresh(admin)

        existing = User(
            username="existingemp",
            password_hash="x",
            password_salt="x",
            display_name="Existing",
            role="employee",
            is_active=True,
        )
        self.session.add(existing)
        self.session.commit()
        self.session.refresh(existing)
        self.session.add(EmployeeProfile(
            user_id=existing.id,
            email_ciphertext=encrypt_pii("collision@example.com"),
            email_lookup_hash=email_lookup_hash("collision@example.com"),
        ))
        self.session.commit()

        raw = generate_invite_token(self.session, role="employee", created_by_user_id=admin.id)
        csrf = self._csrf()
        r = self.client.post(
            f"/team/invite/accept/{raw}",
            data={
                "new_username": "autofillvictim",
                "new_password": "StrongPass9#xy",
                "email": "collision@example.com",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertIn("/team/?flash=Welcome+to+the+team!", r.headers["location"])
        self.assertIn("banner=Email+not+saved", r.headers["location"])

        u = self.session.exec(select(User).where(User.username == "autofillvictim")).first()
        self.assertIsNotNone(u)
        prof = self.session.get(EmployeeProfile, u.id)
        self.assertIsNotNone(prof)
        self.assertIsNone(prof.email_ciphertext)
        self.assertIsNone(prof.email_lookup_hash)

        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.target_user_id == u.id)
        ).all())
        actions = {row.action for row in rows}
        self.assertIn("account.invite_accepted", actions)
        self.assertIn("account.invite_email_dropped", actions)
        accepted = next(row for row in rows if row.action == "account.invite_accepted")
        dropped = next(row for row in rows if row.action == "account.invite_email_dropped")
        accepted_details = json.loads(accepted.details_json)
        dropped_details = json.loads(dropped.details_json)
        self.assertTrue(accepted_details["email_skipped_due_to_clash"])
        self.assertNotIn("collision@example.com", dropped.details_json)
        self.assertNotIn("collision@example.com", accepted.details_json)
        self.assertEqual(dropped_details["reason"], "address_already_on_file_for_another_employee")


class SupplyAndPoliciesTests(unittest.TestCase, _PortalHarness):
    def setUp(self):
        self._setup_portal()

    def tearDown(self):
        self._teardown_portal()

    def _seed_employee(self, user_id: int = 40, username: str = "emp_s") -> int:
        from app.auth import hash_password
        from app.models import EmployeeProfile, User

        ph, salt = hash_password("xx")
        self.session.add(User(
            id=user_id, username=username, password_hash=ph, password_salt=salt,
            display_name=username, role="employee", is_active=True,
        ))
        self.session.add(EmployeeProfile(user_id=user_id))
        self.session.commit()
        self._login_as("employee", user_id=user_id, username=username)
        return user_id

    def _seed_admin(self, user_id: int = 39, username: str = "policy_admin") -> int:
        from app.auth import hash_password
        from app.models import User

        ph, salt = hash_password("xx")
        self.session.add(User(
            id=user_id, username=username, password_hash=ph, password_salt=salt,
            display_name=username, role="admin", is_active=True,
        ))
        self.session.commit()
        self._login_as("admin", user_id=user_id, username=username)
        return user_id

    def test_supply_post_without_csrf_is_403(self):
        self._seed_employee()
        r = self.client.post(
            "/team/supply",
            data={"title": "new stapler", "urgency": "normal"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_supply_post_with_csrf_creates_row(self):
        uid = self._seed_employee(user_id=41, username="emp_sc")
        csrf = self._csrf()
        r = self.client.post(
            "/team/supply",
            data={"title": "printer ink", "description": "", "urgency": "normal", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        from app.models import SupplyRequest

        rows = self.session.exec(
            select(SupplyRequest).where(SupplyRequest.submitted_by_user_id == uid)
        ).all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "printer ink")

    def test_policy_acknowledge_writes_audit_log(self):
        uid = self._seed_employee(user_id=42, username="emp_p")
        csrf = self._csrf()
        r = self.client.post(
            "/team/policies/acknowledge/code-of-conduct",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        from app.models import AuditLog

        rows = self.session.exec(
            select(AuditLog).where(
                AuditLog.actor_user_id == uid,
                AuditLog.action == "policy.acknowledge",
            )
        ).all()
        self.assertEqual(len(rows), 1)
        details = json.loads(rows[0].details_json)
        self.assertEqual(details["policy_id"], "code-of-conduct")

    def test_admin_policy_publish_shows_on_employee_policies_page(self):
        self._seed_admin()
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/policies",
            data={
                "title": "Phone Use Policy",
                "body": "Keep phones away from inventory unless a manager says otherwise.",
                "version": "v1",
                "kind": "policy",
                "requires_acknowledgement": "1",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)

        from app.models import AuditLog, TeamPolicy

        policy = self.session.exec(
            select(TeamPolicy).where(TeamPolicy.title == "Phone Use Policy")
        ).one()
        self._seed_employee(user_id=44, username="emp_policy_reader")
        page = self.client.get("/team/policies")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Phone Use Policy", page.text)
        self.assertIn("Keep phones away from inventory", page.text)

        csrf = self._csrf()
        ack = self.client.post(
            f"/team/policies/acknowledge/{policy.public_id}",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(ack.status_code, 303)
        rows = self.session.exec(
            select(AuditLog).where(
                AuditLog.action == "policy.acknowledge",
                AuditLog.details_json.contains(policy.public_id),
            )
        ).all()
        self.assertEqual(len(rows), 1)

    def test_profile_post_updates_preferred_name_not_role(self):
        uid = self._seed_employee(user_id=43, username="emp_pr")
        csrf = self._csrf()
        r = self.client.post(
            "/team/profile",
            data={
                "preferred_name": "Renamed",
                "phone": "",
                "emergency_contact_name": "",
                "emergency_contact_phone": "",
                "address_street": "", "address_city": "",
                "address_state": "", "address_zip": "",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        from app.models import User

        refreshed = self.session.get(User, uid)
        self.assertEqual(refreshed.display_name, "Renamed")
        self.assertEqual(refreshed.role, "employee")  # unchanged


if __name__ == "__main__":
    unittest.main()
