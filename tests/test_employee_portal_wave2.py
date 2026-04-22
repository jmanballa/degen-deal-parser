"""Wave 2 — permissions matrix UI + admin router tests."""
from __future__ import annotations

import importlib
import json
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    return engine


class PermissionsMatrixTests(unittest.TestCase):
    def setUp(self):
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults

        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def test_matrix_shape_has_every_role_and_key(self):
        from app import permissions as perms

        m = perms.permissions_matrix(self.session)
        self.assertEqual(set(m.keys()), set(perms.ROLES))
        for role in perms.ROLES:
            self.assertEqual(set(m[role].keys()) >= set(perms.RESOURCE_KEYS), True)
            for k in perms.RESOURCE_KEYS:
                self.assertIsInstance(m[role][k], bool)

    def test_set_permission_upserts_and_audits(self):
        from app import permissions as perms
        from app.models import AuditLog, RolePermission

        perms.set_permission(
            self.session,
            role="viewer",
            resource_key="page.dashboard",
            is_allowed=False,
            actor_user_id=1,
        )
        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "viewer",
                RolePermission.resource_key == "page.dashboard",
            )
        ).first()
        self.assertIsNotNone(row)
        self.assertFalse(row.is_allowed)

        logs = self.session.exec(
            select(AuditLog).where(AuditLog.action == "permission.set")
        ).all()
        self.assertEqual(len(logs), 1)
        details = json.loads(logs[0].details_json)
        self.assertEqual(details["role"], "viewer")
        self.assertEqual(details["resource_key"], "page.dashboard")
        self.assertEqual(details["is_allowed"], False)
        self.assertEqual(details["prev"], True)

    def test_set_permission_idempotent_writes_log_each_time(self):
        from app import permissions as perms
        from app.models import AuditLog

        for _ in range(3):
            perms.set_permission(
                self.session,
                role="employee",
                resource_key="action.pii.reveal",
                is_allowed=True,
                actor_user_id=None,
            )
        logs = self.session.exec(
            select(AuditLog).where(AuditLog.action == "permission.set")
        ).all()
        self.assertEqual(len(logs), 3)

    def test_set_permission_rejects_unknown_role(self):
        from app import permissions as perms

        with self.assertRaises(ValueError):
            perms.set_permission(
                self.session,
                role="bogus",
                resource_key="page.dashboard",
                is_allowed=True,
                actor_user_id=None,
            )

    def test_reset_to_defaults_audits_once(self):
        from app import permissions as perms
        from app.models import AuditLog, RolePermission

        # Mutate first.
        perms.set_permission(
            self.session,
            role="admin",
            resource_key="page.dashboard",
            is_allowed=False,
            actor_user_id=99,
        )
        count = perms.reset_to_defaults(self.session, actor_user_id=99)
        self.assertEqual(count, 5 * len(perms.RESOURCE_KEYS))

        reset_logs = self.session.exec(
            select(AuditLog).where(AuditLog.action == "permission.reset_all")
        ).all()
        self.assertEqual(len(reset_logs), 1)

        # Admin row should be back to True after reset.
        row = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "admin",
                RolePermission.resource_key == "page.dashboard",
            )
        ).first()
        self.assertTrue(row.is_allowed)

    def test_grouped_keys_covers_every_resource(self):
        from app import permissions as perms

        flat = [k for _, _, keys in perms.grouped_resource_keys() for k in keys]
        self.assertEqual(set(flat), set(perms.RESOURCE_KEYS))

    def test_resource_label_is_friendly(self):
        from app import permissions as perms

        self.assertEqual(perms.resource_label("page.dashboard"), "Dashboard page")
        self.assertEqual(perms.resource_label("action.pii.reveal"), "Reveal employee PII")


class RouteGatingTests(unittest.TestCase):
    """End-to-end: /team/admin requires admin + feature flag + CSRF on POST."""

    def _build_client(self, *, portal_enabled: bool = True):
        # Reload the app with the desired EMPLOYEE_PORTAL_ENABLED env.
        prev = os.environ.get("EMPLOYEE_PORTAL_ENABLED")
        os.environ["EMPLOYEE_PORTAL_ENABLED"] = "true" if portal_enabled else "false"

        from app import config as cfg
        cfg.get_settings.cache_clear()

        # Reload main to re-run router-mount side effects.
        import app.main as app_main
        importlib.reload(app_main)

        from fastapi.testclient import TestClient

        client = TestClient(app_main.app)

        # Restore env for isolation.
        if prev is None:
            os.environ.pop("EMPLOYEE_PORTAL_ENABLED", None)
        else:
            os.environ["EMPLOYEE_PORTAL_ENABLED"] = prev
        return client, app_main

    def _login_as(self, client, role):
        """Inject a user into the session via middleware shim."""
        # The existing app uses session-cookie auth; easier path: monkey-patch
        # get_request_user within a request scope. We use dependency overrides
        # on a specific helper by patching shared.get_request_user.
        from app.models import User
        from app import shared

        user = User(id=42 if role == "admin" else 43, username=f"{role}_t",
                    password_hash="x", role=role, is_active=True)
        self._patcher = patch.object(shared, "get_request_user", return_value=user)
        self._patcher.start()
        return user

    def tearDown(self):
        p = getattr(self, "_patcher", None)
        if p:
            p.stop()
            self._patcher = None

    def test_non_admin_redirected_from_permissions_page(self):
        client, _ = self._build_client(portal_enabled=True)
        self._login_as(client, "viewer")
        r = client.get("/team/admin/permissions", follow_redirects=False)
        # Non-admin → 403 HTML (require_role_response returns 403).
        self.assertIn(r.status_code, (303, 403))

    def test_portal_disabled_hides_route(self):
        client, _ = self._build_client(portal_enabled=False)
        self._login_as(client, "admin")
        r = client.get("/team/admin/permissions", follow_redirects=False)
        # Route is not mounted when disabled → 404.
        self.assertEqual(r.status_code, 404)

    def test_csrf_required_on_set(self):
        client, _ = self._build_client(portal_enabled=True)
        self._login_as(client, "admin")
        r = client.post(
            "/team/admin/permissions/set",
            data={"role": "viewer", "resource_key": "page.dashboard", "is_allowed": "1"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)


if __name__ == "__main__":
    unittest.main()
