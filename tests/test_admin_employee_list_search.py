from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave-g-list-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "wave-g-list-hmac-key")
os.environ.setdefault("SESSION_SECRET", "wave-g-list-session-xxxxxxxxxxxxxxxx")
os.environ.setdefault("ADMIN_PASSWORD", "wave-g-list-admin-password")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class EmployeeListSearchTests(unittest.TestCase):
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
            username="list-admin",
            password_hash="x",
            password_salt="x",
            display_name="List Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(self.admin)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def _request(self, *, path="/team/admin/employees", query=""):
        return SimpleNamespace(
            state=SimpleNamespace(current_user=self.admin),
            scope={"session": {}},
            session={},
            headers={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path=path, query=query),
        )

    def _list_html(self, *, q=None):
        from app.routers.team_admin_employees import admin_employees_list

        response = admin_employees_list(
            self._request(query=f"q={q}" if q else ""),
            q=q,
            session=self.session,
        )
        self.assertEqual(response.status_code, 200)
        return response.body.decode("utf-8")

    def _seed_user(self, uid: int, username: str, *, display_name: str = "", legal_name: str = ""):
        from app.models import EmployeeProfile, User
        from app.team.pii import encrypt_pii

        user = User(
            id=uid,
            username=username,
            password_hash="hash",
            password_salt="salt",
            display_name=display_name,
            role="employee",
            is_active=True,
        )
        self.session.add(user)
        profile = EmployeeProfile(user_id=uid)
        if legal_name:
            profile.legal_name_enc = encrypt_pii(legal_name)
        self.session.add(profile)
        return user

    def test_search_matches_rows_beyond_first_200(self):
        for offset in range(250):
            self._seed_user(1000 + offset, f"user-{offset:03d}", display_name=f"User {offset:03d}")
        self._seed_user(2000, "zz-needle-user", display_name="Needle User")
        self.session.commit()

        html = self._list_html(q="needle")
        self.assertIn("zz-needle-user", html)

    def test_list_does_not_decrypt_legal_names(self):
        self._seed_user(3000, "secret-user", display_name="", legal_name="SECRET-NAME")
        self.session.commit()

        with patch("app.routers.team_admin_employees.decrypt_pii", side_effect=AssertionError("decrypt called")):
            html = self._list_html()
        self.assertNotIn("SECRET-NAME", html)
        self.assertIn("(no display name)", html)

    def test_list_shows_display_name_not_legal_name(self):
        self._seed_user(3001, "alex-user", display_name="Alex", legal_name="Alexandra Smith")
        self.session.commit()

        html = self._list_html()
        self.assertIn("Alex", html)
        self.assertNotIn("Alexandra Smith", html)

    def test_list_filter_uses_ilike_on_username(self):
        self._seed_user(3002, "alice", display_name="Alice")
        self._seed_user(3003, "zzbob", display_name="Bob")
        self._seed_user(3004, "alicia", display_name="Alicia")
        self.session.commit()

        html = self._list_html(q="ali")
        self.assertIn("alice", html)
        self.assertIn("alicia", html)
        self.assertNotIn("zzbob", html)

    def test_empty_display_name_does_not_fall_back_to_legal_name(self):
        self._seed_user(3005, "empty-display", display_name="", legal_name="Hidden Legal")
        self.session.commit()

        html = self._list_html()
        self.assertIn("(no display name)", html)
        self.assertNotIn("Hidden Legal", html)
