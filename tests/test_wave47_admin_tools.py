"""Tests for Wave 4.7 additions:

- simplified add-employee form (display_name required, legal_name optional)
- /team/admin/employees default filter = active only, toggle to show inactive
- /team/password/change self-serve password change
- admin PII edit on /team/admin/employees/<id>/pii-update
- ShiftEntry model + classify_shift_label heuristic
- admin schedule grid save flow
- read-only employee schedule view

These reuse the harness shape from `test_employee_portal_draft_flow.py`
(CSRF + session + auth patching) so behavior is consistent with the rest
of the portal suite.
"""
from __future__ import annotations

import importlib
import json
import os
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "wave47-salt")
os.environ.setdefault("SESSION_SECRET", "test-secret-wave47-" + "x" * 32)


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


class _W47Harness:
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

    def _login_as(
        self,
        role: str,
        *,
        user_id: int = 500,
        username: str = "adm_w47",
        password_hash: str = "x",
        password_salt: str = "x",
        is_active: bool = True,
    ):
        from app import shared
        from app.models import User
        import app.main as app_main

        u = User(
            id=user_id,
            username=username,
            password_hash=password_hash,
            password_salt=password_salt,
            display_name=username,
            role=role,
            is_active=is_active,
        )
        # Stop BOTH the default anonymous patchers AND any previously-active
        # `_login_as` patchers before starting new ones. Otherwise a second
        # call to `_login_as` within the same test leaks the first patch past
        # tearDown, where it survives into other test files' `importlib.reload`
        # and returns a stale (detached) User from the middleware.
        for attr in (
            "_default_user_patcher_shared",
            "_default_user_patcher_main",
            "_patcher_shared",
            "_patcher_main",
        ):
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


# ---------------------------------------------------------------------------
# Simplified add-employee
# ---------------------------------------------------------------------------


class SimplifiedAddEmployeeTests(unittest.TestCase, _W47Harness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_display_name_only_creates_draft(self):
        """Admin should be able to create a draft with JUST a display name.

        Legal name left blank is the common case now — the employee fills
        it in during onboarding. Profile should be created with legal_name_enc
        still None.
        """
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile

        admin = self._login_as("admin")
        user = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="David",
        )
        self.assertEqual(user.display_name, "David")
        self.assertFalse(user.is_active)
        profile = self.session.get(EmployeeProfile, user.id)
        self.assertIsNotNone(profile)
        self.assertIsNone(profile.legal_name_enc, "legal_name should be blank when not supplied")

    def test_display_name_plus_legal_name(self):
        """Admin can also supply both at once for people they have full info on."""
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile
        from app.pii import decrypt_pii

        admin = self._login_as("admin")
        user = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="Big Chris",
            legal_name="Christopher Allen",
        )
        self.assertEqual(user.display_name, "Big Chris")
        profile = self.session.get(EmployeeProfile, user.id)
        self.assertEqual(decrypt_pii(profile.legal_name_enc), "Christopher Allen")

    def test_legal_name_only_backward_compat(self):
        """Older callers that pass just legal_name should still work.

        We fall back to legal_name as display_name if the admin didn't
        give us a nicer one. Keeps the change non-breaking for any other
        callers / tests that already use the `legal_name=` kwarg.
        """
        from app.auth import create_draft_employee

        admin = self._login_as("admin")
        user = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            legal_name="Legal Only",
        )
        self.assertEqual(user.display_name, "Legal Only")


# ---------------------------------------------------------------------------
# Employees list default active-only
# ---------------------------------------------------------------------------


class EmployeesListActiveFilterTests(unittest.TestCase, _W47Harness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def _seed_users(self):
        from app.models import User

        # Seed:
        # - 701: active employee
        # - 702: terminated employee (has password, but is_active=False)
        # - 703: draft employee (no password, inactive — but still on the team)
        self.session.add(
            User(
                id=701, username="active_emp",
                password_hash="x", password_salt="x",
                display_name="ActiveOne", role="employee", is_active=True,
            )
        )
        self.session.add(
            User(
                id=702, username="gone_emp",
                password_hash="realhash", password_salt="realsalt",
                display_name="FormerEmployee", role="employee", is_active=False,
            )
        )
        self.session.add(
            User(
                id=703, username="__draft_abc__",
                password_hash="", password_salt="",
                display_name="DraftPerson", role="employee", is_active=False,
            )
        )
        self.session.commit()

    def test_default_view_hides_terminated_but_keeps_drafts(self):
        """Default filter hides truly-terminated employees but keeps drafts.

        Drafts are inactive by design (they haven't accepted an invite
        yet) — but the whole point of the draft flow is that Jeffrey
        can put them on the schedule BEFORE they register. Hiding
        drafts by default would defeat that. The filter hides only
        "real" inactives — people who used to have a password and are
        now switched off.
        """
        self._login_as("admin")
        self._seed_users()
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertIn("ActiveOne", r.text)
        self.assertIn("DraftPerson", r.text, "drafts must show in the default view")
        self.assertNotIn("FormerEmployee", r.text)
        self.assertIn("Show inactive", r.text, "toggle checkbox should be present")

    def test_show_inactive_toggle_reveals_terminated(self):
        """`?show_inactive=1` should reveal terminated employees too."""
        self._login_as("admin")
        self._seed_users()
        r = self.client.get("/team/admin/employees?show_inactive=1")
        self.assertEqual(r.status_code, 200)
        self.assertIn("ActiveOne", r.text)
        self.assertIn("DraftPerson", r.text)
        self.assertIn("FormerEmployee", r.text)


# ---------------------------------------------------------------------------
# Self-serve password change
# ---------------------------------------------------------------------------


class SelfServePasswordChangeTests(unittest.TestCase, _W47Harness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_change_password_happy_path(self):
        from app.auth import change_user_password, hash_password, verify_password
        from app.models import User

        pwd_hash, pwd_salt = hash_password("OldPassword1234!")
        user = self._login_as(
            "employee",
            user_id=801,
            username="employee_pw",
            password_hash=pwd_hash,
            password_salt=pwd_salt,
        )
        updated = change_user_password(
            self.session,
            user,
            current_password="OldPassword1234!",
            new_password="NewPassword5678!",
        )
        self.assertTrue(
            verify_password(
                "NewPassword5678!", updated.password_hash, salt=updated.password_salt
            )
        )
        self.assertFalse(
            verify_password(
                "OldPassword1234!", updated.password_hash, salt=updated.password_salt
            )
        )

    def test_change_password_rejects_wrong_current(self):
        from app.auth import BadCurrentPasswordError, change_user_password, hash_password

        pwd_hash, pwd_salt = hash_password("RealPassword1234!")
        user = self._login_as(
            "employee",
            user_id=802,
            username="wrongcurr",
            password_hash=pwd_hash,
            password_salt=pwd_salt,
        )
        with self.assertRaises(BadCurrentPasswordError) as cm:
            change_user_password(
                self.session,
                user,
                current_password="WrongPasswordNope!",
                new_password="Something1234Strong!",
            )
        self.assertEqual(str(cm.exception), "current_password_wrong")

    def test_change_password_rejects_weak_new(self):
        from app.auth import WeakPasswordError, change_user_password, hash_password

        pwd_hash, pwd_salt = hash_password("CurrentPass1234!")
        user = self._login_as(
            "employee",
            user_id=803,
            username="weakpw",
            password_hash=pwd_hash,
            password_salt=pwd_salt,
        )
        with self.assertRaises(WeakPasswordError):
            change_user_password(
                self.session,
                user,
                current_password="CurrentPass1234!",
                new_password="short",
            )

    def test_change_password_rejects_same_as_current(self):
        from app.auth import change_user_password, hash_password

        pwd_hash, pwd_salt = hash_password("SamePassword1234!")
        user = self._login_as(
            "employee",
            user_id=804,
            username="samepw",
            password_hash=pwd_hash,
            password_salt=pwd_salt,
        )
        with self.assertRaises(ValueError) as cm:
            change_user_password(
                self.session,
                user,
                current_password="SamePassword1234!",
                new_password="SamePassword1234!",
            )
        self.assertEqual(str(cm.exception), "new_password_same_as_current")

    def test_change_password_page_renders(self):
        self._login_as("employee", user_id=805, username="rendered_pw")
        r = self.client.get("/team/password/change")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Current password", r.text)
        self.assertIn("New password", r.text)
        self.assertIn("Confirm new password", r.text)


# ---------------------------------------------------------------------------
# Admin PII edit
# ---------------------------------------------------------------------------


class AdminPIIEditTests(unittest.TestCase, _W47Harness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_admin_can_write_legal_name(self):
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile
        from app.pii import decrypt_pii

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="Emily",
        )
        r = self.client.post(
            f"/team/admin/employees/{draft.id}/pii-update",
            data={
                "csrf_token": self._csrf(),
                "legal_name": "Emily Jean Smith",
                "email": "",
                "phone": "",
                "emergency_contact_name": "",
                "emergency_contact_phone": "",
                "address_street": "",
                "address_city": "",
                "address_state": "",
                "address_zip": "",
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, draft.id)
        self.assertIsNotNone(profile)
        self.assertEqual(decrypt_pii(profile.legal_name_enc), "Emily Jean Smith")

    def test_admin_blank_fields_do_not_clobber(self):
        """An admin saving an empty form must NOT wipe existing PII.

        This is the whole point of the "blank = unchanged" rule — we'd
        lose real employee data if a blurred-out form overwrote stuff.
        """
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile
        from app.pii import decrypt_pii, encrypt_pii

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="Gisell",
        )
        profile = self.session.get(EmployeeProfile, draft.id)
        profile.legal_name_enc = encrypt_pii("Gisell Preset")
        profile.phone_enc = encrypt_pii("5551234567")
        self.session.add(profile)
        self.session.commit()

        r = self.client.post(
            f"/team/admin/employees/{draft.id}/pii-update",
            data={
                "csrf_token": self._csrf(),
                "legal_name": "",
                "email": "",
                "phone": "",
                "emergency_contact_name": "",
                "emergency_contact_phone": "",
                "address_street": "",
                "address_city": "",
                "address_state": "",
                "address_zip": "",
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))

        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, draft.id)
        self.assertEqual(decrypt_pii(profile.legal_name_enc), "Gisell Preset")
        self.assertEqual(decrypt_pii(profile.phone_enc), "5551234567")

    def test_admin_email_uniqueness_enforced(self):
        """Trying to assign an already-used email should redirect with a flash."""
        from app.auth import create_draft_employee

        admin = self._login_as("admin")
        a = create_draft_employee(
            self.session, created_by_user_id=admin.id,
            display_name="A", email="dup@example.com",
        )
        b = create_draft_employee(
            self.session, created_by_user_id=admin.id,
            display_name="B",
        )
        r = self.client.post(
            f"/team/admin/employees/{b.id}/pii-update",
            data={
                "csrf_token": self._csrf(),
                "legal_name": "", "email": "dup@example.com",
                "phone": "", "emergency_contact_name": "", "emergency_contact_phone": "",
                "address_street": "", "address_city": "", "address_state": "", "address_zip": "",
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        self.assertIn("already+taken", r.headers.get("location", ""))

    def test_admin_sensitive_write_requires_reveal_authority(self):
        from app.auth import create_draft_employee
        from app.models import EmployeeProfile, RolePermission

        admin = self._login_as("admin")
        perm = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "admin",
                RolePermission.resource_key == "admin.employees.reveal_pii",
            )
        ).one()
        perm.is_allowed = False
        self.session.add(perm)
        self.session.commit()
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="Reveal Locked",
        )
        r = self.client.post(
            f"/team/admin/employees/{draft.id}/pii-update",
            data={
                "csrf_token": self._csrf(),
                "legal_name": "Hidden Name",
                "email": "",
                "phone": "",
                "emergency_contact_name": "",
                "emergency_contact_phone": "",
                "address_street": "",
                "address_city": "",
                "address_state": "",
                "address_zip": "",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)
        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, draft.id)
        self.assertFalse(bool(profile and profile.legal_name_enc))

    def test_admin_blank_sensitive_write_allowed_without_reveal_authority(self):
        from app.auth import create_draft_employee
        from app.models import RolePermission

        admin = self._login_as("admin")
        perm = self.session.exec(
            select(RolePermission).where(
                RolePermission.role == "admin",
                RolePermission.resource_key == "admin.employees.reveal_pii",
            )
        ).one()
        perm.is_allowed = False
        self.session.add(perm)
        self.session.commit()
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="No Reveal Needed",
        )
        r = self.client.post(
            f"/team/admin/employees/{draft.id}/pii-update",
            data={
                "csrf_token": self._csrf(),
                "legal_name": "",
                "email": "",
                "phone": "",
                "emergency_contact_name": "",
                "emergency_contact_phone": "",
                "address_street": "",
                "address_city": "",
                "address_state": "",
                "address_zip": "",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)

    def test_admin_pii_audit_includes_fingerprints(self):
        from app.auth import create_draft_employee
        from app.models import AuditLog

        admin = self._login_as("admin")
        draft = create_draft_employee(
            self.session,
            created_by_user_id=admin.id,
            display_name="Fingerprint Me",
        )
        r = self.client.post(
            f"/team/admin/employees/{draft.id}/pii-update",
            data={
                "csrf_token": self._csrf(),
                "legal_name": "Jane Example",
                "email": "jane@example.com",
                "phone": "5551234567",
                "emergency_contact_name": "John Example",
                "emergency_contact_phone": "5552223333",
                "address_street": "1 Main",
                "address_city": "Austin",
                "address_state": "TX",
                "address_zip": "78701",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.pii_update")
        ).one()
        details = json.loads(row.details_json or "{}")
        self.assertIn("fingerprints", details)
        self.assertEqual(details["fingerprints"]["email"]["len"], len("jane@example.com"))
        self.assertTrue(details["fingerprints"]["address"]["present"])
        self.assertEqual(len(details["fingerprints"]["email"]["sha256_12"]), 12)


# ---------------------------------------------------------------------------
# Shift classification heuristic
# ---------------------------------------------------------------------------


class ClassifyShiftLabelTests(unittest.TestCase):
    def test_blank(self):
        from app.models import classify_shift_label, SHIFT_KIND_BLANK
        self.assertEqual(classify_shift_label(""), SHIFT_KIND_BLANK)
        self.assertEqual(classify_shift_label("   "), SHIFT_KIND_BLANK)
        self.assertEqual(classify_shift_label(None), SHIFT_KIND_BLANK)

    def test_off(self):
        from app.models import classify_shift_label, SHIFT_KIND_OFF
        self.assertEqual(classify_shift_label("OFF"), SHIFT_KIND_OFF)
        self.assertEqual(classify_shift_label("off"), SHIFT_KIND_OFF)

    def test_show_request_all(self):
        from app.models import (
            classify_shift_label,
            SHIFT_KIND_SHOW,
            SHIFT_KIND_REQUEST,
            SHIFT_KIND_ALL,
        )
        self.assertEqual(classify_shift_label("SHOW"), SHIFT_KIND_SHOW)
        self.assertEqual(classify_shift_label("REQUEST"), SHIFT_KIND_REQUEST)
        self.assertEqual(classify_shift_label("ALL"), SHIFT_KIND_ALL)

    def test_stream_variants(self):
        from app.models import classify_shift_label, SHIFT_KIND_STREAM
        self.assertEqual(classify_shift_label("Stream"), SHIFT_KIND_STREAM)
        self.assertEqual(classify_shift_label("STREAM"), SHIFT_KIND_STREAM)

    def test_if_needed(self):
        from app.models import classify_shift_label, SHIFT_KIND_IF_NEEDED
        self.assertEqual(classify_shift_label("IF NEEDED"), SHIFT_KIND_IF_NEEDED)
        self.assertEqual(classify_shift_label("if needed"), SHIFT_KIND_IF_NEEDED)

    def test_work_for_time_ranges(self):
        from app.models import classify_shift_label, SHIFT_KIND_WORK
        self.assertEqual(classify_shift_label("10:30 AM - 6:30 PM"), SHIFT_KIND_WORK)
        self.assertEqual(classify_shift_label("4 PM - 8:15 PM"), SHIFT_KIND_WORK)
        self.assertEqual(classify_shift_label("12PM-8PM"), SHIFT_KIND_WORK)


class ParseShiftHoursTests(unittest.TestCase):
    """Pin the shift-hour parser behavior powering the 7shifts-style totals."""

    def test_blank_and_non_shift_tokens_are_zero(self):
        from app.routers.team_admin_schedule import _parse_shift_hours
        for label in ("", "   ", "OFF", "SHOW", "REQUEST", "IF NEEDED"):
            self.assertEqual(_parse_shift_hours(label), 0.0, label)

    def test_ampm_ranges(self):
        from app.routers.team_admin_schedule import _parse_shift_hours
        self.assertEqual(_parse_shift_hours("10:30 AM - 6:30 PM"), 8.0)
        self.assertEqual(_parse_shift_hours("10am-2pm"), 4.0)
        self.assertEqual(_parse_shift_hours("4 PM - 8:15 PM"), 4.25)

    def test_business_day_heuristic_for_bare_numbers(self):
        """'9-5' should be 8 hrs (9 AM to 5 PM), not 20 hrs overnight."""
        from app.routers.team_admin_schedule import _parse_shift_hours
        self.assertEqual(_parse_shift_hours("9-5"), 8.0)
        self.assertEqual(_parse_shift_hours("10-6"), 8.0)

    def test_overnight_wrap(self):
        from app.routers.team_admin_schedule import _parse_shift_hours
        # 10 PM to 2 AM = 4 hours overnight.
        self.assertEqual(_parse_shift_hours("10 PM - 2 AM"), 4.0)

    def test_multiple_ranges_sum(self):
        from app.routers.team_admin_schedule import _parse_shift_hours
        # Split shift: morning + afternoon.
        self.assertEqual(_parse_shift_hours("9 AM - 12 PM / 2 PM - 6 PM"), 7.0)

    def test_unparseable_is_zero_not_exception(self):
        from app.routers.team_admin_schedule import _parse_shift_hours
        self.assertEqual(_parse_shift_hours("whatever"), 0.0)
        self.assertEqual(_parse_shift_hours("10:30 AM -"), 0.0)


# ---------------------------------------------------------------------------
# Admin schedule save
# ---------------------------------------------------------------------------


class AdminScheduleSaveTests(unittest.TestCase, _W47Harness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def _active_employees(self) -> list:
        from app.models import User

        rows = []
        for i, name in enumerate(["David", "Emily", "Chris"], start=1):
            u = User(
                id=900 + i,
                username=name.lower(),
                password_hash="x",
                password_salt="x",
                display_name=name,
                role="employee",
                is_active=True,
                is_schedulable=True,
            )
            self.session.add(u)
            rows.append(u)
        self.session.commit()
        return rows

    def _roster(self, emps, week_start: date, *, admin_id: int = 500) -> None:
        """Put the given employees on the roster for the given week.

        The grid is empty by default now — admins add people per week —
        so tests that exercise the save flow need to seed the roster
        before posting cell edits.
        """
        from app.models import ScheduleRosterMember

        for u in emps:
            self.session.add(
                ScheduleRosterMember(
                    week_start=week_start,
                    user_id=u.id,
                    added_by_user_id=admin_id,
                )
            )
        self.session.commit()

    def _monday_of_this_week(self) -> date:
        today = date.today()
        return today - timedelta(days=today.weekday())

    def test_admin_schedule_page_renders_grid(self):
        admin = self._login_as("admin")
        emps = self._active_employees()
        self._roster(emps, self._monday_of_this_week(), admin_id=admin.id)
        # edit=1 flips the page into edit mode so Save/remove markup
        # renders. The default view is read-only for everyone.
        r = self.client.get("/team/admin/schedule?edit=1")
        self.assertEqual(r.status_code, 200)
        for e in emps:
            self.assertIn(e.display_name, r.text)
        self.assertIn("Save storefront schedule", r.text)
        # Stream grid is now also editable (writes sync with Stream Manager).
        self.assertIn("Save stream schedule", r.text)
        self.assertIn("Prev", r.text)
        self.assertIn("Next", r.text)

        # Read-only view does NOT render the Save button.
        r_ro = self.client.get("/team/admin/schedule")
        self.assertEqual(r_ro.status_code, 200)
        self.assertNotIn("Save storefront schedule", r_ro.text)
        self.assertIn("Edit schedule", r_ro.text)

    def test_admin_schedule_empty_by_default(self):
        """A fresh week shows NO employees on the grid rows.

        Admins opt people in per week via the roster picker; we should
        not pre-fill with every active employee. The employees can
        still appear in the "Add employee" picker options — that's
        correct — but the grid body itself must be empty.
        """
        self._login_as("admin")
        self._active_employees()
        r = self.client.get("/team/admin/schedule")
        self.assertEqual(r.status_code, 200)
        # Dual-grid layout renders two empty-state messages. The stream
        # grid auto-rosters Stream-role users, so its empty-state wording
        # points admins at the Employees page instead of per-week add.
        self.assertIn("No storefront employees on this week yet", r.text)
        self.assertIn("No Stream-role employees yet", r.text)
        # No employee rows in the body — the sch-name-col cells should
        # not exist yet.
        self.assertNotIn('class="sch-name-col"', r.text)
        self.assertIn("0 on this week", r.text)

    def test_roster_add_lists_employee(self):
        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        r = self.client.post(
            "/team/admin/schedule/roster/add",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
                "user_id": str(emps[0].id),
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))

        page = self.client.get(f"/team/admin/schedule?week={monday.isoformat()}")
        self.assertEqual(page.status_code, 200)
        # David gets a grid row (sch-name-inner wraps the name cell).
        self.assertIn(">David<", page.text)
        self.assertIn("sch-name-inner", page.text)
        # Roster count reflects 1 on this week.
        self.assertIn("1 on this week", page.text)
        # Emily and Chris still addable in the picker, but no Emily row.
        self.assertNotIn("sch-remove-storefront-{}".format(emps[1].id), page.text)
        self.assertNotIn("sch-remove-storefront-{}".format(emps[2].id), page.text)

    def test_roster_add_rejects_terminated_user(self):
        from app.models import User

        admin = self._login_as("admin")
        u = User(
            id=9999, username="exEmployee", password_hash="realhash",
            password_salt="s", display_name="Gone", role="employee",
            is_active=False,
        )
        self.session.add(u)
        self.session.commit()
        monday = self._monday_of_this_week()
        r = self.client.post(
            "/team/admin/schedule/roster/add",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
                "user_id": "9999",
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        # Follow to confirm they are NOT on the grid.
        page = self.client.get(f"/team/admin/schedule?week={monday.isoformat()}")
        self.assertNotIn("Gone", page.text)

    def test_roster_add_allows_draft_user(self):
        """Draft employees (not yet onboarded) should be schedulable.

        They can't log in yet, but the admin often wants to book them
        before sending the invite.
        """
        from app.models import User

        admin = self._login_as("admin")
        draft = User(
            id=777, username="newhire", password_hash="",  # draft
            password_salt="", display_name="Newbie", role="employee",
            is_active=False, is_schedulable=True,
        )
        self.session.add(draft)
        self.session.commit()
        monday = self._monday_of_this_week()
        r = self.client.post(
            "/team/admin/schedule/roster/add",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
                "user_id": "777",
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        page = self.client.get(f"/team/admin/schedule?week={monday.isoformat()}")
        self.assertIn("Newbie", page.text)

    def test_roster_remove_drops_user_and_clears_week_shifts(self):
        from app.models import ShiftEntry, ScheduleRosterMember, classify_shift_label

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        self._roster(emps, monday, admin_id=admin.id)

        # Give David (emps[0]) a shift this week; we expect removal to
        # clear it so he actually disappears from the grid.
        self.session.add(
            ShiftEntry(
                user_id=emps[0].id,
                shift_date=monday,
                label="10:30 AM - 6:30 PM",
                kind=classify_shift_label("10:30 AM - 6:30 PM"),
                created_by_user_id=admin.id,
            )
        )
        self.session.commit()

        r = self.client.post(
            "/team/admin/schedule/roster/remove",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
                "user_id": str(emps[0].id),
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))

        self.session.expire_all()
        remaining_roster = list(
            self.session.exec(
                select(ScheduleRosterMember).where(
                    ScheduleRosterMember.week_start == monday,
                    ScheduleRosterMember.user_id == emps[0].id,
                )
            ).all()
        )
        self.assertEqual(remaining_roster, [], "roster row should be deleted")
        remaining_shifts = list(
            self.session.exec(
                select(ShiftEntry).where(ShiftEntry.user_id == emps[0].id)
            ).all()
        )
        self.assertEqual(
            remaining_shifts, [],
            "removing from the week should clear that week's shifts"
        )

        # Other employees still on the grid; David does not have a row
        # anymore (no remove-form for him). Remove-form markup only
        # renders when the admin is in edit mode (?edit=1).
        page = self.client.get(
            f"/team/admin/schedule?week={monday.isoformat()}&edit=1"
        )
        self.assertNotIn(f'id="sch-remove-storefront-{emps[0].id}"', page.text)
        self.assertIn(f'id="sch-remove-storefront-{emps[1].id}"', page.text)
        self.assertIn(f'id="sch-remove-storefront-{emps[2].id}"', page.text)

    def test_roster_copy_previous_carries_forward(self):
        from app.models import ScheduleRosterMember

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        prev_monday = monday - timedelta(days=7)

        # Previous week has David + Chris on the roster.
        self._roster([emps[0], emps[2]], prev_monday, admin_id=admin.id)

        # Nothing on current week yet.
        r = self.client.post(
            "/team/admin/schedule/roster/copy-previous",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))

        self.session.expire_all()
        now_on = {
            r.user_id
            for r in self.session.exec(
                select(ScheduleRosterMember).where(
                    ScheduleRosterMember.week_start == monday
                )
            ).all()
        }
        self.assertEqual(now_on, {emps[0].id, emps[2].id})

        # Remove-form markup renders only in edit mode.
        page = self.client.get(
            f"/team/admin/schedule?week={monday.isoformat()}&edit=1"
        )
        self.assertIn(f'id="sch-remove-storefront-{emps[0].id}"', page.text)  # David
        self.assertIn(f'id="sch-remove-storefront-{emps[2].id}"', page.text)  # Chris
        self.assertNotIn(f'id="sch-remove-storefront-{emps[1].id}"', page.text)  # Emily not copied

    def test_roster_copy_previous_reports_closed_days_on_target_week(self):
        from app.models import StoreClosure

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        prev_monday = monday - timedelta(days=7)
        self._roster([emps[0]], prev_monday, admin_id=admin.id)
        self.session.add(
            StoreClosure(
                day_date=monday + timedelta(days=2),
                reason="Closed for maintenance",
                is_closed=True,
            )
        )
        self.session.commit()

        r = self.client.post(
            "/team/admin/schedule/roster/copy-previous",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        self.assertIn("Copied+1+employee", r.headers["location"])
        self.assertIn("1+closed+day+stay+marked+closed", r.headers["location"])

    def test_generate_from_previous_skips_closed_days_and_reports_partial_copy(self):
        from app.models import ShiftEntry, StoreClosure, classify_shift_label

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        prev_monday = monday - timedelta(days=7)
        prev_tuesday = prev_monday + timedelta(days=1)
        prev_wednesday = prev_monday + timedelta(days=2)

        self._roster([emps[0]], monday, admin_id=admin.id)
        self.session.add(
            ShiftEntry(
                user_id=emps[0].id,
                shift_date=prev_tuesday,
                label="10:00 AM - 6:00 PM",
                kind=classify_shift_label("10:00 AM - 6:00 PM"),
                created_by_user_id=admin.id,
            )
        )
        self.session.add(
            ShiftEntry(
                user_id=emps[0].id,
                shift_date=prev_wednesday,
                label="11:00 AM - 7:00 PM",
                kind=classify_shift_label("11:00 AM - 7:00 PM"),
                created_by_user_id=admin.id,
            )
        )
        self.session.add(
            StoreClosure(
                day_date=monday + timedelta(days=2),
                reason="Store closed for inventory",
                is_closed=True,
            )
        )
        self.session.commit()

        r = self.client.post(
            "/team/admin/schedule/generate-from-previous",
            data={
                "csrf_token": self._csrf(),
                "week": monday.isoformat(),
                "staff_kind": "storefront",
            },
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        self.assertIn("Copied 1 storefront shift", r.headers["location"])
        self.assertIn("Skipped 1 closed day", r.headers["location"])

        self.session.expire_all()
        copied_rows = list(
            self.session.exec(
                select(ShiftEntry).where(ShiftEntry.shift_date >= monday)
            ).all()
        )
        self.assertEqual(len(copied_rows), 1)
        self.assertEqual(copied_rows[0].shift_date, monday + timedelta(days=1))

    def test_save_creates_updates_and_clears_cells(self):
        from app.models import ShiftEntry, classify_shift_label

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        friday = monday + timedelta(days=4)
        self._roster(emps, monday, admin_id=admin.id)

        # 1) First save: add two cells.
        def _build_cell_key(uid, d):
            return f"cell__{uid}__{d.isoformat()}"

        def _build_dayloc_key(d):
            return f"dayloc__{d.isoformat()}"

        data = {"csrf_token": self._csrf(), "week": monday.isoformat()}
        data[_build_cell_key(emps[0].id, monday)] = "10:30 AM - 6:30 PM"
        data[_build_cell_key(emps[1].id, friday)] = "SHOW"
        data[_build_dayloc_key(friday)] = "East Bay Santa Clara"
        r = self.client.post("/team/admin/schedule", data=data, follow_redirects=False)
        self.assertIn(r.status_code, (302, 303))

        self.session.expire_all()
        rows = list(self.session.exec(select(ShiftEntry)).all())
        self.assertEqual(len(rows), 2)
        labels = {(r.user_id, r.label) for r in rows}
        self.assertIn((emps[0].id, "10:30 AM - 6:30 PM"), labels)
        self.assertIn((emps[1].id, "SHOW"), labels)
        for r in rows:
            if r.label == "SHOW":
                self.assertEqual(r.kind, "show")

        # 2) Second save: update one, clear the other, add a new OFF.
        data2 = {"csrf_token": self._csrf(), "week": monday.isoformat()}
        data2[_build_cell_key(emps[0].id, monday)] = "11 AM - 7 PM"  # updated
        data2[_build_cell_key(emps[1].id, friday)] = ""               # cleared
        data2[_build_cell_key(emps[2].id, monday)] = "OFF"            # added
        r = self.client.post("/team/admin/schedule", data=data2, follow_redirects=False)
        self.assertIn(r.status_code, (302, 303))

        self.session.expire_all()
        rows = list(self.session.exec(select(ShiftEntry)).all())
        label_by_user = {r.user_id: r for r in rows}
        self.assertEqual(label_by_user[emps[0].id].label, "11 AM - 7 PM")
        self.assertNotIn(emps[1].id, label_by_user, "empty label should have deleted the row")
        self.assertEqual(label_by_user[emps[2].id].label, "OFF")
        self.assertEqual(label_by_user[emps[2].id].kind, "off")

    def test_save_handles_day_note_location(self):
        from app.models import ScheduleDayNote

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        saturday = monday + timedelta(days=5)
        self._roster(emps, monday, admin_id=admin.id)

        data = {"csrf_token": self._csrf(), "week": monday.isoformat()}
        data[f"dayloc__{saturday.isoformat()}"] = "East Bay Santa Clara"
        r = self.client.post("/team/admin/schedule", data=data, follow_redirects=False)
        self.assertIn(r.status_code, (302, 303))

        self.session.expire_all()
        notes = list(self.session.exec(select(ScheduleDayNote)).all())
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0].location_label, "East Bay Santa Clara")

        # Clearing the label should delete the note.
        data2 = {"csrf_token": self._csrf(), "week": monday.isoformat()}
        data2[f"dayloc__{saturday.isoformat()}"] = ""
        r = self.client.post("/team/admin/schedule", data=data2, follow_redirects=False)
        self.assertIn(r.status_code, (302, 303))
        self.session.expire_all()
        notes = list(self.session.exec(select(ScheduleDayNote)).all())
        self.assertEqual(len(notes), 0)


# ---------------------------------------------------------------------------
# Employee read-only schedule view
# ---------------------------------------------------------------------------


class EmployeeScheduleViewTests(unittest.TestCase, _W47Harness):
    def setUp(self):
        self._setup()

    def tearDown(self):
        self._teardown()

    def test_employee_sees_published_grid(self):
        from app.models import ShiftEntry, User, classify_shift_label

        # Boss is the admin who publishes; employee is who views.
        boss = self._login_as("admin", user_id=990, username="boss")

        emp = User(
            id=991, username="emp1", display_name="FloorStaff",
            password_hash="x", password_salt="x", role="employee", is_active=True,
        )
        self.session.add(emp)
        self.session.commit()

        today = date.today()
        monday = today - timedelta(days=today.weekday())
        self.session.add(
            ShiftEntry(
                user_id=emp.id,
                shift_date=monday,
                label="10:30 AM - 6:30 PM",
                kind=classify_shift_label("10:30 AM - 6:30 PM"),
                created_by_user_id=boss.id,
            )
        )
        self.session.commit()

        # Now log in as the employee and view the schedule page.
        self._login_as("employee", user_id=991, username="emp1", password_hash="x", password_salt="x")
        r = self.client.get("/team/schedule")
        self.assertEqual(r.status_code, 200)
        self.assertIn("FloorStaff", r.text)
        self.assertIn("10:30 AM - 6:30 PM", r.text)
        # No admin-only affordances.
        self.assertNotIn("Save schedule", r.text)
        # Own row should be visually distinguished.
        self.assertIn("(you)", r.text)


if __name__ == "__main__":
    unittest.main()
