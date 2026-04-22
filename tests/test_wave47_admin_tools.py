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
            )
            self.session.add(u)
            rows.append(u)
        self.session.commit()
        return rows

    def _monday_of_this_week(self) -> date:
        today = date.today()
        return today - timedelta(days=today.weekday())

    def test_admin_schedule_page_renders_grid(self):
        self._login_as("admin")
        emps = self._active_employees()
        r = self.client.get("/team/admin/schedule")
        self.assertEqual(r.status_code, 200)
        for e in emps:
            self.assertIn(e.display_name, r.text)
        self.assertIn("Save schedule", r.text)
        self.assertIn("Prev", r.text)
        self.assertIn("Next", r.text)

    def test_save_creates_updates_and_clears_cells(self):
        from app.models import ShiftEntry, classify_shift_label

        admin = self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        friday = monday + timedelta(days=4)

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

        self._login_as("admin")
        emps = self._active_employees()
        monday = self._monday_of_this_week()
        saturday = monday + timedelta(days=5)

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
