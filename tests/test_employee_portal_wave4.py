"""Wave 4 — admin employee mgmt, invites, supply queue, PII reveal."""
from __future__ import annotations

import importlib
import json
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "unit-test-salt")
os.environ.setdefault("SESSION_SECRET", "test-secret-wave4-" + "x" * 32)


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


class _W4Harness:
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

    def _teardown(self):
        self.app_main.app.dependency_overrides.clear()
        self.session.close()
        for attr in ("_patcher_shared", "_patcher_main"):
            patcher = getattr(self, attr, None)
            if patcher:
                patcher.stop()
                setattr(self, attr, None)

    def _login(self, *, role: str, user_id: int = 100, username: str = "admin_t"):
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
        self._patcher_shared = patch.object(shared, "get_request_user", return_value=u)
        self._patcher_shared.start()
        self._patcher_main = patch.object(app_main, "get_request_user", return_value=u)
        self._patcher_main.start()
        # Ensure the user row is ALSO present in the in-memory DB so FK
        # audit-log rows referencing actor_user_id are valid.
        existing = self.session.get(User, user_id)
        if existing is None:
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

    def _seed_employee(self, *, user_id: int = 500, username: str = "tgt", role: str = "employee"):
        from app.models import EmployeeProfile, User
        from app.pii import encrypt_pii
        from datetime import date

        u = User(
            id=user_id,
            username=username,
            password_hash="x",
            password_salt="x",
            display_name=username,
            role=role,
            is_active=True,
        )
        self.session.add(u)
        p = EmployeeProfile(
            user_id=user_id,
            phone_enc=encrypt_pii("555-867-5309"),
            address_enc=encrypt_pii(json.dumps({"street": "1 Main St", "city": "Town", "state": "CA", "zip": "90210"})),
            legal_name_enc=encrypt_pii("Jane Q Test"),
            hire_date=date(2024, 1, 15),
        )
        self.session.add(p)
        self.session.commit()
        return u


class EmployeeListTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_admin_can_list_employees(self):
        self._login(role="admin", user_id=101, username="adm1")
        self._seed_employee(user_id=501, username="emp501")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)
        self.assertIn("emp501", r.text)

    def test_manager_can_list_employees(self):
        self._login(role="manager", user_id=102, username="mgr1")
        self._seed_employee(user_id=502, username="emp502")
        r = self.client.get("/team/admin/employees")
        self.assertEqual(r.status_code, 200)

    def test_employee_cannot_list(self):
        self._login(role="employee", user_id=103, username="emp103")
        r = self.client.get("/team/admin/employees", follow_redirects=False)
        self.assertEqual(r.status_code, 403)

    def test_admin_search_matches_display_name_and_email_fingerprint(self):
        from app.models import EmployeeProfile
        from app.pii import email_lookup_hash, encrypt_pii

        self._login(role="admin", user_id=104, username="adm-search")
        emp = self._seed_employee(user_id=504, username="emp504")
        profile = self.session.get(EmployeeProfile, emp.id)
        profile.email_ciphertext = encrypt_pii("friendly.search@example.com")
        profile.email_lookup_hash = email_lookup_hash("friendly.search@example.com")
        self.session.add(profile)
        self.session.commit()

        by_name = self.client.get("/team/admin/employees?q=emp504")
        self.assertEqual(by_name.status_code, 200)
        self.assertIn("emp504", by_name.text)
        self.assertIn("Search username", by_name.text)

        by_hash = self.client.get(
            f"/team/admin/employees?q={profile.email_lookup_hash[:12]}"
        )
        self.assertEqual(by_hash.status_code, 200)
        self.assertIn("emp504", by_hash.text)
        self.assertIn("display name, legal name, email", by_hash.text)


class DetailAndRevealTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_detail_masks_pii_by_default(self):
        self._login(role="admin", user_id=201, username="adm2")
        emp = self._seed_employee(user_id=601, username="emp601")
        r = self.client.get(f"/team/admin/employees/{emp.id}")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("555-867-5309", r.text)
        self.assertNotIn("1 Main St", r.text)
        self.assertIn("Redacted", r.text)

    def test_reveal_writes_audit_and_shows_plaintext(self):
        from app.models import AuditLog
        self._login(role="admin", user_id=202, username="adm3")
        emp = self._seed_employee(user_id=602, username="emp602")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reveal",
            data={"field": "phone", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("555-867-5309", r.text)
        # Audit row present.
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal")
        ).all())
        self.assertEqual(len(rows), 1)
        self.assertIn("phone", rows[0].details_json)

    def test_reveal_without_csrf_rejected_and_no_audit(self):
        from app.models import AuditLog
        self._login(role="admin", user_id=203, username="adm4")
        emp = self._seed_employee(user_id=603, username="emp603")
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reveal",
            data={"field": "phone"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "pii.reveal")
        ).all())
        self.assertEqual(len(rows), 0)

    def test_manager_cannot_reveal_pii(self):
        self._login(role="manager", user_id=204, username="mgr2")
        emp = self._seed_employee(user_id=604, username="emp604")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reveal",
            data={"field": "phone", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)

    def test_manager_cannot_blind_overwrite_sensitive_pii(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii

        self._login(role="manager", user_id=205, username="mgr3")
        emp = self._seed_employee(user_id=605, username="emp605")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/pii-update",
            data={"phone": "444-222-1111", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)
        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(decrypt_pii(profile.phone_enc), "555-867-5309")
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.pii_update")
        ).all())
        self.assertEqual(rows, [])

    def test_manager_blank_sensitive_pii_update_preserves_existing_values(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii

        self._login(role="manager", user_id=206, username="mgr4")
        emp = self._seed_employee(user_id=606, username="emp606")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/pii-update",
            data={"phone": "   ", "legal_name": "", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)
        self.session.expire_all()
        profile = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(decrypt_pii(profile.phone_enc), "555-867-5309")
        self.assertEqual(decrypt_pii(profile.legal_name_enc), "Jane Q Test")
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.pii_update")
        ).all())
        self.assertEqual(rows, [])

    def test_admin_pii_update_audit_uses_safe_fingerprints(self):
        from app.models import AuditLog

        self._login(role="admin", user_id=207, username="adm7")
        emp = self._seed_employee(user_id=607, username="emp607")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/pii-update",
            data={
                "phone": "444-222-1111",
                "email": "new607@example.com",
                "address_street": "77 Broadway",
                "address_city": "New York",
                "address_state": "NY",
                "address_zip": "10001",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.pii_update")
        ).one()
        details = json.loads(row.details_json)
        self.assertEqual(set(details["fields"]), {"phone", "email", "address"})
        self.assertIn("fingerprints", details)
        self.assertNotIn("field_fingerprints", details)
        self.assertNotIn("444-222-1111", row.details_json)
        self.assertNotIn("new607@example.com", row.details_json)
        self.assertNotIn("77 Broadway", row.details_json)
        self.assertRegex(details["fingerprints"]["phone"]["sha256_12"], r"^[0-9a-f]{12}$")
        self.assertRegex(details["fingerprints"]["email"]["sha256_12"], r"^[0-9a-f]{12}$")
        self.assertRegex(details["fingerprints"]["address"]["sha256_12"], r"^[0-9a-f]{12}$")


class AdminProfileUpdateHardeningTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_employee_detail_hourly_rate_uses_dollars_not_cents(self):
        from app.models import EmployeeProfile
        from app.pii import encrypt_pii
        from app.routers.team_admin_employees import _detail_context

        admin = self._login(role="admin", user_id=519, username="adm519")
        emp = self._seed_employee(user_id=819, username="emp819")
        profile = self.session.get(EmployeeProfile, emp.id)
        profile.hourly_rate_cents_enc = encrypt_pii("2300")
        self.session.add(profile)
        self.session.commit()

        ctx = _detail_context(
            SimpleNamespace(session={}),
            self.session,
            admin,
            emp,
            profile,
        )
        template = Path("app/templates/team/admin/employee_detail.html").read_text()

        self.assertEqual(ctx["hourly_rate_value"], "23.00")
        self.assertIn('name="hourly_rate_dollars"', template)
        self.assertNotIn("hourly_rate_cents", template)
        self.assertNotIn("cents", template.lower())

    def test_profile_hourly_rate_parser_accepts_dollars(self):
        from app.routers.team_admin_employees import _parse_profile_hourly_rate

        self.assertEqual(
            _parse_profile_hourly_rate(
                hourly_rate_dollars="27.50",
                hourly_rate_cents="",
            ),
            (2750, False),
        )
        self.assertEqual(
            _parse_profile_hourly_rate(
                hourly_rate_dollars="$25.00",
                hourly_rate_cents="",
            ),
            (2500, False),
        )
        self.assertEqual(
            _parse_profile_hourly_rate(
                hourly_rate_dollars="12..50",
                hourly_rate_cents="",
            ),
            (None, True),
        )
        self.assertEqual(
            _parse_profile_hourly_rate(
                hourly_rate_dollars="",
                hourly_rate_cents="25.00",
            ),
            (None, True),
        )

    def test_hourly_rate_rejects_invalid_inputs_without_mutating_existing_value(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii, encrypt_pii

        self._login(role="admin", user_id=520, username="adm520")
        emp = self._seed_employee(user_id=820, username="emp820")
        profile = self.session.get(EmployeeProfile, emp.id)
        profile.hourly_rate_cents_enc = encrypt_pii("2300")
        self.session.add(profile)
        self.session.commit()
        csrf = self._csrf()

        for bad in ("abc", "12..50", "-1"):
            r = self.client.post(
                f"/team/admin/employees/{emp.id}/profile-update",
                data={"hourly_rate_dollars": bad, "csrf_token": csrf},
                follow_redirects=False,
            )
            self.assertEqual(r.status_code, 303)
            self.assertIn("Invalid+hourly+rate+ignored", r.headers["location"])
            self.session.expire_all()
            refreshed = self.session.get(EmployeeProfile, emp.id)
            self.assertEqual(decrypt_pii(refreshed.hourly_rate_cents_enc), "2300")

        r = self.client.post(
            f"/team/admin/employees/{emp.id}/profile-update",
            data={"hourly_rate_dollars": "999999.99", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.assertIn("flash=Saved.", r.headers["location"])
        self.session.expire_all()
        refreshed = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(decrypt_pii(refreshed.hourly_rate_cents_enc), "1000000")

        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.profile_update")
        ).all())
        self.assertEqual(len(rows), 1)
        self.assertIn("hourly_rate_cents", rows[0].details_json)

    def test_hourly_rate_accepts_sane_integer_value(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii

        self._login(role="admin", user_id=521, username="adm521")
        emp = self._seed_employee(user_id=821, username="emp821")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/profile-update",
            data={"hourly_rate_dollars": "27.50", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(decrypt_pii(refreshed.hourly_rate_cents_enc), "2750")
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.profile_update")
        ).one()
        self.assertIn("hourly_rate_cents", row.details_json)

    def test_profile_update_saves_payment_method(self):
        from app.models import AuditLog, EmployeeProfile

        self._login(role="admin", user_id=522, username="adm522")
        emp = self._seed_employee(user_id=822, username="emp822")
        csrf = self._csrf()

        r = self.client.post(
            f"/team/admin/employees/{emp.id}/profile-update",
            data={"payment_method": "check", "csrf_token": csrf},
            follow_redirects=False,
        )

        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(refreshed.payment_method, "check")
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.profile_update")
        ).one()
        self.assertIn("payment_method", row.details_json)

    def test_profile_update_saves_monthly_salary_compensation(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii

        self._login(role="admin", user_id=524, username="adm524")
        emp = self._seed_employee(user_id=825, username="emp825")
        csrf = self._csrf()

        r = self.client.post(
            f"/team/admin/employees/{emp.id}/profile-update",
            data={
                "compensation_type": "monthly_salary",
                "monthly_salary_dollars": "4500.00",
                "monthly_salary_pay_day": "15",
                "csrf_token": csrf,
            },
            follow_redirects=False,
        )

        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(refreshed.compensation_type, "monthly_salary")
        self.assertEqual(decrypt_pii(refreshed.monthly_salary_cents_enc), "450000")
        self.assertEqual(refreshed.monthly_salary_pay_day, 15)
        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.profile_update")
        ).one()
        self.assertIn("compensation_type", row.details_json)
        self.assertIn("monthly_salary_cents", row.details_json)
        self.assertIn("monthly_salary_pay_day", row.details_json)
        self.assertNotIn("4500.00", row.details_json)
        self.assertNotIn("450000", row.details_json)

    def test_bulk_pay_rates_page_updates_rates_and_payment_methods(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii, encrypt_pii

        self._login(role="admin", user_id=523, username="adm523")
        emp_a = self._seed_employee(user_id=823, username="emp823")
        emp_b = self._seed_employee(user_id=824, username="emp824")

        profile_a = self.session.get(EmployeeProfile, emp_a.id)
        profile_a.hourly_rate_cents_enc = encrypt_pii("2300")
        profile_a.payment_method = "cash"
        profile_b = self.session.get(EmployeeProfile, emp_b.id)
        profile_b.hourly_rate_cents_enc = encrypt_pii("3000")
        profile_b.payment_method = "check"
        self.session.add_all([profile_a, profile_b])
        self.session.commit()

        page = self.client.get("/team/admin/employees/pay-rates")
        self.assertEqual(page.status_code, 200)
        self.assertIn("/team/admin/employees/823", page.text)
        self.assertIn('value="23.00"', page.text)

        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/pay-rates",
            data={
                "csrf_token": csrf,
                f"rate_{emp_a.id}": "24.50",
                f"payment_{emp_a.id}": "check",
                f"rate_{emp_b.id}": "",
                f"payment_{emp_b.id}": "cash",
            },
            follow_redirects=False,
        )

        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed_a = self.session.get(EmployeeProfile, emp_a.id)
        refreshed_b = self.session.get(EmployeeProfile, emp_b.id)
        self.assertEqual(decrypt_pii(refreshed_a.hourly_rate_cents_enc), "2450")
        self.assertEqual(refreshed_a.payment_method, "check")
        self.assertIsNone(refreshed_b.hourly_rate_cents_enc)
        self.assertEqual(refreshed_b.payment_method, "cash")

        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.pay_rates.bulk_update")
        ).one()
        self.assertIn("rate_changes", row.details_json)
        self.assertNotIn("24.50", row.details_json)
        self.assertNotIn("2450", row.details_json)

    def test_bulk_pay_rates_page_updates_monthly_salary_compensation(self):
        from app.models import AuditLog, EmployeeProfile
        from app.pii import decrypt_pii

        self._login(role="admin", user_id=525, username="adm525")
        emp = self._seed_employee(user_id=826, username="emp826")

        page = self.client.get("/team/admin/employees/pay-rates")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Monthly salary", page.text)
        self.assertIn(f'name="salary_{emp.id}"', page.text)
        self.assertIn(f'name="pay_day_{emp.id}"', page.text)

        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/pay-rates",
            data={
                "csrf_token": csrf,
                f"comp_{emp.id}": "monthly_salary",
                f"salary_{emp.id}": "5200.00",
                f"pay_day_{emp.id}": "31",
                f"payment_{emp.id}": "check",
            },
            follow_redirects=False,
        )

        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(EmployeeProfile, emp.id)
        self.assertEqual(refreshed.compensation_type, "monthly_salary")
        self.assertEqual(decrypt_pii(refreshed.monthly_salary_cents_enc), "520000")
        self.assertEqual(refreshed.monthly_salary_pay_day, 31)
        self.assertEqual(refreshed.payment_method, "check")

        row = self.session.exec(
            select(AuditLog).where(AuditLog.action == "admin.pay_rates.bulk_update")
        ).one()
        self.assertIn("monthly_salary_changes", row.details_json)
        self.assertIn("monthly_pay_day_changes", row.details_json)
        self.assertNotIn("5200.00", row.details_json)
        self.assertNotIn("520000", row.details_json)

    def test_bulk_compensation_only_updates_fields_for_selected_pay_type(self):
        from app.models import EmployeeProfile
        from app.pii import decrypt_pii, encrypt_pii
        from app.routers.team_admin_employees import _payroll_cost_summary

        self._login(role="admin", user_id=529, username="adm529")
        hourly = self._seed_employee(user_id=830, username="emp830")
        salary = self._seed_employee(user_id=831, username="emp831")
        owner = self._seed_employee(user_id=832, username="owner832")

        hourly_profile = self.session.get(EmployeeProfile, hourly.id)
        hourly_profile.compensation_type = "hourly"
        hourly_profile.monthly_salary_cents_enc = encrypt_pii("900000")
        salary_profile = self.session.get(EmployeeProfile, salary.id)
        salary_profile.compensation_type = "monthly_salary"
        salary_profile.hourly_rate_cents_enc = encrypt_pii("9900")
        salary_profile.monthly_salary_cents_enc = encrypt_pii("300000")
        owner_profile = self.session.get(EmployeeProfile, owner.id)
        owner_profile.compensation_type = "hourly"
        owner_profile.payment_method = "cash"
        self.session.add_all([hourly_profile, salary_profile, owner_profile])
        self.session.commit()

        page = self.client.get("/team/admin/employees/pay-rates")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Not paid", page.text)
        self.assertIn("data-hourly-input", page.text)
        self.assertIn("data-salary-input", page.text)

        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/pay-rates",
            data={
                "csrf_token": csrf,
                f"comp_{hourly.id}": "hourly",
                f"rate_{hourly.id}": "25.00",
                f"salary_{hourly.id}": "9999.00",
                f"pay_day_{hourly.id}": "22",
                f"payment_{hourly.id}": "check",
                f"comp_{salary.id}": "monthly_salary",
                f"rate_{salary.id}": "88.00",
                f"salary_{salary.id}": "4000.00",
                f"pay_day_{salary.id}": "15",
                f"payment_{salary.id}": "check",
                f"comp_{owner.id}": "unpaid",
                f"rate_{owner.id}": "99.00",
                f"salary_{owner.id}": "5000.00",
                f"pay_day_{owner.id}": "1",
                f"payment_{owner.id}": "check",
            },
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)

        self.session.expire_all()
        hourly_profile = self.session.get(EmployeeProfile, hourly.id)
        salary_profile = self.session.get(EmployeeProfile, salary.id)
        owner_profile = self.session.get(EmployeeProfile, owner.id)
        self.assertEqual(decrypt_pii(hourly_profile.hourly_rate_cents_enc), "2500")
        self.assertEqual(decrypt_pii(hourly_profile.monthly_salary_cents_enc), "900000")
        self.assertIsNone(hourly_profile.monthly_salary_pay_day)
        self.assertEqual(decrypt_pii(salary_profile.hourly_rate_cents_enc), "9900")
        self.assertEqual(decrypt_pii(salary_profile.monthly_salary_cents_enc), "400000")
        self.assertEqual(salary_profile.monthly_salary_pay_day, 15)
        self.assertEqual(owner_profile.compensation_type, "unpaid")
        self.assertIsNone(owner_profile.hourly_rate_cents_enc)
        self.assertIsNone(owner_profile.monthly_salary_cents_enc)
        self.assertEqual(owner_profile.payment_method, "cash")

        summary = _payroll_cost_summary(self.session)
        self.assertGreaterEqual(summary["unpaid_count"], 1)

    def test_bulk_pay_rates_sorts_unpaid_people_to_bottom(self):
        from app.models import EmployeeProfile
        from app.routers.team_admin_employees import _pay_rate_rows

        paid_a = self._seed_employee(user_id=833, username="aaa-paid")
        unpaid = self._seed_employee(user_id=834, username="bbb-unpaid")
        paid_z = self._seed_employee(user_id=835, username="zzz-paid")
        unpaid_profile = self.session.get(EmployeeProfile, unpaid.id)
        unpaid_profile.compensation_type = "unpaid"
        self.session.add(unpaid_profile)
        self.session.commit()

        rows = _pay_rate_rows(self.session)
        names = [
            row["user"].username
            for row in rows
            if row["user"].username in {"aaa-paid", "bbb-unpaid", "zzz-paid"}
        ]

        self.assertEqual(names, [paid_a.username, paid_z.username, unpaid.username])

    def test_pay_rate_rows_are_limited(self):
        from app.models import User
        from app.routers.team_admin_employees import PAY_RATE_PAGE_LIMIT, _pay_rate_rows

        users = [
            User(
                id=9000 + idx,
                username=f"limit-{idx:03d}",
                password_hash="x",
                password_salt="x",
                display_name=f"Limit {idx:03d}",
                role="employee",
                is_active=True,
            )
            for idx in range(PAY_RATE_PAGE_LIMIT + 5)
        ]
        self.session.add_all(users)
        self.session.commit()

        rows = _pay_rate_rows(self.session)

        self.assertEqual(len(rows), PAY_RATE_PAGE_LIMIT)

    def test_payroll_cost_summary_includes_salary_accrual_and_scheduled_hourly(self):
        from datetime import date

        from app.models import EmployeeProfile, ShiftEntry
        from app.pii import encrypt_pii
        from app.routers.team_admin_employees import _payroll_cost_summary

        self._login(role="admin", user_id=526, username="adm526")
        salaried = self._seed_employee(user_id=827, username="emp827")
        hourly = self._seed_employee(user_id=828, username="emp828")

        salaried_profile = self.session.get(EmployeeProfile, salaried.id)
        salaried_profile.compensation_type = "monthly_salary"
        salaried_profile.monthly_salary_cents_enc = encrypt_pii("300000")
        hourly_profile = self.session.get(EmployeeProfile, hourly.id)
        hourly_profile.compensation_type = "hourly"
        hourly_profile.hourly_rate_cents_enc = encrypt_pii("2000")
        self.session.add_all([salaried_profile, hourly_profile])
        self.session.add(
            ShiftEntry(
                user_id=hourly.id,
                shift_date=date(2026, 4, 24),
                label="10 AM - 2 PM",
                kind="work",
                created_by_user_id=526,
            )
        )
        self.session.commit()

        summary = _payroll_cost_summary(self.session, today=date(2026, 4, 24))
        periods = {row["key"]: row for row in summary["periods"]}
        self.assertEqual(periods["today"]["total_label"], "$180.00")
        self.assertEqual(periods["week_to_date"]["total_label"], "$580.00")
        self.assertEqual(periods["month_to_date"]["total_label"], "$2,480.00")
        self.assertEqual(summary["monthly_salary_commitment_label"], "$3,000.00")

    def test_payroll_cost_summary_prorates_monthly_salary_by_calendar_days(self):
        from datetime import date

        from app.models import EmployeeProfile
        from app.pii import encrypt_pii
        from app.routers.team_admin_employees import _payroll_cost_summary

        self._login(role="admin", user_id=527, username="adm527")
        salaried = self._seed_employee(user_id=829, username="emp829")
        profile = self.session.get(EmployeeProfile, salaried.id)
        profile.compensation_type = "monthly_salary"
        profile.monthly_salary_cents_enc = encrypt_pii("600000")
        self.session.add(profile)
        self.session.commit()

        summary = _payroll_cost_summary(self.session, today=date(2026, 4, 24))
        periods = {row["key"]: row for row in summary["periods"]}
        self.assertEqual(periods["today"]["salary_label"], "$200.00")
        self.assertEqual(periods["week_to_date"]["salary_label"], "$1,000.00")
        self.assertEqual(periods["month_to_date"]["salary_label"], "$4,800.00")


class ResetPasswordTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_admin_reset_request_queue_lists_open_requests(self):
        from app.models import AuditLog
        from app.routers.team_admin import _pending_password_reset_request_rows

        self._login(role="admin", user_id=300, username="adm-reset-queue")
        emp = self._seed_employee(user_id=700, username="emp700")
        self.session.add(
            AuditLog(
                target_user_id=emp.id,
                action="password.reset_manager_request",
                resource_key="admin.employees.reset_password",
                details_json=json.dumps({"source": "http_forgot"}),
            )
        )
        self.session.commit()

        rows = _pending_password_reset_request_rows(self.session)
        template = Path("app/templates/team/admin/password_reset_requests.html").read_text()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["user"].username, "emp700")
        self.assertIn("/team/admin/employees/{{ employee.id }}/reset-password", template)

    def test_admin_reset_shows_link_once_and_audits(self):
        from app.models import AuditLog
        self._login(role="admin", user_id=301, username="adm5")
        emp = self._seed_employee(user_id=701, username="emp701")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/reset-password",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("/team/password/reset/", r.text)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "password.reset_issued")
        ).all())
        self.assertGreaterEqual(len(rows), 1)


class TerminateAndPurgeTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_terminate_sets_inactive_and_audits(self):
        from app.models import AuditLog, User
        self._login(role="admin", user_id=401, username="adm6")
        emp = self._seed_employee(user_id=801, username="emp801")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/terminate",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(User, emp.id)
        self.assertFalse(refreshed.is_active)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.terminated")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_purge_wrong_username_rejected_and_preserves_pii(self):
        from app.models import AuditLog, EmployeeProfile
        self._login(role="admin", user_id=402, username="adm7")
        emp = self._seed_employee(user_id=802, username="emp802")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"csrf_token": csrf, "confirm_username": "wrong"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 400)
        self.session.expire_all()
        p = self.session.get(EmployeeProfile, emp.id)
        self.assertIsNotNone(p.phone_enc)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purged")
        ).all())
        self.assertEqual(len(rows), 0)

    def test_purge_correct_username_wipes_pii_and_audits(self):
        from app.models import AuditLog, EmployeeProfile, User
        self._login(role="admin", user_id=403, username="adm8")
        emp = self._seed_employee(user_id=803, username="emp803")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/employees/{emp.id}/purge",
            data={"csrf_token": csrf, "confirm_username": "PURGE"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        p = self.session.get(EmployeeProfile, emp.id)
        self.assertIsNone(p.phone_enc)
        self.assertIsNone(p.address_enc)
        self.assertIsNone(p.legal_name_enc)
        refreshed = self.session.get(User, emp.id)
        self.assertFalse(refreshed.is_active)
        purge_rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "account.purged")
        ).all())
        self.assertEqual(len(purge_rows), 1)
        # AuditLog as a whole should remain readable (not wiped).
        all_rows = list(self.session.exec(select(AuditLog)).all())
        self.assertGreaterEqual(len(all_rows), 1)


class InviteTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def test_issue_invite_shows_url_once_and_audits(self):
        from app.models import AuditLog, InviteToken
        self._login(role="admin", user_id=501, username="adm_inv")
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/invites/issue",
            data={"csrf_token": csrf, "role": "employee", "email_hint": "new@example.com"},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn("/team/invite/accept/", r.text)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "invite.issued")
        ).all())
        self.assertEqual(len(rows), 1)
        tokens = list(self.session.exec(select(InviteToken)).all())
        self.assertEqual(len(tokens), 1)

    def test_revoke_marks_used_and_audits(self):
        from app.auth import generate_invite_token
        from app.models import AuditLog, InviteToken

        self._login(role="admin", user_id=502, username="adm_rev")
        raw = generate_invite_token(
            self.session, role="employee", created_by_user_id=502
        )
        row = self.session.exec(select(InviteToken)).first()
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/invites/{row.id}/revoke",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        refreshed = self.session.get(InviteToken, row.id)
        self.assertIsNotNone(refreshed.used_at)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "invite.revoked")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_invites_list_renders_with_naive_expires_at(self):
        """Regression: SQLite returns `expires_at` as tz-naive, while
        `utcnow()` is tz-aware. The invites list page previously
        exploded with `TypeError: can't compare offset-naive and
        offset-aware datetimes`. It must not anymore."""
        from datetime import datetime, timedelta, timezone
        from app.models import InviteToken

        self._login(role="admin", user_id=503, username="adm_tz")

        # Build a tz-naive "now" the way SQLite hands it back (by stripping
        # tzinfo from an aware UTC datetime).
        naive_now = datetime.now(timezone.utc).replace(tzinfo=None)

        expired_row = InviteToken(
            token_hash="a" * 64,
            role="employee",
            email_hint="old@example.com",
            created_by_user_id=503,
            expires_at=naive_now - timedelta(days=1),
        )
        fresh_row = InviteToken(
            token_hash="b" * 64,
            role="employee",
            email_hint="new@example.com",
            created_by_user_id=503,
            expires_at=naive_now + timedelta(hours=6),
        )
        self.session.add(expired_row)
        self.session.add(fresh_row)
        self.session.commit()

        r = self.client.get("/team/admin/invites")
        self.assertEqual(r.status_code, 200)
        self.assertIn("expired", r.text)
        self.assertIn("outstanding", r.text)


class SupplyQueueTests(unittest.TestCase, _W4Harness):
    def setUp(self): self._setup()
    def tearDown(self): self._teardown()

    def _seed_request(self, submitted_by: int = 901, title: str = "Tape") -> int:
        from app.models import SupplyRequest
        row = SupplyRequest(
            submitted_by_user_id=submitted_by,
            title=title,
            description="need more",
            urgency="normal",
            status="submitted",
        )
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row.id

    def test_manager_sees_pending(self):
        self._login(role="manager", user_id=601, username="mgr_s")
        self._seed_request(submitted_by=601, title="Envelopes")
        r = self.client.get("/team/admin/supply")
        self.assertEqual(r.status_code, 200)
        self.assertIn("Envelopes", r.text)

    def test_approve_transitions_and_audits(self):
        from app.models import AuditLog, SupplyRequest
        self._login(role="manager", user_id=602, username="mgr_a")
        rid = self._seed_request(submitted_by=602, title="Boxes")
        csrf = self._csrf()
        r = self.client.post(
            f"/team/admin/supply/{rid}/approve",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 303)
        self.session.expire_all()
        row = self.session.get(SupplyRequest, rid)
        self.assertEqual(row.status, "approved")
        self.assertEqual(row.approved_by_user_id, 602)
        rows = list(self.session.exec(
            select(AuditLog).where(AuditLog.action == "supply.approved")
        ).all())
        self.assertEqual(len(rows), 1)

    def test_deny_and_mark_ordered(self):
        from app.models import AuditLog, SupplyRequest
        self._login(role="admin", user_id=603, username="adm_s")
        r1 = self._seed_request(submitted_by=603, title="Sleeves")
        csrf = self._csrf()
        self.client.post(
            f"/team/admin/supply/{r1}/approve",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.client.post(
            f"/team/admin/supply/{r1}/mark-ordered",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.session.expire_all()
        row = self.session.get(SupplyRequest, r1)
        self.assertEqual(row.status, "ordered")

        r2 = self._seed_request(submitted_by=603, title="Tape rolls")
        self.client.post(
            f"/team/admin/supply/{r2}/deny",
            data={"csrf_token": csrf, "notes": "not budgeted"},
            follow_redirects=False,
        )
        self.session.expire_all()
        row2 = self.session.get(SupplyRequest, r2)
        self.assertEqual(row2.status, "denied")
        self.assertIn("not budgeted", row2.notes)

    def test_manager_cannot_terminate(self):
        self._login(role="manager", user_id=604, username="mgr_t")
        self._seed_employee(user_id=904, username="emp904")
        csrf = self._csrf()
        r = self.client.post(
            "/team/admin/employees/904/terminate",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(r.status_code, 403)


if __name__ == "__main__":
    unittest.main()
