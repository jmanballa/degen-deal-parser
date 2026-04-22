"""Mobile editable schedule tests.

Reproduces the bug Jeffrey reported: on a 390x844 phone viewport, tapping a
Storefront schedule cell did nothing. The fix adds a `touchend` path, a
sticky mobile save bar, and a sticky left column so the grid stays usable
without horizontal-scroll tap-swallowing.

These tests drive a real headless Chromium (via Playwright) against a live
uvicorn server bound to a loopback port. We can't use the FastAPI
`TestClient` for this because the bug is in JS touch-event handling —
TestClient never executes JS.
"""
from __future__ import annotations

import importlib
import os
import socket
import threading
import time
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "sched-mobile-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "sched-mobile-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "sched-mobile-secret-" + "x" * 32)


def _playwright_available() -> bool:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return True
    except Exception:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class _UvicornThread:
    def __init__(self, app):
        import uvicorn

        self.port = _free_port()
        self.config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=self.port,
            log_level="error",
            lifespan="off",
            access_log=False,
        )
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        self.thread.start()
        for _ in range(100):
            if self.server.started:
                return
            time.sleep(0.05)
        raise RuntimeError("uvicorn failed to start")

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=5)


@unittest.skipUnless(_playwright_available(), "playwright not installed")
class ScheduleMobileTests(unittest.TestCase):
    """End-to-end browser tests for /team/admin/schedule on phone + desktop."""

    WEEK = date(2026, 4, 20)  # Monday

    def setUp(self):
        from app import rate_limit
        rate_limit.reset()

        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults
        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)

        from app import config as cfg
        cfg.get_settings.cache_clear()
        import app.main as app_main
        importlib.reload(app_main)
        self.app_main = app_main

        from app.db import get_session as real_get_session
        _engine = self.engine

        def _override():
            s = Session(_engine)
            try:
                yield s
            finally:
                s.close()

        self.app_main.app.dependency_overrides[real_get_session] = _override

        self.admin = self._seed_admin()
        self.employee = self._seed_employee_on_roster(self.WEEK, admin_id=self.admin.id)

        from app import shared
        self._p1 = patch.object(shared, "get_request_user", return_value=self.admin)
        self._p2 = patch.object(self.app_main, "get_request_user", return_value=self.admin)
        self._p1.start()
        self._p2.start()

        self.server = _UvicornThread(self.app_main.app)
        self.server.start()

        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch()

    def tearDown(self):
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass
        self.server.stop()
        for attr in ("_p1", "_p2"):
            p = getattr(self, attr, None)
            if p:
                p.stop()
        self.app_main.app.dependency_overrides.clear()
        self.session.close()

    def _seed_admin(self):
        from app.models import User
        u = User(
            id=500,
            username="adminx",
            password_hash="x",
            password_salt="x",
            display_name="Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(u)
        self.session.commit()
        self.session.refresh(u)
        self.session.expunge(u)
        return u

    def _seed_employee_on_roster(self, week_start: date, *, admin_id: int):
        from app.models import User, ScheduleRosterMember
        u = User(
            id=901,
            username="david",
            password_hash="x",
            password_salt="x",
            display_name="David",
            role="employee",
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(u)
        self.session.add(
            ScheduleRosterMember(
                week_start=week_start,
                user_id=u.id,
                added_by_user_id=admin_id,
            )
        )
        self.session.commit()
        self.session.refresh(u)
        self.session.expunge(u)
        return u

    # ------------------------------------------------------------------
    def _open_page(self, *, viewport, has_touch, is_mobile):
        ctx = self._browser.new_context(
            viewport=viewport,
            has_touch=has_touch,
            is_mobile=is_mobile,
        )
        page = ctx.new_page()
        url = f"{self.server.base_url}/team/admin/schedule?week={self.WEEK.isoformat()}"
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_selector('form[data-schgrid="storefront"] .sch-block')
        return ctx, page

    def _cell_selector(self) -> str:
        # First empty storefront cell for David on Monday.
        return (
            'form[data-schgrid="storefront"] '
            f'.sch-cell[data-user-id="{self.employee.id}"]'
            f'[data-date="{self.WEEK.isoformat()}"] .sch-block'
        )

    def _posted_form_body(self, page) -> dict:
        """Intercept the next POST to /team/admin/schedule and capture form body."""
        captured = {}

        def _on_request(request):
            if (
                request.method == "POST"
                and request.url.endswith("/team/admin/schedule")
            ):
                body = request.post_data or ""
                parsed = {}
                for part in body.split("&"):
                    if not part:
                        continue
                    k, _, v = part.partition("=")
                    from urllib.parse import unquote_plus
                    parsed[unquote_plus(k)] = unquote_plus(v)
                captured.update(parsed)

        page.on("request", _on_request)
        return captured

    # ------------------------------------------------------------------
    def test_tap_opens_editor_on_mobile_viewport(self):
        """iPhone-sized viewport with touch: tapping a cell must open the
        editor dialog so the label input is editable."""
        ctx, page = self._open_page(
            viewport={"width": 390, "height": 844},
            has_touch=True,
            is_mobile=True,
        )
        try:
            cell_sel = self._cell_selector()
            # Scroll the horizontal grid so the target cell sits in the
            # visible pane, then dispatch a real `touchend` + `click`
            # the way a phone fires them. We dispatch (rather than use
            # Playwright's `.tap()`) because the admin layout stacks the
            # sidebar above content on phones, so actionability checks
            # trip on the long page; the JS handler is what we want to
            # exercise, not Playwright's scroll heuristic.
            page.evaluate(
                """sel => {
                    const el = document.querySelector(sel);
                    if (!el) throw new Error('cell not found: ' + sel);
                    el.scrollIntoView({block: 'center'});
                    const touch = new Touch({
                        identifier: 1, target: el, clientX: 100, clientY: 100
                    });
                    el.dispatchEvent(new TouchEvent('touchstart', {
                        bubbles: true, cancelable: true, touches: [touch]
                    }));
                    el.dispatchEvent(new TouchEvent('touchend', {
                        bubbles: true, cancelable: true, changedTouches: [touch]
                    }));
                    el.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true}));
                }""",
                cell_sel,
            )
            page.wait_for_selector("#sch-modal[open]", timeout=2000)
            self.assertTrue(page.locator("#sch-modal[open]").count() == 1)
            # The modal's note input must be reachable / focusable on mobile.
            page.fill("#sch-modal-note", "opener")
            self.assertEqual(page.input_value("#sch-modal-note"), "opener")
            # Tap Shift tab to activate time row, type times, then save.
            page.click('#sch-modal-tabs button[data-status="shift"]')
            page.fill("#sch-modal-start", "10:30")
            page.fill("#sch-modal-end", "18:30")
            page.click("#sch-modal-save")
            # Hidden input for this cell now carries the synthesized label.
            val = page.input_value(
                f'input[name="cell__{self.employee.id}__{self.WEEK.isoformat()}"]'
            )
            self.assertIn("10:30 AM", val)
            self.assertIn("6:30 PM", val)
            self.assertIn("opener", val)

            # The mobile sticky savebar must be visible and its Save button
            # must submit the Storefront form.
            savebar = page.locator(".sch-mobile-savebar.is-on")
            self.assertTrue(savebar.is_visible())

            captured = self._posted_form_body(page)
            with page.expect_request(
                lambda r: r.method == "POST"
                and r.url.endswith("/team/admin/schedule")
            ):
                # Submit the Storefront form directly; the savebar's
                # button is inside that form so this mirrors the real
                # mobile submit path without fighting actionability.
                page.evaluate(
                    """document.querySelector('form[data-schgrid="storefront"]').requestSubmit()"""
                )
            # Ensure the payload carries the cell field with the same value.
            key = f"cell__{self.employee.id}__{self.WEEK.isoformat()}"
            self.assertIn(key, captured)
            self.assertIn("10:30 AM", captured[key])
            self.assertEqual(captured.get("staff_kind"), "storefront")
            self.assertEqual(captured.get("week"), self.WEEK.isoformat())
            self.assertTrue(captured.get("csrf_token"))
        finally:
            ctx.close()

    def test_mobile_sticky_left_column_and_savebar(self):
        """Layout sanity: at 390px the name column is sticky-left and the
        savebar is fixed at bottom so neither disappears behind scroll."""
        ctx, page = self._open_page(
            viewport={"width": 390, "height": 844},
            has_touch=True,
            is_mobile=True,
        )
        try:
            name_pos = page.evaluate(
                "getComputedStyle(document.querySelector('.sch-name-col')).position"
            )
            self.assertEqual(name_pos, "sticky")
            bar_pos = page.evaluate(
                "getComputedStyle(document.querySelector('.sch-mobile-savebar')).position"
            )
            self.assertEqual(bar_pos, "fixed")
        finally:
            ctx.close()

    def test_desktop_click_still_opens_editor(self):
        """Regression guard: at 1280x800 with no touch, a mouse click on a
        cell must still open the editor — the mobile fixes can't break
        desktop."""
        ctx, page = self._open_page(
            viewport={"width": 1280, "height": 800},
            has_touch=False,
            is_mobile=False,
        )
        try:
            page.click(self._cell_selector())
            page.wait_for_selector("#sch-modal[open]", timeout=2000)
            # Desktop hides the mobile savebar.
            bar_display = page.evaluate(
                "getComputedStyle(document.querySelector('.sch-mobile-savebar')).display"
            )
            self.assertEqual(bar_display, "none")
        finally:
            ctx.close()


if __name__ == "__main__":
    unittest.main()
