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
from types import SimpleNamespace
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
        # edit=1 opts the admin into edit mode (the default is now a
        # read-only view for everyone). Without it, cells render as
        # static blocks and the modal never opens.
        url = (
            f"{self.server.base_url}/team/admin/schedule"
            f"?week={self.WEEK.isoformat()}&edit=1"
        )
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
            untouched_key = f"cell__{self.employee.id}__{(self.WEEK + timedelta(days=1)).isoformat()}"
            self.assertEqual(
                page.locator(f'input[name="{untouched_key}"]').count(),
                0,
                "untouched empty cells must not have named hidden inputs",
            )
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
            page.wait_for_selector("#sch-modal.is-open", timeout=2000)
            self.assertTrue(page.locator("#sch-modal.is-open").count() == 1)
            # The modal's note input must be reachable / focusable on mobile.
            # We use JS-driven interactions because Playwright's actionability
            # check interacts poorly with `is_mobile=True` emulation when a
            # `position: fixed` element sits outside the 844px "visual"
            # viewport (the emulated layout viewport is ~2294px tall). Real
            # iOS Safari taps work fine on the fixed overlay — we're just
            # working around a test-harness quirk, not a product bug.
            page.evaluate(
                """() => {
                    const n = document.getElementById('sch-modal-note');
                    n.value = 'opener';
                    n.dispatchEvent(new Event('input', {bubbles: true}));
                }"""
            )
            self.assertEqual(page.input_value("#sch-modal-note"), "opener")
            page.evaluate(
                """() => {
                    document.querySelector('#sch-modal-tabs button[data-status=\"shift\"]').click();
                    const s = document.getElementById('sch-modal-start');
                    s.value = '10:30';
                    s.dispatchEvent(new Event('input', {bubbles:true}));
                    const e = document.getElementById('sch-modal-end');
                    e.value = '18:30';
                    e.dispatchEvent(new Event('input', {bubbles:true}));
                    document.getElementById('sch-modal-save').click();
                }"""
            )
            # Hidden input for this cell now carries the synthesized label.
            val = page.input_value(
                f'input[name="cell__{self.employee.id}__{self.WEEK.isoformat()}"]'
            )
            self.assertIn("10:30 AM", val)
            self.assertIn("6:30 PM", val)
            self.assertIn("opener", val)

            # The inline Save button (no longer a sticky savebar) is inside
            # the storefront form and must be present in edit mode.
            save_btn = page.locator(
                'form[data-schgrid="storefront"] .sch-edit-btn.primary[type="submit"]'
            )
            self.assertTrue(save_btn.count() >= 1)

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
            self.assertNotIn(
                untouched_key,
                captured,
                "saving one cell must not submit untouched empty cells",
            )
            self.assertEqual(captured.get("cleared_cells"), "[]")
            self.assertEqual(captured.get("staff_kind"), "storefront")
            self.assertEqual(captured.get("week"), self.WEEK.isoformat())
            self.assertTrue(captured.get("csrf_token"))
        finally:
            ctx.close()

    def test_touch_drag_then_click_does_not_open_editor_on_mobile_viewport(self):
        """A scroll/drag gesture ending on a shift must not become a tap."""
        ctx, page = self._open_page(
            viewport={"width": 390, "height": 844},
            has_touch=True,
            is_mobile=True,
        )
        try:
            supports_synthetic_drag = page.evaluate(
                """() => {
                    let pointerOk = false;
                    let touchOk = false;
                    try {
                        if (window.PointerEvent) {
                            new PointerEvent('pointerdown', {pointerType: 'touch'});
                            pointerOk = true;
                        }
                    } catch (_) {}
                    try {
                        if (window.TouchEvent && typeof Touch === 'function') {
                            const target = document.body;
                            const touch = new Touch({
                                identifier: 1,
                                target,
                                clientX: 1,
                                clientY: 1,
                            });
                            new TouchEvent('touchstart', {touches: [touch]});
                            touchOk = true;
                        }
                    } catch (_) {}
                    return pointerOk || touchOk;
                }"""
            )
            if not supports_synthetic_drag:
                self.skipTest("browser cannot synthesize touch/pointer drag")

            page.evaluate(
                """sel => {
                    const el = document.querySelector(sel);
                    if (!el) throw new Error('cell not found: ' + sel);
                    el.scrollIntoView({block: 'center'});
                    const start = {x: 120, y: 120};
                    const end = {x: 124, y: 148};

                    let pointerEvents = null;
                    try {
                        if (window.PointerEvent) {
                            pointerEvents = [
                                new PointerEvent('pointerdown', {
                                    bubbles: true,
                                    cancelable: true,
                                    pointerId: 7,
                                    pointerType: 'touch',
                                    clientX: start.x,
                                    clientY: start.y,
                                }),
                                new PointerEvent('pointermove', {
                                    bubbles: true,
                                    cancelable: true,
                                    pointerId: 7,
                                    pointerType: 'touch',
                                    clientX: end.x,
                                    clientY: end.y,
                                }),
                                new PointerEvent('pointerup', {
                                    bubbles: true,
                                    cancelable: true,
                                    pointerId: 7,
                                    pointerType: 'touch',
                                    clientX: end.x,
                                    clientY: end.y,
                                }),
                            ];
                        }
                    } catch (_) {
                        pointerEvents = null;
                    }
                    if (pointerEvents) pointerEvents.forEach(ev => el.dispatchEvent(ev));

                    try {
                        if (!(window.TouchEvent && typeof Touch === 'function')) {
                            throw new Error('Touch constructor unavailable');
                        }
                        const t0 = new Touch({
                            identifier: 9,
                            target: el,
                            clientX: start.x,
                            clientY: start.y,
                        });
                        const t1 = new Touch({
                            identifier: 9,
                            target: el,
                            clientX: end.x,
                            clientY: end.y,
                        });
                        el.dispatchEvent(new TouchEvent('touchstart', {
                            bubbles: true,
                            cancelable: true,
                            touches: [t0],
                            changedTouches: [t0],
                        }));
                        el.dispatchEvent(new TouchEvent('touchmove', {
                            bubbles: true,
                            cancelable: true,
                            touches: [t1],
                            changedTouches: [t1],
                        }));
                        el.dispatchEvent(new TouchEvent('touchend', {
                            bubbles: true,
                            cancelable: true,
                            changedTouches: [t1],
                        }));
                    } catch (_) {}

                    el.dispatchEvent(new MouseEvent('click', {
                        bubbles: true,
                        cancelable: true,
                        clientX: end.x,
                        clientY: end.y,
                    }));
                }""",
                self._cell_selector(),
            )
            page.wait_for_timeout(150)
            self.assertEqual(
                page.locator("#sch-modal.is-open").count(),
                0,
                "scroll gestures must not open the shift editor",
            )
        finally:
            ctx.close()

    def test_mobile_sticky_left_column_and_edit_actions(self):
        """Layout sanity: at 390px the name column is sticky-left so it
        doesn't disappear behind horizontal scroll, and the inline Save /
        Discard action row is present (the old sticky savebar is gone)."""
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
            # Inline edit actions (Save + Discard) sit at the bottom of
            # each editable grid form and are always static (not fixed).
            actions = page.locator(
                'form[data-schgrid="storefront"] .sch-grid-actions'
            )
            self.assertTrue(actions.count() >= 1)
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
            page.wait_for_selector("#sch-modal.is-open", timeout=2000)
            # The edit-action row (Save + Discard) must always be visible
            # on desktop in edit mode.
            actions_visible = page.evaluate(
                "getComputedStyle(document.querySelector('form[data-schgrid=\"storefront\"] .sch-grid-actions')).display"
            )
            self.assertNotEqual(actions_visible, "none")
        finally:
            ctx.close()


@unittest.skipUnless(_playwright_available(), "playwright not installed")
class ScheduleMobileWebKitTests(ScheduleMobileTests):
    """iOS Safari surrogate.

    The <dialog> regression Jeffrey hit on his real phone only reproduces
    on WebKit (Safari, iOS WebView): Chromium ships <dialog> support
    unconditionally, so the Chromium-only tests above passed even while
    tap-to-edit was broken in production. This subclass re-runs the same
    cases against Playwright's WebKit build, which is the closest
    headless stand-in for iOS Safari we can run in CI.
    """

    def setUp(self):
        super().setUp()
        # Replace the Chromium browser the parent opened with a WebKit one.
        try:
            self._browser.close()
        except Exception:
            pass
        self._browser = self._pw.webkit.launch()

    def test_tap_opens_editor_on_mobile_viewport(self):
        """WebKit tap path.

        WebKit lacks the `new Touch(...)` JS constructor (it's Chrome-only),
        so we synthesize the tap by directly clicking the cell via JS. The
        critical assertion vs the prior bug: before this fix, the bootstrap
        bailed early on WebKit because `typeof modal.showModal !== 'function'`
        — so the click handler was NEVER bound and this would never open.
        """
        ctx, page = self._open_page(
            viewport={"width": 390, "height": 844},
            has_touch=True,
            is_mobile=True,
        )
        try:
            cell_sel = self._cell_selector()
            page.evaluate(
                """sel => {
                    const el = document.querySelector(sel);
                    if (!el) throw new Error('cell not found: ' + sel);
                    el.scrollIntoView({block: 'center'});
                    el.click();
                }""",
                cell_sel,
            )
            page.wait_for_selector("#sch-modal.is-open", timeout=2000)
            self.assertTrue(page.locator("#sch-modal.is-open").count() == 1)
            # aria-hidden flips correctly once the overlay opens.
            self.assertEqual(
                page.get_attribute("#sch-modal", "aria-hidden"),
                "false",
            )
            # Escape closes the overlay (part of the focus-trap contract).
            page.evaluate(
                """() => document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true}))"""
            )
            self.assertEqual(
                page.locator("#sch-modal.is-open").count(),
                0,
                "Escape should remove .is-open class and close the overlay",
            )
        finally:
            ctx.close()


class ScheduleMobileSummaryRenderTests(unittest.TestCase):
    """Direct-render mobile guards for the expanded summary/header markup.

    These avoid TestClient and browser startup; the existing Playwright tests
    above still cover real tap behavior.
    """

    WEEK = date(2026, 4, 20)

    def setUp(self):
        self.engine = _fresh_engine()
        from app.db import seed_employee_portal_defaults

        self.session = Session(self.engine)
        seed_employee_portal_defaults(self.session)
        self.admin = self._seed_admin()
        self._seed_storefront_roster()

    def tearDown(self):
        self.session.close()

    def _seed_admin(self):
        from app.models import User

        u = User(
            id=6500,
            username="mobileadmin",
            password_hash="x",
            password_salt="x",
            display_name="Mobile Admin",
            role="admin",
            is_active=True,
        )
        self.session.add(u)
        self.session.commit()
        return u

    def _seed_storefront_roster(self):
        from app.models import (
            EmployeeProfile,
            ScheduleRosterMember,
            SHIFT_KIND_WORK,
            ShiftEntry,
            User,
        )
        from app.pii import encrypt_pii

        paid = User(
            id=6501,
            username="paidmobile",
            password_hash="x",
            password_salt="x",
            display_name="Paid Mobile",
            role="employee",
            is_active=True,
            is_schedulable=True,
        )
        missing = User(
            id=6502,
            username="missingmobile",
            password_hash="x",
            password_salt="x",
            display_name="Missing Mobile",
            role="employee",
            is_active=True,
            is_schedulable=True,
        )
        self.session.add(paid)
        self.session.add(missing)
        self.session.add(
            EmployeeProfile(
                user_id=paid.id,
                hourly_rate_cents_enc=encrypt_pii("2500"),
            )
        )
        for u in (paid, missing):
            self.session.add(
                ScheduleRosterMember(
                    week_start=self.WEEK,
                    user_id=u.id,
                    added_by_user_id=self.admin.id,
                )
            )
        self.session.add(
            ShiftEntry(
                user_id=paid.id,
                shift_date=self.WEEK,
                label="10-6",
                kind=SHIFT_KIND_WORK,
                created_by_user_id=self.admin.id,
            )
        )
        self.session.commit()

    def _render_direct(self) -> str:
        from app import shared
        from app.models import STAFF_KIND_STOREFRONT, STAFF_KIND_STREAM
        from app.routers.team_admin_schedule import (
            _build_cell_key,
            _build_day_loc_key,
            _grid_context,
        )

        storefront = _grid_context(
            self.session,
            self.WEEK,
            staff_kind=STAFF_KIND_STOREFRONT,
        )
        stream = _grid_context(
            self.session,
            self.WEEK,
            staff_kind=STAFF_KIND_STREAM,
        )
        request = SimpleNamespace(
            state=SimpleNamespace(
                can_view_admin_announcements=False,
                can_view_admin_timeoff=False,
            ),
            url=SimpleNamespace(path="/team/admin/schedule"),
        )
        template = shared.templates.env.get_template("team/admin/schedule.html")
        return template.render(
            request=request,
            title="Schedule",
            active="schedule",
            current_user=self.admin,
            can_edit=True,
            edit_mode=True,
            stream_accounts=[],
            stream_account_colors={},
            holiday_options=[],
            custom_closures=[],
            csrf_token="csrf-token",
            build_cell_key=_build_cell_key,
            build_day_loc_key=_build_day_loc_key,
            storefront=storefront,
            stream=stream,
            week_start=storefront["week_start"],
            week_start_iso=storefront["week_start_iso"],
            week_days=storefront["week_days"],
            day_note_map=storefront["day_note_map"],
            prev_week=storefront["prev_week"],
            next_week=storefront["next_week"],
            this_week=storefront["this_week"],
            is_current_week=storefront["is_current_week"],
            today=self.WEEK,
            flash=None,
        )

    def test_summary_strip_visible_on_mobile(self):
        html = self._render_direct()
        self.assertIn("sch-summary", html)
        self.assertIn("Labor this week", html)
        self.assertIn("$200.00", html)
        self.assertIn("⚠ 1 missing rates", html)

    def test_cells_still_editable_with_new_headers(self):
        html = self._render_direct()
        self.assertIn('form method="post" action="/team/admin/schedule" data-schgrid="storefront"', html)
        self.assertIn("sch-cell-edit", html)
        self.assertIn("Save storefront schedule", html)

    def test_totals_column_does_not_break_table_layout(self):
        html = self._render_direct()
        self.assertIn("sch-total-head", html)
        self.assertIn("sch-total-col", html)
        self.assertIn("Daily hours", html)


if __name__ == "__main__":
    unittest.main()
