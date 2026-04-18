"""Verify mobile PWA chrome (bottom nav, viewport-fit, safe-area.css) renders on every
page a signed-in user hits on their iPhone.

Calls handlers directly with a signed request, same pattern as tests/test_navigation.py.
Exits non-zero on any failure.
"""
import shutil
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request


def make_request(path: str, role: str = "admin") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        username="tester",
        display_name="Test Operator",
        role=role,
    )
    return request


def check_html(path: str, html: str, expect_nav: bool = True) -> list[str]:
    failures: list[str] = []
    if 'viewport-fit=cover' not in html:
        failures.append("viewport-fit=cover missing from <meta viewport>")
    if '/static/safe-area.css' not in html:
        failures.append("<link> to /static/safe-area.css missing")
    if expect_nav:
        if 'class="mobile-bottom-nav"' not in html:
            failures.append("<nav class=\"mobile-bottom-nav\"> missing")
        for href in ('href="/dashboard"', 'href="/tiktok/streamer"',
                     'href="/degen_eye"', 'href="/deals"', 'href="/review"'):
            if href not in html:
                failures.append(f"nav link {href!r} missing")
    return failures


def check_safe_area_css() -> list[str]:
    css = Path("app/static/safe-area.css").read_text(encoding="utf-8")
    failures: list[str] = []
    if 'env(safe-area-inset-top)' not in css:
        failures.append("safe-area.css does not reference env(safe-area-inset-top)")
    if '.mobile-bottom-nav' not in css:
        failures.append("safe-area.css does not include .mobile-bottom-nav rules")
    return failures


def main() -> int:
    from app.routers.dashboard import dashboard_page
    from app.routers.deals import deals_page, login_page
    from app.routers.messages import reviewer_queue_page
    from app.routers.tiktok_streamer import tiktok_streamer_page
    from app import inventory as inventory_module

    temp_dir = Path.cwd() / "tests" / ".tmp_verify_chrome" / str(uuid.uuid4())
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path = temp_dir / "verify.db"
    engine = create_engine(
        f"sqlite:///{db_path.as_posix()}",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(engine)

    overall_failures: dict[str, list[str]] = {}

    try:
        patches = [
            patch("app.routers.dashboard.require_role_response", return_value=None),
            patch("app.routers.messages.require_role_response", return_value=None),
            patch("app.routers.messages.get_available_channel_choices", return_value=([], False)),
            patch("app.routers.deals.require_role_response", return_value=None),
            patch("app.routers.tiktok_streamer.require_role_response", return_value=None),
            patch.object(inventory_module, "_require_viewer", return_value=None),
        ]
        for p in patches:
            p.start()

        try:
            with Session(engine) as session:
                pages: list[tuple[str, callable, bool]] = [
                    (
                        "/dashboard",
                        lambda: dashboard_page(make_request("/dashboard"), session=session),
                        True,
                    ),
                    (
                        "/tiktok/streamer",
                        lambda: tiktok_streamer_page(make_request("/tiktok/streamer"), session=session),
                        True,
                    ),
                    (
                        "/degen_eye",
                        lambda: _sync(inventory_module.inventory_scan_pokemon_page(make_request("/degen_eye"))),
                        True,
                    ),
                    (
                        "/deals",
                        lambda: deals_page(
                            make_request("/deals"),
                            channel_id=None,
                            entry_kind=None,
                            after=None,
                            before=None,
                            page=1,
                            limit=25,
                            session=session,
                        ),
                        True,
                    ),
                    (
                        "/review",
                        lambda: reviewer_queue_page(
                            make_request("/review"),
                            channel_id=None,
                            expense_category=None,
                            after=None,
                            before=None,
                            sort_by="time",
                            sort_dir="desc",
                            page=1,
                            limit=25,
                            success=None,
                            error=None,
                            session=session,
                        ),
                        True,
                    ),
                    (
                        "/login",
                        lambda: login_page(make_request("/login"), error=None),
                        False,
                    ),
                ]

                for label, call, expect_nav in pages:
                    try:
                        resp = call()
                    except Exception as exc:
                        overall_failures[label] = [f"handler raised {type(exc).__name__}: {exc}"]
                        continue
                    status = getattr(resp, "status_code", 0)
                    body = getattr(resp, "body", b"")
                    if isinstance(body, bytes):
                        html = body.decode("utf-8", errors="replace")
                    else:
                        html = str(body)
                    if status != 200:
                        # login is allowed to redirect (303) for an authed user — fetch raw template instead.
                        if label == "/login" and status in (301, 303):
                            html = Path("app/templates/login.html").read_text(encoding="utf-8")
                        else:
                            overall_failures[label] = [f"unexpected status {status}"]
                            continue
                    failures = check_html(label, html, expect_nav=expect_nav)
                    if failures:
                        overall_failures[label] = failures
        finally:
            for p in patches:
                p.stop()
    finally:
        engine.dispose()
        shutil.rmtree(temp_dir, ignore_errors=True)

    css_failures = check_safe_area_css()
    if css_failures:
        overall_failures["safe-area.css"] = css_failures

    for label, _, expect_nav in []:
        pass

    all_labels = ["/dashboard", "/tiktok/streamer", "/degen_eye", "/deals", "/review", "/login", "safe-area.css"]
    for label in all_labels:
        if label in overall_failures:
            print(f"  {label}: FAIL")
            for f in overall_failures[label]:
                print(f"      - {f}")
        else:
            if label == "safe-area.css":
                print(f"  {label}: OK  (env(safe-area-inset) + .mobile-bottom-nav rules present)")
            elif label == "/login":
                print(f"  {label}: OK  viewport=yes safearea=yes (splash, no nav expected)")
            else:
                print(f"  {label}: OK  nav=yes safearea=yes viewport=yes")

    return 1 if overall_failures else 0


def _sync(coro):
    import asyncio
    return asyncio.get_event_loop().run_until_complete(coro) if hasattr(coro, "__await__") else coro


if __name__ == "__main__":
    sys.exit(main())
