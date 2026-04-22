import re
import shutil
import unittest
import uuid
from html import unescape
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

import app.main as main_module
from app.shared import app_home_for_role, REPORT_SOURCE_ALL, FINANCE_WINDOW_MTD
from app.routers.deals import deals_page, login_page
from app.routers.dashboard import dashboard_page, partner_page, status_page
from app.routers.messages import reviewer_queue_page, review_table, messages_table
from app.routers.admin import admin_home_page, admin_debug_page, admin_health_page
from app.routers.reports import reports_page, finance_page
from app.routers.bookkeeping import bookkeeping_page
from app.routers.shopify import shopify_orders_page


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


def make_request_with_query_string(path: str, query_string: str, role: str = "admin") -> Request:
    request = make_request(path, role=role)
    request.scope["query_string"] = query_string.encode("latin-1")
    return request


def read_template(name: str) -> str:
    return Path("app/templates", name).read_text(encoding="utf-8")


def visible_template_text(source: str) -> str:
    text = re.sub(r"{#.*?#}", " ", source, flags=re.DOTALL)
    text = re.sub(r"{%.*?%}", " ", text, flags=re.DOTALL)
    text = re.sub(r"{{.*?}}", " ", text, flags=re.DOTALL)
    text = re.sub(
        r"<details[^>]*class=\"[^\"]*tech-details[^\"]*\"[^>]*>.*?</details>",
        " ",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


class NavigationValidationTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_navigation" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "navigation.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_workspace_nav_contains_expected_sections_and_home_link(self) -> None:
        source = read_template("_workspace_nav.html")

        self.assertIn('href="/dashboard"', source)
        self.assertIn(">Workspace<", source)
        self.assertIn(">Operators<", source)

        workspace_links = [
            ('href="/dashboard">Dashboard<'),
            ('href="/review">Review<'),
            ('href="/deals">Deals<'),
            ('href="/finance">Finance<'),
            ('href="/reports">Reports<'),
            ('href="/bookkeeping">Bookkeeping<'),
            ('href="/shopify/orders">Shopify Orders<'),
        ]
        operator_links = [
            ('href="/table">Table<'),
            ('href="/status">Status<'),
            ('href="/admin">Admin<'),
        ]

        for expected in workspace_links + operator_links:
            self.assertIn(expected, source)

        self.assertNotIn('href="/review-table"', source)

    def test_admin_role_home_redirects_to_dashboard(self) -> None:
        self.assertEqual(app_home_for_role("admin"), "/dashboard")
        self.assertEqual(app_home_for_role("owner"), "/dashboard")

        with patch("app.routers.deals.get_request_user", return_value=SimpleNamespace(role="admin")):
            response = login_page(make_request("/login"))
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers.get("location"), "/dashboard")

    def test_operator_view_badge_is_present_on_operator_templates(self) -> None:
        operator_templates = {
            "/table": "messages_table.html",
            "/status": "status.html",
            "/admin": "admin_home.html",
            "/admin/debug": "admin_debug.html",
            "/admin/health": "admin_health.html",
            "/ops-log": "ops_log.html",
            "/review-table": "messages_table.html",
        }

        missing = [
            route
            for route, template_name in operator_templates.items()
            if "Operator View" not in read_template(template_name)
        ]
        self.assertEqual(missing, [])

    def test_partner_facing_templates_do_not_show_internal_jargon_as_visible_text(self) -> None:
        # The deal page now includes an operator-only <details class="tech-details"> block.
        # This assertion should keep partner-facing copy clean while exempting collapsed
        # technical content that is intentionally reserved for operators.
        forbidden_terms = [
            "parse_status",
            "review_required",
            "processing",
            "worker",
            "backfill",
            "reparse",
            "heartbeat",
            "parser",
            "normalized",
        ]
        templates = [
            "partner.html",
            "deal_detail.html",
            "dashboard.html",
        ]

        found = {}
        for template_name in templates:
            text = visible_template_text(read_template(template_name))
            matches = [term for term in forbidden_terms if term in text]
            if matches:
                found[template_name] = matches

        self.assertEqual(found, {})

    def test_review_table_is_removed_from_shared_nav_and_cross_linked_from_review(self) -> None:
        nav_source = read_template("_workspace_nav.html")
        review_source = read_template("review_queue.html")
        review_table_source = read_template("messages_table.html")

        self.assertNotIn('href="/review-table"', nav_source)
        self.assertIn('href="/review-table"', review_source)
        self.assertIn("Advanced: open the bulk admin review table", review_source)
        self.assertIn('href="/review"', review_table_source)
        self.assertIn("Open Primary Review Queue", review_table_source)
        self.assertIn("Review Queue", review_table_source)
        self.assertIn("Operator View", review_table_source)

    def test_dashboard_is_home_and_status_admin_have_dashboard_back_links(self) -> None:
        nav_source = read_template("_workspace_nav.html")
        status_source = read_template("status.html")
        admin_source = read_template("admin_home.html")

        self.assertIn('href="/dashboard" aria-label="Open dashboard"', nav_source)
        self.assertIn('class="streamer-title-wrap" href="/dashboard"', nav_source)
        self.assertIn('href="/dashboard">&larr; Dashboard<', status_source)
        self.assertIn('href="/dashboard">&larr; Dashboard<', admin_source)

    def test_key_navigation_pages_return_http_200(self) -> None:
        with Session(self.engine) as session:
            ok_routes = [
                (
                    "/dashboard",
                    lambda request: dashboard_page(request, session=session),
                ),
                (
                    "/review",
                    lambda request: reviewer_queue_page(
                        request,
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
                ),
                (
                    "/review-table",
                    lambda request: review_table(
                        request,
                        channel_id=None,
                        expense_category=None,
                        after=None,
                        before=None,
                        sort_by="time",
                        sort_dir="desc",
                        page=1,
                        limit=100,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
                (
                    "/table",
                    lambda request: messages_table(
                        request,
                        status=None,
                        channel_id=None,
                        expense_category=None,
                        source=REPORT_SOURCE_ALL,
                        after=None,
                        before=None,
                        sort_by="time",
                        sort_dir="desc",
                        page=1,
                        limit=100,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
                (
                    "/status",
                    lambda request: status_page(request, session=session),
                ),
                (
                    "/admin",
                    lambda request: admin_home_page(request, session=session),
                ),
                (
                    "/status",
                    lambda request: status_page(request, session=session),
                ),
                (
                    "/deals",
                    lambda request: deals_page(
                        request,
                        channel_id=None,
                        entry_kind=None,
                        after=None,
                        before=None,
                        page=1,
                        limit=25,
                        session=session,
                    ),
                ),
                (
                    "/reports",
                    lambda request: reports_page(
                        request,
                        start=None,
                        end=None,
                        channel_id=None,
                        entry_kind=None,
                        source=REPORT_SOURCE_ALL,
                        session=session,
                    ),
                ),
                (
                    "/finance",
                    lambda request: finance_page(
                        request,
                        start=None,
                        end=None,
                        window=FINANCE_WINDOW_MTD,
                        session=session,
                    ),
                ),
                (
                    "/bookkeeping",
                    lambda request: bookkeeping_page(
                        request,
                        import_id=None,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
                (
                    "/shopify/orders",
                    lambda request: shopify_orders_page(
                        request,
                        start=None,
                        end=None,
                        financial_status=None,
                        source=None,
                        search=None,
                        sort_by="date",
                        sort_dir="desc",
                        page=1,
                        success=None,
                        error=None,
                        session=session,
                    ),
                ),
            ]
            redirect_routes = [
                (
                    "/partner",
                    lambda request: partner_page(request, session=session),
                    "/dashboard",
                ),
                (
                    "/admin/health",
                    lambda request: admin_health_page(request, session=session),
                    "/status",
                ),
                (
                    "/admin/debug",
                    lambda request: admin_debug_page(request, session=session),
                    "/status",
                ),
            ]

            patches = [
                patch("app.routers.dashboard.require_role_response", return_value=None),
                patch("app.routers.messages.require_role_response", return_value=None),
                patch("app.routers.messages.get_available_channel_choices", return_value=([], False)),
                patch("app.routers.admin.require_role_response", return_value=None),
                patch("app.routers.deals.require_role_response", return_value=None),
                patch("app.routers.reports.require_role_response", return_value=None),
                patch("app.routers.bookkeeping.require_role_response", return_value=None),
                patch("app.routers.shopify.require_role_response", return_value=None),
            ]
            for p in patches:
                p.start()
            try:
                for path, call in ok_routes:
                    response = call(make_request(path))
                    self.assertEqual(response.status_code, 200, path)
                    self.assertTrue(response.body, path)
                for path, call, expected_location in redirect_routes:
                    response = call(make_request(path))
                    self.assertEqual(response.status_code, 301, path)
                    self.assertEqual(response.headers.get("location"), expected_location, path)
            finally:
                for p in patches:
                    p.stop()

    def test_review_and_deals_default_after_ignores_unrelated_query_params(self) -> None:
        with Session(self.engine) as session, patch("app.routers.messages.require_role_response", return_value=None), patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ), patch("app.routers.messages.get_available_channel_choices", return_value=([], False)):
            review_response = reviewer_queue_page(
                make_request_with_query_string("/review?page=2", "page=2", role="reviewer"),
                channel_id=None,
                expense_category=None,
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=2,
                limit=25,
                success=None,
                error=None,
                session=session,
            )
            deals_response = deals_page(
                make_request_with_query_string("/deals?page=2", "page=2", role="viewer"),
                channel_id=None,
                entry_kind=None,
                after=None,
                before=None,
                page=2,
                limit=25,
                session=session,
            )

        self.assertRegex(review_response.context["selected_after"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertRegex(deals_response.context["selected_after"], r"^\d{4}-\d{2}-\d{2}$")

    def test_review_and_deals_respect_empty_date_filters_from_query_string(self) -> None:
        with Session(self.engine) as session, patch("app.routers.messages.require_role_response", return_value=None), patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ), patch("app.routers.messages.get_available_channel_choices", return_value=([], False)):
            review_response = reviewer_queue_page(
                make_request_with_query_string("/review?after=&page=1", "after=&page=1", role="reviewer"),
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
            )
            deals_response = deals_page(
                make_request_with_query_string("/deals?before=&page=1", "before=&page=1", role="viewer"),
                channel_id=None,
                entry_kind=None,
                after=None,
                before=None,
                page=1,
                limit=25,
                session=session,
            )

        self.assertEqual(review_response.context["selected_after"], "")
        self.assertEqual(deals_response.context["selected_after"], "")

    def test_review_and_deals_ignore_similar_param_names_when_defaulting_after(self) -> None:
        with Session(self.engine) as session, patch("app.routers.messages.require_role_response", return_value=None), patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ), patch("app.routers.messages.get_available_channel_choices", return_value=([], False)):
            review_response = reviewer_queue_page(
                make_request_with_query_string("/review?notafter=1", "notafter=1", role="reviewer"),
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
            )
            deals_response = deals_page(
                make_request_with_query_string("/deals?notbefore=1", "notbefore=1", role="viewer"),
                channel_id=None,
                entry_kind=None,
                after=None,
                before=None,
                page=1,
                limit=25,
                session=session,
            )

        self.assertRegex(review_response.context["selected_after"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertRegex(deals_response.context["selected_after"], r"^\d{4}-\d{2}-\d{2}$")

    def test_status_page_surfaces_background_task_failures(self) -> None:
        with Session(self.engine) as session, patch("app.routers.dashboard.require_role_response", return_value=None), patch(
            "app.shared.read_background_task_state",
            return_value={
                "failed_tasks": {
                    "tiktok-order-pull": {
                        "error": "simulated task failure",
                    }
                }
            },
        ):
            response = status_page(make_request("/status"), session=session)
            body = response.body.decode("utf-8")

        self.assertEqual(response.status_code, 200)
        self.assertIn("tiktok order pull failed: simulated task failure", body.lower())

    def test_primary_review_page_exposes_reparse_and_ignore_workflow_copy(self) -> None:
        with Session(self.engine) as session, patch("app.routers.messages.require_role_response", return_value=None), patch(
            "app.routers.messages.get_available_channel_choices",
            return_value=([], False),
        ):
            response = reviewer_queue_page(
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
            )

        body = response.body.decode("utf-8")
        self.assertIn("Reparse Filtered Review Rows", body)
        self.assertIn("Primary Review Queue", body)
        self.assertIn("Advanced: open the bulk admin review table", body)


if __name__ == "__main__":
    unittest.main()
