import re
import shutil
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

import app.main as main_module
from app.models import DiscordMessage, PARSE_PARSED, WatchedChannel, utcnow


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


def read_template(name: str) -> str:
    return Path("app/templates", name).read_text(encoding="utf-8")


class Pass2ConsolidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_pass2_consolidation" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "pass2_consolidation.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_admin_health_redirects_to_status(self) -> None:
        with Session(self.engine) as session:
            response = main_module.admin_health_page(make_request("/admin/health"), session=session)
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.headers["location"], "/status")

    def test_admin_debug_redirects_to_status(self) -> None:
        with Session(self.engine) as session:
            response = main_module.admin_debug_page(make_request("/admin/debug"), session=session)
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.headers["location"], "/status")

    def test_status_page_contains_merged_sections_and_collapsed_debug_details(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None):
            response = main_module.status_page(
                make_request("/status", role="viewer"),
                success=None,
                error=None,
                session=session,
            )
        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Runtime Health", body)
        self.assertIn("Queue Summary", body)
        self.assertIn("TikTok Sync", body)
        self.assertIn("Recent Reparse", body)
        self.assertIn("Debug Detail", body)
        self.assertIn('<details class="card" id="debug-detail">', body)

    def test_partner_redirects_to_dashboard(self) -> None:
        with Session(self.engine) as session:
            response = main_module.partner_page(make_request("/partner"), session=session)
        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.headers["location"], "/dashboard")

    def test_dashboard_contains_partner_content_and_mobile_responsive_rules(self) -> None:
        source = read_template("dashboard.html")
        self.assertIn("What Needs Attention", source)
        self.assertIn('href="/review"', source)
        self.assertIn('review_summary["needs_review"]', source)
        self.assertIn("@media (max-width:1000px)", source)
        self.assertIn("grid-template-columns:1fr", source)
        self.assertIn("@media (max-width:720px)", source)

    def test_no_partner_href_links_remain_in_templates(self) -> None:
        matches = []
        for path in Path("app/templates").glob("*.html"):
            text = path.read_text(encoding="utf-8")
            if 'href="/partner"' in text:
                matches.append(path.name)
        self.assertEqual(matches, [])

    def test_messages_route_redirects_to_deals_for_valid_id(self) -> None:
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="msg-redirect-1",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="redirect me",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            req = make_request(f"/messages/{row.id}")
            with patch("app.main.require_role_response", return_value=None):
                response = main_module.get_message(request=req, message_id=row.id, session=session)

        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.headers["location"], f"/deals/{row.id}")

    def test_deal_detail_page_renders_technical_details_in_details_element(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None), patch(
            "app.main.get_watched_channels",
            return_value=[WatchedChannel(channel_id="chan-1", channel_name="deals", is_enabled=True)],
        ), patch(
            "app.main.get_correction_pattern_counts",
            return_value=[],
        ), patch(
            "app.main.get_learning_signal",
            return_value={"promoted_rule": False, "exact_match": False, "similar_count": 0},
        ):
            row = DiscordMessage(
                discord_message_id="deal-1",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="technical details deal",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            response = main_module.deal_detail_page(
                message_id=row.id,
                request=make_request(f"/deals/{row.id}", role="viewer"),
                return_path="/deals",
                status=None,
                channel_id=None,
                entry_kind=None,
                expense_category=None,
                after=None,
                before=None,
                sort_by=None,
                sort_dir=None,
                page=1,
                limit=25,
                success=None,
                error=None,
                session=session,
            )

        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Technical Details (Operator)", body)
        self.assertIn('<details class="card tech-details">', body)

    def test_no_orphaned_redirect_hrefs_remain_in_templates(self) -> None:
        matches = []
        for path in Path("app/templates").glob("*.html"):
            text = path.read_text(encoding="utf-8")
            static_patterns = [
                'href="/admin/health"',
                'href="/admin/debug"',
                'href="/partner"',
            ]
            for pattern in static_patterns:
                if pattern in text:
                    matches.append((path.name, pattern))
            if '/messages/' in text:
                for detail_href in set(re.findall(r'href="/messages/(\d+)(?:[\"?])', text)):
                    matches.append((path.name, f'href="/messages/{detail_href}"'))
        self.assertEqual(matches, [])


if __name__ == "__main__":
    unittest.main()
