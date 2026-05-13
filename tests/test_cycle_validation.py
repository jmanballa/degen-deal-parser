import inspect
import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine, select
from starlette.requests import Request

import app.config as config_module
import app.discord.corrections as corrections_module
import app.db as db_module
import app.discord.discord_ingest as discord_ingest_module
import app.main as main_module
from app.discord.corrections import get_learned_rule_match
from app.db import fixup_transaction_parse_status_aliases
from app.routers.admin_actions import admin_parser_reparse_runs_json, admin_parser_reparse_runs_page
from app.models import ReparseRun, ReviewCorrection, Transaction, utcnow


def make_request(path: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": path, "headers": []})


class CycleValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_cycle_validation" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "cycle_validation.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def managed_session_override(self):
        with Session(self.engine) as session:
            yield session

    def make_correction(
        self,
        *,
        source_message_id: int,
        normalized_text: str = "$11 zelle",
        pattern_type: str = "payment_only_sell",
    ) -> ReviewCorrection:
        return ReviewCorrection(
            source_message_id=source_message_id,
            normalized_text=normalized_text,
            pattern_type=pattern_type,
            corrected_after_json=json.dumps(
                {
                    "deal_type": "sell",
                    "expense_category": "inventory",
                    "confidence": 0.98,
                }
            ),
            features_json=json.dumps(
                {
                    "tokens": ["11", "zelle"],
                    "payment_amount": 11.0,
                    "payment_method": "zelle",
                    "payment_only_text": True,
                }
            ),
        )

    def test_learned_rule_with_fewer_than_threshold_is_not_applied_and_returns_skip_event(self) -> None:
        with Session(self.engine) as session:
            session.add(self.make_correction(source_message_id=1))
            session.commit()

        with patch("app.discord.corrections.managed_session", self.managed_session_override):
            learned_parse, event = get_learned_rule_match("$11 zelle")

        self.assertIsNone(learned_parse)
        self.assertIsNotNone(event)
        self.assertEqual(event["status"], "skipped")
        self.assertIn("requires more matching corrections", event.get("reason", ""))

    def test_learned_rule_with_exact_threshold_is_applied(self) -> None:
        threshold = corrections_module.MIN_LEARNED_RULE_CORRECTION_COUNT
        with Session(self.engine) as session:
            for index in range(threshold):
                session.add(self.make_correction(source_message_id=index + 1))
            session.commit()

        with patch("app.discord.corrections.managed_session", self.managed_session_override):
            learned_parse, event = get_learned_rule_match("$11 zelle")

        self.assertIsNotNone(learned_parse)
        self.assertEqual(learned_parse["parsed_type"], "sell")
        self.assertEqual(learned_parse["parsed_amount"], 11.0)
        self.assertEqual(event["status"], "applied")

    def test_learned_rule_threshold_uses_named_constant(self) -> None:
        source = inspect.getsource(corrections_module.get_learned_rule_match)
        self.assertTrue(hasattr(corrections_module, "MIN_LEARNED_RULE_CORRECTION_COUNT"))
        self.assertIn("MIN_LEARNED_RULE_CORRECTION_COUNT", source)

    def test_transaction_parse_status_fixup_is_idempotent(self) -> None:
        with Session(self.engine) as session:
            session.add(Transaction(source_message_id=1, occurred_at=utcnow(), parse_status="needs_review"))
            session.add(Transaction(source_message_id=2, occurred_at=utcnow(), parse_status="queued"))
            session.add(Transaction(source_message_id=3, occurred_at=utcnow(), parse_status="deleted"))
            session.add(Transaction(source_message_id=4, occurred_at=utcnow(), parse_status="parsed"))
            session.commit()
            initial_count = session.exec(select(Transaction)).all()
            self.assertEqual(len(initial_count), 4)

        with patch.object(db_module, "engine", self.engine):
            fixup_transaction_parse_status_aliases()
            fixup_transaction_parse_status_aliases()

        with Session(self.engine) as session:
            rows = session.exec(select(Transaction)).all()
            statuses = sorted(row.parse_status for row in rows)

        self.assertEqual(len(rows), 4)
        self.assertNotIn("needs_review", statuses)
        self.assertNotIn("queued", statuses)
        self.assertNotIn("deleted", statuses)
        self.assertIn("review_required", statuses)
        self.assertIn("pending", statuses)
        self.assertIn("ignored", statuses)

    def test_reparse_runs_html_route_returns_200_and_contains_expected_fields(self) -> None:
        with Session(self.engine) as session:
            session.add(
                ReparseRun(
                    run_id="run-html-1",
                    source="manual",
                    reason="validation",
                    requested_at=utcnow(),
                    finished_at=utcnow() + timedelta(seconds=2),
                    range_after=utcnow() - timedelta(days=1),
                    range_before=utcnow(),
                    selected_count=4,
                    queued_count=3,
                    already_queued_count=1,
                    succeeded_count=2,
                    failed_count=1,
                    status="completed",
                )
            )
            session.commit()

            with patch("app.routers.admin_actions.require_role_response", return_value=None):
                response = admin_parser_reparse_runs_page(
                    make_request("/admin/parser/reparse-runs"),
                    limit=20,
                    session=session,
                )

        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Reparse Runs", body)
        self.assertIn("Timestamp", body)
        self.assertIn("Range", body)
        self.assertIn("Selected", body)
        self.assertIn("Reparsed", body)
        self.assertIn("Failed", body)
        self.assertIn("4", body)
        self.assertIn("2", body)

    def test_reparse_runs_json_contains_expected_range_and_count_fields(self) -> None:
        with Session(self.engine) as session:
            session.add(
                ReparseRun(
                    run_id="run-json-1",
                    source="manual",
                    reason="validation-json",
                    requested_at=utcnow(),
                    range_after=utcnow() - timedelta(days=2),
                    range_before=utcnow() - timedelta(days=1),
                    selected_count=7,
                    queued_count=5,
                    already_queued_count=1,
                    skipped_reviewed_count=1,
                    succeeded_count=4,
                    failed_count=1,
                    status="completed",
                )
            )
            session.commit()

            with patch("app.routers.admin_actions.require_role_response", return_value=None):
                payload = admin_parser_reparse_runs_json(
                    make_request("/admin/parser/reparse-runs.json"),
                    limit=20,
                    session=session,
                )

        self.assertIn("runs", payload)
        self.assertEqual(len(payload["runs"]), 1)
        run = payload["runs"][0]
        self.assertEqual(run["run_id"], "run-json-1")
        self.assertIn("requested_at", run)
        self.assertIn("range_after", run)
        self.assertIn("range_before", run)
        self.assertIn("queued_count", run)
        self.assertIn("selected_count", run)

    def test_reviewer_focus_page_uses_cached_or_proxy_attachment_urls(self) -> None:
        from app.routers.messages import reviewer_focus_page
        from app.shared import build_message_list_items
        from app.display_media import get_cached_attachment_map

        source = inspect.getsource(reviewer_focus_page)
        self.assertIn("build_message_list_items", source)
        self.assertNotIn("attachment_urls_json", source)

        build_source = inspect.getsource(build_message_list_items)
        self.assertIn('f"/messages/{item[\'id\']}/attachments/{index}"', build_source)
        self.assertNotIn("attachment_urls_json", source)

        cache_source = inspect.getsource(get_cached_attachment_map)
        self.assertIn('f"/attachments/{asset_id}"', cache_source)

    def test_settings_raise_on_public_host_mode_with_weak_defaults(self) -> None:
        settings = config_module.Settings(
            PUBLIC_BASE_URL="https://ops.example.com",
            SESSION_SECRET=config_module.DEFAULT_SESSION_SECRET,
            ADMIN_PASSWORD=config_module.DEFAULT_ADMIN_PASSWORD,
        )

        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()

        self.assertIn("SESSION_SECRET", str(exc.exception))
        self.assertIn("ADMIN_PASSWORD", str(exc.exception))

    def test_settings_raise_on_local_defaults_outside_tests(self) -> None:
        settings = config_module.Settings(
            EMPLOYEE_PORTAL_ENABLED="false",
            EMPLOYEE_PII_KEY="",
            EMPLOYEE_EMAIL_HASH_SALT="",
            EMPLOYEE_TOKEN_HMAC_KEY="",
        )
        settings.public_base_url = "http://127.0.0.1:8000"
        settings.session_https_only = False
        settings.session_domain = ""
        settings.session_secret = config_module.DEFAULT_SESSION_SECRET
        settings.admin_password = config_module.DEFAULT_ADMIN_PASSWORD
        with self.assertRaises(RuntimeError):
            settings.validate_runtime_secrets()

    def test_backfill_cancellation_interval_is_five_messages_or_fewer(self) -> None:
        self.assertLessEqual(discord_ingest_module.BACKFILL_PROGRESS_EVERY_MESSAGES, 5)

        source = inspect.getsource(discord_ingest_module.DealIngestBot.backfill_channel)
        self.assertIn("processed_count % BACKFILL_PROGRESS_EVERY_MESSAGES", source)


if __name__ == "__main__":
    unittest.main()
