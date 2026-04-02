import unittest
from datetime import timedelta
from pathlib import Path
import shutil
import uuid
from unittest.mock import patch

from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine, select
from starlette.requests import Request

from app.main import (
    admin_parser_learned_rule_log_page,
    admin_parser_reparse_range,
    admin_queue_state_counts,
    bulk_reparse_filtered_messages_form,
    build_debug_snapshot,
    reparse_message_form,
)
from app.models import (
    DiscordMessage,
    OperationsLog,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    ParseAttempt,
    ReparseRun,
    Transaction,
    utcnow,
)
from app.reparse_runs import (
    create_reparse_run_record,
    finalize_reparse_run_queue_record,
    list_recent_reparse_runs,
    record_reparse_run_outcome,
)
from app.reporting import get_financial_rows
from app.transactions import get_transactions, sync_transaction_from_message
from app.worker import MAX_ATTEMPTS_ERROR, close_or_recover_unfinished_attempts, queue_reparse_range


def make_request(path: str) -> Request:
    return Request({"type": "http", "method": "POST", "path": path, "headers": []})


class QueueReparseValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_validation" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "validation.db"
        self.engine = create_engine(f"sqlite:///{db_path.as_posix()}", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def session(self) -> Session:
        return Session(self.engine)

    def make_message(self, **overrides) -> DiscordMessage:
        now = utcnow()
        defaults = {
            "discord_message_id": f"msg-{now.timestamp()}",
            "channel_id": "chan-1",
            "channel_name": "chan-1",
            "author_name": "tester",
            "content": "sold card $20 zelle",
            "created_at": now,
            "parse_status": PARSE_PARSED,
            "parse_attempts": 0,
            "needs_review": False,
        }
        defaults.update(overrides)
        return DiscordMessage(**defaults)

    def test_close_or_recover_marks_exhausted_pending_row_failed(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="exhausted-row",
                parse_status=PARSE_PENDING,
                parse_attempts=3,
                last_error=None,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            close_or_recover_unfinished_attempts(session)
            session.refresh(row)

            self.assertEqual(row.parse_status, PARSE_FAILED)
            self.assertEqual(row.last_error, MAX_ATTEMPTS_ERROR)

    def test_queue_reparse_range_includes_legacy_needs_review_alias(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-needs-review",
                parse_status="needs_review",
                needs_review=False,
                parse_attempts=2,
            )
            session.add(row)
            session.commit()

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_REVIEW_REQUIRED],
                include_reviewed=False,
                reason="validation reparse",
            )

            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 1)
            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_queue_reparse_range_includes_legacy_deleted_alias(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-deleted",
                parse_status="deleted",
                needs_review=False,
                is_deleted=False,
                parse_attempts=1,
            )
            session.add(row)
            session.commit()

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_IGNORED],
                include_reviewed=False,
                reason="validation reparse",
            )

            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 1)
            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_queue_reparse_range_includes_legacy_queued_alias(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-queued",
                parse_status="queued",
                needs_review=False,
                parse_attempts=1,
            )
            session.add(row)
            session.commit()

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_PENDING],
                include_reviewed=False,
                reason="validation reparse",
            )

            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["already_queued"], 1)
            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_admin_range_reparse_blocks_reviewed_rows_without_force_confirmation(self) -> None:
        with self.session() as session, patch("app.main.require_role_response", return_value=None):
            with self.assertRaises(HTTPException) as exc:
                admin_parser_reparse_range(
                    make_request("/admin/parser/reparse-range"),
                    after="2026-03-01",
                    before="2026-03-31",
                    include_reviewed="true",
                    force_reviewed=None,
                    session=session,
                )

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("force_reviewed", exc.exception.detail)

    def test_reparse_run_records_queue_summary_and_outcomes(self) -> None:
        with self.session() as session:
            queued_row = self.make_message(
                discord_message_id="reparse-run-queued",
                parse_status=PARSE_PARSED,
            )
            reviewed_row = self.make_message(
                discord_message_id="reparse-run-reviewed",
                parse_status=PARSE_PARSED,
                reviewed_at=utcnow(),
            )
            session.add(queued_row)
            session.add(reviewed_row)
            session.commit()
            session.refresh(queued_row)

            run = create_reparse_run_record(
                session,
                source="test",
                reason="validation reparse",
                range_after=utcnow() - timedelta(days=1),
                range_before=utcnow() + timedelta(days=1),
                channel_id="chan-1",
                include_reviewed=False,
                force_reviewed=False,
                requested_statuses=[PARSE_PARSED],
            )

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_PARSED],
                include_reviewed=False,
                reason="validation reparse",
                reparse_run_id=run.run_id,
            )
            finalize_reparse_run_queue_record(
                session,
                run_id=run.run_id,
                selected_count=result["matched"],
                queued_count=result["queued"],
                already_queued_count=result["already_queued"],
                skipped_reviewed_count=result["skipped_reviewed"],
                first_message_id=result["first_message_id"],
                last_message_id=result["last_message_id"],
                first_message_created_at=result["first_message_created_at"],
                last_message_created_at=result["last_message_created_at"],
            )
            session.refresh(queued_row)
            self.assertEqual(queued_row.active_reparse_run_id, run.run_id)

            record_reparse_run_outcome(session, run_id=run.run_id, success=True)
            refreshed_run = session.exec(select(ReparseRun).where(ReparseRun.run_id == run.run_id)).first()

            self.assertIsNotNone(refreshed_run)
            self.assertEqual(refreshed_run.selected_count, 1)
            self.assertEqual(refreshed_run.skipped_reviewed_count, 1)
            self.assertEqual(refreshed_run.succeeded_count, 1)
            self.assertEqual(refreshed_run.failed_count, 0)
            self.assertEqual(refreshed_run.status, "completed")
            self.assertIsNotNone(refreshed_run.finished_at)
            self.assertEqual(list_recent_reparse_runs(session, limit=5)[0].run_id, run.run_id)

    def test_admin_learned_rule_log_page_shows_recent_events(self) -> None:
        with self.session() as session, patch("app.main.require_role_response", return_value=None):
            row = self.make_message(
                discord_message_id="learned-rule-log-row",
                content="sold charizard 45 cash",
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            session.add(
                OperationsLog(
                    event_type="queue.learned_rule_applied",
                    level="info",
                    source="worker",
                    message="learned_rule_applied",
                    details_json=(
                        '{"message_id": %d, "pattern_type": "payment_only_sell", '
                        '"status": "applied", "reason": "matched payment-only sell phrase", '
                        '"correction_source": "learned_rule"}'
                    ) % row.id,
                )
            )
            session.commit()

            response = admin_parser_learned_rule_log_page(
                make_request("/admin/parser/learned-rule-log"),
                limit=50,
                session=session,
            )

            events = response.context["events"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["outcome"], "applied")
            self.assertEqual(events[0]["rule_matched"], "payment only sell (learned_rule)")
            self.assertEqual(events[0]["message_snippet"], "sold charizard 45 cash")

    def test_admin_queue_state_counts_includes_legacy_alias_rows_and_filters(self) -> None:
        with self.session() as session:
            session.add(
                self.make_message(
                    discord_message_id="legacy-queued-count",
                    parse_status="queued",
                )
            )
            session.add(
                self.make_message(
                    discord_message_id="legacy-needs-review-count",
                    parse_status="needs_review",
                )
            )
            session.add(
                self.make_message(
                    discord_message_id="legacy-deleted-count",
                    parse_status="deleted",
                    is_deleted=False,
                )
            )
            session.commit()

            result = admin_queue_state_counts(
                status=None,
                channel_id=None,
                entry_kind=None,
                after=None,
                before=None,
                session=session,
            )
            ignored_result = admin_queue_state_counts(
                status="ignored",
                channel_id=None,
                entry_kind=None,
                after=None,
                before=None,
                session=session,
            )

            self.assertEqual(result["counts"]["queued"], 1)
            self.assertEqual(result["counts"]["needs_review"], 1)
            self.assertEqual(ignored_result["counts"]["ignored"], 1)

    def test_grouped_child_ignored_row_does_not_keep_transaction(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="grouped-child",
                parse_status=PARSE_PARSED,
                amount=15.0,
                entry_kind="sale",
                payment_method="cash",
                cash_direction="to_store",
                money_in=15.0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            sync_transaction_from_message(session, row)
            session.commit()

            row.parse_status = PARSE_IGNORED
            row.stitched_group_id = "group-1"
            row.stitched_primary = False
            session.add(row)
            sync_transaction_from_message(session, row)
            session.commit()

            transaction = session.exec(
                select(Transaction).where(Transaction.source_message_id == row.id)
            ).first()
            self.assertIsNone(transaction)

    def test_get_transactions_misses_legacy_transaction_needs_review_status(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-transaction-needs-review",
                parse_status="needs_review",
                needs_review=True,
                amount=11.0,
                entry_kind="sale",
                payment_method="zelle",
                cash_direction="to_store",
                money_in=11.0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            sync_transaction_from_message(session, row)
            session.commit()

            transactions = get_transactions(session)

            self.assertEqual(len(transactions), 1)
            self.assertEqual(transactions[0].source_message_id, row.id)

    def test_get_financial_rows_misses_legacy_message_needs_review_status(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-reporting-needs-review",
                parse_status="needs_review",
                needs_review=True,
                amount=17.0,
                entry_kind="sale",
                payment_method="cash",
                cash_direction="to_store",
                money_in=17.0,
            )
            session.add(row)
            session.commit()

            rows = get_financial_rows(session)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].discord_message_id, "legacy-reporting-needs-review")

    def test_build_debug_snapshot_reports_stuck_processing_and_recent_worker_failure(self) -> None:
        with self.session() as session:
            stale_started_at = utcnow() - timedelta(minutes=25)
            row = self.make_message(
                discord_message_id="stuck-processing",
                parse_status=PARSE_PROCESSING,
                parse_attempts=2,
                created_at=utcnow() - timedelta(minutes=30),
                ingested_at=utcnow() - timedelta(minutes=30),
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            session.add(
                ParseAttempt(
                    message_id=row.id,
                    attempt_number=2,
                    started_at=stale_started_at,
                    finished_at=None,
                    success=False,
                    error="still running",
                )
            )
            session.add(
                OperationsLog(
                    event_type="queue.parse_failed",
                    level="warning",
                    source="worker",
                    message="parse_failed",
                    details_json='{"error":"still running"}',
                )
            )
            session.commit()

            snapshot = build_debug_snapshot(session)

            self.assertEqual(snapshot["queue_counts"]["processing"], 1)
            self.assertEqual(len(snapshot["stuck_processing"]), 1)
            self.assertEqual(snapshot["stuck_processing"][0]["message_id"], row.id)
            self.assertGreaterEqual(len(snapshot["recent_worker_failures"]), 1)

    def test_bulk_requeue_filtered_messages_form_resets_matching_review_rows(self) -> None:
        with self.session() as session, patch("app.main.require_role_response", return_value=None):
            matching = self.make_message(
                discord_message_id="filtered-review-match",
                channel_id="chan-review",
                channel_name="chan-review",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=3,
                last_error="needs human review",
                reviewed_by="reviewer",
                reviewed_at=utcnow(),
                amount=25.0,
                entry_kind="sale",
                money_in=25.0,
                expense_category="inventory",
            )
            other_channel = self.make_message(
                discord_message_id="filtered-review-other-channel",
                channel_id="chan-other",
                channel_name="chan-other",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=2,
                amount=30.0,
                entry_kind="sale",
                money_in=30.0,
                expense_category="inventory",
            )
            other_category = self.make_message(
                discord_message_id="filtered-review-other-category",
                channel_id="chan-review",
                channel_name="chan-review",
                parse_status=PARSE_FAILED,
                needs_review=False,
                parse_attempts=4,
                last_error="parse failed",
                amount=40.0,
                entry_kind="sale",
                money_in=40.0,
                expense_category="travel",
            )
            session.add(matching)
            session.add(other_channel)
            session.add(other_category)
            session.commit()
            session.refresh(matching)
            session.refresh(other_channel)
            session.refresh(other_category)

            sync_transaction_from_message(session, matching)
            sync_transaction_from_message(session, other_channel)
            sync_transaction_from_message(session, other_category)
            session.commit()

            response = bulk_reparse_filtered_messages_form(
                make_request("/messages/bulk/requeue-filtered-form"),
                return_path="/review",
                status="review_queue",
                channel_id="chan-review",
                expense_category="inventory",
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=25,
                session=session,
            )

            self.assertEqual(response.status_code, 303)
            session.refresh(matching)
            session.refresh(other_channel)
            session.refresh(other_category)

            self.assertEqual(matching.parse_status, PARSE_PENDING)
            self.assertEqual(matching.parse_attempts, 0)
            self.assertEqual(matching.last_error, "manual filtered reparse")
            self.assertFalse(matching.needs_review)
            self.assertIsNone(matching.reviewed_by)
            self.assertIsNone(matching.reviewed_at)
            self.assertIsNone(matching.active_reparse_run_id)

            self.assertEqual(other_channel.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertTrue(other_channel.needs_review)
            self.assertEqual(other_category.parse_status, PARSE_FAILED)
            self.assertEqual(other_category.last_error, "parse failed")

            matching_tx = session.exec(select(Transaction).where(Transaction.source_message_id == matching.id)).all()
            other_channel_tx = session.exec(select(Transaction).where(Transaction.source_message_id == other_channel.id)).all()
            other_category_tx = session.exec(select(Transaction).where(Transaction.source_message_id == other_category.id)).all()
            self.assertEqual(len(matching_tx), 0)
            self.assertEqual(len(other_channel_tx), 1)
            self.assertEqual(len(other_category_tx), 0)

    def test_retry_message_form_resets_attempts_and_removes_transaction_until_reparsed(self) -> None:
        with self.session() as session, patch("app.main.require_role_response", return_value=None):
            reviewed_at = utcnow()
            row = self.make_message(
                discord_message_id="retry-row",
                parse_status=PARSE_PARSED,
                parse_attempts=3,
                reviewed_by="reviewer",
                reviewed_at=reviewed_at,
                amount=20.0,
                entry_kind="sale",
                payment_method="zelle",
                cash_direction="to_store",
                money_in=20.0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            sync_transaction_from_message(session, row)
            session.commit()

            response = reparse_message_form(
                make_request(f"/messages/{row.id}/retry-form"),
                message_id=row.id,
                page=1,
                limit=100,
                session=session,
            )

            session.refresh(row)
            transaction = session.exec(
                select(Transaction).where(Transaction.source_message_id == row.id)
            ).first()

            self.assertEqual(response.status_code, 303)
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertEqual(row.parse_attempts, 0)
            self.assertIsNone(row.reviewed_by)
            self.assertIsNone(row.reviewed_at)
            self.assertIsNone(transaction)


if __name__ == "__main__":
    unittest.main()
