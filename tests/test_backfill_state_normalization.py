import json
import unittest

from sqlmodel import Session, SQLModel, create_engine

from app.discord.backfill_requests import apply_backfill_request_state, repair_backfill_request_state_rows
from app.models import (
    BACKFILL_CANCELLED,
    BACKFILL_COMPLETED,
    BACKFILL_FAILED,
    BACKFILL_PROCESSING,
    BACKFILL_QUEUED,
    BackfillRequest,
)


class BackfillStateNormalizationTests(unittest.TestCase):
    def test_queued_rows_are_born_with_progress(self) -> None:
        row = BackfillRequest(status=BACKFILL_QUEUED)
        apply_backfill_request_state(
            row,
            status=BACKFILL_QUEUED,
            progress_stage=BACKFILL_QUEUED,
            started_at=None,
            finished_at=None,
            error_message=None,
            final_result=None,
            waiting_reason=None,
            queued_count=None,
        )
        payload = json.loads(row.result_json)
        self.assertEqual(payload["progress"]["stage"], BACKFILL_QUEUED)
        self.assertEqual(payload["progress"]["inserted"], 0)
        self.assertEqual(payload["progress"]["skipped"], 0)
        self.assertNotIn("final_result", payload)

    def test_terminal_rows_carry_final_result(self) -> None:
        row = BackfillRequest(status=BACKFILL_PROCESSING)
        apply_backfill_request_state(
            row,
            status=BACKFILL_FAILED,
            progress={"channels": {}},
            progress_stage=BACKFILL_FAILED,
            error_message="boom",
            final_result={"ok": False, "error": "boom"},
        )
        payload = json.loads(row.result_json)
        self.assertEqual(row.status, BACKFILL_FAILED)
        self.assertEqual(payload["progress"]["stage"], BACKFILL_FAILED)
        self.assertEqual(payload["final_result"]["error"], "boom")

    def test_cancelled_rows_keep_terminal_progress(self) -> None:
        row = BackfillRequest(status=BACKFILL_QUEUED)
        apply_backfill_request_state(
            row,
            status=BACKFILL_CANCELLED,
            progress={"channels": {}},
            progress_stage=BACKFILL_CANCELLED,
            error_message="Cancelled by test.",
            final_result={"ok": False, "cancelled": True, "error": "Cancelled by test."},
        )
        payload = json.loads(row.result_json)
        self.assertEqual(payload["progress"]["stage"], BACKFILL_CANCELLED)
        self.assertTrue(payload["final_result"]["cancelled"])

    def test_repair_fills_blank_legacy_payloads(self) -> None:
        engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
        try:
            SQLModel.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(
                    BackfillRequest(
                        status=BACKFILL_COMPLETED,
                        result_json="{}",
                        error_message=None,
                    )
                )
                session.commit()
                repaired = repair_backfill_request_state_rows(session)
                self.assertEqual(repaired, 1)
                row = session.get(BackfillRequest, 1)
                self.assertIsNotNone(row)
                payload = json.loads(row.result_json)
                self.assertEqual(payload["progress"]["stage"], BACKFILL_COMPLETED)
                self.assertIn("final_result", payload)
        finally:
            engine.dispose()


if __name__ == "__main__":
    unittest.main()
