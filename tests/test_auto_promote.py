import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine, select

from app.corrections import auto_promote_eligible_patterns
from app.models import OperationsLog, ReviewCorrection
from app.worker import auto_promote_once


def _utcnow():
    return datetime.now(timezone.utc)


_correction_counter = 0


def _make_correction(normalized_text, confidence=None, correction_source="manual_edit"):
    global _correction_counter
    _correction_counter += 1
    return ReviewCorrection(
        source_message_id=_correction_counter,
        normalized_text=normalized_text,
        confidence=confidence,
        correction_source=correction_source,
        entry_kind="sale",
        created_at=_utcnow(),
        updated_at=_utcnow(),
    )


class AutoPromoteTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed(self, session, rows):
        for row in rows:
            session.add(row)
        session.commit()

    def test_below_min_count_not_promoted(self):
        with Session(self.engine) as session:
            # Only 3 corrections, min_count=5
            for _ in range(3):
                session.add(_make_correction("buy nike shoes", confidence=0.95))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)
        self.assertEqual(promoted, [])

    def test_below_min_confidence_not_promoted(self):
        with Session(self.engine) as session:
            for _ in range(5):
                session.add(_make_correction("sell jordans", confidence=0.70))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)
        self.assertEqual(promoted, [])

    def test_all_none_confidence_not_promoted(self):
        with Session(self.engine) as session:
            for _ in range(5):
                session.add(_make_correction("trade cards", confidence=None))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)
        self.assertEqual(promoted, [])

    def test_meets_thresholds_gets_promoted(self):
        with Session(self.engine) as session:
            for _ in range(5):
                session.add(_make_correction("buy adidas", confidence=0.90))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)

        self.assertIn("buy adidas", promoted)
        with Session(self.engine) as session:
            rows = session.exec(
                select(ReviewCorrection).where(ReviewCorrection.normalized_text == "buy adidas")
            ).all()
        self.assertTrue(all(r.correction_source == "promoted_rule" for r in rows))

    def test_already_promoted_patterns_excluded(self):
        with Session(self.engine) as session:
            for _ in range(5):
                session.add(_make_correction("sell puma", confidence=0.90, correction_source="promoted_rule"))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)
        self.assertEqual(promoted, [])

    def test_mixed_confidence_uses_average(self):
        with Session(self.engine) as session:
            # avg = (0.80 + 0.90 + 0.90 + 0.90 + 0.90) / 5 = 0.88 >= 0.85
            confidences = [0.80, 0.90, 0.90, 0.90, 0.90]
            for c in confidences:
                session.add(_make_correction("resell sneakers", confidence=c))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)
        self.assertIn("resell sneakers", promoted)

    def test_multiple_groups_independent(self):
        with Session(self.engine) as session:
            # Group A: qualifies
            for _ in range(5):
                session.add(_make_correction("group a", confidence=0.90))
            # Group B: low confidence
            for _ in range(5):
                session.add(_make_correction("group b", confidence=0.60))
            session.commit()
            promoted = auto_promote_eligible_patterns(session, min_count=5, min_confidence=0.85)

        self.assertIn("group a", promoted)
        self.assertNotIn("group b", promoted)


class AutoPromoteOnceLoggingTests(unittest.TestCase):
    """Verify that auto_promote_once() persists events to OperationsLog."""

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def _seed_promotable(self):
        with Session(self.engine) as session:
            for _ in range(5):
                session.add(_make_correction("log test pattern", confidence=0.90))
            session.commit()

    def test_promoted_pattern_logged_to_operations_log(self):
        self._seed_promotable()
        with patch("app.worker.managed_session", self._managed_session), \
             patch("app.worker.settings") as mock_settings:
            mock_settings.auto_promote_min_count = 5
            mock_settings.auto_promote_min_confidence = 0.85
            auto_promote_once()

        with Session(self.engine) as session:
            rows = session.exec(
                select(OperationsLog).where(
                    OperationsLog.event_type == "queue.auto_promoted_correction_pattern"
                )
            ).all()
        self.assertEqual(len(rows), 1)
        self.assertIn("log test pattern", rows[0].details_json)

    def test_no_promotions_no_log_entries(self):
        # Seed below min_count threshold
        with Session(self.engine) as session:
            for _ in range(2):
                session.add(_make_correction("too few", confidence=0.90))
            session.commit()

        with patch("app.worker.managed_session", self._managed_session), \
             patch("app.worker.settings") as mock_settings:
            mock_settings.auto_promote_min_count = 5
            mock_settings.auto_promote_min_confidence = 0.85
            auto_promote_once()

        with Session(self.engine) as session:
            rows = session.exec(select(OperationsLog)).all()
        self.assertEqual(len(rows), 0)


if __name__ == "__main__":
    unittest.main()
