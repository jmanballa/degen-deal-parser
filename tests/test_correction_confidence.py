import unittest
from datetime import datetime, timezone

from sqlmodel import SQLModel, Session, create_engine

from app.corrections import compute_correction_confidence
from app.models import ReviewCorrection


def _utcnow():
    return datetime.now(timezone.utc)


_counter = 0


def _make_peer(session, normalized_text, entry_kind="sale", amount=50.0, confidence=0.90):
    global _counter
    _counter += 1
    c = ReviewCorrection(
        source_message_id=_counter,
        normalized_text=normalized_text,
        entry_kind=entry_kind,
        amount=amount,
        confidence=confidence,
        correction_source="manual_edit",
        created_at=_utcnow(),
        updated_at=_utcnow(),
    )
    session.add(c)
    session.commit()
    return c


class ComputeCorrectionConfidenceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_no_field_diffs_preserves_parser_confidence(self):
        with Session(self.engine) as session:
            result = compute_correction_confidence(session, "buy shoes", {}, 0.90)
        self.assertAlmostEqual(result, 0.90, places=3)

    def test_few_field_diffs_small_reduction(self):
        # 1-2 diffs → severity 0.95
        with Session(self.engine) as session:
            diffs = {"entry_kind": {"before": "buy", "after": "sale"}}
            result = compute_correction_confidence(session, "buy shoes", diffs, 0.90)
        self.assertAlmostEqual(result, 0.90 * 0.95, places=3)

    def test_many_field_diffs_larger_reduction(self):
        # 5 diffs → severity 0.75
        with Session(self.engine) as session:
            diffs = {f"field_{i}": {"before": "x", "after": "y"} for i in range(5)}
            result = compute_correction_confidence(session, "buy shoes", diffs, 0.90)
        self.assertAlmostEqual(result, 0.90 * 0.75, places=3)

    def test_none_parser_confidence_uses_default(self):
        with Session(self.engine) as session:
            result = compute_correction_confidence(session, "buy shoes", {}, None)
        # base defaults to 0.85
        self.assertAlmostEqual(result, 0.85, places=3)

    def test_agreeing_peers_no_penalty(self):
        with Session(self.engine) as session:
            _make_peer(session, "sell jordans", entry_kind="sale", amount=100.0)
            _make_peer(session, "sell jordans", entry_kind="sale", amount=100.0)
            result = compute_correction_confidence(session, "sell jordans", {}, 0.90)
        # agreement_factor = 1.0 (all agree), severity_factor = 1.0
        self.assertAlmostEqual(result, 0.90, places=3)

    def test_disagreeing_peers_reduces_confidence(self):
        with Session(self.engine) as session:
            _make_peer(session, "trade item", entry_kind="sale", amount=50.0)
            _make_peer(session, "trade item", entry_kind="buy", amount=50.0)  # different entry_kind
            result = compute_correction_confidence(session, "trade item", {}, 0.90)
        # agreement_factor = 0.80
        self.assertAlmostEqual(result, 0.90 * 0.80, places=3)

    def test_result_capped_at_one(self):
        # High parser confidence + agreeing peers should not exceed 1.0
        with Session(self.engine) as session:
            result = compute_correction_confidence(session, "text", {}, 1.0)
        self.assertLessEqual(result, 1.0)

    def test_single_peer_no_agreement_penalty(self):
        with Session(self.engine) as session:
            _make_peer(session, "solo pattern", entry_kind="sale")
            result = compute_correction_confidence(session, "solo pattern", {}, 0.88)
        # Only 1 peer → agreement_factor = 1.0
        self.assertAlmostEqual(result, 0.88, places=3)


if __name__ == "__main__":
    unittest.main()
