import asyncio
import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine

from app.discord.corrections import (
    MIN_LEARNED_RULE_CORRECTION_COUNT,
    build_field_diffs,
    build_learned_rule_parse,
    extract_learning_features,
    get_learned_rule_match,
    infer_pattern_type,
)
from app.models import ReviewCorrection
from app.discord.parser import parse_message


class CorrectionLearningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_learning" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "learning.db"
        self.engine = create_engine(f"sqlite:///{db_path.as_posix()}", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def managed_session_override(self):
        with Session(self.engine) as session:
            yield session

    def test_infers_payment_only_sell_pattern(self):
        corrected_after = {
            "deal_type": "sell",
            "expense_category": "inventory",
        }
        features = extract_learning_features("$11 zelle")
        pattern_type = infer_pattern_type(
            "$11 zelle",
            corrected_after,
            field_diffs={},
            features=features,
        )
        self.assertEqual(pattern_type, "payment_only_sell")

    def test_builds_structured_field_diffs(self):
        before = {
            "deal_type": "unknown",
            "amount": None,
            "payment_method": None,
            "parse_status": "review_required",
            "needs_review": True,
        }
        after = {
            "deal_type": "sell",
            "amount": 11.0,
            "payment_method": "zelle",
            "parse_status": "parsed",
            "needs_review": False,
        }
        diffs = build_field_diffs(before, after)
        self.assertEqual(diffs["deal_type"]["before"], "unknown")
        self.assertEqual(diffs["deal_type"]["after"], "sell")
        self.assertEqual(diffs["payment_method"]["after"], "zelle")
        self.assertFalse(diffs["needs_review"]["after"])

    def test_applies_trade_in_out_learned_rule(self):
        correction = ReviewCorrection(
            id=7,
            source_message_id=42,
            normalized_text="top in + $145 bottom out",
            pattern_type="trade_in_out",
            corrected_after_json=json.dumps(
                {
                    "deal_type": "trade",
                    "category": "slabs",
                    "cash_direction": "to_store",
                    "items_in": ["top case items"],
                    "items_out": ["bottom case items"],
                    "trade_summary": "out: bottom case items | in: top case items | plus $145 cash",
                    "confidence": 0.98,
                }
            ),
            features_json=json.dumps(
                {
                    "directional_tokens": ["top in", "bottom out", "in", "out"],
                    "tokens": ["top", "in", "bottom", "out"],
                }
            ),
        )
        learned_parse, event = build_learned_rule_parse(
            correction,
            message_text="top in + $30 bottom out",
            incoming_features=extract_learning_features("top in + $30 bottom out"),
        )
        self.assertIsNotNone(learned_parse)
        self.assertEqual(learned_parse["parsed_type"], "trade")
        self.assertEqual(learned_parse["parsed_amount"], 30.0)
        self.assertEqual(event["status"], "applied")

    def test_exact_correction_memory_still_wins(self):
        exact_match = {
            "parsed_type": "sell",
            "parsed_amount": 25.0,
            "parsed_payment_method": "cash",
            "parsed_cash_direction": "to_store",
            "parsed_category": "unknown",
            "parsed_items": [],
            "parsed_items_in": [],
            "parsed_items_out": [],
            "parsed_trade_summary": "",
            "parsed_notes": "exact memory",
            "image_summary": "matched prior correction memory",
            "confidence": 0.99,
            "needs_review": False,
        }

        with patch("app.discord.parser.get_exact_correction_match", return_value=exact_match), patch(
            "app.discord.parser.get_learned_rule_match",
            side_effect=AssertionError("learned rule lookup should not run after exact match"),
        ):
            parsed = asyncio.run(parse_message("sold 25 cash", [], "tester"))
        self.assertEqual(parsed["parsed_notes"], "exact memory")

    def test_learned_rule_skips_below_minimum_correction_count(self):
        with Session(self.engine) as session:
            session.add(
                ReviewCorrection(
                    source_message_id=101,
                    normalized_text="$11 zelle",
                    pattern_type="payment_only_sell",
                    deal_type="sell",
                    category="unknown",
                    payment_method="zelle",
                    cash_direction="to_store",
                    corrected_after_json=json.dumps(
                        {
                            "deal_type": "sell",
                            "category": "unknown",
                            "payment_method": "zelle",
                            "cash_direction": "to_store",
                            "items_out": [],
                            "confidence": 0.97,
                        }
                    ),
                    features_json=json.dumps(
                        {
                            "tokens": ["11", "zelle"],
                            "payment_only_text": True,
                            "payment_method": "zelle",
                            "payment_amount": 11.0,
                        }
                    ),
                )
            )
            session.commit()

        with patch("app.discord.corrections.managed_session", self.managed_session_override):
            learned_parse, event = get_learned_rule_match("$11 zelle")

        self.assertIsNone(learned_parse)
        self.assertIsNotNone(event)
        self.assertEqual(event["status"], "skipped")
        self.assertEqual(event["correction_count"], 1)
        self.assertEqual(event["required_correction_count"], MIN_LEARNED_RULE_CORRECTION_COUNT)

    def test_learned_rule_applies_once_minimum_correction_count_is_met(self):
        with Session(self.engine) as session:
            for source_message_id in (201, 202):
                session.add(
                    ReviewCorrection(
                        source_message_id=source_message_id,
                        normalized_text="$11 zelle",
                        pattern_type="payment_only_sell",
                        deal_type="sell",
                        category="unknown",
                        payment_method="zelle",
                        cash_direction="to_store",
                        corrected_after_json=json.dumps(
                            {
                                "deal_type": "sell",
                                "category": "unknown",
                                "payment_method": "zelle",
                                "cash_direction": "to_store",
                                "items_out": [],
                                "confidence": 0.97,
                            }
                        ),
                        features_json=json.dumps(
                            {
                                "tokens": ["11", "zelle"],
                                "payment_only_text": True,
                                "payment_method": "zelle",
                                "payment_amount": 11.0,
                            }
                        ),
                    )
                )
            session.commit()

        with patch("app.discord.corrections.managed_session", self.managed_session_override):
            learned_parse, event = get_learned_rule_match("$11 zelle")

        self.assertIsNotNone(learned_parse)
        self.assertEqual(learned_parse["parsed_type"], "sell")
        self.assertEqual(learned_parse["parsed_amount"], 11.0)
        self.assertEqual(event["status"], "applied")


if __name__ == "__main__":
    unittest.main()
