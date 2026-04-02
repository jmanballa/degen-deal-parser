import unittest

from app.financials import compute_financials
from app.parser import (
    has_transaction_signal,
    infer_explicit_buy_sell_type,
    looks_like_internal_cash_transfer,
    parse_by_rules,
)


class ParserStoreRulesTests(unittest.TestCase):
    def test_payment_only_defaults_to_buy_in_store_buys_channel(self):
        parsed = parse_by_rules("$22 zelle", channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")
        self.assertEqual(parsed["parsed_amount"], 22.0)
        self.assertEqual(parsed["parsed_payment_method"], "zelle")
        self.assertIsNone(parsed["parsed_cash_direction"])

    def test_payment_only_defaults_to_buy_in_purchases_channel(self):
        parsed = parse_by_rules("$22 zelle", channel_name="team-purchases")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")

    def test_payment_only_defaults_to_sell_elsewhere(self):
        parsed = parse_by_rules("$22 zelle", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertIsNone(parsed["parsed_cash_direction"])

    def test_owe_me_phrase_counts_as_buy_not_internal_transfer(self):
        message_text = "Gts distro 3183$ (owe me)"
        self.assertEqual(infer_explicit_buy_sell_type(message_text), "buy")
        self.assertTrue(has_transaction_signal(message_text))
        self.assertFalse(looks_like_internal_cash_transfer(message_text))
        parsed = parse_by_rules(message_text, channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")
        self.assertEqual(parsed["parsed_amount"], 3183.0)
        self.assertFalse(parsed["needs_review"])

    def test_reimburse_us_phrase_counts_as_buy_and_skips_review(self):
        message_text = "reimburse us 145"
        self.assertEqual(infer_explicit_buy_sell_type(message_text), "buy")
        parsed = parse_by_rules(message_text, channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")
        self.assertEqual(parsed["parsed_amount"], 145.0)
        self.assertFalse(parsed["needs_review"])

    def test_cash_direction_only_changes_trade_financials(self):
        buy_financials = compute_financials(
            parsed_type="buy",
            parsed_category="unknown",
            amount=22.0,
            cash_direction="to_store",
            message_text="$22 zelle",
        )
        self.assertEqual(buy_financials.money_in, 0.0)
        self.assertEqual(buy_financials.money_out, 22.0)

        sale_financials = compute_financials(
            parsed_type="sell",
            parsed_category="unknown",
            amount=22.0,
            cash_direction="from_store",
            message_text="$22 zelle",
        )
        self.assertEqual(sale_financials.money_in, 22.0)
        self.assertEqual(sale_financials.money_out, 0.0)

        trade_financials = compute_financials(
            parsed_type="trade",
            parsed_category="mixed",
            amount=22.0,
            cash_direction="from_store",
            message_text="trade + 22 cash",
        )
        self.assertEqual(trade_financials.money_in, 0.0)
        self.assertEqual(trade_financials.money_out, 22.0)


if __name__ == "__main__":
    unittest.main()
