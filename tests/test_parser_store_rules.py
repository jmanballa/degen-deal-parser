import unittest

from app.discord.financials import compute_financials
from app.discord.parser import (
    detect_non_transaction_message,
    build_prompt,
    has_transaction_signal,
    infer_explicit_buy_sell_type,
    looks_like_internal_cash_transfer,
    normalize_payment_method,
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

    def test_worker_message_prefix_does_not_block_payment_only_default(self):
        parsed = parse_by_rules("Message 1: 140 cash", channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")
        self.assertEqual(parsed["parsed_amount"], 140.0)
        self.assertEqual(parsed["parsed_payment_method"], "cash")

    def test_payment_shorthand_with_context_defaults_by_channel(self):
        parsed = parse_by_rules(
            "Message 1: 106 Zelle for Andrew employee discount",
            channel_name="║store-sales-and-trades",
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 106.0)
        self.assertEqual(parsed["parsed_payment_method"], "zelle")
        self.assertFalse(parsed["needs_review"])

    def test_trailing_dollar_payment_shorthand(self):
        parsed = parse_by_rules("Message 1: 623$ Zelle", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 623.0)
        self.assertEqual(parsed["parsed_payment_method"], "zelle")

    def test_multi_payment_shorthand_does_not_double_count_overlapping_tokens(self):
        parsed = parse_by_rules("Message 1: 50$ cash 260 Zelle", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 310.0)
        self.assertEqual(parsed["parsed_payment_method"], "mixed")

        parsed = parse_by_rules("Message 1: cash 50 zelle 260", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_amount"], 310.0)
        self.assertEqual(parsed["parsed_payment_method"], "mixed")

    def test_stitched_multi_payment_fragments_are_summed(self):
        parsed = parse_by_rules(
            "Message 1: Sold singles\n\nMessage 2: 50 cash\n\nMessage 3: 260 zelle",
            channel_name="store-sales-and-trades",
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 310.0)
        self.assertEqual(parsed["parsed_payment_method"], "mixed")
        self.assertFalse(parsed["needs_review"])

    def test_zelled_phrase_defaults_to_sale(self):
        parsed = parse_by_rules("Message 1: my sister zelled 38", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 38.0)
        self.assertEqual(parsed["parsed_payment_method"], "zelle")

    def test_item_out_cash_in_flow_is_sale_not_failed_trade(self):
        parsed = parse_by_rules(
            "Message 1: Sealed out, 4750 cash in 95% (eBay comps last sold: 2,515, 1100, 600, 550)",
            channel_name="║store-sales-and-trades",
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 4750.0)
        self.assertEqual(parsed["parsed_payment_method"], "cash")
        self.assertEqual(parsed["parsed_category"], "sealed")

    def test_plus_in_sale_text_does_not_force_trade_path(self):
        parsed = parse_by_rules("Message 1: Sold box plus 50 cash", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_amount"], 50.0)
        self.assertEqual(parsed["parsed_payment_method"], "cash")

    def test_trade_on_top_trailing_dollar_extracts_cash(self):
        parsed = parse_by_rules(
            "Message 1: Trade left out, right in with 40$ on top",
            channel_name="store-sales-and-trades",
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "trade")
        self.assertEqual(parsed["parsed_amount"], 40.0)
        self.assertEqual(parsed["parsed_cash_direction"], "to_store")

    def test_bougjt_typo_counts_as_buy(self):
        parsed = parse_by_rules("Message 1: Bougjt for 900 cash", channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")
        self.assertEqual(parsed["parsed_amount"], 900.0)
        self.assertEqual(parsed["parsed_payment_method"], "cash")

    def test_sold_us_counts_as_store_buy(self):
        parsed = parse_by_rules("Message 1: guy came in sold us binder 300 zelle", channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "buy")
        self.assertEqual(parsed["parsed_amount"], 300.0)
        self.assertEqual(parsed["parsed_payment_method"], "zelle")

    def test_contact_followup_date_is_ignored(self):
        parsed = detect_non_transaction_message("TEXTED 5/8/26", image_urls=[])
        self.assertIsNotNone(parsed)
        self.assertTrue(parsed["ignore_message"])

    def test_ai_prompt_mentions_json_for_openai_compatible_response_format(self):
        prompt = build_prompt(
            author_name="cashier",
            message_text="Message 1: 125 Zelle",
            rule_hint=None,
            has_images=True,
            channel_name="store-sales-and-trades",
        )
        self.assertIn("JSON", prompt)

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


class ApplePayMappingTests(unittest.TestCase):
    def test_normalize_apple_pay_variants(self):
        self.assertEqual(normalize_payment_method("apple_pay"), "apple_pay")
        self.assertEqual(normalize_payment_method("applepay"), "apple_pay")
        self.assertEqual(normalize_payment_method("appstd"), "apple_pay")
        self.assertEqual(normalize_payment_method("apple pay"), "apple_pay")

    def test_parse_by_rules_apple_pay_two_word(self):
        # "$800 Jeff Apple Pay" has a name in the middle — rule-based parser defers to AI,
        # but "$800 Apple Pay" without a name is handled directly.
        parsed = parse_by_rules("$800 Apple Pay", channel_name="║store-buys")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_payment_method"], "apple_pay")
        self.assertEqual(parsed["parsed_amount"], 800.0)

    def test_parse_by_rules_appstd_variant(self):
        parsed = parse_by_rules("$150 Appstd", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_payment_method"], "apple_pay")

    def test_parse_by_rules_applepay_single_word(self):
        parsed = parse_by_rules("$200 ApplePay", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_payment_method"], "apple_pay")

    def test_has_transaction_signal_apple_pay(self):
        self.assertTrue(has_transaction_signal("$800 Apple Pay"))
        self.assertTrue(has_transaction_signal("$50 Appstd"))

    def test_payment_only_apple_pay_defaults_to_sell(self):
        parsed = parse_by_rules("$100 Apple Pay", channel_name="store-sales-and-trades")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["parsed_type"], "sell")
        self.assertEqual(parsed["parsed_payment_method"], "apple_pay")
        self.assertEqual(parsed["parsed_amount"], 100.0)


if __name__ == "__main__":
    unittest.main()
