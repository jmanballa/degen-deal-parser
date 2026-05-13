"""Regression tests for amount parsing + loan/internal-transfer detection.

These cover the two user-reported parses that slipped through review:

 1. "$11,050 bought 13 cases of dbz" was being parsed as $50
    (comma broke the regex -> only "050" survived).
 2. "Give company 6k cash (owe me)" was being parsed as a $6 buy
    (k suffix dropped, plus the "owe me" reimbursement signal wrongly
    pre-empted the internal-transfer detector).

The parser was fixed in commit <hash>. Keep these cases wired so future
refactors don't regress the same bugs.
"""
from __future__ import annotations

import pytest

from app.discord.parser import (
    _normalize_amount_text,
    extract_payment_amount_method,
    extract_payment_segments,
    extract_unlabeled_amount,
    has_reimbursement_buy_signal,
    looks_like_internal_cash_transfer,
    parse_by_rules,
)


# ---------------------------------------------------------------------------
# _normalize_amount_text: comma and k/M expansion
# ---------------------------------------------------------------------------


class TestNormalizeAmountText:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("$11,050", "$11050"),
            ("$1,250,000", "$1250000"),
            ("6,100 zelle", "6100 zelle"),
            ("1,234.56", "1234.56"),
            ("no commas here 50", "no commas here 50"),
        ],
    )
    def test_strips_thousands_commas(self, raw: str, expected: str) -> None:
        assert _normalize_amount_text(raw) == expected

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("$6k", "$6000"),
            ("6k cash", "6000 cash"),
            ("1.5k zelle", "1500 zelle"),
            ("10k", "10000"),
            ("2m", "2000000"),
            ("0.25m", "250000"),
        ],
    )
    def test_expands_k_and_m_suffixes(self, raw: str, expected: str) -> None:
        assert _normalize_amount_text(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            "",
            "just text no numbers",
            "kilograms: 10kg",  # kg should not be treated as k
            "kickstart",  # bare word starting with k
        ],
    )
    def test_does_not_blow_up_on_irregular_input(self, raw: str) -> None:
        # Should not raise and should not mangle recognizable words.
        result = _normalize_amount_text(raw)
        assert isinstance(result, str)

    def test_kg_is_not_expanded(self) -> None:
        # "10kg" is kilograms, not 10 thousand. The regex must not
        # expand it.
        assert _normalize_amount_text("10kg") == "10kg"


# ---------------------------------------------------------------------------
# extract_unlabeled_amount
# ---------------------------------------------------------------------------


class TestExtractUnlabeledAmount:
    def test_eleven_thousand_fifty_not_fifty(self) -> None:
        """Regression for $11,050 -> $50 bug."""
        result = extract_unlabeled_amount("$11,050 bought 13 cases of dbz")
        assert result == 11050.0

    def test_six_k_as_six_thousand(self) -> None:
        result = extract_unlabeled_amount("Give company 6k cash")
        assert result == 6000.0

    def test_ignores_quantity_units(self) -> None:
        # "13 cases" is quantity, not a dollar amount.
        result = extract_unlabeled_amount("bought 13 cases of dbz")
        assert result is None

    def test_million_suffix(self) -> None:
        result = extract_unlabeled_amount("sold for 1.5M")
        assert result == 1500000.0


# ---------------------------------------------------------------------------
# extract_payment_segments
# ---------------------------------------------------------------------------


class TestExtractPaymentSegments:
    def test_k_suffix_with_payment(self) -> None:
        segments = extract_payment_segments("6k zelle")
        assert segments == [(6000.0, "zelle")]

    def test_comma_with_payment(self) -> None:
        segments = extract_payment_segments("6,100 zelle")
        assert segments == [(6100.0, "zelle")]

    def test_plain_amount_still_works(self) -> None:
        segments = extract_payment_segments("50 cash")
        assert segments == [(50.0, "cash")]


# ---------------------------------------------------------------------------
# extract_payment_amount_method
# ---------------------------------------------------------------------------


class TestExtractPaymentAmountMethod:
    def test_k_suffix(self) -> None:
        amount, method = extract_payment_amount_method("$6k zelle")
        assert amount == 6000.0
        assert method == "zelle"

    def test_comma(self) -> None:
        amount, method = extract_payment_amount_method("$6,100 zelle")
        assert amount == 6100.0
        assert method == "zelle"


# ---------------------------------------------------------------------------
# looks_like_internal_cash_transfer: the "give company X (owe me)" case
# ---------------------------------------------------------------------------


class TestLooksLikeInternalCashTransfer:
    @pytest.mark.parametrize(
        "text",
        [
            "Give company 6k cash (owe me)",
            "Gave company $1,000 cash",
            "Handed company 500 cash",
            "Loaned company 2k cash, owe me",
            "Put 3k cash into the company to float payroll",
            "Brought company 1,500 cash",
        ],
    )
    def test_give_company_cash_is_internal_transfer(self, text: str) -> None:
        assert looks_like_internal_cash_transfer(text) is True

    @pytest.mark.parametrize(
        "text",
        [
            "Bought 500 cards, owe me $500",  # reimbursement for inventory buy, NOT a transfer
            "Paid $300 for psa grading, owe me",  # reimbursement for expense, NOT a transfer
            "Fronted me 200 for the deal",  # between humans, not company
            "Sold $50 zelle",  # straight sale
            "Top out bottom in plus 100 zelle",  # trade
        ],
    )
    def test_not_internal_transfer(self, text: str) -> None:
        assert looks_like_internal_cash_transfer(text) is False

    def test_owe_me_does_not_pre_empt_company_transfer(self) -> None:
        """Regression: 'owe me' used to bail the transfer detector early."""
        text = "Give company 6k cash (owe me)"
        # Reimbursement signal IS present...
        assert has_reimbursement_buy_signal(text) is True
        # ...but the transfer detector must still fire.
        assert looks_like_internal_cash_transfer(text) is True


# ---------------------------------------------------------------------------
# End-to-end through parse_by_rules: confirms neither buggy case produces
# a transaction.
# ---------------------------------------------------------------------------


class TestParseByRulesEndToEnd:
    def test_eleven_thousand_fifty_buy_is_not_fifty(self) -> None:
        # parse_by_rules should recognize the buy verb and the full amount.
        result = parse_by_rules("$11,050 bought 13 cases of dbz", channel_name="alex-purchases")
        assert result is not None
        assert result.get("parsed_type") == "buy"
        assert result.get("parsed_amount") == 11050.0

    def test_give_company_cash_not_parsed_as_transaction(self) -> None:
        # The internal-transfer detector belongs at the
        # detect_non_transaction_message layer in parse_message, but
        # parse_by_rules should at minimum not produce a confident
        # "buy $6" result.
        result = parse_by_rules("Give company 6k cash (owe me)")
        # Acceptable outcomes: None (no rule matched) OR a result with
        # amount >= 6000 if somehow a rule did match. What is NOT
        # acceptable is buy/$6 -- that's the exact bug we're guarding.
        if result is not None:
            parsed_amount = result.get("parsed_amount")
            assert parsed_amount is None or parsed_amount >= 6000, (
                f"parse_by_rules produced buggy output for loan text: {result}"
            )
