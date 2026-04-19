"""Unit tests for amount parsing — k-suffix and comma-thousands fixes."""
import pytest
from app.parser import (
    _normalize_amount_text,
    extract_unlabeled_amount,
    extract_payment_segments,
)


class TestNormalizeAmountText:
    def test_plain_number_unchanged(self):
        assert _normalize_amount_text("50") == "50"

    def test_decimal_unchanged(self):
        assert _normalize_amount_text("1.50") == "1.50"

    def test_comma_thousands_single(self):
        assert _normalize_amount_text("1,250") == "1250"

    def test_comma_thousands_larger(self):
        assert _normalize_amount_text("11,050") == "11050"

    def test_comma_thousands_with_dollar(self):
        assert _normalize_amount_text("$1,250") == "$1250"

    def test_comma_thousands_with_decimal(self):
        assert _normalize_amount_text("$1,250.00") == "$1250.00"

    def test_comma_multi_group(self):
        assert _normalize_amount_text("1,250,000") == "1250000"

    def test_k_suffix_integer(self):
        assert _normalize_amount_text("6k") == "6000"

    def test_k_suffix_uppercase(self):
        assert _normalize_amount_text("6K") == "6000"

    def test_k_suffix_decimal(self):
        assert _normalize_amount_text("1.5k") == "1500"

    def test_k_suffix_two_decimal(self):
        assert _normalize_amount_text("1.25k") == "1250"

    def test_k_suffix_with_dollar(self):
        assert _normalize_amount_text("$1.5k") == "$1500"

    def test_m_suffix_integer(self):
        assert _normalize_amount_text("1m") == "1000000"

    def test_m_suffix_decimal(self):
        assert _normalize_amount_text("1.5m") == "1500000"

    def test_k_in_word_not_expanded(self):
        # "kicks" should not be modified
        result = _normalize_amount_text("kicks")
        assert result == "kicks"

    def test_km_unit_not_expanded(self):
        # "10km" — k is not at a word boundary
        result = _normalize_amount_text("10km")
        assert result == "10km"

    def test_bare_k_not_expanded(self):
        assert _normalize_amount_text("k") == "k"

    def test_dollar_k_not_expanded(self):
        assert _normalize_amount_text("$k") == "$k"

    def test_k_before_digit_not_expanded(self):
        assert _normalize_amount_text("k5") == "k5"


class TestExtractUnlabeledAmount:
    def test_plain_integer(self):
        assert extract_unlabeled_amount("$50") == 50.0

    def test_plain_decimal(self):
        assert extract_unlabeled_amount("$50.00") == 50.0

    def test_decimal_no_dollar(self):
        assert extract_unlabeled_amount("1.5") == 1.5

    def test_comma_thousands(self):
        assert extract_unlabeled_amount("$1,250") == 1250.0

    def test_comma_thousands_no_dollar(self):
        assert extract_unlabeled_amount("1,250") == 1250.0

    def test_comma_thousands_decimal(self):
        assert extract_unlabeled_amount("$1,250.00") == 1250.0

    def test_comma_large(self):
        assert extract_unlabeled_amount("$11,050 bought 13 cases of dbz") == 11050.0

    def test_comma_trailing(self):
        # "Buy 13 case 11,050$" — comma-formatted amount
        assert extract_unlabeled_amount("Buy 13 case 11,050$") == 11050.0

    def test_k_suffix_integer(self):
        assert extract_unlabeled_amount("Give company 6k cash (owe me)") == 6000.0

    def test_k_suffix_no_context(self):
        assert extract_unlabeled_amount("$6k") == 6000.0

    def test_k_suffix_decimal(self):
        assert extract_unlabeled_amount("Sold 1.5k zelle") == 1500.0

    def test_k_suffix_uppercase(self):
        assert extract_unlabeled_amount("Sold 1.3K Zelle") == 1300.0

    def test_k_suffix_ten(self):
        assert extract_unlabeled_amount("Give company 10k cash (owe me)") == 10000.0

    def test_k_suffix_five(self):
        assert extract_unlabeled_amount("Give company 5k (owe me)") == 5000.0

    def test_k_suffix_two(self):
        assert extract_unlabeled_amount("Buy 2k") == 2000.0

    def test_m_suffix_paranoia(self):
        assert extract_unlabeled_amount("$1M") == 1_000_000.0

    def test_zero_amount(self):
        assert extract_unlabeled_amount("$0") == 0.0

    def test_empty_text(self):
        assert extract_unlabeled_amount("") is None

    def test_no_amount(self):
        assert extract_unlabeled_amount("hello world") is None

    def test_negative_not_parsed(self):
        # Negative amounts are not in scope; parser sees "50" not "-50"
        result = extract_unlabeled_amount("-$50")
        assert result == 50.0  # minus sign is not captured by the regex

    def test_bare_k_returns_none(self):
        assert extract_unlabeled_amount("just k") is None

    def test_dollar_k_returns_none(self):
        assert extract_unlabeled_amount("$k") is None


class TestExtractPaymentSegments:
    def test_plain_cash(self):
        assert extract_payment_segments("50 cash") == [(50.0, "cash")]

    def test_plain_zelle(self):
        assert extract_payment_segments("100 zelle") == [(100.0, "zelle")]

    def test_comma_thousands_zelle(self):
        # "6,100 zelle" should parse as 6100, not 100
        assert extract_payment_segments("6,100 zelle") == [(6100.0, "zelle")]

    def test_comma_large_cash(self):
        assert extract_payment_segments("$11,050 cash") == [(11050.0, "cash")]

    def test_k_suffix_cash(self):
        assert extract_payment_segments("6k cash") == [(6000.0, "cash")]

    def test_k_suffix_zelle(self):
        assert extract_payment_segments("1.5k zelle") == [(1500.0, "zelle")]

    def test_k_suffix_uppercase(self):
        assert extract_payment_segments("10K cash") == [(10000.0, "cash")]

    def test_mixed_split(self):
        segs = extract_payment_segments("500 cash + 200 zelle")
        assert (500.0, "cash") in segs
        assert (200.0, "zelle") in segs

    def test_no_payment_method(self):
        assert extract_payment_segments("no method here") == []
