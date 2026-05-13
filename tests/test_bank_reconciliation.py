import json
from datetime import datetime, timezone

from sqlmodel import Session, SQLModel, create_engine, select

from app.discord.bank_reconciliation import (
    bank_row_review_reason,
    bank_row_suggested_action,
    build_bank_review_items,
    build_finance_bank_expense_data,
    categorize_bank_payload,
    compute_bank_row_dedupe_key,
    import_bank_statement_file,
    match_bank_rows_to_transactions,
    summarize_bank_transactions,
)
from app.models import BankStatementImport, BankTransaction, Transaction


class FakeBankRow:
    def __init__(self, amount, expense_category):
        self.amount = amount
        self.expense_category = expense_category
        self.classification = "expense_or_purchase_needs_review"
        self.review_status = "open"


class FakeMatchedTransaction:
    entry_kind = "buy"
    expense_category = "inventory"
    category = "inventory"


def test_categorize_psa_as_grading_fee():
    result = categorize_bank_payload(
        {
            "amount": -934.99,
            "description": "WWW.PSACARD.COM",
            "raw_row_json": json.dumps({"Category": "Merchandise & Inventory"}),
        }
    )

    assert result["expense_category"] == "grading_fees"
    assert result["category_confidence"] == "high"


def test_categorize_zelle_outflow_as_inventory_purchase():
    result = categorize_bank_payload(
        {
            "amount": -5000.00,
            "description": "Zelle payment to Example Seller JPM99abc",
        }
    )

    assert result["expense_category"] == "inventory_purchases"
    assert result["category_confidence"] == "medium"


def test_partner_paybacks_are_not_inventory_purchases():
    for payee in ("Chia Hua Wang", "Chia Wang", "Jeffrey Lee"):
        result = categorize_bank_payload(
            {
                "amount": -5000.00,
                "description": f"Zelle payment to {payee} JPM99abc",
            }
        )

        assert result["expense_category"] == "partner_paybacks"
        assert result["category_confidence"] == "high"


def test_partner_payback_rule_overrides_matched_inventory_transaction():
    result = categorize_bank_payload(
        {
            "amount": -5000.00,
            "description": "Zelle payment to Jeffrey Lee JPM99abc",
        },
        FakeMatchedTransaction(),
    )

    assert result["expense_category"] == "partner_paybacks"


def test_check_outflows_are_payroll():
    result = categorize_bank_payload(
        {
            "amount": -1250.00,
            "description": "CHECK PAID",
            "check_or_slip": "1042",
        }
    )

    assert result["expense_category"] == "payroll"
    assert result["category_confidence"] == "high"


def test_summary_excludes_transfers_from_expense_total():
    summary = summarize_bank_transactions(
        [
            FakeBankRow(-100.0, "shipping_postage"),
            FakeBankRow(-250.0, "transfers"),
            FakeBankRow(-400.0, "loan_owner_payments"),
            FakeBankRow(-50.0, "partner_paybacks"),
        ]
    )

    assert summary["debits"] == -800.0
    assert summary["expense_total"] == 100.0
    assert summary["non_operating_debits"] == 700.0


def test_finance_bank_data_excludes_discord_matches_from_bank_only_totals():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    posted_at = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)

    with Session(engine) as session:
        session.add(
            BankStatementImport(
                id=1,
                label="Test import",
                account_label="Checking",
            )
        )
        session.add(
            BankTransaction(
                import_id=1,
                row_index=2,
                account_label="Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Logged shipping",
                amount=-100.0,
                classification="logged_in_discord_strong",
                expense_category="shipping_postage",
                matched_transaction_id=123,
            )
        )
        session.add(
            BankTransaction(
                import_id=1,
                row_index=3,
                account_label="Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Bank-only shipping",
                amount=-80.0,
                classification="expense_or_purchase_needs_review",
                expense_category="shipping_postage",
            )
        )
        session.add(
            BankTransaction(
                import_id=1,
                row_index=4,
                account_label="Checking",
                account_type="checking",
                posted_at=posted_at,
                description="Zelle payment to Jeffrey Lee",
                amount=-50.0,
                classification="direct_payment_out_needs_log_check",
                expense_category="partner_paybacks",
            )
        )
        session.commit()

        data = build_finance_bank_expense_data(
            session,
            start=datetime(2026, 5, 1, tzinfo=timezone.utc),
            end=datetime(2026, 5, 2, tzinfo=timezone.utc),
        )

    shipping = next(row for row in data["category_rows"] if row["category"] == "shipping_postage")
    assert data["gross_outflow_total"] == 230.0
    assert data["discord_logged_total"] == 100.0
    assert data["bank_only_total"] == 130.0
    assert data["operating_total"] == 80.0
    assert data["non_operating_total"] == 50.0
    assert shipping["total"] == 180.0
    assert shipping["discord_logged_total"] == 100.0
    assert shipping["bank_only_total"] == 80.0


def test_apple_cash_bank_row_does_not_match_cash_discord_buy():
    bank_rows = [
        {
            "posted_at": datetime(2026, 5, 15, 12, tzinfo=timezone.utc),
            "description": "PYMT SENT APPLE CASH SENT MONEY CUPERTINO CA 8293",
            "amount": -280.0,
        }
    ]
    cash_buy = Transaction(
        id=1282,
        source_message_id=2600,
        occurred_at=datetime(2026, 5, 10, 5, tzinfo=timezone.utc),
        parse_status="parsed",
        entry_kind="buy",
        payment_method="cash",
        expense_category="inventory",
        amount=280.0,
        money_in=0.0,
        money_out=280.0,
        source_content="Bought $280",
    )

    match_bank_rows_to_transactions(bank_rows, [cash_buy])

    assert bank_rows[0]["matched_transaction_id"] is None
    assert bank_rows[0]["matched_source_message_id"] is None
    assert bank_rows[0]["matched_platform"] is None
    assert bank_rows[0]["classification"] == "direct_payment_out_needs_log_check"


def test_bank_credit_does_not_match_discord_buy_outflow():
    bank_rows = [
        {
            "posted_at": datetime(2026, 5, 11, 12, tzinfo=timezone.utc),
            "description": "Zelle payment from LONG NGUYEN BACovtim2q6e",
            "amount": 185.0,
        }
    ]
    buy = Transaction(
        id=1390,
        source_message_id=2900,
        occurred_at=datetime(2026, 5, 11, 1, tzinfo=timezone.utc),
        parse_status="parsed",
        entry_kind="buy",
        payment_method="zelle",
        expense_category="inventory",
        amount=185.0,
        money_in=0.0,
        money_out=185.0,
        source_content="Bought singles for 185 zelle",
    )

    match_bank_rows_to_transactions(bank_rows, [buy])

    assert bank_rows[0]["matched_transaction_id"] is None
    assert bank_rows[0]["matched_source_message_id"] is None
    assert bank_rows[0]["matched_platform"] is None
    assert bank_rows[0]["classification"] == "direct_customer_payment_needs_log_check"


def test_amazon_prime_video_does_not_match_discord_inventory_buy():
    bank_rows = [
        {
            "posted_at": datetime(2026, 5, 15, 12, tzinfo=timezone.utc),
            "description": "Amazon Prime Video",
            "amount": -11.99,
            "raw_row_json": json.dumps({"Category": "Entertainment"}),
        }
    ]
    discord_buy = Transaction(
        id=1401,
        source_message_id=3001,
        occurred_at=datetime(2026, 5, 13, 12, tzinfo=timezone.utc),
        parse_status="parsed",
        entry_kind="buy",
        payment_method="cash",
        expense_category="inventory",
        amount=11.99,
        money_in=0.0,
        money_out=11.99,
        source_content="Bought for 12 cash",
    )

    match_bank_rows_to_transactions(bank_rows, [discord_buy])

    assert bank_rows[0]["matched_transaction_id"] is None
    assert bank_rows[0]["matched_platform"] is None
    assert bank_rows[0]["classification"] == "expense_or_purchase_needs_review"
    assert bank_rows[0]["expense_category"] == "meals_entertainment"


def _make_test_engine():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def _csv_bytes(rows):
    header = "Posting Date,Description,Amount\n"
    body = "\n".join(rows)
    return (header + body + "\n").encode("utf-8")


class TestBankRowDedupeKey:
    """Area C: deterministic row-level dedupe key for overlapping CSV imports."""

    def test_key_is_deterministic_for_same_input(self):
        row = {
            "account_label": "Chase Checking",
            "account_type": "checking",
            "posted_at": datetime(2026, 5, 10, 12, tzinfo=timezone.utc),
            "transaction_at": None,
            "description": "ZELLE PAYMENT FROM ALICE 12345",
            "description_stem": "ZELLE PAYMENT FROM ALICE #",
            "amount": -185.0,
            "check_or_slip": "",
            "raw_type": "DEBIT",
        }
        key_a = compute_bank_row_dedupe_key(row, occurrence_index=0)
        key_b = compute_bank_row_dedupe_key(row, occurrence_index=0)
        assert key_a == key_b
        assert isinstance(key_a, str)
        assert len(key_a) >= 16

    def test_key_differs_for_different_amounts(self):
        base = {
            "account_label": "Chase Checking",
            "account_type": "checking",
            "posted_at": datetime(2026, 5, 10, 12, tzinfo=timezone.utc),
            "description": "ZELLE PAYMENT FROM ALICE",
            "description_stem": "ZELLE PAYMENT FROM ALICE",
            "amount": -185.0,
            "check_or_slip": "",
            "raw_type": "DEBIT",
        }
        other = dict(base, amount=-186.0)
        assert compute_bank_row_dedupe_key(base, occurrence_index=0) != compute_bank_row_dedupe_key(other, occurrence_index=0)

    def test_key_differs_for_different_account(self):
        base = {
            "account_label": "Chase Checking",
            "account_type": "checking",
            "posted_at": datetime(2026, 5, 10, 12, tzinfo=timezone.utc),
            "description": "ZELLE PAYMENT FROM ALICE",
            "description_stem": "ZELLE PAYMENT FROM ALICE",
            "amount": -185.0,
            "check_or_slip": "",
            "raw_type": "DEBIT",
        }
        other = dict(base, account_label="Chase Credit Card")
        assert compute_bank_row_dedupe_key(base, occurrence_index=0) != compute_bank_row_dedupe_key(other, occurrence_index=0)

    def test_key_differs_per_occurrence(self):
        row = {
            "account_label": "Chase Checking",
            "account_type": "checking",
            "posted_at": datetime(2026, 5, 10, 12, tzinfo=timezone.utc),
            "description": "STARBUCKS",
            "description_stem": "STARBUCKS",
            "amount": -5.0,
            "check_or_slip": "",
            "raw_type": "DEBIT",
        }
        k0 = compute_bank_row_dedupe_key(row, occurrence_index=0)
        k1 = compute_bank_row_dedupe_key(row, occurrence_index=1)
        assert k0 != k1

    def test_key_ignores_optional_balance_for_column_layout_independence(self):
        base = {
            "account_label": "Chase Checking",
            "account_type": "checking",
            "posted_at": datetime(2026, 5, 10, 12, tzinfo=timezone.utc),
            "description": "STARBUCKS STORE",
            "description_stem": "STARBUCKS STORE",
            "amount": -5.0,
            "check_or_slip": "",
            "raw_type": "DEBIT",
        }
        with_balance = dict(base, balance=95.0)

        assert compute_bank_row_dedupe_key(base, occurrence_index=0) == compute_bank_row_dedupe_key(
            with_balance,
            occurrence_index=0,
        )


class TestOverlappingCsvImports:
    """Importing CSVs that share rows must dedupe at the row level."""

    def test_same_file_hash_short_circuits_idempotently(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 12345,150.00",
            "05/10/2026,Starbucks store,-5.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="chase-a.csv", content=csv_a, account_label="Chase Checking"
            )
            first_rows = list(session.exec(select(BankTransaction)).all())
            import_bank_statement_file(
                session, filename="chase-a.csv", content=csv_a, account_label="Chase Checking"
            )
            second_rows = list(session.exec(select(BankTransaction)).all())
        assert len(first_rows) == 2
        assert len(second_rows) == 2

    def test_overlapping_csv_skips_duplicate_rows(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 12345,150.00",
            "05/10/2026,Starbucks store,-5.00",
        ])
        csv_b = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/12/2026,Amazon Prime Video,-11.99",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="chase-a.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="chase-b.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 3
        descriptions = sorted(row.description for row in rows)
        assert descriptions.count("Starbucks store") == 1
        assert "Amazon Prime Video" in descriptions

    def test_overlap_dedupe_respects_account_label_boundary(self):
        engine = _make_test_engine()
        csv_checking = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
        ])
        csv_credit = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="chk.csv", content=csv_checking, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="cc.csv", content=csv_credit, account_label="Chase Credit Card"
            )
            rows = list(session.exec(select(BankTransaction)).all())
        assert len(rows) == 2

    def test_legitimately_repeating_rows_within_same_csv_are_kept(self):
        engine = _make_test_engine()
        csv = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="cafe.csv", content=csv, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())
        assert len(rows) == 3

    def test_later_single_repeated_transaction_is_kept(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 12345,150.00",
        ])
        csv_b = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 67890,150.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-zelle.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="second-zelle.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 2
        assert len({row.row_dedupe_key for row in rows}) == 2

    def test_single_row_exact_duplicate_reimport_with_different_csv_shape_is_skipped(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
        ])
        csv_b = b"Date,Description,Amount,Ignored\n05/10/2026,Starbucks store,-5.00,extra\n"
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-starbucks.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="single-starbucks-different-shape.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 1
        assert rows[0].description == "Starbucks store"

    def test_duplicate_reimport_with_later_balance_column_is_skipped(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
        ])
        csv_b = b"Posting Date,Description,Amount,Balance\n05/10/2026,Starbucks store,-5.00,95.00\n"
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-starbucks-no-balance.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="second-starbucks-with-balance.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 1
        assert rows[0].description == "Starbucks store"

    def test_single_row_exact_repeat_with_distinct_balance_is_kept(self):
        engine = _make_test_engine()
        csv_a = b"Posting Date,Description,Amount,Balance\n05/10/2026,Starbucks store,-5.00,95.00\n"
        csv_b = b"Posting Date,Description,Amount,Balance\n05/10/2026,Starbucks store,-5.00,90.00\n"
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-starbucks-balance.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="second-starbucks-balance.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 2
        assert sorted(row.balance for row in rows if row.balance is not None) == [90.0, 95.0]
        assert len({row.row_dedupe_key for row in rows}) == 2

    def test_single_row_later_balance_repeat_after_no_balance_row_is_kept(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Starbucks store 0001,-5.00",
        ])
        csv_b = b"Posting Date,Description,Amount,Balance\n05/10/2026,Starbucks store 0002,-5.00,90.00\n"
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-starbucks-no-balance.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="second-starbucks-balance-repeat.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 2
        balances = sorted(row.balance for row in rows if row.balance is not None)
        assert [row.balance for row in rows].count(None) == 1
        assert balances == [90.0]
        assert len({row.row_dedupe_key for row in rows}) == 2

    def test_reimporting_multirow_repeating_export_does_not_duplicate_rows(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
        ])
        csv_b = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00,ignored extra export column",
            "05/10/2026,Starbucks store,-5.00,ignored extra export column",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-export.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="second-export.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 2
        assert len({row.row_dedupe_key for row in rows}) == 2

    def test_single_row_later_no_balance_repeat_after_balance_row_is_skipped(self):
        engine = _make_test_engine()
        csv_a = "Posting Date,Description,Amount,Balance\n05/10/2026,Starbucks store,-5.00,95.00\n".encode()
        csv_b = "Posting Date,Description,Amount\n05/10/2026,Starbucks store,-5.00\n".encode()
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="with-balance.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="without-balance.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 1
        assert rows[0].balance == 95.0

    def test_newer_first_balance_export_skips_later_overlap_with_added_balance(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes(["05/10/2026,Zelle payment from Alice 12345,150.00"])
        csv_b = "".join([
            "Posting Date,Description,Amount,Balance\n",
            "05/10/2026,Zelle payment from Alice 67890,150.00,250.00\n",
            "05/10/2026,Zelle payment from Alice 12345,150.00,100.00\n",
        ]).encode()
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-zelle-no-balance.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="newer-first-with-balance.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 2
        descriptions = sorted(row.description for row in rows)
        assert descriptions == [
            "Zelle payment from Alice 12345",
            "Zelle payment from Alice 67890",
        ]
        assert len({row.row_dedupe_key for row in rows}) == 2

    def test_newer_first_balance_export_skips_trailing_identical_overlap(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes(["05/10/2026,Starbucks store,-5.00"])
        csv_b = "".join([
            "Posting Date,Description,Amount,Balance\n",
            "05/10/2026,Starbucks store,-5.00,90.00\n",
            "05/10/2026,Starbucks store,-5.00,95.00\n",
        ]).encode()
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-starbucks-no-balance.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="newer-first-identical-with-balance.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 2
        balances = sorted(row.balance for row in rows if row.balance is not None)
        assert [row.balance for row in rows].count(None) == 1
        assert balances == [90.0]
        assert len({row.row_dedupe_key for row in rows}) == 2

    def test_newer_first_no_balance_export_keeps_leading_identical_repeat(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/11/2026,Amazon,-7.00",
        ])
        csv_b = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
            "05/11/2026,Amazon,-7.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-starbucks-amazon.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="newer-first-no-balance-repeat.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 3
        descriptions = sorted(row.description for row in rows)
        assert descriptions == ["Amazon", "Starbucks store", "Starbucks store"]
        assert len({row.row_dedupe_key for row in rows}) == 3

    def test_multirow_reimport_keeps_new_legitimate_repeat_beyond_existing_count(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
        ])
        csv_b = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
            "05/10/2026,Starbucks store,-5.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-two-repeats.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="later-three-repeats.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 3
        assert len({row.row_dedupe_key for row in rows}) == 3

    def test_mixed_partial_export_keeps_new_same_fingerprint_repeat(self):
        engine = _make_test_engine()
        csv_a = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 12345,150.00",
        ])
        csv_b = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 67890,150.00",
            "05/11/2026,Amazon,-5.00",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="first-zelle.csv", content=csv_a, account_label="Chase Checking"
            )
            import_bank_statement_file(
                session, filename="mixed-repeat.csv", content=csv_b, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())

        assert len(rows) == 3
        descriptions = sorted(row.description for row in rows)
        assert descriptions == [
            "Amazon",
            "Zelle payment from Alice 12345",
            "Zelle payment from Alice 67890",
        ]
        assert len({row.row_dedupe_key for row in rows}) == 3

    def test_historical_null_dedupe_key_row_is_backfilled_and_skipped(self):
        engine = _make_test_engine()
        posted_at = datetime(2026, 5, 10, 12, tzinfo=timezone.utc)
        csv = _csv_bytes([
            "05/10/2026,Starbucks store,-5.00",
        ])
        with Session(engine) as session:
            session.add(
                BankStatementImport(
                    id=1,
                    label="Legacy import",
                    account_label="Chase Checking",
                )
            )
            legacy_row = BankTransaction(
                import_id=1,
                row_index=2,
                account_label="Chase Checking",
                account_type="checking",
                posted_at=posted_at,
                transaction_at=None,
                description="Starbucks store",
                description_stem="STARBUCKS STORE",
                amount=-5.0,
                check_or_slip="",
                raw_type="",
                row_dedupe_key=None,
            )
            session.add(legacy_row)
            session.commit()

            import_bank_statement_file(
                session, filename="overlap.csv", content=csv, account_label="Chase Checking"
            )

            rows = list(session.exec(select(BankTransaction)).all())
            session.refresh(legacy_row)

        assert len(rows) == 1
        assert rows[0].description == "Starbucks store"
        assert legacy_row.row_dedupe_key


class TestBankReviewReason:
    """Area E: bank-row review reason + suggested action helpers."""

    def _row(self, **overrides):
        defaults = dict(
            classification="expense_or_purchase_needs_review",
            confidence="low",
            amount=-25.0,
            description="Some descriptor",
            expense_category="uncategorized",
            review_status="open",
            matched_transaction_id=None,
            match_reason="",
            category_confidence="low",
        )
        defaults.update(overrides)
        return type("FakeRow", (), defaults)()

    def test_cash_deposit_needs_source(self):
        row = self._row(
            classification="cash_deposit_needs_source",
            amount=200.0,
            description="ATM CASH DEPOSIT",
            expense_category="cash_deposits",
        )
        reason = bank_row_review_reason(row)
        action = bank_row_suggested_action(row)
        assert "deposit" in reason.lower()
        assert action
        assert "source" in action.lower() or "sale" in action.lower()

    def test_direct_customer_payment_needs_log_check(self):
        row = self._row(
            classification="direct_customer_payment_needs_log_check",
            amount=185.0,
            description="ZELLE PAYMENT FROM ALICE",
            expense_category="sales_collections",
        )
        reason = bank_row_review_reason(row)
        action = bank_row_suggested_action(row)
        assert reason
        assert "discord" in action.lower() or "match" in action.lower() or "log" in action.lower()

    def test_uncategorized_expense_suggests_categorization(self):
        row = self._row(
            amount=-50.0,
            classification="expense_or_purchase_needs_review",
            expense_category="uncategorized",
            description="Unknown vendor",
        )
        reason = bank_row_review_reason(row)
        action = bank_row_suggested_action(row)
        assert reason
        assert "category" in action.lower() or "categor" in action.lower()

    def test_high_confidence_matched_row_has_no_review_reason(self):
        row = self._row(
            classification="logged_in_discord_strong",
            confidence="high",
            matched_transaction_id=42,
            match_reason="Amount/date match, score 120.",
            amount=-50.0,
            expense_category="inventory_purchases",
        )
        assert bank_row_review_reason(row) == ""
        assert bank_row_suggested_action(row) == ""

    def test_high_confidence_categorized_bank_row_has_no_review_or_action(self):
        row = self._row(
            classification="expense_or_purchase_needs_review",
            amount=-50.0,
            expense_category="inventory_purchases",
            category_confidence="high",
        )
        assert bank_row_review_reason(row) == ""
        assert bank_row_suggested_action(row) == ""

    def test_resolved_row_has_no_review_reason(self):
        row = self._row(
            classification="cash_deposit_needs_source",
            review_status="resolved",
            amount=200.0,
        )
        assert bank_row_review_reason(row) == ""
        assert bank_row_suggested_action(row) == ""


class TestBuildBankReviewItems:
    def test_returns_only_actionable_rows(self):
        engine = _make_test_engine()
        csv = _csv_bytes([
            "05/10/2026,Zelle payment from Alice 12345,150.00",
            "05/10/2026,ATM cash deposit,500.00",
            "05/10/2026,WWW.PSACARD.COM,-99.99",
        ])
        with Session(engine) as session:
            import_bank_statement_file(
                session, filename="x.csv", content=csv, account_label="Chase Checking"
            )
            rows = list(session.exec(select(BankTransaction)).all())
            items = build_bank_review_items(rows)

        # PSA row should be matched-by-rule (high confidence grading fee) so not flagged.
        flagged_descriptions = sorted(item["description"] for item in items)
        assert any("Zelle payment from Alice" in d for d in flagged_descriptions)
        assert any("ATM cash deposit" in d for d in flagged_descriptions)
        assert not any("PSACARD" in d for d in flagged_descriptions)
        for item in items:
            assert item["reason"]
            assert item["suggested_action"]


def test_discord_owe_me_note_does_not_match_bank_outflow():
    bank_rows = [
        {
            "posted_at": datetime(2026, 5, 16, 12, tzinfo=timezone.utc),
            "description": "PYMT SENT APPLE CASH BALANCE CUPERTINO CA",
            "amount": -2500.0,
        }
    ]
    owed_later = Transaction(
        id=1402,
        source_message_id=3002,
        occurred_at=datetime(2026, 5, 13, 12, tzinfo=timezone.utc),
        parse_status="parsed",
        entry_kind="buy",
        payment_method="unknown",
        expense_category="inventory",
        amount=2500.0,
        money_in=0.0,
        money_out=2500.0,
        source_content="Bought airbnb 2500$ (owe me)",
    )

    match_bank_rows_to_transactions(bank_rows, [owed_later])

    assert bank_rows[0]["matched_transaction_id"] is None
    assert bank_rows[0]["matched_platform"] is None
    assert bank_rows[0]["classification"] == "direct_payment_out_needs_log_check"
