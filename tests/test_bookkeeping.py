import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine

from app.discord.bookkeeping import fetch_google_sheet_export, read_tabular_rows, reconcile_bookkeeping_import
from app.models import (
    BookkeepingEntry,
    BookkeepingImport,
    DiscordMessage,
    Transaction,
    PARSE_PARSED,
)


def _utcnow():
    return datetime.now(timezone.utc)


def _dt(year, month, day):
    return datetime(year, month, day, 12, 0, 0, tzinfo=timezone.utc)


class ReadTabularRowsTests(unittest.TestCase):
    def test_csv_parses_headers_and_values(self):
        csv_bytes = b"date,kind,amount\n2024-01-01,sale,50\n2024-01-02,buy,30\n"
        rows = read_tabular_rows("export.csv", csv_bytes)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["kind"], "sale")
        self.assertEqual(rows[0]["amount"], "50")
        self.assertEqual(rows[1]["kind"], "buy")

    def test_csv_strips_utf8_bom(self):
        csv_bytes = b"\xef\xbb\xbfdate,kind,amount\n2024-01-01,sale,50\n"
        rows = read_tabular_rows("export.csv", csv_bytes)
        self.assertEqual(len(rows), 1)
        # BOM should be stripped; header should be "date" not "\ufeffdate"
        self.assertIn("date", rows[0])
        self.assertNotIn("\ufeffdate", rows[0])

    def test_csv_adds_sheet_name_key(self):
        csv_bytes = b"date,amount\n2024-01-01,50\n"
        rows = read_tabular_rows("export.csv", csv_bytes)
        self.assertEqual(rows[0]["__sheet_name"], "import")

    def test_csv_empty_file_returns_empty_list(self):
        rows = read_tabular_rows("export.csv", b"")
        self.assertEqual(rows, [])

    def test_csv_headers_only_returns_empty_list(self):
        rows = read_tabular_rows("export.csv", b"date,kind,amount\n")
        self.assertEqual(rows, [])

    def test_unsupported_extension_raises(self):
        with self.assertRaises(ValueError):
            read_tabular_rows("export.txt", b"some data")


class GoogleSheetExportFetchTests(unittest.TestCase):
    def test_follows_safe_google_redirect_before_streaming_export(self):
        class FakeResponse:
            def __init__(self, status_code=200, headers=None, chunks=None):
                self.status_code = status_code
                self.headers = headers or {}
                self._chunks = chunks or [b"date,amount\n", b"2026-05-15,50\n"]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def raise_for_status(self):
                return None

            async def aiter_bytes(self):
                for chunk in self._chunks:
                    yield chunk

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                self.streamed_urls = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def stream(self, method, url):
                self.streamed_urls.append(url)
                if len(self.streamed_urls) == 1:
                    return FakeResponse(
                        status_code=302,
                        headers={"location": "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx&gid=0"},
                    )
                return FakeResponse()

        export_url = "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx"
        client = FakeAsyncClient()
        with patch("app.discord.bookkeeping.httpx.AsyncClient", return_value=client):
            content = asyncio.run(fetch_google_sheet_export(export_url))

        self.assertEqual(content, b"date,amount\n2026-05-15,50\n")
        self.assertEqual(len(client.streamed_urls), 2)

    def test_rejects_google_sheet_export_redirect_to_untrusted_host(self):
        class FakeResponse:
            status_code = 302
            headers = {"location": "https://evil.example/export.xlsx"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

        class FakeAsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def stream(self, method, url):
                return FakeResponse()

        export_url = "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx"
        with patch("app.discord.bookkeeping.httpx.AsyncClient", return_value=FakeAsyncClient()):
            with self.assertRaises(ValueError):
                asyncio.run(fetch_google_sheet_export(export_url))


class ReconcileBookkeepingTests(unittest.TestCase):
    _tx_counter = 0

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed_import(self, session, entries):
        bk_import = BookkeepingImport(
            show_label="Test Import",
            row_count=len(entries),
        )
        session.add(bk_import)
        session.commit()
        session.refresh(bk_import)

        for i, entry_data in enumerate(entries):
            entry = BookkeepingEntry(
                import_id=bk_import.id,
                row_index=i,
                **entry_data,
            )
            session.add(entry)
        session.commit()
        return bk_import.id

    def _seed_transaction(self, session, money_in=None, money_out=None, occurred_at=None, entry_kind="sale"):
        ReconcileBookkeepingTests._tx_counter += 1
        uid = ReconcileBookkeepingTests._tx_counter
        dm = DiscordMessage(
            discord_message_id=f"disc-bk-{uid}",
            channel_id="999",
            channel_name="deals",
            author_id="777",
            author_name="Trader#0001",
            content="$50 sale",
            attachment_urls_json="[]",
            parse_status=PARSE_PARSED,
            created_at=_utcnow(),
        )
        session.add(dm)
        session.commit()
        session.refresh(dm)

        tx = Transaction(
            source_message_id=dm.id,
            discord_message_id=f"disc-bk-{uid}",
            occurred_at=occurred_at or _dt(2024, 1, 15),
            parse_status=PARSE_PARSED,
            entry_kind=entry_kind,
            money_in=money_in,
            money_out=money_out,
            amount=money_in or money_out,
            is_deleted=False,
        )
        session.add(tx)
        session.commit()
        session.refresh(tx)
        return tx.id

    def test_matches_entry_to_transaction_by_amount(self):
        with Session(self.engine) as session:
            self._seed_transaction(session, money_in=50.0, occurred_at=_dt(2024, 1, 15))
            import_id = self._seed_import(session, [
                {"amount": 50.0, "occurred_at": _dt(2024, 1, 15), "entry_kind": "sale"},
            ])
            result = reconcile_bookkeeping_import(session, import_id)

        summary = result["summary"]
        self.assertGreater(summary["matched_exact"] + summary["matched_amount_only"], 0)

    def test_unmatched_when_no_transactions(self):
        with Session(self.engine) as session:
            import_id = self._seed_import(session, [
                {"amount": 75.0, "occurred_at": _dt(2024, 3, 10), "entry_kind": "sale"},
            ])
            result = reconcile_bookkeeping_import(session, import_id)

        summary = result["summary"]
        self.assertEqual(summary["matched_exact"], 0)
        self.assertEqual(summary["matched_amount_only"], 0)
        self.assertEqual(summary["unmatched_rows"], 1)

    def test_no_double_match(self):
        with Session(self.engine) as session:
            # One transaction, two entries with the same amount
            self._seed_transaction(session, money_in=100.0, occurred_at=_dt(2024, 2, 1))
            import_id = self._seed_import(session, [
                {"amount": 100.0, "occurred_at": _dt(2024, 2, 1), "entry_kind": "sale"},
                {"amount": 100.0, "occurred_at": _dt(2024, 2, 1), "entry_kind": "sale"},
            ])
            result = reconcile_bookkeeping_import(session, import_id)

        summary = result["summary"]
        # Only one can match; the other must be unmatched
        self.assertEqual(summary["import_rows"], 2)
        self.assertLessEqual(summary["matched_rows"], 1)

    def test_raises_for_missing_import(self):
        with Session(self.engine) as session:
            with self.assertRaises(ValueError):
                reconcile_bookkeeping_import(session, 99999)


if __name__ == "__main__":
    unittest.main()
