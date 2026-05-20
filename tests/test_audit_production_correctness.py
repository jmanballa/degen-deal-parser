"""Regression tests for production correctness audit fixes (Area 1)."""
from __future__ import annotations

import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from app.models import TikTokOrder, utcnow
from app.reporting import (
    build_tiktok_buyer_insights,
    build_tiktok_product_performance,
    build_tiktok_reporting_summary,
    classify_tiktok_reporting_status,
    external_order_net_revenue,
)
from app.tiktok.tiktok_ingest import upsert_tiktok_order_from_payload


def _make_engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class TikTokStatusDominanceTests(unittest.TestCase):
    def test_classify_cancelled_dominates_paid_in_order_status(self) -> None:
        row = TikTokOrder(
            tiktok_order_id="tt-1",
            order_number="#1",
            created_at=utcnow(),
            updated_at=utcnow(),
            financial_status="paid",
            order_status="cancelled",
        )
        self.assertEqual(classify_tiktok_reporting_status(row), "refunded")

    def test_classify_refunded_dominates_completed(self) -> None:
        row = TikTokOrder(
            tiktok_order_id="tt-1",
            order_number="#1",
            created_at=utcnow(),
            updated_at=utcnow(),
            financial_status="refunded",
            order_status="completed",
        )
        self.assertEqual(classify_tiktok_reporting_status(row), "refunded")

    def test_classify_canceled_alias_dominates_paid(self) -> None:
        row = TikTokOrder(
            tiktok_order_id="tt-1",
            order_number="#1",
            created_at=utcnow(),
            updated_at=utcnow(),
            financial_status="paid",
            order_status="CANCELED",
        )
        self.assertEqual(classify_tiktok_reporting_status(row), "refunded")


class TikTokBuyerProductReportingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = _make_engine()

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_buyer_insights_excludes_paid_but_cancelled_orders(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-real",
                    order_number="#1",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    customer_name="Alice",
                    subtotal_price=20.0,
                    total_price=22.0,
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-refunded",
                    order_number="#2",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    order_status="cancelled",
                    customer_name="Alice",
                    subtotal_price=500.0,
                    total_price=510.0,
                )
            )
            session.commit()

            buyers = build_tiktok_buyer_insights(session, days=30)

        self.assertEqual(len(buyers), 1)
        self.assertEqual(buyers[0]["name"], "Alice")
        self.assertEqual(buyers[0]["total_spent"], 20.0)
        self.assertEqual(buyers[0]["order_count"], 1)

    def test_product_performance_excludes_cancelled_orders(self) -> None:
        line_items = json.dumps(
            [{"product_name": "Charizard", "quantity": 1, "sale_price": 100.0}]
        )
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-real",
                    order_number="#1",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    subtotal_price=100.0,
                    total_price=100.0,
                    line_items_json=line_items,
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-cancelled",
                    order_number="#2",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    order_status="cancelled",
                    subtotal_price=100.0,
                    total_price=100.0,
                    line_items_json=line_items,
                )
            )
            session.commit()

            products = build_tiktok_product_performance(session, days=30)

        self.assertEqual(len(products), 1)
        self.assertEqual(products[0]["title"], "Charizard")
        self.assertEqual(products[0]["qty"], 1)
        self.assertEqual(products[0]["revenue"], 100.0)

    def test_reporting_summary_paid_but_cancelled_counts_as_refunded(self) -> None:
        with Session(self.engine) as session:
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-cancel",
                    order_number="#1",
                    created_at=utcnow(),
                    updated_at=utcnow(),
                    financial_status="paid",
                    order_status="cancelled",
                    total_price=50.0,
                    subtotal_ex_tax=45.0,
                    total_tax=5.0,
                )
            )
            session.commit()
            rows = list(session.exec(select(TikTokOrder)).all())

        summary = build_tiktok_reporting_summary(rows)
        self.assertEqual(summary["status_counts"]["refunded"], 1)
        self.assertEqual(summary["status_counts"]["paid"], 0)
        self.assertEqual(summary["paid_orders"], 0)
        self.assertEqual(summary["gross_revenue"], 0.0)


class TikTokThinPayloadEnrichmentTests(unittest.TestCase):
    """Thin webhooks/list payloads must not blank enriched fields on existing rows."""

    def setUp(self) -> None:
        self.engine = _make_engine()

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_thin_status_only_webhook_preserves_enriched_fields(self) -> None:
        from scripts.tiktok_backfill import upsert_tiktok_order

        enriched_payload = {
            "order_id": "tt-100",
            "create_time": int(utcnow().timestamp()),
            "update_time": int(utcnow().timestamp()),
            "buyer_nickname": "Alice",
            "payment_status": "paid",
            "total_amount": "55.00",
            "tax_amount": "5.00",
            "subtotal_price": "50.00",
            "line_items": [
                {"product_name": "Pikachu", "quantity": 1, "sale_price": 50.0},
            ],
        }
        thin_payload = {
            "order_id": "tt-100",
            "update_time": int(utcnow().timestamp()),
            "order_status": "AWAITING_SHIPMENT",
        }

        with Session(self.engine) as session:
            upsert_tiktok_order(session, enriched_payload, shop_id="shop-1", shop_cipher="", source="backfill")
            session.commit()
            upsert_tiktok_order(session, thin_payload, shop_id="shop-1", shop_cipher="", source="webhook")
            session.commit()
            stmt = select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "tt-100")
            row = session.exec(stmt).first()

        self.assertIsNotNone(row)
        # Status fields reflect the latest payload.
        self.assertEqual(row.financial_status, "paid")
        self.assertEqual((row.order_status or "").lower(), "awaiting_shipment")
        # Enriched fields must survive.
        self.assertEqual(row.customer_name, "Alice")
        self.assertEqual(row.total_price, 55.0)
        self.assertEqual(row.subtotal_price, 50.0)
        self.assertEqual(row.total_tax, 5.0)
        self.assertEqual(row.shop_id, "shop-1")
        self.assertEqual(row.source, "webhook")
        items = json.loads(row.line_items_json or "[]")
        self.assertTrue(items, "thin webhook should not blank line items")
        self.assertEqual(items[0]["product_name"], "Pikachu")

    def test_app_webhook_upsert_preserves_existing_enriched_fields(self) -> None:
        enriched_payload = {
            "order_id": "tt-app-webhook-100",
            "create_time": int(utcnow().timestamp()),
            "update_time": int(utcnow().timestamp()),
            "buyer_nickname": "Alice",
            "shop_id": "shop-1",
            "payment_status": "paid",
            "total_amount": "55.00",
            "tax_amount": "5.00",
            "subtotal_price": "50.00",
            "line_items": [
                {"product_name": "Pikachu", "quantity": 1, "sale_price": 50.0},
            ],
        }
        thin_payload = {
            "data": {
                "order_id": "tt-app-webhook-100",
                "update_time": int(utcnow().timestamp()),
                "order_status": "AWAITING_SHIPMENT",
            }
        }

        with Session(self.engine) as session:
            upsert_tiktok_order_from_payload(session, TikTokOrder, enriched_payload, source="sync")
            session.commit()
            upsert_tiktok_order_from_payload(session, TikTokOrder, thin_payload, source="webhook")
            session.commit()
            row = session.exec(
                select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "tt-app-webhook-100")
            ).first()

        self.assertIsNotNone(row)
        self.assertEqual(row.financial_status, "paid")
        self.assertEqual((row.order_status or "").lower(), "awaiting_shipment")
        self.assertEqual(row.customer_name, "Alice")
        self.assertEqual(row.total_price, 55.0)
        self.assertEqual(row.subtotal_price, 50.0)
        self.assertEqual(row.total_tax, 5.0)
        self.assertEqual(row.shop_id, "shop-1")
        self.assertEqual(row.source, "webhook")
        items = json.loads(row.line_items_json or "[]")
        self.assertTrue(items, "thin webhook should not blank line items")
        self.assertEqual(items[0]["product_name"], "Pikachu")

    def test_zero_price_payload_can_correct_existing_nonzero_price(self) -> None:
        from scripts.tiktok_backfill import upsert_tiktok_order

        base_payload = {
            "order_id": "tt-zero-correction",
            "create_time": int(utcnow().timestamp()),
            "update_time": int(utcnow().timestamp()),
            "payment_status": "paid",
            "total_amount": "55.00",
            "subtotal_price": "50.00",
        }
        zero_payload = {
            "order_id": "tt-zero-correction",
            "create_time": int(utcnow().timestamp()),
            "update_time": int(utcnow().timestamp()),
            "payment_status": "paid",
            "total_amount": "0.00",
            "subtotal_price": "0.00",
        }

        with Session(self.engine) as session:
            upsert_tiktok_order(session, base_payload, shop_id="shop-1", shop_cipher="", source="backfill")
            session.commit()
            upsert_tiktok_order(session, zero_payload, shop_id="shop-1", shop_cipher="", source="backfill")
            session.commit()
            row = session.exec(
                select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "tt-zero-correction")
            ).first()

        self.assertIsNotNone(row)
        self.assertEqual(row.total_price, 0.0)
        self.assertEqual(row.subtotal_price, 0.0)

    def test_zero_taxed_payload_clears_existing_net_revenue(self) -> None:
        base_payload = {
            "order_id": "tt-zero-taxed-correction",
            "create_time": int(utcnow().timestamp()),
            "update_time": int(utcnow().timestamp()),
            "payment_status": "paid",
            "total_price": "55.00",
            "tax_amount": "5.00",
            "subtotal_price": "50.00",
        }
        zero_payload = {
            "order_id": "tt-zero-taxed-correction",
            "create_time": int(utcnow().timestamp()),
            "update_time": int(utcnow().timestamp()),
            "payment_status": "paid",
            "total_price": "0.00",
            "tax_amount": "0.00",
            "subtotal_price": "0.00",
        }

        with Session(self.engine) as session:
            upsert_tiktok_order_from_payload(session, TikTokOrder, base_payload, source="sync")
            session.commit()
            upsert_tiktok_order_from_payload(session, TikTokOrder, zero_payload, source="sync")
            session.commit()
            row = session.exec(
                select(TikTokOrder).where(TikTokOrder.tiktok_order_id == "tt-zero-taxed-correction")
            ).first()

        assert row is not None
        self.assertEqual(row.total_price, 0.0)
        self.assertEqual(row.subtotal_price, 0.0)
        self.assertEqual(row.total_tax, 0.0)
        self.assertEqual(row.subtotal_ex_tax, 0.0)
        self.assertEqual(external_order_net_revenue(row), 0.0)


class TikTokPullErrorRecordingTests(unittest.TestCase):
    """Pull cycle exceptions must surface the actual failure reason."""

    def setUp(self) -> None:
        import app.shared as shared_module

        self.engine = _make_engine()
        self._shared = shared_module
        with shared_module._tiktok_state_lock:
            shared_module._tiktok_state["last_error"] = None

    def tearDown(self) -> None:
        with self._shared._tiktok_state_lock:
            self._shared._tiktok_state["last_error"] = None
        self.engine.dispose()

    def test_pull_records_actual_exception_message(self) -> None:
        from contextlib import contextmanager
        from unittest.mock import patch

        import app.shared as shared_module

        def boom(*args, **kwargs):
            raise RuntimeError("network exploded for shop-1 Bearer tok-super-secret")

        @contextmanager
        def managed():
            with Session(self.engine) as session:
                yield session

        with patch.object(shared_module, "managed_session", managed), patch.object(
            shared_module, "pull_tiktok_orders", side_effect=boom
        ), patch.object(
            shared_module.settings, "tiktok_app_key", "key", create=True
        ), patch.object(
            shared_module.settings, "tiktok_app_secret", "secret", create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_enabled", True, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_lookback_hours", 1.0, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_limit", 50, create=True
        ), patch.object(
            shared_module.settings, "tiktok_startup_backfill_days", 1, create=True
        ), patch.object(
            shared_module, "ensure_tiktok_auth_row", return_value=object()
        ), patch.object(
            shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            shared_module,
            "_resolve_tiktok_pull_credentials",
            return_value=("shop-1", "cipher-1", "tok"),
        ), patch.object(
            shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://example/"
        ):
            with self.assertRaises(RuntimeError):
                shared_module.run_tiktok_pull_cycle(runtime_name="test", trigger="manual")

        state = shared_module.read_tiktok_integration_state()
        last_error_text = str(state.get("last_error") or "")
        self.assertIn("network exploded", last_error_text)
        self.assertNotIn("tok-super-secret", last_error_text)
        self.assertIn("[REDACTED]", last_error_text)

    def test_successful_pull_state_does_not_persist_or_expose_shop_cipher(self) -> None:
        from contextlib import contextmanager
        from unittest.mock import patch

        import app.shared as shared_module
        from app.models import TikTokSyncState

        class _Summary:
            fetched = 1
            inserted = 1
            updated = 0
            failed = 0
            detail_calls = 1
            auth_updated = 0

        @contextmanager
        def managed():
            with Session(self.engine) as session:
                yield session

        with patch.object(shared_module, "managed_session", managed), patch.object(
            shared_module, "pull_tiktok_orders", return_value=_Summary()
        ), patch.object(
            shared_module.settings, "tiktok_app_key", "key", create=True
        ), patch.object(
            shared_module.settings, "tiktok_app_secret", "secret", create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_enabled", True, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_lookback_hours", 1.0, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_limit", 50, create=True
        ), patch.object(
            shared_module.settings, "tiktok_startup_backfill_days", 1, create=True
        ), patch.object(
            shared_module, "ensure_tiktok_auth_row", return_value=object()
        ), patch.object(
            shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            shared_module,
            "_resolve_tiktok_pull_credentials",
            return_value=("", "cipher-state-secret", "tok"),
        ), patch.object(
            shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://example/"
        ):
            result = shared_module.run_tiktok_pull_cycle(runtime_name="test", trigger="manual")

        state = shared_module.read_tiktok_integration_state()
        state_text = json.dumps(state, default=str)
        result_text = json.dumps(result, default=str)
        with Session(self.engine) as session:
            persisted = session.get(TikTokSyncState, 1)
            persisted_text = (persisted.last_pull_json if persisted else "") or ""
            snapshot = shared_module.describe_tiktok_sync_status(None, state)
        snapshot_text = json.dumps(snapshot, default=str)
        self.assertNotIn("cipher-state-secret", state_text)
        self.assertNotIn("cipher-state-secret", persisted_text)
        self.assertNotIn("cipher-state-secret", snapshot_text)
        self.assertNotIn("shop_cipher", state_text)
        self.assertNotIn("shop_cipher", persisted_text)
        self.assertNotIn("cipher-state-secret", result_text)

    def test_manual_background_pull_outer_handler_preserves_redacted_failure(self) -> None:
        from unittest.mock import patch

        import app.shared as shared_module

        def boom(*args, **kwargs):
            raise RuntimeError("manual failed access_token=tok-manual-secret")

        with patch.object(
            shared_module, "run_tiktok_pull_cycle", side_effect=boom
        ), patch.object(shared_module, "print") as fake_print:
            shared_module.run_tiktok_pull_in_background(since=None, limit=10, trigger="manual")

        state = shared_module.read_tiktok_integration_state()
        last_error_text = str(state.get("last_error") or "")
        last_pull_text = json.dumps(state.get("last_pull") or {})
        printed_text = "\n".join(str(call) for call in fake_print.call_args_list)
        self.assertIn("manual failed", last_error_text)
        self.assertNotIn("tok-manual-secret", last_error_text)
        self.assertNotIn("tok-manual-secret", last_pull_text)
        self.assertNotIn("tok-manual-secret", printed_text)
        self.assertIn("[REDACTED]", last_error_text)

    def test_periodic_pull_outer_handler_preserves_redacted_failure(self) -> None:
        from contextlib import contextmanager
        from unittest.mock import patch

        import app.shared as shared_module

        def boom(*args, **kwargs):
            raise RuntimeError("periodic failed access_token=tok-periodic-secret")

        @contextmanager
        def managed():
            with Session(self.engine) as session:
                yield session

        async def run_once() -> None:
            stop_event = asyncio.Event()

            async def fake_wait():
                stop_event.set()
                return None

            stop_event.wait = fake_wait
            await shared_module.periodic_tiktok_pull_loop(stop_event)

        with patch.object(shared_module, "managed_session", managed), patch.object(
            shared_module, "pull_tiktok_orders", side_effect=boom
        ), patch.object(
            shared_module.settings, "tiktok_app_key", "key", create=True
        ), patch.object(
            shared_module.settings, "tiktok_app_secret", "secret", create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_enabled", True, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_lookback_hours", 1.0, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_limit", 50, create=True
        ), patch.object(
            shared_module, "ensure_tiktok_auth_row", return_value=object()
        ), patch.object(
            shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            shared_module,
            "_resolve_tiktok_pull_credentials",
            return_value=("shop-1", "cipher-1", "tok"),
        ), patch.object(
            shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://example/"
        ):
            asyncio.run(run_once())

        state = shared_module.read_tiktok_integration_state()
        last_error_text = str(state.get("last_error") or "")
        last_pull_text = json.dumps(state.get("last_pull") or {})
        self.assertIn("periodic failed", last_error_text)
        self.assertNotIn("tok-periodic-secret", last_error_text)
        self.assertNotIn("tok-periodic-secret", last_pull_text)
        self.assertIn("[REDACTED]", last_error_text)


class TikTokPaginationTests(unittest.TestCase):
    """Pagination supports update-time scans and exhaustive cursors; limit None means no cap."""

    def setUp(self) -> None:
        self.engine = _make_engine()

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_fetch_order_list_uses_documented_create_time_filters(self) -> None:
        from scripts.tiktok_backfill import fetch_tiktok_order_list_page

        captured = {}

        class FakeClient:
            def request(self, method, url, **kwargs):
                captured["body"] = kwargs.get("content")
                return _FakeResponse({"code": 0, "message": "OK", "data": {"orders": []}})

            def post(self, url, content=None, headers=None):
                captured["body"] = content
                return _FakeResponse({"code": 0, "message": "OK", "data": {"orders": []}})

            def get(self, url, headers=None):
                return _FakeResponse({"code": 0, "message": "OK", "data": {"orders": []}})

        since = datetime(2026, 4, 1, tzinfo=timezone.utc)
        until = datetime(2026, 4, 10, tzinfo=timezone.utc)

        fetch_tiktok_order_list_page(
            FakeClient(),
            base_url="https://example/",
            app_key="k",
            app_secret="s",
            access_token="t",
            shop_id="shop-1",
            shop_cipher="",
            since=since,
            until=until,
            page_size=50,
        )
        body = json.loads(captured["body"])
        self.assertIn("create_time_ge", body)
        self.assertIn("create_time_lt", body)
        self.assertNotIn("update_time_ge", body)
        self.assertNotIn("update_time_lt", body)

    def test_backfill_continues_through_empty_page_with_cursor(self) -> None:
        from unittest.mock import patch

        import scripts.tiktok_backfill as backfill_module
        from scripts.tiktok_backfill import backfill_tiktok_orders

        # Three pages: page1 has 1 order + cursor p2, page2 empty + cursor p3, page3 has 1 order no cursor.
        now_ts = int(utcnow().timestamp())
        pages = [
            (
                {"data": {"orders": [], "next_page_token": "p2"}},
                [{"order_id": "tt-a", "create_time": now_ts, "payment_status": "paid"}],
            ),
            (
                {"data": {"orders": [], "next_page_token": "p3"}},
                [],
            ),
            (
                {"data": {"orders": []}},
                [{"order_id": "tt-b", "create_time": now_ts, "payment_status": "paid"}],
            ),
        ]

        call_log = []

        def fake_fetch_page(client, **kwargs):
            call_log.append(kwargs.get("cursor"))
            return pages.pop(0)

        with Session(self.engine) as session:
            with patch.object(backfill_module, "fetch_tiktok_order_list_page", side_effect=fake_fetch_page), patch.object(
                backfill_module, "httpx", _FakeHttpx()
            ):
                summary = backfill_tiktok_orders(
                    session,
                    base_url="https://example/",
                    app_key="k",
                    app_secret="s",
                    access_token="t",
                    shop_id="shop-1",
                    shop_cipher="",
                    since=utcnow() - timedelta(days=1),
                    limit=None,
                )

        # Should fetch both real orders despite the empty middle page.
        self.assertEqual(summary.fetched, 2)
        self.assertEqual(summary.inserted, 2)
        self.assertEqual(call_log, [None, "p2", "p3"])

    def test_backfill_stops_on_repeated_empty_cursor(self) -> None:
        from unittest.mock import patch

        import scripts.tiktok_backfill as backfill_module
        from scripts.tiktok_backfill import backfill_tiktok_orders

        call_log = []

        def fake_fetch_page(client, **kwargs):
            call_log.append(kwargs.get("cursor"))
            return {"data": {"orders": [], "next_page_token": "p1"}}, []

        with Session(self.engine) as session:
            with patch.object(backfill_module, "fetch_tiktok_order_list_page", side_effect=fake_fetch_page), patch.object(
                backfill_module, "httpx", _FakeHttpx()
            ):
                summary = backfill_tiktok_orders(
                    session,
                    base_url="https://example/",
                    app_key="k",
                    app_secret="s",
                    access_token="t",
                    shop_id="shop-1",
                    shop_cipher="",
                    since=utcnow() - timedelta(days=1),
                    limit=None,
                )

        self.assertEqual(summary.fetched, 0)
        self.assertEqual(call_log, [None, "p1"])

    def test_pull_cycle_does_not_clamp_zero_limit_to_one(self) -> None:
        from contextlib import contextmanager
        from unittest.mock import patch

        import app.shared as shared_module

        captured = {}

        def fake_pull(*args, **kwargs):
            captured["limit"] = kwargs.get("limit")

            class _Summary:
                fetched = 0
                inserted = 0
                updated = 0
                failed = 0
                detail_calls = 0
                auth_updated = 0

            return _Summary()

        @contextmanager
        def managed():
            with Session(self.engine) as session:
                yield session

        with patch.object(shared_module, "managed_session", managed), patch.object(
            shared_module, "pull_tiktok_orders", side_effect=fake_pull
        ), patch.object(
            shared_module.settings, "tiktok_app_key", "key", create=True
        ), patch.object(
            shared_module.settings, "tiktok_app_secret", "secret", create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_enabled", True, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_lookback_hours", 1.0, create=True
        ), patch.object(
            shared_module.settings, "tiktok_sync_limit", 0, create=True
        ), patch.object(
            shared_module.settings, "tiktok_startup_backfill_days", 1, create=True
        ), patch.object(
            shared_module, "ensure_tiktok_auth_row", return_value=object()
        ), patch.object(
            shared_module, "_refresh_tiktok_auth_if_needed", return_value=None
        ), patch.object(
            shared_module,
            "_resolve_tiktok_pull_credentials",
            return_value=("shop-1", "cipher-1", "tok"),
        ), patch.object(
            shared_module, "resolve_tiktok_shop_pull_base_url", return_value="https://example/"
        ):
            shared_module.run_tiktok_pull_cycle(
                runtime_name="test", trigger="manual", limit=0,
            )

        # 0 (and None) must mean "no cap", not clamped to 1.
        self.assertIn(captured["limit"], (None, 0))


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload

    @property
    def text(self):
        return json.dumps(self._payload)

    @property
    def headers(self):
        return {}

    @property
    def status_code(self):
        return 200

    @property
    def is_success(self):
        return True


class _FakeHttpx:
    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    HTTPStatusError = Exception


class ConfigSecretFailClosedTests(unittest.TestCase):
    """Area A: app/config.py auth fail-closed hardening."""

    def _build_settings(self, **overrides):
        from app import config as config_module

        return config_module.Settings(**overrides)

    def test_placeholder_session_secret_rejected(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="changeme",
            ADMIN_PASSWORD="x" * 32,
        )
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        self.assertIn("SESSION_SECRET", str(exc.exception))

    def test_placeholder_admin_password_rejected(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="a" * 64,
            ADMIN_PASSWORD="password",
        )
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        self.assertIn("ADMIN_PASSWORD", str(exc.exception))

    def test_placeholder_replace_me_rejected(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="REPLACE_ME",
            ADMIN_PASSWORD="x" * 32,
        )
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        self.assertIn("SESSION_SECRET", str(exc.exception))

    def test_short_session_secret_rejected(self) -> None:
        # Distinct from any known placeholder, just too short.
        settings = self._build_settings(
            SESSION_SECRET="abc123",
            ADMIN_PASSWORD="x" * 32,
        )
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        self.assertIn("SESSION_SECRET", str(exc.exception))

    def test_short_admin_password_rejected(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="a" * 64,
            ADMIN_PASSWORD="short",
        )
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        self.assertIn("ADMIN_PASSWORD", str(exc.exception))

    def test_nine_character_admin_password_rejected(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="a" * 64,
            ADMIN_PASSWORD="safe9char",
        )
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        self.assertIn("ADMIN_PASSWORD", str(exc.exception))

    def test_error_does_not_leak_secret_values(self) -> None:
        leakable_secret = "supersecret-value-XYZ-12345"
        leakable_password = "leaky-password-ABCDEF"
        settings = self._build_settings(
            SESSION_SECRET=leakable_secret,  # below min length so rejected
            ADMIN_PASSWORD=leakable_password,  # below min length so rejected
        )
        # Force both to be rejected (short or placeholder) regardless of length.
        # Use too-short variants to guarantee rejection without using known placeholders.
        settings.session_secret = "tiny"
        settings.admin_password = "tiny"
        with self.assertRaises(RuntimeError) as exc:
            settings.validate_runtime_secrets()
        text = str(exc.exception)
        self.assertNotIn("tiny", text)
        # Field names should be listed; values must not appear.
        self.assertIn("SESSION_SECRET", text)
        self.assertIn("ADMIN_PASSWORD", text)

    def test_compatible_existing_admin_password_length_validates(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="a" * 64,
            ADMIN_PASSWORD="safe10char",
            EMPLOYEE_PORTAL_ENABLED="false",
        )
        # Must not raise for existing production-compatible credentials.
        settings.validate_runtime_secrets()

    def test_strong_secrets_validate(self) -> None:
        settings = self._build_settings(
            SESSION_SECRET="a" * 64,
            ADMIN_PASSWORD="x" * 32,
            EMPLOYEE_PORTAL_ENABLED="false",
        )
        # Must not raise.
        settings.validate_runtime_secrets()


class StitchedChildTransactionCleanupTests(unittest.TestCase):
    """Area D: a row that becomes a stitched child must lose its Transaction
    and any BookkeepingEntry references."""

    def setUp(self) -> None:
        self.engine = _make_engine()

    def tearDown(self) -> None:
        self.engine.dispose()

    def _seed_parsed_row(self, session, *, discord_message_id):
        from app.models import DiscordMessage, PARSE_PARSED

        row = DiscordMessage(
            discord_message_id=discord_message_id,
            channel_id="chan-1",
            channel_name="general",
            author_name="alice",
            content="bought a slab for 50",
            created_at=utcnow(),
            parse_status=PARSE_PARSED,
            deal_type="buy",
            entry_kind="buy",
            amount=50.0,
            money_in=0.0,
            money_out=50.0,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return row.id

    def _fetch_transaction(self, session, row_id):
        from app.models import Transaction

        statement = select(Transaction).where(Transaction.source_message_id == row_id)
        return session.scalars(statement).first()

    def test_marking_row_as_stitched_child_deletes_existing_transaction(self):
        from app.models import DiscordMessage, TransactionItem
        from app.discord.transactions import sync_transaction_from_message

        with Session(self.engine) as session:
            row_id = self._seed_parsed_row(session, discord_message_id="d-1")
            row = session.get(DiscordMessage, row_id)
            sync_transaction_from_message(session, row)
            session.commit()
            tx = self._fetch_transaction(session, row_id)
            self.assertIsNotNone(tx)
            session.add(TransactionItem(transaction_id=tx.id, direction="named", item_name="Charizard"))
            session.commit()

            row.stitched_group_id = "grp-1"
            row.stitched_primary = False
            session.add(row)
            session.commit()
            sync_transaction_from_message(session, row)
            session.commit()

            tx_after = self._fetch_transaction(session, row_id)
            items_after = list(
                session.scalars(
                    select(TransactionItem).where(TransactionItem.transaction_id == (tx.id or 0))
                ).all()
            )
        self.assertIsNone(tx_after)
        self.assertEqual(items_after, [])

    def test_stitched_child_clears_bookkeeping_entry_match(self):
        from app.models import (
            BookkeepingEntry,
            BookkeepingImport,
            DiscordMessage,
        )
        from app.discord.transactions import sync_transaction_from_message

        with Session(self.engine) as session:
            row_id = self._seed_parsed_row(session, discord_message_id="d-2")
            row = session.get(DiscordMessage, row_id)
            sync_transaction_from_message(session, row)
            session.commit()
            tx = self._fetch_transaction(session, row_id)
            self.assertIsNotNone(tx)

            import_row = BookkeepingImport(show_label="ledger")
            session.add(import_row)
            session.commit()
            session.refresh(import_row)
            entry = BookkeepingEntry(
                import_id=import_row.id,
                row_index=1,
                matched_transaction_id=tx.id,
                match_status="matched_strong",
            )
            session.add(entry)
            session.commit()
            session.refresh(entry)

            row.stitched_group_id = "grp-2"
            row.stitched_primary = False
            session.add(row)
            session.commit()
            sync_transaction_from_message(session, row)
            session.commit()

            entry_after = session.get(BookkeepingEntry, entry.id)
            tx_after = self._fetch_transaction(session, row_id)
        self.assertIsNone(tx_after)
        self.assertIsNotNone(entry_after)
        self.assertIsNone(entry_after.matched_transaction_id)
        self.assertEqual(entry_after.match_status, "unmatched")

    def test_marking_row_as_ignored_also_deletes_transaction(self):
        from app.models import DiscordMessage, PARSE_IGNORED
        from app.discord.transactions import sync_transaction_from_message

        with Session(self.engine) as session:
            row_id = self._seed_parsed_row(session, discord_message_id="d-3")
            row = session.get(DiscordMessage, row_id)
            sync_transaction_from_message(session, row)
            session.commit()
            self.assertIsNotNone(self._fetch_transaction(session, row_id))

            row.parse_status = PARSE_IGNORED
            session.add(row)
            session.commit()
            sync_transaction_from_message(session, row)
            session.commit()
            self.assertIsNone(self._fetch_transaction(session, row_id))


if __name__ == "__main__":
    unittest.main()
