import shutil
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.models import InventoryItem, ShopifyOrder, TikTokOrder
from app.pack_station import (
    PACK_SCAN_DUPLICATE,
    PACK_SCAN_MATCHED,
    PACK_SCAN_OVERRIDE,
    PACK_SCAN_REOPENED,
    PACK_SCAN_UNEXPECTED,
    PACK_SCAN_UNLINKED_ORDER,
    build_pack_order_row,
    extract_expected_pack_items,
    load_pack_exception_queue,
    load_pack_queue,
    pack_queue_summary,
    record_pack_override,
    record_pack_reopen,
    record_pack_scan,
)
from app.routers.pack_station import pack_exception_queue_page, pack_station_page


def make_request(path: str = "/pack-station") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        id=1,
        username="packer",
        display_name="Pack Operator",
        role="reviewer",
    )
    return request


class PackStationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_pack_station" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "pack_station.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_extract_expected_pack_items_merges_dgn_skus(self) -> None:
        expected = extract_expected_pack_items(
            '[{"title":"Charizard","quantity":1,"sku":"dgn-000001"},{"title":"Charizard","quantity":2,"sku":"DGN-000001"},{"title":"Pikachu","quantity":1,"sku":"OTHER"}]',
            "[]",
        )

        self.assertEqual(expected, [{"barcode": "DGN-000001", "title": "Charizard", "quantity": 3, "unit_price": None}])

    def test_record_pack_scan_matches_expected_barcode_and_marks_duplicate(self) -> None:
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            session.add(
                InventoryItem(
                    barcode="DGN-000001",
                    item_type="single",
                    game="Pokemon",
                    card_name="Charizard",
                    status="sold",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-1",
                    order_number="TT1001",
                    created_at=now,
                    updated_at=now,
                    customer_name="Ash",
                    total_price=25.0,
                    subtotal_price=25.0,
                    financial_status="paid",
                    fulfillment_status="unfulfilled",
                    order_status="awaiting_shipment",
                    line_items_summary_json='[{"title":"Charizard","quantity":1,"sku":"DGN-000001"}]',
                )
            )
            session.commit()

            first = record_pack_scan(session, source="tiktok", order_id="tt-1", barcode="dgn-000001")
            second = record_pack_scan(session, source="tiktok", order_id="tt-1", barcode="DGN-000001")
            row = build_pack_order_row(
                session.get(TikTokOrder, 1),
                source="tiktok",
                scans=[first, second],
            )

        self.assertEqual(first.status, PACK_SCAN_MATCHED)
        self.assertTrue(first.expected)
        self.assertEqual(second.status, PACK_SCAN_DUPLICATE)
        self.assertEqual(row["pack_status"], "exception")
        self.assertEqual(row["matched_count"], 1)

    def test_unlinked_order_scan_is_visible_but_not_verified(self) -> None:
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            session.add(
                InventoryItem(
                    barcode="DGN-000002",
                    item_type="single",
                    game="Pokemon",
                    card_name="Pikachu",
                    status="sold",
                )
            )
            session.add(
                ShopifyOrder(
                    shopify_order_id="sh-1",
                    order_number="#1001",
                    created_at=now,
                    updated_at=now,
                    customer_name="Misty",
                    total_price=10.0,
                    subtotal_price=10.0,
                    financial_status="paid",
                    fulfillment_status="unfulfilled",
                    line_items_summary_json='[{"title":"Mystery Pull","quantity":1}]',
                )
            )
            session.commit()

            event = record_pack_scan(session, source="shopify", order_id="sh-1", barcode="DGN-000002")
            queue = load_pack_queue(session, source="shopify", days=1, limit=10)
            summary = pack_queue_summary(queue)

        self.assertEqual(event.status, PACK_SCAN_UNLINKED_ORDER)
        self.assertEqual(queue[0]["pack_status"], "needs_item_link")
        self.assertEqual(summary["needs_item_link"], 1)

    def test_pack_exception_override_and_reopen_are_audited(self) -> None:
        now = datetime.now(timezone.utc)
        user = SimpleNamespace(id=7, username="lead", display_name="Pack Lead")
        with Session(self.engine) as session:
            session.add(
                InventoryItem(
                    barcode="DGN-000001",
                    item_type="single",
                    game="Pokemon",
                    card_name="Charizard",
                    status="sold",
                )
            )
            session.add(
                InventoryItem(
                    barcode="DGN-000003",
                    item_type="single",
                    game="Pokemon",
                    card_name="Squirtle",
                    status="in_stock",
                )
            )
            session.add(
                TikTokOrder(
                    tiktok_order_id="tt-override",
                    order_number="TT2001",
                    created_at=now,
                    updated_at=now,
                    customer_name="Brock",
                    total_price=25.0,
                    subtotal_price=25.0,
                    financial_status="paid",
                    fulfillment_status="unfulfilled",
                    order_status="awaiting_shipment",
                    line_items_summary_json='[{"title":"Charizard","quantity":1,"sku":"DGN-000001"}]',
                )
            )
            session.commit()

            bad_scan = record_pack_scan(session, source="tiktok", order_id="tt-override", barcode="DGN-000003")
            blocked = load_pack_exception_queue(session, source="tiktok", status_filter="blocked", days=1, limit=10)
            override = record_pack_override(
                session,
                source="tiktok",
                order_id="tt-override",
                reason="Verified item against order photo",
                user=user,
            )
            overridden = load_pack_exception_queue(session, source="tiktok", status_filter="override", days=1, limit=10)
            reopened_event = record_pack_reopen(
                session,
                source="tiktok",
                order_id="tt-override",
                reason="Need a second check",
                user=user,
            )
            reopened = load_pack_exception_queue(session, source="tiktok", status_filter="blocked", days=1, limit=10)

        self.assertEqual(bad_scan.status, PACK_SCAN_UNEXPECTED)
        self.assertEqual(blocked[0]["pack_status"], "exception")
        self.assertEqual(blocked[0]["exception_reasons"][0]["status"], PACK_SCAN_UNEXPECTED)
        self.assertEqual(override.status, PACK_SCAN_OVERRIDE)
        self.assertEqual(overridden[0]["pack_status"], "override")
        self.assertEqual(overridden[0]["exception_reasons"][0]["notes"], "Verified item against order photo")
        self.assertEqual(reopened_event.status, PACK_SCAN_REOPENED)
        self.assertEqual(reopened[0]["pack_status"], "exception")

    def test_pack_station_page_renders_empty_queue(self) -> None:
        with Session(self.engine) as session, patch(
            "app.routers.pack_station.require_role_response",
            return_value=None,
        ):
            response = pack_station_page(
                make_request(),
                source="all",
                search="",
                days=30,
                limit=75,
                session=session,
            )

        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Pack Station", body)
        self.assertIn("No open paid orders matched", body)

    def test_pack_exception_queue_page_renders_needs_link_order(self) -> None:
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            session.add(
                ShopifyOrder(
                    shopify_order_id="sh-needs-link",
                    order_number="#2001",
                    created_at=now,
                    updated_at=now,
                    customer_name="Tracey",
                    total_price=15.0,
                    subtotal_price=15.0,
                    financial_status="paid",
                    fulfillment_status="unfulfilled",
                    line_items_summary_json='[{"title":"Mystery Pull","quantity":1,"sku":"SKU-1"}]',
                )
            )
            session.commit()
            with patch("app.routers.pack_station.require_role_response", return_value=None):
                response = pack_exception_queue_page(
                    make_request("/pack-station/exceptions"),
                    source="shopify",
                    status="blocked",
                    search="",
                    days=30,
                    limit=75,
                    session=session,
                )

        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Pack Exceptions", body)
        self.assertIn("#2001", body)
        self.assertIn("Needs item link", body)


if __name__ == "__main__":
    unittest.main()
