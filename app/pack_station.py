from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from sqlmodel import Session, select

from .models import (
    InventoryItem,
    PackScanEvent,
    ShopifyOrder,
    TikTokOrder,
    User,
    utcnow,
)
from .reporting import classify_tiktok_reporting_status

PACK_SOURCE_TIKTOK = "tiktok"
PACK_SOURCE_SHOPIFY = "shopify"
PACK_SOURCES = {PACK_SOURCE_TIKTOK, PACK_SOURCE_SHOPIFY}

PACK_SCAN_MATCHED = "matched"
PACK_SCAN_DUPLICATE = "duplicate"
PACK_SCAN_UNEXPECTED = "unexpected"
PACK_SCAN_UNKNOWN_BARCODE = "unknown_barcode"
PACK_SCAN_UNLINKED_ORDER = "unlinked_order"
PACK_SCAN_OVERRIDE = "override"
PACK_SCAN_REOPENED = "reopened"

PACK_EXCEPTION_STATUSES = {
    PACK_SCAN_DUPLICATE,
    PACK_SCAN_UNEXPECTED,
    PACK_SCAN_UNKNOWN_BARCODE,
    PACK_SCAN_UNLINKED_ORDER,
}
PACK_CONTROL_STATUSES = {PACK_SCAN_OVERRIDE, PACK_SCAN_REOPENED}
PACK_QUEUE_EXCEPTION_FILTERS = {"all", "blocked", "exception", "needs_item_link", "override"}

_BARCODE_RE = re.compile(r"^DGN-\d{6}$", re.IGNORECASE)


def normalize_pack_barcode(value: str | None) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip().upper()


def _safe_json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def _quantity_from_item(item: dict[str, Any]) -> int:
    try:
        quantity = int(item.get("quantity") or item.get("qty") or 0)
    except (TypeError, ValueError):
        quantity = 0
    return quantity if quantity > 0 else 1


def _line_items(summary_json: str | None, fallback_json: str | None) -> list[dict[str, Any]]:
    summary_items = _safe_json_list(summary_json)
    if summary_items:
        return summary_items
    return _safe_json_list(fallback_json)


def extract_expected_pack_items(summary_json: str | None, fallback_json: str | None) -> list[dict[str, Any]]:
    expected_by_barcode: dict[str, dict[str, Any]] = {}
    for item in _line_items(summary_json, fallback_json):
        sku = normalize_pack_barcode(
            item.get("sku") or item.get("seller_sku") or item.get("barcode") or ""
        )
        if not _BARCODE_RE.match(sku):
            continue
        existing = expected_by_barcode.setdefault(
            sku,
            {
                "barcode": sku,
                "title": str(item.get("title") or item.get("product_name") or item.get("sku_name") or sku).strip(),
                "quantity": 0,
                "unit_price": item.get("unit_price") or item.get("price"),
            },
        )
        existing["quantity"] = int(existing["quantity"] or 0) + _quantity_from_item(item)
    return list(expected_by_barcode.values())


def display_line_items(summary_json: str | None, fallback_json: str | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _line_items(summary_json, fallback_json):
        title = str(item.get("title") or item.get("product_name") or item.get("sku_name") or "").strip()
        sku = normalize_pack_barcode(item.get("sku") or item.get("seller_sku") or item.get("barcode") or "")
        if not title and not sku:
            continue
        rows.append(
            {
                "title": title or sku,
                "quantity": _quantity_from_item(item),
                "sku": sku,
                "unit_price": item.get("unit_price") or item.get("price"),
            }
        )
    return rows


def _expected_counter(expected_items: Iterable[dict[str, Any]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for item in expected_items:
        barcode = normalize_pack_barcode(item.get("barcode"))
        if not barcode:
            continue
        try:
            quantity = int(item.get("quantity") or 0)
        except (TypeError, ValueError):
            quantity = 0
        counter[barcode] += quantity if quantity > 0 else 1
    return counter


def _scan_counter(scans: Iterable[PackScanEvent], *, status: str = PACK_SCAN_MATCHED) -> Counter[str]:
    counter: Counter[str] = Counter()
    for scan in scans:
        if scan.status == status:
            counter[normalize_pack_barcode(scan.barcode)] += 1
    return counter


def _matched_count(expected: Counter[str], scans: Iterable[PackScanEvent]) -> int:
    matched = _scan_counter(scans)
    return sum(min(expected[barcode], matched[barcode]) for barcode in expected)


def _scan_sort_key(scan: PackScanEvent) -> tuple[datetime, int]:
    created = scan.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return created.astimezone(timezone.utc), int(scan.id or 0)


def _latest_scan_with_status(scans: Iterable[PackScanEvent], statuses: set[str]) -> PackScanEvent | None:
    matches = [scan for scan in scans if scan.status in statuses]
    if not matches:
        return None
    return max(matches, key=_scan_sort_key)


def _has_active_override(scans: list[PackScanEvent]) -> bool:
    latest_override = _latest_scan_with_status(scans, {PACK_SCAN_OVERRIDE})
    if latest_override is None:
        return False
    latest_reopen = _latest_scan_with_status(scans, {PACK_SCAN_REOPENED})
    latest_exception = _latest_scan_with_status(scans, PACK_EXCEPTION_STATUSES)
    override_key = _scan_sort_key(latest_override)
    if latest_reopen is not None and _scan_sort_key(latest_reopen) > override_key:
        return False
    if latest_exception is not None and _scan_sort_key(latest_exception) > override_key:
        return False
    return True


def pack_status_for(expected_items: list[dict[str, Any]], scans: list[PackScanEvent]) -> str:
    if _has_active_override(scans):
        return "override"
    expected = _expected_counter(expected_items)
    if not expected:
        return "needs_item_link"
    if any(scan.status in PACK_EXCEPTION_STATUSES for scan in scans):
        return "exception"
    if _matched_count(expected, scans) >= sum(expected.values()):
        return "verified"
    if scans:
        return "in_progress"
    return "ready_to_scan"


def _order_identity(order: TikTokOrder | ShopifyOrder, source: str) -> tuple[str, str]:
    if source == PACK_SOURCE_TIKTOK:
        return order.tiktok_order_id, order.order_number
    return order.shopify_order_id, order.order_number


def _order_total(order: TikTokOrder | ShopifyOrder) -> float:
    value = getattr(order, "subtotal_ex_tax", None)
    if value is not None:
        return round(float(value or 0.0), 2)
    subtotal = getattr(order, "subtotal_price", 0.0)
    if subtotal:
        return round(float(subtotal or 0.0), 2)
    return round(float(getattr(order, "total_price", 0.0) or 0.0), 2)


def _created_at_utc(order: TikTokOrder | ShopifyOrder) -> datetime:
    created = order.created_at
    if created.tzinfo is None:
        return created.replace(tzinfo=timezone.utc)
    return created.astimezone(timezone.utc)


def _is_open_fulfillment_status(value: str | None) -> bool:
    status = str(value or "").strip().lower()
    return status in {"", "none", "null", "unfulfilled", "partial", "pending", "awaiting_shipment", "awaiting_collection"}


def is_tiktok_pack_candidate(order: TikTokOrder) -> bool:
    if classify_tiktok_reporting_status(order) != "paid":
        return False
    order_status = str(order.order_status or "").strip().lower()
    if order_status in {"cancelled", "canceled", "refunded", "delivered", "completed", "in_transit"}:
        return False
    return _is_open_fulfillment_status(order.fulfillment_status) or order_status in {"awaiting_shipment", "awaiting_collection"}


def is_shopify_pack_candidate(order: ShopifyOrder) -> bool:
    if str(order.financial_status or "").strip().lower() != "paid":
        return False
    return _is_open_fulfillment_status(order.fulfillment_status)


def build_pack_order_row(
    order: TikTokOrder | ShopifyOrder,
    *,
    source: str,
    scans: list[PackScanEvent],
) -> dict[str, Any]:
    order_id, order_number = _order_identity(order, source)
    expected_items = extract_expected_pack_items(order.line_items_summary_json, order.line_items_json)
    expected = _expected_counter(expected_items)
    expected_count = sum(expected.values())
    matched_count = _matched_count(expected, scans)
    status = pack_status_for(expected_items, scans)
    exception_scans = sorted(
        [scan for scan in scans if scan.status in PACK_EXCEPTION_STATUSES or scan.status in PACK_CONTROL_STATUSES],
        key=_scan_sort_key,
        reverse=True,
    )
    latest_exception = _latest_scan_with_status(scans, PACK_EXCEPTION_STATUSES)
    latest_control = _latest_scan_with_status(scans, PACK_CONTROL_STATUSES)
    return {
        "source": source,
        "order_id": order_id,
        "order_number": order_number,
        "customer_name": (order.customer_name or "").strip() or "Guest",
        "created_at": _created_at_utc(order),
        "total": _order_total(order),
        "financial_status": getattr(order, "financial_status", "") or "",
        "fulfillment_status": getattr(order, "fulfillment_status", "") or "",
        "order_status": getattr(order, "order_status", "") or "",
        "line_items": display_line_items(order.line_items_summary_json, order.line_items_json),
        "expected_items": expected_items,
        "expected_count": expected_count,
        "matched_count": matched_count,
        "scan_count": len(scans),
        "exception_count": sum(1 for scan in scans if scan.status in PACK_EXCEPTION_STATUSES),
        "pack_status": status,
        "progress_label": f"{matched_count}/{expected_count}" if expected_count else "0/0",
        "recent_scans": sorted(scans, key=lambda scan: scan.created_at, reverse=True)[:4],
        "exception_scans": exception_scans,
        "latest_exception": latest_exception,
        "latest_control": latest_control,
    }


def _load_scans_for_orders(session: Session, source: str, order_ids: list[str]) -> dict[str, list[PackScanEvent]]:
    if not order_ids:
        return {}
    rows = session.exec(
        select(PackScanEvent)
        .where(PackScanEvent.order_source == source)
        .where(PackScanEvent.order_id.in_(order_ids))
        .order_by(PackScanEvent.created_at.desc(), PackScanEvent.id.desc())
    ).all()
    grouped: dict[str, list[PackScanEvent]] = {}
    for row in rows:
        grouped.setdefault(row.order_id, []).append(row)
    return grouped


def load_pack_queue(
    session: Session,
    *,
    source: str = "all",
    days: int = 30,
    limit: int = 75,
    search: str = "",
) -> list[dict[str, Any]]:
    cutoff = utcnow() - timedelta(days=max(days, 1))
    rows: list[dict[str, Any]] = []
    requested_sources = [PACK_SOURCE_TIKTOK, PACK_SOURCE_SHOPIFY] if source == "all" else [source]
    search_key = search.strip().lower()

    if PACK_SOURCE_TIKTOK in requested_sources:
        tiktok_orders = session.exec(
            select(TikTokOrder)
            .where(TikTokOrder.created_at >= cutoff)
            .order_by(TikTokOrder.created_at.desc())
            .limit(max(limit * 3, limit))
        ).all()
        tiktok_orders = [order for order in tiktok_orders if is_tiktok_pack_candidate(order)]
        scans_by_order = _load_scans_for_orders(session, PACK_SOURCE_TIKTOK, [order.tiktok_order_id for order in tiktok_orders])
        for order in tiktok_orders:
            row = build_pack_order_row(
                order,
                source=PACK_SOURCE_TIKTOK,
                scans=scans_by_order.get(order.tiktok_order_id, []),
            )
            rows.append(row)

    if PACK_SOURCE_SHOPIFY in requested_sources:
        shopify_orders = session.exec(
            select(ShopifyOrder)
            .where(ShopifyOrder.created_at >= cutoff)
            .order_by(ShopifyOrder.created_at.desc())
            .limit(max(limit * 3, limit))
        ).all()
        shopify_orders = [order for order in shopify_orders if is_shopify_pack_candidate(order)]
        scans_by_order = _load_scans_for_orders(session, PACK_SOURCE_SHOPIFY, [order.shopify_order_id for order in shopify_orders])
        for order in shopify_orders:
            row = build_pack_order_row(
                order,
                source=PACK_SOURCE_SHOPIFY,
                scans=scans_by_order.get(order.shopify_order_id, []),
            )
            rows.append(row)

    if search_key:
        rows = [
            row
            for row in rows
            if search_key in str(row["order_number"]).lower()
            or search_key in str(row["order_id"]).lower()
            or search_key in str(row["customer_name"]).lower()
            or any(search_key in str(item.get("title") or "").lower() for item in row["line_items"])
            or any(search_key in str(item.get("barcode") or "").lower() for item in row["expected_items"])
        ]

    rows.sort(key=lambda row: row["created_at"], reverse=True)
    return rows[: max(limit, 1)]


def _get_order(session: Session, source: str, order_id: str) -> TikTokOrder | ShopifyOrder | None:
    if source == PACK_SOURCE_TIKTOK:
        return session.exec(select(TikTokOrder).where(TikTokOrder.tiktok_order_id == order_id)).first()
    if source == PACK_SOURCE_SHOPIFY:
        return session.exec(select(ShopifyOrder).where(ShopifyOrder.shopify_order_id == order_id)).first()
    return None


def _snapshot_item(item: InventoryItem | None) -> str:
    if item is None:
        return "{}"
    return json.dumps(
        {
            "id": item.id,
            "barcode": item.barcode,
            "item_type": item.item_type,
            "game": item.game,
            "card_name": item.card_name,
            "set_name": item.set_name,
            "card_number": item.card_number,
            "condition": item.condition,
            "status": item.status,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def record_pack_scan(
    session: Session,
    *,
    source: str,
    order_id: str,
    barcode: str,
    user: Optional[User] = None,
    notes: str | None = None,
) -> PackScanEvent:
    source = str(source or "").strip().lower()
    if source not in PACK_SOURCES:
        raise ValueError("Unknown pack source")
    clean_order_id = str(order_id or "").strip()
    clean_barcode = normalize_pack_barcode(barcode)
    if not clean_order_id:
        raise ValueError("Order id is required")
    if not clean_barcode:
        raise ValueError("Barcode is required")

    order = _get_order(session, source, clean_order_id)
    if order is None:
        raise LookupError("Order not found")

    order_id_value, order_number = _order_identity(order, source)
    expected_items = extract_expected_pack_items(order.line_items_summary_json, order.line_items_json)
    expected = _expected_counter(expected_items)
    expected_count = expected.get(clean_barcode, 0)
    item = session.exec(select(InventoryItem).where(InventoryItem.barcode == clean_barcode)).first()
    existing_matched = session.exec(
        select(PackScanEvent)
        .where(PackScanEvent.order_source == source)
        .where(PackScanEvent.order_id == order_id_value)
        .where(PackScanEvent.barcode == clean_barcode)
        .where(PackScanEvent.status == PACK_SCAN_MATCHED)
    ).all()

    if item is None:
        status = PACK_SCAN_UNKNOWN_BARCODE
    elif not expected:
        status = PACK_SCAN_UNLINKED_ORDER
    elif expected_count <= 0:
        status = PACK_SCAN_UNEXPECTED
    elif len(existing_matched) >= expected_count:
        status = PACK_SCAN_DUPLICATE
    else:
        status = PACK_SCAN_MATCHED

    label = ""
    user_id = None
    if user is not None:
        user_id = user.id
        label = (user.display_name or user.username or "").strip()

    event = PackScanEvent(
        order_source=source,
        order_id=order_id_value,
        order_number=order_number,
        barcode=clean_barcode,
        inventory_item_id=item.id if item else None,
        expected=expected_count > 0,
        status=status,
        item_snapshot_json=_snapshot_item(item),
        scanned_by_user_id=user_id,
        scanned_by_label=label or None,
        notes=(notes or "").strip() or None,
        created_at=utcnow(),
    )
    session.add(event)
    session.commit()
    session.refresh(event)
    return event


def _record_pack_control_event(
    session: Session,
    *,
    source: str,
    order_id: str,
    status: str,
    user: Optional[User] = None,
    notes: str | None = None,
) -> PackScanEvent:
    source = str(source or "").strip().lower()
    if source not in PACK_SOURCES:
        raise ValueError("Unknown pack source")
    clean_order_id = str(order_id or "").strip()
    if not clean_order_id:
        raise ValueError("Order id is required")
    order = _get_order(session, source, clean_order_id)
    if order is None:
        raise LookupError("Order not found")

    order_id_value, order_number = _order_identity(order, source)
    label = ""
    user_id = None
    if user is not None:
        user_id = user.id
        label = (user.display_name or user.username or "").strip()

    event = PackScanEvent(
        order_source=source,
        order_id=order_id_value,
        order_number=order_number,
        barcode="OVERRIDE" if status == PACK_SCAN_OVERRIDE else "REOPEN",
        inventory_item_id=None,
        expected=False,
        status=status,
        item_snapshot_json="{}",
        scanned_by_user_id=user_id,
        scanned_by_label=label or None,
        notes=(notes or "").strip() or None,
        created_at=utcnow(),
    )
    session.add(event)
    session.commit()
    session.refresh(event)
    return event


def record_pack_override(
    session: Session,
    *,
    source: str,
    order_id: str,
    reason: str,
    user: Optional[User] = None,
) -> PackScanEvent:
    clean_reason = str(reason or "").strip()
    if len(clean_reason) < 3:
        raise ValueError("Override reason is required")
    return _record_pack_control_event(
        session,
        source=source,
        order_id=order_id,
        status=PACK_SCAN_OVERRIDE,
        user=user,
        notes=clean_reason,
    )


def record_pack_reopen(
    session: Session,
    *,
    source: str,
    order_id: str,
    reason: str | None = None,
    user: Optional[User] = None,
) -> PackScanEvent:
    return _record_pack_control_event(
        session,
        source=source,
        order_id=order_id,
        status=PACK_SCAN_REOPENED,
        user=user,
        notes=reason,
    )


def _exception_reason_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if row.get("pack_status") == "needs_item_link":
        reasons.append(
            {
                "status": "needs_item_link",
                "label": "Needs item link",
                "barcode": "",
                "operator": "",
                "notes": "No DGN barcode SKU was found on this order.",
                "created_at": row.get("created_at"),
            }
        )
    for scan in row.get("exception_scans") or []:
        if scan.status not in PACK_EXCEPTION_STATUSES and scan.status not in PACK_CONTROL_STATUSES:
            continue
        reasons.append(
            {
                "status": scan.status,
                "label": scan.status.replace("_", " ").title(),
                "barcode": scan.barcode,
                "operator": scan.scanned_by_label or "",
                "notes": scan.notes or "",
                "created_at": scan.created_at,
            }
        )
    return reasons


def _exception_filter_matches(row: dict[str, Any], status_filter: str) -> bool:
    pack_status = str(row.get("pack_status") or "")
    if status_filter == "all":
        return pack_status in {"exception", "needs_item_link", "override"}
    if status_filter == "blocked":
        return pack_status in {"exception", "needs_item_link"}
    return pack_status == status_filter


def load_pack_exception_queue(
    session: Session,
    *,
    source: str = "all",
    status_filter: str = "blocked",
    days: int = 30,
    limit: int = 75,
    search: str = "",
) -> list[dict[str, Any]]:
    selected_filter = status_filter if status_filter in PACK_QUEUE_EXCEPTION_FILTERS else "blocked"
    rows = load_pack_queue(
        session,
        source=source,
        days=days,
        limit=max(limit * 4, limit),
        search=search,
    )
    queue_rows = [row for row in rows if _exception_filter_matches(row, selected_filter)]
    for row in queue_rows:
        reasons = _exception_reason_rows(row)
        row["exception_reasons"] = reasons
        row["latest_exception_activity"] = reasons[0] if reasons else None
    queue_rows.sort(
        key=lambda row: (
            row.get("latest_exception_activity", {}).get("created_at")
            if row.get("latest_exception_activity")
            else row["created_at"]
        ),
        reverse=True,
    )
    return queue_rows[: max(limit, 1)]


def pack_exception_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "total": len(rows),
        "blocked": 0,
        "exception": 0,
        "needs_item_link": 0,
        "override": 0,
        PACK_SCAN_UNKNOWN_BARCODE: 0,
        PACK_SCAN_UNEXPECTED: 0,
        PACK_SCAN_DUPLICATE: 0,
        PACK_SCAN_UNLINKED_ORDER: 0,
    }
    for row in rows:
        pack_status = str(row.get("pack_status") or "")
        if pack_status in {"exception", "needs_item_link"}:
            counts["blocked"] += 1
        if pack_status in counts:
            counts[pack_status] += 1
        for scan in row.get("exception_scans") or []:
            if scan.status in counts:
                counts[scan.status] += 1
    return counts


def pack_queue_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "total": len(rows),
        "ready_to_scan": 0,
        "in_progress": 0,
        "verified": 0,
        "override": 0,
        "exception": 0,
        "needs_item_link": 0,
    }
    for row in rows:
        status = str(row.get("pack_status") or "")
        if status in counts:
            counts[status] += 1
    return counts
