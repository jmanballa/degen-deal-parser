from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, or_
from sqlmodel import Session, select
from zoneinfo import ZoneInfo

import logging

def _ensure_utc(dt: datetime | None) -> datetime | None:
    """Return a timezone-aware (UTC) datetime regardless of input."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

from .models import (
    DiscordMessage,
    expand_parse_status_filter_values,
    PARSE_PARSED,
    PARSE_REVIEW_REQUIRED,
    ShopifyOrder,
    TikTokOrder,
    TikTokProduct,
    normalize_money_value,
    signed_money_delta,
)

logger = logging.getLogger(__name__)


REPORTING_TZ = ZoneInfo("America/Los_Angeles")


def parse_report_datetime(value: Optional[str], *, end_of_day: bool = False) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if len(value) == 10:
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=REPORTING_TZ)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=REPORTING_TZ)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def financial_base_query(
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
):
    stmt = select(DiscordMessage).where(DiscordMessage.is_deleted == False)
    stmt = stmt.where(
        DiscordMessage.parse_status.in_(
            sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED]))
        )
    )
    stmt = stmt.where(
        (DiscordMessage.stitched_group_id == None) | (DiscordMessage.stitched_primary == True)
    )

    if start:
        stmt = stmt.where(DiscordMessage.created_at >= start)
    if end:
        stmt = stmt.where(DiscordMessage.created_at <= end)
    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)

    return stmt.order_by(DiscordMessage.created_at)


def get_financial_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
) -> list[DiscordMessage]:
    stmt = financial_base_query(start=start, end=end, channel_id=channel_id)
    return session.exec(stmt).all()


def get_shopify_reporting_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> list[ShopifyOrder]:
    stmt = select(ShopifyOrder)
    if start:
        stmt = stmt.where(ShopifyOrder.created_at >= start)
    if end:
        stmt = stmt.where(ShopifyOrder.created_at <= end)
    stmt = stmt.order_by(ShopifyOrder.created_at.asc(), ShopifyOrder.id.asc())
    return session.exec(stmt).all()


def get_tiktok_reporting_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> list[TikTokOrder]:
    return get_tiktok_order_rows(session, start=start, end=end, sort_by="date", sort_dir="asc")


def _normalize_tiktok_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _build_tiktok_orders_base_query(
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    shop_id: Optional[str] = None,
    shop_cipher: Optional[str] = None,
    seller_id: Optional[str] = None,
    source: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    order_status: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None,
):
    stmt = select(TikTokOrder)
    if start:
        stmt = stmt.where(TikTokOrder.created_at >= start)
    if end:
        stmt = stmt.where(TikTokOrder.created_at <= end)
    if shop_id:
        stmt = stmt.where(TikTokOrder.shop_id == shop_id)
    if shop_cipher:
        stmt = stmt.where(TikTokOrder.shop_cipher == shop_cipher)
    if seller_id:
        stmt = stmt.where(TikTokOrder.seller_id == seller_id)
    if source:
        stmt = stmt.where(func.lower(func.coalesce(TikTokOrder.source, "")) == source.strip().lower())
    if financial_status:
        stmt = stmt.where(func.lower(TikTokOrder.financial_status) == financial_status.strip().lower())
    if fulfillment_status:
        stmt = stmt.where(func.lower(func.coalesce(TikTokOrder.fulfillment_status, "")) == fulfillment_status.strip().lower())
    if order_status:
        stmt = stmt.where(func.lower(func.coalesce(TikTokOrder.order_status, "")) == order_status.strip().lower())
    if currency:
        stmt = stmt.where(func.lower(func.coalesce(TikTokOrder.currency, "")) == currency.strip().lower())
    if search:
        pattern = f"%{search.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(TikTokOrder.tiktok_order_id).like(pattern),
                func.lower(TikTokOrder.order_number).like(pattern),
                func.lower(func.coalesce(TikTokOrder.customer_name, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.customer_email, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.shop_id, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.shop_cipher, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.seller_id, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.currency, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.financial_status, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.fulfillment_status, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.order_status, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.line_items_summary_json, "")).like(pattern),
                func.lower(func.coalesce(TikTokOrder.line_items_json, "")).like(pattern),
            )
        )
    return stmt


def _resolve_tiktok_order_sort_column(sort_by: str):
    normalized = _normalize_tiktok_text(sort_by)
    net_expr = func.coalesce(TikTokOrder.subtotal_ex_tax, TikTokOrder.total_price - func.coalesce(TikTokOrder.total_tax, 0.0))
    return {
        "date": TikTokOrder.created_at,
        "created_at": TikTokOrder.created_at,
        "updated_at": TikTokOrder.updated_at,
        "gross": TikTokOrder.total_price,
        "tax": func.coalesce(TikTokOrder.total_tax, 0.0),
        "net": net_expr,
        "order_number": TikTokOrder.order_number,
        "status": TikTokOrder.financial_status,
        "fulfillment": func.coalesce(TikTokOrder.fulfillment_status, ""),
        "currency": func.coalesce(TikTokOrder.currency, ""),
    }.get(normalized, TikTokOrder.created_at)


def count_tiktok_order_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    shop_id: Optional[str] = None,
    shop_cipher: Optional[str] = None,
    seller_id: Optional[str] = None,
    source: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    order_status: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None,
) -> int:
    stmt = _build_tiktok_orders_base_query(
        start=start,
        end=end,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        seller_id=seller_id,
        source=source,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        currency=currency,
        search=search,
    )
    count_stmt = select(func.count()).select_from(stmt.order_by(None).subquery())
    return int(session.exec(count_stmt).one())


def get_tiktok_order_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    shop_id: Optional[str] = None,
    shop_cipher: Optional[str] = None,
    seller_id: Optional[str] = None,
    source: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    order_status: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "date",
    sort_dir: str = "desc",
    page: int = 1,
    limit: Optional[int] = None,
) -> list[TikTokOrder]:
    stmt = _build_tiktok_orders_base_query(
        start=start,
        end=end,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        seller_id=seller_id,
        source=source,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        currency=currency,
        search=search,
    )
    safe_sort_dir = _normalize_tiktok_text(sort_dir)
    sort_column = _resolve_tiktok_order_sort_column(sort_by)
    if safe_sort_dir == "asc":
        stmt = stmt.order_by(sort_column.asc(), TikTokOrder.id.asc())
    else:
        stmt = stmt.order_by(sort_column.desc(), TikTokOrder.id.desc())
    if limit:
        offset = (max(page, 1) - 1) * limit
        stmt = stmt.offset(offset).limit(limit)
    return session.exec(stmt).all()


def _safe_json_list(value: Optional[str]) -> list[dict[str, object]]:
    try:
        loaded = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def _build_external_line_item_summary(
    rows: list[object],
    *,
    summary_attr: str = "line_items_summary_json",
    fallback_attr: str = "line_items_json",
) -> dict:
    total_orders_with_items = 0
    total_line_items = 0
    title_counts: defaultdict[str, int] = defaultdict(int)

    for row in rows:
        summary_value = getattr(row, summary_attr, None)
        fallback_value = getattr(row, fallback_attr, None)
        summaries = _safe_json_list(summary_value if summary_value != "[]" else fallback_value)
        if not summaries:
            continue

        total_orders_with_items += 1
        for item in summaries:
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            quantity_raw = item.get("quantity")
            try:
                quantity = int(quantity_raw or 0)
            except (TypeError, ValueError):
                quantity = 0
            if quantity <= 0:
                quantity = 1
            total_line_items += quantity
            title_counts[title] += quantity

    return {
        "orders_with_items": total_orders_with_items,
        "line_items_total": total_line_items,
        "avg_line_items_per_order": (
            round(total_line_items / total_orders_with_items, 2) if total_orders_with_items else 0.0
        ),
        "top_item_titles": [
            {"title": title, "quantity": quantity}
            for title, quantity in sorted(title_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
        ],
    }


def build_shopify_line_item_summary(rows: list[ShopifyOrder]) -> dict:
    return _build_external_line_item_summary(rows)


def build_tiktok_line_item_summary(rows: list[TikTokOrder]) -> dict:
    return _build_external_line_item_summary(rows)


def _tiktok_daily_totals_bucket() -> dict[str, object]:
    return {
        "orders": 0,
        "paid_orders": 0,
        "pending_orders": 0,
        "refunded_orders": 0,
        "other_orders": 0,
        "gross": 0.0,
        "tax": 0.0,
        "net": 0.0,
        "tax_unknown_orders": 0,
    }


def _tiktok_status_key(row: TikTokOrder) -> str:
    return (row.financial_status or row.order_status or "").strip().lower()


TIKTOK_PAID_STATUSES = {
    "paid",
    "completed",
    "awaiting_shipment",
    "awaiting_collection",
    "awaiting_delivery",
    "in_transit",
    "delivered",
}

_TIKTOK_PENDING_STATUSES = {
    "pending",
    "unpaid",
    "awaiting_payment",
    "payment_pending",
}

_TIKTOK_REFUNDED_STATUSES = {
    "refunded",
    "cancelled",
    "canceled",
    "return_requested",
    "refund_requested",
    "return_or_refund_request_pending",
    "refund_complete",
}


def classify_tiktok_reporting_status(row: TikTokOrder) -> str:
    status = _tiktok_status_key(row)
    if status in TIKTOK_PAID_STATUSES:
        return "paid"
    if status in _TIKTOK_PENDING_STATUSES:
        return "pending"
    if status in _TIKTOK_REFUNDED_STATUSES:
        return "refunded"
    return "other"


def _tiktok_day_key(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    parsed = value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(REPORTING_TZ).date().isoformat()


def build_tiktok_daily_totals(rows: list[TikTokOrder]) -> list[dict[str, object]]:
    daily_totals: defaultdict[str, dict[str, object]] = defaultdict(_tiktok_daily_totals_bucket)

    for row in rows:
        day_key = _tiktok_day_key(row.created_at)
        if not day_key:
            continue

        bucket = daily_totals[day_key]
        bucket["orders"] += 1

        status = classify_tiktok_reporting_status(row)
        if status == "paid":
            bucket["paid_orders"] += 1
        elif status == "pending":
            bucket["pending_orders"] += 1
        elif status == "refunded":
            bucket["refunded_orders"] += 1
        else:
            bucket["other_orders"] += 1

        if status != "paid":
            continue

        gross_value = float(row.total_price or 0.0)
        bucket["gross"] += gross_value

        if row.total_tax is None:
            bucket["tax_unknown_orders"] += 1
            continue

        tax_value = float(row.total_tax or 0.0)
        net_value = float(row.subtotal_ex_tax) if row.subtotal_ex_tax is not None else gross_value - tax_value
        bucket["tax"] += tax_value
        bucket["net"] += net_value

    return [
        {
            "date": date_key,
            "orders": values["orders"],
            "paid_orders": values["paid_orders"],
            "pending_orders": values["pending_orders"],
            "refunded_orders": values["refunded_orders"],
            "other_orders": values["other_orders"],
            "gross": round(values["gross"], 2),
            "tax": round(values["tax"], 2),
            "net": round(values["net"], 2),
            "tax_unknown_orders": values["tax_unknown_orders"],
        }
        for date_key, values in sorted(daily_totals.items())
    ]


def build_shopify_reporting_summary(rows: list[ShopifyOrder]) -> dict:
    status_counts = {"paid": 0, "pending": 0, "refunded": 0, "other": 0}
    paid_order_count = 0
    tax_unknown_count = 0
    paid_gross = 0.0
    paid_tax = 0.0
    paid_net = 0.0
    paid_known_tax_orders = 0
    line_item_summary = build_shopify_line_item_summary(rows)

    for row in rows:
        status = (row.financial_status or "").strip().lower()
        if status == "paid":
            status_counts["paid"] += 1
        elif status == "pending":
            status_counts["pending"] += 1
        elif status == "refunded":
            status_counts["refunded"] += 1
        else:
            status_counts["other"] += 1

        if status != "paid":
            continue

        paid_order_count += 1
        gross_value = float(row.total_price or 0.0)
        paid_gross += gross_value

        if row.total_tax is None:
            tax_unknown_count += 1
            continue

        tax_value = float(row.total_tax or 0.0)
        net_value = float(row.subtotal_ex_tax) if row.subtotal_ex_tax is not None else gross_value - tax_value
        paid_tax += tax_value
        paid_net += net_value
        paid_known_tax_orders += 1

    avg_paid_net = round(paid_net / paid_known_tax_orders, 2) if paid_known_tax_orders else 0.0

    return {
        "orders": len(rows),
        "status_counts": status_counts,
        "paid_orders": paid_order_count,
        "paid_orders_with_known_tax": paid_known_tax_orders,
        "tax_unknown_orders": tax_unknown_count,
        "gross_revenue": round(paid_gross, 2),
        "total_tax": round(paid_tax, 2),
        "net_revenue": round(paid_net, 2),
        "avg_order_value_net": avg_paid_net,
        "has_missing_tax_data": tax_unknown_count > 0,
        "warning": (
            f"{tax_unknown_count} orders missing tax data - totals may be incomplete"
            if tax_unknown_count
            else ""
        ),
        "line_item_summary": line_item_summary,
    }


def build_tiktok_reporting_summary(rows: list[TikTokOrder]) -> dict:
    status_counts = {"paid": 0, "pending": 0, "refunded": 0, "other": 0}
    order_status_counts: defaultdict[str, int] = defaultdict(int)
    fulfillment_status_counts: defaultdict[str, int] = defaultdict(int)
    paid_order_count = 0
    tax_unknown_count = 0
    paid_gross = 0.0
    paid_tax = 0.0
    paid_net = 0.0
    paid_known_tax_orders = 0
    line_item_summary = build_tiktok_line_item_summary(rows)

    for row in rows:
        status = classify_tiktok_reporting_status(row)
        order_status = (row.order_status or "").strip().lower()
        fulfillment_status = (row.fulfillment_status or "").strip().lower()
        if status == "paid":
            status_counts["paid"] += 1
        elif status == "pending":
            status_counts["pending"] += 1
        elif status == "refunded":
            status_counts["refunded"] += 1
        else:
            status_counts["other"] += 1

        if order_status:
            order_status_counts[order_status] += 1
        if fulfillment_status:
            fulfillment_status_counts[fulfillment_status] += 1

        if status != "paid":
            continue

        paid_order_count += 1
        gross_value = float(row.total_price or 0.0)
        paid_gross += gross_value

        if row.total_tax is None:
            tax_unknown_count += 1
            continue

        tax_value = float(row.total_tax or 0.0)
        net_value = float(row.subtotal_ex_tax) if row.subtotal_ex_tax is not None else gross_value - tax_value
        paid_tax += tax_value
        paid_net += net_value
        paid_known_tax_orders += 1

    avg_paid_net = round(paid_net / paid_known_tax_orders, 2) if paid_known_tax_orders else 0.0

    return {
        "orders": len(rows),
        "status_counts": status_counts,
        "paid_orders": paid_order_count,
        "paid_orders_with_known_tax": paid_known_tax_orders,
        "tax_unknown_orders": tax_unknown_count,
        "gross_revenue": round(paid_gross, 2),
        "total_tax": round(paid_tax, 2),
        "net_revenue": round(paid_net, 2),
        "avg_order_value_net": avg_paid_net,
        "has_missing_tax_data": tax_unknown_count > 0,
        "warning": (
            f"{tax_unknown_count} orders missing tax data - totals may be incomplete"
            if tax_unknown_count
            else ""
        ),
        "order_status_counts": dict(sorted(order_status_counts.items(), key=lambda item: (-item[1], item[0]))),
        "fulfillment_status_counts": dict(
            sorted(fulfillment_status_counts.items(), key=lambda item: (-item[1], item[0]))
        ),
        "line_item_summary": line_item_summary,
        "daily_totals": build_tiktok_daily_totals(rows),
    }


def build_tiktok_orders_page_data(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    shop_id: Optional[str] = None,
    shop_cipher: Optional[str] = None,
    seller_id: Optional[str] = None,
    source: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    order_status: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "date",
    sort_dir: str = "desc",
    page: int = 1,
    limit: Optional[int] = None,
) -> dict[str, object]:
    rows = get_tiktok_order_rows(
        session,
        start=start,
        end=end,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        seller_id=seller_id,
        source=source,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        currency=currency,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    total_count = count_tiktok_order_rows(
        session,
        start=start,
        end=end,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        seller_id=seller_id,
        source=source,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        currency=currency,
        search=search,
    )
    summary_rows = get_tiktok_order_rows(
        session,
        start=start,
        end=end,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        seller_id=seller_id,
        source=source,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        currency=currency,
        search=search,
        sort_by="date",
        sort_dir="desc",
    )
    summary = build_tiktok_reporting_summary(summary_rows)
    daily_totals = summary.get("daily_totals", [])
    return {
        "rows": rows,
        "total_count": total_count,
        "summary": summary,
        "daily_totals": daily_totals,
        "line_item_summary": summary.get("line_item_summary", {}),
        "page": max(page, 1),
        "page_size": limit,
        "has_more": bool(limit and (page * limit) < total_count),
    }


def build_reporting_periods(
    *,
    selected_start: Optional[datetime] = None,
    selected_end: Optional[datetime] = None,
) -> list[dict[str, object]]:
    now_local = datetime.now(REPORTING_TZ)
    now_utc = now_local.astimezone(timezone.utc)
    today_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start_local = today_start_local - timedelta(days=today_start_local.weekday())
    month_start_local = today_start_local.replace(day=1)

    periods = [
        {
            "key": "today",
            "label": "Today",
            "start": today_start_local.astimezone(timezone.utc),
            "end": now_utc,
        },
        {
            "key": "this_week",
            "label": "This Week",
            "start": week_start_local.astimezone(timezone.utc),
            "end": now_utc,
        },
        {
            "key": "this_month",
            "label": "This Month",
            "start": month_start_local.astimezone(timezone.utc),
            "end": now_utc,
        },
    ]

    if selected_start or selected_end:
        periods.append(
            {
                "key": "selected_range",
                "label": "Selected Range",
                "start": selected_start,
                "end": selected_end,
            }
        )

    return periods


def build_financial_summary(rows: list[DiscordMessage]) -> dict:
    totals = defaultdict(float)
    count_by_kind = defaultdict(int)
    expense_categories = defaultdict(float)
    category_breakdown = defaultdict(float)
    payment_breakdown = defaultdict(float)
    timeline = defaultdict(lambda: defaultdict(float))

    for row in rows:
        money_in = normalize_money_value(row.money_in)
        money_out = normalize_money_value(row.money_out)
        entry_kind = row.entry_kind or "unknown"
        day_key = row.created_at.date().isoformat()
        net_value = signed_money_delta(money_in, money_out)
        amount_value = row.amount if row.amount is not None else money_in or money_out

        totals["money_in"] += money_in
        totals["money_out"] += money_out
        totals["net"] += net_value
        count_by_kind[entry_kind] += 1
        if row.needs_review:
            count_by_kind["needs_review"] += 1

        if entry_kind == "sale":
            totals["sales"] += money_in
            timeline[day_key]["sales"] += money_in
        elif entry_kind == "buy":
            totals["buys"] += money_out
            timeline[day_key]["buys"] += money_out
        elif entry_kind == "expense":
            totals["expenses"] += money_out
            timeline[day_key]["expenses"] += money_out
        elif entry_kind == "trade":
            totals["trade_cash_in"] += money_in
            totals["trade_cash_out"] += money_out
            timeline[day_key]["trade_in"] += money_in
            timeline[day_key]["trade_out"] += money_out

        if row.expense_category:
            expense_categories[row.expense_category] += money_out

        if row.category:
            category_breakdown[row.category] += normalize_money_value(amount_value)
        if row.payment_method:
            payment_breakdown[row.payment_method] += normalize_money_value(amount_value)

    totals["gross_margin"] = totals["sales"] - totals["buys"]

    return {
        "totals": {key: round(value, 2) for key, value in totals.items()},
        "counts": dict(count_by_kind),
        "expense_categories": {
            key: round(value, 2)
            for key, value in sorted(expense_categories.items(), key=lambda item: (-item[1], item[0]))
        },
        "deal_categories": {
            key: round(value, 2)
            for key, value in sorted(category_breakdown.items(), key=lambda item: (-item[1], item[0]))
        },
        "payment_methods": {
            key: round(value, 2)
            for key, value in sorted(payment_breakdown.items(), key=lambda item: (-item[1], item[0]))
        },
        "timeline": [
            {
                "date": date_key,
                "sales": round(values.get("sales", 0.0), 2),
                "buys": round(values.get("buys", 0.0), 2),
                "expenses": round(values.get("expenses", 0.0), 2),
                "trade_in": round(values.get("trade_in", 0.0), 2),
                "trade_out": round(values.get("trade_out", 0.0), 2),
                "net": round(
                    values.get("sales", 0.0)
                    + values.get("trade_in", 0.0)
                    - values.get("buys", 0.0)
                    - values.get("expenses", 0.0)
                    - values.get("trade_out", 0.0),
                    2,
                ),
            }
            for date_key, values in sorted(timeline.items())
        ],
        "rows": len(rows),
    }


# ---------------------------------------------------------------------------
# TikTok Analytics — Repeat Buyers
# ---------------------------------------------------------------------------

def build_tiktok_buyer_insights(session: Session, days: int = 90) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    orders = session.exec(
        select(TikTokOrder).where(TikTokOrder.created_at >= cutoff)
    ).all()

    agg: dict[str, dict] = {}
    for o in orders:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status not in TIKTOK_PAID_STATUSES:
            continue
        buyer_name = (o.customer_name or "").strip() or "Guest"
        buyer_key = buyer_name.lower()
        gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        if buyer_key not in agg:
            agg[buyer_key] = {
                "name": buyer_name,
                "total_spent": 0.0,
                "order_count": 0,
                "first_order": o.created_at,
                "last_order": o.created_at,
                "stream_dates": set(),
            }
        entry = agg[buyer_key]
        entry["total_spent"] += gmv
        entry["order_count"] += 1
        if o.created_at and (entry["first_order"] is None or o.created_at < entry["first_order"]):
            entry["first_order"] = o.created_at
        if o.created_at and (entry["last_order"] is None or o.created_at > entry["last_order"]):
            entry["last_order"] = o.created_at
        if o.created_at:
            entry["stream_dates"].add(o.created_at.strftime("%Y-%m-%d"))

    results = []
    for entry in agg.values():
        oc = entry["order_count"]
        results.append({
            "name": entry["name"],
            "total_spent": round(entry["total_spent"], 2),
            "order_count": oc,
            "avg_order": round(entry["total_spent"] / oc, 2) if oc > 0 else 0,
            "first_order": entry["first_order"].isoformat() if entry["first_order"] else None,
            "last_order": entry["last_order"].isoformat() if entry["last_order"] else None,
            "stream_count": len(entry["stream_dates"]),
            "is_repeat": oc > 1,
        })
    results.sort(key=lambda x: x["total_spent"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# TikTok Analytics — Product Performance
# ---------------------------------------------------------------------------

def build_tiktok_product_performance(
    session: Session,
    days: int = 30,
    stream_sessions: list[dict] | None = None,
) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    orders = session.exec(
        select(TikTokOrder).where(TikTokOrder.created_at >= cutoff)
    ).all()

    def _is_during_stream(ts: datetime | None) -> bool:
        if not ts or not stream_sessions:
            return False
        ts_epoch = ts.timestamp()
        for s in stream_sessions:
            st = s.get("start_time", 0) or 0
            et = s.get("end_time", 0) or 0
            if st > 0 and ts_epoch >= st:
                if et == 0 or ts_epoch <= et:
                    return True
        return False

    agg: dict[str, dict] = {}
    for o in orders:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status not in TIKTOK_PAID_STATUSES:
            continue
        raw_items: list[dict] = []
        try:
            raw_items = json.loads(o.line_items_json) if o.line_items_json else []
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(raw_items, list):
            continue
        is_live = _is_during_stream(o.created_at)
        for item in raw_items:
            title = (item.get("product_name") or item.get("title") or "Unknown").strip()
            key = title.lower()
            qty = int(item.get("quantity", 1) or 1)
            price = float(item.get("sale_price") or item.get("price") or 0)
            revenue = price * qty
            if key not in agg:
                agg[key] = {
                    "title": title,
                    "qty": 0, "revenue": 0.0, "orders": 0,
                    "live_qty": 0, "nonlive_qty": 0,
                }
            entry = agg[key]
            entry["qty"] += qty
            entry["revenue"] += revenue
            entry["orders"] += 1
            if is_live:
                entry["live_qty"] += qty
            else:
                entry["nonlive_qty"] += qty

    results = []
    for entry in agg.values():
        total_qty = entry["qty"] or 1
        results.append({
            "title": entry["title"],
            "qty": entry["qty"],
            "revenue": round(entry["revenue"], 2),
            "orders": entry["orders"],
            "avg_price": round(entry["revenue"] / entry["qty"], 2) if entry["qty"] > 0 else 0,
            "live_pct": round((entry["live_qty"] / total_qty) * 100, 1),
            "nonlive_pct": round((entry["nonlive_qty"] / total_qty) * 100, 1),
        })
    results.sort(key=lambda x: x["revenue"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Shared helpers — paid TikTok order loading, line-item normalization
# ---------------------------------------------------------------------------

def load_paid_tiktok_orders(session: Session, days: int = 90) -> list[TikTokOrder]:
    """Load TikTok orders with paid statuses.  days=0 means all-time."""
    q = select(TikTokOrder)
    if days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        q = q.where(TikTokOrder.created_at >= cutoff)
    orders = session.exec(q).all()
    result = []
    for o in orders:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status in TIKTOK_PAID_STATUSES:
            result.append(o)
    return result


def parse_line_items(order: TikTokOrder) -> list[dict]:
    """Extract normalized line items from an order, preferring summary JSON."""
    for field in (order.line_items_summary_json, order.line_items_json):
        if not field:
            continue
        try:
            items = json.loads(field)
            if isinstance(items, list) and items:
                return items
        except (json.JSONDecodeError, TypeError):
            continue
    return []


def normalize_item(item: dict) -> dict:
    """Return a normalized dict with consistent keys from a raw line item."""
    title = (item.get("product_name") or item.get("title") or "Unknown").strip()
    return {
        "title": title,
        "title_key": title.lower(),
        "product_id": str(item.get("product_id") or ""),
        "sku_id": str(item.get("sku_id") or item.get("variant_id") or ""),
        "qty": int(item.get("quantity", 1) or 1),
        "price": float(item.get("sale_price") or item.get("unit_price") or item.get("price") or 0),
        "sku_image": item.get("sku_image") or item.get("image_url") or "",
    }


def build_catalog_lookup(session: Session) -> dict[str, dict]:
    """Build lookup maps for matching line items to catalog products."""
    products = session.exec(select(TikTokProduct)).all()
    by_id: dict[str, dict] = {}
    by_title: dict[str, dict] = {}
    for p in products:
        info = {"title": p.title, "status": p.status or "unknown", "product_id": p.tiktok_product_id}
        by_id[p.tiktok_product_id] = info
        by_title[p.title.lower().strip()] = info
    return {"by_id": by_id, "by_title": by_title}


def match_catalog(item: dict, catalog: dict[str, dict]) -> dict | None:
    """Match a normalized item to the catalog. Returns catalog info or None."""
    if item["product_id"] and item["product_id"] in catalog["by_id"]:
        return catalog["by_id"][item["product_id"]]
    if item["title_key"] in catalog["by_title"]:
        return catalog["by_title"][item["title_key"]]
    return None


def _buyer_key(order: TikTokOrder) -> tuple[str, str]:
    """Return (buyer_key, display_name) from an order."""
    name = (order.customer_name or "").strip() or "Guest"
    return name.lower(), name


# ---------------------------------------------------------------------------
# TikTok Client Intelligence — Buyer Profiles
# ---------------------------------------------------------------------------

def build_buyer_profiles(
    session: Session,
    days: int = 180,
    vip_threshold: float = 1000.0,
    regular_threshold: float = 100.0,
) -> list[dict]:
    orders = load_paid_tiktok_orders(session, days=days)
    now = datetime.now(timezone.utc)

    agg: dict[str, dict] = {}
    for o in orders:
        bkey, bname = _buyer_key(o)
        if bkey not in agg:
            agg[bkey] = {
                "name": bname,
                "total_spent": 0.0,
                "order_count": 0,
                "first_order": _ensure_utc(o.created_at),
                "last_order": _ensure_utc(o.created_at),
                "order_dates": [],
                "product_counts": defaultdict(int),
            }
        entry = agg[bkey]
        gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        entry["total_spent"] += gmv
        entry["order_count"] += 1
        ca = _ensure_utc(o.created_at)
        if ca:
            if entry["first_order"] is None or ca < entry["first_order"]:
                entry["first_order"] = ca
            if entry["last_order"] is None or ca > entry["last_order"]:
                entry["last_order"] = ca
            entry["order_dates"].append(ca)

        for raw in parse_line_items(o):
            ni = normalize_item(raw)
            entry["product_counts"][ni["title"]] += ni["qty"]

    results = []
    for bkey, entry in agg.items():
        oc = entry["order_count"]
        spent = entry["total_spent"]
        last = entry["last_order"]

        days_since = (now - last).days if last else 999
        if days_since <= 14:
            recency = "Active"
        elif days_since <= 30:
            recency = "Recent"
        elif days_since <= 90:
            recency = "Lapsed"
        else:
            recency = "Dormant"

        if spent >= vip_threshold:
            tier = "VIP"
        elif spent >= regular_threshold:
            tier = "Regular"
        else:
            tier = "Casual"

        first = entry["first_order"]
        span_days = (last - first).days if first and last and last > first else 0
        freq = round(oc / max(span_days / 30.0, 1), 2) if oc > 1 else 0
        avg_gap = round(span_days / (oc - 1), 1) if oc > 1 else None

        top_prods = sorted(entry["product_counts"].items(), key=lambda x: x[1], reverse=True)[:3]

        results.append({
            "buyer_key": bkey,
            "name": entry["name"],
            "total_spent": round(spent, 2),
            "order_count": oc,
            "avg_order": round(spent / oc, 2) if oc else 0,
            "first_order": first.isoformat() if first else None,
            "last_order": last.isoformat() if last else None,
            "frequency_per_month": freq,
            "avg_days_between_orders": avg_gap,
            "recency_bucket": recency,
            "spend_tier": tier,
            "top_products": [{"title": t, "qty": q} for t, q in top_prods],
            "is_repeat": oc > 1,
        })

    results.sort(key=lambda x: x["total_spent"], reverse=True)
    return results


def build_buyer_detail(session: Session, buyer_key: str, days: int = 0) -> dict | None:
    """Fetch detailed order history and product breakdown for a single buyer."""
    orders = load_paid_tiktok_orders(session, days=days)
    buyer_orders = []
    product_counts: dict[str, dict] = {}
    total_spent = 0.0

    for o in orders:
        bkey, _ = _buyer_key(o)
        if bkey != buyer_key:
            continue
        gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        total_spent += gmv
        items_merged: dict[str, dict] = {}
        for raw in parse_line_items(o):
            ni = normalize_item(raw)
            tkey = ni["title_key"]
            if tkey not in items_merged:
                items_merged[tkey] = {"title": ni["title"], "qty": 0, "price": ni["price"]}
            items_merged[tkey]["qty"] += ni["qty"]

            if tkey not in product_counts:
                product_counts[tkey] = {"title": ni["title"], "qty": 0, "revenue": 0.0}
            product_counts[tkey]["qty"] += ni["qty"]
            product_counts[tkey]["revenue"] += ni["price"] * ni["qty"]

        buyer_orders.append({
            "order_number": o.order_number,
            "created_at": o.created_at.isoformat() if o.created_at else None,
            "total": gmv,
            "items": list(items_merged.values()),
        })

    if not buyer_orders:
        return None

    buyer_orders.sort(key=lambda x: x["created_at"] or "", reverse=True)
    top_products = sorted(product_counts.values(), key=lambda x: x["revenue"], reverse=True)

    return {
        "buyer_key": buyer_key,
        "orders": buyer_orders[:50],
        "top_products": top_products[:10],
        "total_spent": round(total_spent, 2),
        "order_count": len(buyer_orders),
    }


# ---------------------------------------------------------------------------
# TikTok Client Intelligence — Product Velocity
# ---------------------------------------------------------------------------

def build_product_velocity(session: Session, days: int = 90) -> list[dict]:
    orders = load_paid_tiktok_orders(session, days=days)
    catalog = build_catalog_lookup(session)
    now = datetime.now(timezone.utc)
    cutoff_30 = now - timedelta(days=30)
    cutoff_60 = now - timedelta(days=60)

    agg: dict[str, dict] = {}
    for o in orders:
        bkey, bname = _buyer_key(o)
        for raw in parse_line_items(o):
            ni = normalize_item(raw)
            key = ni["product_id"] or ni["title_key"]
            qty = ni["qty"]
            revenue = ni["price"] * qty

            if key not in agg:
                cat = match_catalog(ni, catalog)
                agg[key] = {
                    "title": ni["title"],
                    "product_key": key,
                    "qty": 0, "revenue": 0.0, "orders": 0,
                    "sale_dates": [],
                    "buyers": set(),
                    "qty_last_30": 0, "qty_prev_30": 0,
                    "catalog_status": cat["status"] if cat else "unmatched",
                }
            entry = agg[key]
            entry["qty"] += qty
            entry["revenue"] += revenue
            entry["orders"] += 1
            entry["buyers"].add(bkey)
            ca = _ensure_utc(o.created_at)
            if ca:
                entry["sale_dates"].append(ca)
                if ca >= cutoff_30:
                    entry["qty_last_30"] += qty
                elif ca >= cutoff_60:
                    entry["qty_prev_30"] += qty

    results = []
    for entry in agg.values():
        dates = sorted(entry["sale_dates"])
        last_sale = dates[-1] if dates else None
        days_since = (now - last_sale).days if last_sale else None

        if len(dates) >= 2:
            span = (dates[-1] - dates[0]).days
            avg_gap = round(span / (len(dates) - 1), 1)
        else:
            avg_gap = None

        q_cur = entry["qty_last_30"]
        q_prev = entry["qty_prev_30"]
        if q_prev == 0 and q_cur > 0:
            trend = "new"
        elif q_prev == 0 and q_cur == 0:
            trend = "flat"
        elif q_cur > q_prev * 1.15:
            trend = "rising"
        elif q_cur < q_prev * 0.85:
            trend = "falling"
        else:
            trend = "flat"

        total_qty = entry["qty"] or 1
        results.append({
            "product_key": entry["product_key"],
            "title": entry["title"],
            "qty": entry["qty"],
            "revenue": round(entry["revenue"], 2),
            "orders": entry["orders"],
            "avg_price": round(entry["revenue"] / total_qty, 2),
            "last_sale_at": last_sale.isoformat() if last_sale else None,
            "days_since_last_sale": days_since,
            "avg_days_between_sales": avg_gap,
            "trend": trend,
            "unique_buyers": len(entry["buyers"]),
            "catalog_status": entry["catalog_status"],
        })

    results.sort(key=lambda x: x["revenue"], reverse=True)
    return results


def build_product_detail(session: Session, product_key: str, days: int = 0) -> dict | None:
    """Fetch detailed buyer breakdown and sales timeline for a single product."""
    orders = load_paid_tiktok_orders(session, days=days)
    buyer_agg: dict[str, dict] = {}
    sales_timeline: list[dict] = []
    total_qty = 0
    total_revenue = 0.0

    for o in orders:
        bkey, bname = _buyer_key(o)
        for raw in parse_line_items(o):
            ni = normalize_item(raw)
            key = ni["product_id"] or ni["title_key"]
            if key != product_key:
                continue
            qty = ni["qty"]
            revenue = ni["price"] * qty
            total_qty += qty
            total_revenue += revenue

            if bkey not in buyer_agg:
                buyer_agg[bkey] = {"name": bname, "qty": 0, "revenue": 0.0, "orders": 0}
            buyer_agg[bkey]["qty"] += qty
            buyer_agg[bkey]["revenue"] += revenue
            buyer_agg[bkey]["orders"] += 1

            sales_timeline.append({
                "order_number": o.order_number,
                "buyer": bname,
                "created_at": o.created_at.isoformat() if o.created_at else None,
                "qty": qty,
                "revenue": revenue,
            })

    if not sales_timeline:
        return None

    sales_timeline.sort(key=lambda x: x["created_at"] or "", reverse=True)
    top_buyers = sorted(buyer_agg.values(), key=lambda x: x["revenue"], reverse=True)

    return {
        "product_key": product_key,
        "recent_sales": sales_timeline[:50],
        "top_buyers": top_buyers[:20],
        "total_qty": total_qty,
        "total_revenue": round(total_revenue, 2),
    }
