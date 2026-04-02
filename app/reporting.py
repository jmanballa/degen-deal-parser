from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlmodel import Session, select
from zoneinfo import ZoneInfo

from .models import (
    DiscordMessage,
    expand_parse_status_filter_values,
    PARSE_PARSED,
    PARSE_REVIEW_REQUIRED,
    ShopifyOrder,
    normalize_money_value,
    signed_money_delta,
)


REPORTING_TZ = ZoneInfo("America/Los_Angeles")


def parse_report_datetime(value: Optional[str], *, end_of_day: bool = False) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    dt = datetime.fromisoformat(value)
    if len(value) == 10:
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

    if dt.tzinfo is None:
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


def _safe_json_list(value: Optional[str]) -> list[dict[str, object]]:
    try:
        loaded = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def build_shopify_line_item_summary(rows: list[ShopifyOrder]) -> dict:
    total_orders_with_items = 0
    total_line_items = 0
    title_counts: defaultdict[str, int] = defaultdict(int)

    for row in rows:
        summaries = _safe_json_list(
            row.line_items_summary_json if row.line_items_summary_json != "[]" else row.line_items_json
        )
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
