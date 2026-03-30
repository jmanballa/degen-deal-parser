from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Session, select

from .models import DiscordMessage, normalize_money_value, signed_money_delta


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
    stmt = stmt.where(DiscordMessage.parse_status.in_(["parsed", "needs_review"]))
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
