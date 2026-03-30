from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Optional

from sqlmodel import Session, select

from .models import (
    BookkeepingEntry,
    DiscordMessage,
    Transaction,
    TransactionItem,
    normalize_money_value,
    signed_money_delta,
    utcnow,
)


def is_transaction_message(row: DiscordMessage) -> bool:
    if row.is_deleted:
        return False
    if row.parse_status in {"ignored", "queued", "processing", "failed"}:
        return False
    if row.stitched_group_id and not row.stitched_primary:
        return False
    return True


def _safe_json_list(value: Optional[str]) -> list[str]:
    try:
        loaded = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in loaded:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned


def sync_transaction_from_message(session: Session, row: DiscordMessage) -> Optional[Transaction]:
    existing = session.exec(
        select(Transaction).where(Transaction.source_message_id == row.id)
    ).first()

    if not is_transaction_message(row):
        if existing:
            bookkeeping_rows = session.exec(
                select(BookkeepingEntry).where(BookkeepingEntry.matched_transaction_id == existing.id)
            ).all()
            for bookkeeping_row in bookkeeping_rows:
                bookkeeping_row.matched_transaction_id = None
                bookkeeping_row.match_status = "unmatched"
                session.add(bookkeeping_row)

            items = session.exec(
                select(TransactionItem).where(TransactionItem.transaction_id == existing.id)
            ).all()
            for item in items:
                session.delete(item)
            session.flush()
            session.delete(existing)
        return None

    if existing is None:
        transaction = Transaction(
            source_message_id=row.id,
            occurred_at=row.created_at,
        )
    else:
        transaction = existing

    transaction.discord_message_id = row.discord_message_id
    transaction.guild_id = row.guild_id
    transaction.channel_id = row.channel_id
    transaction.channel_name = row.channel_name
    transaction.author_name = row.author_name
    transaction.occurred_at = row.created_at
    transaction.parse_status = row.parse_status
    transaction.deal_type = row.deal_type
    transaction.entry_kind = row.entry_kind
    transaction.payment_method = row.payment_method
    transaction.cash_direction = row.cash_direction
    transaction.category = row.category
    transaction.expense_category = row.expense_category
    normalized_money_in = normalize_money_value(row.money_in)
    normalized_money_out = normalize_money_value(row.money_out)
    normalized_amount = row.amount
    if normalized_amount is None:
        inferred_amount = max(normalized_money_in, normalized_money_out)
        normalized_amount = inferred_amount or None

    transaction.amount = normalized_amount
    transaction.money_in = normalized_money_in
    transaction.money_out = normalized_money_out
    transaction.needs_review = row.needs_review
    transaction.confidence = row.confidence
    transaction.notes = row.notes
    transaction.trade_summary = row.trade_summary
    transaction.source_content = row.content or ""
    transaction.is_deleted = row.is_deleted
    transaction.updated_at = utcnow()

    session.add(transaction)
    session.flush()

    existing_items = session.exec(
        select(TransactionItem).where(TransactionItem.transaction_id == transaction.id)
    ).all()
    for item in existing_items:
        session.delete(item)
    session.flush()

    item_names = _safe_json_list(row.item_names_json)
    items_in = _safe_json_list(row.items_in_json)
    items_out = _safe_json_list(row.items_out_json)

    for item_name in item_names:
        session.add(
            TransactionItem(transaction_id=transaction.id, direction="named", item_name=item_name)
        )
    for item_name in items_in:
        session.add(
            TransactionItem(transaction_id=transaction.id, direction="in", item_name=item_name)
        )
    for item_name in items_out:
        session.add(
            TransactionItem(transaction_id=transaction.id, direction="out", item_name=item_name)
        )

    return transaction


def rebuild_transactions(session: Session) -> int:
    rows = session.exec(select(DiscordMessage)).all()
    synced = 0
    for row in rows:
        transaction = sync_transaction_from_message(session, row)
        if transaction:
            synced += 1
    session.commit()
    return synced


def transaction_base_query(
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
):
    stmt = select(Transaction).where(Transaction.is_deleted == False)
    stmt = stmt.where(Transaction.parse_status.in_(["parsed", "needs_review"]))

    if start:
        stmt = stmt.where(Transaction.occurred_at >= start)
    if end:
        stmt = stmt.where(Transaction.occurred_at <= end)
    if channel_id:
        stmt = stmt.where(Transaction.channel_id == channel_id)
    if entry_kind:
        stmt = stmt.where(Transaction.entry_kind == entry_kind)

    return stmt.order_by(Transaction.occurred_at)


def get_transactions(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
) -> list[Transaction]:
    return session.exec(
        transaction_base_query(start=start, end=end, channel_id=channel_id, entry_kind=entry_kind)
    ).all()


def build_transaction_summary(rows: list[Transaction]) -> dict:
    totals = defaultdict(float)
    counts = defaultdict(int)
    expense_categories = defaultdict(float)
    channels = defaultdict(float)
    channels_money_in = defaultdict(float)
    channels_money_out = defaultdict(float)
    payment_methods = defaultdict(float)
    categories = defaultdict(float)
    timeline = defaultdict(lambda: defaultdict(float))

    for row in rows:
        money_in = normalize_money_value(row.money_in)
        money_out = normalize_money_value(row.money_out)
        entry_kind = row.entry_kind or "unknown"
        day_key = row.occurred_at.date().isoformat()
        net_value = signed_money_delta(money_in, money_out)
        reporting_amount = row.amount
        if reporting_amount is None:
            reporting_amount = money_in or money_out or 0.0

        totals["money_in"] += money_in
        totals["money_out"] += money_out
        totals["net"] += net_value
        counts[entry_kind] += 1
        if row.needs_review:
            counts["needs_review"] += 1
        if entry_kind == "unknown":
            counts["unknown"] += 1

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
            categories[row.category] += normalize_money_value(reporting_amount)
        if row.payment_method:
            payment_methods[row.payment_method] += normalize_money_value(reporting_amount)
        if row.channel_name or row.channel_id:
            channel_key = row.channel_name or row.channel_id or "unknown"
            channels[channel_key] += net_value
            channels_money_in[channel_key] += money_in
            channels_money_out[channel_key] += money_out

    totals["gross_margin"] = totals["sales"] - totals["buys"]
    totals["inventory_spend"] = expense_categories.get("inventory", 0.0)

    return {
        "totals": {key: round(value, 2) for key, value in totals.items()},
        "counts": dict(counts),
        "expense_categories": {
            key: round(value, 2)
            for key, value in sorted(expense_categories.items(), key=lambda item: (-item[1], item[0]))
        },
        "channel_net": {
            key: round(value, 2)
            for key, value in sorted(channels.items(), key=lambda item: (-item[1], item[0]))
        },
        "channel_detail": [
            {
                "channel": key,
                "money_in": round(channels_money_in[key], 2),
                "money_out": round(channels_money_out[key], 2),
                "net": round(channels[key], 2),
            }
            for key in sorted(channels.keys(), key=lambda value: (-channels[value], value))
        ],
        "payment_methods": {
            key: round(value, 2)
            for key, value in sorted(payment_methods.items(), key=lambda item: (-item[1], item[0]))
        },
        "categories": {
            key: round(value, 2)
            for key, value in sorted(categories.items(), key=lambda item: (-item[1], item[0]))
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
