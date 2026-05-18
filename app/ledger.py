from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

from sqlmodel import Session, select

from .ai_client import get_ai_client, get_fast_model, has_ai_key
from .bank_reconciliation import (
    ATTENTION_CLASSIFICATIONS,
    HIGH_CONFIDENCE_CLASSIFICATIONS,
    all_expense_category_choices,
    bank_payload_discord_match_block_reason,
    base_classification,
    categorize_bank_payload,
    classification_label,
    classification_confidence,
    expense_category_label,
    transaction_bank_match_block_reason,
)
from .models import BankTransaction, LedgerRule, Transaction, utcnow


LEDGER_STATUS_LABELS = {
    "needs_action": "Needs Action",
    "reconciled": "Ready",
    "force_unmatched": "Forced Unmatched",
    "ignored": "Ignored",
    "cash": "Cash",
}

LEDGER_SOURCE_LABELS = {
    "bank": "Bank",
    "discord": "Discord",
    "shopify": "Shopify",
    "tiktok": "TikTok",
    "processor": "Processor",
    "paypal": "PayPal",
    "cash": "Cash",
}

LEDGER_ACTION_REASON_LABELS = {
    "possible_discord_match": "Check Discord match",
    "needs_match_check": "Needs match check",
    "needs_log_check": "Needs log check",
    "needs_source": "Needs source",
    "needs_category": "Needs category",
    "expense_review": "Expense review",
    "credit_review": "Credit review",
    "payout_review": "Payout review",
    "cash_only": "Cash only",
}

RULE_ALLOWED_REVIEW_STATUSES = {"open", "reviewed", "ignored"}
RULE_ALLOWED_MATCH_OVERRIDES = {"force_unmatched", "clear", "none", ""}

LEDGER_AUTOMATION_ACTIONS = {
    "mark_needs_log_checked": {
        "label": "Mark log-check rows reviewed",
        "description": "For incoming customer payments already checked against the Discord/bookkeeping log.",
        "review_note": "Log checked from ledger automation workbench",
    },
}

LEDGER_AGENT_AUTO_REVIEW_CATEGORIES = {
    "bank_fees",
    "grading_fees",
    "meals_entertainment",
    "payroll",
    "rent_facilities",
    "shipping_postage",
    "show_fees",
    "software_subscriptions",
    "supplies_packaging",
    "taxes_licenses",
    "travel_airfare",
    "travel_ground_transport",
    "travel_lodging",
}

LEDGER_AGENT_REVIEW_CONFIDENCES = {"high", "manual", "rule"}


@dataclass
class LedgerFilters:
    account: str = ""
    start: str = ""
    end: str = ""
    status: str = "needs_action"
    category: str = ""
    source: str = ""
    action_reason: str = ""
    search: str = ""
    sort: str = "posted_at"
    direction: str = "desc"
    limit: int = 250
    include_cash: bool = False


def _as_dict(value: str | dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _date_value(value: Optional[datetime]) -> Optional[date]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.date()


def _parse_date(value: str | None) -> Optional[date]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _money(value: float | int | None) -> float:
    return round(float(value or 0.0), 2)


def format_ledger_money(value: float | int | None) -> str:
    amount = _money(value)
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.2f}"


def _short_date(value: Optional[datetime]) -> str:
    parsed = _date_value(value)
    return parsed.isoformat() if parsed else ""


def _contains_any(text: str, tokens: Iterable[str]) -> bool:
    lowered = text.lower()
    return any(token and token.lower() in lowered for token in tokens)


def _payment_rail_for_text(text: str) -> str:
    lowered = (text or "").lower()
    if _contains_any(lowered, ("apple cash", "apple pay", "applepay")):
        return "apple_cash"
    if "zelle" in lowered:
        return "zelle"
    if "venmo" in lowered:
        return "venmo"
    if "paypal" in lowered:
        return "paypal"
    if _contains_any(lowered, ("cash app", "cashapp", "sq *cash", "square cash")):
        return "cash_app"
    if "shopify" in lowered:
        return "shopify"
    if "tiktok" in lowered:
        return "tiktok"
    return ""


def ledger_source_for_bank_row(row: BankTransaction) -> str:
    platform = (row.matched_platform or "").strip().lower()
    if platform in {"discord", "shopify", "tiktok", "paypal"}:
        return platform
    classification = (row.classification or "").strip().lower()
    if classification.startswith("shopify_"):
        return "shopify"
    if classification.startswith("tiktok_"):
        return "tiktok"
    if classification.startswith("paypal_"):
        return "paypal"
    if classification in {"processor_payout"}:
        return "processor"
    if classification in {"logged_in_discord_strong", "logged_in_discord_possible"}:
        return "discord"
    return "bank"


def ledger_status_for_bank_row(row: BankTransaction) -> str:
    if (row.review_status or "") == "ignored":
        return "ignored"
    if (row.match_override_status or "") == "force_unmatched":
        return "force_unmatched"
    if (row.review_status or "") == "reviewed":
        return "reconciled"
    category = row.expense_category or "uncategorized"
    classification = row.classification or ""
    if classification in ATTENTION_CLASSIFICATIONS:
        return "needs_action"
    if _money(row.amount) < 0 and category == "uncategorized":
        return "needs_action"
    if classification in HIGH_CONFIDENCE_CLASSIFICATIONS or classification == "logged_in_discord_strong":
        return "reconciled"
    if row.matched_transaction_id and row.matched_platform == "discord":
        return "reconciled"
    return "needs_action"


def ledger_action_reason_for_bank_row(row: BankTransaction) -> str:
    if ledger_status_for_bank_row(row) != "needs_action":
        return ""
    classification = (row.classification or "").strip()
    category = (row.expense_category or "uncategorized").strip() or "uncategorized"
    if classification == "logged_in_discord_possible":
        return "possible_discord_match"
    if classification == "direct_payment_out_needs_log_check":
        return "needs_match_check"
    if classification == "direct_customer_payment_needs_log_check":
        return "needs_log_check"
    if classification == "cash_deposit_needs_source":
        return "needs_source"
    if classification == "transfer_or_possible_processor_sweep":
        return "payout_review"
    if classification == "credit_needs_review":
        return "credit_review"
    if classification == "expense_or_purchase_needs_review":
        return "expense_review"
    if _money(row.amount) < 0 and category == "uncategorized":
        return "needs_category"
    if classification in ATTENTION_CLASSIFICATIONS:
        return "expense_review"
    return "needs_category"


def _bank_row_view(row: BankTransaction, matched: Optional[Transaction] = None) -> dict[str, Any]:
    source = ledger_source_for_bank_row(row)
    status = ledger_status_for_bank_row(row)
    action_reason = ledger_action_reason_for_bank_row(row)
    category = row.expense_category or "uncategorized"
    return {
        "row_kind": "bank",
        "id": row.id,
        "row_index": row.row_index,
        "posted_at": row.posted_at,
        "posted_at_display": _short_date(row.posted_at),
        "account_label": row.account_label,
        "account_type": row.account_type,
        "description": row.description,
        "details": row.details or "",
        "amount": _money(row.amount),
        "amount_display": format_ledger_money(row.amount),
        "classification": row.classification,
        "classification_label": classification_label(row.classification or ""),
        "confidence": row.confidence,
        "expense_category": category,
        "expense_category_label": expense_category_label(category),
        "expense_subcategory": row.expense_subcategory or "",
        "category_confidence": row.category_confidence,
        "category_reason": row.category_reason or "",
        "review_status": row.review_status or "open",
        "review_note": row.review_note or "",
        "match_reason": row.match_reason or "",
        "match_override_status": row.match_override_status or "",
        "match_override_note": row.match_override_note or "",
        "matched_transaction_id": row.matched_transaction_id,
        "matched_source_message_id": row.matched_source_message_id,
        "matched_platform": row.matched_platform or "",
        "ledger_status": status,
        "ledger_status_label": LEDGER_STATUS_LABELS.get(status, status.replace("_", " ").title()),
        "action_reason": action_reason,
        "action_reason_label": LEDGER_ACTION_REASON_LABELS.get(action_reason, ""),
        "source": source,
        "source_label": LEDGER_SOURCE_LABELS.get(source, source.title()),
        "payment_rail": _payment_rail_for_text(" ".join([row.description or "", row.details or "", row.raw_row_json or ""])),
        "matched_transaction": {
            "id": matched.id,
            "source_message_id": matched.source_message_id,
            "occurred_at_display": _short_date(matched.occurred_at),
            "entry_kind": matched.entry_kind,
            "payment_method": matched.payment_method,
            "expense_category": matched.expense_category or matched.category,
            "amount": _money(matched.amount if matched.amount is not None else (matched.money_in or matched.money_out)),
            "source_content": matched.source_content,
        }
        if matched
        else None,
    }


def _cash_transaction_amount(tx: Transaction) -> float:
    if tx.money_out:
        return _money(tx.money_out)
    if tx.money_in:
        return _money(tx.money_in)
    return abs(_money(tx.amount))


def _cash_transaction_signed_amount(tx: Transaction) -> float:
    if tx.money_in or tx.money_out:
        return _money((tx.money_in or 0.0) - (tx.money_out or 0.0))
    amount = abs(_money(tx.amount))
    if (tx.entry_kind or "").lower() in {"buy", "expense", "trade"}:
        return -amount
    return amount


def _unbanked_cash_view(tx: Transaction) -> dict[str, Any]:
    amount = _cash_transaction_amount(tx)
    return {
        "transaction_id": tx.id,
        "source_message_id": tx.source_message_id,
        "occurred_at": tx.occurred_at,
        "occurred_at_display": _short_date(tx.occurred_at),
        "entry_kind": tx.entry_kind or "",
        "payment_method": tx.payment_method or "",
        "expense_category": tx.expense_category or tx.category or "uncategorized",
        "expense_category_label": expense_category_label(tx.expense_category or tx.category or "uncategorized"),
        "amount": amount,
        "amount_display": format_ledger_money(amount),
        "source_content": tx.source_content,
    }


def _cash_row_view(tx: Transaction) -> dict[str, Any]:
    category = tx.expense_category or tx.category or "uncategorized"
    signed_amount = _cash_transaction_signed_amount(tx)
    return {
        "row_kind": "cash",
        "id": f"cash-{tx.id}",
        "transaction_id": tx.id,
        "row_index": 0,
        "posted_at": tx.occurred_at,
        "posted_at_display": _short_date(tx.occurred_at),
        "account_label": "Cash",
        "account_type": "cash",
        "description": tx.source_content or "Discord cash deal",
        "details": "",
        "amount": signed_amount,
        "amount_display": format_ledger_money(signed_amount),
        "classification": "unbanked_cash",
        "classification_label": "Unbanked cash",
        "confidence": "high",
        "expense_category": category,
        "expense_category_label": expense_category_label(category),
        "expense_subcategory": "",
        "category_confidence": "discord",
        "category_reason": "Cash deal logged in Discord without a matching bank row.",
        "review_status": "cash",
        "review_note": "",
        "match_reason": "Cash transaction is shown in-grid by request; it does not affect bank totals.",
        "match_override_status": "",
        "match_override_note": "",
        "matched_transaction_id": tx.id,
        "matched_source_message_id": tx.source_message_id,
        "matched_platform": "discord",
        "ledger_status": "cash",
        "ledger_status_label": "Cash",
        "action_reason": "cash_only",
        "action_reason_label": LEDGER_ACTION_REASON_LABELS["cash_only"],
        "source": "cash",
        "source_label": "Cash",
        "payment_rail": "cash",
        "matched_transaction": {
            "id": tx.id,
            "source_message_id": tx.source_message_id,
            "occurred_at_display": _short_date(tx.occurred_at),
            "entry_kind": tx.entry_kind,
            "payment_method": tx.payment_method,
            "expense_category": category,
            "amount": _cash_transaction_amount(tx),
            "source_content": tx.source_content,
        },
    }


def _cash_row_matches_filters(row: dict[str, Any], filters: LedgerFilters) -> bool:
    source = (filters.source or "").strip()
    if source and source != "cash":
        return False
    status = (filters.status or "").strip() or "needs_action"
    if status not in {"all", "any", "needs_action", "cash"}:
        return False
    start = _parse_date(filters.start)
    end = _parse_date(filters.end)
    posted = _date_value(row.get("posted_at"))
    if start and (posted is None or posted < start):
        return False
    if end and (posted is None or posted > end):
        return False
    category = (filters.category or "").strip()
    if category and category != (row.get("expense_category") or "uncategorized"):
        return False
    action_reason = (getattr(filters, "action_reason", "") or "").strip()
    if action_reason and action_reason != (row.get("action_reason") or ""):
        return False
    account = (filters.account or "").strip().lower()
    if account and account not in {"all", "any", "cash"}:
        return False
    search = (filters.search or "").strip().lower()
    if search:
        haystack = " ".join(
            [
                str(row.get("description") or ""),
                str(row.get("expense_category") or ""),
                str(row.get("source_label") or ""),
                str(row.get("match_reason") or ""),
            ]
        ).lower()
        if search not in haystack:
            return False
    return True


def _row_matches_filters(row: BankTransaction, filters: LedgerFilters) -> bool:
    if row.is_removed:
        return False
    account = (filters.account or "").strip().lower()
    if account and account not in {"all", "any"}:
        account_text = " ".join([row.account_label or "", row.account_type or ""]).lower()
        if account not in account_text:
            return False
    start = _parse_date(filters.start)
    end = _parse_date(filters.end)
    posted = _date_value(row.posted_at)
    if start and (posted is None or posted < start):
        return False
    if end and (posted is None or posted > end):
        return False
    category = (filters.category or "").strip()
    if category and category != (row.expense_category or "uncategorized"):
        return False
    source = (filters.source or "").strip()
    if source and source != ledger_source_for_bank_row(row):
        return False
    status = (filters.status or "").strip() or "needs_action"
    if status not in {"all", "any"} and status != ledger_status_for_bank_row(row):
        return False
    action_reason = (getattr(filters, "action_reason", "") or "").strip()
    if action_reason and action_reason != ledger_action_reason_for_bank_row(row):
        return False
    search = (filters.search or "").strip().lower()
    if search:
        haystack = " ".join(
            [
                row.description or "",
                row.details or "",
                row.account_label or "",
                row.classification or "",
                row.expense_category or "",
                row.category_reason or "",
                row.match_reason or "",
                row.review_note or "",
            ]
        ).lower()
        if search not in haystack:
            return False
    return True


def _sort_rows(rows: list[BankTransaction], sort: str, direction: str) -> list[BankTransaction]:
    reverse = (direction or "desc").lower() == "desc"
    sort_key = (sort or "posted_at").strip()

    def key(row: BankTransaction) -> tuple[Any, int]:
        if sort_key == "amount":
            value: Any = _money(row.amount)
        elif sort_key == "account":
            value = (row.account_label or "").lower()
        elif sort_key == "status":
            value = ledger_status_for_bank_row(row)
        elif sort_key == "category":
            value = (row.expense_category or "").lower()
        elif sort_key == "source":
            value = ledger_source_for_bank_row(row)
        else:
            value = row.posted_at or datetime.min.replace(tzinfo=timezone.utc)
        return value, row.id or 0

    return sorted(rows, key=key, reverse=reverse)


def _sort_row_views(rows: list[dict[str, Any]], sort: str, direction: str) -> list[dict[str, Any]]:
    reverse = (direction or "desc").lower() == "desc"
    sort_key = (sort or "posted_at").strip()

    def key(row: dict[str, Any]) -> tuple[Any, str]:
        if sort_key == "amount":
            value: Any = _money(row.get("amount"))
        elif sort_key == "account":
            value = str(row.get("account_label") or "").lower()
        elif sort_key == "status":
            value = str(row.get("ledger_status") or "")
        elif sort_key == "category":
            value = str(row.get("expense_category") or "").lower()
        elif sort_key == "source":
            value = str(row.get("source") or "")
        else:
            value = row.get("posted_at") or datetime.min.replace(tzinfo=timezone.utc)
        return value, str(row.get("id") or "")

    return sorted(rows, key=key, reverse=reverse)


def _load_bank_rows(session: Session) -> list[BankTransaction]:
    return list(session.exec(select(BankTransaction).where(BankTransaction.is_removed == False)).all())  # noqa: E712


def _matched_transactions_by_id(session: Session, rows: list[BankTransaction]) -> dict[int, Transaction]:
    ids = sorted({int(row.matched_transaction_id) for row in rows if row.matched_transaction_id})
    if not ids:
        return {}
    return {row.id: row for row in session.exec(select(Transaction).where(Transaction.id.in_(ids))).all() if row.id is not None}


def _bank_row_payload(row: BankTransaction) -> dict[str, Any]:
    return {
        "description": row.description or "",
        "raw_type": row.raw_type or "",
        "details": row.details or "",
        "check_or_slip": row.check_or_slip or "",
        "amount": row.amount or 0.0,
        "classification": row.classification or "",
        "raw_row_json": row.raw_row_json or "{}",
    }


def _append_review_note(existing: str | None, note: str) -> str:
    existing = (existing or "").strip()
    return f"{existing}\n{note}" if existing else note


def _automation_matching_rows(
    session: Session,
    *,
    action_key: str,
    filters: Optional[LedgerFilters] = None,
) -> list[BankTransaction]:
    selected = filters or LedgerFilters(status="needs_action")
    if action_key != "mark_needs_log_checked":
        raise ValueError("Unknown ledger automation action")
    return [
        row
        for row in _load_bank_rows(session)
        if _row_matches_filters(row, selected)
        and ledger_action_reason_for_bank_row(row) == "needs_log_check"
    ]


def preview_ledger_automation(
    session: Session,
    *,
    action_key: str,
    filters: Optional[LedgerFilters] = None,
    sample_limit: int = 8,
) -> dict[str, Any]:
    rows = _automation_matching_rows(session, action_key=action_key, filters=filters)
    action = LEDGER_AUTOMATION_ACTIONS[action_key]
    count = len(rows)
    return {
        "action_key": action_key,
        "label": action["label"],
        "description": action["description"],
        "affected_count": count,
        "sample_rows": [_preview_sample_view(row) for row in _sort_rows(rows, "posted_at", "desc")[:sample_limit]],
        "summary": f"Mark {count} needs-log-check row(s) reviewed after log verification.",
        "warnings": [] if count else ["No needs-log-check rows match the current filters."],
    }


def apply_ledger_automation(
    session: Session,
    *,
    action_key: str,
    filters: Optional[LedgerFilters] = None,
    applied_by: str = "",
) -> dict[str, Any]:
    rows = _automation_matching_rows(session, action_key=action_key, filters=filters)
    action = LEDGER_AUTOMATION_ACTIONS[action_key]
    now = utcnow()
    updated = 0
    note = action["review_note"]
    if applied_by:
        note = f"{note} by {applied_by}."
    else:
        note = f"{note}."
    for row in rows:
        changed = False
        if row.review_status != "reviewed":
            row.review_status = "reviewed"
            changed = True
        if note not in (row.review_note or ""):
            row.review_note = _append_review_note(row.review_note, note)
            changed = True
        if changed:
            row.updated_at = now
            session.add(row)
            updated += 1
    session.commit()
    return {"action_key": action_key, "matched_count": len(rows), "updated_count": updated}


def _set_category_from_payload(row: BankTransaction, payload: dict[str, str]) -> None:
    row.expense_category = payload.get("expense_category") or row.expense_category or "uncategorized"
    row.expense_subcategory = payload.get("expense_subcategory") or None
    row.category_confidence = payload.get("category_confidence") or row.category_confidence or "low"
    row.category_reason = payload.get("category_reason") or row.category_reason or ""


def run_ledger_review_agent(
    session: Session,
    *,
    filters: Optional[LedgerFilters] = None,
    limit: int = 250,
    applied_by: str = "Ledger agent",
) -> dict[str, Any]:
    selected = filters or LedgerFilters(status="needs_action")
    rows = [
        row
        for row in _load_bank_rows(session)
        if ledger_status_for_bank_row(row) == "needs_action" and _row_matches_filters(row, selected)
    ][: max(min(int(limit or 250), 1000), 1)]
    matched_by_id = _matched_transactions_by_id(session, rows)
    result = {
        "scanned_count": len(rows),
        "updated_count": 0,
        "cleared_false_matches": 0,
        "auto_reviewed": 0,
        "left_open": 0,
        "sample_actions": [],
    }
    now = utcnow()
    for row in rows:
        changed = False
        actions: list[str] = []
        matched = matched_by_id.get(row.matched_transaction_id or -1)
        bank_reason = bank_payload_discord_match_block_reason(_bank_row_payload(row))
        transaction_reason = transaction_bank_match_block_reason(matched) if matched else ""

        if row.matched_platform == "discord" and (bank_reason or transaction_reason):
            reason = transaction_reason or bank_reason
            classification = base_classification(row.description or "", _money(row.amount))
            row.classification = classification
            row.confidence = classification_confidence(classification)
            row.matched_transaction_id = None
            row.matched_source_message_id = None
            row.matched_platform = None
            row.match_reason = f"Ledger agent cleared Discord match: {reason}"
            if (row.category_confidence or "").lower() not in {"manual", "rule"}:
                category_payload = _bank_row_payload(row)
                category_payload["classification"] = classification
                _set_category_from_payload(row, categorize_bank_payload(category_payload))
            changed = True
            result["cleared_false_matches"] += 1
            actions.append("cleared false Discord match")

        safe_category = row.expense_category in LEDGER_AGENT_AUTO_REVIEW_CATEGORIES
        safe_confidence = (row.category_confidence or "").lower() in LEDGER_AGENT_REVIEW_CONFIDENCES
        if row.review_status == "open" and not row.matched_transaction_id and safe_category and safe_confidence:
            row.review_status = "reviewed"
            row.review_note = _append_review_note(
                row.review_note,
                f"Auto-reviewed by {applied_by}: {expense_category_label(row.expense_category)} with {row.category_confidence} confidence.",
            )
            changed = True
            result["auto_reviewed"] += 1
            actions.append("auto-reviewed safe operating expense")

        if changed:
            row.updated_at = now
            session.add(row)
            result["updated_count"] += 1
            if len(result["sample_actions"]) < 10:
                result["sample_actions"].append(
                    {
                        "id": row.id,
                        "description": row.description,
                        "actions": actions,
                        "category": row.expense_category,
                        "review_status": row.review_status,
                    }
                )
        else:
            result["left_open"] += 1

    if result["updated_count"]:
        session.commit()
    return result


def _is_cash_payment_transaction(tx: Transaction) -> bool:
    payment = (tx.payment_method or "").strip().lower().replace(" ", "_").replace("-", "_")
    if payment in {"cash_app", "cashapp", "apple_cash", "applepay", "apple_pay"}:
        return False
    if payment == "cash":
        return True
    content = (tx.source_content or "").lower()
    if _payment_rail_for_text(content) in {"apple_cash", "cash_app"}:
        return False
    return bool(re.search(r"\bcash\b", content))


def _load_unbanked_cash_transactions(session: Session) -> list[Transaction]:
    matched_ids = {
        int(matched_id)
        for matched_id in session.exec(
            select(BankTransaction.matched_transaction_id)
            .where(BankTransaction.is_removed == False)  # noqa: E712
            .where(BankTransaction.matched_transaction_id.is_not(None))
        ).all()
        if matched_id is not None
    }
    transactions = list(
        session.exec(
            select(Transaction)
            .where(Transaction.is_deleted == False)  # noqa: E712
            .order_by(Transaction.occurred_at.desc(), Transaction.id.desc())
        ).all()
    )
    cash_rows: list[Transaction] = []
    for tx in transactions:
        if tx.id in matched_ids:
            continue
        if _is_cash_payment_transaction(tx):
            if _cash_transaction_amount(tx) > 0:
                cash_rows.append(tx)
    return cash_rows


def build_ledger_page_data(session: Session, filters: Optional[LedgerFilters] = None) -> dict[str, Any]:
    selected = filters or LedgerFilters()
    all_rows = _load_bank_rows(session)
    filtered = [row for row in all_rows if _row_matches_filters(row, selected)]
    matched_by_id = _matched_transactions_by_id(session, filtered)
    row_views = [_bank_row_view(row, matched_by_id.get(row.matched_transaction_id or -1)) for row in filtered]
    unbanked_cash_transactions = _load_unbanked_cash_transactions(session)
    unbanked_cash = [_unbanked_cash_view(tx) for tx in unbanked_cash_transactions]
    if selected.include_cash or (selected.source or "").strip() == "cash":
        cash_row_views = [
            row
            for row in (_cash_row_view(tx) for tx in unbanked_cash_transactions)
            if _cash_row_matches_filters(row, selected)
        ]
        row_views.extend(cash_row_views)
    sorted_row_views = _sort_row_views(row_views, selected.sort, selected.direction)
    visible_row_views = sorted_row_views[: max(int(selected.limit or 250), 1)]
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for row in all_rows:
        status_counts[ledger_status_for_bank_row(row)] = status_counts.get(ledger_status_for_bank_row(row), 0) + 1
        source_counts[ledger_source_for_bank_row(row)] = source_counts.get(ledger_source_for_bank_row(row), 0) + 1
    bank_net = round(sum(_money(row.amount) for row in all_rows), 2)
    bank_inflow = round(sum(_money(row.amount) for row in all_rows if _money(row.amount) > 0), 2)
    bank_outflow = round(sum(_money(row.amount) for row in all_rows if _money(row.amount) < 0), 2)
    account_options = sorted({row.account_label for row in all_rows if row.account_label})
    recent_rules = list(
        session.exec(
            select(LedgerRule).order_by(LedgerRule.updated_at.desc(), LedgerRule.id.desc()).limit(20)
        ).all()
    )
    return {
        "rows": visible_row_views,
        "unbanked_cash_rows": unbanked_cash,
        "rules": recent_rules,
        "summary": {
            "bank_row_count": len(all_rows),
            "filtered_row_count": len(sorted_row_views),
            "hidden_row_count": max(len(sorted_row_views) - len(visible_row_views), 0),
            "bank_inflow_total": bank_inflow,
            "bank_outflow_total": bank_outflow,
            "bank_net_total": bank_net,
            "bank_inflow_display": format_ledger_money(bank_inflow),
            "bank_outflow_display": format_ledger_money(bank_outflow),
            "bank_net_display": format_ledger_money(bank_net),
            "needs_action_count": status_counts.get("needs_action", 0),
            "reconciled_count": status_counts.get("reconciled", 0),
            "force_unmatched_count": status_counts.get("force_unmatched", 0),
            "unbanked_cash_count": len(unbanked_cash),
            "unbanked_cash_total": round(sum(row["amount"] for row in unbanked_cash), 2),
            "unbanked_cash_display": format_ledger_money(sum(row["amount"] for row in unbanked_cash)),
        },
        "status_counts": status_counts,
        "source_counts": source_counts,
        "account_options": account_options,
        "category_choices": all_expense_category_choices(),
        "source_choices": [{"value": key, "label": value} for key, value in LEDGER_SOURCE_LABELS.items()],
        "status_choices": [{"value": key, "label": value} for key, value in LEDGER_STATUS_LABELS.items()],
        "action_reason_choices": [
            {"value": key, "label": value}
            for key, value in LEDGER_ACTION_REASON_LABELS.items()
            if key != "cash_only"
        ],
        "selected": selected,
    }


def _normalize_category_from_text(text: str) -> str:
    lowered = text.lower()
    if _contains_any(lowered, ("inventory", "card buy", "buy cards", "purchase cards")):
        return "inventory_purchases"
    if _contains_any(lowered, ("grading", "psa", "bgs", "cgc", "sgc")):
        return "grading_fees"
    if _contains_any(lowered, ("shipping", "postage", "shippo", "pirateship", "ups", "usps", "fedex")):
        return "shipping_postage"
    if _contains_any(lowered, ("partner", "payback", "reimburse")):
        return "partner_paybacks"
    if _contains_any(lowered, ("transfer", "credit card payment", "card payment")):
        return "transfers"
    if _contains_any(lowered, ("shopify", "tiktok", "payout", "stripe", "processor")):
        return "platform_payouts"
    if _contains_any(lowered, ("sale", "sales collection", "customer payment")):
        return "sales_collections"
    if _contains_any(lowered, ("bank fee", "interest", "finance charge")):
        return "bank_fees"
    return ""


def draft_ledger_rule_from_instruction(instruction: str) -> dict[str, Any]:
    text = (instruction or "").strip()
    lowered = text.lower()
    conditions: dict[str, Any] = {}
    actions: dict[str, Any] = {}
    warnings: list[str] = []
    quoted = re.findall(r"['\"]([^'\"]{3,80})['\"]", text)
    if quoted:
        conditions["description_contains"] = quoted[0].strip().lower()
    elif "apple cash" in lowered or "apple pay" in lowered:
        conditions["description_contains"] = "apple cash"
        conditions["payment_rail"] = "apple_cash"
    elif "zelle" in lowered:
        conditions["description_contains"] = "zelle"
        conditions["payment_rail"] = "zelle"
    elif "venmo" in lowered:
        conditions["description_contains"] = "venmo"
        conditions["payment_rail"] = "venmo"
    elif "paypal" in lowered:
        conditions["description_contains"] = "paypal"
        conditions["payment_rail"] = "paypal"
    elif "cash app" in lowered:
        conditions["description_contains"] = "cash app"
        conditions["payment_rail"] = "cash_app"
    elif "shopify" in lowered:
        conditions["description_contains"] = "shopify"
        conditions["provider"] = "shopify"
    elif "tiktok" in lowered:
        conditions["description_contains"] = "tiktok"
        conditions["provider"] = "tiktok"

    if _contains_any(lowered, ("sent", "outflow", "debit", "expense", "purchase", "paid", "payment to")):
        conditions["amount_sign"] = "debit"
    elif _contains_any(lowered, ("deposit", "credit", "incoming", "payment from", "received", "payout")):
        conditions["amount_sign"] = "credit"

    if "checking" in lowered:
        conditions["account_type"] = "checking"
    elif "credit card" in lowered or "card account" in lowered:
        conditions["account_type"] = "credit_card"

    category = _normalize_category_from_text(lowered)
    if category:
        actions["category"] = category
    if "mark reviewed" in lowered or "mark as reviewed" in lowered or "keep them reviewed" in lowered:
        actions["review_status"] = "reviewed"
    if "ignore" in lowered:
        actions["review_status"] = "ignored"
    if _contains_any(lowered, ("unmatch", "force unmatched", "force-unmatched", "no discord")):
        actions["match_override_status"] = "force_unmatched"
    if _contains_any(lowered, ("platform payout", "shopify payout", "tiktok payout")):
        actions["classification"] = "shopify_payout" if "shopify" in lowered else "tiktok_payout" if "tiktok" in lowered else "processor_payout"

    if not conditions:
        warnings.append("No narrow bank-row condition was detected; add a phrase like Apple Cash, Zelle, Shopify, or a quoted description.")
    if not actions:
        warnings.append("No action was detected; mention a category, review status, or force-unmatch action.")

    name_base = conditions.get("description_contains") or conditions.get("provider") or "ledger rule"
    return {
        "name": f"{str(name_base).title()} rule",
        "summary": _rule_summary(conditions, actions),
        "conditions": conditions,
        "actions": actions,
        "confidence": "medium" if warnings else "high",
        "warnings": warnings,
        "source": "deterministic",
    }


def _coerce_rule_json(payload: dict[str, Any], fallback_instruction: str) -> dict[str, Any]:
    draft = draft_ledger_rule_from_instruction(fallback_instruction)
    conditions = payload.get("conditions") if isinstance(payload.get("conditions"), dict) else draft["conditions"]
    actions = payload.get("actions") if isinstance(payload.get("actions"), dict) else draft["actions"]
    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    return {
        "name": str(payload.get("name") or draft["name"])[:160],
        "summary": str(payload.get("summary") or _rule_summary(conditions, actions))[:500],
        "conditions": conditions,
        "actions": actions,
        "confidence": str(payload.get("confidence") or draft["confidence"]),
        "warnings": [str(item)[:240] for item in warnings],
        "source": "ai",
    }


def draft_ledger_rule_with_ai(instruction: str) -> dict[str, Any]:
    if not has_ai_key():
        return draft_ledger_rule_from_instruction(instruction)
    prompt = (
        "You are a page-local Ledger Assistant for a collectibles business. "
        "Convert the operator request into JSON only. The rule can only target bank rows. "
        "Allowed condition keys: description_contains, description_regex, amount_sign, amount_min, "
        "amount_max, account_type, payment_rail, classification, category, provider. "
        "Allowed action keys: category, classification, review_status, match_override_status, note. "
        "review_status must be open, reviewed, or ignored. match_override_status can be force_unmatched or clear. "
        "Bank rows are the counted money source; Discord, Shopify, and TikTok are context only."
    )
    try:
        response = get_ai_client().with_options(timeout=20).chat.completions.create(
            model=get_fast_model(),
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": instruction},
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return _coerce_rule_json(parsed, instruction)
    except Exception:
        return draft_ledger_rule_from_instruction(instruction)
    return draft_ledger_rule_from_instruction(instruction)


def _condition_value_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    return [str(value).strip().lower()] if str(value).strip() else []


def row_matches_rule(row: BankTransaction, conditions: dict[str, Any]) -> bool:
    if not conditions:
        return False
    description_text = " ".join([row.description or "", row.details or "", row.raw_row_json or ""]).lower()
    contains = _condition_value_list(conditions.get("description_contains"))
    if contains and not any(token in description_text for token in contains):
        return False
    regex = str(conditions.get("description_regex") or "").strip()
    if regex:
        try:
            if not re.search(regex, description_text, flags=re.IGNORECASE):
                return False
        except re.error:
            return False
    amount = _money(row.amount)
    sign = str(conditions.get("amount_sign") or "").lower()
    if sign in {"debit", "negative", "outflow"} and amount >= 0:
        return False
    if sign in {"credit", "positive", "inflow"} and amount <= 0:
        return False
    if conditions.get("amount_min") not in {None, ""} and amount < float(conditions["amount_min"]):
        return False
    if conditions.get("amount_max") not in {None, ""} and amount > float(conditions["amount_max"]):
        return False
    account_type = str(conditions.get("account_type") or "").strip().lower()
    if account_type and account_type != (row.account_type or "").lower():
        return False
    payment_rail = str(conditions.get("payment_rail") or "").strip().lower()
    if payment_rail and payment_rail != _payment_rail_for_text(description_text):
        return False
    classification = str(conditions.get("classification") or "").strip()
    if classification and classification != (row.classification or ""):
        return False
    category = str(conditions.get("category") or "").strip()
    if category and category != (row.expense_category or "uncategorized"):
        return False
    provider = str(conditions.get("provider") or "").strip().lower()
    if provider and provider not in description_text and provider != ledger_source_for_bank_row(row):
        return False
    return True


def _rule_summary(conditions: dict[str, Any], actions: dict[str, Any]) -> str:
    condition_bits = []
    if conditions.get("description_contains"):
        condition_bits.append(f"description contains {conditions['description_contains']}")
    if conditions.get("description_regex"):
        condition_bits.append("description matches regex")
    if conditions.get("amount_sign"):
        condition_bits.append(f"amount is {conditions['amount_sign']}")
    if conditions.get("account_type"):
        condition_bits.append(f"account type is {conditions['account_type']}")
    if conditions.get("payment_rail"):
        condition_bits.append(f"rail is {conditions['payment_rail']}")
    action_bits = []
    if actions.get("category"):
        action_bits.append(f"set category to {expense_category_label(str(actions['category']))}")
    if actions.get("classification"):
        action_bits.append(f"set classification to {classification_label(str(actions['classification']))}")
    if actions.get("review_status"):
        action_bits.append(f"mark {actions['review_status']}")
    if actions.get("match_override_status") == "force_unmatched":
        action_bits.append("force unmatched")
    if actions.get("note"):
        action_bits.append("add note")
    return f"When {' and '.join(condition_bits) or 'rows match'}, {'; '.join(action_bits) or 'make no changes'}."


def _preview_sample_view(row: BankTransaction) -> dict[str, Any]:
    category = row.expense_category or "uncategorized"
    return {
        "id": row.id,
        "posted_at": _short_date(row.posted_at),
        "posted_at_display": _short_date(row.posted_at),
        "account_label": row.account_label,
        "description": row.description,
        "amount": _money(row.amount),
        "amount_display": format_ledger_money(row.amount),
        "expense_category": category,
        "expense_category_label": expense_category_label(category),
        "classification": row.classification,
        "ledger_status": ledger_status_for_bank_row(row),
    }


def preview_ledger_rule(
    session: Session,
    *,
    conditions: dict[str, Any],
    actions: dict[str, Any],
    filters: Optional[LedgerFilters] = None,
    sample_limit: int = 8,
) -> dict[str, Any]:
    selected = filters or LedgerFilters(status="all")
    rows = [row for row in _load_bank_rows(session) if _row_matches_filters(row, selected) and row_matches_rule(row, conditions)]
    sample = [_preview_sample_view(row) for row in _sort_rows(rows, selected.sort, selected.direction)[:sample_limit]]
    warnings: list[str] = []
    if not conditions:
        warnings.append("No conditions were provided, so the rule cannot be applied.")
    if not actions:
        warnings.append("No actions were provided, so the rule would not change rows.")
    return {
        "affected_count": len(rows),
        "sample_rows": sample,
        "summary": _rule_summary(conditions, actions),
        "warnings": warnings,
    }


def create_ledger_rule(
    session: Session,
    *,
    name: str,
    description: str,
    conditions: dict[str, Any],
    actions: dict[str, Any],
    created_by: str,
) -> LedgerRule:
    rule = LedgerRule(
        name=(name or "Ledger rule").strip()[:160],
        description=(description or _rule_summary(conditions, actions)).strip(),
        conditions_json=_json_dumps(conditions),
        actions_json=_json_dumps(actions),
        created_by=created_by or None,
    )
    session.add(rule)
    session.commit()
    session.refresh(rule)
    return rule


def apply_ledger_rule(
    session: Session,
    rule: LedgerRule,
    *,
    filters: Optional[LedgerFilters] = None,
    applied_by: str = "",
) -> dict[str, Any]:
    conditions = _as_dict(rule.conditions_json)
    actions = _as_dict(rule.actions_json)
    selected = filters or LedgerFilters(status="all")
    rows = [row for row in _load_bank_rows(session) if _row_matches_filters(row, selected) and row_matches_rule(row, conditions)]
    now = utcnow()
    updated = 0
    for row in rows:
        changed = False
        category = str(actions.get("category") or "").strip()
        if category and category != (row.expense_category or ""):
            row.expense_category = category
            row.expense_subcategory = f"Ledger rule #{rule.id}" if rule.id else "Ledger rule"
            row.category_confidence = "rule"
            row.category_reason = f"Ledger rule: {rule.name}"
            changed = True
        classification = str(actions.get("classification") or "").strip()
        if classification and classification != (row.classification or ""):
            row.classification = classification
            changed = True
        review_status = str(actions.get("review_status") or "").strip()
        if review_status in RULE_ALLOWED_REVIEW_STATUSES and review_status != (row.review_status or "open"):
            row.review_status = review_status
            changed = True
        match_override = str(actions.get("match_override_status") or "").strip()
        if match_override and match_override in RULE_ALLOWED_MATCH_OVERRIDES:
            if match_override == "force_unmatched":
                row.match_override_status = "force_unmatched"
                row.match_override_note = str(actions.get("note") or rule.description or rule.name).strip() or None
                row.match_override_at = now
                row.match_override_by = applied_by or None
                row.matched_transaction_id = None
                row.matched_source_message_id = None
                row.matched_platform = None
                row.match_reason = f"Forced unmatched by ledger rule: {rule.name}"
                changed = True
            elif row.match_override_status:
                row.match_override_status = None
                row.match_override_note = None
                row.match_override_at = None
                row.match_override_by = None
                changed = True
        note = str(actions.get("note") or "").strip()
        if note:
            row.review_note = _append_review_note(row.review_note, note)
            changed = True
        if changed:
            row.updated_at = now
            session.add(row)
            updated += 1
    rule.applied_count = int(rule.applied_count or 0) + updated
    rule.last_applied_at = now
    rule.updated_at = now
    session.add(rule)
    session.commit()
    return {"matched_count": len(rows), "updated_count": updated}


def ledger_filters_from_values(
    *,
    account: str = "",
    start: str = "",
    end: str = "",
    status: str = "needs_action",
    category: str = "",
    source: str = "",
    action_reason: str = "",
    search: str = "",
    sort: str = "posted_at",
    direction: str = "desc",
    limit: int = 250,
    include_cash: bool | str = False,
) -> LedgerFilters:
    include_cash_bool = include_cash
    if isinstance(include_cash, str):
        include_cash_bool = include_cash.strip().lower() in {"1", "true", "yes", "on"}
    return LedgerFilters(
        account=(account or "").strip(),
        start=(start or "").strip(),
        end=(end or "").strip(),
        status=(status or "needs_action").strip(),
        category=(category or "").strip(),
        source=(source or "").strip(),
        action_reason=(action_reason or "").strip(),
        search=(search or "").strip(),
        sort=(sort or "posted_at").strip(),
        direction=(direction or "desc").strip(),
        limit=max(min(int(limit or 250), 1000), 1),
        include_cash=bool(include_cash_bool),
    )
