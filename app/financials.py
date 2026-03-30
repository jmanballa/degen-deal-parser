from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


ENTRY_KINDS = {"sale", "buy", "trade", "expense", "unknown"}

EXPENSE_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("rent", ("rent", "lease")),
    ("utilities", ("electric", "electricity", "water bill", "internet", "wifi", "utility", "utilities")),
    ("software", ("software", "subscription", "quickbooks", "shopify", "canva", "adobe", "discord boost")),
    ("fees", ("vendor fee", "table fee", "booth fee", "event fee", "processing fee", "stripe fee", "square fee")),
    ("shipping", ("shipping", "postage", "usps", "ups", "fedex", "label cost")),
    ("travel", ("gas", "parking", "hotel", "mileage", "uber", "lyft")),
    ("food", ("food", "lunch", "dinner", "snacks", "coffee")),
    ("payroll", ("payroll", "wages", "salary", "commission", "paid staff")),
    ("tax", ("tax", "sales tax", "franchise tax", "license renewal")),
    ("insurance", ("insurance",)),
    ("maintenance", ("repair", "maintenance", "cleaning", "printer ink")),
    ("supplies", ("supplies", "paper", "tape", "bubble mailer", "mailer", "shipping supplies")),
]

INVENTORY_HINTS = (
    "slab",
    "slabs",
    "single",
    "singles",
    "pack",
    "packs",
    "booster",
    "box",
    "boxes",
    "binder",
    "collection",
    "card",
    "cards",
    "pokemon",
    "mtg",
    "yugioh",
    "one piece",
    "sealed",
    "trade",
    "psa",
    "bgs",
)


@dataclass
class FinancialSummary:
    entry_kind: str
    money_in: float
    money_out: float
    expense_category: Optional[str]


def normalize_payment_amount(amount: Optional[float]) -> float:
    if amount is None:
        return 0.0
    return round(float(amount), 2)


def detect_expense_category(message_text: str) -> Optional[str]:
    lower = (message_text or "").lower()
    if not lower:
        return None

    if any(token in lower for token in INVENTORY_HINTS):
        return None

    for category, keywords in EXPENSE_PATTERNS:
        if any(keyword in lower for keyword in keywords):
            return category

    if re.search(r"\b(paid|expense|spent)\b", lower) and re.search(r"\b(store|shop|booth|vendor|business)\b", lower):
        return "other"

    return None


def is_inventory_transaction(parsed_type: Optional[str], parsed_category: Optional[str], message_text: str) -> bool:
    if parsed_type in {"buy", "sell", "trade"}:
        if parsed_category in {"slabs", "singles", "sealed", "packs", "mixed"}:
            return True
        lower = (message_text or "").lower()
        if any(token in lower for token in INVENTORY_HINTS):
            return True
    return False


def derive_entry_kind(
    parsed_type: Optional[str],
    parsed_category: Optional[str],
    cash_direction: Optional[str],
    message_text: str,
) -> tuple[str, Optional[str]]:
    expense_category = detect_expense_category(message_text)
    if is_inventory_transaction(parsed_type, parsed_category, message_text):
        inventory_category = "inventory"
    else:
        inventory_category = None

    if expense_category and parsed_type in {None, "unknown", "buy"}:
        return "expense", expense_category

    if parsed_type == "sell":
        return "sale", inventory_category
    if parsed_type == "buy":
        return "buy", inventory_category
    if parsed_type == "trade":
        return "trade", inventory_category

    if expense_category and cash_direction == "from_store":
        return "expense", expense_category

    return "unknown", expense_category or inventory_category


def compute_financials(
    *,
    parsed_type: Optional[str],
    parsed_category: Optional[str],
    amount: Optional[float],
    cash_direction: Optional[str],
    message_text: str,
) -> FinancialSummary:
    entry_kind, expense_category = derive_entry_kind(
        parsed_type=parsed_type,
        parsed_category=parsed_category,
        cash_direction=cash_direction,
        message_text=message_text,
    )

    normalized_amount = normalize_payment_amount(amount)
    money_in = 0.0
    money_out = 0.0

    if entry_kind == "sale":
        money_in = normalized_amount
    elif entry_kind in {"buy", "expense"}:
        money_out = normalized_amount
    elif entry_kind == "trade":
        if cash_direction == "to_store":
            money_in = normalized_amount
        elif cash_direction == "from_store":
            money_out = normalized_amount
    else:
        if cash_direction == "to_store":
            money_in = normalized_amount
        elif cash_direction == "from_store":
            money_out = normalized_amount

    return FinancialSummary(
        entry_kind=entry_kind,
        money_in=money_in,
        money_out=money_out,
        expense_category=expense_category,
    )
