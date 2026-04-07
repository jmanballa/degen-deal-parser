from __future__ import annotations

import json
import re
from typing import Any, Optional

from sqlmodel import Session, select

from .db import engine, managed_session
from .financials import EXPENSE_PATTERNS
from .models import DiscordMessage, PARSE_PARSED, PARSE_REVIEW_REQUIRED, ReviewCorrection, normalize_parse_status, utcnow

CORRECTION_SNAPSHOT_FIELDS = (
    "deal_type",
    "amount",
    "payment_method",
    "cash_direction",
    "category",
    "entry_kind",
    "expense_category",
    "notes",
    "trade_summary",
    "item_names",
    "items_in",
    "items_out",
    "confidence",
    "parse_status",
    "needs_review",
)

PAYMENT_ONLY_PATTERNS = (
    r"^(?:plus|\+)?\s*\$?\s*(\d+(?:\.\d{1,2})?)\s*(cash|zelle|venmo|paypal|card|tap|cc|dc)$",
    r"^(cash|zelle|venmo|paypal|card|tap|cc|dc)\s*\$?\s*(\d+(?:\.\d{1,2})?)$",
)

TRADE_DIRECTION_PATTERNS = (
    "top in",
    "top out",
    "bottom in",
    "bottom out",
    "left in",
    "left out",
    "right in",
    "right out",
    "left side in",
    "left side out",
    "right side in",
    "right side out",
)

MIN_LEARNED_RULE_CORRECTION_COUNT = 2


def normalize_correction_text(text: str) -> str:
    normalized = re.sub(r"(?im)^\s*message\s+\d+:\s*", "", text or "")
    normalized = re.sub(r"https?://\S+", " ", normalized, flags=re.I)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def tokenize_normalized_text(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9$+#]+", normalize_correction_text(text))
        if len(token) > 1
    }


def _safe_json_load(value: str, fallback: Any) -> Any:
    try:
        return json.loads(value or "")
    except json.JSONDecodeError:
        return fallback


def _safe_json_list(value: str) -> list[str]:
    loaded = _safe_json_load(value, [])
    if not isinstance(loaded, list):
        return []
    return [str(item).strip() for item in loaded if str(item).strip()]


def snapshot_message_parse(row: DiscordMessage) -> dict[str, Any]:
    return {
        "deal_type": row.deal_type,
        "amount": row.amount,
        "payment_method": row.payment_method,
        "cash_direction": row.cash_direction,
        "category": row.category,
        "entry_kind": row.entry_kind,
        "expense_category": row.expense_category,
        "notes": row.notes,
        "trade_summary": row.trade_summary,
        "item_names": _safe_json_list(row.item_names_json or "[]"),
        "items_in": _safe_json_list(row.items_in_json or "[]"),
        "items_out": _safe_json_list(row.items_out_json or "[]"),
        "confidence": row.confidence,
        "parse_status": normalize_parse_status(
            row.parse_status,
            is_deleted=row.is_deleted,
            needs_review=row.needs_review,
        ),
        "needs_review": bool(row.needs_review),
    }


def snapshot_correction_parse(correction: ReviewCorrection) -> dict[str, Any]:
    stored = _safe_json_load(correction.corrected_after_json or "{}", {})
    if isinstance(stored, dict) and stored:
        return stored
    return {
        "deal_type": correction.deal_type,
        "amount": correction.amount,
        "payment_method": correction.payment_method,
        "cash_direction": correction.cash_direction,
        "category": correction.category,
        "entry_kind": correction.entry_kind,
        "expense_category": correction.expense_category,
        "notes": correction.notes,
        "trade_summary": correction.trade_summary,
        "item_names": _safe_json_list(correction.item_names_json or "[]"),
        "items_in": _safe_json_list(correction.items_in_json or "[]"),
        "items_out": _safe_json_list(correction.items_out_json or "[]"),
        "confidence": correction.confidence,
        "parse_status": PARSE_PARSED,
        "needs_review": False,
    }


def build_field_diffs(parsed_before: dict[str, Any], corrected_after: dict[str, Any]) -> dict[str, dict[str, Any]]:
    diffs: dict[str, dict[str, Any]] = {}
    for field in CORRECTION_SNAPSHOT_FIELDS:
        before_value = parsed_before.get(field)
        after_value = corrected_after.get(field)
        if before_value != after_value:
            diffs[field] = {
                "before": before_value,
                "after": after_value,
            }
    return diffs


def extract_payment_phrase(message_text: str) -> tuple[float | None, str | None]:
    normalized = normalize_correction_text(message_text)
    if not normalized:
        return None, None

    for pattern in PAYMENT_ONLY_PATTERNS:
        match = re.fullmatch(pattern, normalized, re.I)
        if not match:
            continue
        if match.group(1).replace(".", "", 1).isdigit():
            amount = float(match.group(1))
            payment_method = match.group(2).lower()
        else:
            payment_method = match.group(1).lower()
            amount = float(match.group(2))
        if payment_method in {"tap", "cc", "dc"}:
            payment_method = "card"
        return amount, payment_method

    return None, None


def extract_trade_cash_phrase(message_text: str) -> tuple[float | None, str | None]:
    normalized = normalize_correction_text(message_text)
    if not normalized:
        return None, None

    match = re.search(
        r"(?:plus|\+)\s*\$?\s*(\d+(?:\.\d{1,2})?)(?:\s*(cash|zelle|venmo|paypal|card|tap|cc|dc))?",
        normalized,
        re.I,
    )
    if not match:
        return None, None

    amount = float(match.group(1))
    payment_method = (match.group(2) or "").lower() or None
    if payment_method in {"tap", "cc", "dc"}:
        payment_method = "card"
    return amount, payment_method


def has_trade_in_out_shorthand(message_text: str) -> bool:
    lower = normalize_correction_text(message_text)
    if not lower:
        return False
    if " out " in f" {lower} " and " in " in f" {lower} ":
        return True
    return any(pattern in lower for pattern in TRADE_DIRECTION_PATTERNS)


def extract_directional_tokens(message_text: str) -> list[str]:
    lower = normalize_correction_text(message_text)
    tokens = [pattern for pattern in TRADE_DIRECTION_PATTERNS if pattern in lower]
    if " in " in f" {lower} ":
        tokens.append("in")
    if " out " in f" {lower} ":
        tokens.append("out")
    seen: set[str] = set()
    ordered: list[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def extract_expense_keywords(message_text: str) -> list[str]:
    lower = normalize_correction_text(message_text)
    found: list[str] = []
    for _category, keywords in EXPENSE_PATTERNS:
        for keyword in keywords:
            if keyword in lower and keyword not in found:
                found.append(keyword)
    return found


def extract_learning_features(message_text: str) -> dict[str, Any]:
    amount, payment_method = extract_payment_phrase(message_text)
    normalized = normalize_correction_text(message_text)
    return {
        "normalized_text": normalized,
        "tokens": sorted(tokenize_normalized_text(message_text)),
        "payment_only_text": amount is not None and payment_method is not None,
        "payment_amount": amount,
        "payment_method": payment_method,
        "trade_in_out_text": has_trade_in_out_shorthand(message_text),
        "directional_tokens": extract_directional_tokens(message_text),
        "expense_keywords": extract_expense_keywords(message_text),
    }


def infer_pattern_type(
    message_text: str,
    corrected_after: dict[str, Any],
    field_diffs: dict[str, dict[str, Any]],
    features: dict[str, Any],
) -> str | None:
    del message_text, field_diffs
    if corrected_after.get("deal_type") == "sell" and features.get("payment_only_text"):
        return "payment_only_sell"
    if corrected_after.get("deal_type") == "trade" and features.get("trade_in_out_text"):
        return "trade_in_out"
    expense_category = corrected_after.get("expense_category")
    if expense_category and expense_category != "inventory" and features.get("expense_keywords"):
        return "expense_keyword_override"
    return None


def compute_correction_confidence(
    session: Session,
    normalized_text: str,
    field_diffs: dict,
    parser_confidence: Optional[float],
) -> float:
    """Compute a blended confidence for a correction.

    Factors:
    - parser_confidence: base score from OpenAI (or learned rule floor)
    - severity_factor: penalises corrections that changed many fields
    - agreement_factor: boosts when other corrections for the same text agree
      on entry_kind and amount; penalises disagreement
    """
    base = float(parser_confidence or 0.85)

    n_diffs = len(field_diffs)
    if n_diffs == 0:
        severity_factor = 1.0
    elif n_diffs <= 2:
        severity_factor = 0.95
    elif n_diffs <= 4:
        severity_factor = 0.85
    else:
        severity_factor = 0.75

    peers = session.exec(
        select(ReviewCorrection).where(ReviewCorrection.normalized_text == normalized_text)
    ).all()

    if len(peers) < 2:
        agreement_factor = 1.0
    else:
        entry_kinds = {c.entry_kind for c in peers if c.entry_kind}
        amounts = {round(float(c.amount), 2) for c in peers if c.amount is not None}
        if len(entry_kinds) <= 1 and len(amounts) <= 1:
            agreement_factor = 1.0  # capped below at min(1.0, ...)
        else:
            agreement_factor = 0.80

    return min(1.0, base * severity_factor * agreement_factor)


def save_review_correction(
    session: Session,
    row: DiscordMessage,
    *,
    parsed_before: Optional[dict[str, Any]] = None,
) -> ReviewCorrection | None:
    if normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review) not in {
        PARSE_PARSED,
        PARSE_REVIEW_REQUIRED,
    }:
        return None
    if row.is_deleted:
        return None

    normalized_text = normalize_correction_text(row.content or "")
    if not normalized_text:
        return None

    existing = session.exec(
        select(ReviewCorrection).where(ReviewCorrection.source_message_id == row.id)
    ).first()

    correction = existing or ReviewCorrection(source_message_id=row.id, normalized_text=normalized_text)
    correction.normalized_text = normalized_text
    correction.deal_type = row.deal_type
    correction.amount = row.amount
    correction.payment_method = row.payment_method
    correction.cash_direction = row.cash_direction
    correction.category = row.category
    correction.entry_kind = row.entry_kind
    correction.expense_category = row.expense_category
    correction.notes = row.notes
    correction.trade_summary = row.trade_summary
    correction.items_in_json = row.items_in_json or "[]"
    correction.items_out_json = row.items_out_json or "[]"
    correction.item_names_json = row.item_names_json or "[]"
    corrected_after = snapshot_message_parse(row)
    baseline_before = parsed_before or {}
    field_diffs = build_field_diffs(baseline_before, corrected_after)
    correction.confidence = compute_correction_confidence(
        session, normalized_text, field_diffs, row.confidence
    )
    features = extract_learning_features(row.content or "")
    correction.pattern_type = infer_pattern_type(row.content or "", corrected_after, field_diffs, features)
    correction.parsed_before_json = json.dumps(baseline_before, sort_keys=True)
    correction.corrected_after_json = json.dumps(corrected_after, sort_keys=True)
    correction.field_diffs_json = json.dumps(field_diffs, sort_keys=True)
    correction.features_json = json.dumps(features, sort_keys=True)
    correction.updated_at = utcnow()

    session.add(correction)
    return correction


def build_correction_parse(correction: ReviewCorrection) -> dict:
    note_prefix = "matched promoted rule" if correction.correction_source == "promoted_rule" else "matched prior manual correction"
    return {
        "parsed_type": correction.deal_type,
        "parsed_amount": correction.amount,
        "parsed_payment_method": correction.payment_method,
        "parsed_cash_direction": correction.cash_direction,
        "parsed_category": correction.category,
        "parsed_items": _safe_json_list(correction.item_names_json or "[]"),
        "parsed_items_in": _safe_json_list(correction.items_in_json or "[]"),
        "parsed_items_out": _safe_json_list(correction.items_out_json or "[]"),
        "parsed_trade_summary": correction.trade_summary or "",
        "parsed_notes": f"{note_prefix}: {correction.notes or 'store correction memory'}",
        "image_summary": "matched prior correction memory",
        "confidence": max(float(correction.confidence or 0.0), 0.96),
        "needs_review": False,
        "matched_correction_id": correction.id,
        "matched_correction_source": correction.correction_source,
    }


def build_learning_event(
    *,
    status: str,
    correction: ReviewCorrection | None,
    pattern_type: str | None,
    reason: str,
    details: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    event = {
        "status": status,
        "pattern_type": pattern_type,
        "reason": reason,
    }
    if correction is not None:
        event["correction_id"] = correction.id
        event["correction_source"] = correction.correction_source
        event["normalized_text"] = correction.normalized_text
    if details:
        event.update(details)
    return event


def build_learned_rule_parse(
    correction: ReviewCorrection,
    *,
    message_text: str,
    incoming_features: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    corrected_after = snapshot_correction_parse(correction)
    stored_features = _safe_json_load(correction.features_json or "{}", {})
    pattern_type = correction.pattern_type

    if pattern_type == "payment_only_sell":
        amount = incoming_features.get("payment_amount")
        payment_method = incoming_features.get("payment_method")
        if amount is None or payment_method is None:
            return None, build_learning_event(
                status="skipped",
                correction=correction,
                pattern_type=pattern_type,
                reason="incoming text is not a payment-only phrase",
            )
        return {
            "parsed_type": "sell",
            "parsed_amount": amount,
            "parsed_payment_method": payment_method,
            "parsed_cash_direction": "to_store",
            "parsed_category": corrected_after.get("category") or "unknown",
            "parsed_items": corrected_after.get("item_names") or [],
            "parsed_items_in": [],
            "parsed_items_out": corrected_after.get("items_out") or [],
            "parsed_trade_summary": "",
            "parsed_notes": "learned correction rule: payment-only sell",
            "image_summary": "learned deterministic correction rule",
            "confidence": max(float(corrected_after.get("confidence") or 0.0), 0.94),
            "needs_review": False,
            "matched_correction_id": correction.id,
            "matched_correction_source": "learned_rule",
        }, build_learning_event(
            status="applied",
            correction=correction,
            pattern_type=pattern_type,
            reason="matched payment-only sell phrase",
            details={"payment_method": payment_method, "amount": amount},
        )

    if pattern_type == "trade_in_out":
        if not incoming_features.get("trade_in_out_text"):
            return None, build_learning_event(
                status="skipped",
                correction=correction,
                pattern_type=pattern_type,
                reason="incoming text does not contain trade in/out shorthand",
            )
        expected_tokens = set(stored_features.get("directional_tokens") or [])
        incoming_tokens = set(incoming_features.get("directional_tokens") or [])
        if expected_tokens and incoming_tokens != expected_tokens:
            return None, build_learning_event(
                status="rejected",
                correction=correction,
                pattern_type=pattern_type,
                reason="directional tokens do not match stored shorthand pattern",
                details={
                    "expected_directional_tokens": sorted(expected_tokens),
                    "incoming_directional_tokens": sorted(incoming_tokens),
                },
            )
        amount, payment_method = extract_trade_cash_phrase(message_text)
        if amount is None:
            amount = incoming_features.get("payment_amount")
        payment_method = payment_method or incoming_features.get("payment_method") or corrected_after.get("payment_method")
        cash_direction = corrected_after.get("cash_direction")
        if amount is None and payment_method:
            payment_method = None
        if amount is None and not corrected_after.get("items_in") and not corrected_after.get("items_out"):
            return None, build_learning_event(
                status="rejected",
                correction=correction,
                pattern_type=pattern_type,
                reason="stored trade correction lacks reusable trade flow fields",
            )
        return {
            "parsed_type": "trade",
            "parsed_amount": amount,
            "parsed_payment_method": payment_method,
            "parsed_cash_direction": cash_direction or ("to_store" if amount is not None else "none"),
            "parsed_category": corrected_after.get("category") or "mixed",
            "parsed_items": corrected_after.get("item_names") or [],
            "parsed_items_in": corrected_after.get("items_in") or [],
            "parsed_items_out": corrected_after.get("items_out") or [],
            "parsed_trade_summary": corrected_after.get("trade_summary") or "learned trade shorthand",
            "parsed_notes": "learned correction rule: trade in/out shorthand",
            "image_summary": "learned deterministic correction rule",
            "confidence": max(float(corrected_after.get("confidence") or 0.0), 0.93),
            "needs_review": False,
            "matched_correction_id": correction.id,
            "matched_correction_source": "learned_rule",
        }, build_learning_event(
            status="applied",
            correction=correction,
            pattern_type=pattern_type,
            reason="matched learned trade shorthand",
            details={"directional_tokens": sorted(incoming_tokens)},
        )

    if pattern_type == "expense_keyword_override":
        expense_keywords = set(incoming_features.get("expense_keywords") or [])
        stored_expense_keywords = set(stored_features.get("expense_keywords") or [])
        overlap = sorted(expense_keywords & stored_expense_keywords)
        if not overlap:
            return None, build_learning_event(
                status="skipped",
                correction=correction,
                pattern_type=pattern_type,
                reason="incoming text does not share learned expense keywords",
            )
        amount = incoming_features.get("payment_amount")
        payment_method = incoming_features.get("payment_method") or corrected_after.get("payment_method")
        items_out = corrected_after.get("items_out") or overlap
        return {
            "parsed_type": corrected_after.get("deal_type") or "buy",
            "parsed_amount": amount,
            "parsed_payment_method": payment_method,
            "parsed_cash_direction": corrected_after.get("cash_direction") or "from_store",
            "parsed_category": corrected_after.get("category") or "mixed",
            "parsed_items": corrected_after.get("item_names") or [],
            "parsed_items_in": [],
            "parsed_items_out": items_out,
            "parsed_trade_summary": "",
            "parsed_notes": "learned correction rule: expense keyword override",
            "image_summary": "learned deterministic correction rule",
            "confidence": max(float(corrected_after.get("confidence") or 0.0), 0.92),
            "needs_review": False,
            "matched_correction_id": correction.id,
            "matched_correction_source": "learned_rule",
        }, build_learning_event(
            status="applied",
            correction=correction,
            pattern_type=pattern_type,
            reason="matched learned expense keyword override",
            details={"expense_keywords": overlap},
        )

    return None, build_learning_event(
        status="rejected",
        correction=correction,
        pattern_type=pattern_type,
        reason="unsupported learned pattern type",
    )


def get_exact_correction_match(message_text: str) -> dict | None:
    normalized_text = normalize_correction_text(message_text)
    if not normalized_text:
        return None

    with managed_session() as session:
        correction = session.exec(
            select(ReviewCorrection)
            .where(ReviewCorrection.normalized_text == normalized_text)
        ).first()
        if not correction:
            return None
        all_matches = session.exec(
            select(ReviewCorrection).where(ReviewCorrection.normalized_text == normalized_text)
        ).all()
        all_matches.sort(
            key=lambda row: (
                0 if row.correction_source == "promoted_rule" else 1,
                -(row.updated_at.timestamp() if row.updated_at else 0),
            )
        )
        correction = all_matches[0]
        return build_correction_parse(correction)


def get_learned_rule_match(message_text: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    incoming_features = extract_learning_features(message_text)
    with managed_session() as session:
        corrections = session.exec(
            select(ReviewCorrection)
            .where(ReviewCorrection.pattern_type != None)  # noqa: E711
            .order_by(ReviewCorrection.updated_at.desc())
        ).all()

    if not corrections:
        return None, None

    correction_counts: dict[str, int] = {}
    for correction in corrections:
        normalized_text = correction.normalized_text or ""
        if not normalized_text:
            continue
        correction_counts[normalized_text] = correction_counts.get(normalized_text, 0) + 1

    ranked: list[tuple[int, ReviewCorrection]] = []
    incoming_tokens = set(incoming_features.get("tokens") or [])
    for correction in corrections:
        stored_features = _safe_json_load(correction.features_json or "{}", {})
        stored_tokens = set(stored_features.get("tokens") or [])
        overlap = len(incoming_tokens & stored_tokens)
        pattern_bonus = 0
        if correction.pattern_type == "payment_only_sell" and incoming_features.get("payment_only_text"):
            pattern_bonus = 10
        elif correction.pattern_type == "trade_in_out" and incoming_features.get("trade_in_out_text"):
            pattern_bonus = 10
        elif correction.pattern_type == "expense_keyword_override":
            overlap += len(set(incoming_features.get("expense_keywords") or []) & set(stored_features.get("expense_keywords") or [])) * 2
            if incoming_features.get("expense_keywords"):
                pattern_bonus = 6
        if overlap + pattern_bonus <= 0:
            continue
        ranked.append((overlap + pattern_bonus, correction))

    ranked.sort(
        key=lambda item: (
            -item[0],
            -(item[1].updated_at.timestamp() if item[1].updated_at else 0),
        )
    )

    fallback_event: dict[str, Any] | None = None
    for _score, correction in ranked[:5]:
        correction_count = correction_counts.get(correction.normalized_text or "", 0)
        if correction_count < MIN_LEARNED_RULE_CORRECTION_COUNT:
            event = build_learning_event(
                status="skipped",
                correction=correction,
                pattern_type=correction.pattern_type,
                reason="learned rule requires more matching corrections before auto-apply",
                details={
                    "correction_count": correction_count,
                    "required_correction_count": MIN_LEARNED_RULE_CORRECTION_COUNT,
                },
            )
            if fallback_event is None:
                fallback_event = event
            continue
        learned_parse, event = build_learned_rule_parse(
            correction,
            message_text=message_text,
            incoming_features=incoming_features,
        )
        if learned_parse is not None:
            learned_parse["_learned_rule_event"] = event
            return learned_parse, event
        if fallback_event is None:
            fallback_event = event

    return None, fallback_event


def get_relevant_correction_hints(message_text: str, limit: int = 3) -> list[dict]:
    message_tokens = tokenize_normalized_text(message_text)
    if not message_tokens:
        return []

    with managed_session() as session:
        corrections = session.exec(
            select(ReviewCorrection).order_by(ReviewCorrection.updated_at.desc())
        ).all()

    ranked: list[tuple[int, ReviewCorrection]] = []
    for correction in corrections:
        correction_tokens = tokenize_normalized_text(correction.normalized_text)
        overlap = len(message_tokens & correction_tokens)
        if overlap < 2:
            continue
        ranked.append((overlap, correction))

    ranked.sort(
        key=lambda item: (
            -(1 if item[1].correction_source == "promoted_rule" else 0),
            -item[0],
            -(item[1].updated_at.timestamp() if item[1].updated_at else 0),
        )
    )

    hints: list[dict] = []
    seen_ids: set[int] = set()
    for overlap, correction in ranked:
        if correction.id in seen_ids:
            continue
        seen_ids.add(correction.id)
        hints.append(
            {
                "overlap": overlap,
                "normalized_text": correction.normalized_text,
                "deal_type": correction.deal_type,
                "amount": correction.amount,
                "payment_method": correction.payment_method,
                "cash_direction": correction.cash_direction,
                "category": correction.category,
                "entry_kind": correction.entry_kind,
                "expense_category": correction.expense_category,
                "notes": correction.notes,
                "pattern_type": correction.pattern_type,
                "correction_source": correction.correction_source,
            }
        )
        if len(hints) >= limit:
            break

    return hints


def get_correction_pattern_counts(limit: int = 10, session: Session | None = None) -> list[dict]:
    if session is None:
        with managed_session() as managed:
            corrections = managed.exec(select(ReviewCorrection)).all()
    else:
        corrections = session.exec(select(ReviewCorrection)).all()

    grouped: dict[str, dict] = {}
    for correction in corrections:
        key = correction.normalized_text
        bucket = grouped.setdefault(
            key,
            {
                "normalized_text": key,
                "count": 0,
                "deal_type": correction.deal_type,
                "payment_method": correction.payment_method,
                "cash_direction": correction.cash_direction,
                "correction_source": correction.correction_source,
            },
        )
        bucket["count"] += 1
        if correction.correction_source == "promoted_rule":
            bucket["correction_source"] = "promoted_rule"

    patterns = sorted(grouped.values(), key=lambda row: (-row["count"], row["normalized_text"]))
    return patterns[:limit]


def get_learning_signal(session: Session, message_text: str) -> dict:
    normalized_text = normalize_correction_text(message_text)
    if not normalized_text:
        return {
            "exact_match": False,
            "promoted_rule": False,
            "similar_count": 0,
        }

    exact_matches = session.exec(
        select(ReviewCorrection).where(ReviewCorrection.normalized_text == normalized_text)
    ).all()
    exact_match = bool(exact_matches)
    promoted_rule = any(match.correction_source == "promoted_rule" for match in exact_matches)

    message_tokens = tokenize_normalized_text(message_text)
    similar_count = 0
    if message_tokens:
        all_corrections = session.exec(select(ReviewCorrection)).all()
        seen_texts: set[str] = set()
        for correction in all_corrections:
            if correction.normalized_text == normalized_text:
                continue
            correction_text = correction.normalized_text
            if correction_text in seen_texts:
                continue
            correction_tokens = tokenize_normalized_text(correction_text)
            overlap = len(message_tokens & correction_tokens)
            if overlap >= 2:
                similar_count += 1
                seen_texts.add(correction_text)

    return {
        "exact_match": exact_match,
        "promoted_rule": promoted_rule,
        "similar_count": similar_count,
    }


def get_learning_signals(session: Session, message_texts: list[str]) -> dict[str, dict]:
    normalized_texts = {
        message_text: normalize_correction_text(message_text)
        for message_text in message_texts
    }
    non_empty_normalized = {value for value in normalized_texts.values() if value}
    if not non_empty_normalized:
        return {
            message_text: {
                "exact_match": False,
                "promoted_rule": False,
                "similar_count": 0,
            }
            for message_text in message_texts
        }

    exact_matches = session.exec(
        select(ReviewCorrection).where(ReviewCorrection.normalized_text.in_(non_empty_normalized))
    ).all()

    exact_matches_by_text: dict[str, list[ReviewCorrection]] = {}
    for correction in exact_matches:
        exact_matches_by_text.setdefault(correction.normalized_text, []).append(correction)

    results: dict[str, dict] = {}
    for message_text in message_texts:
        normalized_text = normalized_texts[message_text]
        if not normalized_text:
            results[message_text] = {
                "exact_match": False,
                "promoted_rule": False,
                "similar_count": 0,
            }
            continue

        matching_rows = exact_matches_by_text.get(normalized_text, [])
        exact_match = bool(matching_rows)
        promoted_rule = any(match.correction_source == "promoted_rule" for match in matching_rows)

        results[message_text] = {
            "exact_match": exact_match,
            "promoted_rule": promoted_rule,
            "similar_count": 0,
        }

    return results


def promote_correction_pattern(session: Session, normalized_text: str) -> int:
    normalized = normalize_correction_text(normalized_text)
    if not normalized:
        return 0

    corrections = session.exec(
        select(ReviewCorrection).where(ReviewCorrection.normalized_text == normalized)
    ).all()
    for correction in corrections:
        correction.correction_source = "promoted_rule"
        correction.updated_at = utcnow()
        session.add(correction)
    return len(corrections)


def auto_promote_eligible_patterns(
    session: Session,
    *,
    min_count: int = 5,
    min_confidence: float = 0.85,
) -> list[str]:
    """Promote correction patterns that meet the count and confidence thresholds.

    Returns the list of normalized_text values that were promoted.
    """
    candidates = session.exec(
        select(ReviewCorrection).where(ReviewCorrection.correction_source != "promoted_rule")
    ).all()

    # Group by normalized_text
    groups: dict[str, list[ReviewCorrection]] = {}
    for c in candidates:
        key = c.normalized_text or ""
        if not key:
            continue
        groups.setdefault(key, []).append(c)

    promoted: list[str] = []
    for normalized_text, group in groups.items():
        if len(group) < min_count:
            continue
        confidence_values = [c.confidence for c in group if c.confidence is not None]
        if not confidence_values:
            continue
        avg_confidence = sum(confidence_values) / len(confidence_values)
        if avg_confidence < min_confidence:
            continue

        for correction in group:
            correction.correction_source = "promoted_rule"
            correction.updated_at = utcnow()
            session.add(correction)
        promoted.append(normalized_text)

    if promoted:
        session.commit()
    return promoted
