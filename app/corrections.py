from __future__ import annotations

import json
import re
from typing import Optional

from sqlmodel import Session, select

from .db import engine
from .models import DiscordMessage, ReviewCorrection, utcnow


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


def save_review_correction(session: Session, row: DiscordMessage) -> ReviewCorrection | None:
    if row.parse_status not in {"parsed", "needs_review"}:
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
    correction.confidence = row.confidence
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
        "parsed_items": json.loads(correction.item_names_json or "[]"),
        "parsed_items_in": json.loads(correction.items_in_json or "[]"),
        "parsed_items_out": json.loads(correction.items_out_json or "[]"),
        "parsed_trade_summary": correction.trade_summary or "",
        "parsed_notes": f"{note_prefix}: {correction.notes or 'store correction memory'}",
        "image_summary": "matched prior correction memory",
        "confidence": max(float(correction.confidence or 0.0), 0.96),
        "needs_review": False,
        "matched_correction_id": correction.id,
        "matched_correction_source": correction.correction_source,
    }


def get_exact_correction_match(message_text: str) -> dict | None:
    normalized_text = normalize_correction_text(message_text)
    if not normalized_text:
        return None

    with Session(engine) as session:
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


def get_relevant_correction_hints(message_text: str, limit: int = 3) -> list[dict]:
    message_tokens = tokenize_normalized_text(message_text)
    if not message_tokens:
        return []

    with Session(engine) as session:
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
                "correction_source": correction.correction_source,
            }
        )
        if len(hints) >= limit:
            break

    return hints


def get_correction_pattern_counts(limit: int = 10) -> list[dict]:
    with Session(engine) as session:
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

    corrections = session.exec(select(ReviewCorrection)).all()

    exact_matches_by_text: dict[str, list[ReviewCorrection]] = {}
    correction_tokens_by_text: dict[str, set[str]] = {}
    for correction in corrections:
        exact_matches_by_text.setdefault(correction.normalized_text, []).append(correction)
        if correction.normalized_text not in correction_tokens_by_text:
            correction_tokens_by_text[correction.normalized_text] = tokenize_normalized_text(correction.normalized_text)

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

        exact_matches = exact_matches_by_text.get(normalized_text, [])
        exact_match = bool(exact_matches)
        promoted_rule = any(match.correction_source == "promoted_rule" for match in exact_matches)

        message_tokens = tokenize_normalized_text(message_text)
        similar_count = 0
        if message_tokens:
            seen_texts: set[str] = set()
            for correction_text, correction_tokens in correction_tokens_by_text.items():
                if correction_text == normalized_text or correction_text in seen_texts:
                    continue
                overlap = len(message_tokens & correction_tokens)
                if overlap >= 2:
                    similar_count += 1
                    seen_texts.add(correction_text)

        results[message_text] = {
            "exact_match": exact_match,
            "promoted_rule": promoted_rule,
            "similar_count": similar_count,
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
