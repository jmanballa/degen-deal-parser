import asyncio
import json
import uuid
import re
from datetime import datetime, timezone, timedelta

from sqlmodel import Session, select

from .config import get_settings
from .db import engine
from .financials import compute_financials
from .models import DiscordMessage, ParseAttempt
from .parser import parse_message, TimedOutRowError
from .transactions import sync_transaction_from_message

settings = get_settings()
STALE_PROCESSING_AFTER = timedelta(minutes=10)


def utcnow():
    return datetime.now(timezone.utc)


def close_or_recover_unfinished_attempts(session: Session) -> None:
    recovery_now = utcnow()
    cutoff = recovery_now - STALE_PROCESSING_AFTER
    attempts = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.finished_at == None)  # noqa: E711
        .order_by(ParseAttempt.started_at)
    ).all()

    changed = False
    for attempt in attempts:
        row = session.get(DiscordMessage, attempt.message_id)
        if not row:
            attempt.finished_at = utcnow()
            attempt.success = False
            attempt.error = attempt.error or "message missing during recovery"
            session.add(attempt)
            changed = True
            continue

        if row.parse_status in {"parsed", "needs_review", "ignored"}:
            attempt.finished_at = utcnow()
            attempt.success = True
            attempt.error = None
            session.add(attempt)
            changed = True
            continue

        attempt_started_at = attempt.started_at
        if attempt_started_at is not None and attempt_started_at.tzinfo is None:
            attempt_started_at = attempt_started_at.replace(tzinfo=timezone.utc)

        if row.parse_status == "processing" and attempt_started_at and attempt_started_at < cutoff:
            row.parse_status = "queued"
            row.last_error = "Recovered from stale processing state after worker interruption."
            attempt.finished_at = recovery_now
            attempt.success = False
            attempt.error = "recovered stale processing attempt"
            session.add(row)
            session.add(attempt)
            changed = True

    if changed:
        session.commit()


def clear_stitch_fields(row: DiscordMessage) -> None:
    row.stitched_group_id = None
    row.stitched_primary = False
    row.stitched_message_ids_json = "[]"


def clear_parsed_fields(row: DiscordMessage) -> None:
    row.deal_type = None
    row.amount = None
    row.payment_method = None
    row.cash_direction = None
    row.category = None
    row.item_names_json = "[]"
    row.items_in_json = "[]"
    row.items_out_json = "[]"
    row.trade_summary = None
    row.notes = None
    row.confidence = None
    row.needs_review = False
    row.image_summary = None
    row.entry_kind = None
    row.money_in = None
    row.money_out = None
    row.expense_category = None


def mark_grouped_child_ignored(row: DiscordMessage) -> None:
    clear_parsed_fields(row)
    row.parse_status = "ignored"
    row.last_error = None


def clear_stale_group_members(
    session: Session,
    group_rows: list[DiscordMessage],
    primary_row: DiscordMessage,
) -> list[DiscordMessage]:
    row_ids = [grouped_row.id for grouped_row in group_rows if grouped_row.id is not None]
    stale_rows: list[DiscordMessage] = []

    for grouped_row in group_rows:
        prior_group_id = grouped_row.stitched_group_id
        if not prior_group_id:
            continue

        existing_group_rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.stitched_group_id == prior_group_id)
        ).all()

        for existing_row in existing_group_rows:
            if existing_row.id in row_ids:
                continue
            clear_stitch_fields(existing_row)
            clear_parsed_fields(existing_row)
            if not existing_row.is_deleted:
                existing_row.parse_status = "queued"
                existing_row.last_error = None
            stale_rows.append(existing_row)

    if len(group_rows) == 1:
        clear_stitch_fields(primary_row)

    return stale_rows


async def parser_loop(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await process_once()
        except Exception as e:
            print(f"[worker] loop error: {e}")
        await asyncio.sleep(settings.parser_poll_seconds)


async def process_once():
    row_ids: list[int] = []

    with Session(engine) as session:
        close_or_recover_unfinished_attempts(session)

        rows = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.parse_status.in_(["queued", "failed"]))
            .where(DiscordMessage.parse_attempts < settings.parser_max_attempts)
            .order_by(DiscordMessage.created_at)
            .limit(settings.parser_batch_size)
        ).all()

        for row in rows:
            row.parse_status = "processing"
            row.parse_attempts += 1

            session.add(
                ParseAttempt(
                    message_id=row.id,
                    attempt_number=row.parse_attempts,
                    model_used="gpt-5-nano",
                )
            )

            row_ids.append(row.id)

        session.commit()

    for row_id in row_ids:
        await process_row(row_id)


async def process_row(row_id: int):
    with Session(engine) as session:
        row = session.get(DiscordMessage, row_id)
        if not row:
            return

        if row.is_deleted:
            return

        if row.parse_status not in ["processing", "queued", "failed"]:
            return

        group_rows = [row]
        if settings.stitch_enabled:
            group_rows = build_stitch_group(
                session=session,
                row=row,
                window_seconds=settings.stitch_window_seconds,
                max_messages=settings.stitch_max_messages,
            )

        group_rows = sorted(group_rows, key=lambda r: r.created_at)
        primary_row = group_rows[0]
        stale_rows = clear_stale_group_members(session, group_rows, primary_row)

        combined_text, combined_attachments, grouped_row_ids = combine_group_payload(group_rows)

        group_id = str(uuid.uuid4()) if len(group_rows) > 1 else None

        attempt = session.exec(
            select(ParseAttempt)
            .where(ParseAttempt.message_id == row.id)
            .order_by(ParseAttempt.id.desc())
        ).first()

        try:
            result = await parse_message(
                content=combined_text,
                attachment_urls=combined_attachments,
                author_name=row.author_name or "",
            )
            usage = result.pop("_openai_usage", None) or {}
            model_used = result.pop("_openai_model", None)
            financials = compute_financials(
                parsed_type=result.get("parsed_type"),
                parsed_category=result.get("parsed_category"),
                amount=result.get("parsed_amount"),
                cash_direction=result.get("parsed_cash_direction"),
                message_text=combined_text,
            )

            for grouped_row in group_rows:
                grouped_row.stitched_group_id = group_id
                grouped_row.stitched_primary = (grouped_row.id == primary_row.id)
                grouped_row.stitched_message_ids_json = json.dumps(grouped_row_ids)

            primary_row.deal_type = result.get("parsed_type")
            primary_row.amount = result.get("parsed_amount")
            primary_row.payment_method = result.get("parsed_payment_method")
            primary_row.cash_direction = result.get("parsed_cash_direction")
            primary_row.category = result.get("parsed_category")
            primary_row.item_names_json = json.dumps(result.get("parsed_items", []))
            primary_row.items_in_json = json.dumps(result.get("parsed_items_in", []))
            primary_row.items_out_json = json.dumps(result.get("parsed_items_out", []))
            primary_row.trade_summary = result.get("parsed_trade_summary")
            primary_row.notes = result.get("parsed_notes")
            primary_row.confidence = result.get("confidence")
            primary_row.needs_review = bool(result.get("needs_review", False))
            primary_row.image_summary = result.get("image_summary")
            primary_row.entry_kind = financials.entry_kind
            primary_row.money_in = financials.money_in
            primary_row.money_out = financials.money_out
            primary_row.expense_category = financials.expense_category
            if result.get("ignore_message"):
                primary_row.parse_status = "ignored"
                primary_row.needs_review = False
                primary_row.entry_kind = None
                primary_row.money_in = None
                primary_row.money_out = None
                primary_row.expense_category = None
            else:
                primary_row.parse_status = "needs_review" if primary_row.needs_review else "parsed"
            primary_row.last_error = None

            for grouped_row in group_rows:
                if grouped_row.id != primary_row.id:
                    mark_grouped_child_ignored(grouped_row)

            if attempt:
                attempt.success = True
                attempt.error = None
                attempt.finished_at = utcnow()
                attempt.model_used = model_used or attempt.model_used
                attempt.input_tokens = usage.get("input_tokens")
                attempt.cached_input_tokens = usage.get("cached_input_tokens")
                attempt.output_tokens = usage.get("output_tokens")
                attempt.total_tokens = usage.get("total_tokens")
                attempt.estimated_cost_usd = usage.get("estimated_cost_usd")
                session.add(attempt)

            for grouped_row in group_rows:
                session.add(grouped_row)
            for stale_row in stale_rows:
                session.add(stale_row)

            for grouped_row in group_rows:
                sync_transaction_from_message(session, grouped_row)
            for stale_row in stale_rows:
                sync_transaction_from_message(session, stale_row)
            session.commit()

        except TimedOutRowError as e:
            row.parse_status = "failed"
            row.last_error = f"timeout: {e}"

            if attempt:
                attempt.success = False
                attempt.error = f"timeout: {e}"
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            session.commit()

        except Exception as e:
            row.parse_status = "failed"
            row.last_error = str(e)

            if attempt:
                attempt.success = False
                attempt.error = str(e)
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            session.commit()
def looks_like_fragment(row: DiscordMessage) -> bool:
    text = (row.content or "").strip().lower()
    has_images = bool(json.loads(row.attachment_urls_json or "[]"))

    if has_images and len(text) <= 30:
        return True

    fragment_patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card)\s*\$?\d+",
        r"^\+?\s*\$?\d+\s*(zelle|venmo|paypal|cash|tap|card)$",
        r"^(top|bottom|left|right).*\b(in|out)\b",
        r"^\+?\s*\d+\s*(zelle|venmo|paypal|cash|tap|card)$",
    ]

    return any(re.search(p, text, re.I) for p in fragment_patterns)
def build_stitch_group(
    session: Session,
    row: DiscordMessage,
    window_seconds: int,
    max_messages: int,
) -> list[DiscordMessage]:
    if row.is_deleted:
        return [row]

    start_time = row.created_at - timedelta(seconds=window_seconds)
    end_time = row.created_at + timedelta(seconds=window_seconds)

    candidates = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id == row.channel_id)
        .where(DiscordMessage.is_deleted == False)
        .where(DiscordMessage.created_at >= start_time)
        .where(DiscordMessage.created_at <= end_time)
        .order_by(DiscordMessage.created_at)
    ).all()

    if not candidates:
        return [row]

    candidates = [c for c in candidates if c.parse_status != "deleted"]
    candidates = [candidate for candidate in candidates if same_author(candidate, row)]
    candidates = [
        candidate for candidate in candidates
        if abs((candidate.created_at - row.created_at).total_seconds()) <= window_seconds
    ]

    if row not in candidates:
        candidates.append(row)

    group_rows = [row]
    nearby_candidates = sorted(
        [candidate for candidate in candidates if candidate.id != row.id],
        key=lambda candidate: (
            abs((candidate.created_at - row.created_at).total_seconds()),
            candidate.created_at,
        ),
    )

    for candidate in nearby_candidates:
        if len(group_rows) >= max_messages:
            break
        if not stitch_group_needs_more_context(group_rows):
            break
        if not candidate_improves_group(group_rows, candidate):
            continue

        tentative_group = sorted(group_rows + [candidate], key=lambda grouped_row: grouped_row.created_at)
        if should_stitch_rows(row, tentative_group):
            group_rows = tentative_group

    if len(group_rows) <= 1 or not should_stitch_rows(row, group_rows):
        return [row]

    return group_rows

def combine_group_payload(rows: list[DiscordMessage]) -> tuple[str, list[str], list[int]]:
    combined_parts = []
    combined_attachments = []
    row_ids = []

    for i, r in enumerate(rows, start=1):
        text = (r.content or "").strip()
        if text:
            combined_parts.append(f"Message {i}: {text}")
        else:
            combined_parts.append(f"Message {i}: [no text]")

        combined_attachments.extend(json.loads(r.attachment_urls_json or "[]"))
        row_ids.append(r.id)

    combined_text = "\n\n".join(combined_parts)
    return combined_text, combined_attachments, row_ids
def normalize_text(text: str) -> str:
    return (text or "").strip().lower()


def has_images(row: DiscordMessage) -> bool:
    return bool(json.loads(row.attachment_urls_json or "[]"))


def same_author(left: DiscordMessage, right: DiscordMessage) -> bool:
    left_author_id = (left.author_id or "").strip()
    right_author_id = (right.author_id or "").strip()
    if left_author_id and right_author_id:
        return left_author_id == right_author_id

    left_author_name = (left.author_name or "").strip().lower()
    right_author_name = (right.author_name or "").strip().lower()
    return bool(left_author_name and right_author_name and left_author_name == right_author_name)


def is_payment_only_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card)\s*\$?\d+(?:\.\d{1,2})?$",
        r"^\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)$",
        r"^\+\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)?$",
        r"^(plus|\+)\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)?$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_trade_fragment_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r".*\b(in|out)\b.*",
        r"^(top|bottom|left|right).*$",
        r"^.*\bplus\b.*$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_explicit_buy_sell_text(text: str) -> bool:
    text = normalize_text(text)
    has_explicit_verb = bool(re.search(r"\b(sold|sell|bought|buy|paid)\b", text, re.I))
    has_payment_amount = bool(
        re.search(r"\b(zelle|venmo|paypal|cash|tap|card)\s*\$?\d+(?:\.\d{1,2})?\b", text, re.I)
        or re.search(r"\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)\b", text, re.I)
    )
    has_non_quantity_number = bool(
        re.search(r"\b(sold|sell|bought|buy|paid)\b.*\b\d+(?:\.\d{1,2})?\b(?!\s*(box|boxes|pack|packs|slab|slabs|case|cases|card|cards|binder|binders|lot|lots)\b)", text, re.I)
    )
    return has_payment_amount or (has_explicit_verb and has_non_quantity_number)


def is_short_fragment(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if has_images(row) and len(text) <= 20:
        return True
    if is_payment_only_text(text):
        return True
    if is_trade_fragment_text(text) and len(text) <= 50:
        return True
    return False


def looks_like_complete_deal(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    complete = is_explicit_buy_sell_text(text)

    # image + substantial text can also be a complete standalone log
    if has_images(row) and len(text) >= 25:
        return True

    return complete


def contains_amount(text: str) -> bool:
    return bool(re.search(r"\$?\d+(?:\.\d{1,2})?", normalize_text(text)))


def has_descriptive_text(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if not text or is_payment_only_text(text):
        return False
    return len(text) >= 8


def stitch_profile(rows: list[DiscordMessage]) -> dict[str, int]:
    profile = {
        "images": 0,
        "payment_fragments": 0,
        "descriptions": 0,
        "trade_fragments": 0,
    }

    for row in rows:
        text = normalize_text(row.content)
        if has_images(row):
            profile["images"] += 1
        if is_payment_only_text(text):
            profile["payment_fragments"] += 1
        if has_descriptive_text(row):
            profile["descriptions"] += 1
        if is_trade_fragment_text(text):
            profile["trade_fragments"] += 1

    return profile


def stitch_group_needs_more_context(rows: list[DiscordMessage]) -> bool:
    profile = stitch_profile(rows)

    if profile["descriptions"] >= 1 and profile["payment_fragments"] >= 1:
        return False
    if (
        profile["trade_fragments"] >= 1
        and profile["payment_fragments"] >= 1
        and (profile["images"] >= 1 or profile["descriptions"] >= 1)
    ):
        return False
    if (
        profile["images"] >= 1
        and profile["descriptions"] >= 1
        and profile["payment_fragments"] >= 1
    ):
        return False

    return True


def candidate_improves_group(group_rows: list[DiscordMessage], candidate: DiscordMessage) -> bool:
    before = stitch_profile(group_rows)
    after = stitch_profile(group_rows + [candidate])

    if after == before:
        return False

    return (
        (before["images"] == 0 and after["images"] > before["images"])
        or (before["payment_fragments"] == 0 and after["payment_fragments"] > before["payment_fragments"])
        or (before["descriptions"] == 0 and after["descriptions"] > before["descriptions"])
        or (
            before["trade_fragments"] == 0
            and after["trade_fragments"] > before["trade_fragments"]
            and stitch_group_needs_more_context(group_rows)
        )
    )


def should_force_stitch(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) != 2:
        return False

    sorted_rows = sorted(candidate_rows, key=lambda candidate: candidate.created_at)
    first_row, second_row = sorted_rows
    first_text = normalize_text(first_row.content)
    second_text = normalize_text(second_row.content)

    if has_large_gap(sorted_rows, max_gap_seconds=8):
        return False

    if has_images(first_row) and len(first_text) <= 20 and is_explicit_buy_sell_text(second_text):
        return True

    return False


def has_large_gap(candidate_rows: list[DiscordMessage], max_gap_seconds: int = 12) -> bool:
    if len(candidate_rows) <= 1:
        return False

    sorted_rows = sorted(candidate_rows, key=lambda row: row.created_at)
    for previous, current in zip(sorted_rows, sorted_rows[1:]):
        gap = abs((current.created_at - previous.created_at).total_seconds())
        if gap > max_gap_seconds:
            return True
    return False


def should_stitch_rows(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) <= 1:
        return False

    if should_force_stitch(base_row, candidate_rows):
        return True

    payment_fragments = 0
    short_fragments = 0
    complete_deals = 0
    rows_with_images = 0
    amount_mentions = 0

    for r in candidate_rows:
        text = normalize_text(r.content)
        if is_payment_only_text(text):
            payment_fragments += 1
        if is_short_fragment(r):
            short_fragments += 1
        if looks_like_complete_deal(r):
            complete_deals += 1
        if has_images(r):
            rows_with_images += 1
        if contains_amount(text):
            amount_mentions += 1

    if has_large_gap(candidate_rows):
        return False

    # Too many full standalone deals close together -> do not stitch
    if complete_deals >= 2:
        return False

    # Two image posts close together are often separate showroom deals
    if rows_with_images >= 2 and short_fragments < len(candidate_rows):
        return False

    # Multiple amount mentions usually mean multiple separate deals unless one row is clearly just a payment fragment.
    if amount_mentions >= 2 and payment_fragments == 0:
        return False

    # More than one payment fragment usually means multiple separate deals
    if payment_fragments >= 2:
        return False

    # If the base row itself already looks complete, only stitch when the neighbor is clearly a short fragment.
    if looks_like_complete_deal(base_row) and short_fragments <= 1:
        return False

    # Stitch only if there is at least one short/incomplete fragment
    if short_fragments == 0:
        return False

    # Good common case:
    # one image/incomplete row + one payment/direction fragment
    return True
