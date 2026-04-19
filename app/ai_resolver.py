"""
AI review-resolver agent.

Background job that takes a DiscordMessage sitting in `review_required` and
asks a heavy LLM to resolve it using richer context than the first-pass
parser had access to:

 - The row's text + attached images
 - The previous parse output (deal_type, amount, etc.) and any rule/AI
   disagreement that was recorded
 - The author's last N messages in the same channel (time-ordered)
 - Nearby sibling messages from the same author within +/- 2 minutes
 - The most-similar past ReviewCorrection rows (by token overlap)
 - Store-rule guidance text baked in as a system prompt

The model is asked to reply with a structured JSON object. If the model
is confident (>= AI_RESOLVER_AUTO_CONFIDENCE) AND its `resolution` is
`auto_resolve`, the row is updated in place, its transaction sync is
refreshed, and a ReviewCorrection is written with
correction_source = "ai_resolver". Otherwise the resolver's reasoning
is stored on `ai_resolver_reasoning_json` so the human reviewer sees
the agent's analysis next to the raw row.

This module is deliberately single-purpose: it does not retry, does not
stitch, and never modifies rows that have already been human-reviewed.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlmodel import Session, select

from .ai_client import get_ai_client, get_model, is_nvidia
from .config import get_settings
from .corrections import (
    get_relevant_correction_hints,
    save_review_correction,
    snapshot_message_parse,
)
from .db import managed_session
from .display_media import (
    encode_attachment_asset_as_vision_data_url,
    stable_attachment_url_key,
)
from .models import (
    PARSE_PARSED,
    PARSE_REVIEW_REQUIRED,
    AttachmentAsset,
    DiscordMessage,
    ReviewCorrection,
    normalize_parse_status,
    utcnow,
)
from .parser import is_image_url

# Cap on images sent to the resolver per row. The primary row's images
# come first; any remaining budget is filled with sibling images
# (which often carry the other half of a stitched deal). Claude Opus
# tolerates many more, but keeping this bounded limits token cost and
# keeps the prompt reviewable in logs.
MAX_PRIMARY_IMAGES = 3
MAX_SIBLING_IMAGES = 3

logger = logging.getLogger(__name__)
settings = get_settings()

CORRECTION_SOURCE_AI_RESOLVER = "ai_resolver"


STORE_RULES_SYSTEM_PROMPT = """
You are a deal-parsing resolver for a sports card / TCG store.

Store conventions you MUST apply:
 - "out" means items leaving the store; "in" means items coming into the store.
 - "top out bottom in" (or any side + in/out combo) is a trade.
 - For trades, "plus 195 zelle", "+ 50 cash", or "& $100" typically means
   the store RECEIVED that amount (cash_direction = "to_store"). If the
   text clearly says the store paid out (e.g. "gave 50 cash"), that is
   from_store.
 - "tap", "cc", "dc" all normalize to payment_method = "card".
 - "Apple Pay", "ApplePay", "Appstd" all normalize to payment_method = "apple_pay".
 - A payment-only message like "$11 zelle" or "zelle 11" by itself
   defaults to a SELL (the store received money) unless other context
   clearly says otherwise.
 - "2x Ninja Spinner" is a quantity, not a $2 amount. Never extract
   quantity multipliers as monetary amounts.
 - Card buys, sells, and trades should use expense_category = "inventory".
 - Emoji-only, "wrong chat", and short conversational messages with no
   numbers or images should be ignored (resolution "auto_resolve" with
   parse = null and ignore = true).

Given a row that was flagged for review, along with surrounding context,
decide whether you can resolve it confidently. Reply with a JSON object:

{
  "resolution": "auto_resolve" | "needs_human",
  "confidence": 0.0-1.0,
  "reasoning": "short natural-language explanation referencing the
    specific context signals you used",
  "parse": {
    "parsed_type": "buy" | "sell" | "trade" | null,
    "parsed_amount": number | null,
    "parsed_payment_method": "cash"|"zelle"|"venmo"|"paypal"|"card"|"apple_pay"|"unknown"|null,
    "parsed_cash_direction": "to_store" | "from_store" | "none" | null,
    "parsed_category": string | null,
    "parsed_items": [string, ...],
    "parsed_items_in": [string, ...],
    "parsed_items_out": [string, ...],
    "parsed_trade_summary": string,
    "parsed_notes": string,
    "ignore": boolean
  }
}

Only use resolution "auto_resolve" when you are >= 0.90 confident and
the parse is fully specified. Otherwise return "needs_human" with a
reasoning note describing exactly what ambiguity remains so the human
reviewer can resolve it faster.

Never invent items or amounts. If a field is not clearly supported by
the text or surrounding context, leave it null.
""".strip()


@dataclass
class ResolverContext:
    primary: DiscordMessage
    siblings_before: list[DiscordMessage]
    siblings_after: list[DiscordMessage]
    author_history: list[DiscordMessage]
    correction_hints: list[dict[str, Any]]
    # (url, source_label) tuples in the order they should appear in the
    # multipart prompt. source_label lets the model know whether a given
    # image is from the primary row or a sibling.
    images: list[tuple[str, str]]


def _row_image_urls(row: DiscordMessage) -> list[str]:
    try:
        attachments = json.loads(row.attachment_urls_json or "[]")
    except (json.JSONDecodeError, TypeError):
        return []
    return [url for url in attachments if isinstance(url, str) and is_image_url(url)]


def _encode_cached_image(
    session: Session, message_id: int, url: str
) -> str | None:
    """Return a base64 data URL for a cached attachment, or None if not cached.

    Bedrock-hosted Claude (via NVIDIA) does NOT accept URL content sources
    and returns HTTP 400 "URL content sources are not yet supported for
    this model" when we pass a raw Discord URL. Instead we look up the
    attachment bytes in AttachmentAsset (populated at ingest time) and
    send them inline as a base64 data URL, shrinking any oversized
    images to fit Bedrock's 5 MiB per-image cap.

    Match is done on the stable portion of the URL (see
    ``stable_attachment_url_key``) so rows whose live Discord URL has
    been re-signed since caching still find their asset.
    """
    target_key = stable_attachment_url_key(url)
    if not target_key:
        return None

    candidates = session.exec(
        select(AttachmentAsset)
        .where(AttachmentAsset.message_id == message_id)
        .where(AttachmentAsset.is_image == True)  # noqa: E712
    ).all()

    asset: AttachmentAsset | None = None
    for candidate in candidates:
        if stable_attachment_url_key(candidate.source_url) == target_key:
            asset = candidate
            break

    if asset is None:
        return None
    return encode_attachment_asset_as_vision_data_url(asset)


def _collect_context_images(
    session: Session,
    primary: DiscordMessage,
    siblings_before: list[DiscordMessage],
    siblings_after: list[DiscordMessage],
) -> list[tuple[str, str]]:
    """Gather image inputs for the resolver as (data_url, source_label) tuples.

    Only images cached in AttachmentAsset are included. Discord URLs
    expire and are also rejected by the Bedrock-hosted model; relying on
    the cache keeps the resolver both portable and robust.
    """
    images: list[tuple[str, str]] = []

    for url in _row_image_urls(primary)[:MAX_PRIMARY_IMAGES]:
        data_url = _encode_cached_image(session, primary.id, url)
        if data_url is None:
            continue
        images.append((data_url, f"primary row id={primary.id}"))

    remaining = MAX_SIBLING_IMAGES
    for row in siblings_before + siblings_after:
        if remaining <= 0:
            break
        sibling_urls = _row_image_urls(row)
        if not sibling_urls:
            continue
        # Take only the first cached image per sibling so one image-heavy
        # row doesn't starve the budget.
        for url in sibling_urls:
            data_url = _encode_cached_image(session, row.id, url)
            if data_url is None:
                continue
            images.append(
                (
                    data_url,
                    f"sibling row id={row.id} at {row.created_at.isoformat() if row.created_at else 'unknown'}",
                )
            )
            remaining -= 1
            break

    return images


def _serialize_message_for_prompt(row: DiscordMessage) -> dict[str, Any]:
    return {
        "id": row.id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "author": row.author_name,
        "content": (row.content or "").strip(),
        "has_images": bool(json.loads(row.attachment_urls_json or "[]")),
        "parse_status": normalize_parse_status(
            row.parse_status,
            is_deleted=row.is_deleted,
            needs_review=row.needs_review,
        ),
        "deal_type": row.deal_type,
        "amount": row.amount,
        "payment_method": row.payment_method,
        "cash_direction": row.cash_direction,
        "parsed_notes": row.notes,
    }


def _build_context(session: Session, row: DiscordMessage) -> ResolverContext:
    window_before = row.created_at - timedelta(minutes=2)
    window_after = row.created_at + timedelta(minutes=2)

    siblings_before = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id == row.channel_id)
        .where(DiscordMessage.author_id == row.author_id)
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(DiscordMessage.id != row.id)
        .where(DiscordMessage.created_at >= window_before)
        .where(DiscordMessage.created_at < row.created_at)
        .order_by(DiscordMessage.created_at.asc())
        .limit(5)
    ).all()

    siblings_after = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id == row.channel_id)
        .where(DiscordMessage.author_id == row.author_id)
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(DiscordMessage.id != row.id)
        .where(DiscordMessage.created_at > row.created_at)
        .where(DiscordMessage.created_at <= window_after)
        .order_by(DiscordMessage.created_at.asc())
        .limit(5)
    ).all()

    history_limit = max(settings.ai_resolver_max_context_messages, 0)
    author_history: list[DiscordMessage] = []
    if history_limit > 0:
        author_history = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.channel_id == row.channel_id)
            .where(DiscordMessage.author_id == row.author_id)
            .where(DiscordMessage.is_deleted == False)  # noqa: E712
            .where(DiscordMessage.id != row.id)
            .where(DiscordMessage.created_at < window_before)
            .order_by(DiscordMessage.created_at.desc())
            .limit(history_limit)
        ).all()
        author_history.reverse()

    correction_hints = get_relevant_correction_hints(
        row.content or "",
        limit=max(settings.ai_resolver_max_correction_hints, 0),
    )

    siblings_before_list = list(siblings_before)
    siblings_after_list = list(siblings_after)
    images = _collect_context_images(session, row, siblings_before_list, siblings_after_list)

    return ResolverContext(
        primary=row,
        siblings_before=siblings_before_list,
        siblings_after=siblings_after_list,
        author_history=list(author_history),
        correction_hints=correction_hints,
        images=images,
    )


def _build_prompt(context: ResolverContext) -> str:
    primary = context.primary

    primary_attachments = json.loads(primary.attachment_urls_json or "[]")
    payload = {
        "primary_message": _serialize_message_for_prompt(primary),
        "primary_attachment_urls": primary_attachments,
        "previous_parse_disagreement": json.loads(primary.parse_disagreement_json or "null"),
        "siblings_before": [_serialize_message_for_prompt(r) for r in context.siblings_before],
        "siblings_after": [_serialize_message_for_prompt(r) for r in context.siblings_after],
        "author_history": [_serialize_message_for_prompt(r) for r in context.author_history],
        "correction_hints": context.correction_hints,
    }
    if context.images:
        # Only include the label in the text prompt. The actual image
        # bytes are attached via multipart image_url blocks below, so
        # echoing a base64 data URL into the text payload would waste
        # tokens (and make logs unreadable).
        payload["attached_images"] = [
            {"index": idx, "source": label}
            for idx, (_url, label) in enumerate(context.images, start=1)
        ]

    lines = [
        "Resolve this review_required row using the provided context.",
        "Return ONLY a single JSON object as specified in the system prompt.",
    ]
    if context.images:
        lines.append(
            f"{len(context.images)} image(s) are attached below the JSON. "
            "Each image's 'source' field in attached_images tells you which "
            "row it belongs to (primary or sibling)."
        )
    lines.append("")
    lines.append("CONTEXT:")
    lines.append(json.dumps(payload, indent=2, default=str))
    return "\n".join(lines)


def _build_user_content(prompt: str, images: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """Build a chat.completions multipart user-content array.

    When no images are attached, returns a single text part. When images
    are present, returns [text, image_url, image_url, ...] so the model
    can see both the context JSON and the actual images.
    """
    content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
    for url, _label in images:
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": url, "detail": "auto"},
            }
        )
    return content


def _call_resolver_model(
    prompt: str,
    images: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    client = get_ai_client().with_options(timeout=90.0)
    model = get_model()
    user_content = _build_user_content(prompt, images or [])

    # Both providers use chat.completions with the multipart content form
    # so vision works uniformly (NVIDIA Claude supports image_url blocks;
    # OpenAI GPT-5 does too). The OpenAI Responses API path was
    # intentionally omitted to keep the resolver provider-portable.
    _ = is_nvidia  # referenced to keep the import meaningful in logs
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": STORE_RULES_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        response_format={"type": "json_object"},
        max_tokens=2048,
    )
    raw_text = response.choices[0].message.content or "{}"

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        logger.warning("ai_resolver: model returned non-JSON output: %s", exc)
        return {
            "resolution": "needs_human",
            "confidence": 0.0,
            "reasoning": f"resolver model returned non-JSON output: {exc}",
            "parse": None,
        }


def _apply_resolution(
    session: Session,
    row: DiscordMessage,
    response: dict[str, Any],
) -> bool:
    """Apply the resolver response to the row. Returns True if auto-resolved."""
    resolution = (response.get("resolution") or "").lower()
    confidence = float(response.get("confidence") or 0.0)
    parse = response.get("parse") or {}
    reasoning = response.get("reasoning") or ""

    auto_threshold = float(settings.ai_resolver_auto_confidence)
    auto_resolved = (
        resolution == "auto_resolve"
        and confidence >= auto_threshold
        and isinstance(parse, dict)
    )

    reasoning_payload = {
        "resolution": resolution,
        "confidence": confidence,
        "reasoning": reasoning,
        "proposed_parse": parse if isinstance(parse, dict) else None,
        "resolved_at": utcnow().isoformat(),
        "model": get_model(),
        "images_sent": int(response.get("_images_sent") or 0),
    }
    row.ai_resolver_reasoning_json = json.dumps(reasoning_payload, sort_keys=True)

    if not auto_resolved:
        session.add(row)
        return False

    if parse.get("ignore"):
        # The resolver concluded this row is non-transactional noise.
        row.deal_type = None
        row.amount = None
        row.payment_method = None
        row.cash_direction = None
        row.category = None
        row.item_names_json = "[]"
        row.items_in_json = "[]"
        row.items_out_json = "[]"
        row.trade_summary = None
        row.notes = (reasoning or "auto-resolved as non-transaction by AI resolver")[:500]
        row.confidence = confidence
        row.needs_review = False
        row.parse_status = "ignored"
        session.add(row)
        return True

    parsed_before = snapshot_message_parse(row)

    row.deal_type = parse.get("parsed_type")
    row.amount = parse.get("parsed_amount")
    row.payment_method = parse.get("parsed_payment_method")
    row.cash_direction = parse.get("parsed_cash_direction") if parse.get("parsed_type") == "trade" else None
    row.category = parse.get("parsed_category")
    row.item_names_json = json.dumps(list(parse.get("parsed_items") or []))
    row.items_in_json = json.dumps(list(parse.get("parsed_items_in") or []))
    row.items_out_json = json.dumps(list(parse.get("parsed_items_out") or []))
    row.trade_summary = parse.get("parsed_trade_summary") or None
    new_notes = (parse.get("parsed_notes") or "").strip()
    row.notes = (f"{new_notes} | resolved by ai_resolver: {reasoning}" if new_notes else f"resolved by ai_resolver: {reasoning}")[:1000]
    row.confidence = max(confidence, 0.95)
    row.needs_review = False
    row.parse_status = PARSE_PARSED
    row.reviewed_by = "ai_resolver"
    row.reviewed_at = utcnow()
    session.add(row)

    correction = save_review_correction(session, row, parsed_before=parsed_before)
    if correction is not None:
        correction.correction_source = CORRECTION_SOURCE_AI_RESOLVER
        session.add(correction)

    return True


def resolve_one(session: Session, row_id: int) -> dict[str, Any] | None:
    row = session.get(DiscordMessage, row_id)
    if row is None:
        return None

    current_status = normalize_parse_status(
        row.parse_status,
        is_deleted=row.is_deleted,
        needs_review=row.needs_review,
    )
    if current_status != PARSE_REVIEW_REQUIRED or row.reviewed_at is not None:
        return None

    context = _build_context(session, row)
    prompt = _build_prompt(context)

    try:
        response = _call_resolver_model(prompt, images=context.images)
    except Exception as exc:
        logger.warning("ai_resolver: model call failed for row %s: %s", row_id, exc)
        reasoning_payload = {
            "resolution": "needs_human",
            "confidence": 0.0,
            "reasoning": f"resolver model call failed: {exc}",
            "proposed_parse": None,
            "resolved_at": utcnow().isoformat(),
            "model": get_model(),
        }
        row.ai_resolver_reasoning_json = json.dumps(reasoning_payload, sort_keys=True)
        session.add(row)
        return reasoning_payload

    response["_images_sent"] = len(context.images)
    auto_resolved = _apply_resolution(session, row, response)
    return {
        "resolution": response.get("resolution"),
        "confidence": response.get("confidence"),
        "auto_resolved": auto_resolved,
        "row_id": row_id,
        "images_sent": len(context.images),
    }


def resolve_review_queue_once() -> dict[str, int]:
    """Resolve up to ai_resolver_batch_size pending review_required rows.

    Returns a summary dict with counts for logging. Each row is handled in
    its own session commit so a single model failure doesn't roll back
    good progress.
    """
    batch_size = max(int(settings.ai_resolver_batch_size), 1)
    min_age_cutoff = utcnow() - timedelta(minutes=max(int(settings.ai_resolver_min_age_minutes), 0))

    summary = {
        "scanned": 0,
        "auto_resolved": 0,
        "needs_human": 0,
        "errors": 0,
    }

    with managed_session() as session:
        candidates = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.parse_status == PARSE_REVIEW_REQUIRED)
            .where(DiscordMessage.needs_review == True)  # noqa: E712
            .where(DiscordMessage.is_deleted == False)  # noqa: E712
            .where(DiscordMessage.reviewed_at == None)  # noqa: E711
            .where(DiscordMessage.ai_resolver_reasoning_json == None)  # noqa: E711
            .where(DiscordMessage.created_at <= min_age_cutoff)
            .order_by(DiscordMessage.created_at.desc())
            .limit(batch_size)
        ).all()
        candidate_ids = [c.id for c in candidates if c.id is not None]

    for row_id in candidate_ids:
        summary["scanned"] += 1
        try:
            with managed_session() as session:
                result = resolve_one(session, row_id)
                if result is None:
                    continue
                if result.get("auto_resolved"):
                    summary["auto_resolved"] += 1
                else:
                    summary["needs_human"] += 1
        except Exception as exc:
            summary["errors"] += 1
            logger.warning("ai_resolver: unexpected failure on row %s: %s", row_id, exc, exc_info=True)

    return summary


async def ai_review_resolver_loop(stop_event: asyncio.Event) -> None:
    if not settings.ai_resolver_enabled or not settings.parser_worker_enabled:
        return

    interval_minutes = max(float(settings.ai_resolver_interval_minutes), 1.0)
    # Stagger initial run slightly so it doesn't hammer the first-pass
    # parser on startup.
    await asyncio.sleep(60)

    while not stop_event.is_set():
        try:
            summary = await asyncio.to_thread(resolve_review_queue_once)
            if summary["scanned"]:
                logger.info("ai_resolver loop summary: %s", summary)
        except Exception as exc:
            logger.warning("ai_resolver loop tick failed: %s", exc, exc_info=True)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_minutes * 60)
        except asyncio.TimeoutError:
            continue
