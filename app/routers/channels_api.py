"""
Channels & Messages API routes.

Extracted from app/main.py -- JSON APIs for channels, messages, parser
progress, queue state counts, plus the message detail redirect, edit form,
and correction-promote form.
"""
from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from ..csrf import CSRFProtectedRoute
from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..discord.channels import get_channel_filter_choices
from ..discord.corrections import promote_correction_pattern, save_review_correction, snapshot_message_parse
from ..db import get_session
from ..models import (
    DiscordMessage,
    PARSE_PARSED,
    PARSE_REVIEW_REQUIRED,
    normalize_parse_status,
    utcnow,
)
from ..discord.transactions import sync_transaction_from_message

router = APIRouter(route_class=CSRFProtectedRoute)


@router.get("/channels")
def list_channels(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    return get_channel_filter_choices(session)

@router.get("/messages")
def list_messages(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    return {
        "rows": build_message_list_items(session, rows),
        "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
    }

@router.get("/api/review")
def review_queue_api(
    request: Request,
    status: Optional[str] = Query(default="review_queue"),
    channel_id: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    return {
        "rows": build_message_list_items(session, rows),
        "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
    }

@router.get("/messages/{message_id}")
def get_message(request: Request, message_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    target = f"/deals/{message_id}" if row else "/deals"
    return RedirectResponse(url=target, status_code=301)

@router.get("/admin/parser-progress")
def admin_parser_progress(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return get_parser_progress(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
    )

@router.get("/admin/queue-state-counts")
def admin_queue_state_counts(
    request: Request,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    entry_kind: Optional[str] = None,
    expense_category: Optional[str] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return {
        "counts": get_summary(
            session,
            status=status,
            channel_id=channel_id,
            entry_kind=entry_kind,
            expense_category=expense_category,
            after=after,
            before=before,
        ),
        "progress": get_parser_progress(
            session,
            status=status,
            channel_id=channel_id,
            entry_kind=entry_kind,
            expense_category=expense_category,
            after=after,
            before=before,
        ),
    }

@router.get("/table/messages/{message_id}", response_class=HTMLResponse)
def message_detail_page(
    message_id: int,
    request: Request,
    return_path: str = Query(default="/table"),
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=None),
    limit: Optional[int] = Query(default=None),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.get(DiscordMessage, message_id)
    target = "/deals"
    if row:
        target = build_message_detail_url(
            message_id,
            return_path=return_path,
            status=status,
            channel_id=channel_id,
            expense_category=expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
        separator = "&" if "?" in target else "?"
        if success:
            target = f"{target}{separator}success={success}"
            separator = "&"
        if error:
            target = f"{target}{separator}error={error}"
    return RedirectResponse(url=target, status_code=301)

@router.post("/admin/corrections/promote-form")
def promote_correction_form(
    request: Request,
    normalized_text: str = Form(...),
    return_to: str = Form(default="/table"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    promoted_count = promote_correction_pattern(session, normalized_text)
    session.commit()
    separator = "&" if "?" in return_to else "?"
    success = f"Promoted {promoted_count} correction pattern(s) into parser memory."
    return RedirectResponse(url=f"{return_to}{separator}success={success}", status_code=303)

@router.post("/messages/{message_id}/edit-form")
def edit_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    parse_status: str = Form(default=PARSE_PARSED),
    needs_review: Optional[str] = Form(default=None),
    deal_type: Optional[str] = Form(default=None),
    amount: Optional[str] = Form(default=None),
    payment_method: Optional[str] = Form(default=None),
    cash_direction: Optional[str] = Form(default=None),
    category: Optional[str] = Form(default=None),
    entry_kind: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    confidence: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    trade_summary: Optional[str] = Form(default=None),
    item_names_text: Optional[str] = Form(default=None),
    items_in_text: Optional[str] = Form(default=None),
    items_out_text: Optional[str] = Form(default=None),
    approve_after_save: Optional[str] = Form(default=None),
    stay_on_detail: Optional[str] = Form(default=None),
    review_action: Optional[str] = Form(default=None),
    next_message_id: Optional[int] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if not isinstance(review_action, str):
        review_action = None
    if not isinstance(next_message_id, int):
        next_message_id = None
    reviewer_label = current_user_label(request)
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")
    parsed_before = snapshot_message_parse(row)

    try:
        parsed_amount = parse_optional_float(amount)
        parsed_confidence = parse_optional_float(confidence)
    except ValueError:
        detail_url = build_message_detail_url(
            message_id,
            return_path=return_path,
            status=status,
            channel_id=channel_id,
            expense_category=filter_expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
        separator = "&" if "?" in detail_url else "?"
        return RedirectResponse(
            url=f"{detail_url}{separator}error=Amount+and+confidence+must+be+valid+numbers",
            status_code=303,
        )

    row.parse_status = normalize_parse_status(parse_status or PARSE_PARSED)
    row.needs_review = bool(needs_review)
    if row.parse_status == PARSE_REVIEW_REQUIRED or row.needs_review:
        row.parse_status = PARSE_REVIEW_REQUIRED
        row.needs_review = True
    elif row.parse_status == PARSE_PARSED:
        row.needs_review = False

    normalized_deal_type = (deal_type or "").strip() or None
    normalized_payment_method = (payment_method or "").strip() or None
    normalized_cash_direction = row.cash_direction if cash_direction is None else ((cash_direction or "").strip() or None)
    normalized_category = (category or "").strip() or None
    normalized_entry_kind = (entry_kind or "").strip() or None
    normalized_expense_category = (expense_category or "").strip() or None
    if normalized_deal_type != "trade":
        normalized_cash_direction = None

    row.deal_type = normalized_deal_type
    row.amount = parsed_amount
    row.payment_method = normalized_payment_method
    row.cash_direction = normalized_cash_direction
    row.category = normalized_category
    row.notes = (notes or "").strip() or None
    row.trade_summary = (trade_summary or "").strip() or None
    row.confidence = parsed_confidence
    row.item_names_json = json.dumps(parse_string_list(item_names_text))
    row.items_in_json = json.dumps(parse_string_list(items_in_text))
    row.items_out_json = json.dumps(parse_string_list(items_out_text))

    entry_kind_value, money_in, money_out, expense_category_value = compute_manual_financials(
        row=row,
        deal_type=normalized_deal_type,
        category=normalized_category,
        amount=parsed_amount,
        cash_direction=normalized_cash_direction,
        entry_kind_override=normalized_entry_kind,
        expense_category_override=normalized_expense_category,
    )
    row.entry_kind = entry_kind_value
    row.money_in = money_in
    row.money_out = money_out
    row.expense_category = expense_category_value
    if approve_after_save:
        row.parse_status = PARSE_PARSED
        row.needs_review = False
    if row.parse_status == PARSE_PARSED and not row.needs_review:
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
    elif row.parse_status != PARSE_PARSED or row.needs_review:
        row.reviewed_by = None
        row.reviewed_at = None
    row.last_error = None if row.parse_status in {PARSE_PARSED, PARSE_REVIEW_REQUIRED} else row.last_error

    normalized_review_action = (review_action or "").strip().lower()
    if normalized_review_action == "approve_next":
        approve_after_save = "true"
        row.parse_status = PARSE_PARSED
        row.needs_review = False
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()

    session.add(row)
    save_review_correction(session, row, parsed_before=parsed_before)
    sync_transaction_from_message(session, row)
    session.commit()

    if normalized_review_action in {"save_next", "approve_next"} and next_message_id:
        redirect_target = build_return_url(
            f"/review/focus/{next_message_id}",
            channel_id=channel_id,
            expense_category=filter_expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
    elif stay_on_detail:
        redirect_target = build_message_detail_url(
            message_id,
            return_path=return_path,
            status=status,
            channel_id=channel_id,
            expense_category=filter_expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
    else:
        redirect_target = build_return_url(
            return_path,
            status=status,
            channel_id=channel_id,
            expense_category=filter_expense_category,
            after=after,
            before=before,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            limit=limit,
        )
    separator = "&" if "?" in redirect_target else "?"
    success_message = (
        f"Saved+manual+correction+and+approved+message+{message_id}"
        if approve_after_save
        else f"Saved+manual+correction+for+message+{message_id}"
    )
    return RedirectResponse(
        url=f"{redirect_target}{separator}success={success_message}",
        status_code=303,
    )
