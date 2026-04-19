"""
Message action and table/review routes.

Extracted from app/main.py -- /messages/*, /table, /review-table, /review, /review/*.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

_REVIEW_DEFAULT_TZ = ZoneInfo("America/Los_Angeles")

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, select

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..backfill_requests import list_recent_backfill_requests
from ..channels import get_available_channel_choices
from ..corrections import save_review_correction, snapshot_message_parse
from ..db import get_session

router = APIRouter()


@router.post("/messages/{message_id}/retry")
def retry_message(request: Request, message_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    row.parse_status = PARSE_PENDING
    row.parse_attempts = 0
    row.last_error = None
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    return {"ok": True, "message": f"Message {message_id} re-queued for parsing."}

@router.post("/messages/{message_id}/approve")
def approve_message(request: Request, message_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    parsed_before = snapshot_message_parse(row)
    row.needs_review = False
    row.parse_status = PARSE_PARSED
    session.add(row)
    save_review_correction(session, row, parsed_before=parsed_before)
    sync_transaction_from_message(session, row)
    session.commit()
    return {"ok": True, "message": f"Message {message_id} approved."}

@router.post("/messages/{message_id}/approve-form")
def approve_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reviewer_label = current_user_label(request)
    row = session.get(DiscordMessage, message_id)
    if row:
        parsed_before = snapshot_message_parse(row)
        row.needs_review = False
        row.parse_status = PARSE_PARSED
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
        session.add(row)
        save_review_correction(session, row, parsed_before=parsed_before)
        sync_transaction_from_message(session, row)
        session.commit()
    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Approved+message+{message_id}", status_code=303)

@router.post("/messages/bulk/approve-form")
def bulk_approve_messages_form(
    request: Request,
    message_ids: list[int] = Form(default=[]),
    return_path: str = Form(default="/review-table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reviewer_label = current_user_label(request)
    updated = 0
    for message_id in message_ids:
        row = session.get(DiscordMessage, message_id)
        if not row:
            continue
        parsed_before = snapshot_message_parse(row)
        row.needs_review = False
        row.parse_status = PARSE_PARSED
        row.reviewed_by = reviewer_label
        row.reviewed_at = utcnow()
        session.add(row)
        save_review_correction(session, row, parsed_before=parsed_before)
        sync_transaction_from_message(session, row)
        updated += 1
    session.commit()

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Approved+{updated}+messages", status_code=303)

@router.post("/messages/bulk/reparse-form")
@router.post("/messages/bulk/retry-form")
def bulk_reparse_messages_form(
    request: Request,
    message_ids: list[int] = Form(default=[]),
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rows = [
        row
        for row in (
            session.get(DiscordMessage, message_id)
            for message_id in message_ids
        )
        if row is not None
    ]
    updated = reparse_message_rows(session, rows, reason="manual bulk reparse", reset_attempts=True)

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Reparsed+{updated}+messages", status_code=303)

@router.post("/messages/bulk/reparse-filtered-form")
@router.post("/messages/bulk/requeue-filtered-form")
def bulk_reparse_filtered_messages_form(
    request: Request,
    return_path: str = Form(default="/review"),
    status: Optional[str] = Form(default="review_queue"),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial

    stmt = build_message_stmt(
        status=status or "review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    row_ids = [
        row_id
        for row_id in session.exec(stmt.with_only_columns(DiscordMessage.id)).all()
        if row_id is not None
    ]

    def reparse_chunk(chunk_ids: list[int]) -> int:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.id.in_(chunk_ids))
        ).all()
        return reparse_message_rows(session, rows, reason="manual filtered reparse", reset_attempts=True)

    updated = 0
    chunk_size = 25
    for start_index in range(0, len(row_ids), chunk_size):
        updated += reparse_chunk(row_ids[start_index:start_index + chunk_size])

    redirect_url = build_return_url(
        return_path,
        status=status if return_path not in {"/review", "/review-table"} else None,
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(
        url=f"{redirect_url}{separator}success=Reparsed+{updated}+filtered+review+rows",
        status_code=303,
    )

@router.post("/messages/{message_id}/reparse-form")
@router.post("/messages/{message_id}/retry-form")
def reparse_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    reparse_message_row(session, message_id, reason="manual row reparse", reset_attempts=True)

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(url=f"{redirect_url}{separator}success=Reparsed+message+{message_id}", status_code=303)

@router.post("/messages/{message_id}/mark-incorrect-form")
def mark_incorrect_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    row.needs_review = True
    row.parse_status = PARSE_REVIEW_REQUIRED
    row.last_error = "Manually marked incorrect for review."
    row.reviewed_by = None
    row.reviewed_at = None
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    selected_expense_category = filter_expense_category or expense_category
    detail_url = build_return_url(
        f"/deals/{message_id}",
        status="review_queue" if return_path == "/review-table" else status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in detail_url else "?"
    return RedirectResponse(
        url=f"{detail_url}{separator}success=Marked+message+{message_id}+incorrect+and+sent+to+review",
        status_code=303,
    )

@router.post("/messages/{message_id}/disregard-form")
def disregard_message_form(
    request: Request,
    message_id: int,
    return_path: str = Form(default="/table"),
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    expense_category: Optional[str] = Form(default=None),
    filter_expense_category: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    sort_by: Optional[str] = Form(default=None),
    sort_dir: Optional[str] = Form(default=None),
    page: int = Form(default=1),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    clear_parsed_fields(row)
    row.parse_status = PARSE_IGNORED
    row.needs_review = False
    row.last_error = None
    row.reviewed_by = current_user_label(request)
    row.reviewed_at = utcnow()
    row.notes = "Manually disregarded in review."
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()

    selected_expense_category = filter_expense_category or expense_category
    redirect_url = build_return_url(
        return_path,
        status=status,
        channel_id=channel_id,
        expense_category=selected_expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    separator = "&" if "?" in redirect_url else "?"
    return RedirectResponse(
        url=f"{redirect_url}{separator}success=Disregarded+message+{message_id}",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Table / review pages
# ---------------------------------------------------------------------------

@router.get("/table", response_class=HTMLResponse)
def messages_table(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=REPORT_SOURCE_ALL),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    selected_source = normalize_report_source(source)
    shopify_table_only = selected_source == REPORT_SOURCE_SHOPIFY

    if shopify_table_only:
        rows = []
        total_rows = 0
        items = []
    else:
        rows, total_rows = get_message_rows(
            session,
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
        items = build_message_list_items(session, rows, expense_category=expense_category)
    channels = get_channel_filter_choices(session)
    expense_category_options = get_expense_category_filter_choices(session)
    summary = (
        get_summary(
            session,
            status=status,
            channel_id=channel_id,
            expense_category=expense_category,
            after=after,
            before=before,
        )
        if not shopify_table_only
        else {"total": 0, "parsed": 0, "needs_review": 0, "failed": 0, "queued": 0, "processing": 0, "ignored": 0, "with_images": 0}
    )
    watched_channels = get_watched_channels(session)
    available_discord_channels, has_live_available_discord_channels = get_available_channel_choices(session)
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    if shopify_table_only:
        financial_summary = build_financial_summary([])
    else:
        financial_rows = get_financial_rows(
            session,
            start=parse_report_datetime(after),
            end=parse_report_datetime(before, end_of_day=True),
            channel_id=channel_id,
        )
        if expense_category:
            financial_rows = [row for row in financial_rows if row.expense_category == expense_category]
        financial_summary = build_financial_summary(financial_rows)
    recent_backfill_requests = serialize_backfill_requests(list_recent_backfill_requests(session))
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    parser_progress = get_parser_progress(
        session,
        status=status,
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    review_shortcuts = []

    return templates.TemplateResponse(
        request,
        "messages_table.html",
        {
            "request": request,
            "title": "Messages Table",
            "return_path": "/table",
            "is_review_page": False,
            "rows": items,
            "channels": channels,
            "expense_category_options": expense_category_options,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_status": status or "",
            "selected_source": selected_source,
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by or "time",
            "selected_sort_dir": sort_dir or "desc",
            "selected_limit": limit,
            "pagination": pagination,
            "summary": summary,
            "financial_summary": financial_summary,
            "recent_backfill_requests": recent_backfill_requests,
            "parser_progress": parser_progress,
            "success": success,
            "error": error,
            "watched_channels": watched_channels,
            "watched_channel_groups": watched_channel_groups,
            "available_discord_channels": available_discord_channels,
            "has_live_available_discord_channels": has_live_available_discord_channels,
            "next_sort_direction": next_sort_direction,
            "sort_indicator": sort_indicator,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "review_shortcuts": review_shortcuts,
            "shopify_source_notice": {
                "link": build_shopify_orders_url(start=after or "", end=before or ""),
                "message": "Shopify orders are on the Shopify Orders page.",
            } if shopify_table_only else None,
        },
    )

@router.get("/review-table", response_class=HTMLResponse)
def review_table(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    rows, total_rows = get_message_rows(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows, expense_category=expense_category)
    channels = get_channel_filter_choices(session)
    expense_category_options = get_expense_category_filter_choices(session)
    summary = get_summary(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    financial_rows = get_financial_rows(session)
    if expense_category:
        financial_rows = [row for row in financial_rows if (row.expense_category or "") == expense_category]
    financial_summary = build_financial_summary(financial_rows)
    watched_channels = get_watched_channels(session)
    available_discord_channels, has_live_available_discord_channels = get_available_channel_choices(session)
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    recent_backfill_requests = serialize_backfill_requests(list_recent_backfill_requests(session))
    parser_progress = get_parser_progress(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    review_shortcuts = build_review_shortcuts(items)

    return templates.TemplateResponse(
        request,
        "messages_table.html",
        {
            "request": request,
            "title": "Review Queue",
            "return_path": "/review-table",
            "is_review_page": True,
            "rows": items,
            "channels": channels,
            "expense_category_options": expense_category_options,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_status": "review_queue",
            "selected_source": REPORT_SOURCE_DISCORD,
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "selected_limit": limit,
            "pagination": pagination,
            "summary": summary,
            "financial_summary": financial_summary,
            "recent_backfill_requests": recent_backfill_requests,
            "parser_progress": parser_progress,
            "success": success,
            "error": error,
            "watched_channels": watched_channels,
            "watched_channel_groups": watched_channel_groups,
            "available_discord_channels": available_discord_channels,
            "has_live_available_discord_channels": has_live_available_discord_channels,
            "next_sort_direction": next_sort_direction,
            "sort_indicator": sort_indicator,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "review_shortcuts": review_shortcuts,
            "shopify_source_notice": None,
        },
    )

@router.get("/review", response_class=HTMLResponse)
def reviewer_queue_page(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if "after" not in request.query_params and "before" not in request.query_params:
        after = datetime.now(_REVIEW_DEFAULT_TZ).strftime("%Y-%m-%d")
    rows, total_rows = get_message_rows(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows, expense_category=expense_category)
    pagination = build_pagination(page=page, limit=limit, total_rows=total_rows)
    channels = get_channel_filter_choices(session)
    expense_category_options = get_expense_category_filter_choices(session)
    summary = get_summary(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
    )

    return templates.TemplateResponse(
        request,
        "review_queue.html",
        {
            "request": request,
            "title": "Review Queue",
            "rows": items,
            "channels": channels,
            "expense_category_options": expense_category_options,
            "summary": summary,
            "pagination": pagination,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "selected_limit": limit,
            "success": success,
            "error": error,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "current_user": getattr(request.state, "current_user", None),
        },
    )

@router.get("/review/focus/{message_id}", response_class=HTMLResponse)
def reviewer_focus_page(
    message_id: int,  # build_message_list_items supplies cached/proxy attachment URLs
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    item = build_message_list_items(session, [row], expense_category=expense_category)[0]
    ordered_ids = get_ordered_message_ids(
        session,
        status="review_queue",
        channel_id=channel_id,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    current_index = ordered_ids.index(message_id) if message_id in ordered_ids else -1
    previous_id = ordered_ids[current_index - 1] if current_index > 0 else None
    next_id = ordered_ids[current_index + 1] if current_index >= 0 and current_index < len(ordered_ids) - 1 else None

    return templates.TemplateResponse(
        request,
        "review_focus.html",
        {
            "request": request,
            "title": f"Review Deal {message_id}",
            "message": item,
            "success": success,
            "error": error,
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "selected_page": page,
            "selected_limit": limit,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "back_url": build_return_url(
                "/review",
                channel_id=channel_id,
                expense_category=expense_category,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ),
            "previous_url": build_return_url(
                f"/review/focus/{previous_id}",
                channel_id=channel_id,
                expense_category=expense_category,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ) if previous_id else None,
            "next_url": build_return_url(
                f"/review/focus/{next_id}",
                channel_id=channel_id,
                expense_category=expense_category,
                after=after,
                before=before,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                limit=limit,
            ) if next_id else None,
            "current_position": current_index + 1 if current_index >= 0 else None,
            "queue_size": len(ordered_ids),
            "current_user": getattr(request.state, "current_user", None),
        },
    )

@router.get("/review/history", response_class=HTMLResponse)
def reviewer_history_page(
    request: Request,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial

    rows, total_rows = get_review_history_rows(session, page=page, limit=limit)
    items = build_message_list_items(session, rows)
    return templates.TemplateResponse(
        request,
        "review_history.html",
        {
            "request": request,
            "title": "Review History",
            "rows": items,
            "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
            "current_user": getattr(request.state, "current_user", None),
        },
    )
