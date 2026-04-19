"""
Deals, login, and logout routes.

Extracted from app/main.py -- /deals, /deals/{message_id}, /login, /logout.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..corrections import get_correction_pattern_counts
from ..db import get_session

_DEALS_DEFAULT_TZ = ZoneInfo("America/Los_Angeles")

router = APIRouter()


@router.get("/deals", response_class=HTMLResponse)
def deals_page(
    request: Request,
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    # Default `after` to today in PT (midnight-to-now) only when no date params
    # are present in the URL. Once the user touches the filter — including
    # clearing it to an empty string — we respect their choice.
    if "after" not in request.query_params and "before" not in request.query_params:
        after = datetime.now(_DEALS_DEFAULT_TZ).strftime("%Y-%m-%d")

    rows, total_rows = get_partner_deal_rows(
        session,
        channel_id=channel_id,
        entry_kind=entry_kind,
        after=after,
        before=before,
        page=page,
        limit=limit,
    )
    items = build_message_list_items(session, rows)
    channels = get_partner_channel_choices(session)
    watched_channel_ids = {row["channel_id"] for row in channels}
    summary_rows = [
        row for row in get_financial_rows(
            session,
            start=parse_report_datetime(after),
            end=parse_report_datetime(before, end_of_day=True),
            channel_id=channel_id if channel_id else None,
        )
        if row.channel_id in watched_channel_ids and normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review) == PARSE_PARSED and not row.is_deleted
    ]
    summary = build_financial_summary(summary_rows)
    return templates.TemplateResponse(
        request,
        "deals.html",
        {
            "request": request,
            "title": "Deals",
            "rows": items,
            "channels": channels,
            "selected_channel_id": channel_id or "",
            "selected_entry_kind": entry_kind or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_limit": limit,
            "pagination": build_pagination(page=page, limit=limit, total_rows=total_rows),
            "summary": summary,
            "current_user": getattr(request.state, "current_user", None),
        },
    )


@router.get("/deals/{message_id}", response_class=HTMLResponse)
def deal_detail_page(
    message_id: int,
    request: Request,
    return_path: str = Query(default="/deals"),
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default=None),
    sort_dir: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    watched_channel_ids = {
        row.channel_id
        for row in get_watched_channels(session)
        if row.is_enabled
    }
    row = session.get(DiscordMessage, message_id)
    if not row or row.channel_id not in watched_channel_ids or normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review) != PARSE_PARSED:
        raise HTTPException(status_code=404, detail="Deal not found")

    item = build_message_list_items(session, [row])[0]
    item["trade_summary"] = row.trade_summary
    item["notes"] = row.notes
    item["image_summary"] = row.image_summary
    item["reviewed_by"] = row.reviewed_by
    item["reviewed_at"] = format_pacific_datetime(row.reviewed_at)
    item["parse_status"] = normalize_parse_status(row.parse_status, is_deleted=row.is_deleted, needs_review=row.needs_review)
    item["needs_review"] = row.needs_review
    item["is_deleted"] = row.is_deleted
    item["confidence"] = row.confidence
    item["parse_attempts"] = row.parse_attempts
    item["discord_message_id"] = row.discord_message_id
    item["channel_name"] = row.channel_name
    item["item_names"] = json.loads(row.item_names_json or "[]")
    item["items_in"] = json.loads(row.items_in_json or "[]")
    item["items_out"] = json.loads(row.items_out_json or "[]")
    item["last_error"] = row.last_error
    back_url = build_return_url(
        return_path,
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
    if entry_kind:
        separator = "&" if "?" in back_url else "?"
        back_url = f"{back_url}{separator}entry_kind={entry_kind}"
    learning_signal = get_learning_signal(session, row.content or "")

    return templates.TemplateResponse(
        request,
        "deal_detail.html",
        {
            "request": request,
            "title": f"Deal {message_id}",
            "deal": item,
            "back_url": back_url,
            "success": success,
            "error": error,
            "current_user": getattr(request.state, "current_user", None),
            "parse_status_options": PARSE_STATUS_OPTIONS,
            "deal_type_options": DEAL_TYPE_OPTIONS,
            "entry_kind_options": ENTRY_KIND_OPTIONS,
            "payment_method_options": PAYMENT_METHOD_OPTIONS,
            "cash_direction_options": CASH_DIRECTION_OPTIONS,
            "category_options": CATEGORY_OPTIONS,
            "correction_patterns": get_correction_pattern_counts(session=session),
            "learning_signal": learning_signal,
            "return_path": return_path,
            "selected_status": status or "",
            "selected_channel_id": channel_id or "",
            "selected_expense_category": expense_category or "",
            "selected_after": after or "",
            "selected_before": before or "",
            "selected_sort_by": sort_by or "",
            "selected_sort_dir": sort_dir or "",
            "selected_page": page or 1,
            "selected_limit": limit or 25,
        },
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(
    request: Request,
    next: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
):
    user = get_request_user(request)
    if user:
        return RedirectResponse(url=app_home_for_role(user.role), status_code=303)

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "title": "Sign In",
            "next_url": next or "",
            "error": error,
        },
    )


@router.post("/login")
def login_form(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    user = authenticate_user(session, username, password)
    if not user:
        return RedirectResponse(
            url=f"/login?error=Invalid+username+or+password&next={urlencode({'next': next or ''})[5:]}",
            status_code=303,
        )

    request.session["user_id"] = user.id
    if next and next.startswith("/") and not next.startswith("//"):
        redirect_target = next
    else:
        redirect_target = app_home_for_role(user.role)
    return RedirectResponse(url=redirect_target, status_code=303)


@router.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
