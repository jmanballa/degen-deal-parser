"""
Bookkeeping reconciliation routes.

Extracted from app/main.py — all routes under /bookkeeping/.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from ..shared import *  # noqa: F401,F403 — shared helpers, constants, state
from ..bookkeeping import (
    extract_google_sheet_url,
    auto_import_public_google_sheet,
    import_bookkeeping_file,
    list_bookkeeping_imports,
    list_detected_bookkeeping_posts,
    reconcile_bookkeeping_import,
    refresh_bookkeeping_import_from_source,
)
from ..db import get_session

router = APIRouter()


@router.get("/bookkeeping", response_class=HTMLResponse)
def bookkeeping_page(
    request: Request,
    import_id: Optional[int] = Query(default=None),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    # Reviewer-level covers both "reviewer" accounts and "manager" accounts
    # (they share a rank tier). Admins are still allowed because rank-admin
    # > rank-reviewer. Bookkeeping is ops-layer, not admin-layer.
    if denial := require_role_response(request, "reviewer"):
        return denial
    imports = list_bookkeeping_imports(session)
    selected_import = None
    reconciliation = None
    detected_posts = list_detected_bookkeeping_posts(session)
    for post in detected_posts:
        post["action_links"] = build_row_action_links(
            post.get("message_id"),
            channel_id=None,
            created_at=post.get("created_at"),
        )
    if import_id:
        selected_import = session.get(BookkeepingImport, import_id)
        if selected_import:
            reconciliation = reconcile_bookkeeping_import(session, import_id)
            for entry in reconciliation["entries"]:
                matched_transaction = entry.get("matched_transaction")
                entry["action_links"] = build_row_action_links(
                    matched_transaction.source_message_id if matched_transaction and matched_transaction.source_message_id is not None else None,
                    channel_id=matched_transaction.channel_id if matched_transaction else None,
                    created_at=matched_transaction.occurred_at if matched_transaction else entry.get("occurred_at"),
                    status="parsed" if matched_transaction else None,
                )
            enriched_unmatched_transactions = []
            for row in reconciliation["unmatched_transactions"]:
                action_links = build_row_action_links(
                    row.source_message_id if row.source_message_id is not None else None,
                    channel_id=row.channel_id,
                    created_at=row.occurred_at,
                    status="parsed",
                )
                enriched_unmatched_transactions.append(
                    {
                        "occurred_at": row.occurred_at,
                        "channel_name": row.channel_name,
                        "channel_id": row.channel_id,
                        "entry_kind": row.entry_kind,
                        "amount": row.amount,
                        "payment_method": row.payment_method,
                        "category": row.category,
                        "expense_category": row.expense_category,
                        "notes": row.notes,
                        "action_links": action_links,
                    }
                )
            reconciliation["unmatched_transactions"] = enriched_unmatched_transactions

    return templates.TemplateResponse(
        request,
        "bookkeeping.html",
        {
            "request": request,
            "title": "Bookkeeping Reconciliation",
            "imports": imports,
            "selected_import": selected_import,
            "reconciliation": reconciliation,
            "detected_posts": detected_posts,
            "success": success,
            "error": error,
        },
    )


@router.post("/bookkeeping/import-form")
async def bookkeeping_import_form(
    request: Request,
    show_label: str = Form(...),
    show_date: Optional[str] = Form(default=None),
    range_start: Optional[str] = Form(default=None),
    range_end: Optional[str] = Form(default=None),
    source_url: Optional[str] = Form(default=None),
    upload_file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if not upload_file.filename:
        return RedirectResponse(
            url="/bookkeeping?error=Choose+a+CSV+or+XLSX+file+first",
            status_code=303,
        )

    try:
        imported = import_bookkeeping_file(
            session,
            filename=upload_file.filename,
            content=await upload_file.read(),
            show_label=show_label.strip(),
            show_date=parse_report_datetime(show_date),
            range_start=parse_report_datetime(range_start),
            range_end=parse_report_datetime(range_end, end_of_day=True),
            source_url=(source_url or "").strip() or None,
        )
        return RedirectResponse(
            url=f"/bookkeeping?import_id={imported.id}&success=Imported+{imported.row_count}+bookkeeping+rows",
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/bookkeeping?error={str(exc).replace(' ', '+')}",
            status_code=303,
        )


@router.post("/bookkeeping/import-detected/{message_id}")
async def bookkeeping_import_detected_message(
    request: Request,
    message_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        return RedirectResponse(
            url="/bookkeeping?error=Detected+bookkeeping+message+not+found",
            status_code=303,
        )

    sheet_url = extract_google_sheet_url(row.content or "")
    if not sheet_url:
        return RedirectResponse(
            url="/bookkeeping?error=No+Google+Sheet+link+found+on+that+message",
            status_code=303,
        )

    try:
        import_id = await auto_import_public_google_sheet(
            message_text=row.content or "",
            created_at=row.created_at,
            sheet_url=sheet_url,
        )
        if import_id:
            return RedirectResponse(
                url=f"/bookkeeping?import_id={import_id}&success=Imported+detected+Google+Sheet",
                status_code=303,
            )
        return RedirectResponse(
            url="/bookkeeping?error=Import+did+not+create+a+bookkeeping+record",
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/bookkeeping?error={str(exc).replace(' ', '+')}",
            status_code=303,
        )


@router.post("/bookkeeping/refresh-import/{import_id}")
async def bookkeeping_refresh_import(
    request: Request,
    import_id: int,
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        refreshed_import_id = await refresh_bookkeeping_import_from_source(import_id)
        return RedirectResponse(
            url=f"/bookkeeping?import_id={refreshed_import_id}&success=Refreshed+bookkeeping+import",
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/bookkeeping?import_id={import_id}&error={str(exc).replace(' ', '+')}",
            status_code=303,
        )
