"""
Bookkeeping reconciliation routes.

Extracted from app/main.py — all routes under /bookkeeping/.
"""
from __future__ import annotations

import csv
from io import StringIO
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlmodel import Session

from ..csrf import CSRFProtectedRoute
from ..shared import *  # noqa: F401,F403 — shared helpers, constants, state
from ..discord.bookkeeping import (
    extract_google_sheet_url,
    auto_import_public_google_sheet,
    import_bookkeeping_file,
    list_bookkeeping_imports,
    list_detected_bookkeeping_posts,
    reconcile_bookkeeping_import,
    refresh_bookkeeping_import_from_source,
)
from ..discord.bank_reconciliation import (
    ATTENTION_CLASSIFICATIONS,
    all_classification_choices,
    all_expense_category_choices,
    build_classification_options,
    build_expense_category_options,
    classification_label,
    delete_bank_import,
    expense_category_label,
    get_bank_transactions,
    import_bank_statement_file,
    list_bank_statement_imports,
    rerun_bank_reconciliation,
    summarize_bank_transactions,
)
from ..db import get_session
from ..discord.plaid_bank_feed import (
    create_plaid_link_token,
    exchange_public_token,
    handle_plaid_webhook,
    list_bank_feed_connections,
    plaid_config_status,
    sync_all_plaid_connections,
    sync_plaid_connection,
)

router = APIRouter(route_class=CSRFProtectedRoute)


def _bank_redirect_url(
    *,
    import_id: Optional[int] = None,
    classification: str = "",
    expense_category: str = "",
    review_status: str = "",
    attention: bool = False,
    expenses_only: bool = True,
    search: str = "",
    limit: Optional[int] = None,
    success: str = "",
    error: str = "",
) -> str:
    params: dict[str, str] = {}
    if import_id:
        params["import_id"] = str(import_id)
    if classification:
        params["classification"] = classification
    if expense_category:
        params["expense_category"] = expense_category
    if review_status:
        params["review_status"] = review_status
    if attention:
        params["attention"] = "true"
    if not expenses_only:
        params["expenses_only"] = "false"
    if search:
        params["search"] = search
    if limit and limit != 250:
        params["limit"] = str(limit)
    if success:
        params["success"] = success
    if error:
        params["error"] = error
    return "/bookkeeping/bank" + (f"?{urlencode(params)}" if params else "")


def _bank_row_view(row: BankTransaction, matched_transaction: Optional[Transaction]) -> dict[str, object]:
    action_links = None
    if matched_transaction:
        action_links = build_row_action_links(
            matched_transaction.source_message_id,
            channel_id=matched_transaction.channel_id,
            created_at=matched_transaction.occurred_at,
            status="parsed",
        )
    return {
        "row": row,
        "classification_label": classification_label(row.classification),
        "expense_category_label": expense_category_label(row.expense_category or "uncategorized"),
        "needs_attention": row.classification in ATTENTION_CLASSIFICATIONS and row.review_status == "open",
        "matched_transaction": matched_transaction,
        "action_links": action_links,
    }


@router.get("/bookkeeping/bank", response_class=HTMLResponse)
def bank_reconciliation_page(
    request: Request,
    import_id: Optional[int] = Query(default=None),
    classification: str = Query(default=""),
    expense_category: str = Query(default=""),
    review_status: str = Query(default=""),
    attention: bool = Query(default=False),
    expenses_only: bool = Query(default=True),
    search: str = Query(default=""),
    limit: int = Query(default=250, ge=25, le=1000),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    import_id = import_id if isinstance(import_id, int) else None
    classification = classification if isinstance(classification, str) else ""
    expense_category = expense_category if isinstance(expense_category, str) else ""
    review_status = review_status if isinstance(review_status, str) else ""
    search = search if isinstance(search, str) else ""
    attention = attention if isinstance(attention, bool) else False
    expenses_only = expenses_only if isinstance(expenses_only, bool) else True
    limit = limit if isinstance(limit, int) else 250

    imports = list_bank_statement_imports(session)
    selected_import = session.get(BankStatementImport, import_id) if import_id else (imports[0] if imports else None)
    all_rows = []
    filtered_rows = []
    summary = None
    classification_options = []
    expense_category_options = []
    row_views = []
    hidden_count = 0
    matched_by_id = {}
    plaid_status = plaid_config_status()
    bank_feed_connections = list_bank_feed_connections(session)

    if selected_import:
        all_rows = get_bank_transactions(session, import_id=selected_import.id)
        summary = summarize_bank_transactions(all_rows)
        classification_options = build_classification_options(all_rows)
        expense_category_options = build_expense_category_options(all_rows)
        filtered_rows = get_bank_transactions(
            session,
            import_id=selected_import.id,
            classification=classification,
            expense_category=expense_category,
            review_status=review_status,
            search=search,
            attention_only=attention,
            expenses_only=expenses_only,
        )
        visible_rows = filtered_rows[:limit]
        hidden_count = max(len(filtered_rows) - len(visible_rows), 0)
        matched_ids = sorted({row.matched_transaction_id for row in visible_rows if row.matched_transaction_id})
        if matched_ids:
            matched_by_id = {
                row.id: row
                for row in session.exec(select(Transaction).where(Transaction.id.in_(matched_ids))).all()
                if row.id is not None
            }
        row_views = [
            _bank_row_view(row, matched_by_id.get(row.matched_transaction_id))
            for row in visible_rows
        ]

    return templates.TemplateResponse(
        request,
        "bank_reconciliation.html",
        {
            "request": request,
            "title": "Bank Reconciliation",
            "current_user": getattr(request.state, "current_user", None),
            "imports": imports,
            "selected_import": selected_import,
            "summary": summary,
            "classification_options": classification_options,
            "expense_category_options": expense_category_options,
            "classification_choices": all_classification_choices(),
            "expense_category_choices": all_expense_category_choices(),
            "row_views": row_views,
            "filtered_count": len(filtered_rows),
            "hidden_count": hidden_count,
            "selected_classification": classification,
            "selected_expense_category": expense_category,
            "selected_review_status": review_status,
            "selected_attention": attention,
            "selected_expenses_only": expenses_only,
            "selected_search": search,
            "limit": limit,
            "success": success,
            "error": error,
            "plaid_status": plaid_status,
            "bank_feed_connections": bank_feed_connections,
        },
    )


@router.post("/bookkeeping/bank/plaid/link-token")
async def bank_plaid_link_token(
    request: Request,
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        current_user = getattr(request.state, "current_user", None)
        user_id = str(getattr(current_user, "id", None) or getattr(current_user, "username", None) or "degen-admin")
        link_token = create_plaid_link_token(user_id=user_id)
        return JSONResponse({"link_token": link_token})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


@router.post("/bookkeeping/bank/plaid/exchange")
async def bank_plaid_exchange(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    public_token = str(payload.get("public_token") or "").strip()
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    if not public_token:
        return JSONResponse({"error": "Plaid public_token missing"}, status_code=400)
    try:
        result = exchange_public_token(session, public_token=public_token, metadata=metadata)
        return JSONResponse({"ok": True, **result})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


@router.post("/bookkeeping/bank/plaid/sync-form")
def bank_plaid_sync_form(
    request: Request,
    connection_id: int = Form(default=0),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        if connection_id:
            result = sync_plaid_connection(session, connection_id)
        else:
            result = sync_all_plaid_connections(session)
        added = int(result.get("added") or 0)
        modified = int(result.get("modified") or 0)
        removed = int(result.get("removed") or 0)
        ledger_agent = result.get("ledger_agent") if isinstance(result.get("ledger_agent"), dict) else {}
        agent_updated = int(ledger_agent.get("updated_count") or 0)
        agent_cleared = int(ledger_agent.get("cleared_false_matches") or 0)
        agent_reviewed = int(ledger_agent.get("auto_reviewed") or 0)
        return RedirectResponse(
            url=_bank_redirect_url(
                success=(
                    f"Synced Plaid feed: {added} new, {modified} updated, {removed} removed. "
                    f"Ledger agent updated {agent_updated} row(s): "
                    f"{agent_cleared} bad match(es) cleared, {agent_reviewed} auto-reviewed."
                )
            ),
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=_bank_redirect_url(error=f"Plaid sync failed: {exc}"),
            status_code=303,
        )


@router.post("/webhooks/plaid")
async def plaid_webhook(
    request: Request,
    session: Session = Depends(get_session),
):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    try:
        return JSONResponse(handle_plaid_webhook(session, payload))
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@router.post("/bookkeeping/bank/import-form")
async def bank_reconciliation_import_form(
    request: Request,
    account_label: str = Form(...),
    account_type: str = Form(default="checking"),
    upload_file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if not upload_file.filename:
        return RedirectResponse(
            url=_bank_redirect_url(error="Choose a Chase CSV file first"),
            status_code=303,
        )
    try:
        imported = import_bank_statement_file(
            session,
            filename=upload_file.filename,
            content=await upload_file.read(),
            account_label=account_label,
            account_type=account_type,
        )
        return RedirectResponse(
            url=_bank_redirect_url(
                import_id=imported.id,
                attention=True,
                success=f"Imported {imported.row_count} bank rows",
            ),
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=_bank_redirect_url(error=str(exc)),
            status_code=303,
        )


@router.post("/bookkeeping/bank/{import_id}/rerun-form")
def bank_reconciliation_rerun_form(
    request: Request,
    import_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        rerun_bank_reconciliation(session, import_id)
        return RedirectResponse(
            url=_bank_redirect_url(import_id=import_id, success="Re-ran matching and expense categories"),
            status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=_bank_redirect_url(import_id=import_id, error=str(exc)),
            status_code=303,
        )


@router.post("/bookkeeping/bank/{import_id}/delete-form")
def bank_reconciliation_delete_form(
    request: Request,
    import_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    delete_bank_import(session, import_id)
    return RedirectResponse(
        url=_bank_redirect_url(success="Deleted bank import"),
        status_code=303,
    )


@router.post("/bookkeeping/bank/rows/{row_id}/status-form")
def bank_reconciliation_row_status_form(
    request: Request,
    row_id: int,
    import_id: int = Form(...),
    review_status: str = Form(...),
    classification: str = Form(default=""),
    expense_category: str = Form(default=""),
    note: str = Form(default=""),
    selected_classification: str = Form(default=""),
    selected_expense_category: str = Form(default=""),
    selected_review_status: str = Form(default=""),
    selected_attention: str = Form(default=""),
    selected_expenses_only: str = Form(default="true"),
    selected_search: str = Form(default=""),
    selected_limit: str = Form(default="250"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(BankTransaction, row_id)
    if row:
        if classification:
            row.classification = classification
        if expense_category:
            row.expense_category = expense_category
            row.expense_subcategory = "Manual override"
            row.category_confidence = "manual"
            row.category_reason = "Manually changed from the bank reconciliation page."
        if review_status in {"open", "reviewed", "ignored"}:
            row.review_status = review_status
        row.review_note = (note or "").strip() or None
        row.updated_at = utcnow()
        session.add(row)
        session.commit()
    return RedirectResponse(
        url=_bank_redirect_url(
            import_id=import_id,
            classification=selected_classification,
            expense_category=selected_expense_category,
            review_status=selected_review_status,
            attention=(selected_attention == "true"),
            expenses_only=(selected_expenses_only != "false"),
            search=selected_search,
            limit=int(selected_limit) if selected_limit.isdigit() else 250,
        ),
        status_code=303,
    )


@router.get("/bookkeeping/bank/{import_id}/export.csv")
def bank_reconciliation_export_csv(
    request: Request,
    import_id: int,
    classification: str = Query(default=""),
    expense_category: str = Query(default=""),
    review_status: str = Query(default=""),
    attention: bool = Query(default=False),
    expenses_only: bool = Query(default=True),
    search: str = Query(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    classification = classification if isinstance(classification, str) else ""
    expense_category = expense_category if isinstance(expense_category, str) else ""
    review_status = review_status if isinstance(review_status, str) else ""
    attention = attention if isinstance(attention, bool) else False
    expenses_only = expenses_only if isinstance(expenses_only, bool) else True
    search = search if isinstance(search, str) else ""
    rows = get_bank_transactions(
        session,
        import_id=import_id,
        classification=classification,
        expense_category=expense_category,
        review_status=review_status,
        attention_only=attention,
        expenses_only=expenses_only,
        search=search,
    )
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "statement_line",
            "posted_at",
            "account",
            "amount",
            "expense_category",
            "expense_category_label",
            "expense_subcategory",
            "category_confidence",
            "category_reason",
            "classification",
            "bank_bucket_label",
            "confidence",
            "review_status",
            "description",
            "match_reason",
            "matched_transaction_id",
            "matched_source_message_id",
            "review_note",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.row_index,
                row.posted_at.date().isoformat() if row.posted_at else "",
                row.account_label,
                f"{float(row.amount or 0.0):.2f}",
                row.expense_category or "uncategorized",
                expense_category_label(row.expense_category or "uncategorized"),
                row.expense_subcategory or "",
                row.category_confidence or "",
                row.category_reason or "",
                row.classification,
                classification_label(row.classification),
                row.confidence,
                row.review_status,
                row.description,
                row.match_reason,
                row.matched_transaction_id or "",
                row.matched_source_message_id or "",
                row.review_note or "",
            ]
        )
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="bank-reconciliation-{import_id}.csv"'},
    )


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
