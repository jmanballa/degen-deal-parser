"""
Unified ledger routes.

Bank rows are the counted money source. Discord, Shopify, and TikTok are
supporting context that help reviewers decide what to do with each row.
"""
from __future__ import annotations

import csv
import json
import logging
from io import StringIO
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from sqlmodel import Session

from ..csrf import CSRFProtectedRoute
from ..db import get_session
from ..ledger import (
    LEDGER_ACTION_REASON_LABELS,
    LEDGER_STATUS_LABELS,
    apply_ledger_automation,
    apply_ledger_rule,
    build_ledger_page_data,
    create_ledger_rule,
    draft_ledger_rule_from_instruction,
    draft_ledger_rule_with_ai,
    expense_category_label,
    format_ledger_money,
    ledger_action_reason_for_bank_row,
    ledger_filters_from_values,
    ledger_source_for_bank_row,
    ledger_status_for_bank_row,
    preview_ledger_automation,
    preview_ledger_rule,
    run_ledger_review_agent,
)
from ..models import BankTransaction, LedgerRule, utcnow
from ..shared import *  # noqa: F401,F403 -- templates, auth helpers, user labels

router = APIRouter(route_class=CSRFProtectedRoute)
logger = logging.getLogger(__name__)


def _ledger_redirect_url(
    *,
    account: str = "",
    start: str = "",
    end: str = "",
    status: str = "needs_action",
    category: str = "",
    source: str = "",
    action_reason: str = "",
    search: str = "",
    sort: str = "posted_at",
    direction: str = "desc",
    include_cash: bool | str = False,
    success: str = "",
    error: str = "",
) -> str:
    params: dict[str, str] = {}
    for key, value in {
        "account": account,
        "start": start,
        "end": end,
        "status": status,
        "category": category,
        "source": source,
        "action_reason": action_reason,
        "search": search,
        "sort": sort,
        "direction": direction,
        "include_cash": "true" if include_cash is True or str(include_cash).lower() in {"1", "true", "yes", "on"} else "",
        "success": success,
        "error": error,
    }.items():
        if value:
            params[key] = str(value)
    return "/ledger" + (f"?{urlencode(params)}" if params else "")


def _wants_json(request: Request) -> bool:
    requested_with = request.headers.get("x-requested-with", "").lower()
    accept = request.headers.get("accept", "").lower()
    return requested_with in {"fetch", "xmlhttprequest"} or "application/json" in accept


def _ledger_row_json(row: BankTransaction) -> dict[str, object]:
    status = ledger_status_for_bank_row(row)
    action_reason = ledger_action_reason_for_bank_row(row)
    source = ledger_source_for_bank_row(row)
    category = row.expense_category or "uncategorized"
    return {
        "id": row.id,
        "amount": float(row.amount or 0.0),
        "amount_display": format_ledger_money(row.amount),
        "ledger_status": status,
        "ledger_status_label": LEDGER_STATUS_LABELS.get(status, status.replace("_", " ").title()),
        "action_reason": action_reason,
        "action_reason_label": LEDGER_ACTION_REASON_LABELS.get(action_reason, ""),
        "source": source,
        "expense_category": category,
        "expense_category_label": expense_category_label(category),
        "review_status": row.review_status or "open",
        "review_note": row.review_note or "",
        "category_confidence": row.category_confidence or "",
    }


@router.get("/ledger")
def ledger_page(
    request: Request,
    account: str = Query(default=""),
    start: str = Query(default=""),
    end: str = Query(default=""),
    status: str = Query(default="needs_action"),
    category: str = Query(default=""),
    source: str = Query(default=""),
    action_reason: str = Query(default=""),
    search: str = Query(default=""),
    sort: str = Query(default="posted_at"),
    direction: str = Query(default="desc"),
    include_cash: bool = Query(default=False),
    success: str = Query(default=""),
    error: str = Query(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=account,
        start=start,
        end=end,
        status=status,
        category=category,
        source=source,
        action_reason=action_reason,
        search=search,
        sort=sort,
        direction=direction,
        include_cash=include_cash,
    )
    data = build_ledger_page_data(session, filters)
    data["automation_previews"] = [
        preview_ledger_automation(session, action_key="mark_needs_log_checked", filters=filters)
    ]
    return templates.TemplateResponse(
        request,
        "ledger.html",
        {
            "request": request,
            "title": "Unified Ledger",
            "current_user": getattr(request.state, "current_user", None),
            "success": success,
            "error": error,
            **data,
        },
    )


@router.post("/ledger/rows/{row_id}/status-form")
def ledger_row_status_form(
    request: Request,
    row_id: int,
    review_status: str = Form(default=""),
    classification: str = Form(default=""),
    expense_category: str = Form(default=""),
    note: str = Form(default=""),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(BankTransaction, row_id)
    if row:
        changed = False
        if classification:
            row.classification = classification
            changed = True
        if expense_category:
            row.expense_category = expense_category
            row.expense_subcategory = "Manual override"
            row.category_confidence = "manual"
            row.category_reason = "Manually changed from the ledger."
            changed = True
        if review_status in {"open", "reviewed", "ignored"}:
            row.review_status = review_status
            changed = True
        stripped_note = note.strip() if isinstance(note, str) else ""
        if stripped_note:
            row.review_note = stripped_note
            changed = True
        if changed:
            row.updated_at = utcnow()
            session.add(row)
            session.commit()
            session.refresh(row)
    if row and _wants_json(request):
        return JSONResponse({"ok": True, "row": _ledger_row_json(row)})
    if _wants_json(request):
        return JSONResponse({"ok": False, "error": "Ledger row not found"}, status_code=404)
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success="Updated ledger row",
        ),
        status_code=303,
    )


@router.post("/ledger/rows/{row_id}/force-unmatch-form")
def ledger_row_force_unmatch_form(
    request: Request,
    row_id: int,
    mode: str = Form(default="force"),
    note: str = Form(default=""),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    row = session.get(BankTransaction, row_id)
    if row:
        if mode in {"clear", "none"}:
            row.match_override_status = None
            row.match_override_note = None
            row.match_override_at = None
            row.match_override_by = None
            success = "Cleared match override"
        else:
            row.match_override_status = "force_unmatched"
            row.match_override_note = (note or "").strip() or "Forced unmatched from the ledger."
            row.match_override_at = utcnow()
            row.match_override_by = current_user_label(request)
            row.matched_transaction_id = None
            row.matched_source_message_id = None
            row.matched_platform = None
            row.match_reason = "Manually forced unmatched from the ledger."
            success = "Forced row unmatched"
        row.updated_at = utcnow()
        session.add(row)
        session.commit()
    else:
        success = ""
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
        ),
        status_code=303,
    )


@router.post("/ledger/agent/run-form")
def ledger_agent_run_form(
    request: Request,
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "needs_action",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
        limit=1000,
    )
    result = run_ledger_review_agent(
        session,
        filters=filters,
        limit=1000,
        applied_by=current_user_label(request),
    )
    success = (
        f"Ledger agent updated {result['updated_count']} row(s): "
        f"{result['cleared_false_matches']} bad match(es) cleared, "
        f"{result['auto_reviewed']} row(s) auto-reviewed."
    )
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
        ),
        status_code=303,
    )


@router.post("/ledger/automation/{action_key}/apply-form")
def ledger_automation_apply_form(
    request: Request,
    action_key: str,
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "needs_action",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
        limit=1000,
    )
    try:
        result = apply_ledger_automation(
            session,
            action_key=action_key,
            filters=filters,
            applied_by=current_user_label(request),
        )
        success = f"Automation updated {result['updated_count']} of {result['matched_count']} matching row(s)."
        error = ""
    except ValueError as exc:
        success = ""
        error = str(exc)
    except Exception:
        logger.exception("ledger automation apply failed")
        success = ""
        error = "An unexpected error occurred, please try again."
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
            error=error,
        ),
        status_code=303,
    )


async def _preview_payload(request: Request) -> dict[str, str]:
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            payload = await request.json()
            return {str(key): value for key, value in payload.items()}
        except Exception:
            return {}
    form = await request.form()
    return {str(key): value for key, value in form.items()}


@router.post("/ledger/rules/preview")
async def ledger_rule_preview(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    payload = await _preview_payload(request)
    instruction = str(payload.get("instruction") or "").strip()
    try:
        if payload.get("conditions_json") and payload.get("actions_json"):
            draft = {
                "name": str(payload.get("name") or "Ledger rule"),
                "summary": "",
                "conditions": json.loads(str(payload.get("conditions_json") or "{}")),
                "actions": json.loads(str(payload.get("actions_json") or "{}")),
                "confidence": "manual",
                "warnings": [],
                "source": "submitted",
            }
        else:
            use_ai = str(payload.get("use_ai") or "true").lower() != "false"
            draft = draft_ledger_rule_with_ai(instruction) if use_ai else draft_ledger_rule_from_instruction(instruction)
        filters = ledger_filters_from_values(
            account=str(payload.get("account") or ""),
            start=str(payload.get("start") or ""),
            end=str(payload.get("end") or ""),
            status=str(payload.get("status") or "all"),
            category=str(payload.get("category") or ""),
            source=str(payload.get("source") or ""),
            action_reason=str(payload.get("action_reason") or ""),
            search=str(payload.get("search") or ""),
            sort=str(payload.get("sort") or "posted_at"),
            direction=str(payload.get("direction") or "desc"),
            include_cash=str(payload.get("include_cash") or ""),
        )
        preview = preview_ledger_rule(
            session,
            conditions=draft.get("conditions") or {},
            actions=draft.get("actions") or {},
            filters=filters,
        )
        warnings = list(draft.get("warnings") or []) + list(preview.get("warnings") or [])
        draft["warnings"] = warnings
        return JSONResponse({"ok": True, "draft": draft, "preview": preview})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@router.post("/ledger/rules/create-form")
def ledger_rule_create_form(
    request: Request,
    name: str = Form(default="Ledger rule"),
    description: str = Form(default=""),
    conditions_json: str = Form(default="{}"),
    actions_json: str = Form(default="{}"),
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="needs_action"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        conditions = json.loads(conditions_json or "{}")
        actions = json.loads(actions_json or "{}")
        rule = create_ledger_rule(
            session,
            name=name,
            description=description,
            conditions=conditions if isinstance(conditions, dict) else {},
            actions=actions if isinstance(actions, dict) else {},
            created_by=current_user_label(request),
        )
        success = f"Saved ledger rule #{rule.id}"
        error = ""
    except Exception as exc:
        success = ""
        error = str(exc)
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=success,
            error=error,
        ),
        status_code=303,
    )


@router.post("/ledger/rules/{rule_id}/apply-form")
def ledger_rule_apply_form(
    request: Request,
    rule_id: int,
    selected_account: str = Form(default=""),
    selected_start: str = Form(default=""),
    selected_end: str = Form(default=""),
    selected_status: str = Form(default="all"),
    selected_category: str = Form(default=""),
    selected_source: str = Form(default=""),
    selected_action_reason: str = Form(default=""),
    selected_search: str = Form(default=""),
    selected_sort: str = Form(default="posted_at"),
    selected_direction: str = Form(default="desc"),
    selected_include_cash: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    rule = session.get(LedgerRule, rule_id)
    if not rule:
        return RedirectResponse(url=_ledger_redirect_url(error="Ledger rule not found"), status_code=303)
    filters = ledger_filters_from_values(
        account=selected_account,
        start=selected_start,
        end=selected_end,
        status=selected_status or "all",
        category=selected_category,
        source=selected_source,
        action_reason=selected_action_reason,
        search=selected_search,
        sort=selected_sort,
        direction=selected_direction,
        include_cash=selected_include_cash,
    )
    result = apply_ledger_rule(session, rule, filters=filters, applied_by=current_user_label(request))
    return RedirectResponse(
        url=_ledger_redirect_url(
            account=selected_account,
            start=selected_start,
            end=selected_end,
            status=selected_status,
            category=selected_category,
            source=selected_source,
            action_reason=selected_action_reason,
            search=selected_search,
            sort=selected_sort,
            direction=selected_direction,
            include_cash=selected_include_cash,
            success=f"Applied {rule.name} to {result['updated_count']} row(s)",
        ),
        status_code=303,
    )


@router.get("/ledger/export.csv")
def ledger_export_csv(
    request: Request,
    account: str = Query(default=""),
    start: str = Query(default=""),
    end: str = Query(default=""),
    status: str = Query(default="needs_action"),
    category: str = Query(default=""),
    source: str = Query(default=""),
    action_reason: str = Query(default=""),
    search: str = Query(default=""),
    sort: str = Query(default="posted_at"),
    direction: str = Query(default="desc"),
    include_cash: bool = Query(default=False),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    filters = ledger_filters_from_values(
        account=account,
        start=start,
        end=end,
        status=status,
        category=category,
        source=source,
        action_reason=action_reason,
        search=search,
        sort=sort,
        direction=direction,
        include_cash=include_cash,
        limit=1000,
    )
    data = build_ledger_page_data(session, filters)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "row_id",
            "row_kind",
            "posted_at",
            "account",
            "amount",
            "ledger_status",
            "source",
            "category",
            "classification",
            "description",
            "matched_transaction_id",
            "match_reason",
            "review_status",
            "review_note",
        ]
    )
    for row in data["rows"]:
        writer.writerow(
            [
                row["id"],
                row.get("row_kind", "bank"),
                row["posted_at_display"],
                row["account_label"],
                row["amount"],
                row["ledger_status"],
                row["source"],
                row["expense_category"],
                row["classification"],
                row["description"],
                row.get("matched_transaction_id"),
                row.get("match_reason"),
                row.get("review_status"),
                row.get("review_note"),
            ]
        )
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="ledger-export.csv"'},
    )
