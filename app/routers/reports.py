"""
Reports & Finance routes.

Extracted from app/main.py -- all routes under /reports/, /messages/export.csv,
/pnl, and /finance.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..discord.bank_reconciliation import (
    BANK_ACCOUNT_FILTER_LABELS,
    build_finance_bank_expense_data,
    normalize_bank_account_filter,
)
from ..db import get_session

router = APIRouter()


def _decorate_bank_expense_data(data: dict[str, object]) -> dict[str, object]:
    decorated = dict(data)
    for key in (
        "gross_outflow_total",
        "bank_only_total",
        "discord_logged_total",
        "operating_total",
        "non_operating_total",
        "inventory_total",
        "partner_paybacks_total",
        "uncategorized_total",
    ):
        decorated[f"{key}_display"] = format_dashboard_money(float(data.get(key, 0.0) or 0.0))

    category_rows = []
    for row in data.get("category_rows", []) or []:
        item = dict(row)
        item["total_display"] = format_dashboard_money(float(item.get("total", 0.0) or 0.0))
        item["bank_only_total_display"] = format_dashboard_money(float(item.get("bank_only_total", 0.0) or 0.0))
        item["discord_logged_total_display"] = format_dashboard_money(float(item.get("discord_logged_total", 0.0) or 0.0))
        item["operating_total_display"] = format_dashboard_money(float(item.get("operating_total", 0.0) or 0.0))
        item["non_operating_total_display"] = format_dashboard_money(float(item.get("non_operating_total", 0.0) or 0.0))
        item["share_display"] = format_percent_value(float(item.get("share_pct", 0.0) or 0.0))
        category_rows.append(item)
    decorated["category_rows"] = category_rows

    account_rows = []
    for row in data.get("account_rows", []) or []:
        item = dict(row)
        item["total_display"] = format_dashboard_money(float(item.get("total", 0.0) or 0.0))
        item["bank_only_total_display"] = format_dashboard_money(float(item.get("bank_only_total", 0.0) or 0.0))
        item["discord_logged_total_display"] = format_dashboard_money(float(item.get("discord_logged_total", 0.0) or 0.0))
        item["operating_total_display"] = format_dashboard_money(float(item.get("operating_total", 0.0) or 0.0))
        item["non_operating_total_display"] = format_dashboard_money(float(item.get("non_operating_total", 0.0) or 0.0))
        item["share_display"] = format_percent_value(float(item.get("share_pct", 0.0) or 0.0))
        account_rows.append(item)
    decorated["account_rows"] = account_rows
    return decorated


def _build_bank_expense_chart_data(data: dict[str, object]) -> dict[str, object]:
    category_rows = list(data.get("category_rows", []) or [])[:12]
    account_rows = list(data.get("account_rows", []) or [])
    daily_rows = list(data.get("daily_rows", []) or [])
    return {
        "categoryMix": {
            "labels": [row["label"] for row in category_rows],
            "values": [row["bank_only_total"] for row in category_rows],
            "groups": [row.get("group", "operating") for row in category_rows],
        },
        "accountSplit": {
            "labels": [row["label"] for row in account_rows],
            "values": [row["bank_only_total"] for row in account_rows],
        },
        "daily": {
            "labels": [row["label"] for row in daily_rows],
            "operating": [row["operating"] for row in daily_rows],
            "inventory": [row["inventory"] for row in daily_rows],
            "partnerPaybacks": [row["partner_paybacks"] for row in daily_rows],
            "nonOperating": [row["non_operating"] for row in daily_rows],
            "uncategorized": [row["uncategorized"] for row in daily_rows],
            "alreadyLogged": [row["already_logged"] for row in daily_rows],
            "bankOnlyTotal": [row["bank_only_total"] for row in daily_rows],
            "total": [row["total"] for row in daily_rows],
        },
    }


@router.get("/reports/summary")
def report_summary(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    summary = build_financial_summary(rows)
    summary["filters"] = {
        "start": start_dt.isoformat() if start_dt else None,
        "end": end_dt.isoformat() if end_dt else None,
        "channel_id": channel_id,
    }
    return summary


@router.get("/reports/messages")
def report_messages(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    if entry_kind:
        rows = [row for row in rows if row.entry_kind == entry_kind]
    return build_message_list_items(session, rows)


@router.get("/reports/export.csv")
def report_transactions_csv(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    transactions = get_transactions(
        session,
        start=start_dt,
        end=end_dt,
        channel_id=channel_id,
        entry_kind=entry_kind,
    )
    rows = [
        {
            "transaction_id": row.id,
            "source_message_id": row.source_message_id,
            "occurred_at": row.occurred_at.isoformat(sep=" ", timespec="seconds"),
            "channel_name": row.channel_name or "",
            "author_name": row.author_name or "",
            "entry_kind": row.entry_kind or "",
            "deal_type": row.deal_type or "",
            "amount": row.amount or "",
            "money_in": row.money_in or "",
            "money_out": row.money_out or "",
            "payment_method": row.payment_method or "",
            "cash_direction": row.cash_direction or "",
            "category": row.category or "",
            "expense_category": row.expense_category or "",
            "needs_review": row.needs_review,
            "confidence": row.confidence or "",
            "notes": row.notes or "",
            "trade_summary": row.trade_summary or "",
            "source_content": row.source_content or "",
        }
        for row in transactions
    ]
    return csv_response("transactions-report.csv", rows or [{"message": "No transactions matched the current filters"}])


@router.get("/messages/export.csv")
def messages_csv(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    expense_category: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    sort_by: str = Query(default="time"),
    sort_dir: str = Query(default="desc"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    rows, _ = get_message_rows(
        session,
        status=status,
        channel_id=channel_id,
        entry_kind=entry_kind,
        expense_category=expense_category,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=1,
        limit=50000,
    )
    items = build_message_list_items(session, rows, expense_category=expense_category)
    export_rows = [
        {
            "message_id": item["id"],
            "time": item["time"] or "",
            "channel": item["channel"] or "",
            "channel_id": item["channel_id"] or "",
            "author": item["author"] or "",
            "status": item["status"] or "",
            "entry_kind": item["entry_kind"] or "",
            "deal_type": item["type"] or "",
            "amount": item["amount"] if item["amount"] is not None else "",
            "payment_method": item["payment"] or "",
            "cash_direction": item["cash_direction"] or "",
            "category": item["category"] or "",
            "money_in": item["money_in"] if item["money_in"] is not None else "",
            "money_out": item["money_out"] if item["money_out"] is not None else "",
            "expense_category": item["expense_category"] or "",
            "needs_review": item["needs_review"],
            "notes": item["notes"] or "",
            "message": item["message"] or "",
        }
        for item in items
    ]
    return csv_response("messages-export.csv", export_rows or [{"message": "No messages matched the current filters"}])


@router.get("/reports", response_class=HTMLResponse)
def reports_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=REPORT_SOURCE_ALL),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    selected_source = normalize_report_source(source)
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)

    reports_cache_key = f"reports:{start or ''}:{end or ''}:{channel_id or ''}:{entry_kind or ''}:{selected_source}"
    cached_reports = cache_get(reports_cache_key)
    if cached_reports is None:
        transactions_all = get_transactions(
            session,
            start=start_dt,
            end=end_dt,
            channel_id=channel_id,
            entry_kind=entry_kind,
        )
        discord_summary = build_transaction_summary(transactions_all)
        shopify_rows = get_shopify_reporting_rows(session, start=start_dt, end=end_dt)
        shopify_summary = build_shopify_reporting_summary(shopify_rows)
        tiktok_rows = get_tiktok_reporting_rows(session, start=start_dt, end=end_dt)
        tiktok_summary = build_tiktok_reporting_summary(tiktok_rows)
        shopify_daily_totals = build_shopify_daily_totals(shopify_rows)
        tiktok_daily_totals = list(tiktok_summary.get("daily_totals", []))
        period_rows = build_report_period_comparison_rows(
            session,
            periods=build_reporting_periods(selected_start=start_dt, selected_end=end_dt),
            channel_id=channel_id,
            entry_kind=entry_kind,
        )
        cached_reports = {
            "discord_summary": discord_summary,
            "shopify_summary": shopify_summary,
            "tiktok_summary": tiktok_summary,
            "shopify_daily_totals": shopify_daily_totals,
            "tiktok_daily_totals": tiktok_daily_totals,
            "period_rows": period_rows,
        }
        cache_set(reports_cache_key, cached_reports)
    else:
        discord_summary = cached_reports["discord_summary"]
        shopify_summary = cached_reports["shopify_summary"]
        tiktok_summary = cached_reports["tiktok_summary"]
        shopify_daily_totals = cached_reports["shopify_daily_totals"]
        tiktok_daily_totals = cached_reports["tiktok_daily_totals"]
        period_rows = cached_reports["period_rows"]

    transactions = get_transactions(
        session,
        start=start_dt,
        end=end_dt,
        channel_id=channel_id,
        entry_kind=entry_kind,
        limit=50,
    )
    summary = discord_summary
    channels = get_channel_filter_choices(session)
    report_totals = {
        "discord_gross": round(float(discord_summary["totals"].get("money_in", 0.0) or 0.0), 2),
        "discord_outflow": round(float(discord_summary["totals"].get("money_out", 0.0) or 0.0), 2),
        "discord_net": round(float(discord_summary["totals"].get("net", 0.0) or 0.0), 2),
        "shopify_gross": round(float(shopify_summary["gross_revenue"] or 0.0), 2),
        "shopify_tax": round(float(shopify_summary["total_tax"] or 0.0), 2),
        "shopify_net": round(float(shopify_summary["net_revenue"] or 0.0), 2),
        "tiktok_gross": round(float(tiktok_summary["gross_revenue"] or 0.0), 2),
        "tiktok_tax": round(float(tiktok_summary["total_tax"] or 0.0), 2),
        "tiktok_net": round(float(tiktok_summary["net_revenue"] or 0.0), 2),
        "combined_revenue": round(
            float(discord_summary["totals"].get("money_in", 0.0) or 0.0)
            + float(shopify_summary["net_revenue"] or 0.0)
            + float(tiktok_summary["net_revenue"] or 0.0),
            2,
        ),
    }

    return templates.TemplateResponse(
        request,
        "reports.html",
        {
            "request": request,
            "title": "Reports",
            "channels": channels,
            "selected_start": start or "",
            "selected_end": end or "",
            "selected_channel_id": channel_id or "",
            "selected_entry_kind": entry_kind or "",
            "selected_source": selected_source,
            "summary": summary,
            "discord_summary": discord_summary,
            "shopify_summary": shopify_summary,
            "tiktok_summary": tiktok_summary,
            "report_totals": report_totals,
            "period_rows": period_rows,
            "show_discord_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_DISCORD},
            "show_shopify_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_SHOPIFY},
            "show_tiktok_reports": selected_source in {REPORT_SOURCE_ALL, REPORT_SOURCE_TIKTOK},
            "reports_url": build_reports_url,
            "expense_chart": build_bar_chart_rows(summary["expense_categories"]),
            "channel_chart": build_bar_chart_rows(summary["channel_net"]),
            "transactions": transactions[-50:],
            "shopify_daily_totals": shopify_daily_totals,
            "tiktok_daily_totals": tiktok_daily_totals,
        },
    )


@router.get("/pnl", include_in_schema=False)
def finance_redirect(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/finance", status_code=307)


@router.get("/finance", response_class=HTMLResponse)
def finance_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    window: Optional[str] = Query(default=FINANCE_WINDOW_MTD),
    bank_account: Optional[str] = Query(default="all"),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    range_data = resolve_finance_range(start=start, end=end, window=window)
    selected_bank_account = normalize_bank_account_filter(bank_account if isinstance(bank_account, str) else "all")
    finance_cache_key = f"finance:{start or ''}:{end or ''}:{window or ''}"
    cached_finance = cache_get(finance_cache_key)
    if cached_finance is None:
        current_snapshot = build_finance_range_snapshot(
            session,
            start=range_data["start_dt"],
            end=range_data["end_dt"],
            day_count=int(range_data["day_count"]),
        )
        prior_snapshot = build_finance_range_snapshot(
            session,
            start=range_data["previous_start_dt"],
            end=range_data["previous_end_dt"],
            day_count=int(range_data["day_count"]),
        )
        cache_set(finance_cache_key, {"current": current_snapshot, "prior": prior_snapshot})
    else:
        current_snapshot = cached_finance["current"]
        prior_snapshot = cached_finance["prior"]

    current_statement = current_snapshot["statement"]
    prior_statement = prior_snapshot["statement"]
    source_mix_rows = build_finance_source_mix_rows(current_statement)
    spend_mix_rows = build_finance_spend_mix_rows(current_statement)
    top_channels = build_finance_channel_rows(current_snapshot["transactions"])
    monthly_rows = build_finance_monthly_rows(session)
    bank_expense_data = _decorate_bank_expense_data(
        build_finance_bank_expense_data(
            session,
            start=range_data["start_dt"],
            end=range_data["end_dt"],
            account_filter=selected_bank_account,
        )
    )
    finance_chart_data = build_finance_chart_data(
        daily_rows=current_snapshot["daily_rows"],
        source_mix_rows=source_mix_rows,
        spend_mix_rows=spend_mix_rows,
        monthly_rows=monthly_rows,
    )
    finance_chart_data["bank"] = _build_bank_expense_chart_data(bank_expense_data)
    analyst_notes = build_finance_notes(
        current_statement=current_statement,
        prior_statement=prior_statement,
        range_label=str(range_data["label"]),
        prior_label=str(range_data["previous_label"]),
        source_mix_rows=source_mix_rows,
        top_channels=top_channels,
    )
    quick_windows = [
        {
            "label": FINANCE_WINDOW_LABELS[window_key],
            "url": build_finance_url(window=window_key, bank_account=selected_bank_account),
            "active": range_data["selected_window"] == window_key,
        }
        for window_key in (
            FINANCE_WINDOW_MTD,
            FINANCE_WINDOW_30D,
            FINANCE_WINDOW_90D,
            FINANCE_WINDOW_YTD,
        )
    ]
    bank_account_views = [
        {
            "key": account_key,
            "label": account_label,
            "url": build_finance_url(
                start=str(range_data["selected_start"] or ""),
                end=str(range_data["selected_end"] or ""),
                window=str(range_data["selected_window"]),
                bank_account=account_key,
            ),
            "active": selected_bank_account == account_key,
        }
        for account_key, account_label in BANK_ACCOUNT_FILTER_LABELS.items()
    ]

    return templates.TemplateResponse(
        request,
        "finance.html",
        {
            "request": request,
            "title": "Executive Finance",
            "current_user": getattr(request.state, "current_user", None),
            "selected_start": range_data["selected_start"],
            "selected_end": range_data["selected_end"],
            "selected_window": range_data["selected_window"],
            "selected_bank_account": selected_bank_account,
            "range_data": range_data,
            "quick_windows": quick_windows,
            "bank_account_views": bank_account_views,
            "current_statement": current_statement,
            "prior_statement": prior_statement,
            "kpi_rows": build_finance_kpi_rows(current_statement, prior_statement),
            "statement_rows": build_finance_statement_rows(current_statement, prior_statement),
            "source_mix_rows": source_mix_rows,
            "spend_mix_rows": spend_mix_rows,
            "top_channels": top_channels,
            "analyst_notes": analyst_notes,
            "quality_rows": build_finance_quality_rows(
                current_statement=current_statement,
                range_data=range_data,
            ),
            "monthly_rows": monthly_rows,
            "bank_expense_data": bank_expense_data,
            "finance_chart_data": finance_chart_data,
            "finance_url": build_finance_url,
        },
    )
