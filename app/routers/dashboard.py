"""
Dashboard, status, and ops-log routes.

Extracted from app/main.py.
"""
from __future__ import annotations

from datetime import timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from ..shared import *  # noqa: F401,F403 — shared helpers, constants, state
from ..shared import _is_currently_live as _stream_currently_live, _get_live_session_snapshot as _stream_snap
from ..db import get_session

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    user = get_request_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return RedirectResponse(url=app_home_for_role(user.role), status_code=303)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    user = getattr(request.state, "current_user", None)
    review_summary = get_summary(session, status="review_queue")
    overall_summary = get_summary(session)
    dashboard_snapshot = build_dashboard_snapshot(session)
    dashboard_snapshot.setdefault("today", {})
    dashboard_snapshot["today"].setdefault(
        "shopify",
        {
            "order_count": 0,
            "gross": 0.0,
            "tax": 0.0,
            "net": 0.0,
            "refunds": 0.0,
            "tax_missing_count": 0,
            "includes_tax_count": 0,
            "gross_display": format_dashboard_money(0.0),
            "tax_display": format_dashboard_money(0.0),
            "net_display": format_dashboard_money(0.0),
            "refunds_display": format_dashboard_money(0.0),
        },
    )
    dashboard_snapshot["today"].setdefault(
        "revenue",
        {
            "discord_sales": 0.0,
            "discord_trade_in": 0.0,
            "discord_total": 0.0,
            "shopify_total": 0.0,
            "tiktok_total": 0.0,
            "total": 0.0,
            "total_display": format_dashboard_money(0.0),
            "discord_total_display": format_dashboard_money(0.0),
            "discord_sales_display": format_dashboard_money(0.0),
            "discord_trade_in_display": format_dashboard_money(0.0),
            "shopify_total_display": format_dashboard_money(0.0),
            "tiktok_total_display": format_dashboard_money(0.0),
        },
    )
    dashboard_snapshot["today"].setdefault(
        "purchases",
        {
            "buys": 0.0,
            "trade_out": 0.0,
            "expenses": 0.0,
            "shopify_refunds": 0.0,
            "total": 0.0,
            "total_display": format_dashboard_money(0.0),
            "buys_display": format_dashboard_money(0.0),
            "trade_out_display": format_dashboard_money(0.0),
            "expenses_display": format_dashboard_money(0.0),
            "shopify_refunds_display": format_dashboard_money(0.0),
            "has_shopify_refunds": False,
        },
    )
    parser_progress = get_parser_progress(session)
    today_start_local = utcnow().astimezone(PACIFIC_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start_local = today_start_local + timedelta(days=1)
    today_start = today_start_local.astimezone(timezone.utc)
    tomorrow_start = tomorrow_start_local.astimezone(timezone.utc)
    tiktok_rows = get_tiktok_reporting_rows(session, start=today_start, end=tomorrow_start)
    tiktok_summary = build_tiktok_reporting_summary(tiktok_rows)
    tiktok_auth_row = get_latest_tiktok_auth_row(session)
    tiktok_sync_snapshot = describe_tiktok_sync_status(tiktok_auth_row, read_tiktok_integration_state())
    dashboard_snapshot["today"]["tiktok"] = {
        "order_count": int(tiktok_summary.get("orders", 0) or 0),
        "paid_order_count": int(tiktok_summary.get("paid_orders", 0) or 0),
        "gross": round(float(tiktok_summary.get("gross_revenue", 0.0) or 0.0), 2),
        "tax": round(float(tiktok_summary.get("total_tax", 0.0) or 0.0), 2),
        "net": round(float(tiktok_summary.get("net_revenue", 0.0) or 0.0), 2),
        "order_count_display": str(int(tiktok_summary.get("orders", 0) or 0)),
        "paid_order_count_display": str(int(tiktok_summary.get("paid_orders", 0) or 0)),
        "gross_display": format_dashboard_money(float(tiktok_summary.get("gross_revenue", 0.0) or 0.0)),
        "tax_display": format_dashboard_money(float(tiktok_summary.get("total_tax", 0.0) or 0.0)),
        "net_display": format_dashboard_money(float(tiktok_summary.get("net_revenue", 0.0) or 0.0)),
    }
    dashboard_snapshot["today"]["revenue"]["tiktok_total"] = round(float(tiktok_summary.get("net_revenue", 0.0) or 0.0), 2)
    dashboard_snapshot["today"]["revenue"]["tiktok_total_display"] = format_dashboard_money(
        float(tiktok_summary.get("net_revenue", 0.0) or 0.0)
    )
    dashboard_snapshot["today"]["revenue"]["total"] = round(
        float(dashboard_snapshot["today"]["revenue"].get("discord_total", 0.0) or 0.0)
        + float(dashboard_snapshot["today"]["revenue"].get("shopify_total", 0.0) or 0.0)
        + float(dashboard_snapshot["today"]["revenue"].get("tiktok_total", 0.0) or 0.0),
        2,
    )
    dashboard_snapshot["today"]["revenue"]["total_display"] = format_dashboard_money(
        float(dashboard_snapshot["today"]["revenue"]["total"] or 0.0)
    )
    tiktok_orders: list[dict] = []
    _last_tiktok_paid_dt = None
    recent_tiktok_rows = session.exec(
        select(TikTokOrder)
        .where(TikTokOrder.created_at >= today_start)
        .where(TikTokOrder.created_at <= tomorrow_start)
        .order_by(TikTokOrder.created_at.desc(), TikTokOrder.id.desc())
        .limit(25)
    ).all()
    for order in recent_tiktok_rows:
        if classify_tiktok_reporting_status(order) != "paid":
            continue
        if _last_tiktok_paid_dt is None:
            _last_tiktok_paid_dt = order.created_at
        fulfillment_value = (order.fulfillment_status or order.order_status or "").strip().lower()
        if fulfillment_value in {"fulfilled", "completed", "delivered"}:
            fulfillment_label = "Completed"
        elif fulfillment_value in {"awaiting_shipment", "awaiting_collection"}:
            fulfillment_label = "Awaiting shipment"
        elif fulfillment_value in {"partial", "partially_shipped"}:
            fulfillment_label = "Partial"
        else:
            fulfillment_label = (order.fulfillment_status or order.order_status or "").strip() or "Pending"
        tiktok_orders.append(
            {
                "id": order.id,
                "order_number": order.order_number or order.tiktok_order_id,
                "created_at": format_pacific_datetime(order.created_at, include_zone=False),
                "customer_name": (order.customer_name or "").strip() or "Customer",
                "total_price": round(float(order.total_price or 0.0), 2),
                "fulfillment_label": fulfillment_label,
            }
        )
        if len(tiktok_orders) >= 10:
            break
    tiktok_recent_order_count = len(tiktok_orders)

    # Stream status for hero card
    _snap_now = datetime.now(timezone.utc)
    _live_snap = _stream_snap()
    _live_start_ts = float(_live_snap.get("start_time") or 0)
    _live_end_ts = float(_live_snap.get("end_time") or 0)

    def _ts_mins(ts: float):
        if ts <= 0:
            return None
        return max(0, int((_snap_now.timestamp() - ts) / 60))

    _stream_last_order_mins = None
    if _last_tiktok_paid_dt is not None:
        _dt_utc = _last_tiktok_paid_dt if _last_tiktok_paid_dt.tzinfo else _last_tiktok_paid_dt.replace(tzinfo=timezone.utc)
        _stream_last_order_mins = max(0, int((_snap_now - _dt_utc).total_seconds() / 60))

    stream_status = {
        "active": _stream_currently_live(),
        "started_mins": _ts_mins(_live_start_ts),
        "ended_mins": _ts_mins(_live_end_ts) if _live_end_ts > 0 else None,
        "last_order_mins": _stream_last_order_mins,
        "title": str(_live_snap.get("title") or ""),
    }

    recent_reviewed = build_message_list_items(
        session,
        session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.reviewed_at != None)  # noqa: E711
            .order_by(DiscordMessage.reviewed_at.desc())
            .limit(5)
        ).all(),
    )
    recent_deals = build_message_list_items(
        session,
        session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.is_deleted == False)  # noqa: E712
            .where(DiscordMessage.parse_status.in_(status_filter_values(PARSE_PARSED)))
            .order_by(DiscordMessage.created_at.desc(), DiscordMessage.id.desc())
            .limit(10)
        ).all(),
    )
    shopify_connected = True
    shopify_message = ""
    shopify_orders: list[dict] = []
    try:
        recent_shopify_rows = session.exec(
            select(ShopifyOrder)
            .where(ShopifyOrder.financial_status == "paid")
            .order_by(ShopifyOrder.created_at.desc(), ShopifyOrder.id.desc())
            .limit(10)
        ).all()
        for order in recent_shopify_rows:
            fulfillment_status = (order.fulfillment_status or "").strip().lower()
            if fulfillment_status == "fulfilled":
                fulfillment_label = "Shipped"
            elif fulfillment_status == "partial":
                fulfillment_label = "Partial"
            else:
                fulfillment_label = "Pending"
            shopify_orders.append(
                {
                    "id": order.id,
                    "order_number": order.order_number,
                    "created_at": format_pacific_datetime(order.created_at, include_zone=False),
                    "customer_name": (order.customer_name or "").strip() or "Customer",
                    "total_price": round(float(order.total_price or 0.0), 2),
                    "fulfillment_label": fulfillment_label,
                }
            )
    except OperationalError:
        shopify_connected = False
        shopify_message = "Shopify not connected yet"

    _raw_promotions = list_operations_logs(
        session,
        event_type_prefix="queue.auto_promoted_correction_pattern",
        limit=10,
        since=utcnow() - timedelta(days=7),
    )
    recent_promotions = [
        {
            "normalized_text": (parse_operations_log_details(row) or {}).get("normalized_text") or row.message,
            "promoted_at": row.created_at,
        }
        for row in _raw_promotions
    ]

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "title": "Dashboard",
            "current_user": user,
            "review_summary": review_summary,
            "overall_summary": overall_summary,
            "dashboard_snapshot": dashboard_snapshot,
            "recent_reviewed": recent_reviewed,
            "recent_deals": recent_deals,
            "shopify_connected": shopify_connected,
            "shopify_message": shopify_message,
            "shopify_orders": shopify_orders,
            "tiktok_connected": bool(tiktok_auth_row or tiktok_sync_snapshot.get("has_tokens")),
            "tiktok_message": "" if (tiktok_auth_row or tiktok_sync_snapshot.get("has_tokens")) else "TikTok Shop not connected yet",
            "tiktok_orders": tiktok_orders,
            "tiktok_recent_order_count": tiktok_recent_order_count,
            "parser_progress": parser_progress,
            "tiktok_summary": tiktok_summary,
            "tiktok_sync_snapshot": tiktok_sync_snapshot,
            "tiktok_auth_row": tiktok_auth_row,
            "tiktok_today_rows": tiktok_rows,
            "tiktok_today_totals": tiktok_summary.get("totals", {}),
            "recent_promotions": recent_promotions,
            "stream_status": stream_status,
        },
    )


@router.get("/partner", response_class=HTMLResponse)
def partner_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/dashboard", status_code=301)


@router.get("/status", response_class=HTMLResponse)
def status_page(
    request: Request,
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    def _normalize_optional_query_text(value: object) -> str:
        candidate = getattr(value, "default", value)
        if candidate is None:
            return ""
        if isinstance(candidate, str):
            return candidate.strip()
        return str(candidate).strip()

    success_text = _normalize_optional_query_text(success)
    error_text = _normalize_optional_query_text(error)
    status_snapshot = build_status_snapshot(session)
    query_alerts = [message for message in (error_text, success_text) if message]
    if query_alerts:
        status_snapshot["alert_messages"] = [
            *status_snapshot.get("alert_messages", []),
            *query_alerts,
        ]
    health_snapshot = build_health_snapshot(session)
    debug_snapshot = build_debug_snapshot(session)

    return templates.TemplateResponse(
        request,
        "status.html",
        {
            "request": request,
            "title": "System Status",
            "current_user": getattr(request.state, "current_user", None),
            "snapshot": status_snapshot,
            "health": health_snapshot,
            "debug": debug_snapshot,
        },
    )


@router.get("/status.json")
def status_json(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return build_status_snapshot(session)


@router.get("/ops-log", response_class=HTMLResponse)
def operations_log_page(
    request: Request,
    event_type_prefix: Optional[str] = Query(default=None),
    level: Optional[str] = Query(default=None),
    since: Optional[str] = Query(default=None),
    until: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    since_dt = parse_report_datetime(since)
    until_dt = parse_report_datetime(until, end_of_day=True)

    logs = serialize_operations_logs(
        list_operations_logs(
            session,
            event_type_prefix=event_type_prefix or None,
            level=level or None,
            since=since_dt,
            until=until_dt,
        )
    )

    return templates.TemplateResponse(
        request,
        "ops_log.html",
        {
            "request": request,
            "title": "Operations Log",
            "current_user": getattr(request.state, "current_user", None),
            "logs": logs,
            "snapshot": build_status_snapshot(session),
            "error_badge_count": count_recent_errors(session),
            "selected_event_type_prefix": event_type_prefix or "",
            "selected_level": level or "",
            "selected_since": since or "",
            "selected_until": until or "",
            "event_type_prefixes": ["queue.", "ingest.", "backfill"],
        },
    )


@router.get("/ops-log/error-count")
def ops_log_error_count(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    return {"count": count_recent_errors(session)}


@router.get("/ops-log/backfill/{request_id}", response_class=HTMLResponse)
def backfill_request_detail_page(
    request_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    row = session.get(BackfillRequest, request_id)
    if not row:
        raise HTTPException(status_code=404, detail="Backfill request not found")

    return templates.TemplateResponse(
        request,
        "backfill_request_detail.html",
        {
            "request": request,
            "title": f"Backfill Request {request_id}",
            "current_user": getattr(request.state, "current_user", None),
            "backfill_request": serialize_backfill_request_detail(row),
            "logs": serialize_operations_logs(
                list_operations_logs_for_backfill_request(session, request_id=request_id)
            ),
            "snapshot": build_backfill_queue_snapshot(session),
        },
    )
