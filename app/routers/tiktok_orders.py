"""
TikTok Orders routes.

Extracted from app/main.py -- all routes under /tiktok/orders/, /tiktok redirect,
and the POST /webhooks/tiktok/orders webhook handler.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import func
from sqlmodel import Session, select

from ..csrf import CSRFProtectedRoute
from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..shared import (  # noqa: F401 - explicit imports for underscore-prefixed names
    _get_app_setting,
    _get_live_sessions_list,
    _start_tiktok_webhook_enrichment,
)
from ..db import get_session, run_write_with_retry
from ..config import get_settings
from ..models import TikTokAuth, TikTokOrder, utcnow
from ..reporting import build_tiktok_orders_page_data as build_tiktok_orders_page_reporting_data, parse_report_datetime
from ..runtime_logging import structured_log_line
from ..tiktok_ingest import (
    _build_webhook_signature_candidates,
    parse_tiktok_webhook_headers,
    parse_tiktok_webhook_payload,
    upsert_tiktok_order_from_payload,
)

from .tiktok_streamer import _account_scope_from_live_session, _compute_buyer_lifetime_totals
from ..tiktok_alerts import alert_ghost_cancellation, alert_reverse_order

settings = get_settings()

router = APIRouter(route_class=CSRFProtectedRoute)


# TikTok Shop webhook envelope "type" values. Unlike most platforms, TikTok
# sends the event type as an integer inside the JSON body (envelope.type) --
# not in an HTTP header. All subscribed types flow through the single
# registered callback URL, so this handler sees everything (order changes,
# package updates, settlement notifications, seller deauthorization, etc.).
#
# Only a subset actually carries an order identifier in data.order_id.
# Everything else must be acknowledged with 200 OK, otherwise TikTok retries
# aggressively and we get burst storms of "missing order identifier" logs.
TIKTOK_WEBHOOK_TYPE_NAMES: dict[int, str] = {
    1: "ORDER_STATUS_CHANGE",
    2: "REVERSE_ORDER_STATUS_CHANGE",
    3: "RECIPIENT_ADDRESS_UPDATE",
    4: "PACKAGE_UPDATE",
    5: "CANCELLATION_STATUS_CHANGE",
    6: "NEW_SETTLED_DEDUCTION",
    7: "SELLER_DEAUTHORIZATION",
    8: "PRODUCT_CHANGE",
    9: "PRODUCT_CATEGORY_CHANGED",
    10: "PRODUCT_STATUS_CHANGED",
    11: "PRODUCT_AUDIT_STATUS_CHANGE",
    12: "SETTLEMENT_INFO",
    13: "PRODUCT_INVENTORY_UPDATE",
    14: "PROMOTION_STATUS_UPDATE",
    15: "RETURN_REFUND_UPDATE",
    16: "OPEN_PLATFORM_AUTHORIZED",
    17: "CHAT_MESSAGE",
    18: "SHIPPING_INFO_UPDATE",
    21: "ORDER_FULFILLMENT_UPDATE",
}

# Types we actually persist as TikTokOrder rows. Everything else is
# acknowledged but not written (we log it so we can see it in the audit trail).
TIKTOK_ORDER_WEBHOOK_TYPES: frozenset[int] = frozenset({1, 2, 3, 5, 21})


# ---------------------------------------------------------------------------
# Helper functions (only used by routes in this module)
# ---------------------------------------------------------------------------

def _get_tiktok_filter_options(session: Session) -> dict[str, list[str]]:
    def _distinct(col):
        try:
            return sorted({v for v in session.exec(select(col).distinct()).all() if v not in (None, "")})
        except Exception as exc:
            print(structured_log_line(runtime="app", action="tiktok.filter_options.distinct_failed", success=False, error=str(exc)))
            return []
    return {
        "financial_statuses": _distinct(TikTokOrder.financial_status),
        "fulfillment_statuses": _distinct(TikTokOrder.fulfillment_status),
        "order_statuses": _distinct(TikTokOrder.order_status),
        "source_options": _distinct(TikTokOrder.source),
        "currency_options": _distinct(TikTokOrder.currency),
    }


def _first_scope_value(scope: dict, key: str) -> Optional[str]:
    values = sorted(str(v).strip() for v in (scope.get(key) or []) if str(v).strip())
    return values[0] if len(values) == 1 else None


def _collect_tiktok_orders_page_data(
    session: Session,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    shop_id: Optional[str] = None,
    shop_cipher: Optional[str] = None,
    seller_id: Optional[str] = None,
    financial_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    order_status: Optional[str] = None,
    source: Optional[str] = None,
    currency: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "date",
    sort_dir: str = "desc",
    page: int = 1,
    limit: int = 50,
) -> dict[str, object]:
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    auth_row = ensure_tiktok_auth_row(session)
    integration_state = read_tiktok_integration_state()
    sync_snapshot = describe_tiktok_sync_status(auth_row, integration_state)
    page_data = build_tiktok_orders_page_reporting_data(
        session,
        start=start_dt,
        end=end_dt,
        shop_id=shop_id,
        shop_cipher=shop_cipher,
        seller_id=seller_id,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        source=source,
        currency=currency,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    recent_orders = [
        {
            "order": row,
            "customer_label": (row.customer_name or "").strip() or "Guest",
            "items_summary": build_tiktok_item_summary(
                row.line_items_summary_json or "",
                row.line_items_json or "",
            ),
            "net_amount": (
                round(float(row.subtotal_ex_tax or 0.0), 2)
                if row.subtotal_ex_tax is not None
                else round(float(row.total_price or 0.0) - float(row.total_tax or 0.0), 2)
            ),
        }
        for row in page_data["rows"]
    ]
    return {
        "summary": page_data["summary"],
        "orders": recent_orders,
        "auth_row": auth_row,
        "sync_snapshot": sync_snapshot,
        "integration_state": integration_state,
        "daily_totals": page_data.get("daily_totals", []),
        "line_item_summary": page_data.get("line_item_summary", {}),
        "total_count": page_data.get("total_count", 0),
        "page": page_data.get("page", max(page, 1)),
        "page_size": page_data.get("page_size", limit),
        "has_more": page_data.get("has_more", False),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/webhooks/tiktok/orders")
async def tiktok_orders_webhook(request: Request):
    raw_body = await request.body()
    runtime_name = f"{settings.runtime_name}_tiktok"
    body_hash = hashlib.sha256(raw_body).hexdigest()

    primary_secret = (settings.tiktok_app_secret or "").strip()
    configured_shop_id = (settings.tiktok_shop_id or "").strip()

    topic = (
        request.headers.get("X-TikTok-Topic")
        or request.headers.get("X-Event-Type")
        or request.headers.get("X-TT-Event")
        or "unknown"
    )

    try:
        payload = parse_tiktok_webhook_payload(
            raw_body,
            app_secret=primary_secret,
            app_key=(settings.tiktok_app_key or "").strip(),
            headers=request.headers,
            request_path=str(request.url.path),
            strict_signature=True,
        )
    except Exception as exc:
        update_tiktok_integration_state(
            last_error=str(exc),
            last_webhook={
                "received_at": utcnow().isoformat(),
                "topic": topic,
                "body_sha256": body_hash,
                "error": str(exc),
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.failed",
                success=False,
                error=str(exc),
                topic=topic,
                request_path=str(request.url.path),
                body_sha256=body_hash,
            )
        )
        raise HTTPException(status_code=400, detail="Invalid TikTok webhook payload") from exc

    sig_verified = payload.pop("_signature_verified", False)
    payload_shop_id = str(payload.get("shop_id") or "").strip()
    shop_id_matches = (
        not configured_shop_id
        or not payload_shop_id
        or payload_shop_id == configured_shop_id
    )

    if not sig_verified and not shop_id_matches:
        err = f"Unverified webhook with mismatched shop_id (got {payload_shop_id})"
        update_tiktok_integration_state(
            last_error=err,
            last_webhook={
                "received_at": utcnow().isoformat(),
                "topic": topic,
                "body_sha256": body_hash,
                "error": err,
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.rejected",
                success=False,
                error=err,
                topic=topic,
                payload_shop_id=payload_shop_id,
                configured_shop_id=configured_shop_id,
            )
        )
        raise HTTPException(status_code=400, detail="Invalid TikTok webhook payload")

    if not sig_verified:
        all_headers = dict(request.headers.items())
        parsed_h = parse_tiktok_webhook_headers(request.headers)
        payload_ts = str(payload.get("timestamp") or "").strip() or None
        header_ts = parsed_h.get("timestamp")
        ts_for_candidates = header_ts or payload_ts
        candidates = _build_webhook_signature_candidates(
            raw_body=raw_body,
            app_secret=primary_secret,
            app_key=(settings.tiktok_app_key or "").strip(),
            received_timestamp=ts_for_candidates,
            request_path=str(request.url.path),
        )
        received_norm = parsed_h.get("signature", "").lower().strip()
        candidate_detail = {label: digest[:16] for label, digest in candidates}
        match_found = any(
            hmac.compare_digest(received_norm, digest.lower())
            for _, digest in candidates
        ) if received_norm else False
        if settings.debug_webhook_capture:
            import pathlib as _pathlib
            _capture_path = _pathlib.Path("logs/webhook_capture.json")
            try:
                _capture_path.write_text(json.dumps({
                    "received_signature": received_norm,
                    "parsed_header_signature": parsed_h.get("signature"),
                    "parsed_header_timestamp": header_ts,
                    "request_path": str(request.url.path),
                    "payload_timestamp": payload_ts,
                }, indent=2))
            except Exception as exc:
                print(
                    structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.webhook.capture_write_failed",
                        success=False,
                        context="tiktok_orders.tiktok_orders_webhook",
                        error=str(exc)[:400],
                    )
                )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.signature_debug",
                success=True,
                topic=topic,
                body_sha256=body_hash,
                shop_id_matches=shop_id_matches,
                payload_shop_id=payload_shop_id,
                all_headers=all_headers,
                received_sig=received_norm[:16] if received_norm else None,
                header_timestamp=header_ts,
                payload_timestamp=payload_ts,
                candidate_digests=candidate_detail,
                any_match=match_found,
                secret_len=len(primary_secret),
                body_len=len(raw_body),
            )
        )

    envelope_type_raw = payload.get("type") if isinstance(payload, dict) else None
    try:
        envelope_type = int(envelope_type_raw) if envelope_type_raw is not None else None
    except (TypeError, ValueError):
        envelope_type = None

    event_type_name = TIKTOK_WEBHOOK_TYPE_NAMES.get(
        envelope_type,
        f"type_{envelope_type}" if envelope_type is not None else "unknown",
    )
    if topic == "unknown":
        topic = event_type_name

    # Order identifier may live under envelope.data.order_id (real TikTok
    # webhooks), at the top level (legacy flat payloads / tests), or inside
    # a list like data.order_list. Detect any of these so we only attempt
    # an order upsert when something sensible is present.
    def _has_order_id(obj) -> bool:
        if not isinstance(obj, dict):
            return False
        if str(obj.get("order_id") or "").strip():
            return True
        for list_key in ("order_list", "orders"):
            lst = obj.get(list_key)
            if isinstance(lst, list):
                for item in lst:
                    if isinstance(item, dict) and str(item.get("order_id") or "").strip():
                        return True
        return False

    inner_data = payload.get("data") if isinstance(payload, dict) else None
    has_order_identifier = _has_order_id(payload) or _has_order_id(inner_data)
    is_order_type = envelope_type in TIKTOK_ORDER_WEBHOOK_TYPES or has_order_identifier

    order_upsert_status = ""
    order_record: dict = {}
    if isinstance(payload, dict) and is_order_type:
        try:
            def persist_tiktok_order(session: Session):
                return upsert_tiktok_order_from_payload(
                    session,
                    TikTokOrder,
                    payload,
                    source="webhook",
                    received_at=utcnow(),
                    dry_run=False,
                )

            order_upsert_status, order_record = await asyncio.to_thread(run_write_with_retry, persist_tiktok_order)
        except Exception as exc:
            # If the payload looked like an order type but the upsert couldn't
            # find an order id (malformed / unexpected shape), ack with 200 so
            # TikTok does not retry-storm us. Real DB errors still return 500.
            err_text = str(exc)
            if "missing an order identifier" in err_text:
                print(
                    structured_log_line(
                        runtime=runtime_name,
                        action="tiktok.webhook.non_order_payload_skipped",
                        success=True,
                        topic=topic,
                        event_type=envelope_type,
                        event_type_name=event_type_name,
                        request_path=str(request.url.path),
                        body_sha256=body_hash,
                        error=err_text,
                    )
                )
                # Alert on ghost cancellations — TikTok cancelled something
                # but didn't tell us which order.
                if envelope_type == 5:  # CANCELLATION_STATUS_CHANGE
                    alert_ghost_cancellation(
                        event_type_name=event_type_name,
                        body_sha256=body_hash,
                    )
                return Response(status_code=200)
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.webhook.order_upsert_failed",
                    success=False,
                    error=err_text,
                    topic=topic,
                    event_type=envelope_type,
                    event_type_name=event_type_name,
                    request_path=str(request.url.path),
                    body_sha256=body_hash,
                )
            )
            return Response(status_code=500)
        else:
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.webhook.order_upserted",
                    success=True,
                    topic=topic,
                    event_type=envelope_type,
                    event_type_name=event_type_name,
                    order_status=order_upsert_status,
                    tiktok_order_id=order_record.get("tiktok_order_id"),
                    shop_id=order_record.get("shop_id"),
                )
            )
            enrich_order_id = (order_record.get("tiktok_order_id") or "").strip()
            _start_tiktok_webhook_enrichment(enrich_order_id)
            # Alert immediately on reverse / cancellation events.
            if envelope_type in (2, 5):  # REVERSE_ORDER_STATUS_CHANGE or CANCELLATION_STATUS_CHANGE
                _price = order_record.get("total_price")
                alert_reverse_order(
                    tiktok_order_id=order_record.get("tiktok_order_id", ""),
                    customer_name=order_record.get("customer_name", ""),
                    total_price=float(_price) if _price is not None else None,
                    order_status=order_record.get("order_status", ""),
                    upsert_status=order_upsert_status,
                )
    elif isinstance(payload, dict):
        # Non-order webhook type (package update, settlement, seller deauth,
        # product change, etc). Acknowledge so TikTok stops retrying.
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.webhook.non_order_received",
                success=True,
                topic=topic,
                event_type=envelope_type,
                event_type_name=event_type_name,
                request_path=str(request.url.path),
                body_sha256=body_hash,
                payload=summarize_tiktok_payload(payload),
            )
        )

    webhook_summary = {
        "received_at": utcnow().isoformat(),
        "topic": topic,
        "body_sha256": body_hash,
        "payload": summarize_tiktok_payload(payload),
        "order_status": order_upsert_status or None,
        "tiktok_order_id": order_record.get("tiktok_order_id"),
    }
    update_tiktok_integration_state(
        last_webhook_at=utcnow(),
        last_webhook=webhook_summary,
        last_error=None,
    )
    print(
        structured_log_line(
            runtime=runtime_name,
            action="tiktok.webhook.received",
            success=True,
            topic=topic,
            event_type=envelope_type,
            event_type_name=event_type_name,
            request_path=str(request.url.path),
            body_sha256=body_hash,
            payload=summarize_tiktok_payload(payload),
        )
    )
    return Response(status_code=200)


@router.get("/tiktok", include_in_schema=False)
def tiktok_orders_redirect(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/tiktok/orders", status_code=307)

@router.post("/tiktok/orders/sync-form")
def tiktok_orders_sync_form(
    request: Request,
    since: Optional[str] = Form(default=None),
    limit: Optional[str] = Form(default=""),
):
    if denial := require_role_response(request, "admin"):
        return denial

    if read_tiktok_integration_state().get("is_pull_running"):
        return RedirectResponse(
            url="/tiktok/orders?success=TikTok+sync+already+running",
            status_code=303,
        )

    raw_limit = (limit or "").strip()
    safe_limit: Optional[int]
    if not raw_limit:
        safe_limit = settings.tiktok_sync_limit
    else:
        try:
            safe_limit = max(int(raw_limit), 1)
        except ValueError:
            return RedirectResponse(
                url="/tiktok/orders?error=Sync+limit+must+be+a+number",
                status_code=303,
            )

    thread = threading.Thread(
        target=run_tiktok_pull_in_background,
        kwargs={
            "since": (since or "").strip() or None,
            "limit": safe_limit,
            "trigger": "manual",
        },
        daemon=True,
        name="tiktok-pull-manual",
    )
    thread.start()
    return RedirectResponse(
        url="/tiktok/orders?success=Started+TikTok+sync+orders+will+appear+shortly",
        status_code=303,
    )


@router.get("/tiktok/orders", response_class=HTMLResponse)
def tiktok_orders_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    stream: Optional[str] = Query(default=None),
    financial_status: Optional[str] = Query(default=None),
    fulfillment_status: Optional[str] = Query(default=None),
    order_status: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    currency: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1),
    limit: int = Query(default=50, ge=1, le=100),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    effective_start = start
    effective_end = end
    stream_shop_id = None
    stream_shop_cipher = None
    stream_seller_id = None
    if stream:
        stream_sessions = _get_live_sessions_list()
        match = next((s for s in stream_sessions if s.get("id") == stream), None)
        if match:
            s_ts = match.get("start_time") or 0
            e_ts = match.get("end_time") or 0
            if s_ts > 0:
                effective_start = datetime.fromtimestamp(s_ts, tz=PACIFIC_TZ).isoformat()
            if e_ts > 0:
                effective_end = datetime.fromtimestamp(e_ts, tz=PACIFIC_TZ).isoformat()
            selected_creator = str(match.get("username") or "").strip().lstrip("@").lower()
            account_scope = _account_scope_from_live_session(match, selected_creator)
            stream_shop_id = _first_scope_value(account_scope, "shop_ids")
            if not stream_shop_id:
                stream_shop_cipher = _first_scope_value(account_scope, "shop_ciphers")
            if not stream_shop_id and not stream_shop_cipher:
                stream_seller_id = _first_scope_value(account_scope, "seller_ids")

    page_data = _collect_tiktok_orders_page_data(
        session,
        start=effective_start,
        end=effective_end,
        shop_id=stream_shop_id,
        shop_cipher=stream_shop_cipher,
        seller_id=stream_seller_id,
        financial_status=financial_status,
        fulfillment_status=fulfillment_status,
        order_status=order_status,
        source=source,
        currency=currency,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        limit=limit,
    )
    total_count = int(page_data.get("total_count", 0) or 0)
    pagination = build_pagination(page=page, limit=limit, total_rows=total_count)
    filter_opts = _get_tiktok_filter_options(session)
    sync_since_default = (utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    unfiltered_total = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())
    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()
    orders = page_data["orders"]
    vip_threshold = float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000")
    buyer_totals = _compute_buyer_lifetime_totals(session)
    for row in orders:
        buyer_key = (row["customer_label"]).strip().lower() or "guest"
        spent = buyer_totals.get(buyer_key, 0.0)
        row["is_vip"] = spent >= vip_threshold > 0
        row["lifetime_spent"] = round(spent, 2)
    context = {
        "request": request,
        "title": "TikTok Orders",
        "success": success,
        "error": error,
        "summary": page_data["summary"],
        "orders": orders,
        "recent_orders": orders,  # kept for test compatibility
        "daily_totals": page_data.get("daily_totals", []),
        "line_item_summary": page_data.get("line_item_summary", {}),
        "auth_row": page_data["auth_row"],
        "sync_snapshot": page_data["sync_snapshot"],
        "integration_state": page_data["integration_state"],
        "pagination": pagination,
        "selected_start": effective_start or "",
        "selected_end": effective_end or "",
        "selected_stream": stream or "",
        "stream_sessions": _get_live_sessions_list(),
        "selected_financial_status": financial_status or "",
        "selected_fulfillment_status": fulfillment_status or "",
        "selected_order_status": order_status or "",
        "selected_source": source or "",
        "selected_currency": currency or "",
        "selected_search": search or "",
        "selected_sort_by": sort_by,
        "selected_sort_dir": sort_dir,
        "financial_statuses": filter_opts["financial_statuses"],
        "fulfillment_statuses": filter_opts["fulfillment_statuses"],
        "order_statuses": filter_opts["order_statuses"],
        "source_options": filter_opts["source_options"],
        "currency_options": filter_opts["currency_options"],
        "auto_sync_enabled": bool(settings.tiktok_sync_enabled),
        "sync_interval_minutes": int(settings.tiktok_sync_interval_minutes or 0),
        "sync_since_default": sync_since_default,
        "sync_limit_default": int(settings.tiktok_sync_limit or 250),
        "current_user": getattr(request.state, "current_user", None),
        "page_url": build_tiktok_orders_url,
        "sort_url": build_tiktok_sort_url,
        "unfiltered_total": unfiltered_total,
        "latest_updated_at": latest_updated_at_text,
        "vip_buyer_threshold": vip_threshold,
    }
    return templates.TemplateResponse(request, "tiktok_orders.html", context)

@router.get("/tiktok/orders/poll")
def tiktok_orders_poll(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    total = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())
    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()
    return {"total": total, "latest_updated_at": latest_updated_at_text}
