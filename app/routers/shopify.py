"""
Shopify & TikTok OAuth routes.

Extracted from app/main.py -- Shopify webhook/orders/backfill and TikTok OAuth callbacks.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import threading
from typing import Optional
from urllib.parse import urlencode, urlparse, urlunparse

from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from sqlalchemy import distinct
from sqlmodel import Session, select

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..db import get_session

router = APIRouter()

TIKTOK_SHOP_OAUTH_AUTHORIZE_URL = "https://auth.tiktok-shops.com/oauth/authorize"
TIKTOK_CREATOR_OAUTH_AUTHORIZE_URL = "https://www.tiktok.com/v2/auth/authorize/"
TIKTOK_CREATOR_OAUTH_SCOPE = "data.analytics.public.read"


def _tiktok_creator_redirect_uri() -> str:
    shop_uri = (settings.tiktok_redirect_uri or "").strip()
    if shop_uri:
        trimmed = shop_uri.rstrip("/")
        if trimmed.endswith("/integrations/tiktok/callback"):
            return trimmed[: -len("/integrations/tiktok/callback")] + "/integrations/tiktok/creator-callback"
        parsed = urlparse(shop_uri)
        if parsed.scheme and parsed.netloc:
            return urlunparse((parsed.scheme, parsed.netloc, "/integrations/tiktok/creator-callback", "", "", ""))
    base = (settings.public_base_url or "").strip().rstrip("/")
    if base:
        return f"{base}/integrations/tiktok/creator-callback"
    return ""


@router.post("/webhooks/shopify/orders")
async def shopify_orders_webhook(request: Request):
    raw_body = await request.body()
    received_hmac = request.headers.get("X-Shopify-Hmac-Sha256")
    topic = request.headers.get("X-Shopify-Topic") or "unknown"
    webhook_runtime = f"{settings.runtime_name}_shopify"

    if not validate_shopify_webhook(
        raw_body=raw_body,
        shared_secret=settings.shopify_webhook_secret,
        received_hmac=received_hmac,
    ):
        body_hash = hashlib.sha256(raw_body).hexdigest()
        print(
            structured_log_line(
                runtime=webhook_runtime,
                action="shopify.webhook.rejected",
                success=False,
                error="Invalid Shopify webhook HMAC",
                topic=topic,
                request_path=str(request.url.path),
                body_sha256=body_hash,
            )
        )
        raise HTTPException(status_code=401, detail="Invalid Shopify webhook signature")

    try:
        payload = json.loads(raw_body.decode("utf-8"))
        received_at = utcnow()
        def write_shopify_order(session: Session) -> str:
            return upsert_shopify_order(
                session,
                payload,
                source="webhook",
                received_at=received_at,
                runtime_name=webhook_runtime,
            )

        outcome = await asyncio.to_thread(run_write_with_retry, write_shopify_order)
        # Mark inventory items sold if any line item SKU matches a DGN-XXXXXX barcode
        try:
            def _mark_sold(session: Session) -> int:
                return mark_inventory_sold_from_shopify_order(
                    session, payload, runtime_name=webhook_runtime
                )
            await asyncio.to_thread(run_write_with_retry, _mark_sold)
        except Exception as _inv_exc:
            print(
                structured_log_line(
                    runtime=webhook_runtime,
                    action="inventory.sold_marking.failed",
                    success=False,
                    error=str(_inv_exc),
                )
            )
    except Exception as exc:
        print(
            structured_log_line(
                runtime=webhook_runtime,
                action="shopify.webhook.failed",
                success=False,
                error=str(exc),
                topic=topic,
            )
        )
        raise HTTPException(status_code=400, detail="Invalid Shopify payload") from exc

    print(
        structured_log_line(
            runtime=webhook_runtime,
            action="shopify.webhook.received",
            success=True,
            topic=topic,
            shopify_order_id=payload.get("id"),
            order_number=payload.get("name") or payload.get("order_number"),
            operation=outcome,
        )
    )
    return Response(status_code=200)


@router.get("/integrations/tiktok/oauth/start")
def tiktok_oauth_shop_start(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    app_key = (settings.tiktok_app_key or "").strip()
    redirect_uri = (settings.tiktok_redirect_uri or "").strip()
    if not app_key or not redirect_uri:
        raise HTTPException(
            status_code=400,
            detail="TikTok app key and TIKTOK_REDIRECT_URI must be configured to start Shop OAuth",
        )
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
    auth_url = f"{TIKTOK_SHOP_OAUTH_AUTHORIZE_URL}?{urlencode({'app_key': app_key, 'redirect_uri': redirect_uri, 'state': state})}"
    return RedirectResponse(url=auth_url, status_code=302)


@router.get("/integrations/tiktok/oauth/creator-start")
def tiktok_oauth_creator_start(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    app_key = (settings.tiktok_app_key or "").strip()
    creator_redirect = _tiktok_creator_redirect_uri()
    if not app_key or not creator_redirect:
        raise HTTPException(
            status_code=400,
            detail="TikTok app key and a creator callback URL (derive from TIKTOK_REDIRECT_URI or PUBLIC_BASE_URL) are required",
        )
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state
    params = {
        "client_key": app_key,
        "response_type": "code",
        "scope": TIKTOK_CREATOR_OAUTH_SCOPE,
        "redirect_uri": creator_redirect,
        "state": state,
    }
    auth_url = f"{TIKTOK_CREATOR_OAUTH_AUTHORIZE_URL}?{urlencode(params)}"
    return RedirectResponse(url=auth_url, status_code=302)


@router.get("/integrations/tiktok/callback")
def tiktok_oauth_callback(request: Request):
    query_params = dict(request.query_params)
    query_state = (query_params.get("state") or "").strip()
    stored_state = request.session.pop("oauth_state", None)
    if not query_state or not stored_state or query_state != stored_state:
        raise HTTPException(status_code=403, detail="Invalid OAuth state")
    runtime_name = f"{settings.runtime_name}_tiktok"
    received_at = utcnow()
    app_key = (query_params.get("app_key") or "").strip()
    code = (query_params.get("code") or "").strip()
    if not code:
        update_tiktok_integration_state(
            last_error="Missing authorization code",
            last_callback={
                "received_at": received_at.isoformat(),
                "query": summarize_tiktok_query_params(query_params),
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.oauth.callback.failed",
                success=False,
                error="Missing TikTok authorization code",
                request_path=str(request.url.path),
                query=summarize_tiktok_query_params(query_params),
            )
        )
        return RedirectResponse(
            url="/status?error=TikTok+callback+missing+authorization+code",
            status_code=303,
        )

    expected_app_key = (settings.tiktok_app_key or "").strip()
    if expected_app_key and app_key and app_key != expected_app_key:
        update_tiktok_integration_state(
            last_error="App key mismatch",
            last_callback={
                "received_at": received_at.isoformat(),
                "query": summarize_tiktok_query_params(query_params),
            },
        )
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.oauth.callback.failed",
                success=False,
                error="TikTok app key mismatch",
                request_path=str(request.url.path),
                query=summarize_tiktok_query_params(query_params),
            )
        )
        return RedirectResponse(
            url="/status?error=TikTok+callback+app+key+mismatch",
            status_code=303,
        )

    callback_summary = {
        "received_at": received_at.isoformat(),
        "query": summarize_tiktok_query_params(query_params),
    }
    missing_auth_config = [
        label
        for label, value in (
            ("app key", settings.tiktok_app_key),
            ("app secret", settings.tiktok_app_secret),
        )
        if not (value or "").strip()
    ]
    if not missing_auth_config:
        try:
            token_result = exchange_tiktok_authorization_code(
                auth_code=code,
                app_key=(settings.tiktok_app_key or "").strip(),
                app_secret=(settings.tiktok_app_secret or "").strip(),
                runtime_name=runtime_name,
            )

            def persist_tiktok_auth(session: Session):
                status, auth_record = upsert_tiktok_auth_from_callback(
                    session,
                    TikTokAuth,
                    token_result=token_result,
                    app_key=(settings.tiktok_app_key or "").strip(),
                    redirect_uri=(settings.tiktok_redirect_uri or "").strip(),
                    fallback_shop_id=(settings.tiktok_shop_id or "").strip(),
                    pending_key_seed=code,
                    source="oauth_callback",
                    received_at=received_at,
                    dry_run=False,
                )
                auth_row = session.exec(
                    select(TikTokAuth).where(
                        TikTokAuth.tiktok_shop_id == auth_record["tiktok_shop_id"]
                    )
                ).first()
                shop_name = auth_row.shop_name if auth_row is not None else None
                return status, auth_record, shop_name

            auth_status, auth_record, shop_name = run_write_with_retry(persist_tiktok_auth)
            auth_lookup_key = str(auth_record.get("tiktok_shop_id") or "").strip()
            auth_pending = auth_lookup_key.startswith("pending:")
            callback_summary["auth_status"] = auth_status
            callback_summary["shop_key_status"] = "pending" if auth_pending else "resolved"
            if auth_pending:
                callback_summary["pending_shop_key"] = auth_lookup_key
            else:
                callback_summary["shop_id"] = auth_lookup_key
            callback_summary["shop_region"] = query_params.get("shop_region") or auth_record.get("shop_region")
            if shop_name:
                callback_summary["shop_name"] = shop_name
        except Exception as exc:
            callback_summary["exchange_error"] = str(exc)
            update_tiktok_integration_state(
                last_error=str(exc),
                last_callback=callback_summary,
            )
            request.session["tiktok_callback"] = callback_summary
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.oauth.callback.exchange_failed",
                    success=False,
                    error=str(exc),
                    request_path=str(request.url.path),
                    query=summarize_tiktok_query_params(query_params),
                )
            )
            return RedirectResponse(
                url="/status?error=TikTok+authorization+exchange+failed",
                status_code=303,
            )
    else:
        missing_label = ", ".join(missing_auth_config)
        config_error = f"TikTok auth config missing: {missing_label}"
        callback_summary["exchange_error"] = config_error
        update_tiktok_integration_state(
            last_error=config_error,
            last_callback=callback_summary,
        )
        request.session["tiktok_callback"] = callback_summary
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.oauth.callback.misconfigured",
                success=False,
                error=config_error,
                request_path=str(request.url.path),
                query=summarize_tiktok_query_params(query_params),
            )
        )
        return RedirectResponse(
            url=f"/status?{urlencode({'error': config_error})}",
            status_code=303,
        )

    update_tiktok_integration_state(
        last_authorization_at=received_at,
        last_callback=callback_summary,
        last_error=None,
    )
    request.session["tiktok_callback"] = callback_summary
    print(
        structured_log_line(
            runtime=runtime_name,
            action="tiktok.oauth.callback.received",
            success=True,
            request_path=str(request.url.path),
            query=summarize_tiktok_query_params(query_params),
            shop_key_status=callback_summary.get("shop_key_status") or "resolved",
        )
    )
    success_message = "TikTok authorization captured"
    if callback_summary.get("shop_key_status") == "pending":
        success_message = "TikTok authorization captured; waiting for shop identifier"
    return RedirectResponse(
        url=f"/status?{urlencode({'success': success_message})}",
        status_code=303,
    )

@router.get("/integrations/tiktok/creator-callback")
def tiktok_creator_oauth_callback(request: Request):
    """Handle Creator-type OAuth callback — stores creator_access_token for live analytics."""
    query_params = dict(request.query_params)
    query_state = (query_params.get("state") or "").strip()
    stored_state = request.session.pop("oauth_state", None)
    if not query_state or not stored_state or query_state != stored_state:
        raise HTTPException(status_code=403, detail="Invalid OAuth state")
    runtime_name = f"{settings.runtime_name}_tiktok_creator"
    received_at = utcnow()
    code = (query_params.get("code") or "").strip()

    if not code:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.failed",
            success=False, error="Missing authorization code",
        ))
        return RedirectResponse(
            url="/status?error=Creator+callback+missing+authorization+code",
            status_code=303,
        )

    app_key = (settings.tiktok_app_key or "").strip()
    app_secret = (settings.tiktok_app_secret or "").strip()
    if not app_key or not app_secret:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.misconfigured",
            success=False, error="Missing app_key or app_secret",
        ))
        return RedirectResponse(
            url="/status?error=Creator+auth+config+missing", status_code=303,
        )

    try:
        token_result = exchange_tiktok_authorization_code(
            auth_code=code,
            app_key=app_key,
            app_secret=app_secret,
            runtime_name=runtime_name,
        )
    except Exception as exc:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.exchange_failed",
            success=False, error=str(exc),
        ))
        return RedirectResponse(
            url="/status?error=Creator+token+exchange+failed", status_code=303,
        )

    creator_access = token_result.access_token
    creator_refresh = token_result.refresh_token
    creator_expires = token_result.access_token_expires_at

    if not creator_access:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.no_token",
            success=False, error="Token exchange returned empty access_token",
        ))
        return RedirectResponse(
            url="/status?error=Creator+token+empty", status_code=303,
        )

    def _persist_creator_token(session: Session):
        auth_row = get_latest_tiktok_auth_row(session)
        if auth_row is None:
            print(structured_log_line(
                runtime=runtime_name, action="tiktok.creator.callback.no_seller_auth",
                success=False,
                error="No existing TikTokAuth row — authorize as Seller first",
            ))
            return False
        auth_row.creator_access_token = creator_access
        auth_row.creator_refresh_token = creator_refresh
        auth_row.creator_token_expires_at = creator_expires
        auth_row.updated_at = received_at
        session.add(auth_row)
        session.commit()
        return True

    try:
        ok = run_write_with_retry(_persist_creator_token)
    except Exception as exc:
        print(structured_log_line(
            runtime=runtime_name, action="tiktok.creator.callback.db_failed",
            success=False, error=str(exc)[:400],
        ))
        return RedirectResponse(
            url="/status?error=Creator+token+save+failed", status_code=303,
        )

    if not ok:
        return RedirectResponse(
            url="/status?error=No+seller+auth+row+found.+Authorize+as+seller+first.",
            status_code=303,
        )

    print(structured_log_line(
        runtime=runtime_name, action="tiktok.creator.callback.success",
        success=True, expires_at=str(creator_expires),
    ))
    return RedirectResponse(
        url=f"/status?{urlencode({'success': 'Creator authorization captured — live analytics enabled'})}",
        status_code=303,
    )

@router.post("/shopify/backfill")
def shopify_backfill_start(
    request: Request,
    since: Optional[str] = Form(default=None),
    limit: Optional[str] = Form(default="250"),
):
    if denial := require_role_response(request, "admin"):
        return denial

    state = read_shopify_backfill_state()
    if state.get("is_running"):
        return RedirectResponse(
            url="/shopify/orders?success=Backfill+already+running+orders+will+appear+shortly",
            status_code=303,
        )

    raw_limit = (limit or "").strip()
    safe_limit: Optional[int]
    if not raw_limit:
        safe_limit = None
    else:
        try:
            parsed_limit = int(raw_limit)
        except ValueError:
            return RedirectResponse(
                url="/shopify/orders?error=Limit+must+be+a+whole+number+or+blank",
                status_code=303,
            )
        safe_limit = max(1, parsed_limit)
    worker = threading.Thread(
        target=run_shopify_backfill_in_background,
        kwargs={"since": (since or "").strip() or None, "limit": safe_limit},
        daemon=True,
        name="shopify-backfill",
    )
    worker.start()
    return RedirectResponse(
        url="/shopify/orders?success=Backfill+started+orders+will+appear+shortly",
        status_code=303,
    )

@router.get("/shopify-orders")
def shopify_orders_redirect(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return RedirectResponse(url="/shopify/orders", status_code=307)

@router.get("/shopify/orders", response_class=HTMLResponse)
def shopify_orders_page(
    request: Request,
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    financial_status: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    total_rows = count_shopify_order_rows(
        session,
        start=start_dt,
        end=end_dt,
        financial_status=financial_status,
        source=source,
        search=search,
    )
    pagination = build_pagination(page=page, limit=50, total_rows=total_rows)
    orders = get_shopify_order_rows(
        session,
        start=start_dt,
        end=end_dt,
        financial_status=financial_status,
        source=source,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=pagination["page"],
        limit=pagination["limit"],
    )
    summary: dict[str, object] = {}
    summary_chunk_size = 1000
    summary_page = 1
    while True:
        chunk = get_shopify_order_rows(
            session,
            start=start_dt,
            end=end_dt,
            financial_status=financial_status,
            source=source,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=summary_page,
            limit=summary_chunk_size,
        )
        if not chunk:
            break
        part = build_shopify_order_summary(chunk)
        summary = merge_shopify_order_summaries(summary, part) if summary else part
        summary_page += 1
        if len(chunk) < summary_chunk_size:
            break
    if not summary:
        summary = build_shopify_order_summary([])
    status_rows = session.exec(select(distinct(ShopifyOrder.financial_status))).all()
    financial_statuses = sorted(
        {
            str(value[0] if isinstance(value, tuple) else value).strip()
            for value in status_rows
            if str(value[0] if isinstance(value, tuple) else value).strip()
        }
    )
    order_rows = [
        {
            "order": order,
            "customer_label": order.customer_name or "Guest",
            "items_summary": build_shopify_item_summary(order.line_items_json),
            "net_amount": round(
                float(order.subtotal_ex_tax if order.subtotal_ex_tax is not None else float(order.total_price or 0.0) - float(order.total_tax or 0.0)),
                2,
            ),
        }
        for order in orders
    ]
    backfill_state = read_shopify_backfill_state()

    return templates.TemplateResponse(
        request,
        "shopify_orders.html",
        {
            "request": request,
            "title": "Shopify Orders",
            "current_user": getattr(request.state, "current_user", None),
            "success": success,
            "error": error,
            "selected_start": start or "",
            "selected_end": end or "",
            "selected_financial_status": financial_status or "",
            "selected_source": source or "",
            "selected_search": search or "",
            "selected_sort_by": sort_by,
            "selected_sort_dir": sort_dir,
            "financial_statuses": [value for value in financial_statuses if value],
            "summary": summary,
            "orders": order_rows,
            "pagination": pagination,
            "page_url": build_shopify_orders_url,
            "sort_url": build_shopify_sort_url,
            "backfill_state": backfill_state,
        },
    )
