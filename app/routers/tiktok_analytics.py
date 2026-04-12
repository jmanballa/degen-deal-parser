"""
TikTok Analytics routes — daily performance, stream list, stream detail,
buyer insights, product performance, and stream comparison.

Extracted from main.py.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import httpx
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlmodel import Session, select

import time
import threading

from ..reporting import (
    TIKTOK_PAID_STATUSES,
    build_buyer_profiles,
    build_buyer_detail,
    build_product_velocity,
    build_product_detail,
)
from ..shared import (
    PACIFIC_TZ,
    TikTokOrder,
    _fetch_live_session_list,
    _fetch_overview_performance_daily,
    _fetch_stream_performance_per_minutes,
    _get_live_analytics_snapshot,
    _get_live_session_snapshot,
    _get_live_sessions_list,
    _resolve_tiktok_api_creds,
    build_tiktok_buyer_insights,
    build_tiktok_product_performance,
    get_session,
    get_settings,
    require_role_response,
    resolve_tiktok_shop_pull_base_url,
    templates,
)

router = APIRouter()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers (only used by the routes in this file)
# ---------------------------------------------------------------------------

_REFUND_STATUSES = {"refunded", "refund_requested", "cancelled", "cancel_requested"}


def _enrich_orders_for_range(session: Session, start_utc: datetime, end_utc: Optional[datetime]) -> dict:
    """Compute top sellers, top buyers, refund rate, and AOV from local orders in [start, end]."""
    q = select(TikTokOrder).where(TikTokOrder.created_at >= start_utc)
    if end_utc is not None:
        q = q.where(TikTokOrder.created_at <= end_utc)
    rows = session.exec(q).all()

    gmv = 0.0
    paid_count = 0
    total_items = 0
    refund_count = 0
    product_agg: dict[str, dict] = {}
    customer_agg: dict[str, dict] = {}

    for o in rows:
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status in _REFUND_STATUSES:
            refund_count += 1
        if status not in TIKTOK_PAID_STATUSES:
            continue
        order_gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        gmv += order_gmv
        paid_count += 1

        buyer_name = (o.customer_name or "").strip() or "Guest"
        buyer_key = buyer_name.lower()
        if buyer_key in customer_agg:
            customer_agg[buyer_key]["spent"] += order_gmv
            customer_agg[buyer_key]["orders"] += 1
        else:
            customer_agg[buyer_key] = {"name": buyer_name, "spent": order_gmv, "orders": 1}

        raw_items: list[dict] = []
        try:
            raw_items = json.loads(o.line_items_json) if o.line_items_json else []
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(
                "tiktok_analytics._enrich_orders_for_range: line_items_json parse failed, using empty list: %s",
                e,
                exc_info=True,
            )
        if not isinstance(raw_items, list):
            raw_items = [raw_items] if isinstance(raw_items, dict) else []

        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title = str(
                raw.get("product_name") or raw.get("sku_name") or raw.get("title") or raw.get("item_name") or ""
            ).strip() or "Unknown item"
            sku_name = str(raw.get("sku_name") or "").strip()
            variant = sku_name if sku_name and sku_name.lower() != "default" and sku_name.lower() != title.lower() else None
            key = (title.lower() + "||" + (variant or "").lower()).strip()

            qty_raw = raw.get("quantity") or raw.get("sku_quantity") or raw.get("count")
            try:
                qty = max(int(qty_raw or 0), 1)
            except (TypeError, ValueError) as e:
                logger.warning(
                    "tiktok_analytics._enrich_orders_for_range: quantity parse failed, defaulting qty=1 (qty_raw=%r): %s",
                    qty_raw,
                    e,
                    exc_info=True,
                )
                qty = 1
            total_items += qty

            unit_price = 0.0
            for pk in ("sale_price", "sku_sale_price", "price", "unit_price"):
                val = raw.get(pk)
                if val is not None:
                    try:
                        unit_price = float(val)
                    except (TypeError, ValueError) as e:
                        logger.warning(
                            "tiktok_analytics._enrich_orders_for_range: unit_price field %r not numeric (%r), trying next key: %s",
                            pk,
                            val,
                            e,
                            exc_info=True,
                        )
                        continue
                    break

            sku_image = str(
                raw.get("sku_image") or raw.get("product_image") or raw.get("image_url") or ""
            ).strip() or None

            if key in product_agg:
                product_agg[key]["qty"] += qty
                product_agg[key]["revenue"] += round(qty * unit_price, 2)
                if not product_agg[key]["sku_image"] and sku_image:
                    product_agg[key]["sku_image"] = sku_image
            else:
                product_agg[key] = {
                    "title": title, "variant": variant, "sku_image": sku_image,
                    "qty": qty, "revenue": round(qty * unit_price, 2),
                }

    top_sellers = sorted(product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:10]
    for ts in top_sellers:
        ts["revenue"] = round(ts["revenue"], 2)
    top_buyers = sorted(customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
    for tb in top_buyers:
        tb["spent"] = round(tb["spent"], 2)

    aov = round(gmv / paid_count, 2) if paid_count > 0 else 0.0
    refund_rate = round(refund_count / len(rows) * 100, 1) if rows else 0.0

    return {
        "gmv": round(gmv, 2),
        "paid_orders": paid_count,
        "total_orders": len(rows),
        "total_items": total_items,
        "aov": aov,
        "refund_count": refund_count,
        "refund_rate": refund_rate,
        "top_sellers": top_sellers,
        "top_buyers": top_buyers,
    }


def _build_daily_from_local_orders(session: Session, days: int) -> list[dict]:
    """Build daily GMV/orders breakdown from local TikTokOrder data as a fallback."""
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=days)
    rows = session.exec(
        select(TikTokOrder).where(TikTokOrder.created_at >= start_utc)
    ).all()
    daily: dict[str, dict] = {}
    for o in rows:
        if not o.created_at:
            continue
        dt = o.created_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        day_key = dt.astimezone(PACIFIC_TZ).strftime("%Y-%m-%d")
        if day_key not in daily:
            daily[day_key] = {"date": day_key, "gmv": 0.0, "sku_orders": 0, "customers": 0, "items_sold": 0,
                              "click_to_order_rate": "", "click_through_rate": "", "_buyers": set()}
        status = (o.financial_status or o.order_status or "").lower().strip()
        if status not in TIKTOK_PAID_STATUSES:
            continue
        order_gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
        daily[day_key]["gmv"] += order_gmv
        daily[day_key]["sku_orders"] += 1
        buyer = (o.customer_name or "").strip().lower() or "guest"
        daily[day_key]["_buyers"].add(buyer)
        try:
            items = json.loads(o.line_items_json) if o.line_items_json else []
            if isinstance(items, list):
                for it in items:
                    qty = int(it.get("quantity") or it.get("sku_quantity") or 1)
                    daily[day_key]["items_sold"] += qty
        except Exception as e:
            logger.warning(
                "tiktok_analytics._build_daily_from_local_orders: skipping line_items for items_sold tally: %s",
                e,
                exc_info=True,
            )
    result = []
    for d in sorted(daily.values(), key=lambda x: x["date"]):
        d["customers"] = len(d.pop("_buyers", set()))
        d["gmv"] = round(d["gmv"], 2)
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/tiktok/analytics/api/debug")
def tiktok_analytics_debug(request: Request):
    """Diagnostic endpoint -- shows what credentials and data sources are available."""
    settings = get_settings()
    access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
    return {
        "has_access_token": bool(access_token),
        "access_token_preview": (access_token[:8] + "...") if access_token else "",
        "has_shop_cipher": bool(shop_cipher),
        "shop_cipher_preview": (shop_cipher[:8] + "...") if shop_cipher else "",
        "has_app_key": bool(app_key),
        "app_key_preview": (app_key[:8] + "...") if app_key else "",
        "app_secret_set": bool((settings.tiktok_app_secret or "").strip()),
        "base_url": resolve_tiktok_shop_pull_base_url(),
        "cached_sessions_count": len(_get_live_sessions_list()),
        "live_session_cache": {k: v for k, v in _get_live_session_snapshot().items() if k != "ok"},
        "live_analytics_cache_ok": _get_live_analytics_snapshot().get("ok"),
    }


@router.get("/tiktok/analytics", response_class=HTMLResponse)
def tiktok_analytics_page(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial
    return templates.TemplateResponse(request, "tiktok_analytics.html", {
        "request": request,
        "title": "TikTok Stream Analytics",
        "current_user": getattr(request.state, "current_user", None),
    })


@router.get("/tiktok/analytics/api/daily")
def tiktok_analytics_daily(
    request: Request,
    days: int = Query(default=30, ge=7, le=90),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    settings = get_settings()
    source = "tiktok_api"
    intervals = []

    if _fetch_overview_performance_daily is not None:
        access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
        if access_token and shop_cipher:
            now = datetime.now(timezone.utc)
            start_str = (now - timedelta(days=days)).strftime("%Y-%m-%d")
            end_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                with httpx.Client(timeout=25.0, follow_redirects=True) as client:
                    intervals = _fetch_overview_performance_daily(
                        client,
                        base_url=resolve_tiktok_shop_pull_base_url(),
                        app_key=app_key,
                        app_secret=(settings.tiktok_app_secret or "").strip(),
                        access_token=access_token,
                        shop_cipher=shop_cipher,
                        start_date=start_str,
                        end_date=end_str,
                    )
            except Exception as e:
                logger.warning(
                    "tiktok_analytics.tiktok_analytics_daily: _fetch_overview_performance_daily failed, using local fallback: %s",
                    e,
                    exc_info=True,
                )
                intervals = []

    if not intervals:
        intervals = _build_daily_from_local_orders(session, days)
        source = "local_orders"

    return {"intervals": intervals, "source": source}


@router.get("/tiktok/analytics/api/streams")
def tiktok_analytics_streams(
    request: Request,
    days: int = Query(default=30, ge=7, le=90),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    settings = get_settings()
    sessions: list[dict] = []

    if _fetch_live_session_list is not None:
        access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()
        if access_token and shop_cipher:
            now = datetime.now(timezone.utc)
            start_str = (now - timedelta(days=days)).strftime("%Y-%m-%d")
            end_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
            try:
                with httpx.Client(timeout=25.0, follow_redirects=True) as client:
                    sessions = _fetch_live_session_list(
                        client,
                        base_url=resolve_tiktok_shop_pull_base_url(),
                        app_key=app_key,
                        app_secret=(settings.tiktok_app_secret or "").strip(),
                        access_token=access_token,
                        shop_cipher=shop_cipher,
                        start_date=start_str,
                        end_date=end_str,
                    )
            except Exception as e:
                logger.warning(
                    "tiktok_analytics.tiktok_analytics_streams: _fetch_live_session_list failed, continuing with empty/cache: %s",
                    e,
                    exc_info=True,
                )
                sessions = []

    source = "tiktok_api" if sessions else "none"

    if not sessions:
        cached = _get_live_sessions_list()
        if cached:
            sessions = cached
            source = "cache"

    for i, s in enumerate(sessions):
        if i > 0:
            prev = sessions[i - 1]
            prev_gmv = prev.get("gmv") or 0
            cur_gmv = s.get("gmv") or 0
            s["gmv_delta_pct"] = round((cur_gmv - prev_gmv) / prev_gmv * 100, 1) if prev_gmv > 0 else None
        else:
            s["gmv_delta_pct"] = None
        dur_s = (s.get("end_time") or 0) - (s.get("start_time") or 0)
        s["duration_hours"] = round(dur_s / 3600, 1) if dur_s > 0 else 0
        s["revenue_per_hour"] = round(s.get("gmv", 0) / (dur_s / 3600), 2) if dur_s > 3600 else None
    sessions.sort(key=lambda s: s.get("end_time") or 0, reverse=True)
    return {"streams": sessions, "source": source}


@router.get("/tiktok/analytics/api/stream/{live_id}")
def tiktok_analytics_stream_detail(
    request: Request,
    live_id: str,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    settings = get_settings()
    access_token, shop_cipher, app_key = _resolve_tiktok_api_creds()

    per_minutes = None
    if _fetch_stream_performance_per_minutes and access_token and shop_cipher:
        with httpx.Client(timeout=25.0, follow_redirects=True) as client:
            per_minutes = _fetch_stream_performance_per_minutes(
                client,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=app_key,
                app_secret=(settings.tiktok_app_secret or "").strip(),
                access_token=access_token,
                shop_cipher=shop_cipher,
                live_id=live_id,
            )

    streams = _get_live_sessions_list()
    stream_info = next((s for s in streams if s.get("id") == live_id), None)

    local_enrichment = {}
    if stream_info:
        start_ts = stream_info.get("start_time") or 0
        end_ts = stream_info.get("end_time") or 0
        if start_ts > 0:
            start_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc)
            end_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts > 0 else None
            local_enrichment = _enrich_orders_for_range(session, start_utc, end_utc)

    return {
        "live_id": live_id,
        "stream": stream_info,
        "per_minutes": per_minutes,
        "local": local_enrichment,
    }


@router.get("/tiktok/analytics/api/buyers")
def tiktok_analytics_buyers(
    request: Request,
    days: int = Query(default=90, ge=7, le=365),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    return {"buyers": build_tiktok_buyer_insights(session, days=days)}


@router.get("/tiktok/analytics/api/products")
def tiktok_analytics_products(
    request: Request,
    days: int = Query(default=30, ge=7, le=365),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    streams = _get_live_sessions_list()
    return {"products": build_tiktok_product_performance(session, days=days, stream_sessions=streams)}


@router.get("/tiktok/analytics/api/compare")
def tiktok_analytics_compare(
    request: Request,
    stream_a: str | None = Query(default=None),
    stream_b: str | None = Query(default=None),
    mode: str | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    streams = _get_live_sessions_list()
    streams.sort(key=lambda s: s.get("start_time", 0) or 0, reverse=True)

    if mode == "weekly":
        now_ts = datetime.now(timezone.utc).timestamp()
        week_end = now_ts - offset * 7 * 86400
        week_start = week_end - 7 * 86400
        prev_end = week_start
        prev_start = prev_end - 7 * 86400

        def _aggregate_week(lo: float, hi: float) -> dict:
            week_streams = [s for s in streams if lo <= (s.get("start_time") or 0) < hi]
            start_utc = datetime.fromtimestamp(lo, tz=timezone.utc)
            end_utc = datetime.fromtimestamp(hi, tz=timezone.utc)
            enriched = _enrich_orders_for_range(session, start_utc, end_utc)
            total_dur = sum(
                ((s.get("end_time") or s.get("start_time", 0)) - s.get("start_time", 0)) / 3600
                for s in week_streams
            )
            return {
                "label": start_utc.strftime("%b %d") + " - " + end_utc.strftime("%b %d"),
                "stream_count": len(week_streams),
                "gmv": enriched.get("gmv", 0),
                "orders": enriched.get("paid_orders", 0),
                "items": enriched.get("total_items", 0),
                "customers": len(enriched.get("top_buyers", [])),
                "duration_hours": round(total_dur, 1),
                "revenue_per_hour": round(enriched.get("gmv", 0) / total_dur, 2) if total_dur > 0 else 0,
                "top_sellers": enriched.get("top_sellers", [])[:5],
                "top_buyers": enriched.get("top_buyers", [])[:5],
            }

        return {
            "mode": "weekly",
            "a": _aggregate_week(week_start, week_end),
            "b": _aggregate_week(prev_start, prev_end),
        }

    def _build_stream_data(live_id: str) -> dict | None:
        info = next((s for s in streams if s.get("id") == live_id), None)
        if not info:
            return None
        start_ts = info.get("start_time") or 0
        end_ts = info.get("end_time") or 0
        start_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc) if start_ts > 0 else None
        end_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc) if end_ts > 0 else None
        enriched = _enrich_orders_for_range(session, start_utc, end_utc) if start_utc else {}
        dur_hours = ((end_ts or start_ts) - start_ts) / 3600 if start_ts > 0 else 0
        gmv = enriched.get("gmv", 0)
        oc = enriched.get("paid_orders", 0)
        return {
            "live_id": live_id,
            "title": info.get("title", ""),
            "date": datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%b %d, %Y") if start_ts > 0 else "",
            "gmv": gmv,
            "orders": oc,
            "items": enriched.get("total_items", 0),
            "customers": len(enriched.get("top_buyers", [])),
            "duration_hours": round(dur_hours, 1),
            "aov": round(gmv / oc, 2) if oc > 0 else 0,
            "revenue_per_hour": round(gmv / dur_hours, 2) if dur_hours > 0 else 0,
            "top_sellers": enriched.get("top_sellers", [])[:5],
            "top_buyers": enriched.get("top_buyers", [])[:5],
        }

    if not stream_a and not stream_b:
        if len(streams) >= 2:
            stream_a = streams[0].get("id")
            stream_b = streams[1].get("id")
        elif len(streams) == 1:
            stream_a = streams[0].get("id")

    return {
        "mode": "streams",
        "a": _build_stream_data(stream_a) if stream_a else None,
        "b": _build_stream_data(stream_b) if stream_b else None,
        "available_streams": [{"id": s.get("id"), "title": s.get("title", ""), "start_time": s.get("start_time")} for s in streams[:20]],
    }


# ---------------------------------------------------------------------------
# Client & Product Intelligence
# ---------------------------------------------------------------------------

_clients_cache: dict[str, Any] = {}
_clients_cache_lock = threading.Lock()
_CLIENTS_CACHE_TTL = 120

def _get_cached_or_compute(cache_key: str, compute_fn):
    now = time.monotonic()
    with _clients_cache_lock:
        entry = _clients_cache.get(cache_key)
        if entry and now - entry["at"] < _CLIENTS_CACHE_TTL:
            return entry["data"]
    data = compute_fn()
    with _clients_cache_lock:
        _clients_cache[cache_key] = {"data": data, "at": time.monotonic()}
    return data


@router.get("/tiktok/clients", response_class=HTMLResponse)
def tiktok_clients_page(request: Request):
    redirect = require_role_response(request, "viewer")
    if redirect:
        return redirect
    return templates.TemplateResponse("tiktok_clients.html", {
        "request": request,
        "title": "Client & Product Intelligence",
    })


@router.get("/tiktok/clients/api/buyers")
def tiktok_clients_buyers(
    request: Request,
    days: int = Query(default=180),
    session: Session = Depends(get_session),
):
    redirect = require_role_response(request, "viewer")
    if redirect:
        return redirect
    if days not in (0, 30, 90, 180):
        days = 180
    try:
        data = _get_cached_or_compute(
            f"buyers_{days}",
            lambda: build_buyer_profiles(session, days=days),
        )
        return {"ok": True, "buyers": data, "count": len(data)}
    except Exception:
        logger.exception("tiktok_clients_buyers failed")
        return {"ok": False, "error": "Failed to load buyer data", "buyers": []}


@router.get("/tiktok/clients/api/products")
def tiktok_clients_products(
    request: Request,
    days: int = Query(default=90),
    session: Session = Depends(get_session),
):
    redirect = require_role_response(request, "viewer")
    if redirect:
        return redirect
    if days not in (0, 30, 90, 180):
        days = 90
    try:
        data = _get_cached_or_compute(
            f"products_{days}",
            lambda: build_product_velocity(session, days=days),
        )
        return {"ok": True, "products": data, "count": len(data)}
    except Exception:
        logger.exception("tiktok_clients_products failed")
        return {"ok": False, "error": "Failed to load product data", "products": []}


@router.get("/tiktok/clients/api/buyers/{buyer_key}")
def tiktok_clients_buyer_detail(
    request: Request,
    buyer_key: str,
    session: Session = Depends(get_session),
):
    redirect = require_role_response(request, "viewer")
    if redirect:
        return redirect
    try:
        data = build_buyer_detail(session, buyer_key, days=0)
        if data is None:
            return {"ok": False, "error": "Buyer not found"}
        return {"ok": True, **data}
    except Exception:
        logger.exception("tiktok_clients_buyer_detail failed for %s", buyer_key)
        return {"ok": False, "error": "Failed to load buyer detail"}


@router.get("/tiktok/clients/api/products/{product_key:path}")
def tiktok_clients_product_detail(
    request: Request,
    product_key: str,
    session: Session = Depends(get_session),
):
    redirect = require_role_response(request, "viewer")
    if redirect:
        return redirect
    try:
        data = build_product_detail(session, product_key, days=0)
        if data is None:
            return {"ok": False, "error": "Product not found"}
        return {"ok": True, **data}
    except Exception:
        logger.exception("tiktok_clients_product_detail failed for %s", product_key)
        return {"ok": False, "error": "Failed to load product detail"}
