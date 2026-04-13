"""
TikTok Streamer Dashboard routes.

Extracted from app/main.py -- all routes under /tiktok/streamer/.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func
from sqlmodel import Session, select

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..shared import (  # noqa: F401 - explicit imports for underscore-prefixed names
    _BUILD_VERSION,
    _GMV_CACHE_TTL_SECONDS,
    _get_app_setting,
    _get_default_streamer_for_tiktok,
    _get_live_analytics_snapshot,
    _get_live_session_snapshot,
    _get_live_sessions_list,
    _gmv_cache,
    _gmv_cache_lock,
    _is_currently_live,
    _save_stream_range,
    _set_app_setting,
    _stream_range,
    _stream_range_source,
)
from ..db import get_session
from ..reporting import TIKTOK_PAID_STATUSES

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers (only used by streamer routes)
# ---------------------------------------------------------------------------

def _build_streamer_order_card(order: TikTokOrder) -> dict:
    """Build a card-ready dict from a TikTokOrder, extracting sku_image from raw line items."""
    raw_items: list[dict] = []
    try:
        raw_items = json.loads(order.line_items_json) if order.line_items_json else []
    except (json.JSONDecodeError, TypeError):
        pass
    if not isinstance(raw_items, list):
        raw_items = [raw_items] if isinstance(raw_items, dict) else []

    summary_items: list[dict] = []
    try:
        summary_items = json.loads(order.line_items_summary_json) if order.line_items_summary_json else []
    except (json.JSONDecodeError, TypeError):
        pass
    if not isinstance(summary_items, list):
        summary_items = []

    items: list[dict] = []
    for idx, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            continue
        title = str(
            raw.get("product_name") or raw.get("sku_name") or raw.get("title") or raw.get("item_name") or ""
        ).strip()
        if not title and idx < len(summary_items):
            title = str(summary_items[idx].get("title") or "").strip()
        if not title:
            title = "Unknown item"

        sku_name = str(raw.get("sku_name") or "").strip()
        variant = sku_name if sku_name and sku_name.lower() != "default" and sku_name.lower() != title.lower() else None

        qty_raw = raw.get("quantity") or raw.get("sku_quantity") or raw.get("count")
        try:
            quantity = int(qty_raw or 0)
        except (TypeError, ValueError):
            quantity = 0
        if quantity < 1 and idx < len(summary_items):
            try:
                quantity = int(summary_items[idx].get("quantity") or 1)
            except (TypeError, ValueError):
                quantity = 1
        if quantity < 1:
            quantity = 1

        sku_image = str(
            raw.get("sku_image") or raw.get("product_image") or raw.get("image_url") or ""
        ).strip() or None
        if not sku_image and idx < len(summary_items):
            sku_image = str(summary_items[idx].get("sku_image") or "").strip() or None

        unit_price: float = 0.0
        for price_key in ("sale_price", "sku_sale_price", "price", "unit_price"):
            val = raw.get(price_key)
            if val is not None:
                try:
                    unit_price = float(val)
                except (TypeError, ValueError):
                    pass
                else:
                    break

        items.append({
            "title": title,
            "variant": variant,
            "quantity": quantity,
            "sku_image": sku_image,
            "unit_price": unit_price,
        })

    if not items and summary_items:
        for si in summary_items:
            if not isinstance(si, dict):
                continue
            items.append({
                "title": str(si.get("title") or "Unknown item"),
                "variant": None,
                "quantity": int(si.get("quantity") or 1),
                "sku_image": str(si.get("sku_image") or "").strip() or None,
                "unit_price": float(si.get("unit_price") or 0),
            })

    created_at_val = order.created_at
    if created_at_val is not None and hasattr(created_at_val, "tzinfo") and created_at_val.tzinfo is None:
        created_at_val = created_at_val.replace(tzinfo=timezone.utc)
    updated_at_val = order.updated_at
    if updated_at_val is not None and hasattr(updated_at_val, "tzinfo") and updated_at_val.tzinfo is None:
        updated_at_val = updated_at_val.replace(tzinfo=timezone.utc)

    return {
        "tiktok_order_id": order.tiktok_order_id,
        "order_number": order.order_number or "",
        "customer_name": order.customer_name or "Guest",
        "created_at": created_at_val.isoformat() if created_at_val else "",
        "updated_at": updated_at_val.isoformat() if updated_at_val else "",
        "total_price": float(order.total_price or 0),
        "order_status": (order.order_status or "").strip().lower(),
        "financial_status": (order.financial_status or "").strip().lower(),
        "items": items,
    }

_buyer_lifetime_cache: dict = {}
_buyer_lifetime_cache_lock = threading.Lock()
_BUYER_LIFETIME_CACHE_TTL = 120  # seconds

def _compute_buyer_lifetime_totals(session: Session) -> dict[str, float]:
    """Return a dict mapping lowercased buyer name -> lifetime GMV across all paid orders.

    Uses SQL GROUP BY and caches for 120s to avoid full-table scans on every poll.
    """
    now = time.monotonic()
    with _buyer_lifetime_cache_lock:
        if now - _buyer_lifetime_cache.get("at", 0) < _BUYER_LIFETIME_CACHE_TTL and "data" in _buyer_lifetime_cache:
            return _buyer_lifetime_cache["data"]

    status_col = func.coalesce(
        func.lower(func.trim(TikTokOrder.financial_status)),
        func.lower(func.trim(TikTokOrder.order_status)),
        "",
    )
    buyer_name_expr = func.lower(func.trim(func.coalesce(TikTokOrder.customer_name, "guest")))
    rows = session.exec(
        select(
            buyer_name_expr,
            func.sum(func.coalesce(TikTokOrder.subtotal_price, TikTokOrder.total_price, 0.0)),
        )
        .where(status_col.in_(list(TIKTOK_PAID_STATUSES)))
        .group_by(buyer_name_expr)
    ).all()
    agg = {(name or "guest"): float(total or 0) for name, total in rows}

    with _buyer_lifetime_cache_lock:
        _buyer_lifetime_cache["data"] = agg
        _buyer_lifetime_cache["at"] = time.monotonic()
    return agg

def _enrich_cards_with_buyer_totals(cards: list[dict], buyer_totals: dict[str, float]) -> None:
    """Attach buyer_lifetime_spent to each card in-place."""
    for card in cards:
        buyer_key = (card.get("customer_name") or "").strip().lower() or "guest"
        card["buyer_lifetime_spent"] = round(buyer_totals.get(buyer_key, 0.0), 2)

def _streamer_session_gmv(session: Session) -> dict:
    """Cached wrapper -- returns GMV data, recomputing at most once per _GMV_CACHE_TTL_SECONDS."""
    now = time.monotonic()
    with _gmv_cache_lock:
        cached_at = _gmv_cache.get("at", 0)
        if now - cached_at < _GMV_CACHE_TTL_SECONDS and "data" in _gmv_cache:
            return _gmv_cache["data"]
    result = _streamer_session_gmv_uncached(session)
    with _gmv_cache_lock:
        _gmv_cache["data"] = result
        _gmv_cache["at"] = time.monotonic()
    return result

def _streamer_session_gmv_uncached(session: Session) -> dict:
    """Calculate today's GMV and top sellers for the streamer dashboard (Pacific time)."""
    now_pacific = datetime.now(PACIFIC_TZ)
    today_start_pacific = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_start_pacific.astimezone(timezone.utc)

    today_orders = session.exec(
        select(TikTokOrder).where(TikTokOrder.created_at >= today_start_utc)
    ).all()

    gmv = 0.0
    paid_count = 0
    product_agg: dict[str, dict] = {}
    customer_agg: dict[str, dict] = {}

    for o in today_orders:
        status = (o.financial_status or o.order_status or "").lower().strip()
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
            customer_agg[buyer_key] = {"name": buyer_name, "spent": order_gmv, "orders": 1, "order_list": []}

        raw_items: list[dict] = []
        try:
            raw_items = json.loads(o.line_items_json) if o.line_items_json else []
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(raw_items, list):
            raw_items = [raw_items] if isinstance(raw_items, dict) else []

        order_items_merged: dict[str, dict] = {}
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title = str(
                raw.get("product_name") or raw.get("sku_name") or raw.get("title") or raw.get("item_name") or ""
            ).strip()
            if not title:
                title = "Unknown item"

            sku_name = str(raw.get("sku_name") or "").strip()
            variant = sku_name if sku_name and sku_name.lower() != "default" and sku_name.lower() != title.lower() else None
            key = (title.lower() + "||" + (variant or "").lower()).strip()

            qty_raw = raw.get("quantity") or raw.get("sku_quantity") or raw.get("count")
            try:
                qty = max(int(qty_raw or 0), 1)
            except (TypeError, ValueError):
                qty = 1

            unit_price = 0.0
            for pk in ("sale_price", "sku_sale_price", "price", "unit_price"):
                val = raw.get(pk)
                if val is not None:
                    try:
                        unit_price = float(val)
                    except (TypeError, ValueError):
                        continue
                    break

            sku_image = str(
                raw.get("sku_image") or raw.get("product_image") or raw.get("image_url") or ""
            ).strip() or None

            if key in order_items_merged:
                order_items_merged[key]["qty"] += qty
            else:
                order_items_merged[key] = {"title": title, "qty": qty, "sku_image": sku_image}

            if key in product_agg:
                product_agg[key]["qty"] += qty
                product_agg[key]["revenue"] += round(qty * unit_price, 2)
                if not product_agg[key]["sku_image"] and sku_image:
                    product_agg[key]["sku_image"] = sku_image
            else:
                product_agg[key] = {
                    "title": title,
                    "variant": variant,
                    "sku_image": sku_image,
                    "qty": qty,
                    "revenue": round(qty * unit_price, 2),
                    "buyers": {},
                }
            if buyer_name not in product_agg[key].get("buyers", {}):
                product_agg[key].setdefault("buyers", {})[buyer_name] = 0
            product_agg[key]["buyers"][buyer_name] += qty

        if len(customer_agg[buyer_key]["order_list"]) < 20:
            customer_agg[buyer_key]["order_list"].append({
                "order_number": o.order_number,
                "total": round(order_gmv, 2),
                "items": list(order_items_merged.values()),
                "created_at": o.created_at.isoformat() if o.created_at else None,
            })

    top_sellers = sorted(product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:8]
    for ts in top_sellers:
        ts["revenue"] = round(ts["revenue"], 2)
        bd = ts.pop("buyers", {})
        ts["buyers"] = sorted(
            [{"name": n, "qty": q} for n, q in bd.items()],
            key=lambda x: x["qty"], reverse=True,
        )

    top_buyers = sorted(customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
    for tb in top_buyers:
        tb["spent"] = round(tb["spent"], 2)

    result: dict[str, Any] = {
        "session_gmv": round(gmv, 2),
        "session_orders": paid_count,
        "session_total_orders": len(today_orders),
        "top_sellers": top_sellers,
        "top_buyers": top_buyers,
    }

    sr_start = _stream_range.get("start")
    sr_end = _stream_range.get("end")
    if sr_start is not None:
        stream_gmv = 0.0
        stream_paid = 0
        stream_items = 0
        stream_product_agg: dict[str, dict] = {}
        stream_customer_agg: dict[str, dict] = {}
        q = select(TikTokOrder).where(TikTokOrder.created_at >= sr_start)
        if sr_end is not None:
            q = q.where(TikTokOrder.created_at <= sr_end)
        stream_orders_rows = session.exec(q).all()
        for o in stream_orders_rows:
            status = (o.financial_status or o.order_status or "").lower().strip()
            if status not in TIKTOK_PAID_STATUSES:
                continue
            o_gmv = float(o.subtotal_price if o.subtotal_price is not None else (o.total_price or 0))
            stream_gmv += o_gmv
            stream_paid += 1

            buyer_name = (o.customer_name or "").strip() or "Guest"
            buyer_key = buyer_name.lower()
            if buyer_key in stream_customer_agg:
                stream_customer_agg[buyer_key]["spent"] += o_gmv
                stream_customer_agg[buyer_key]["orders"] += 1
            else:
                stream_customer_agg[buyer_key] = {"name": buyer_name, "spent": o_gmv, "orders": 1, "order_list": []}

            try:
                s_items = json.loads(o.line_items_json) if o.line_items_json else []
            except (json.JSONDecodeError, TypeError):
                s_items = []
            if not isinstance(s_items, list):
                s_items = [s_items] if isinstance(s_items, dict) else []
            s_order_merged: dict[str, dict] = {}
            for it in s_items:
                if not isinstance(it, dict):
                    continue
                s_title = str(
                    it.get("product_name") or it.get("sku_name") or it.get("title") or it.get("item_name") or ""
                ).strip() or "Unknown item"
                s_sku = str(it.get("sku_name") or "").strip()
                s_variant = s_sku if s_sku and s_sku.lower() != "default" and s_sku.lower() != s_title.lower() else None
                s_key = (s_title.lower() + "||" + (s_variant or "").lower()).strip()
                try:
                    s_qty = max(int(it.get("quantity") or it.get("sku_quantity") or 1), 1)
                except (TypeError, ValueError):
                    s_qty = 1
                stream_items += s_qty
                s_unit = 0.0
                for pk in ("sale_price", "sku_sale_price", "price", "unit_price"):
                    val = it.get(pk)
                    if val is not None:
                        try:
                            s_unit = float(val)
                        except (TypeError, ValueError):
                            continue
                        break
                s_img = str(it.get("sku_image") or it.get("product_image") or it.get("image_url") or "").strip() or None
                if s_key in s_order_merged:
                    s_order_merged[s_key]["qty"] += s_qty
                else:
                    s_order_merged[s_key] = {"title": s_title, "qty": s_qty, "sku_image": s_img}
                if s_key in stream_product_agg:
                    stream_product_agg[s_key]["qty"] += s_qty
                    stream_product_agg[s_key]["revenue"] += round(s_qty * s_unit, 2)
                    if not stream_product_agg[s_key]["sku_image"] and s_img:
                        stream_product_agg[s_key]["sku_image"] = s_img
                else:
                    stream_product_agg[s_key] = {
                        "title": s_title, "variant": s_variant, "sku_image": s_img,
                        "qty": s_qty, "revenue": round(s_qty * s_unit, 2),
                        "buyers": {},
                    }
                if buyer_name not in stream_product_agg[s_key].get("buyers", {}):
                    stream_product_agg[s_key].setdefault("buyers", {})[buyer_name] = 0
                stream_product_agg[s_key]["buyers"][buyer_name] += s_qty

            if len(stream_customer_agg[buyer_key]["order_list"]) < 20:
                stream_customer_agg[buyer_key]["order_list"].append({
                    "order_number": o.order_number,
                    "total": round(o_gmv, 2),
                    "items": list(s_order_merged.values()),
                    "created_at": o.created_at.isoformat() if o.created_at else None,
                })

        stream_top_sellers = sorted(stream_product_agg.values(), key=lambda p: p["revenue"], reverse=True)[:8]
        for sts in stream_top_sellers:
            sts["revenue"] = round(sts["revenue"], 2)
            sbd = sts.pop("buyers", {})
            sts["buyers"] = sorted(
                [{"name": n, "qty": q} for n, q in sbd.items()],
                key=lambda x: x["qty"], reverse=True,
            )
        stream_top_buyers = sorted(stream_customer_agg.values(), key=lambda b: b["spent"], reverse=True)[:10]
        for stb in stream_top_buyers:
            stb["spent"] = round(stb["spent"], 2)

        result["stream_gmv"] = round(stream_gmv, 2)
        result["stream_orders"] = stream_paid
        result["stream_items"] = stream_items
        result["stream_top_sellers"] = stream_top_sellers
        result["stream_top_buyers"] = stream_top_buyers
        result["stream_start_utc"] = sr_start.isoformat()
        if sr_end:
            result["stream_end_utc"] = sr_end.isoformat()

    return result

def _compute_order_velocity(session: Session) -> list[dict]:
    """Compute per-minute order counts for the current stream window."""
    start = _stream_range.get("start")
    if not start:
        return []
    end_dt = _stream_range.get("end") or datetime.now(timezone.utc)
    orders = session.exec(
        select(TikTokOrder.created_at)
        .where(TikTokOrder.created_at >= start, TikTokOrder.created_at <= end_dt)
        .order_by(TikTokOrder.created_at)
    ).all()
    if not orders:
        return []
    buckets: dict[int, int] = {}
    for ts in orders:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        minute_key = int(ts.timestamp()) // 60
        buckets[minute_key] = buckets.get(minute_key, 0) + 1
    start_min = int(start.timestamp()) // 60
    end_min = int(end_dt.timestamp()) // 60
    result = []
    for m in range(max(start_min, end_min - 59), end_min + 1):
        result.append({"minute": m, "count": buckets.get(m, 0)})
    return result

def _compute_stream_duration_minutes(gmv_data: dict) -> float | None:
    start_utc = gmv_data.get("stream_start_utc")
    end_utc = gmv_data.get("stream_end_utc")
    if not start_utc:
        return None
    try:
        st = datetime.fromisoformat(start_utc) if isinstance(start_utc, str) else start_utc
        if st.tzinfo is None:
            st = st.replace(tzinfo=timezone.utc)
        if end_utc:
            et = datetime.fromisoformat(end_utc) if isinstance(end_utc, str) else end_utc
            if et.tzinfo is None:
                et = et.replace(tzinfo=timezone.utc)
        else:
            et = datetime.now(timezone.utc)
        return round((et - st).total_seconds() / 60, 1)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/tiktok/streamer", response_class=HTMLResponse)
def tiktok_streamer_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    orders = session.exec(
        select(TikTokOrder).order_by(TikTokOrder.created_at.desc()).limit(50)
    ).all()

    cards = [_build_streamer_order_card(o) for o in orders]
    buyer_totals = _compute_buyer_lifetime_totals(session)
    _enrich_cards_with_buyer_totals(cards, buyer_totals)

    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()

    total_count = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())
    gmv_data = _streamer_session_gmv(session)

    chat_info = get_chat_status()
    live_analytics = _get_live_analytics_snapshot()

    live_session = _get_live_session_snapshot()
    stream_data = {
        "stream_gmv": gmv_data.get("stream_gmv"),
        "stream_orders": gmv_data.get("stream_orders"),
        "stream_items": gmv_data.get("stream_items"),
        "stream_start_utc": gmv_data.get("stream_start_utc"),
        "tiktok_gmv": live_session.get("gmv") if live_session.get("ok") else None,
        "tiktok_items_sold": live_session.get("items_sold") if live_session.get("ok") else None,
        "tiktok_customers": live_session.get("customers") if live_session.get("ok") else None,
        "live_title": live_session.get("title") if live_session.get("ok") else None,
        "stream_range_source": _stream_range_source,
        "is_live": _is_currently_live(),
    }

    return templates.TemplateResponse(request, "tiktok_streamer.html", {
        "request": request,
        "title": "TikTok Live Orders",
        "orders_json": json.dumps(cards),
        "latest_updated_at": latest_updated_at_text,
        "total_count": total_count,
        "session_gmv": gmv_data["session_gmv"],
        "session_orders": gmv_data["session_orders"],
        "session_total_orders": gmv_data["session_total_orders"],
        "top_sellers_json": json.dumps(gmv_data.get("top_sellers", [])),
        "top_buyers_json": json.dumps(gmv_data.get("top_buyers", [])),
        "stream_top_sellers_json": json.dumps(gmv_data.get("stream_top_sellers", [])),
        "stream_top_buyers_json": json.dumps(gmv_data.get("stream_top_buyers", [])),
        "live_analytics_json": json.dumps(live_analytics),
        "stream_data_json": json.dumps(stream_data),
        "stream_sessions_json": json.dumps(_get_live_sessions_list()),
        "build_version": _BUILD_VERSION,
        "chat_status": chat_info["status"],
        "current_user": getattr(request.state, "current_user", None),
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "current_streamer": _get_default_streamer_for_tiktok(session) or "",
        "gmv_goal": float(_get_app_setting(session, "stream_gmv_goal", "0") or "0"),
        "high_value_threshold": float(_get_app_setting(session, "high_value_threshold", "100") or "100"),
        "vip_buyer_threshold": float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000"),
    })

@router.get("/tiktok/streamer/poll")
def tiktok_streamer_poll(
    request: Request,
    since: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    created_at_floor = datetime.now(timezone.utc) - timedelta(hours=24)

    query = (
        select(TikTokOrder)
        .where(TikTokOrder.created_at >= created_at_floor)
        .order_by(TikTokOrder.updated_at.desc())
        .limit(20)
    )
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=timezone.utc)
            query = (
                select(TikTokOrder)
                .where(
                    TikTokOrder.updated_at > since_dt,
                    TikTokOrder.created_at >= created_at_floor,
                )
                .order_by(TikTokOrder.updated_at.desc())
                .limit(20)
            )
        except (ValueError, TypeError):
            pass

    orders = session.exec(query).all()
    cards = [_build_streamer_order_card(o) for o in orders]
    if cards:
        buyer_totals = _compute_buyer_lifetime_totals(session)
        _enrich_cards_with_buyer_totals(cards, buyer_totals)

    latest_updated_at = session.exec(select(func.max(TikTokOrder.updated_at))).one()
    latest_updated_at_text = None
    if latest_updated_at is not None:
        if latest_updated_at.tzinfo is None:
            latest_updated_at = latest_updated_at.replace(tzinfo=timezone.utc)
        latest_updated_at_text = latest_updated_at.isoformat()

    total_count = int(session.exec(select(func.count()).select_from(TikTokOrder)).one())

    gmv_data = _streamer_session_gmv(session)

    return {
        "orders": cards,
        "latest_updated_at": latest_updated_at_text,
        "total_count": total_count,
        "session_gmv": gmv_data["session_gmv"],
        "session_orders": gmv_data["session_orders"],
        "session_total_orders": gmv_data["session_total_orders"],
        "top_sellers": gmv_data.get("top_sellers", []),
        "top_buyers": gmv_data.get("top_buyers", []),
        "stream_top_sellers": gmv_data.get("stream_top_sellers", []),
        "stream_top_buyers": gmv_data.get("stream_top_buyers", []),
        "live_analytics": _get_live_analytics_snapshot(),
        "stream_gmv": gmv_data.get("stream_gmv"),
        "stream_orders": gmv_data.get("stream_orders"),
        "stream_items": gmv_data.get("stream_items"),
        "stream_start_utc": gmv_data.get("stream_start_utc"),
        "tiktok_gmv": (lambda snap: snap.get("gmv") if snap.get("ok") else None)(_get_live_session_snapshot()),
        "stream_range_source": _stream_range_source,
        "stream_sessions": _get_live_sessions_list(),
        "is_live": _is_currently_live(),
        "build_version": _BUILD_VERSION,
        "gmv_goal": float(_get_app_setting(session, "stream_gmv_goal", "0") or "0"),
        "high_value_threshold": float(_get_app_setting(session, "high_value_threshold", "100") or "100"),
        "vip_buyer_threshold": float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000"),
        "order_velocity": _compute_order_velocity(session),
        "stream_end_utc": gmv_data.get("stream_end_utc"),
        "stream_duration_minutes": _compute_stream_duration_minutes(gmv_data),
        "current_streamer": get_current_streamer(session) or "",
    }

@router.get("/tiktok/streamer/goal")
def get_streamer_goal(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    return {
        "gmv_goal": float(_get_app_setting(session, "stream_gmv_goal", "0") or "0"),
        "high_value_threshold": float(_get_app_setting(session, "high_value_threshold", "100") or "100"),
        "vip_buyer_threshold": float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000"),
    }

@router.post("/tiktok/streamer/goal")
def set_streamer_goal(request: Request, session: Session = Depends(get_session), body: dict = None):
    if denial := require_role_response(request, "reviewer"):
        return denial
    if body is None:
        body = {}
    if "goal" in body:
        _set_app_setting(session, "stream_gmv_goal", str(float(body["goal"])))
    if "high_value_threshold" in body:
        _set_app_setting(session, "high_value_threshold", str(float(body["high_value_threshold"])))
    if "vip_buyer_threshold" in body:
        _set_app_setting(session, "vip_buyer_threshold", str(float(body["vip_buyer_threshold"])))
    if "vip_presence_timeout_min" in body:
        _set_app_setting(session, "vip_presence_timeout_min", str(max(1.0, float(body["vip_presence_timeout_min"]))))
    return {"ok": True}

@router.get("/tiktok/streamer/config", response_class=HTMLResponse)
def tiktok_streamer_config(request: Request, session: Session = Depends(get_session)):
    """Backend page to configure the stream date range shown on the streamer dashboard."""
    if denial := require_role_response(request, "admin"):
        return denial
    current_goal = float(_get_app_setting(session, "stream_gmv_goal", "0") or "0")
    current_threshold = float(_get_app_setting(session, "high_value_threshold", "100") or "100")
    s = _stream_range["start"]
    e = _stream_range["end"]
    start_val = s.astimezone(PACIFIC_TZ).strftime("%Y-%m-%dT%H:%M") if s else ""
    end_val = e.astimezone(PACIFIC_TZ).strftime("%Y-%m-%dT%H:%M") if e else ""
    source = _stream_range_source

    live_snap = _get_live_session_snapshot()
    has_auto = bool(live_snap.get("ok") and live_snap.get("start_time"))
    auto_title = str(live_snap.get("title") or "") if has_auto else ""
    auto_gmv = float(live_snap.get("gmv") or 0) if has_auto else 0.0
    auto_start_ts = int(live_snap.get("start_time") or 0) if has_auto else 0
    auto_end_ts = int(live_snap.get("end_time") or 0) if has_auto else 0
    if auto_start_ts > 0:
        auto_start_str = datetime.fromtimestamp(auto_start_ts, tz=PACIFIC_TZ).strftime("%b %d, %Y %I:%M %p")
    else:
        auto_start_str = ""
    if auto_end_ts > 0:
        auto_end_str = datetime.fromtimestamp(auto_end_ts, tz=PACIFIC_TZ).strftime("%b %d, %Y %I:%M %p")
    else:
        auto_end_str = ""

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stream Config</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/flatpickr.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/flatpickr/dist/themes/dark.css">
<style>
  body {{ font-family: system-ui, sans-serif; background: #0f0f0f; color: #e5e5e5;
         display: flex; justify-content: center; padding: 40px 16px; margin: 0; }}
  .card {{ background: #1a1a1a; border-radius: 16px; padding: 32px; max-width: 480px; width: 100%; }}
  h1 {{ font-size: 20px; margin: 0 0 8px; }}
  .subtitle {{ font-size: 13px; color: #888; margin-bottom: 24px; }}
  label {{ display: block; font-size: 11px; font-weight: 700; text-transform: uppercase;
           letter-spacing: .1em; color: #888; margin-bottom: 6px; }}
  .fp-input {{ width: 100%; padding: 12px 14px; border-radius: 10px;
    border: 1px solid #333; background: #111; color: #e5e5e5; font-size: 15px;
    margin-bottom: 18px; box-sizing: border-box; cursor: pointer; }}
  .fp-input:focus {{ outline: none; border-color: #22c55e; }}
  .row {{ display: flex; gap: 12px; margin-top: 8px; }}
  button {{ flex: 1; padding: 13px; border: none; border-radius: 10px; font-size: 14px;
            font-weight: 700; cursor: pointer; transition: all .15s; }}
  button:hover {{ opacity: .85; }}
  .btn-save {{ background: #22c55e; color: #000; }}
  .btn-clear {{ background: #333; color: #ccc; }}
  .btn-auto {{ background: #6366f1; color: #fff; }}
  .status {{ margin-top: 16px; font-size: 13px; color: #22c55e; display: none; text-align: center;
             padding: 8px; border-radius: 8px; background: rgba(34,197,94,.1); }}
  a {{ color: #22c55e; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .flatpickr-calendar {{ font-family: system-ui, sans-serif !important; }}
  .source-badge {{ display: inline-block; font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .08em; padding: 3px 8px; border-radius: 6px; }}
  .source-auto {{ background: rgba(99,102,241,.15); color: #818cf8; }}
  .source-manual {{ background: rgba(34,197,94,.15); color: #22c55e; }}
  .auto-card {{ background: #111; border: 1px solid #333; border-radius: 12px; padding: 16px;
    margin-bottom: 20px; }}
  .auto-card h3 {{ font-size: 13px; margin: 0 0 8px; color: #818cf8; }}
  .auto-row {{ font-size: 12px; color: #aaa; margin: 3px 0; }}
  .auto-row strong {{ color: #e5e5e5; }}
  hr {{ border: none; border-top: 1px solid #333; margin: 20px 0; }}
</style>
</head><body>
<div class="card">
  <h1>Stream Config <span class="source-badge {'source-auto' if source == 'auto' else 'source-manual'}">{source}</span></h1>
  <p class="subtitle">Set the date range for the current stream. GMV will be calculated from orders in this window.</p>
  {'<div class="auto-card" id="auto-card"><h3>Latest Detected Stream</h3>' +
   '<div class="auto-row"><strong>' + auto_title + '</strong></div>' +
   '<div class="auto-row">Start: <strong>' + auto_start_str + '</strong></div>' +
   '<div class="auto-row">End: <strong>' + (auto_end_str or 'ongoing') + '</strong></div>' +
   '<div class="auto-row">TikTok GMV: <strong style="color:#22c55e;">$' + f'{auto_gmv:,.2f}' + '</strong></div>' +
   '<div class="row" style="margin-top:12px;"><button class="btn-auto" onclick="useAuto()">Use This Stream</button></div></div>'
   if has_auto else '<div class="auto-card"><h3>Auto-Detection</h3><div class="auto-row">No live sessions detected yet. Data appears after a stream ends.</div></div>'}
  <hr>
  <label>Stream Start (Pacific)</label>
  <input class="fp-input" id="start" placeholder="Click to pick date & time" readonly>
  <label>Stream End (Pacific) — leave blank for ongoing</label>
  <input class="fp-input" id="end" placeholder="Click to pick date & time (optional)" readonly>
  <div class="row">
    <button class="btn-save" onclick="save()">Save (Manual)</button>
    <button class="btn-clear" onclick="clearRange()">Clear</button>
  </div>
  <div class="status" id="status"></div>
  <hr>
  <label>Stream GMV Goal ($)</label>
  <input class="fp-input" id="gmv-goal" type="number" min="0" step="100" value="{current_goal:.0f}" placeholder="e.g. 5000" style="cursor:text;">
  <label>High-Value Order Threshold ($)</label>
  <input class="fp-input" id="hv-threshold" type="number" min="0" step="10" value="{current_threshold:.0f}" placeholder="e.g. 100" style="cursor:text;">
  <label>VIP Buyer Lifetime Spend Threshold ($)</label>
  <input class="fp-input" id="vip-threshold" type="number" min="0" step="500" value="{float(_get_app_setting(session, 'vip_buyer_threshold', '5000') or '5000'):.0f}" placeholder="e.g. 5000" style="cursor:text;">
  <label>VIP Chat Presence Timeout (minutes)</label>
  <input class="fp-input" id="vip-presence-timeout" type="number" min="1" step="5" value="{float(_get_app_setting(session, 'vip_presence_timeout_min', '30') or '30'):.0f}" placeholder="e.g. 30" style="cursor:text;">
  <p style="font-size:11px;color:#666;margin:-12px 0 18px;">How long after a VIP's last activity before they drop off the "In Chat" panel. For long streams, try 60-120 min. They reappear instantly if they interact again.</p>
  <div class="row">
    <button style="background:#6366f1;color:#fff;" onclick="saveGoalSettings()">Save Goal Settings</button>
  </div>
  <div class="status" id="goal-status"></div>
  <p style="margin-top:24px;font-size:12px;text-align:center;">
    <a href="/tiktok/streamer">&larr; Back to Streamer Dashboard</a>
  </p>
</div>
<script src="https://cdn.jsdelivr.net/npm/flatpickr"></script>
<script>
var fpOpts = {{
  enableTime: true,
  dateFormat: 'Y-m-d H:i',
  altInput: true,
  altFormat: 'M j, Y  h:i K',
  time_24hr: false,
  theme: 'dark',
  disableMobile: true
}};
var fpStart = flatpickr('#start', Object.assign({{}}, fpOpts, {{
  defaultDate: '{start_val}'.replace('T', ' ') || null
}}));
var fpEnd = flatpickr('#end', Object.assign({{}}, fpOpts, {{
  defaultDate: '{end_val}'.replace('T', ' ') || null
}}));
function flash(msg, ok) {{
  var el = document.getElementById('status');
  el.textContent = msg;
  el.style.color = ok ? '#22c55e' : '#ef4444';
  el.style.background = ok ? 'rgba(34,197,94,.1)' : 'rgba(239,68,68,.1)';
  el.style.display = 'block';
  setTimeout(function() {{ el.style.display = 'none'; }}, 3000);
}}
function save() {{
  var s = document.getElementById('start').value;
  var e = document.getElementById('end').value;
  fetch('/tiktok/streamer/config', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ start: s, end: e }}) }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{ flash(d.ok ? 'Saved (manual)!' : (d.error || 'Error'), d.ok); }});
}}
function clearRange() {{
  fpStart.clear();
  fpEnd.clear();
  fetch('/tiktok/streamer/config', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ start: '', end: '' }}) }})
    .then(function(r) {{ return r.json(); }})
    .then(function() {{ flash('Cleared!', true); }});
}}
function useAuto() {{
  fetch('/tiktok/streamer/config/auto', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}} }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      if (d.ok) {{ flash('Switched to auto mode!', true); setTimeout(function(){{ location.reload(); }}, 1000); }}
      else {{ flash('Error switching to auto', false); }}
    }});
}}
function saveGoalSettings() {{
  var goal = parseFloat(document.getElementById('gmv-goal').value) || 0;
  var threshold = parseFloat(document.getElementById('hv-threshold').value) || 100;
  var vipThreshold = parseFloat(document.getElementById('vip-threshold').value) || 5000;
  var presenceTimeout = parseFloat(document.getElementById('vip-presence-timeout').value) || 30;
  fetch('/tiktok/streamer/goal', {{ method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{ goal: goal, high_value_threshold: threshold, vip_buyer_threshold: vipThreshold, vip_presence_timeout_min: presenceTimeout }}) }})
    .then(function(r) {{ return r.json(); }})
    .then(function(d) {{
      var el = document.getElementById('goal-status');
      el.textContent = 'Saved!';
      el.style.color = '#22c55e';
      el.style.background = 'rgba(34,197,94,.1)';
      el.style.display = 'block';
      setTimeout(function() {{ el.style.display = 'none'; }}, 3000);
    }});
}}
</script>
</body></html>"""
    return HTMLResponse(content=html)

@router.post("/tiktok/streamer/config")
def tiktok_streamer_config_save(request: Request, body: dict = None):
    if denial := require_role_response(request, "admin"):
        return denial
    if body is None:
        body = {}
    start_str = (body.get("start") or "").strip()
    end_str = (body.get("end") or "").strip()

    if start_str:
        try:
            naive = datetime.fromisoformat(start_str)
            _stream_range["start"] = naive.replace(tzinfo=PACIFIC_TZ).astimezone(timezone.utc)
        except (ValueError, TypeError):
            return {"ok": False, "error": "Invalid start date"}
    else:
        _stream_range["start"] = None

    if end_str:
        try:
            naive = datetime.fromisoformat(end_str)
            _stream_range["end"] = naive.replace(tzinfo=PACIFIC_TZ).astimezone(timezone.utc)
        except (ValueError, TypeError):
            return {"ok": False, "error": "Invalid end date"}
    else:
        _stream_range["end"] = None

    _save_stream_range(source="manual")
    return {"ok": True, "start": str(_stream_range["start"] or ""), "end": str(_stream_range["end"] or "")}

@router.post("/tiktok/streamer/config/auto")
def tiktok_streamer_config_auto(request: Request):
    """Switch stream range to auto mode, applying the latest detected session immediately."""
    if denial := require_role_response(request, "admin"):
        return denial
    snap = _get_live_session_snapshot()
    start_ts = snap.get("start_time") or 0
    end_ts = snap.get("end_time") or 0
    if isinstance(start_ts, int) and start_ts > 0:
        _stream_range["start"] = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        _stream_range["end"] = datetime.fromtimestamp(end_ts, tz=timezone.utc) if isinstance(end_ts, int) and end_ts > 0 else None
    _save_stream_range(source="auto")
    return {
        "ok": True,
        "source": "auto",
        "start": str(_stream_range["start"] or ""),
        "end": str(_stream_range["end"] or ""),
        "session": snap if snap.get("ok") else None,
    }

@router.get("/tiktok/streamer/chat/poll")
def tiktok_streamer_chat_poll(
    request: Request,
    since: int = Query(default=0),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    messages = get_live_chat_messages(since_idx=since)
    status_info = get_chat_status()
    latest_idx = messages[-1]["idx"] if messages else since

    vip_threshold = float(_get_app_setting(session, "vip_buyer_threshold", "5000") or "5000")
    presence_timeout_min = float(_get_app_setting(session, "vip_presence_timeout_min", "30") or "30")
    vip_in_chat: list[dict] = []
    if vip_threshold > 0:
        viewers = get_stream_viewers(presence_timeout=presence_timeout_min * 60)
        buyer_totals = _compute_buyer_lifetime_totals(session)
        for v in viewers:
            key = v["username"].strip().lower()
            spent = buyer_totals.get(key, 0.0)
            if spent >= vip_threshold:
                vip_in_chat.append({
                    "username": v["username"],
                    "lifetime_spent": round(spent, 2),
                    "active": v["active"],
                })
        vip_in_chat.sort(key=lambda x: x["lifetime_spent"], reverse=True)

    return {
        "messages": messages,
        "latest_idx": latest_idx,
        "status": status_info["status"],
        "viewer_count": status_info["viewer_count"],
        "vip_in_chat": vip_in_chat,
    }


# ---------------------------------------------------------------------------
# Giveaway endpoints
# ---------------------------------------------------------------------------

@router.post("/tiktok/streamer/giveaway/start")
async def tiktok_giveaway_start(request: Request, body: dict = None):
    if denial := require_role_response(request, "admin"):
        return denial

    from ..tiktok_giveaway import get_giveaway_state, run_giveaway, _giveaway_task
    import app.tiktok_giveaway as _gmod

    state = get_giveaway_state()
    if state.status == "running":
        return {"ok": False, "error": "A giveaway is already running."}

    body = body or {}
    product_name = (body.get("product_name") or "").strip()
    if not product_name:
        return {"ok": False, "error": "product_name is required."}

    winners = int(body.get("winners", 1))
    keyword = (body.get("keyword") or "giveaway").strip()
    duration = (body.get("duration") or "5 min").strip()

    loop = asyncio.get_event_loop()
    task = loop.create_task(run_giveaway(product_name, winners, keyword, duration))
    _gmod._giveaway_task = task

    return {"ok": True, "message": "Giveaway started."}


@router.get("/tiktok/streamer/giveaway/status")
def tiktok_giveaway_status(request: Request):
    if denial := require_role_response(request, "viewer"):
        return denial

    from ..tiktok_giveaway import get_giveaway_state
    return get_giveaway_state().to_dict()


@router.post("/tiktok/streamer/giveaway/cancel")
def tiktok_giveaway_cancel(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial

    from ..tiktok_giveaway import cancel_giveaway
    cancelled = cancel_giveaway()
    return {"ok": cancelled, "message": "Cancelled." if cancelled else "Nothing to cancel."}


@router.get("/tiktok/streamer/giveaway/health")
async def tiktok_giveaway_health(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial

    from ..tiktok_giveaway import check_session_health
    return await check_session_health()
