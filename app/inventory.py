"""
Inventory management routes.

All routes require at minimum 'viewer' role. Mutations (add, edit, reprice,
push-to-shopify) require 'reviewer' or above.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlmodel import Session, select, func

# Reuse the shared Jinja2Templates instance so custom filters registered in
# app/shared.py (e.g. `money`, `pacific_datetime`) are available in
# inventory templates as well. A separate instance would have an empty
# filter env and any {{ x | money }} in the template would 500.
from .shared import templates as _templates

from .auth import has_role
from .card_scanner import identify_card_from_image, lookup_card_image_and_price
from .cert_lookup import lookup_cert
from .pokemon_scanner import run_pipeline as run_pokemon_pipeline, get_scan_history, fetch_tcg_categories, get_validation_result, text_search_cards
from .degen_eye_v2 import get_v2_scan_history, run_v2_pipeline, run_v2_pipeline_stream
from .degen_eye_v2_training import (
    attach_confirmed_label,
    attach_prediction,
    capture_stats as training_capture_stats,
    create_scan_capture,
    train_confirmed_captures,
)
from .phash_scanner import (
    get_index_stats as phash_index_stats,
    has_index as phash_has_index,
    reload_index as phash_reload_index,
)
from .price_cache import get_warm_stats as price_cache_stats, warm_price_cache
from .config import get_settings
from .db import get_session
from .inventory_barcode import (
    generate_barcode_value,
    label_context_for_items,
    render_barcode_svg,
)
from .inventory_pricing import (
    effective_price,
    fetch_price_for_item,
    price_result_to_json,
)
from .inventory_shopify import push_item_to_shopify, update_shopify_variant_price
from .models import (
    GAMES,
    CONDITIONS,
    GRADING_COMPANIES,
    INVENTORY_IN_STOCK,
    INVENTORY_LISTED,
    INVENTORY_SOLD,
    INVENTORY_HELD,
    ITEM_TYPE_SINGLE,
    ITEM_TYPE_SLAB,
    ALL_INVENTORY_STATUSES,
    InventoryItem,
    PriceHistory,
    utcnow,
)

router = APIRouter()
settings = get_settings()
logger = logging.getLogger(__name__)

# Rate-limit bucket for /degen_eye/client_log: {username: [timestamps]}
_CLIENT_LOG_RATE: dict[str, list[float]] = {}

PAGE_SIZE = 50



def _get_user(request: Request):
    """Look up the current user directly from the session.

    Starlette 1.0's BaseHTTPMiddleware doesn't reliably share
    request.state between middleware and route handlers, so we
    read the session ourselves instead of relying on the
    attach_current_user middleware.
    """
    from .shared import get_request_user
    return get_request_user(request)


def _check_role(request: Request, min_role: str) -> Optional[Response]:
    """Return a redirect/403 if the current user doesn't have min_role; None if ok."""
    user = _get_user(request)
    if not user:
        next_path = request.url.path
        return RedirectResponse(url=f"/login?next={next_path}", status_code=303)
    if not has_role(user, min_role):
        return HTMLResponse("You do not have permission to view this page.", status_code=403)
    return None


def _require_viewer(request: Request) -> Optional[Response]:
    return _check_role(request, "viewer")


def _require_employee(request: Request) -> Optional[Response]:
    # Used for scanner / Degen Eye routes that are safe for rank-and-file
    # employees to use on the buy counter. These routes show public market
    # prices and buy-offer calculators but NOT internal cost basis / margins.
    return _check_role(request, "employee")


def _require_reviewer(request: Request) -> Optional[Response]:
    return _check_role(request, "reviewer")


def _current_user(request: Request):
    return _get_user(request)


def _capture_user_payload(request: Request) -> dict[str, Any]:
    user = _current_user(request)
    if not user:
        return {}
    return {
        "id": getattr(user, "id", None),
        "username": getattr(user, "username", None),
        "display_name": getattr(user, "display_name", None),
        "role": getattr(user, "role", None),
    }


def _capture_request_meta(request: Request) -> dict[str, Any]:
    return {
        "user_agent": (request.headers.get("user-agent") or "")[:300],
    }


def _tag_v2_capture_result(payload: dict[str, Any], capture_id: Optional[str]) -> None:
    if not capture_id or not isinstance(payload, dict):
        return
    payload["capture_id"] = capture_id
    debug = payload.setdefault("debug", {})
    if isinstance(debug, dict):
        debug["v2_capture_id"] = capture_id


def _truthy_form_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _render_v2_training_page(summary: Optional[dict[str, Any]] = None) -> str:
    stats = training_capture_stats()
    phash_stats = phash_index_stats()
    summary_html = ""
    if summary is not None:
        summary_html = (
            "<section>"
            "<h2>Last Run</h2>"
            f"<pre>{html.escape(json.dumps(summary, indent=2, default=str))}</pre>"
            "</section>"
        )
    stats_json = html.escape(json.dumps(stats, indent=2, default=str))
    phash_json = html.escape(json.dumps({
        "card_count": phash_stats.get("card_count"),
        "metadata": phash_stats.get("metadata"),
        "index_path": phash_stats.get("index_path"),
    }, indent=2, default=str))
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Degen Eye v2 Training</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 24px; color: #0f172a; background: #f8fafc; }}
    main {{ max-width: 920px; margin: 0 auto; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin-top: 24px; font-size: 18px; }}
    p {{ color: #475569; line-height: 1.45; }}
    form, section {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; margin-top: 16px; }}
    label {{ display: block; font-weight: 700; margin: 12px 0 6px; }}
    input[type="number"] {{ width: 120px; padding: 8px; border: 1px solid #cbd5e1; border-radius: 6px; }}
    .check {{ display: flex; gap: 8px; align-items: center; margin-top: 12px; }}
    .check label {{ margin: 0; font-weight: 600; }}
    button {{ margin-top: 16px; border: 0; border-radius: 6px; padding: 10px 14px; font-weight: 800; cursor: pointer; background: #111827; color: #fff; }}
    button.secondary {{ background: #475569; }}
    pre {{ white-space: pre-wrap; background: #0f172a; color: #e2e8f0; padding: 14px; border-radius: 8px; overflow: auto; }}
    a {{ color: #2563eb; font-weight: 700; text-decoration: none; }}
  </style>
</head>
<body>
<main>
  <p><a href="/degen_eye/v2">Back to scanner</a></p>
  <h1>Degen Eye v2 Training</h1>
  <p>Confirmed batch-review labels can be promoted into the local pHash index as real employee photo examples. Unconfirmed scans are saved for review and evaluation, but not trusted as labels.</p>
  <form method="post" action="/degen_eye/v2/train-captures">
    <label for="limit">Max confirmed captures to process</label>
    <input id="limit" name="limit" type="number" min="1" max="2000" value="200">
    <div class="check">
      <input id="dry_run" name="dry_run" type="checkbox" value="true">
      <label for="dry_run">Dry run only</label>
    </div>
    <div class="check">
      <input id="include_indexed" name="include_indexed" type="checkbox" value="true">
      <label for="include_indexed">Reprocess already-indexed captures</label>
    </div>
    <div class="check">
      <input id="reload_current_worker" name="reload_current_worker" type="checkbox" value="true" checked>
      <label for="reload_current_worker">Reload pHash index in this worker after training</label>
    </div>
    <button type="submit">Train From Confirmed Captures</button>
  </form>
  <form method="post" action="/degen_eye/v2/reload-index">
    <button class="secondary" type="submit">Reload pHash Index Only</button>
  </form>
  {summary_html}
  <section><h2>Capture Stats</h2><pre>{stats_json}</pre></section>
  <section><h2>Index Stats</h2><pre>{phash_json}</pre></section>
</main>
</body>
</html>"""


# ---------------------------------------------------------------------------
# List view
# ---------------------------------------------------------------------------

@router.get("/inventory", response_class=HTMLResponse)
async def inventory_list(
    request: Request,
    session: Session = Depends(get_session),
    status: str = Query(default=""),
    game: str = Query(default=""),
    item_type: str = Query(default=""),
    q: str = Query(default=""),
    page: int = Query(default=1, ge=1),
):
    if denial := _require_viewer(request):
        return denial

    query = select(InventoryItem)
    if status and status in ALL_INVENTORY_STATUSES:
        query = query.where(InventoryItem.status == status)
    if game:
        query = query.where(InventoryItem.game == game)
    if item_type and item_type in (ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB):
        query = query.where(InventoryItem.item_type == item_type)
    if q:
        like = f"%{q}%"
        query = query.where(
            InventoryItem.card_name.ilike(like)
            | InventoryItem.barcode.ilike(like)
            | InventoryItem.set_name.ilike(like)
            | InventoryItem.cert_number.ilike(like)
        )

    total = session.exec(
        select(func.count()).select_from(query.subquery())
    ).one()
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(page, total_pages)
    offset = (page - 1) * PAGE_SIZE

    items = session.exec(
        query.order_by(InventoryItem.created_at.desc()).offset(offset).limit(PAGE_SIZE)
    ).all()

    return _templates.TemplateResponse(
        request,
        "inventory.html",
        {
            "current_user": _current_user(request),
            "items": items,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "status_filter": status,
            "game_filter": game,
            "type_filter": item_type,
            "q": q,
            "games": GAMES,
            "statuses": sorted(ALL_INVENTORY_STATUSES),
            "effective_price": effective_price,
        },
    )


# ---------------------------------------------------------------------------
# Barcode scan lookup (JSON)
# ---------------------------------------------------------------------------

@router.get("/inventory/api/lookup", response_class=JSONResponse)
async def inventory_lookup(
    request: Request,
    barcode: str = Query(default=""),
    session: Session = Depends(get_session),
):
    # Scanner barcode probe. Safe for employees — returns only whether a
    # barcode exists + its internal id, not cost basis or price. The returned
    # redirect URL points at /inventory/{id} which is still viewer-gated, so
    # employees scanning a known barcode won't accidentally see cost data.
    if denial := _require_employee(request):
        return denial
    if not barcode:
        return JSONResponse({"found": False})
    item = session.exec(
        select(InventoryItem).where(InventoryItem.barcode == barcode.strip())
    ).first()
    if not item:
        return JSONResponse({"found": False, "barcode": barcode})
    return JSONResponse({"found": True, "item_id": item.id, "redirect": f"/inventory/{item.id}"})


# ---------------------------------------------------------------------------
# Scan mode page
# ---------------------------------------------------------------------------

@router.get("/inventory/scan", response_class=HTMLResponse)
async def inventory_scan_page(request: Request):
    if denial := _require_employee(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan.html",
        {"current_user": _current_user(request)},
    )


# ---------------------------------------------------------------------------
# Print labels
# ---------------------------------------------------------------------------

@router.get("/inventory/labels", response_class=HTMLResponse)
async def inventory_labels(
    request: Request,
    session: Session = Depends(get_session),
    ids: str = Query(default=""),
    status: str = Query(default=""),
):
    if denial := _require_viewer(request):
        return denial

    items: list[InventoryItem] = []
    if ids:
        id_list = [int(x) for x in ids.split(",") if x.strip().isdigit()]
        if id_list:
            items = session.exec(
                select(InventoryItem).where(InventoryItem.id.in_(id_list))
            ).all()
    elif status and status in ALL_INVENTORY_STATUSES:
        items = session.exec(
            select(InventoryItem).where(InventoryItem.status == status)
        ).all()

    labels = label_context_for_items(items)
    return _templates.TemplateResponse(
        request,
        "inventory_labels.html",
        {"current_user": _current_user(request), "labels": labels},
    )


# ---------------------------------------------------------------------------
# Add new item
# ---------------------------------------------------------------------------

@router.get("/inventory/new", response_class=HTMLResponse)
async def inventory_new_form(request: Request):
    if denial := _require_reviewer(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_new.html",
        {
            "current_user": _current_user(request),
            "games": GAMES,
            "conditions": CONDITIONS,
            "grading_companies": GRADING_COMPANIES,
            "item_types": [ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB],
            "error": None,
        },
    )


@router.post("/inventory/new")
async def inventory_new_submit(
    request: Request,
    session: Session = Depends(get_session),
    item_type: str = Form(...),
    game: str = Form(...),
    card_name: str = Form(...),
    set_name: str = Form(default=""),
    set_code: str = Form(default=""),
    card_number: str = Form(default=""),
    language: str = Form(default="English"),
    condition: str = Form(default=""),
    quantity: int = Form(default=1),
    grading_company: str = Form(default=""),
    grade: str = Form(default=""),
    cert_number: str = Form(default=""),
    cost_basis: str = Form(default=""),
    list_price: str = Form(default=""),
    notes: str = Form(default=""),
    auto_price_on_save: str = Form(default=""),
    push_shopify_on_save: str = Form(default=""),
):
    if denial := _require_reviewer(request):
        return denial

    card_name = card_name.strip()
    if not card_name:
        return _templates.TemplateResponse(
            request,
            "inventory_new.html",
            {
                "current_user": _current_user(request),
                "games": GAMES,
                "conditions": CONDITIONS,
                "grading_companies": GRADING_COMPANIES,
                "item_types": [ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB],
                "error": "Card name is required.",
            },
            status_code=400,
        )

    item = InventoryItem(
        barcode="PENDING",  # replaced after insert gives us an id
        item_type=item_type,
        game=game,
        card_name=card_name,
        set_name=set_name.strip() or None,
        set_code=set_code.strip() or None,
        card_number=card_number.strip() or None,
        language=language or "English",
        condition=condition.strip() or None,
        quantity=max(1, quantity),
        grading_company=grading_company.strip() or None,
        grade=grade.strip() or None,
        cert_number=cert_number.strip() or None,
        cost_basis=_parse_float(cost_basis),
        list_price=_parse_float(list_price),
        notes=notes.strip() or None,
        status=INVENTORY_IN_STOCK,
        created_at=utcnow(),
    )
    session.add(item)
    session.commit()
    session.refresh(item)

    # Assign barcode now that we have the id
    item.barcode = generate_barcode_value(item.id)
    session.add(item)
    session.commit()
    session.refresh(item)

    # Auto-price
    if auto_price_on_save == "on" and settings.inventory_auto_price_enabled:
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                result = await fetch_price_for_item(
                    item,
                    client,
                    api_key=settings.scrydex_api_key,
                    base_url=settings.scrydex_base_url,
                )
            if result:
                item.auto_price = result.get("market_price")
                item.last_priced_at = utcnow()
                session.add(item)
                history = PriceHistory(
                    item_id=item.id,
                    source=result.get("source", "unknown"),
                    market_price=result.get("market_price"),
                    low_price=result.get("low_price"),
                    high_price=result.get("high_price"),
                    raw_response_json=price_result_to_json(result),
                )
                session.add(history)
                session.commit()
                session.refresh(item)
        except Exception as exc:
            logger.warning("[inventory] auto-price failed on new item %s: %s", item.id, exc)

    # Push to Shopify
    if push_shopify_on_save == "on" or settings.inventory_auto_shopify_push:
        if settings.shopify_store_domain and settings.shopify_access_token:
            try:
                ids_resp = await push_item_to_shopify(
                    item,
                    store_domain=settings.shopify_store_domain,
                    access_token=settings.shopify_access_token,
                )
                if ids_resp:
                    item.shopify_product_id = ids_resp["shopify_product_id"]
                    item.shopify_variant_id = ids_resp["shopify_variant_id"]
                    item.status = INVENTORY_LISTED
                    item.updated_at = utcnow()
                    session.add(item)
                    session.commit()
            except Exception as exc:
                logger.warning("[inventory] shopify push failed on new item %s: %s", item.id, exc)

    return RedirectResponse(f"/inventory/{item.id}", status_code=303)


# ---------------------------------------------------------------------------
# Item detail + edit
# ---------------------------------------------------------------------------

@router.get("/inventory/{item_id}", response_class=HTMLResponse)
async def inventory_item_detail(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_viewer(request):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    history = session.exec(
        select(PriceHistory)
        .where(PriceHistory.item_id == item_id)
        .order_by(PriceHistory.fetched_at.desc())
        .limit(20)
    ).all()

    barcode_svg = render_barcode_svg(item.barcode)

    return _templates.TemplateResponse(
        request,
        "inventory_item.html",
        {
            "current_user": _current_user(request),
            "item": item,
            "price_history": history,
            "barcode_svg": barcode_svg,
            "effective_price": effective_price(item),
            "games": GAMES,
            "conditions": CONDITIONS,
            "grading_companies": GRADING_COMPANIES,
            "item_types": [ITEM_TYPE_SINGLE, ITEM_TYPE_SLAB],
            "statuses": sorted(ALL_INVENTORY_STATUSES),
        },
    )


@router.post("/inventory/{item_id}/edit")
async def inventory_item_edit(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
    card_name: str = Form(...),
    set_name: str = Form(default=""),
    set_code: str = Form(default=""),
    card_number: str = Form(default=""),
    game: str = Form(default=""),
    item_type: str = Form(default=""),
    language: str = Form(default="English"),
    condition: str = Form(default=""),
    quantity: int = Form(default=1),
    grading_company: str = Form(default=""),
    grade: str = Form(default=""),
    cert_number: str = Form(default=""),
    cost_basis: str = Form(default=""),
    list_price: str = Form(default=""),
    notes: str = Form(default=""),
    status: str = Form(default=""),
    image_url: str = Form(default=""),
):
    if denial := _require_reviewer(request):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    item.card_name = card_name.strip() or item.card_name
    item.set_name = set_name.strip() or None
    item.set_code = set_code.strip() or None
    item.card_number = card_number.strip() or None
    item.game = game or item.game
    item.item_type = item_type or item.item_type
    item.language = language or "English"
    item.condition = condition.strip() or None
    item.quantity = max(1, quantity)
    item.grading_company = grading_company.strip() or None
    item.grade = grade.strip() or None
    item.cert_number = cert_number.strip() or None
    item.cost_basis = _parse_float(cost_basis)
    item.list_price = _parse_float(list_price)
    item.notes = notes.strip() or None
    if status and status in ALL_INVENTORY_STATUSES:
        item.status = status
    item.image_url = image_url.strip() or item.image_url
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


# ---------------------------------------------------------------------------
# On-demand reprice
# ---------------------------------------------------------------------------

@router.post("/inventory/{item_id}/reprice")
async def inventory_reprice(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_reviewer(request):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            result = await fetch_price_for_item(
                item,
                client,
                api_key=settings.scrydex_api_key,
                base_url=settings.scrydex_base_url,
            )
        if result:
            item.auto_price = result.get("market_price")
            item.last_priced_at = utcnow()
            item.updated_at = utcnow()
            session.add(item)
            history = PriceHistory(
                item_id=item.id,
                source=result.get("source", "unknown"),
                market_price=result.get("market_price"),
                low_price=result.get("low_price"),
                high_price=result.get("high_price"),
                raw_response_json=price_result_to_json(result),
            )
            session.add(history)
            session.commit()
            logger.info("[inventory] repriced item %s: $%.2f", item_id, result.get("market_price") or 0)
        else:
            logger.info("[inventory] reprice returned no result for item %s", item_id)
    except Exception as exc:
        logger.error("[inventory] reprice error for item %s: %s", item_id, exc)

    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


# ---------------------------------------------------------------------------
# Push to Shopify
# ---------------------------------------------------------------------------

@router.post("/inventory/{item_id}/push-shopify")
async def inventory_push_shopify(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_reviewer(request):
        return denial

    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Item not found.", status_code=404)

    if not settings.shopify_store_domain or not settings.shopify_access_token:
        return HTMLResponse(
            "SHOPIFY_STORE_DOMAIN and SHOPIFY_ACCESS_TOKEN must be configured.", status_code=400
        )

    # If already linked, update price instead of creating a duplicate
    if item.shopify_variant_id:
        ok = await update_shopify_variant_price(
            item,
            store_domain=settings.shopify_store_domain,
            access_token=settings.shopify_access_token,
        )
        if ok:
            item.updated_at = utcnow()
            session.add(item)
            session.commit()
    else:
        ids_resp = await push_item_to_shopify(
            item,
            store_domain=settings.shopify_store_domain,
            access_token=settings.shopify_access_token,
        )
        if ids_resp:
            item.shopify_product_id = ids_resp["shopify_product_id"]
            item.shopify_variant_id = ids_resp["shopify_variant_id"]
            item.status = INVENTORY_LISTED
            item.updated_at = utcnow()
            session.add(item)
            session.commit()

    return RedirectResponse(f"/inventory/{item_id}", status_code=303)


# ---------------------------------------------------------------------------
# Barcode SVG endpoint
# ---------------------------------------------------------------------------

@router.get("/inventory/{item_id}/barcode.svg")
async def inventory_barcode_svg(
    request: Request,
    item_id: int,
    session: Session = Depends(get_session),
):
    if denial := _require_viewer(request):
        return denial
    item = session.get(InventoryItem, item_id)
    if not item:
        return HTMLResponse("Not found.", status_code=404)
    svg = render_barcode_svg(item.barcode)
    return Response(content=svg, media_type="image/svg+xml")


# ---------------------------------------------------------------------------
# Camera scan pages
# ---------------------------------------------------------------------------

@router.get("/inventory/scan/singles", response_class=HTMLResponse)
async def inventory_scan_singles_page(request: Request):
    if denial := _require_employee(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_singles.html",
        {"current_user": _current_user(request)},
    )


@router.get("/inventory/scan/slabs", response_class=HTMLResponse)
async def inventory_scan_slabs_page(request: Request):
    if denial := _require_employee(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_slabs.html",
        {
            "current_user": _current_user(request),
            "grading_companies": GRADING_COMPANIES,
        },
    )


@router.get("/inventory/scan/batch-review", response_class=HTMLResponse)
async def inventory_batch_review_page(request: Request):
    if denial := _require_employee(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_batch_review.html",
        {"current_user": _current_user(request), "conditions": CONDITIONS},
    )


# ---------------------------------------------------------------------------
# Pokemon card scanner (multi-stage pipeline)
# ---------------------------------------------------------------------------

@router.get("/degen_eye", response_class=HTMLResponse)
async def inventory_scan_pokemon_page(request: Request):
    if denial := _require_employee(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_pokemon.html",
        {"current_user": _current_user(request), "conditions": CONDITIONS},
    )


@router.get("/degen_eye/categories")
async def inventory_scan_categories(request: Request):
    """Return TCGTracking categories with preferred ordering."""
    if denial := _require_employee(request):
        return denial
    categories = await fetch_tcg_categories()
    return JSONResponse({"categories": categories})


@router.post("/degen_eye/identify")
async def inventory_scan_pokemon_identify(request: Request):
    """
    Run the full card scanning pipeline on a base64 image.

    Request body: {"image": "<base64 string>", "category_id": "3"}
    Response: ScanResult JSON with best_match, candidates, extracted_fields, debug.
    """
    if denial := _require_employee(request):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    base64_image = (body.get("image") or "").strip()
    if not base64_image:
        return JSONResponse({"error": "Missing image field"}, status_code=400)

    if "," in base64_image:
        base64_image = base64_image.split(",", 1)[1]

    category_id = (body.get("category_id") or "3").strip()
    mode = (body.get("mode") or "balanced").strip().lower()

    result = await run_pokemon_pipeline(base64_image, category_id=category_id, mode=mode)

    status_code = 200
    if result.get("status") == "ERROR":
        status_code = 422

    return JSONResponse(result, status_code=status_code)


@router.post("/degen_eye/client_log")
async def inventory_scan_pokemon_client_log(request: Request):
    """Append a client-side error report for post-mortem analysis.

    Hardened against abuse:
    - 8 KB payload cap (rejects with 413)
    - per-user rate limit: 30 entries / 5 min
    - log file rotation at 5 MB → .log.1 (1 generation kept)
    - disk errors return 500 (no silent failures)
    """
    if denial := _require_employee(request):
        return denial

    # Cap payload before parsing JSON.
    raw = await request.body()
    if len(raw) > 8 * 1024:
        return JSONResponse({"error": "payload too large"}, status_code=413)

    try:
        body = json.loads(raw) if raw else {}
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    if not isinstance(body, dict):
        body = {"raw": body}

    user = _get_user(request)
    username = user.username if user else "anonymous"

    # Simple in-memory rate limit per user. Module-level dict keyed by
    # username → deque of recent timestamps; trim to last 5 min and reject if
    # more than 30 entries in the window.
    now_ts = datetime.now(timezone.utc).timestamp()
    window_start = now_ts - 300.0  # 5 minutes
    bucket = _CLIENT_LOG_RATE.setdefault(username, [])
    # Drop old entries
    bucket[:] = [t for t in bucket if t >= window_start]
    if len(bucket) >= 30:
        return JSONResponse(
            {"error": "rate limit — try again later"}, status_code=429
        )
    bucket.append(now_ts)

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "user": username,
        "ua": (request.headers.get("user-agent", "") or "")[:512],
        "ip": request.client.host if request.client else "",
        **{k: v for k, v in body.items() if k not in ("ts", "user", "ua", "ip")},
    }

    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_path = log_dir / "degen_eye_client.log"
    rotated_path = log_dir / "degen_eye_client.log.1"
    try:
        log_dir.mkdir(exist_ok=True)
        # Rotate if current file > 5 MB.
        if log_path.exists() and log_path.stat().st_size > 5 * 1024 * 1024:
            if rotated_path.exists():
                rotated_path.unlink()
            log_path.rename(rotated_path)
        with open(log_path, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception as exc:
        logger.exception("degen_eye client_log write failed: %s", exc)
        return JSONResponse(
            {"error": "log write failed"}, status_code=500
        )

    return JSONResponse({"ok": True})


@router.post("/degen_eye/text-search")
async def inventory_scan_pokemon_text_search(request: Request):
    """Search for cards by text query (name, set, number)."""
    if denial := _require_employee(request):
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    query = (body.get("query") or "").strip()
    if not query:
        return JSONResponse({"error": "Missing query field"}, status_code=400)
    category_id = (body.get("category_id") or "3").strip()
    result = await text_search_cards(query, category_id=category_id)
    return JSONResponse(result)


@router.get("/degen_eye/history")
async def inventory_scan_pokemon_history(request: Request):
    """Return recent scan results as JSON for debugging."""
    if denial := _require_employee(request):
        return denial
    return JSONResponse(get_scan_history())


@router.get("/degen_eye/validate/{scan_id}")
async def inventory_scan_pokemon_validate(request: Request, scan_id: str):
    """Poll for background OCR validation result."""
    if denial := _require_employee(request):
        return denial
    result = get_validation_result(scan_id)
    if result is None:
        return JSONResponse({"error": "Unknown scan_id"}, status_code=404)
    return JSONResponse(result)


@router.get("/degen_eye/debug", response_class=HTMLResponse)
async def inventory_scan_pokemon_debug_page(request: Request):
    """Live debug page showing recent scan history — open on desktop while scanning on phone."""
    if denial := _require_employee(request):
        return denial
    return HTMLResponse("""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pokemon Scanner Debug Log</title>
<style>
  body{font-family:monospace;background:#1a1a2e;color:#e0e0e0;margin:0;padding:20px;}
  h1{color:#f0c674;font-size:18px;margin:0 0 4px;}
  .sub{color:#888;font-size:12px;margin-bottom:16px;}
  .entry{background:#16213e;border:1px solid #333;border-radius:8px;padding:14px;margin-bottom:12px;}
  .entry.MATCHED{border-left:4px solid #4caf50;}
  .entry.AMBIGUOUS{border-left:4px solid #ff9800;}
  .entry.NO_MATCH{border-left:4px solid #f44336;}
  .entry.ERROR{border-left:4px solid #f44336;}
  .ts{color:#888;font-size:11px;}
  .status{font-weight:bold;font-size:13px;margin-bottom:6px;}
  .status.MATCHED{color:#4caf50;} .status.AMBIGUOUS{color:#ff9800;}
  .status.NO_MATCH{color:#f44336;} .status.ERROR{color:#f44336;}
  .field{margin:3px 0;font-size:12px;line-height:1.5;}
  .label{color:#f0c674;} .val{color:#e0e0e0;}
  .ocr{background:#0d1117;padding:8px;border-radius:4px;white-space:pre-wrap;font-size:11px;
       max-height:150px;overflow-y:auto;margin-top:6px;color:#aaa;border:1px solid #222;}
  .empty{color:#666;text-align:center;padding:40px;font-size:14px;}
  #auto{margin-bottom:12px;display:flex;align-items:center;gap:8px;font-size:12px;color:#888;}
</style>
</head><body>
<h1>Pokemon Scanner Debug Log</h1>
<div class="sub">Auto-refreshes every 3s. Open this on your desktop while scanning on your phone.</div>
<div id="auto"><input type="checkbox" id="autoRefresh" checked> Auto-refresh
  <button onclick="loadHistory()" style="margin-left:8px;padding:4px 10px;border-radius:4px;
    border:1px solid #444;background:#16213e;color:#e0e0e0;cursor:pointer;">Refresh Now</button></div>
<div id="log"></div>
<script>
function loadHistory(){
  fetch('/degen_eye/history').then(r=>r.json()).then(data=>{
    var el=document.getElementById('log');
    if(!data.length){el.innerHTML='<div class="empty">No scans yet. Start scanning on your phone.</div>';return;}
    el.innerHTML=data.map(function(e){
      var s=e.status||'ERROR';
      var h='<div class="entry '+s+'">';
      h+='<div class="status '+s+'">'+s+'</div>';
      h+='<div class="ts">'+e.timestamp+'  |  '+(e.processing_time_ms||0).toFixed(0)+'ms</div>';
      if(e.error) h+='<div class="field"><span class="label">Error: </span>'+esc(e.error)+'</div>';
      h+='<div class="field"><span class="label">Extracted: </span>name='+esc(e.extracted_name)+', number='+esc(e.extracted_number)+', set='+esc(e.extracted_set)+'</div>';
      if(e.best_match_name) h+='<div class="field"><span class="label">Best Match: </span>'+esc(e.best_match_name)+' #'+esc(e.best_match_number)+' | '+esc(e.best_match_set)+' | score='+e.best_match_score+' ('+e.best_match_confidence+')' + (e.best_match_price?' | $'+Number(e.best_match_price).toFixed(2):'')+'</div>';
      h+='<div class="field"><span class="label">Candidates: </span>'+e.candidates_count+'</div>';
      if(e.extraction_method) h+='<div class="field"><span class="label">Extraction: </span>'+e.extraction_method+'</div>';
      if(e.disambiguation) h+='<div class="field"><span class="label">Disambiguation: </span>'+e.disambiguation+'</div>';
      if(e.ocr_text) h+='<div class="field"><span class="label">OCR Text:</span><div class="ocr">'+esc(e.ocr_text)+'</div></div>';
      h+='</div>';
      return h;
    }).join('');
  }).catch(function(){});
}
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
loadHistory();
setInterval(function(){if(document.getElementById('autoRefresh').checked)loadHistory();},3000);
</script>
</body></html>""")


# ---------------------------------------------------------------------------
# Degen Eye v2 — pHash-first scanner (Pokemon MVP, targets sub-1-second)
# ---------------------------------------------------------------------------
# v2 runs entirely locally for identification: OpenCV card detection,
# perceptual-hash nearest-neighbor lookup against a pre-built index of
# every Pokemon card, and a pre-warmed TCGTracking price cache. v1 is
# untouched — both scanners coexist under /degen_eye.

@router.get("/degen_eye/v2", response_class=HTMLResponse)
async def degen_eye_v2_page(request: Request):
    if denial := _require_employee(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_pokemon_v2.html",
        {"current_user": _current_user(request), "conditions": CONDITIONS},
    )


@router.post("/degen_eye/v2/scan")
async def degen_eye_v2_scan(request: Request):
    """Non-streaming v2 scan — accepts a base64 image, returns a full ScanResult.

    Request body: {"image": "<base64>", "category_id": "3"}
    Response shape mirrors v1's /degen_eye/identify so the existing batch
    UI helpers work without changes.
    """
    if denial := _require_employee(request):
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing image field"}, status_code=400)
    if len(image_b64) > _V2_MAX_SCAN_IMAGE_B64_CHARS:
        return JSONResponse({"error": "Image is too large"}, status_code=413)
    category_id = (body.get("category_id") or "3").strip()

    capture_id = await asyncio.to_thread(
        create_scan_capture,
        image_b64,
        source="v2_scan",
        category_id=category_id,
        employee=_capture_user_payload(request),
        request_meta=_capture_request_meta(request),
    )
    result = await run_v2_pipeline(image_b64, category_id=category_id)
    _tag_v2_capture_result(result, capture_id)
    if capture_id:
        await asyncio.to_thread(attach_prediction, capture_id, result)
    status_code = 422 if result.get("status") == "ERROR" else 200
    return JSONResponse(result, status_code=status_code)


@router.post("/degen_eye/v2/scan-init")
async def degen_eye_v2_scan_init(request: Request):
    """Prepare a streaming scan and return a ``scan_id`` to connect via SSE.

    We stash the uploaded image in a short-lived file keyed by scan_id; the
    SSE endpoint atomically claims it and runs the pipeline. This keeps the
    URL query-string small and works when scan-init and scan-stream land on
    different web workers.
    """
    if denial := _require_employee(request):
        return denial
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing image field"}, status_code=400)
    if len(image_b64) > _V2_MAX_SCAN_IMAGE_B64_CHARS:
        return JSONResponse({"error": "Image is too large"}, status_code=413)
    category_id = (body.get("category_id") or "3").strip()

    scan_id = uuid.uuid4().hex
    capture_id = await asyncio.to_thread(
        create_scan_capture,
        image_b64,
        source="v2_stream",
        category_id=category_id,
        employee=_capture_user_payload(request),
        scan_id=scan_id,
        request_meta=_capture_request_meta(request),
    )
    try:
        _write_v2_pending_scan(scan_id, image_b64, category_id, capture_id=capture_id)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except OSError as exc:
        logger.warning("[degen_eye_v2] failed to stage pending scan: %s", exc)
        return JSONResponse({"error": "Unable to stage scan"}, status_code=500)
    return JSONResponse({"scan_id": scan_id, "capture_id": capture_id})


# Small file-backed buffer so scan-init can hand a scan off to scan-stream
# without blowing the SSE URL past browser query-length limits. This must not
# be process-local: with multiple uvicorn/gunicorn workers, the POST and SSE GET
# can land in different processes on the same host.
_V2_PENDING_DIR = Path(__file__).resolve().parent.parent / "data" / "v2_pending_scans"
_V2_PENDING_TTL = 120.0
_V2_PENDING_MAX = 200
_V2_MAX_SCAN_IMAGE_B64_CHARS = 12 * 1024 * 1024
_V2_MAX_DETECT_IMAGE_B64_CHARS = 2 * 1024 * 1024
_V2_HEX_CHARS = set("0123456789abcdef")


def _is_v2_scan_id(scan_id: str) -> bool:
    scan_id = (scan_id or "").strip().lower()
    return len(scan_id) == 32 and all(c in _V2_HEX_CHARS for c in scan_id)


def _v2_pending_path(scan_id: str) -> Path:
    return _V2_PENDING_DIR / f"{scan_id}.json"


def _iter_v2_pending_files() -> list[Path]:
    if not _V2_PENDING_DIR.exists():
        return []
    return [
        p for p in _V2_PENDING_DIR.iterdir()
        if p.is_file() and (p.name.endswith(".json") or p.name.endswith(".json.claimed"))
    ]


def _evict_stale_v2_pending() -> None:
    now = time.time()
    _V2_PENDING_DIR.mkdir(parents=True, exist_ok=True)
    files = _iter_v2_pending_files()
    for path in files:
        try:
            if now - path.stat().st_mtime > _V2_PENDING_TTL:
                path.unlink(missing_ok=True)
        except OSError:
            logger.debug("[degen_eye_v2] unable to inspect/remove pending file %s", path, exc_info=True)

    active = [p for p in _iter_v2_pending_files() if p.name.endswith(".json")]
    overflow = len(active) - _V2_PENDING_MAX
    if overflow > 0:
        oldest = sorted(active, key=lambda p: p.stat().st_mtime)[:overflow]
        for path in oldest:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.debug("[degen_eye_v2] unable to evict pending file %s", path, exc_info=True)


def _write_v2_pending_scan(
    scan_id: str,
    image_b64: str,
    category_id: str,
    *,
    capture_id: Optional[str] = None,
) -> None:
    if not _is_v2_scan_id(scan_id):
        raise ValueError("Invalid scan id")
    if len(image_b64) > _V2_MAX_SCAN_IMAGE_B64_CHARS:
        raise ValueError("Image is too large")
    _evict_stale_v2_pending()
    payload = {
        "created_at": time.time(),
        "image": image_b64,
        "category_id": category_id,
        "capture_id": capture_id,
    }
    path = _v2_pending_path(scan_id)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(path)
    except OSError:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _claim_v2_pending_scan(scan_id: str) -> Optional[tuple[str, str, Optional[str]]]:
    if not _is_v2_scan_id(scan_id):
        return None
    _evict_stale_v2_pending()
    path = _v2_pending_path(scan_id)
    claimed = path.with_name(f"{path.name}.claimed")
    try:
        path.replace(claimed)
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.warning("[degen_eye_v2] failed to claim scan_id=%s: %s", scan_id, exc)
        return None

    try:
        payload = json.loads(claimed.read_text(encoding="utf-8"))
        created_at = float(payload.get("created_at") or 0)
        if time.time() - created_at > _V2_PENDING_TTL:
            return None
        image_b64 = str(payload.get("image") or "")
        category_id = str(payload.get("category_id") or "3")
        capture_id = str(payload.get("capture_id") or "").strip() or None
        if not image_b64:
            return None
        return (image_b64, category_id, capture_id)
    except Exception as exc:
        logger.warning("[degen_eye_v2] failed to read pending scan %s: %s", scan_id, exc)
        return None
    finally:
        try:
            claimed.unlink(missing_ok=True)
        except OSError:
            logger.debug("[degen_eye_v2] unable to remove claimed pending scan %s", claimed, exc_info=True)


def _count_v2_pending_scans() -> int:
    _evict_stale_v2_pending()
    return len([p for p in _iter_v2_pending_files() if p.name.endswith(".json")])


@router.get("/degen_eye/v2/scan-stream")
async def degen_eye_v2_scan_stream(request: Request, scan_id: str):
    """Server-Sent Events stream of a v2 scan's progressive results.

    Events emitted in order: detected, identified, price, variants, done.
    ``event: error`` is emitted on unrecoverable failures before done.
    """
    if denial := _require_employee(request):
        return denial

    pending = _claim_v2_pending_scan(scan_id)
    if pending is None:
        return JSONResponse(
            {"error": "Unknown or expired scan_id; POST to /degen_eye/v2/scan-init first"},
            status_code=404,
        )
    image_b64, category_id, capture_id = pending

    async def _event_source():
        import json as _json
        try:
            yield ": connected\n\n"
            async for event_name, payload in run_v2_pipeline_stream(image_b64, category_id):
                if event_name in {"done", "error"}:
                    _tag_v2_capture_result(payload, capture_id)
                    if capture_id:
                        await asyncio.to_thread(attach_prediction, capture_id, payload)
                safe_payload = _json.dumps(payload, default=str)
                yield f"event: {event_name}\ndata: {safe_payload}\n\n"
                # Disconnection check — bail early if the client closed the tab.
                if await request.is_disconnected():
                    logger.info("[degen_eye_v2] SSE client disconnected mid-scan (scan_id=%s)", scan_id)
                    return
        except Exception as exc:
            logger.exception("[degen_eye_v2] SSE pipeline crashed: %s", exc)
            error_payload = {"status": "ERROR", "error": str(exc), "debug": {"mode": "v2"}}
            _tag_v2_capture_result(error_payload, capture_id)
            if capture_id:
                await asyncio.to_thread(attach_prediction, capture_id, error_payload)
            yield f"event: error\ndata: {_json.dumps(error_payload, default=str)}\n\n"

    from fastapi.responses import StreamingResponse as _StreamingResponse
    return _StreamingResponse(
        _event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",  # disable nginx buffering so events flush
            "Connection": "keep-alive",
        },
    )


@router.post("/degen_eye/v2/detect-only")
async def degen_eye_v2_detect_only(request: Request):
    """Fast card-edge detection endpoint used by Phase B auto-capture.

    Accepts a small thumbnail image (~400px JPEG base64) and runs ONLY the
    OpenCV detection + quad scoring — no pHash lookup, no price. Response
    target latency: < 100ms so the frontend can poll ~3x/second.

    Response shape:
        {
            "found": bool,
            "reason": str,
            "box": [x, y, w, h]?,           # only when found
            "corners": [[x,y], ...]?,       # only when found, 4 entries
            "stability_hash": str?,         # rounded-to-10px corner hash
            "score": float?,                # quad score for debugging
            "elapsed_ms": float,
        }
    """
    if denial := _require_employee(request):
        return denial
    import base64 as _b64
    import time as _time
    from .card_detect import detect_box

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    image_b64 = (body.get("image") or "").strip()
    if not image_b64:
        return JSONResponse({"error": "Missing image field"}, status_code=400)
    if "," in image_b64:
        image_b64 = image_b64.split(",", 1)[1]
    if len(image_b64) > _V2_MAX_DETECT_IMAGE_B64_CHARS:
        return JSONResponse({"error": "Image is too large"}, status_code=413)
    try:
        raw = _b64.b64decode(image_b64)
    except Exception:
        return JSONResponse({"error": "Invalid base64"}, status_code=400)

    t_start = _time.monotonic()
    result = await asyncio.to_thread(detect_box, raw)
    result["elapsed_ms"] = round((_time.monotonic() - t_start) * 1000, 1)
    return JSONResponse(result)


@router.get("/degen_eye/v2/stats")
async def degen_eye_v2_stats(request: Request):
    """Admin-ish JSON: index size, last build, cache warm state."""
    if denial := _require_employee(request):
        return denial
    return JSONResponse({
        "phash_index": phash_index_stats(),
        "price_cache": price_cache_stats(),
        "training_captures": training_capture_stats(),
        "pending_scans": _count_v2_pending_scans(),
        "v2_history_entries": len(get_v2_scan_history()),
    })


@router.get("/degen_eye/v2/training", response_class=HTMLResponse)
async def degen_eye_v2_training_page(request: Request):
    """Reviewer page for capture counts and one-click confirmed-capture training."""
    if denial := _require_reviewer(request):
        return denial
    return HTMLResponse(_render_v2_training_page())


@router.post("/degen_eye/v2/train-captures")
async def degen_eye_v2_train_captures(request: Request):
    """Promote confirmed employee captures into the pHash index."""
    if denial := _require_reviewer(request):
        return denial
    try:
        form = await request.form()
    except Exception:
        form = {}
    try:
        limit = int(form.get("limit") or 200)
    except (TypeError, ValueError):
        limit = 200
    limit = max(1, min(limit, 2000))
    dry_run = _truthy_form_value(form.get("dry_run"))
    include_indexed = _truthy_form_value(form.get("include_indexed"))
    reload_current_worker = _truthy_form_value(form.get("reload_current_worker"))

    summary = await asyncio.to_thread(
        train_confirmed_captures,
        limit=limit,
        include_indexed=include_indexed,
        dry_run=dry_run,
    )
    if reload_current_worker and not dry_run:
        summary["reloaded_current_worker_card_count"] = await asyncio.to_thread(phash_reload_index)
    accept = request.headers.get("accept") or ""
    if "text/html" in accept:
        return HTMLResponse(_render_v2_training_page(summary))
    return JSONResponse(summary)


@router.get("/degen_eye/v2/history")
async def degen_eye_v2_history(request: Request):
    """v2-only scan history. Separate from v1's /degen_eye/history so v2
    debugging doesn't pollute the v1 ops log and vice versa."""
    if denial := _require_employee(request):
        return denial
    return JSONResponse(get_v2_scan_history())


@router.get("/degen_eye/v2/debug", response_class=HTMLResponse)
async def degen_eye_v2_debug_page(request: Request):
    """Live debug page for v2 scans only. Mirrors v1's /degen_eye/debug UX
    but points at /degen_eye/v2/history so only v2 entries show up."""
    if denial := _require_employee(request):
        return denial
    return HTMLResponse("""<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Degen Eye v2 Debug Log</title>
<style>
  body{font-family:monospace;background:#0b0e13;color:#e0e0e0;margin:0;padding:20px;}
  h1{color:#00c2ff;font-size:18px;margin:0 0 4px;}
  .sub{color:#888;font-size:12px;margin-bottom:16px;}
  .entry{background:#101725;border:1px solid #1f2a3a;border-radius:8px;padding:14px;margin-bottom:12px;}
  .entry.MATCHED{border-left:4px solid #4caf50;}
  .entry.AMBIGUOUS{border-left:4px solid #ff9800;}
  .entry.NO_MATCH{border-left:4px solid #f44336;}
  .entry.ERROR{border-left:4px solid #f44336;}
  .ts{color:#888;font-size:11px;}
  .status{font-weight:bold;font-size:13px;margin-bottom:6px;}
  .status.MATCHED{color:#4caf50;} .status.AMBIGUOUS{color:#ff9800;}
  .status.NO_MATCH{color:#f44336;} .status.ERROR{color:#f44336;}
  .field{margin:3px 0;font-size:12px;line-height:1.5;}
  .label{color:#00c2ff;} .val{color:#e0e0e0;}
  .dbg{background:#070a10;padding:8px;border-radius:4px;white-space:pre-wrap;font-size:11px;
       max-height:220px;overflow-y:auto;margin-top:6px;color:#aaa;border:1px solid #1f2a3a;}
  .empty{color:#666;text-align:center;padding:40px;font-size:14px;}
  #auto{margin-bottom:12px;display:flex;align-items:center;gap:8px;font-size:12px;color:#888;}
</style>
</head><body>
<h1>Degen Eye v2 Debug Log</h1>
<div class="sub">Only v2 scans (pHash + optional Ximilar fallback). For v1 scans, open <a style="color:#888;" href="/degen_eye/debug">/degen_eye/debug</a>.</div>
<div id="auto"><input type="checkbox" id="autoRefresh" checked> Auto-refresh
  <button onclick="loadHistory()" style="margin-left:8px;padding:4px 10px;border-radius:4px;
    border:1px solid #1f2a3a;background:#101725;color:#e0e0e0;cursor:pointer;">Refresh Now</button></div>
<div id="log"></div>
<script>
function loadHistory(){
  fetch('/degen_eye/v2/history').then(r=>r.json()).then(data=>{
    var el = document.getElementById('log');
    if (!data || !data.length) {
      el.innerHTML = '<div class="empty">No v2 scans yet. Scan a card on /degen_eye/v2 to see entries here.</div>';
      return;
    }
    el.innerHTML = data.map(function(e){
      var status = e.status || 'UNKNOWN';
      var dbg = e.debug || {};
      var v2 = dbg.v2 || {};
      var fields = '';
      if (e.best_match_name) {
        fields += '<div class="field"><span class="label">Best:</span> <span class="val">' +
          (e.best_match_name||'') + ' #' + (e.best_match_number||'') +
          ' | ' + (e.best_match_set||'') + '</span></div>';
        fields += '<div class="field"><span class="label">Confidence:</span> <span class="val">' +
          (e.best_match_confidence||'') + ' score=' + (e.best_match_score||0).toFixed(1) + '</span></div>';
        if (e.best_match_price != null) {
          fields += '<div class="field"><span class="label">Price:</span> <span class="val">$' +
            Number(e.best_match_price).toFixed(2) + '</span></div>';
        }
      }
      if (e.processing_time_ms != null) {
        fields += '<div class="field"><span class="label">Total:</span> <span class="val">' +
          Math.round(e.processing_time_ms) + 'ms</span></div>';
      }
      if (v2.stages_ms) {
        fields += '<div class="field"><span class="label">Stages:</span> <span class="val">' +
          JSON.stringify(v2.stages_ms) + '</span></div>';
      }
      if (v2.phash && v2.phash.top && v2.phash.top.length) {
        fields += '<div class="field"><span class="label">pHash top:</span> <span class="val">' +
          (v2.phash.top[0].distance + ' (' + v2.phash.top[0].confidence + ')') + '</span></div>';
      }
      if (v2.raw_image_preferred) {
        fields += '<div class="field"><span class="label">Raw-image fallback:</span> <span class="val">' +
          JSON.stringify(v2.raw_image_preferred) + '</span></div>';
      }
      if (dbg.engines_used) {
        fields += '<div class="field"><span class="label">Engines:</span> <span class="val">' +
          dbg.engines_used.join(', ') + '</span></div>';
      }
      if (e.error) {
        fields += '<div class="field"><span class="label">Error:</span> <span class="val" style="color:#f88;">' +
          e.error + '</span></div>';
      }
      return '<div class="entry ' + status + '">' +
        '<div class="ts">' + (e.timestamp||'') + '</div>' +
        '<div class="status ' + status + '">' + status + '</div>' +
        fields +
        '<details><summary style="cursor:pointer;color:#666;font-size:11px;margin-top:6px;">Raw debug</summary>' +
        '<div class="dbg">' + JSON.stringify(dbg, null, 2) + '</div></details>' +
      '</div>';
    }).join('');
  }).catch(function(err){
    document.getElementById('log').innerHTML = '<div class="empty">Failed to load: ' + err + '</div>';
  });
}
loadHistory();
setInterval(function(){if(document.getElementById('autoRefresh').checked)loadHistory();},3000);
</script>
</body></html>""")


@router.post("/degen_eye/v2/warm")
async def degen_eye_v2_warm(request: Request):
    """Trigger an on-demand price-cache warm (reviewer-only — it's network-heavy)."""
    if denial := _require_reviewer(request):
        return denial
    stats = await warm_price_cache()
    return JSONResponse(stats)


@router.post("/degen_eye/v2/reload-index")
async def degen_eye_v2_reload_index(request: Request):
    """Reload the in-memory pHash index after an offline rebuild/training run."""
    if denial := _require_reviewer(request):
        return denial
    card_count = await asyncio.to_thread(phash_reload_index)
    payload = {"card_count": card_count, "phash_index": phash_index_stats()}
    if "text/html" in (request.headers.get("accept") or ""):
        return HTMLResponse(_render_v2_training_page({"reload": payload}))
    return JSONResponse(payload)


# ---------------------------------------------------------------------------
# AI card identification (JSON API) — generic, kept for MTG/other games
# ---------------------------------------------------------------------------

@router.post("/inventory/scan/identify")
async def inventory_scan_identify(request: Request):
    """
    Accept a base64-encoded card image, run AI identification, then fetch
    the stock image + market price from Scryfall / Pokemon TCG API.

    Request body: {"image": "<base64 string>"}
    Response: card info + image_url + market_price
    """
    if denial := _require_employee(request):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    base64_image = (body.get("image") or "").strip()
    if not base64_image:
        return JSONResponse({"error": "Missing image field"}, status_code=400)

    # Strip data URI prefix if the client sent it
    if "," in base64_image:
        base64_image = base64_image.split(",", 1)[1]

    card_info = await identify_card_from_image(base64_image)

    if card_info.get("error"):
        return JSONResponse(
            {"error": card_info["error"], "confidence": card_info.get("confidence", 0)},
            status_code=422,
        )

    confidence = float(card_info.get("confidence") or 0)
    if confidence < 0.3:
        return JSONResponse(
            {
                "error": "Could not identify card clearly",
                "confidence": confidence,
                "notes": card_info.get("notes"),
            },
            status_code=422,
        )

    # Enrich with stock image + market price
    try:
        lookup = await lookup_card_image_and_price(
            card_info.get("card_name", ""),
            game=card_info.get("game", ""),
            set_code=card_info.get("set_code"),
            card_number=card_info.get("card_number"),
            pokemon_tcg_api_key=settings.pokemon_tcg_api_key,
        )
    except Exception as exc:
        logger.warning("[inventory/scan] image lookup failed: %s", exc)
        lookup = {}

    return JSONResponse({**card_info, **lookup, "confidence": confidence})


# ---------------------------------------------------------------------------
# Slab cert lookup (JSON API)
# ---------------------------------------------------------------------------

@router.post("/inventory/scan/cert")
async def inventory_scan_cert(request: Request):
    """
    Look up a graded slab by certificate number.

    Request body: {"cert_number": "...", "grading_company": "PSA"|"BGS"|"CGC"|"SGC"}
    Response: card details + last_solds + suggested_price
    """
    if denial := _require_employee(request):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    cert_number = (body.get("cert_number") or "").strip()
    grading_company = (body.get("grading_company") or "PSA").strip().upper()

    if not cert_number:
        return JSONResponse({"error": "cert_number is required"}, status_code=400)

    result = await lookup_cert(
        cert_number,
        grading_company,
        psa_api_key=settings.psa_api_key,
    )

    if result.get("error") and not result.get("card_name"):
        return JSONResponse(result, status_code=422)

    return JSONResponse(result)


# ---------------------------------------------------------------------------
# Batch confirm — bulk create inventory items from camera scan batch
# ---------------------------------------------------------------------------

@router.post("/inventory/batch/confirm")
async def inventory_batch_confirm(
    request: Request,
    session: Session = Depends(get_session),
):
    """
    Accept a JSON array of scanned card objects and create InventoryItem records.

    Request body: [
      {
        "card_name": "...",
        "game": "...",
        "condition": "NM",
        "set_name": "...",
        "card_number": "...",
        "image_url": "...",
        "auto_price": 4.99,
        "is_foil": false,
        "notes": "..."
      },
      ...
    ]

    Response: {"created": N, "items": [{"id": ..., "barcode": ..., "card_name": ...}]}
    """
    if denial := _require_reviewer(request):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    if not isinstance(body, list) or not body:
        return JSONResponse({"error": "Expected a non-empty JSON array"}, status_code=400)

    created = []
    confirmed_by = _capture_user_payload(request)
    capture_label_updates: list[tuple[str, dict[str, Any], int]] = []
    for raw in body:
        if not isinstance(raw, dict):
            continue
        card_name = (raw.get("card_name") or "").strip()
        if not card_name:
            continue

        item = InventoryItem(
            barcode="PENDING",
            item_type=ITEM_TYPE_SINGLE,
            game=(raw.get("game") or "Other").strip(),
            card_name=card_name,
            set_name=(raw.get("set_name") or "").strip() or None,
            card_number=(raw.get("card_number") or "").strip() or None,
            condition=(raw.get("condition") or "").strip() or None,
            image_url=(raw.get("image_url") or "").strip() or None,
            auto_price=_parse_float(str(raw.get("auto_price") or "")),
            notes=(raw.get("notes") or "").strip() or None,
            status=INVENTORY_IN_STOCK,
            created_at=utcnow(),
        )
        session.add(item)
        session.flush()  # get item.id without full commit

        item.barcode = generate_barcode_value(item.id)
        session.add(item)
        created.append({"id": item.id, "barcode": item.barcode, "card_name": item.card_name})
        capture_id = (raw.get("_v2_capture_id") or raw.get("capture_id") or "").strip()
        if capture_id:
            label = dict(raw)
            label.update({
                "card_name": item.card_name,
                "game": item.game,
                "set_name": item.set_name or "",
                "card_number": item.card_number or "",
                "condition": item.condition or "",
                "image_url": item.image_url or "",
                "auto_price": item.auto_price,
                "notes": item.notes or "",
            })
            capture_label_updates.append((capture_id, label, int(item.id)))

    session.commit()
    for capture_id, label, inventory_item_id in capture_label_updates:
        attach_confirmed_label(
            capture_id,
            label,
            inventory_item_id=inventory_item_id,
            confirmed_by=confirmed_by,
        )
    return JSONResponse({"created": len(created), "items": created})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_float(value: str) -> Optional[float]:
    if not value or not value.strip():
        return None
    try:
        return round(float(value.strip().replace(",", "")), 2)
    except ValueError:
        return None
