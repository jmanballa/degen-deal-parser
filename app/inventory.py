"""
Inventory management routes.

All routes require at minimum 'viewer' role. Mutations (add, edit, reprice,
push-to-shopify) require 'reviewer' or above.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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


def _require_reviewer(request: Request) -> Optional[Response]:
    return _check_role(request, "reviewer")


def _current_user(request: Request):
    return _get_user(request)


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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_singles.html",
        {"current_user": _current_user(request)},
    )


@router.get("/inventory/scan/slabs", response_class=HTMLResponse)
async def inventory_scan_slabs_page(request: Request):
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
        return denial
    return _templates.TemplateResponse(
        request,
        "inventory_scan_pokemon.html",
        {"current_user": _current_user(request), "conditions": CONDITIONS},
    )


@router.get("/degen_eye/categories")
async def inventory_scan_categories(request: Request):
    """Return TCGTracking categories with preferred ordering."""
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
        return denial
    return JSONResponse(get_scan_history())


@router.get("/degen_eye/validate/{scan_id}")
async def inventory_scan_pokemon_validate(request: Request, scan_id: str):
    """Poll for background OCR validation result."""
    if denial := _require_viewer(request):
        return denial
    result = get_validation_result(scan_id)
    if result is None:
        return JSONResponse({"error": "Unknown scan_id"}, status_code=404)
    return JSONResponse(result)


@router.get("/degen_eye/debug", response_class=HTMLResponse)
async def inventory_scan_pokemon_debug_page(request: Request):
    """Live debug page showing recent scan history — open on desktop while scanning on phone."""
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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
    if denial := _require_viewer(request):
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

    session.commit()
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
