"""
Inventory management routes.

All routes require at minimum 'viewer' role. Mutations (add, edit, reprice,
push-to-shopify) require 'reviewer' or above.
"""
from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates  # noqa: F401 — used for _templates instance
from sqlmodel import Session, select, func

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

from .auth import has_role
from .card_scanner import identify_card_from_image, lookup_card_image_and_price
from .cert_lookup import lookup_cert
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
# AI card identification (JSON API)
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
