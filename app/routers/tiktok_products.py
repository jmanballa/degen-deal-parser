"""
TikTok Products routes.

Extracted from app/main.py -- all routes under /tiktok/products/.
"""
from __future__ import annotations

import json
import threading
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import func
from sqlmodel import Session, select

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..shared import (  # noqa: F401 - explicit imports for underscore-prefixed names
    _resolve_tiktok_pull_credentials,
)
from ..db import get_session, managed_session
from ..config import get_settings
from ..models import TikTokProduct, utcnow
from ..runtime_logging import structured_log_line

try:
    from scripts.tiktok_backfill import backfill_tiktok_products as pull_tiktok_products
    from scripts.tiktok_backfill import (
        fetch_tiktok_categories as _fetch_tiktok_categories,
        fetch_tiktok_category_attributes as _fetch_tiktok_category_attributes,
        fetch_tiktok_brands as _fetch_tiktok_brands,
        upload_tiktok_product_image as _upload_tiktok_product_image,
        create_tiktok_product as _create_tiktok_product,
        fetch_tiktok_product_detail as _fetch_tiktok_product_detail,
        upsert_tiktok_product_row as _upsert_tiktok_product_row,
    )
except Exception:  # pragma: no cover - fallback if the script module is unavailable
    pull_tiktok_products = None
    _fetch_tiktok_categories = None
    _fetch_tiktok_category_attributes = None
    _fetch_tiktok_brands = None
    _upload_tiktok_product_image = None
    _create_tiktok_product = None
    _fetch_tiktok_product_detail = None
    _upsert_tiktok_product_row = None

settings = get_settings()

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers (only used by tiktok product routes)
# ---------------------------------------------------------------------------

_tiktok_product_sync_state: dict[str, object] = {
    "is_running": False,
    "last_finished_at": None,
    "last_error": None,
}
_tiktok_product_sync_lock = threading.Lock()

def _read_tiktok_product_sync_state() -> dict[str, object]:
    with _tiktok_product_sync_lock:
        return dict(_tiktok_product_sync_state)

def _update_tiktok_product_sync_state(**changes: object) -> None:
    with _tiktok_product_sync_lock:
        _tiktok_product_sync_state.update(changes)

def run_tiktok_product_sync_background(*, limit: Optional[int], trigger: str = "manual") -> None:
    runtime_name = f"{settings.runtime_name}_tiktok_product_sync"
    _update_tiktok_product_sync_state(is_running=True, last_error=None)
    try:
        if pull_tiktok_products is None:
            _update_tiktok_product_sync_state(is_running=False, last_error="product sync unavailable")
            return

        with managed_session() as session:
            auth_row = ensure_tiktok_auth_row(session)
            shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
            if not shop_id and not shop_cipher:
                _update_tiktok_product_sync_state(is_running=False, last_error="missing shop identity")
                return
            if not access_token:
                _update_tiktok_product_sync_state(is_running=False, last_error="missing access token")
                return
            summary = pull_tiktok_products(
                session,
                base_url=resolve_tiktok_shop_pull_base_url(),
                app_key=(settings.tiktok_app_key or "").strip(),
                app_secret=(settings.tiktok_app_secret or "").strip(),
                access_token=access_token,
                shop_id=shop_id,
                shop_cipher=shop_cipher,
                limit=limit,
                dry_run=False,
                runtime_name=runtime_name,
            )
            session.commit()
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="tiktok.products.sync_complete",
                    success=True,
                    trigger=trigger,
                    fetched=summary.fetched,
                    inserted=summary.inserted,
                    updated=summary.updated,
                    failed=summary.failed,
                )
            )
        _update_tiktok_product_sync_state(is_running=False, last_finished_at=utcnow(), last_error=None)
    except Exception as exc:
        _update_tiktok_product_sync_state(is_running=False, last_finished_at=utcnow(), last_error=str(exc))
        print(
            structured_log_line(
                runtime=runtime_name,
                action="tiktok.products.sync_failed",
                success=False,
                error=str(exc),
                trigger=trigger,
            )
        )

def _get_tiktok_product_filter_options(session: Session) -> dict[str, list[str]]:
    def _distinct(col):
        try:
            return sorted({v for v in session.exec(select(col).distinct()).all() if v not in (None, "")})
        except Exception as exc:
            print(
                structured_log_line(
                    runtime="app",
                    action="tiktok.products.filter_distinct_failed",
                    success=False,
                    context="tiktok_products._get_tiktok_product_filter_options._distinct",
                    column=str(getattr(col, "key", col)),
                    error=str(exc)[:400],
                )
            )
            return []
    return {
        "statuses": _distinct(TikTokProduct.status),
        "audit_statuses": _distinct(TikTokProduct.audit_status),
        "source_options": _distinct(TikTokProduct.source),
    }

def _build_product_sku_summary(skus_json: str) -> dict[str, object]:
    try:
        skus = json.loads(skus_json) if skus_json else []
    except (json.JSONDecodeError, TypeError):
        skus = []
    if not isinstance(skus, list):
        skus = []
    count = len(skus)
    prices = [s.get("price") or 0 for s in skus if isinstance(s, dict)]
    total_inventory = sum(s.get("inventory") or 0 for s in skus if isinstance(s, dict))
    min_price = min(prices) if prices else 0
    max_price = max(prices) if prices else 0
    return {
        "count": count,
        "min_price": round(float(min_price), 2),
        "max_price": round(float(max_price), 2),
        "total_inventory": total_inventory,
    }

def _get_tiktok_api_client_context(session: Session) -> dict[str, Any]:
    auth_row = ensure_tiktok_auth_row(session)
    shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(auth_row)
    return {
        "base_url": resolve_tiktok_shop_pull_base_url(),
        "app_key": (settings.tiktok_app_key or "").strip(),
        "app_secret": (settings.tiktok_app_secret or "").strip(),
        "access_token": access_token,
        "shop_id": shop_id,
        "shop_cipher": shop_cipher,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/tiktok/products/sync-form")
def tiktok_products_sync_form(
    request: Request,
    limit: Optional[str] = Form(default=""),
):
    if denial := require_role_response(request, "admin"):
        return denial
    sync_state = _read_tiktok_product_sync_state()
    if sync_state.get("is_running"):
        return RedirectResponse(url="/tiktok/products?success=Product+sync+already+running", status_code=303)

    raw_limit = (limit or "").strip()
    safe_limit: Optional[int]
    if not raw_limit:
        safe_limit = 200
    else:
        try:
            safe_limit = max(int(raw_limit), 1)
        except ValueError:
            return RedirectResponse(url="/tiktok/products?error=Limit+must+be+a+number", status_code=303)

    thread = threading.Thread(
        target=run_tiktok_product_sync_background,
        kwargs={"limit": safe_limit, "trigger": "manual"},
        daemon=True,
        name="tiktok-product-sync-manual",
    )
    thread.start()
    return RedirectResponse(url="/tiktok/products?success=Started+product+sync", status_code=303)

@router.get("/tiktok/products", response_class=HTMLResponse)
def tiktok_products_page(
    request: Request,
    status: Optional[str] = Query(default=None),
    audit_status: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="updated"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    query = select(TikTokProduct)
    if status:
        query = query.where(TikTokProduct.status == status)
    if audit_status:
        query = query.where(TikTokProduct.audit_status == audit_status)
    if search:
        term = f"%{search}%"
        query = query.where(
            (TikTokProduct.title.ilike(term))
            | (TikTokProduct.tiktok_product_id.ilike(term))
            | (TikTokProduct.category_name.ilike(term))
            | (TikTokProduct.brand_name.ilike(term))
        )

    sort_column_map = {
        "title": TikTokProduct.title,
        "status": TikTokProduct.status,
        "updated": TikTokProduct.updated_at,
        "created": TikTokProduct.created_at,
        "synced": TikTokProduct.synced_at,
    }
    sort_col = sort_column_map.get(sort_by, TikTokProduct.updated_at)
    query = query.order_by(sort_col.desc() if sort_dir == "desc" else sort_col.asc(), TikTokProduct.id.desc())

    count_query = select(func.count()).select_from(TikTokProduct)
    if query.whereclause is not None:
        count_query = count_query.where(query.whereclause)
    total_count = session.exec(count_query).one()
    offset = (max(page, 1) - 1) * limit
    rows = session.exec(query.offset(offset).limit(limit)).all()
    has_more = (offset + limit) < total_count

    products = []
    for row in rows:
        sku_info = _build_product_sku_summary(row.skus_json)
        products.append({
            "product": row,
            "sku_count": sku_info["count"],
            "min_price": sku_info["min_price"],
            "max_price": sku_info["max_price"],
            "total_inventory": sku_info["total_inventory"],
            "price_label": (
                f"${sku_info['min_price']:.2f}" if sku_info["min_price"] == sku_info["max_price"]
                else f"${sku_info['min_price']:.2f} - ${sku_info['max_price']:.2f}"
            ) if sku_info["count"] > 0 else "-",
        })

    filter_options = _get_tiktok_product_filter_options(session)
    sync_state = _read_tiktok_product_sync_state()

    summary_total = int(session.exec(select(func.count()).select_from(TikTokProduct)).one())
    summary_active = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "ACTIVATE")
    ).one())
    summary_draft = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "DRAFT")
    ).one())
    summary_deactivated = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(
            TikTokProduct.status.in_(["SELLER_DEACTIVATED", "PLATFORM_DEACTIVATED"])
        )
    ).one())

    return templates.TemplateResponse(request, "tiktok_products.html", {
        "request": request,
        "title": "TikTok Products",
        "products": products,
        "total_count": total_count,
        "page": max(page, 1),
        "page_size": limit,
        "has_more": has_more,
        "filter_status": status or "",
        "filter_audit_status": audit_status or "",
        "filter_search": search or "",
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "filter_options": filter_options,
        "sync_state": sync_state,
        "success_message": success,
        "error_message": error,
        "summary_total": summary_total,
        "summary_active": summary_active,
        "summary_draft": summary_draft,
        "summary_deactivated": summary_deactivated,
        "current_user": getattr(request.state, "current_user", None),
    })

@router.get("/tiktok/products/poll")
def tiktok_products_poll(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    total = int(session.exec(select(func.count()).select_from(TikTokProduct)).one())
    active = int(session.exec(
        select(func.count()).select_from(TikTokProduct).where(TikTokProduct.status == "ACTIVATE")
    ).one())
    sync_state = _read_tiktok_product_sync_state()
    latest_synced = session.exec(select(func.max(TikTokProduct.synced_at))).one()
    latest_synced_text = None
    if latest_synced is not None:
        if latest_synced.tzinfo is None:
            latest_synced = latest_synced.replace(tzinfo=timezone.utc)
        latest_synced_text = latest_synced.isoformat()
    return {
        "total": total,
        "active": active,
        "is_syncing": sync_state.get("is_running", False),
        "last_error": sync_state.get("last_error"),
        "latest_synced_at": latest_synced_text,
    }

@router.get("/tiktok/products/categories")
def tiktok_products_categories_api(
    request: Request,
    keyword: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    if _fetch_tiktok_categories is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        categories = _fetch_tiktok_categories(
            client, keyword=keyword or None, **ctx,
        )
    return {"categories": categories}

@router.get("/tiktok/products/categories/{category_id}/attributes")
def tiktok_products_category_attributes_api(
    request: Request,
    category_id: str,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    if _fetch_tiktok_category_attributes is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        attributes = _fetch_tiktok_category_attributes(
            client, category_id=category_id, **ctx,
        )
    return {"attributes": attributes}

@router.get("/tiktok/products/brands")
def tiktok_products_brands_api(
    request: Request,
    brand_name: Optional[str] = Query(default=None),
    category_id: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    if _fetch_tiktok_brands is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        brands = _fetch_tiktok_brands(
            client, brand_name=brand_name or None, category_id=category_id or None, **ctx,
        )
    return {"brands": brands}

@router.post("/tiktok/products/upload-image")
async def tiktok_products_upload_image(
    request: Request,
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    if _upload_tiktok_product_image is None:
        return JSONResponse({"error": "TikTok API helpers unavailable"}, status_code=503)
    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return JSONResponse({"error": "TikTok auth not configured"}, status_code=400)
    image_data = await file.read()
    if len(image_data) > 5 * 1024 * 1024:
        return JSONResponse({"error": "Image must be under 5MB"}, status_code=400)
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        try:
            uri = _upload_tiktok_product_image(
                client,
                image_data=image_data,
                file_name=file.filename or "image.jpg",
                **ctx,
            )
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=500)
    return {"uri": uri}

@router.get("/tiktok/products/new", response_class=HTMLResponse)
def tiktok_products_new_page(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return templates.TemplateResponse(request, "tiktok_product_form.html", {
        "request": request,
        "title": "New TikTok Product",
        "mode": "create",
        "product": None,
        "current_user": getattr(request.state, "current_user", None),
    })

@router.post("/tiktok/products/create")
async def tiktok_products_create(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    if _create_tiktok_product is None:
        return RedirectResponse(url="/tiktok/products?error=TikTok+API+helpers+unavailable", status_code=303)

    form = await request.form()
    title = (form.get("title") or "").strip()
    description = (form.get("description") or "").strip()
    category_id = (form.get("category_id") or "").strip()
    brand_id = (form.get("brand_id") or "").strip()

    if not title:
        return RedirectResponse(url="/tiktok/products/new?error=Title+is+required", status_code=303)
    if not category_id:
        return RedirectResponse(url="/tiktok/products/new?error=Category+is+required", status_code=303)

    image_uris_raw = form.get("image_uris") or ""
    image_uris = [u.strip() for u in image_uris_raw.split(",") if u.strip()]
    main_images = [{"uri": uri} for uri in image_uris]

    skus = []
    sku_index = 0
    while True:
        price_key = f"sku_price_{sku_index}"
        if price_key not in form:
            break
        price = (form.get(price_key) or "0").strip()
        inventory = (form.get(f"sku_inventory_{sku_index}") or "0").strip()
        seller_sku = (form.get(f"sku_seller_sku_{sku_index}") or "").strip()
        sku_entry: dict[str, Any] = {
            "sales_attributes": [],
            "price": {
                "amount": price,
                "currency": "USD",
            },
            "inventory": [{"quantity": int(inventory or 0)}],
        }
        if seller_sku:
            sku_entry["seller_sku"] = seller_sku
        skus.append(sku_entry)
        sku_index += 1

    if not skus:
        skus = [{
            "sales_attributes": [],
            "price": {"amount": (form.get("price") or "0").strip(), "currency": "USD"},
            "inventory": [{"quantity": int((form.get("inventory") or "0").strip() or 0)}],
        }]
        seller_sku_single = (form.get("seller_sku") or "").strip()
        if seller_sku_single:
            skus[0]["seller_sku"] = seller_sku_single

    product_body: dict[str, Any] = {
        "title": title,
        "description": description or title,
        "category_id": category_id,
        "main_images": main_images,
        "skus": skus,
        "is_cod_allowed": False,
    }
    if brand_id:
        product_body["brand"] = {"id": brand_id}

    save_as_draft = (form.get("save_as_draft") or "").strip()
    if save_as_draft == "1":
        product_body["save_mode"] = "AS_DRAFT"

    ctx = _get_tiktok_api_client_context(session)
    if not ctx["access_token"]:
        return RedirectResponse(url="/tiktok/products/new?error=TikTok+auth+not+configured", status_code=303)

    try:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            result = _create_tiktok_product(client, product_body=product_body, **ctx)
    except Exception as exc:
        error_msg = str(exc)[:200].replace(" ", "+")
        return RedirectResponse(url=f"/tiktok/products/new?error={error_msg}", status_code=303)

    new_product_id = result.get("product_id") or result.get("id") or ""
    if new_product_id and _upsert_tiktok_product_row is not None:
        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                detail = _fetch_tiktok_product_detail(
                    client, product_id=str(new_product_id), **ctx,
                )
            _upsert_tiktok_product_row(
                session, detail,
                shop_id=ctx["shop_id"], shop_cipher=ctx["shop_cipher"],
                source="created", dry_run=False,
            )
            session.commit()
        except Exception as exc:
            print(structured_log_line(runtime="app", action="tiktok.product_detail_upsert_failed", success=False, error=str(exc)))

    return RedirectResponse(url="/tiktok/products?success=Product+created+successfully", status_code=303)

@router.get("/tiktok/products/{product_id}", response_class=HTMLResponse)
def tiktok_product_detail_page(
    request: Request,
    product_id: str,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    product = session.exec(
        select(TikTokProduct).where(TikTokProduct.tiktok_product_id == product_id)
    ).first()
    if product is None:
        return RedirectResponse(url="/tiktok/products?error=Product+not+found", status_code=303)

    try:
        skus = json.loads(product.skus_json) if product.skus_json else []
    except (json.JSONDecodeError, TypeError):
        skus = []
    try:
        images = json.loads(product.images_json) if product.images_json else []
    except (json.JSONDecodeError, TypeError):
        images = []

    return templates.TemplateResponse(request, "tiktok_product_detail.html", {
        "request": request,
        "title": product.title or "Product Detail",
        "product": product,
        "skus": skus,
        "images": images,
        "current_user": getattr(request.state, "current_user", None),
    })
