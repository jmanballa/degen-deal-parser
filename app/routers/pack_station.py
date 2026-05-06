from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlmodel import Session

from ..csrf import CSRFProtectedRoute
from ..db import get_session
from ..pack_station import (
    PACK_SOURCES,
    PACK_QUEUE_EXCEPTION_FILTERS,
    load_pack_exception_queue,
    load_pack_queue,
    pack_exception_summary,
    pack_queue_summary,
    record_pack_override,
    record_pack_reopen,
    record_pack_scan,
)
from ..shared import require_role_response, templates

router = APIRouter(route_class=CSRFProtectedRoute)


@router.get("/pack-station", response_class=HTMLResponse)
def pack_station_page(
    request: Request,
    source: str = Query(default="all"),
    search: str = Query(default=""),
    days: int = Query(default=30, ge=1, le=120),
    limit: int = Query(default=75, ge=1, le=200),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    selected_source = source if source in (PACK_SOURCES | {"all"}) else "all"
    rows = load_pack_queue(
        session,
        source=selected_source,
        search=search,
        days=days,
        limit=limit,
    )
    return templates.TemplateResponse(
        request,
        "pack_station.html",
        {
            "request": request,
            "title": "Pack Station",
            "current_user": getattr(request.state, "current_user", None),
            "orders": rows,
            "summary": pack_queue_summary(rows),
            "selected_source": selected_source,
            "selected_search": search,
            "selected_days": days,
            "selected_limit": limit,
        },
    )


@router.get("/pack-station/exceptions", response_class=HTMLResponse)
def pack_exception_queue_page(
    request: Request,
    source: str = Query(default="all"),
    status: str = Query(default="blocked"),
    search: str = Query(default=""),
    days: int = Query(default=30, ge=1, le=120),
    limit: int = Query(default=75, ge=1, le=200),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    selected_source = source if source in (PACK_SOURCES | {"all"}) else "all"
    selected_status = status if status in PACK_QUEUE_EXCEPTION_FILTERS else "blocked"
    rows = load_pack_exception_queue(
        session,
        source=selected_source,
        status_filter=selected_status,
        search=search,
        days=days,
        limit=limit,
    )
    return templates.TemplateResponse(
        request,
        "pack_exceptions.html",
        {
            "request": request,
            "title": "Pack Exceptions",
            "current_user": getattr(request.state, "current_user", None),
            "orders": rows,
            "summary": pack_exception_summary(rows),
            "selected_source": selected_source,
            "selected_status": selected_status,
            "selected_search": search,
            "selected_days": days,
            "selected_limit": limit,
        },
    )


@router.post("/pack-station/api/scan", response_class=JSONResponse)
async def pack_station_scan(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc
    try:
        event = record_pack_scan(
            session,
            source=str(payload.get("source") or ""),
            order_id=str(payload.get("order_id") or ""),
            barcode=str(payload.get("barcode") or ""),
            user=getattr(request.state, "current_user", None),
            notes=payload.get("notes"),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    order_row = None
    rows = load_pack_queue(session, source=event.order_source, search=event.order_id, days=120, limit=1)
    if rows:
        order_row = rows[0]
    return JSONResponse(jsonable_encoder(
        {
            "ok": True,
            "event": {
                "id": event.id,
                "source": event.order_source,
                "order_id": event.order_id,
                "order_number": event.order_number,
                "barcode": event.barcode,
                "inventory_item_id": event.inventory_item_id,
                "expected": event.expected,
                "status": event.status,
                "created_at": event.created_at.isoformat(),
            },
            "order": order_row,
        }
    ))


def _redirect_back(request: Request) -> RedirectResponse:
    target = request.headers.get("referer") or "/pack-station/exceptions"
    return RedirectResponse(target, status_code=303)


@router.post("/pack-station/exceptions/override")
def pack_exception_override(
    request: Request,
    source: str = Form(...),
    order_id: str = Form(...),
    reason: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        record_pack_override(
            session,
            source=source,
            order_id=order_id,
            reason=reason,
            user=getattr(request.state, "current_user", None),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _redirect_back(request)


@router.post("/pack-station/exceptions/reopen")
def pack_exception_reopen(
    request: Request,
    source: str = Form(...),
    order_id: str = Form(...),
    reason: str = Form(default=""),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "reviewer"):
        return denial
    try:
        record_pack_reopen(
            session,
            source=source,
            order_id=order_id,
            reason=reason,
            user=getattr(request.state, "current_user", None),
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _redirect_back(request)
