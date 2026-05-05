from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from sqlmodel import Session

from ..csrf import CSRFProtectedRoute
from ..db import get_session
from ..pack_station import (
    PACK_SOURCES,
    load_pack_queue,
    pack_queue_summary,
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
