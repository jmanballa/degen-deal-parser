"""
Live Hit Tracker routes.

Extracted from app/main.py -- all routes under /hits/ and /api/hits/.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Form, Query, Request, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import or_
from sqlmodel import Session, select

from ..config import BASE_DIR
from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..db import get_session

router = APIRouter()

HIT_IMAGES_DIR = BASE_DIR / "data" / "hit_images"
ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"}
MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB


# ---------------------------------------------------------------------------
# Helpers (only used by hit routes)
# ---------------------------------------------------------------------------

def _build_hits_stmt(
    *,
    streamer: Optional[str] = None,
    after: Optional[datetime] = None,
    before: Optional[datetime] = None,
    search: Optional[str] = None,
    min_value: Optional[float] = None,
):
    stmt = select(LiveHit).where(LiveHit.is_deleted == False)
    if streamer:
        stmt = stmt.where(LiveHit.streamer_name == streamer)
    if after:
        stmt = stmt.where(LiveHit.hit_at >= after)
    if before:
        stmt = stmt.where(LiveHit.hit_at <= before)
    if search:
        like = f"%{search}%"
        stmt = stmt.where(
            or_(
                LiveHit.customer_name.ilike(like),
                LiveHit.order_number.ilike(like),
                LiveHit.hit_note.ilike(like),
            )
        )
    if min_value is not None:
        stmt = stmt.where(LiveHit.estimated_value >= min_value)
    return stmt.order_by(LiveHit.hit_at.desc())


def _parse_hit_at(raw: Optional[str]) -> datetime:
    """Parse a datetime-local string from a form field; fall back to utcnow."""
    if raw:
        try:
            return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return utcnow()


def _parse_optional_float(raw: Optional[str]) -> Optional[float]:
    if raw and raw.strip():
        try:
            return float(raw.strip())
        except ValueError:
            pass
    return None


def _hit_to_dict(h: LiveHit) -> dict:
    return {
        "id": h.id,
        "hit_at": h.hit_at.isoformat() if h.hit_at else None,
        "streamer_name": h.streamer_name,
        "customer_name": h.customer_name or "",
        "order_number": h.order_number or "",
        "hit_note": h.hit_note,
        "estimated_value": h.estimated_value,
        "order_value": h.order_value,
        "platform": h.platform or "",
        "stream_label": h.stream_label or "",
        "notes": h.notes or "",
        "created_by": h.created_by or "",
        "created_at": h.created_at.isoformat() if h.created_at else None,
        "is_big_hit": (h.estimated_value or 0) >= BIG_HIT_THRESHOLD,
        "image_filename": h.image_filename or None,
        "image_url": f"/hit-images/{h.image_filename}" if h.image_filename else None,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/hits", response_class=HTMLResponse)
def hits_list_page(
    request: Request,
    streamer: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    min_value: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    after_dt: Optional[datetime] = None
    before_dt: Optional[datetime] = None
    if after:
        try:
            after_dt = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if before:
        try:
            before_dt = datetime.fromisoformat(before).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    min_val = _parse_optional_float(min_value)

    stmt = _build_hits_stmt(
        streamer=streamer,
        after=after_dt,
        before=before_dt,
        search=search,
        min_value=min_val,
    )
    if platform:
        stmt = stmt.where(LiveHit.platform == platform)

    total_rows = count_rows(session, stmt)
    offset = (max(page, 1) - 1) * limit
    hits = session.exec(stmt.offset(offset).limit(limit)).all()
    pagination = build_pagination(page, limit, total_rows)

    # Summary stats for current filter (fetch all rows without pagination)
    all_hits = session.exec(stmt).all()
    total_value = sum(h.estimated_value or 0 for h in all_hits)
    big_hits_count = sum(1 for h in all_hits if (h.estimated_value or 0) >= BIG_HIT_THRESHOLD)
    active_streamers = len({h.streamer_name for h in all_hits})

    return templates.TemplateResponse(request, "hits.html", {
        "request": request,
        "title": "Live Hit Tracker",
        "current_user": getattr(request.state, "current_user", None),
        "hits": hits,
        "pagination": pagination,
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
        "total_rows": total_rows,
        "total_value": total_value,
        "big_hits_count": big_hits_count,
        "active_streamers": active_streamers,
        # filter values for form re-population
        "sel_streamer": streamer or "",
        "sel_platform": platform or "",
        "sel_after": after or "",
        "sel_before": before or "",
        "sel_search": search or "",
        "sel_min_value": min_value or "",
        "sel_limit": limit,
    })


@router.get("/hits/new", response_class=HTMLResponse)
def hits_new_page(
    request: Request,
    success: Optional[str] = Query(default=None),
    last_streamer: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    recent = session.exec(
        select(LiveHit)
        .where(LiveHit.is_deleted == False)
        .order_by(LiveHit.hit_at.desc())
        .limit(5)
    ).all()

    return templates.TemplateResponse(request, "hits_new.html", {
        "request": request,
        "title": "Log a Hit",
        "current_user": getattr(request.state, "current_user", None),
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "success": success,
        "last_streamer": last_streamer or get_current_streamer(session) or "",
        "recent_hits": recent,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
    })


@router.post("/hits/new")
def hits_new_submit(
    request: Request,
    streamer_name: str = Form(...),
    hit_note: str = Form(...),
    customer_name: Optional[str] = Form(default=None),
    order_number: Optional[str] = Form(default=None),
    estimated_value: Optional[str] = Form(default=None),
    order_value: Optional[str] = Form(default=None),
    platform: Optional[str] = Form(default=None),
    stream_label: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    hit_at_raw: Optional[str] = Form(default=None),
    add_another: Optional[str] = Form(default=None),
    image_filename: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    img_fn = (image_filename or "").strip() or None
    if img_fn and (".." in img_fn or "/" in img_fn or "\\" in img_fn):
        img_fn = None

    hit = LiveHit(
        streamer_name=streamer_name.strip(),
        hit_note=hit_note.strip(),
        customer_name=(customer_name or "").strip() or None,
        order_number=(order_number or "").strip() or None,
        estimated_value=_parse_optional_float(estimated_value),
        order_value=_parse_optional_float(order_value),
        platform=(platform or "").strip() or None,
        stream_label=(stream_label or "").strip() or None,
        notes=(notes or "").strip() or None,
        hit_at=_parse_hit_at(hit_at_raw),
        created_by=current_user_label(request),
        image_filename=img_fn,
    )
    session.add(hit)
    session.commit()

    qs = f"success=1&last_streamer={hit.streamer_name}"
    if add_another:
        return RedirectResponse(url=f"/hits/new?{qs}", status_code=303)
    return RedirectResponse(url=f"/hits?success=1", status_code=303)


@router.get("/hits/{hit_id}/edit", response_class=HTMLResponse)
def hits_edit_page(
    request: Request,
    hit_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = session.get(LiveHit, hit_id)
    if not hit or hit.is_deleted:
        return RedirectResponse(url="/hits?error=Hit+not+found", status_code=303)

    return templates.TemplateResponse(request, "hits_edit.html", {
        "request": request,
        "title": "Edit Hit",
        "current_user": getattr(request.state, "current_user", None),
        "hit": hit,
        "streamers": get_streamer_names(session),
        "platforms": PLATFORMS,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
    })


@router.post("/hits/{hit_id}/edit")
def hits_edit_submit(
    request: Request,
    hit_id: int,
    streamer_name: str = Form(...),
    hit_note: str = Form(...),
    customer_name: Optional[str] = Form(default=None),
    order_number: Optional[str] = Form(default=None),
    estimated_value: Optional[str] = Form(default=None),
    order_value: Optional[str] = Form(default=None),
    platform: Optional[str] = Form(default=None),
    stream_label: Optional[str] = Form(default=None),
    notes: Optional[str] = Form(default=None),
    hit_at_raw: Optional[str] = Form(default=None),
    image_filename: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = session.get(LiveHit, hit_id)
    if not hit or hit.is_deleted:
        return RedirectResponse(url="/hits?error=Hit+not+found", status_code=303)

    hit.streamer_name = streamer_name.strip()
    hit.hit_note = hit_note.strip()
    hit.customer_name = (customer_name or "").strip() or None
    hit.order_number = (order_number or "").strip() or None
    hit.estimated_value = _parse_optional_float(estimated_value)
    hit.order_value = _parse_optional_float(order_value)
    hit.platform = (platform or "").strip() or None
    hit.stream_label = (stream_label or "").strip() or None
    hit.notes = (notes or "").strip() or None
    hit.hit_at = _parse_hit_at(hit_at_raw)

    img_val = (image_filename or "").strip()
    if img_val == "__remove__":
        hit.image_filename = None
    elif img_val and ".." not in img_val and "/" not in img_val and "\\" not in img_val:
        hit.image_filename = img_val

    hit.updated_at = utcnow()
    session.add(hit)
    session.commit()

    return RedirectResponse(url="/hits?success=1", status_code=303)


@router.post("/hits/{hit_id}/delete")
def hits_delete(
    request: Request,
    hit_id: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hit = session.get(LiveHit, hit_id)
    if hit and not hit.is_deleted:
        hit.is_deleted = True
        hit.updated_at = utcnow()
        session.add(hit)
        session.commit()

    return RedirectResponse(url="/hits?success=1", status_code=303)


@router.get("/api/hits/export")
def hits_export_csv(
    request: Request,
    streamer: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    min_value: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    after_dt: Optional[datetime] = None
    before_dt: Optional[datetime] = None
    if after:
        try:
            after_dt = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if before:
        try:
            before_dt = datetime.fromisoformat(before).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    stmt = _build_hits_stmt(
        streamer=streamer,
        after=after_dt,
        before=before_dt,
        search=search,
        min_value=_parse_optional_float(min_value),
    )
    if platform:
        stmt = stmt.where(LiveHit.platform == platform)

    hits = session.exec(stmt).all()
    rows = [_hit_to_dict(h) for h in hits]
    # Drop the computed field; keep only DB columns for export
    for r in rows:
        r.pop("is_big_hit", None)

    return csv_response("hits_export.csv", rows if rows else [
        {"message": "No hits found for the given filters"}
    ])


@router.get("/api/hits/summary")
def hits_summary_json(
    request: Request,
    streamer: Optional[str] = Query(default=None),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    after_dt: Optional[datetime] = None
    before_dt: Optional[datetime] = None
    if after:
        try:
            after_dt = datetime.fromisoformat(after).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if before:
        try:
            before_dt = datetime.fromisoformat(before).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    hits = session.exec(_build_hits_stmt(streamer=streamer, after=after_dt, before=before_dt)).all()

    per_streamer: dict = {}
    total_value = 0.0
    big_hits = 0
    for h in hits:
        per_streamer[h.streamer_name] = per_streamer.get(h.streamer_name, 0) + 1
        total_value += h.estimated_value or 0
        if (h.estimated_value or 0) >= BIG_HIT_THRESHOLD:
            big_hits += 1

    return {
        "total_hits": len(hits),
        "total_estimated_value": round(total_value, 2),
        "big_hits_count": big_hits,
        "big_hit_threshold": BIG_HIT_THRESHOLD,
        "per_streamer": per_streamer,
    }


@router.post("/api/hits")
async def hits_api_create(
    request: Request,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)

    streamer_name = (body.get("streamer_name") or "").strip()
    hit_note = (body.get("hit_note") or "").strip()
    if not streamer_name or not hit_note:
        return JSONResponse({"ok": False, "error": "streamer_name and hit_note are required"}, status_code=422)

    img_fn = (body.get("image_filename") or "").strip() or None
    if img_fn and (".." in img_fn or "/" in img_fn or "\\" in img_fn):
        img_fn = None

    hit = LiveHit(
        streamer_name=streamer_name,
        hit_note=hit_note,
        customer_name=(body.get("customer_name") or "").strip() or None,
        order_number=(body.get("order_number") or "").strip() or None,
        estimated_value=_parse_optional_float(str(body.get("estimated_value", "")) if body.get("estimated_value") is not None else ""),
        order_value=_parse_optional_float(str(body.get("order_value", "")) if body.get("order_value") is not None else ""),
        platform=(body.get("platform") or "").strip() or None,
        stream_label=(body.get("stream_label") or "").strip() or None,
        notes=(body.get("notes") or "").strip() or None,
        hit_at=_parse_hit_at(body.get("hit_at")),
        created_by=current_user_label(request),
        image_filename=img_fn,
    )
    session.add(hit)
    session.commit()
    session.refresh(hit)

    return JSONResponse({"ok": True, "id": hit.id, "hit": _hit_to_dict(hit)})


@router.get("/api/hits/recent")
def hits_api_recent(
    request: Request,
    limit: int = Query(default=5, ge=1, le=50),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    hits = session.exec(
        select(LiveHit)
        .where(LiveHit.is_deleted == False)
        .order_by(LiveHit.hit_at.desc())
        .limit(limit)
    ).all()

    return {"hits": [_hit_to_dict(h) for h in hits]}


# ---------------------------------------------------------------------------
# Hit image upload & serving
# ---------------------------------------------------------------------------

@router.post("/api/hits/upload-image")
async def hits_upload_image(
    request: Request,
    file: UploadFile = File(...),
):
    if denial := require_role_response(request, "viewer"):
        return denial

    content_type = (file.content_type or "").lower()
    if content_type not in ALLOWED_IMAGE_TYPES:
        return JSONResponse(
            {"ok": False, "error": f"File type '{content_type}' not allowed. Use JPEG, PNG, or WebP."},
            status_code=400,
        )

    data = await file.read()
    if len(data) > MAX_IMAGE_SIZE:
        return JSONResponse(
            {"ok": False, "error": "Image too large (max 10 MB)"},
            status_code=400,
        )

    ext_map = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/heic": ".heic",
        "image/heif": ".heif",
    }
    ext = ext_map.get(content_type, ".jpg")
    filename = f"{uuid.uuid4().hex}{ext}"

    HIT_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    (HIT_IMAGES_DIR / filename).write_bytes(data)

    return JSONResponse({"ok": True, "filename": filename})


@router.get("/hit-images/{filename}")
def serve_hit_image(filename: str):
    if ".." in filename or "/" in filename or "\\" in filename:
        return JSONResponse({"error": "Invalid filename"}, status_code=400)

    path = HIT_IMAGES_DIR / filename
    if not path.is_file():
        return JSONResponse({"error": "Not found"}, status_code=404)

    return FileResponse(path)
