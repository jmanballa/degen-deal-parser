import asyncio
import csv
import hashlib
import hmac
from html import escape
import json
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import distinct, func
from sqlalchemy.exc import OperationalError
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import Session, delete, select

from .auth import authenticate_user, has_role, seed_default_users
from .attachment_storage import attachment_cache_path, generate_thumbnail, warm_attachment_cache, write_attachment_cache_file
from .bookkeeping import (
    refresh_bookkeeping_import_from_source,
    import_bookkeeping_file,
    get_bookkeeping_status_by_message_ids,
    list_bookkeeping_imports,
    list_detected_bookkeeping_posts,
    reconcile_bookkeeping_import,
)
from .cache import cache_get, cache_invalidate, cache_set
from .backfill_requests import (
    backfill_request_loop,
    cancel_backfill_request,
    enqueue_backfill_request,
    list_recent_backfill_requests,
    requeue_interrupted_backfill_requests,
    trigger_backfill_claim_attempt,
)
from .channels import (
    get_available_channel_choices,
    get_channel_filter_choices,
    get_expense_category_filter_choices,
    get_watched_channels,
    normalize_channel_ids,
    update_backfill_window,
    upsert_watched_channel,
)
from .config import get_settings
from .corrections import (
    get_correction_pattern_counts,
    get_learning_signal,
    get_learning_signals,
    promote_correction_pattern,
    save_review_correction,
    snapshot_message_parse,
)
from .db import (
    dispose_engine,
    engine,
    get_session,
    init_db,
    is_sqlite_lock_error,
    managed_session,
    recent_db_failure,
    run_write_with_retry,
)
from .discord_ingest import (
    discord_runtime_state,
    get_discord_client,
    list_available_discord_channels,
    periodic_attachment_repair_loop,
    recent_message_audit_loop,
    parse_iso_datetime,
    recover_attachment_assets_for_message,
    run_discord_bot,
    seed_channels_from_env,
)
from .financials import compute_financials
from .models import (
    AppSetting,
    AttachmentAsset,
    BackfillRequest,
    BIG_HIT_THRESHOLD,
    BookkeepingImport,
    DiscordMessage,
    expand_parse_status_filter_values,
    LiveHit,
    STREAMER_COLORS,
    OperationsLog,
    ParseAttempt,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    PLATFORMS,
    ReparseRun,
    ShopifyOrder,
    StreamAccount,
    Streamer,
    STREAMERS,
    StreamSchedule,
    TikTokAuth,
    TikTokOrder,
    TikTokProduct,
    TikTokSyncState,
    User,
    WatchedChannel,
    normalize_money_value,
    normalize_parse_status,
    signed_money_delta,
    utcnow,
)
from .ops_log import count_recent_errors, list_operations_logs, list_operations_logs_for_backfill_request, parse_operations_log_details
from .reparse_runs import list_recent_reparse_runs, safe_create_reparse_run, safe_finalize_reparse_run_queue
from .reparse import reparse_message_row, reparse_message_rows
from .reporting import (
    build_financial_summary,
    build_reporting_periods,
    build_shopify_reporting_summary,
    build_tiktok_buyer_insights,
    build_tiktok_orders_page_data as build_tiktok_orders_page_reporting_data,
    build_tiktok_product_performance,
    build_tiktok_reporting_summary,
    classify_tiktok_reporting_status,
    get_financial_rows,
    get_shopify_reporting_rows,
    get_tiktok_reporting_rows,
    parse_report_datetime,
)
from .runtime_logging import resolve_runtime_log_path, setup_runtime_file_logging, structured_log_line
from .runtime_monitor import get_runtime_heartbeat_status, runtime_heartbeat_loop
from .schemas import HealthOut
from .shopify_ingest import (
    backfill_shopify_orders,
    mark_inventory_sold_from_shopify_order,
    read_shopify_backfill_state,
    update_shopify_backfill_state,
    upsert_shopify_order,
    validate_shopify_webhook,
)
from .display_media import (
    extract_image_urls,
    get_cached_attachment_map,
    merge_display_attachment_urls,
    normalize_attachment_urls_for_row,
    row_has_images,
)
from .transactions import build_transaction_summary, get_transactions, rebuild_transactions, sync_transaction_from_message
from .tiktok_auth_refresh import refresh_tiktok_auth_if_needed as _refresh_tiktok_auth_fn
from .tiktok_ingest import (
    TikTokIngestError,
    _build_webhook_signature_candidates,
    exchange_tiktok_authorization_code,
    parse_tiktok_webhook_headers,
    parse_tiktok_webhook_payload,
    upsert_tiktok_auth_from_callback,
    upsert_tiktok_order_from_payload,
)
from .tiktok_live_chat import (
    get_chat_status,
    get_recent_messages as get_live_chat_messages,
    get_room_id as get_live_room_id,
    get_stream_viewers,
    start_live_chat,
    stop_live_chat,
)
from .worker import (
    STALE_PROCESSING_AFTER,
    clear_parsed_fields,
    parser_loop,
    periodic_inventory_price_loop,
    periodic_stitch_audit_loop,
    queue_auto_reprocess_candidates,
    queue_reparse_range,
)
try:
    from scripts.tiktok_backfill import backfill_tiktok_orders as pull_tiktok_orders
    from scripts.tiktok_backfill import backfill_tiktok_products as pull_tiktok_products
    from scripts.tiktok_backfill import refresh_access_token as refresh_tiktok_access_token
    from scripts.tiktok_backfill import fetch_tiktok_order_details as _fetch_tiktok_order_details
    from scripts.tiktok_backfill import order_record_from_payload as _order_record_from_payload
    from scripts.tiktok_backfill import (
        fetch_tiktok_categories as _fetch_tiktok_categories,
        fetch_tiktok_category_attributes as _fetch_tiktok_category_attributes,
        fetch_tiktok_brands as _fetch_tiktok_brands,
        upload_tiktok_product_image as _upload_tiktok_product_image,
        create_tiktok_product as _create_tiktok_product,
        fetch_tiktok_product_detail as _fetch_tiktok_product_detail,
        product_record_from_payload as _product_record_from_payload,
        upsert_tiktok_product_row as _upsert_tiktok_product_row,
        fetch_tiktok_live_analytics as _fetch_tiktok_live_analytics,
        fetch_live_session_list as _fetch_live_session_list,
        fetch_overview_performance_daily as _fetch_overview_performance_daily,
        fetch_stream_performance_per_minutes as _fetch_stream_performance_per_minutes,
    )
except Exception:  # pragma: no cover - fallback if the script module is unavailable
    pull_tiktok_orders = None
    pull_tiktok_products = None
    refresh_tiktok_access_token = None
    _fetch_tiktok_order_details = None
    _order_record_from_payload = None
    _fetch_tiktok_categories = None
    _fetch_tiktok_category_attributes = None
    _fetch_tiktok_brands = None
    _upload_tiktok_product_image = None
    _create_tiktok_product = None
    _fetch_tiktok_product_detail = None
    _fetch_tiktok_live_analytics = None
    _fetch_live_session_list = None
    _fetch_overview_performance_daily = None
    _fetch_stream_performance_per_minutes = None
    _product_record_from_payload = None
    _upsert_tiktok_product_row = None

from .shared import *  # noqa: F401,F403 - shared helpers, constants, state

# Re-exports for backward compatibility with tests that import from app.main
from .routers.shopify import tiktok_oauth_callback  # noqa: F401
from .routers.tiktok_orders import (  # noqa: F401
    _collect_tiktok_orders_page_data,
    tiktok_orders_page,
    tiktok_orders_sync_form,
    tiktok_orders_webhook,
)
from .routers.reports import reports_page  # noqa: F401
from .routers.dashboard import dashboard_page  # noqa: F401
from .shared import (  # noqa: F401 - explicit imports for underscore-prefixed names
    _load_stream_range,
    _load_tiktok_state_from_db,
    _poll_tiktok_live_analytics,
)

settings = get_settings()
setup_runtime_file_logging("app.log")

async def lifespan(app: FastAPI):
    if settings.employee_portal_enabled:
        # Imported for side-effect: fail-closed key validation at startup.
        from . import pii as _pii  # noqa: F401
        print("[main] employee portal: enabled")
    else:
        print("[main] employee portal: disabled")
    init_db()
    _load_stream_range()
    _load_tiktok_state_from_db()
    with managed_session() as session:
        seed_default_users(session)
        requeue_interrupted_backfill_requests(session)
    seed_channels_from_env()
    reset_background_task_failures()

    stop_event = asyncio.Event()
    app.state.stop_event = stop_event
    heartbeat_stop_event = threading.Event()
    app.state.heartbeat_stop_event = heartbeat_stop_event
    app_heartbeat_thread = threading.Thread(
        target=runtime_heartbeat_loop,
        kwargs={
            "stop_event": heartbeat_stop_event,
            "runtime_name": APP_HEARTBEAT_RUNTIME_NAME,
            "host_name": socket.gethostname(),
            "details_provider": app_runtime_details,
        },
        name="app-heartbeat",
        daemon=True,
    )
    app_heartbeat_thread.start()
    app.state.app_heartbeat_thread = app_heartbeat_thread

    background_tasks: list[asyncio.Task] = []

    if settings.discord_ingest_enabled or settings.parser_worker_enabled:
        heartbeat_thread = threading.Thread(
            target=runtime_heartbeat_loop,
            kwargs={
                "stop_event": heartbeat_stop_event,
                "runtime_name": WORKER_RUNTIME_NAME,
                "host_name": socket.gethostname(),
                "details_provider": local_runtime_details,
            },
            name="local-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()
        app.state.heartbeat_thread = heartbeat_thread
    else:
        app.state.heartbeat_thread = None

    if settings.discord_ingest_enabled:
        discord_task = track_background_task(
            asyncio.create_task(run_discord_bot(stop_event), name="discord-ingest"),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="discord-ingest",
            stop_event=stop_event,
        )
        background_tasks.append(discord_task)
        app.state.discord_task = discord_task
        backfill_task = track_background_task(
            asyncio.create_task(
                backfill_request_loop(stop_event, get_discord_client),
                name="backfill-queue",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="backfill-queue",
            stop_event=stop_event,
        )
        background_tasks.append(backfill_task)
        app.state.backfill_task = backfill_task
        recent_audit_task = track_background_task(
            asyncio.create_task(
                recent_message_audit_loop(stop_event, get_discord_client),
                name="recent-message-audit",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="recent-message-audit",
            stop_event=stop_event,
        )
        background_tasks.append(recent_audit_task)
        app.state.recent_audit_task = recent_audit_task
        if settings.periodic_attachment_repair_enabled:
            attachment_repair_task = track_background_task(
                asyncio.create_task(
                    periodic_attachment_repair_loop(stop_event, get_discord_client),
                    name="attachment-repair-audit",
                ),
                runtime_name=WORKER_RUNTIME_NAME,
                task_name="attachment-repair-audit",
                stop_event=stop_event,
            )
            background_tasks.append(attachment_repair_task)
            app.state.attachment_repair_task = attachment_repair_task
        else:
            app.state.attachment_repair_task = None
    else:
        app.state.discord_task = None
        app.state.backfill_task = None
        app.state.recent_audit_task = None
        app.state.attachment_repair_task = None

    if settings.discord_ingest_enabled and settings.parser_worker_enabled:
        stitch_audit_task = track_background_task(
            asyncio.create_task(
                periodic_stitch_audit_loop(stop_event),
                name="stitch-audit",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="stitch-audit",
            stop_event=stop_event,
        )
        background_tasks.append(stitch_audit_task)
        app.state.stitch_audit_task = stitch_audit_task
    else:
        app.state.stitch_audit_task = None

    if settings.parser_worker_enabled:
        worker_task = track_background_task(
            asyncio.create_task(parser_loop(stop_event), name="parser-worker"),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="parser-worker",
            stop_event=stop_event,
        )
        background_tasks.append(worker_task)
        app.state.worker_task = worker_task
    else:
        app.state.worker_task = None
        print("[worker] parser worker disabled by configuration")

    if (settings.tiktok_app_key or "").strip() and (settings.tiktok_app_secret or "").strip():
        tiktok_pull_task = track_background_task(
            asyncio.create_task(
                periodic_tiktok_pull_loop(stop_event),
                name="tiktok-order-pull",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="tiktok-order-pull",
            stop_event=stop_event,
        )
        background_tasks.append(tiktok_pull_task)
        app.state.tiktok_pull_task = tiktok_pull_task
        tiktok_backfill_task = asyncio.create_task(
            tiktok_startup_backfill(stop_event),
            name="tiktok-startup-backfill",
        )
        background_tasks.append(tiktok_backfill_task)
        app.state.tiktok_backfill_task = tiktok_backfill_task
    else:
        app.state.tiktok_pull_task = None
        app.state.tiktok_backfill_task = None

    live_analytics_stop = threading.Event()
    app.state.live_analytics_stop = live_analytics_stop
    live_analytics_thread = threading.Thread(
        target=_poll_tiktok_live_analytics,
        args=(live_analytics_stop,),
        name="tiktok-live-analytics",
        daemon=True,
    )
    live_analytics_thread.start()
    app.state.live_analytics_thread = live_analytics_thread

    live_chat_username = (settings.tiktok_live_username or "").strip()
    live_chat_api_key = (settings.tiktok_live_api_key or "").strip()
    if live_chat_username and live_chat_api_key:
        live_chat_task = asyncio.create_task(
            start_live_chat(live_chat_username, live_chat_api_key),
            name="tiktok-live-chat",
        )
        background_tasks.append(live_chat_task)
        app.state.live_chat_task = live_chat_task
        print(f"[tiktok-live-chat] starting for @{live_chat_username}")
    else:
        app.state.live_chat_task = None

    # Inventory pricing refresh background loop
    if settings.inventory_auto_price_enabled:
        inv_price_task = track_background_task(
            asyncio.create_task(
                periodic_inventory_price_loop(stop_event),
                name="inventory-price-refresh",
            ),
            runtime_name=WORKER_RUNTIME_NAME,
            task_name="inventory-price-refresh",
            stop_event=stop_event,
        )
        background_tasks.append(inv_price_task)
        app.state.inv_price_task = inv_price_task
    else:
        app.state.inv_price_task = None

    yield

    stop_event.set()
    heartbeat_stop_event.set()
    live_analytics_stop.set()
    await stop_live_chat()

    if background_tasks:
        done, pending = await asyncio.wait(background_tasks, timeout=10)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if done:
            await asyncio.gather(*done, return_exceptions=True)
    heartbeat_thread = getattr(app.state, "heartbeat_thread", None)
    if heartbeat_thread:
        heartbeat_thread.join(timeout=5)
    app_heartbeat_thread = getattr(app.state, "app_heartbeat_thread", None)
    if app_heartbeat_thread:
        app_heartbeat_thread.join(timeout=5)

app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    session_cookie=settings.session_cookie_name,
    max_age=settings.session_max_age_seconds,
    https_only=settings.session_https_only,
    same_site=settings.session_same_site,
    domain=settings.effective_session_domain or None,
)
app.mount("/static", StaticFiles(directory=normalize_filesystem_path(BASE_DIR / "static")), name="static")

from .inventory import router as inventory_router  # noqa: E402 — after app is created
app.include_router(inventory_router)

from .routers.stream_manager import router as stream_manager_router  # noqa: E402
app.include_router(stream_manager_router)

from .routers.bookkeeping import router as bookkeeping_router  # noqa: E402
app.include_router(bookkeeping_router)

from .routers.hits import router as hits_router  # noqa: E402
app.include_router(hits_router)

from .routers.tiktok_products import router as tiktok_products_router  # noqa: E402
app.include_router(tiktok_products_router)

from .routers.tiktok_analytics import router as tiktok_analytics_router  # noqa: E402
app.include_router(tiktok_analytics_router)

from .routers.tiktok_streamer import router as tiktok_streamer_router  # noqa: E402
app.include_router(tiktok_streamer_router)

from .routers.tiktok_orders import router as tiktok_orders_router  # noqa: E402
app.include_router(tiktok_orders_router)

from .routers.reports import router as reports_router  # noqa: E402
app.include_router(reports_router)

from .routers.shopify import router as shopify_router  # noqa: E402
app.include_router(shopify_router)

from .routers.dashboard import router as dashboard_router  # noqa: E402
app.include_router(dashboard_router)

from .routers.deals import router as deals_router  # noqa: E402
app.include_router(deals_router)

from .routers.channels_api import router as channels_api_router  # noqa: E402
app.include_router(channels_api_router)

from .routers.admin import router as admin_router  # noqa: E402
app.include_router(admin_router)

from .routers.admin_actions import router as admin_actions_router  # noqa: E402
app.include_router(admin_actions_router)

from .routers.messages import router as messages_router  # noqa: E402
app.include_router(messages_router)

@app.exception_handler(OperationalError)
async def handle_operational_error(request: Request, exc: OperationalError):
    dispose_engine()
    error_text = str(exc).lower()
    is_sqlite_busy = "database is locked" in error_text or "sqlite_busy" in error_text
    if is_sqlite_busy:
        payload = {
            "ok": False,
            "error": "SQLite is temporarily busy.",
            "detail": "The local database is handling another write right now. Please retry in a few seconds.",
        }
        html_message = (
            '<meta http-equiv="refresh" content="3">'
            "<h1>SQLite temporarily busy</h1>"
            "<p>The local database is handling another write right now. Retrying automatically&hellip;</p>"
        )
    else:
        payload = {
            "ok": False,
            "error": "Database connection is temporarily unavailable.",
            "detail": "The shared database did not accept the connection cleanly. Please retry in a few seconds.",
        }
        html_message = (
            "<h1>Database temporarily unavailable</h1>"
            "<p>The shared database connection dropped unexpectedly. Please retry in a few seconds.</p>"
        )
    headers = {"Retry-After": "5"}
    wants_json = (
        request.url.path.startswith("/admin/parser-progress")
        or request.url.path.startswith("/health")
        or "application/json" in request.headers.get("accept", "")
    )
    if wants_json:
        return JSONResponse(status_code=503, content=payload, headers=headers)
    return HTMLResponse(html_message, status_code=503, headers=headers)

@app.get("/attachments/{asset_id}")
def attachment_asset(request: Request, asset_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    asset_meta = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.filename, AttachmentAsset.content_type)
        .where(AttachmentAsset.id == asset_id)
    ).first()
    if not asset_meta:
        raise HTTPException(status_code=404, detail="Attachment not found")

    _, filename, content_type = asset_meta

    etag = f'"{asset_id}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match.strip() == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "public, max-age=31536000, immutable"})

    file_path = attachment_cache_path(
        asset_id,
        filename=filename,
        content_type=content_type,
    )
    if not file_path.exists():
        asset = session.get(AttachmentAsset, asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Attachment not found")
        file_path = write_attachment_cache_file(
            asset_id,
            filename=asset.filename,
            content_type=asset.content_type,
            data=asset.data,
        )

    media_type = content_type or "application/octet-stream"
    headers = {
        "Cache-Control": "public, max-age=31536000, immutable",
        "ETag": etag,
    }
    if filename:
        headers["Content-Disposition"] = f'inline; filename="{filename}"'
    return FileResponse(path=file_path, media_type=media_type, headers=headers)

@app.get("/attachments/{asset_id}/thumb")
def attachment_thumbnail(request: Request, asset_id: int, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "viewer"):
        return denial
    etag = f'"thumb-{asset_id}"'
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and if_none_match.strip() == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": "public, max-age=31536000, immutable"})

    asset_meta = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.filename, AttachmentAsset.content_type, AttachmentAsset.is_image)
        .where(AttachmentAsset.id == asset_id)
    ).first()
    if not asset_meta:
        raise HTTPException(status_code=404, detail="Attachment not found")

    _, filename, content_type, is_image = asset_meta
    if not is_image:
        return RedirectResponse(url=f"/attachments/{asset_id}", status_code=307)

    file_path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
    if not file_path.exists():
        asset = session.get(AttachmentAsset, asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Attachment not found")
        file_path = write_attachment_cache_file(
            asset_id, filename=asset.filename, content_type=asset.content_type, data=asset.data,
        )

    thumb_path = generate_thumbnail(file_path, asset_id)
    if thumb_path and thumb_path.exists():
        return FileResponse(
            path=thumb_path,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=31536000, immutable", "ETag": etag},
        )
    return FileResponse(
        path=file_path,
        media_type=content_type or "application/octet-stream",
        headers={"Cache-Control": "public, max-age=31536000, immutable", "ETag": etag},
    )

@app.get("/messages/{message_id}/attachments/{attachment_index}")
async def message_attachment_fallback(
    request: Request,
    message_id: int,
    attachment_index: int,
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "viewer"):
        return denial
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    attachment_urls = json.loads(row.attachment_urls_json or "[]")
    if attachment_index < 0 or attachment_index >= len(attachment_urls):
        raise HTTPException(status_code=404, detail="Attachment not found")

    cached_assets = session.exec(
        select(AttachmentAsset.id)
        .where(AttachmentAsset.message_id == message_id)
        .order_by(AttachmentAsset.id.asc())
    ).all()
    if attachment_index < len(cached_assets):
        asset_id = cached_assets[attachment_index][0]
        return RedirectResponse(url=f"/attachments/{asset_id}", status_code=307)

    recovered = await recover_attachment_assets_for_message(
        channel_id=row.channel_id,
        discord_message_id=row.discord_message_id,
        message_row_id=message_id,
    )
    if recovered:
        refreshed_assets = session.exec(
            select(AttachmentAsset.id)
            .where(AttachmentAsset.message_id == message_id)
            .order_by(AttachmentAsset.id.asc())
        ).all()
        if attachment_index < len(refreshed_assets):
            asset_id = refreshed_assets[attachment_index][0]
            return RedirectResponse(url=f"/attachments/{asset_id}", status_code=307)

    return RedirectResponse(url=attachment_urls[attachment_index], status_code=307)

@app.middleware("http")
async def attach_current_user(request: Request, call_next):
    if request.url.path.startswith(PUBLIC_PATH_PREFIXES):
        request.state.current_user = None
        return await call_next(request)
    request.state.current_user = get_request_user(request)
    return await call_next(request)

@app.get("/health", response_model=HealthOut)
def health():
    try:
        with managed_session() as session:
            db_health = get_database_health(session)
            local_runtime = get_runtime_heartbeat_status(
                session,
                APP_HEARTBEAT_RUNTIME_NAME,
                runtime_label=APP_RUNTIME_LABEL,
                updated_at_formatter=format_pacific_datetime,
            )
        return HealthOut(
            ok=bool(db_health["ok"]),
            db_ok=bool(db_health["ok"]),
            local_runtime_status=local_runtime["status"],
            local_runtime_label=local_runtime["label"],
            local_runtime_needs_attention=local_runtime["needs_attention"],
            local_runtime_updated_at=local_runtime["updated_at"],
        )
    except Exception as exc:
        return HealthOut(
            ok=False,
            db_ok=False,
            local_runtime_status="unknown",
            local_runtime_label="Unknown",
            local_runtime_needs_attention=True,
            error=str(exc),
        )


