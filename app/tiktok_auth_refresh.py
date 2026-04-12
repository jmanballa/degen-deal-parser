"""Background TikTok token refresh logic.

Extracted from main.py so it can be called from both the FastAPI app
(main.py) and the background worker service (worker_service.py) without
circular imports.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import timedelta, timezone
from typing import Any, Optional

import httpx
from sqlmodel import Session

from .config import get_settings
from .models import TikTokAuth, utcnow
from .tiktok_ingest import (
    TIKTOK_DEFAULT_API_BASE_URL,
    TIKTOK_TOKEN_REFRESH_PATH,
    build_tiktok_api_url,
    upsert_tiktok_auth_from_callback,
)

try:
    from scripts.tiktok_backfill import refresh_access_token as _refresh_fn
except Exception:
    _refresh_fn = None

settings = get_settings()

# Serializes shop + creator refresh so two tasks never consume the same refresh token
# or interleave commits on the shared TikTokAuth row.
_tiktok_auth_refresh_lock = threading.Lock()


def refresh_tiktok_auth_if_needed(
    session: Session,
    *,
    runtime_name: str,
    force: bool = False,
    resolve_base_url: Callable[[], str],
    update_state: Optional[Callable[..., None]] = None,
) -> Optional[dict]:
    """Refresh the TikTok access token if it is close to expiry.

    Parameters
    ----------
    resolve_base_url:
        Callable that returns the TikTok Shop API base URL.
        Passed in to avoid importing from main.py.
    update_state:
        Optional callable to update the in-memory TikTok integration state
        (main.update_tiktok_integration_state).  Safe to omit when called
        from a background thread where the state dict is not needed.
    """
    if _refresh_fn is None:
        return None

    if not _tiktok_auth_refresh_lock.acquire(blocking=False):
        return None

    try:
        from sqlmodel import select  # local import to keep top-level imports minimal

        auth_row: Optional[TikTokAuth] = session.exec(
            select(TikTokAuth).order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())
        ).first()
        if auth_row is None:
            return None

        app_key = (settings.tiktok_app_key or auth_row.app_key or "").strip()
        app_secret = (settings.tiktok_app_secret or "").strip()
        refresh_token = (auth_row.refresh_token or settings.tiktok_refresh_token or "").strip()
        if not app_key or not app_secret or not refresh_token:
            return None

        token_expires_at = auth_row.access_token_expires_at
        if token_expires_at is not None and token_expires_at.tzinfo is None:
            token_expires_at = token_expires_at.replace(tzinfo=timezone.utc)
        should_refresh = (
            force
            or not auth_row.access_token
            or not token_expires_at
            or token_expires_at <= utcnow() + timedelta(minutes=10)
        )
        if not should_refresh:
            return None

        with httpx.Client(timeout=40.0, follow_redirects=True) as client:
            refreshed = _refresh_fn(
                client,
                base_url=resolve_base_url(),
                app_key=app_key,
                app_secret=app_secret,
                refresh_token=refresh_token,
            )

        token_data = refreshed
        if isinstance(refreshed, dict) and isinstance(refreshed.get("data"), dict):
            token_data = refreshed["data"]

        status, auth_record = upsert_tiktok_auth_from_callback(
            session,
            TikTokAuth,
            token_result=token_data,
            app_key=app_key,
            redirect_uri=(auth_row.redirect_uri or settings.tiktok_redirect_uri or "").strip(),
            fallback_shop_id=(settings.tiktok_shop_id or auth_row.tiktok_shop_id or "").strip(),
            source="oauth_refresh",
            received_at=utcnow(),
            dry_run=False,
        )

        if update_state is not None:
            update_state(
                is_pull_running=False,
                last_pull_started_at=utcnow(),
                last_pull_finished_at=utcnow(),
                last_error=None,
                last_pull_at=utcnow(),
                last_pull={
                    "status": "refresh",
                    "auth_status": status,
                    "shop_id": auth_record.get("tiktok_shop_id"),
                    "runtime": runtime_name,
                },
            )

        session.commit()
        return {"status": status, "auth_record": auth_record}
    finally:
        _tiktok_auth_refresh_lock.release()


def _parse_oauth_token_field(payload: dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        raw = payload.get(key)
        if raw in (None, ""):
            continue
        text = str(raw).strip()
        if text:
            return text
    return None


def _parse_expires_in_seconds(payload: dict[str, Any]) -> int:
    raw = payload.get("expires_in")
    if raw in (None, ""):
        raw = payload.get("expiresIn")
    if raw in (None, ""):
        return 86400
    try:
        return max(int(raw), 1)
    except (TypeError, ValueError):
        return 86400


def refresh_tiktok_creator_token_if_needed(
    session: Session,
    *,
    runtime_name: str,
    force: bool = False,
) -> Optional[dict]:
    """Refresh the TikTok Creator (Open API) access token if close to expiry.

    Uses POST ``/v2/oauth/token/`` on ``open.tiktokapis.com`` with
    ``grant_type=refresh_token`` (not the Shop auth host).
    """
    if not _tiktok_auth_refresh_lock.acquire(blocking=False):
        return None

    try:
        from sqlmodel import select

        auth_row: Optional[TikTokAuth] = session.exec(
            select(TikTokAuth).order_by(TikTokAuth.updated_at.desc(), TikTokAuth.id.desc())
        ).first()
        if auth_row is None:
            return None

        app_key = (settings.tiktok_app_key or auth_row.app_key or "").strip()
        app_secret = (settings.tiktok_app_secret or "").strip()
        creator_refresh = (auth_row.creator_refresh_token or "").strip()
        if not app_key or not app_secret or not creator_refresh:
            return None

        creator_expires = auth_row.creator_token_expires_at
        if creator_expires is not None and creator_expires.tzinfo is None:
            creator_expires = creator_expires.replace(tzinfo=timezone.utc)
        should_refresh = (
            force
            or not auth_row.creator_access_token
            or not creator_expires
            or creator_expires <= utcnow() + timedelta(minutes=10)
        )
        if not should_refresh:
            return None

        api_base = (settings.tiktok_api_base_url or "").strip() or TIKTOK_DEFAULT_API_BASE_URL
        token_url = build_tiktok_api_url(api_base_url=api_base, path=TIKTOK_TOKEN_REFRESH_PATH)
        form = {
            "client_key": app_key,
            "client_secret": app_secret,
            "grant_type": "refresh_token",
            "refresh_token": creator_refresh,
        }

        with httpx.Client(timeout=40.0, follow_redirects=True) as client:
            response = client.post(
                token_url,
                data=form,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict):
                raise RuntimeError(f"Creator token refresh: expected JSON object, got {type(body).__name__}")

        if body.get("error"):
            desc = body.get("error_description") or body.get("message") or body.get("error")
            raise RuntimeError(f"Creator token refresh failed: {desc}")

        access_token = _parse_oauth_token_field(body, "access_token", "accessToken")
        if not access_token:
            raise RuntimeError("Creator token refresh: missing access_token in response")

        new_refresh = _parse_oauth_token_field(body, "refresh_token", "refreshToken")
        if new_refresh:
            auth_row.creator_refresh_token = new_refresh

        auth_row.creator_access_token = access_token
        auth_row.creator_token_expires_at = utcnow() + timedelta(seconds=_parse_expires_in_seconds(body))
        auth_row.updated_at = utcnow()
        session.add(auth_row)
        session.commit()

        return {
            "status": "creator_refreshed",
            "runtime": runtime_name,
            "expires_at": auth_row.creator_token_expires_at.isoformat()
            if auth_row.creator_token_expires_at
            else None,
        }
    finally:
        _tiktok_auth_refresh_lock.release()
