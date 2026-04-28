"""
Thin client for the external TikTok username-resolver service.

The service itself lives at scripts/scraper_server.py and is expected to run
on OpenClaw (or any Linux host with a pre-authenticated Seller Center
session). This module is deliberately small: no background workers, no state,
no batching -- just a single `resolve_username(order_id)` call.

The caller is responsible for:
    - deciding when to enrich an order
    - persisting the resolved username (e.g. onto TikTokOrder)
    - tolerating failures (503 session_expired, 502 scrape_error, etc.)

When USERNAME_SCRAPER_BASE_URL is empty this module is a no-op -- every call
returns None -- so it's safe to wire in without any deploy-time coordination.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from .config import get_settings

logger = logging.getLogger(__name__)


class UsernameScraperError(Exception):
    """Any failure talking to the scraper service."""


class UsernameScraperSessionExpired(UsernameScraperError):
    """Scraper reported its Seller Center session is no longer valid."""


class UsernameScraperNotFound(UsernameScraperError):
    """Scraper could not locate the order in Seller Center."""


def is_enabled() -> bool:
    s = get_settings()
    return bool((s.username_scraper_base_url or "").strip() and (s.username_scraper_api_key or "").strip())


def resolve_username(order_id: str, *, timeout: Optional[float] = None) -> Optional[str]:
    """Return the buyer @username for the given order_id, or None on config-miss.

    Raises UsernameScraperSessionExpired / UsernameScraperNotFound /
    UsernameScraperError for service-reported failures so the caller can react
    appropriately (alert, skip, retry later).
    """
    order_id = (order_id or "").strip()
    if not order_id:
        return None

    s = get_settings()
    base = (s.username_scraper_base_url or "").strip().rstrip("/")
    key = (s.username_scraper_api_key or "").strip()
    if not base or not key:
        return None

    url = f"{base}/resolve"
    effective_timeout = timeout if timeout is not None else s.username_scraper_timeout_seconds

    try:
        with httpx.Client(timeout=effective_timeout) as client:
            resp = client.post(
                url,
                json={"order_id": order_id},
                headers={"X-API-Key": key, "Content-Type": "application/json"},
            )
    except httpx.HTTPError as exc:
        raise UsernameScraperError(f"network error calling {url}: {exc}") from exc

    if resp.status_code == 200:
        data = resp.json()
        username = (data.get("username") or "").strip() if isinstance(data, dict) else ""
        return username or None

    # Structured error codes the server speaks
    detail = ""
    try:
        detail = (resp.json() or {}).get("detail", "") or ""
    except Exception:
        detail = resp.text[:200]

    if resp.status_code == 503 and "session_expired" in detail:
        raise UsernameScraperSessionExpired(detail)
    if resp.status_code == 404:
        raise UsernameScraperNotFound(detail or f"order {order_id} not found")
    raise UsernameScraperError(f"{resp.status_code} {detail}")


__all__ = [
    "is_enabled",
    "resolve_username",
    "UsernameScraperError",
    "UsernameScraperSessionExpired",
    "UsernameScraperNotFound",
]
