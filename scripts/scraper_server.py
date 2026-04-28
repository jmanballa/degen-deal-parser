"""
TikTok Seller Center buyer-username resolver.

Runs on a Linux host (e.g. OpenClaw) with a persistent Playwright browser
context that has been pre-authenticated against TikTok Seller Center. Exposes
a small HTTP API that Machine B (or any other internal service) can call to
resolve an order_id -> buyer @username.

WHY THIS EXISTS
    TikTok Shop's Order API returns buyer_nickname and recipient_address.name
    but does NOT expose the buyer's @username. Streamers write @username on
    packing labels, so we need to scrape it from the Seller Center UI.

DEPLOY FLOW (on OpenClaw, one time)
    1. Install deps:          pip install -r scripts/scraper_requirements.txt
    2. Install browsers:      python -m playwright install chromium
    3. Pre-authenticate:      python scripts/scraper_login.py
                              (opens a headed browser so you can log in and
                               pass 2FA; saves storage_state.json)
    4. Set env in .env:       SCRAPER_API_KEY=some-long-random-string
                              SCRAPER_SELLER_CENTER_BASE=https://seller-us.tiktok.com
                              SCRAPER_ORDER_DETAIL_PATH=/order/detail?order_no={order_id}
    5. Run the server:        uvicorn scripts.scraper_server:app --host 0.0.0.0 --port 8787

DAY-TO-DAY USAGE (from Machine B or curl)
    curl -X POST http://openclaw:8787/resolve \\
         -H "X-API-Key: $SCRAPER_API_KEY" \\
         -H "Content-Type: application/json" \\
         -d '{"order_id": "577341910736147304"}'
    -> {"order_id": "...", "username": "@athena_collectibles", "cached": false}

FAILURE MODES
    - Session expired  -> 503 "session_expired", re-run scraper_login.py
    - Order not found  -> 404
    - Username element missing (TikTok UI changed) -> 502 with a hint in the
      response body pointing at SCRAPER_USERNAME_SELECTORS
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from playwright.async_api import (
        Browser,
        BrowserContext,
        Page,
        TimeoutError as PWTimeoutError,
        async_playwright,
    )
except ImportError as exc:
    raise RuntimeError(
        "Playwright is required. Install with: "
        "pip install playwright && python -m playwright install chromium"
    ) from exc


# ---------------------------------------------------------------------------
# Config (env-driven so OpenClaw deploys cleanly)
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
STATE_DIR = Path(os.environ.get("SCRAPER_STATE_DIR", PROJECT_ROOT / "data" / "scraper"))
STATE_DIR.mkdir(parents=True, exist_ok=True)

STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"
CACHE_DB_PATH = STATE_DIR / "username_cache.db"

SELLER_CENTER_BASE = os.environ.get(
    "SCRAPER_SELLER_CENTER_BASE", "https://seller-us.tiktok.com"
).rstrip("/")
ORDER_DETAIL_PATH = os.environ.get(
    "SCRAPER_ORDER_DETAIL_PATH", "/order/detail?order_no={order_id}"
)

# CSS selectors to try, in order. Seller Center occasionally changes these, so
# this is env-overridable (comma-separated) without touching code.
DEFAULT_USERNAME_SELECTORS = [
    "a[href*='/@']",
    "a[href*='tiktok.com/@']",
    "[data-testid='buyer-username']",
    ".buyer-username",
]
USERNAME_SELECTORS = [
    s.strip()
    for s in os.environ.get(
        "SCRAPER_USERNAME_SELECTORS", ",".join(DEFAULT_USERNAME_SELECTORS)
    ).split(",")
    if s.strip()
]

NAV_TIMEOUT_MS = int(os.environ.get("SCRAPER_NAV_TIMEOUT_MS", "20000"))
USERNAME_TIMEOUT_MS = int(os.environ.get("SCRAPER_USERNAME_TIMEOUT_MS", "8000"))
CACHE_TTL_SECONDS = int(os.environ.get("SCRAPER_CACHE_TTL_SECONDS", str(60 * 60 * 24 * 30)))
API_KEY = (os.environ.get("SCRAPER_API_KEY") or "").strip()
HEADLESS = os.environ.get("SCRAPER_HEADLESS", "true").lower() not in ("0", "false", "no")

logger = logging.getLogger("scraper_server")
logging.basicConfig(
    level=os.environ.get("SCRAPER_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


# ---------------------------------------------------------------------------
# Cache (SQLite -- persists across restarts)
# ---------------------------------------------------------------------------

def _cache_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(CACHE_DB_PATH))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS username_cache (
            order_id    TEXT PRIMARY KEY,
            username    TEXT NOT NULL,
            resolved_at INTEGER NOT NULL
        )
        """
    )
    return conn


def cache_get(order_id: str) -> Optional[str]:
    conn = _cache_connect()
    try:
        cutoff = int(time.time()) - CACHE_TTL_SECONDS
        row = conn.execute(
            "SELECT username FROM username_cache WHERE order_id = ? AND resolved_at >= ?",
            (order_id, cutoff),
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def cache_set(order_id: str, username: str) -> None:
    conn = _cache_connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO username_cache(order_id, username, resolved_at) VALUES (?, ?, ?)",
            (order_id, username, int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Browser session management
# ---------------------------------------------------------------------------

class BrowserSession:
    """Single long-lived Playwright browser + context, guarded by a lock.

    Seller Center is not a high-QPS target and we want to be polite, so we
    serialize scrapes. This also avoids races on the shared cookie jar.
    """

    def __init__(self) -> None:
        self._pw = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._lock = asyncio.Lock()
        self._session_valid = False

    async def start(self) -> None:
        if not STORAGE_STATE_PATH.exists():
            raise RuntimeError(
                f"No Seller Center session found at {STORAGE_STATE_PATH}. "
                "Run scripts/scraper_login.py first."
            )
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=HEADLESS)
        self._context = await self._browser.new_context(
            storage_state=str(STORAGE_STATE_PATH),
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
        )
        self._session_valid = True
        logger.info("Browser started (headless=%s) with storage state", HEADLESS)

    async def stop(self) -> None:
        try:
            if self._context:
                await self._context.close()
        except Exception:
            logger.exception("Error closing browser context")
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            logger.exception("Error closing browser")
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            logger.exception("Error stopping playwright")

    async def resolve_username(self, order_id: str) -> str:
        if not self._context:
            raise RuntimeError("Browser not started")
        if not self._session_valid:
            raise SessionExpiredError("Seller Center session expired")

        url = SELLER_CENTER_BASE + ORDER_DETAIL_PATH.format(order_id=order_id)

        async with self._lock:
            page: Page = await self._context.new_page()
            try:
                try:
                    await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
                except PWTimeoutError as exc:
                    raise ScrapeError(f"navigation timed out: {exc}") from exc

                final_url = page.url
                if "login" in final_url or "passport" in final_url:
                    self._session_valid = False
                    raise SessionExpiredError(
                        f"redirected to login page ({final_url}); re-run scraper_login.py"
                    )

                username = await self._extract_username(page)
                if not username:
                    if await self._looks_like_not_found(page):
                        raise OrderNotFoundError(order_id)
                    raise ScrapeError(
                        "username element not found on order detail page; "
                        "selectors may need updating via SCRAPER_USERNAME_SELECTORS"
                    )
                return username
            finally:
                await page.close()

    async def _extract_username(self, page: Page) -> Optional[str]:
        deadline = time.monotonic() + (USERNAME_TIMEOUT_MS / 1000.0)
        while time.monotonic() < deadline:
            for selector in USERNAME_SELECTORS:
                try:
                    el = await page.query_selector(selector)
                except Exception:
                    continue
                if not el:
                    continue
                href = await el.get_attribute("href") or ""
                text = (await el.inner_text() or "").strip()
                candidate = _parse_username(href) or _parse_username(text)
                if candidate:
                    return candidate
            await asyncio.sleep(0.25)
        return None

    @staticmethod
    async def _looks_like_not_found(page: Page) -> bool:
        try:
            body = await page.inner_text("body", timeout=500)
        except Exception:
            return False
        lowered = (body or "").lower()
        return any(
            needle in lowered
            for needle in ("order does not exist", "no such order", "order not found")
        )


def _parse_username(value: str) -> Optional[str]:
    """Extract @handle from a TikTok profile URL or raw text."""
    if not value:
        return None
    value = value.strip()
    if "/@" in value:
        tail = value.split("/@", 1)[1]
        handle = tail.split("/", 1)[0].split("?", 1)[0].strip()
        if handle:
            return "@" + handle
    if value.startswith("@") and len(value) > 1:
        head = value.split()[0]
        return head if len(head) > 1 else None
    return None


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ScrapeError(Exception):
    pass


class SessionExpiredError(ScrapeError):
    pass


class OrderNotFoundError(ScrapeError):
    def __init__(self, order_id: str) -> None:
        super().__init__(f"order {order_id} not found")
        self.order_id = order_id


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

class ResolveRequest(BaseModel):
    order_id: str = Field(..., min_length=1, max_length=64)


class ResolveResponse(BaseModel):
    order_id: str
    username: str
    cached: bool


session = BrowserSession()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not API_KEY:
        logger.warning(
            "SCRAPER_API_KEY is not set. The server will reject all requests. "
            "Set SCRAPER_API_KEY in the environment before exposing this service."
        )
    await session.start()
    try:
        yield
    finally:
        await session.stop()


app = FastAPI(
    title="TikTok Username Resolver",
    version="0.1.0",
    lifespan=lifespan,
)


def require_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    if not API_KEY:
        raise HTTPException(status_code=503, detail="server API key not configured")
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


@app.get("/health")
async def health() -> dict:
    return {
        "ok": True,
        "session_valid": session._session_valid,  # noqa: SLF001
        "storage_state_exists": STORAGE_STATE_PATH.exists(),
        "cache_path": str(CACHE_DB_PATH),
    }


@app.post("/resolve", response_model=ResolveResponse, dependencies=[Depends(require_api_key)])
async def resolve(req: ResolveRequest, request: Request) -> ResolveResponse:
    order_id = req.order_id.strip()
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id is required")

    cached = cache_get(order_id)
    if cached:
        return ResolveResponse(order_id=order_id, username=cached, cached=True)

    try:
        username = await session.resolve_username(order_id)
    except SessionExpiredError as exc:
        logger.error("session expired while resolving %s: %s", order_id, exc)
        raise HTTPException(status_code=503, detail=f"session_expired: {exc}") from exc
    except OrderNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ScrapeError as exc:
        logger.error("scrape error for %s: %s", order_id, exc)
        raise HTTPException(status_code=502, detail=f"scrape_error: {exc}") from exc
    except Exception as exc:
        logger.exception("unexpected error resolving %s", order_id)
        raise HTTPException(status_code=500, detail=f"internal_error: {exc}") from exc

    cache_set(order_id, username)
    logger.info("resolved %s -> %s", order_id, username)
    return ResolveResponse(order_id=order_id, username=username, cached=False)


@app.post("/cache/clear", dependencies=[Depends(require_api_key)])
async def cache_clear() -> dict:
    conn = _cache_connect()
    try:
        n = conn.execute("DELETE FROM username_cache").rowcount
        conn.commit()
    finally:
        conn.close()
    return {"cleared": n}
