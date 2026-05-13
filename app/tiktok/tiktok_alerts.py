"""
TikTok order alert notifications via Telegram.

Sends a message to the configured Telegram chat/topic whenever a
REVERSE_ORDER_STATUS_CHANGE or CANCELLATION_STATUS_CHANGE webhook is received.
No external dependencies beyond httpx (already in requirements).
"""
from __future__ import annotations

import os
import threading
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Config — reads from env vars set in .env
# ---------------------------------------------------------------------------
# Required: TELEGRAM_BOT_TOKEN, TELEGRAM_ALERT_CHAT_ID
# Optional: TELEGRAM_ALERT_TOPIC_ID  (for supergroup topics)

def _cfg(key: str, default: str = "") -> str:
    return (os.environ.get(key) or "").strip() or default


def _send_telegram(text: str) -> None:
    """Fire-and-forget Telegram sendMessage in a daemon thread."""

    def _do():
        token = _cfg("TELEGRAM_BOT_TOKEN")
        chat_id = _cfg("TELEGRAM_ALERT_CHAT_ID")
        if not token or not chat_id:
            print(f"[tiktok_alerts] Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID missing)")
            return
        try:
            import httpx
            params: dict = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            }
            topic_id = _cfg("TELEGRAM_ALERT_TOPIC_ID")
            if topic_id:
                params["message_thread_id"] = topic_id

            resp = httpx.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=params,
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"[tiktok_alerts] Telegram send failed: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:
            print(f"[tiktok_alerts] Telegram send error: {exc}")

    t = threading.Thread(target=_do, daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# Public alert functions
# ---------------------------------------------------------------------------

def alert_supply_request(
    *,
    request_id: Optional[int],
    employee_name: str = "",
    employee_username: str = "",
    title: str = "",
    description: str = "",
    urgency: str = "normal",
) -> None:
    """Alert when an employee submits a supply request."""
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    employee = str(employee_name or "").strip() or str(employee_username or "").strip() or "Unknown employee"
    username = str(employee_username or "").strip()
    title_str = str(title or "").strip() or "Untitled request"
    urgency_str = str(urgency or "normal").strip().lower() or "normal"
    description_str = str(description or "").strip()
    if len(description_str) > 700:
        description_str = description_str[:697].rstrip() + "…"
    request_part = f"\n🆔 Request: <code>{request_id}</code>" if request_id is not None else ""
    username_part = f" (@{_esc(username)})" if username else ""
    description_part = f"\n📝 {_esc(description_str)}" if description_str else ""

    text = (
        f"🧾 <b>New Supply Request</b>\n"
        f"🕐 {now}\n"
        f"👤 {_esc(employee)}{username_part}\n"
        f"⚡ Urgency: <b>{_esc(urgency_str)}</b>\n"
        f"📦 {_esc(title_str)}"
        f"{description_part}"
        f"{request_part}\n"
        f'\n<a href="{_esc(_team_supply_queue_url())}">Open supply queue</a>'
    )
    _send_telegram(text)


def _team_supply_queue_url() -> str:
    base = _cfg("PUBLIC_BASE_URL", "").rstrip("/")
    if not base:
        return "https://ops.degencollectibles.com/team/admin/supply"
    return f"{base}/team/admin/supply"


def alert_reverse_order(
    tiktok_order_id: str,
    customer_name: str = "",
    total_price: Optional[float] = None,
    order_status: str = "",
    upsert_status: str = "",
) -> None:
    """Alert when a REVERSE_ORDER_STATUS_CHANGE webhook is received."""
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    price_str = f"${total_price:.2f}" if total_price is not None else "unknown amount"
    customer_str = str(customer_name or "").strip() or "Unknown customer"
    status_str = str(order_status or "").strip() or "unknown"

    text = (
        f"⚠️ <b>TikTok Order REVERSED</b>\n"
        f"🕐 {now}\n"
        f"👤 {_esc(customer_str)}\n"
        f"💰 {price_str}\n"
        f"📋 Status: {_esc(status_str)}\n"
        f"🔢 Order ID: <code>{_esc(tiktok_order_id)}</code>\n"
        f"\n<i>TikTok reversed this order on their backend — check if it needs manual follow-up.</i>"
    )
    _send_telegram(text)


def alert_ghost_cancellation(
    event_type_name: str = "CANCELLATION_STATUS_CHANGE",
    body_sha256: str = "",
) -> None:
    """Alert when a CANCELLATION webhook arrives with no order ID."""
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    event_type_str = str(event_type_name or "CANCELLATION_STATUS_CHANGE")
    body_sha256_str = str(body_sha256 or "")
    text = (
        f"⚠️ <b>TikTok Ghost Cancellation</b>\n"
        f"🕐 {now}\n"
        f"❓ TikTok sent a <b>{_esc(event_type_str)}</b> webhook with <b>no order ID</b>.\n"
        f"\n<i>An order may have been cancelled on TikTok's backend but we can't tell which one. "
        f"Check Seller Center for recent cancellations.</i>\n"
        f"<code>sha256: {body_sha256_str[:16]}…</code>"
    )
    _send_telegram(text)


def _esc(s: str) -> str:
    """Minimal HTML escaping for Telegram HTML parse mode."""
    s = str(s or "")
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
