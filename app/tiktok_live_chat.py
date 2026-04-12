"""TikTok Live chat integration via TikSync SDK.

Manages a WebSocket connection to TikTok Live, buffering chat/gift/follow
events in an in-memory ring buffer that the streamer dashboard can poll.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

_client: Any = None
_buffer: deque[dict] = deque(maxlen=200)
_buffer_lock = threading.Lock()
_msg_index: int = 0
_status: str = "not_configured"
_viewer_count: int = 0
_room_id: Optional[str] = None
_stop_requested = False

logger = logging.getLogger(__name__)

_viewers: dict[str, dict] = {}  # username -> {"joined_at": float, "last_active_at": float}
_viewers_lock = threading.Lock()
_DEFAULT_PRESENCE_TIMEOUT = 1800  # 30 min default — configurable via AppSetting
_DEFAULT_ACTIVE_THRESHOLD = 300   # 5 min — "chatting" vs "watching" boundary


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _viewer_join(user: str) -> None:
    """Record that a user entered the stream."""
    if not user or user == "???":
        return
    now = time.monotonic()
    with _viewers_lock:
        if user not in _viewers:
            _viewers[user] = {"joined_at": now, "last_active_at": now}


def _viewer_activity(user: str) -> None:
    """Record chat/gift/like/etc activity from a user."""
    if not user or user == "???":
        return
    now = time.monotonic()
    with _viewers_lock:
        entry = _viewers.get(user)
        if entry:
            entry["last_active_at"] = now
        else:
            _viewers[user] = {"joined_at": now, "last_active_at": now}


def _clear_viewers() -> None:
    """Clear all viewer presence — called when stream ends or chat disconnects."""
    with _viewers_lock:
        _viewers.clear()


def _append_message(msg_type: str, user: str, text: str) -> None:
    global _msg_index
    _viewer_activity(user)
    with _buffer_lock:
        _msg_index += 1
        _buffer.append({
            "idx": _msg_index,
            "type": msg_type,
            "user": user,
            "text": text,
            "ts": _now_iso(),
        })


async def start_live_chat(username: str, api_key: str) -> None:
    """Connect to TikTok Live chat via TikSync. Blocks until disconnected."""
    global _client, _status, _viewer_count, _stop_requested

    _stop_requested = False

    try:
        from tiksync import TikSync
    except ImportError:
        print("[tiktok-live-chat] tiksync package not installed, chat disabled")
        _status = "not_configured"
        return

    _status = "connecting"
    client = TikSync(
        username,
        api_key=api_key,
        max_reconnect_attempts=50,
        reconnect_delay=5.0,
    )
    _client = client

    def _try_capture_room_id(data):
        global _room_id
        if not isinstance(data, dict):
            return
        for key in ("roomId", "room_id", "liveRoomId", "live_room_id"):
            val = data.get(key)
            if val and str(val).strip():
                _room_id = str(val).strip()
                print(f"[tiktok-live-chat] captured room_id={_room_id}")
                return

    def on_connected(data):
        global _status
        _status = "connected"
        _try_capture_room_id(data)
        uid = (data or {}).get("uniqueId", username)
        print(f"[tiktok-live-chat] connected to {uid}")

    def on_chat(data):
        user = (data or {}).get("uniqueId", "???")
        comment = (data or {}).get("comment", "")
        _append_message("chat", user, comment)

    def on_gift(data):
        user = (data or {}).get("uniqueId", "???")
        gift_name = (data or {}).get("giftName", "Gift")
        count = (data or {}).get("repeatCount", 1)
        diamond = (data or {}).get("diamondCount", 0)
        text = f"sent {gift_name} x{count}"
        if diamond:
            text += f" ({diamond} diamonds)"
        _append_message("gift", user, text)

    def on_follow(data):
        user = (data or {}).get("uniqueId", "???")
        _append_message("follow", user, "followed!")

    def on_like(data):
        user = (data or {}).get("uniqueId", "???")
        count = (data or {}).get("likeCount", (data or {}).get("totalLikeCount", 0))
        _append_message("like", user, f"liked x{count}" if count > 1 else "liked")

    def on_share(data):
        user = (data or {}).get("uniqueId", "???")
        _append_message("share", user, "shared the stream!")

    def on_member(data):
        user = (data or {}).get("uniqueId", "???")
        _viewer_join(user)
        _append_message("join", user, "joined")

    def on_room_user(data):
        global _viewer_count
        _viewer_count = int((data or {}).get("viewerCount", 0))
        _try_capture_room_id(data)

    def on_disconnected(data):
        global _status
        reason = (data or {}).get("reason", "unknown")
        if _stop_requested:
            _status = "stopped"
        else:
            _status = "disconnected"
        _clear_viewers()
        print(f"[tiktok-live-chat] disconnected: {reason}")

    def on_error(data):
        global _status
        _status = "error"
        print(f"[tiktok-live-chat] error: {data}")

    def on_stream_end(data):
        global _status
        _status = "stream_ended"
        _clear_viewers()
        print("[tiktok-live-chat] stream ended")

    client.on("connected", on_connected)
    client.on("chat", on_chat)
    client.on("gift", on_gift)
    client.on("follow", on_follow)
    client.on("like", on_like)
    client.on("share", on_share)
    client.on("member", on_member)
    client.on("roomUser", on_room_user)
    client.on("disconnected", on_disconnected)
    client.on("error", on_error)
    client.on("streamEnd", on_stream_end)

    try:
        await client.connect()
    except Exception as exc:
        _status = "error"
        print(f"[tiktok-live-chat] connection failed: {exc}")


async def stop_live_chat() -> None:
    """Gracefully disconnect from TikTok Live chat."""
    global _client, _stop_requested, _status
    _stop_requested = True
    if _client is not None:
        try:
            await _client.disconnect()
        except Exception as exc:
            logger.warning(
                "tiktok_live_chat.stop_live_chat: disconnect failed (ignored): %s",
                exc,
                exc_info=True,
            )
        _client = None
    _status = "stopped"
    _clear_viewers()


def get_recent_messages(since_idx: int = 0) -> list[dict]:
    """Return messages with idx > since_idx from the ring buffer."""
    with _buffer_lock:
        return [m for m in _buffer if m["idx"] > since_idx]


def get_room_id() -> Optional[str]:
    return _room_id


def get_stream_viewers(
    presence_timeout: float | None = None,
    active_threshold: float | None = None,
) -> list[dict]:
    """Return viewers still considered present in the stream.

    *presence_timeout* — seconds since last activity before a viewer is evicted
        entirely (default 30 min).  Handles multi-day streams without bloat.
    *active_threshold* — seconds since last interaction to show as "chatting"
        vs "watching" (default 5 min).

    Each entry: {"username": str, "active": bool}
    """
    now = time.monotonic()
    gone_cutoff = now - (presence_timeout or _DEFAULT_PRESENCE_TIMEOUT)
    active_cutoff = now - (active_threshold or _DEFAULT_ACTIVE_THRESHOLD)
    with _viewers_lock:
        expired = [u for u, info in _viewers.items() if info["last_active_at"] < gone_cutoff]
        for u in expired:
            del _viewers[u]
        return [
            {"username": user, "active": info["last_active_at"] >= active_cutoff}
            for user, info in _viewers.items()
        ]


def get_chat_status() -> dict:
    return {
        "status": _status,
        "viewer_count": _viewer_count,
        "room_id": _room_id,
    }
