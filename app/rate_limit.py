from __future__ import annotations

import ipaddress
import threading
import time
from collections import defaultdict, deque
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse

from .config import get_settings

_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_LOCK = threading.Lock()


def check(key: str, *, max_requests: int, window_seconds: float) -> bool:
    """Return True if allowed; False if rate-limited. Prunes old timestamps."""
    now = time.monotonic()
    cutoff = now - window_seconds
    with _LOCK:
        dq = _BUCKETS[key]
        while dq and dq[0] < cutoff:
            dq.popleft()
        # m3: opportunistically prune any other keys whose deques drained to
        # empty so silent clients don't leak an entry forever. Bounded O(k) on
        # the outstanding bucket count, which is already small.
        for stale_key in [k for k, v in _BUCKETS.items() if k != key and not v]:
            _BUCKETS.pop(stale_key, None)
        if len(dq) >= max_requests:
            return False
        dq.append(now)
        return True


def reset(key: Optional[str] = None) -> None:
    with _LOCK:
        if key is None:
            _BUCKETS.clear()
        else:
            _BUCKETS.pop(key, None)


def _client_ip(request: Request) -> str:
    settings = get_settings()
    client_host = request.client.host if request.client and request.client.host else ""
    if settings.trust_x_forwarded_for and settings.is_trusted_proxy(client_host):
        forwarded_for = request.headers.get("x-forwarded-for", "")
        first_hop = forwarded_for.split(",", 1)[0].strip()
        if first_hop:
            try:
                ipaddress.ip_address(first_hop)
            except ValueError:
                first_hop = ""
        if first_hop:
            return first_hop
    if client_host:
        return client_host
    return "unknown"


def rate_limited_or_429(
    request: Request,
    *,
    key_prefix: str,
    max_requests: int = 3,
    window_seconds: float = 900.0,
) -> Optional[JSONResponse]:
    key = f"{key_prefix}:{_client_ip(request)}"
    if check(key, max_requests=max_requests, window_seconds=window_seconds):
        return None
    retry_after = int(window_seconds)
    return JSONResponse(
        {"error": "rate_limited", "retry_after_seconds": retry_after},
        status_code=429,
        headers={"Retry-After": str(retry_after)},
    )
