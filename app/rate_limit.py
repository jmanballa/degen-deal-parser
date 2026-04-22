from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse

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
    if request.client and request.client.host:
        return request.client.host
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
