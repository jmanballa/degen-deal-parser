from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hmac
import secrets
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

SESSION_KEY = "csrf_token"
SESSION_ISSUED_AT_KEY = "csrf_token_issued_at"
MAX_TOKEN_AGE = timedelta(hours=4)
CSRF_EXEMPT_PREFIXES = ("/webhooks/",)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_issued_at(value: object) -> Optional[datetime]:
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def issue_token(request: Request) -> str:
    token = request.session.get(SESSION_KEY)
    issued_at = _coerce_issued_at(request.session.get(SESSION_ISSUED_AT_KEY))
    if not token or issued_at is None or (_now() - issued_at) > MAX_TOKEN_AGE:
        token = secrets.token_urlsafe(32)
        request.session[SESSION_KEY] = token
        request.session[SESSION_ISSUED_AT_KEY] = _now().isoformat()
    return token


def rotate_token(request: Request) -> str:
    """Discard any existing CSRF token and mint a fresh one. Call on auth
    state changes (login, logout, password rotation) so a pre-login attacker
    can't reuse a sniffed token post-auth."""
    request.session.pop(SESSION_KEY, None)
    request.session.pop(SESSION_ISSUED_AT_KEY, None)
    return issue_token(request)


def verify_token(request: Request, submitted: Optional[str]) -> bool:
    expected = request.session.get(SESSION_KEY)
    if not expected or not submitted:
        return False
    issued_at = _coerce_issued_at(request.session.get(SESSION_ISSUED_AT_KEY))
    if issued_at is None or (_now() - issued_at) > MAX_TOKEN_AGE:
        return False
    return hmac.compare_digest(str(expected), str(submitted))


async def require_csrf(request: Request) -> None:
    """FastAPI dependency — reads token from form or X-CSRF-Token header."""
    submitted = request.headers.get("x-csrf-token")
    if not submitted:
        try:
            form = await request.form()
            submitted = form.get("csrf_token")  # type: ignore[assignment]
        except Exception:
            submitted = None
    if not verify_token(request, submitted):
        # Dependency raises via JSONResponse -> caller should check and bail.
        # Use HTTPException for cleaner 403.
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="csrf_invalid")


class CSRFProtectedRoute(APIRoute):
    """APIRoute that applies CSRF checks to unsafe methods."""

    def get_route_handler(self):
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request):
            path = request.url.path
            if (
                request.method.upper() in {"POST", "PUT", "PATCH", "DELETE"}
                and not any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in CSRF_EXEMPT_PREFIXES)
            ):
                from fastapi import HTTPException

                submitted = request.headers.get("x-csrf-token")
                if not submitted:
                    try:
                        form = await request.form()
                        submitted = str(form.get("csrf_token") or "")
                    except Exception:
                        submitted = ""
                if not verify_token(request, submitted):
                    raise HTTPException(status_code=403, detail="csrf_invalid")
            return await original_route_handler(request)

        return custom_route_handler
