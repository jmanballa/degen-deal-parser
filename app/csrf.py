from __future__ import annotations

import hmac
import secrets
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse

SESSION_KEY = "csrf_token"


def issue_token(request: Request) -> str:
    token = request.session.get(SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        request.session[SESSION_KEY] = token
    return token


def rotate_token(request: Request) -> str:
    """Discard any existing CSRF token and mint a fresh one. Call on auth
    state changes (login, logout, password rotation) so a pre-login attacker
    can't reuse a sniffed token post-auth."""
    request.session.pop(SESSION_KEY, None)
    return issue_token(request)


def verify_token(request: Request, submitted: Optional[str]) -> bool:
    expected = request.session.get(SESSION_KEY)
    if not expected or not submitted:
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
