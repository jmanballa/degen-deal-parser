"""Pytest bootstrap for the Degen ops app.

This file runs BEFORE any test module imports, so it's the safe place to
neutralize production-only environment knobs that would otherwise be
inherited from the developer's real `.env` and break test fixtures.

Specifically: Starlette's SessionMiddleware emits Set-Cookie with `Secure`
and `Domain=ops.degencollectibles.com` in production, which causes httpx's
TestClient (talking to http://testserver) to silently drop the cookie —
and without the session cookie, CSRF tokens can't round-trip, so every
POST test that relies on `_csrf()` fails with "Session expired".

We override these env vars via os.environ.setdefault BEFORE any `app.*`
import so pydantic-settings picks up the test-safe values. If a test
already set them explicitly (e.g. in its own os.environ.setdefault), we
respect that.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

# Cookie settings — must be http://testserver friendly.
os.environ.setdefault("SESSION_HTTPS_ONLY", "false")
os.environ.setdefault("SESSION_DOMAIN", "")
os.environ.setdefault("SESSION_SAME_SITE", "lax")
os.environ.setdefault("SESSION_SECRET", "pytest-session-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "pytest-admin-password-" + "x" * 24)

# Database — force a local SQLite so tests that hit `get_session` /
# `managed_session` (e.g. the `attach_current_user` middleware) never
# reach out to the developer's real Postgres URL. Individual tests may
# still build their own in-memory engine via dependency_overrides.
_TEST_DB = Path(tempfile.gettempdir()) / "degen_pytest.db"
# Force-override — the developer's shell often exports a real Postgres
# DATABASE_URL (e.g. from Render) that would otherwise leak in via
# setdefault. Tests must never touch production data.
os.environ["DATABASE_URL"] = f"sqlite:///{_TEST_DB.as_posix()}"

# Portal-specific — same idea: provide a valid-shape default so
# `app.pii` / `app.auth` don't fail-closed at import time when a test
# module forgets to set these. Individual test files can still override.
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "pytest-email-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "pytest-token-hmac-key-" + "x" * 24)

# TikTok auth tests must not need real Partner Center credentials. These are
# inert test values; production routes still reject missing/invalid runtime
# config when OAuth is actually used.
os.environ.setdefault("TIKTOK_APP_KEY", "pytest-tiktok-app-key")
os.environ.setdefault("TIKTOK_APP_SECRET", "pytest-tiktok-app-secret")
os.environ.setdefault("TIKTOK_REDIRECT_URI", "http://testserver/integrations/tiktok/callback")
os.environ.setdefault("TIKTOK_SYNC_ENABLED", "false")

# If the cached Settings singleton was somehow created before conftest
# loaded (unlikely, but possible via plugin import order), clear it so
# the next get_settings() call picks up our overrides.
try:  # pragma: no cover — defensive
    from app import config as _cfg  # type: ignore

    _cfg.get_settings.cache_clear()
except Exception:
    pass
