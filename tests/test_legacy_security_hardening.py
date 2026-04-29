from __future__ import annotations

import os
import unittest
from contextlib import AsyncExitStack
from types import SimpleNamespace

from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "legacy-security-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "legacy-security-hmac-key")


def _fresh_engine():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


class LegacySecurityHardeningTests(unittest.TestCase):
    def setUp(self):
        self.engine = _fresh_engine()
        self.session = Session(self.engine)
        from app.db import seed_employee_portal_defaults

        seed_employee_portal_defaults(self.session)

    def tearDown(self):
        self.session.close()

    def _request_for_role(self, role: str):
        from app.models import User

        user = User(
            id=900,
            username=f"{role}1",
            password_hash="x",
            password_salt="x",
            display_name=f"{role}1",
            role=role,
            is_active=True,
        )
        return SimpleNamespace(
            state=SimpleNamespace(current_user=user),
            session={},
            headers={},
            client=SimpleNamespace(host="testclient"),
            url=SimpleNamespace(path="/tiktok/analytics/api/debug", query=""),
        )

    def test_legacy_routers_use_csrf_protected_route_class(self):
        from app.csrf import CSRFProtectedRoute
        from app.routers import admin, admin_actions, channels_api, stream_manager
        from app.routers import bookkeeping, hits, messages, tiktok_orders

        for router in (
            admin.router,
            admin_actions.router,
            channels_api.router,
            stream_manager.router,
            bookkeeping.router,
            hits.router,
            messages.router,
            tiktok_orders.router,
        ):
            unsafe_routes = [
                route
                for route in router.routes
                if getattr(route, "methods", set()) & {"POST", "PUT", "PATCH", "DELETE"}
            ]
            self.assertTrue(unsafe_routes)
            self.assertTrue(all(isinstance(route, CSRFProtectedRoute) for route in unsafe_routes))

    def test_csrf_protected_route_accepts_form_token_and_rejects_missing_token(self):
        import asyncio
        from datetime import datetime, timezone

        from fastapi import HTTPException
        from fastapi.responses import JSONResponse
        from starlette.requests import Request

        from app.csrf import CSRFProtectedRoute, SESSION_ISSUED_AT_KEY, SESSION_KEY

        async def endpoint():
            return JSONResponse({"ok": True})

        def request_for(body: bytes) -> Request:
            async def receive():
                return {"type": "http.request", "body": body, "more_body": False}

            return Request(
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/mutate",
                    "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
                    "query_string": b"",
                    "session": {
                        SESSION_KEY: "token-1",
                        SESSION_ISSUED_AT_KEY: datetime.now(timezone.utc).isoformat(),
                    },
                    "fastapi_middleware_astack": AsyncExitStack(),
                    "fastapi_inner_astack": AsyncExitStack(),
                    "fastapi_function_astack": AsyncExitStack(),
                },
                receive,
            )

        async def exercise():
            route = CSRFProtectedRoute("/mutate", endpoint, methods=["POST"])
            handler = route.get_route_handler()
            valid_request = request_for(b"csrf_token=token-1")
            async with (
                valid_request.scope["fastapi_middleware_astack"],
                valid_request.scope["fastapi_inner_astack"],
                valid_request.scope["fastapi_function_astack"],
            ):
                response = await handler(valid_request)
            self.assertEqual(response.status_code, 200)

            missing_request = request_for(b"")
            with self.assertRaises(HTTPException) as ctx:
                await handler(missing_request)
            self.assertEqual(ctx.exception.status_code, 403)

        asyncio.run(exercise())

    def test_csrf_token_age_is_enforced(self):
        from datetime import datetime, timedelta, timezone

        from app.csrf import SESSION_ISSUED_AT_KEY, SESSION_KEY, verify_token

        request = SimpleNamespace(
            session={
                SESSION_KEY: "fresh-token",
                SESSION_ISSUED_AT_KEY: datetime.now(timezone.utc).isoformat(),
            }
        )
        self.assertTrue(verify_token(request, "fresh-token"))

        request.session[SESSION_ISSUED_AT_KEY] = (
            datetime.now(timezone.utc) - timedelta(hours=4, seconds=1)
        ).isoformat()
        self.assertFalse(verify_token(request, "fresh-token"))

    def test_tiktok_debug_requires_admin_and_redacts_previews(self):
        from app.routers.tiktok_analytics import tiktok_analytics_debug

        viewer_response = tiktok_analytics_debug(self._request_for_role("viewer"))
        self.assertEqual(viewer_response.status_code, 403)

        admin_payload = tiktok_analytics_debug(self._request_for_role("admin"))
        self.assertIsInstance(admin_payload, dict)
        self.assertNotIn("access_token_preview", admin_payload)
        self.assertNotIn("shop_cipher_preview", admin_payload)
        self.assertNotIn("app_key_preview", admin_payload)

    def test_outbound_fetch_helpers_reject_untrusted_hosts(self):
        from app.attachment_repair import download_attachment
        from app.bookkeeping import build_google_sheet_export_url

        with self.assertRaises(ValueError):
            build_google_sheet_export_url("https://example.com/spreadsheets/d/abc")
        with self.assertRaises(ValueError):
            download_attachment("https://example.com/file.png")

    def test_attachment_download_rejects_redirects_and_declared_large_files(self):
        from unittest.mock import patch

        from app import attachment_repair

        class FakeResponse:
            def __init__(self, *, status_code: int = 200, headers: dict[str, str] | None = None):
                self.status_code = status_code
                self.headers = headers or {}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def raise_for_status(self):
                return None

            def iter_bytes(self):
                yield b"x"

        class FakeClient:
            def __init__(self, response: FakeResponse):
                self.response = response

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def stream(self, *args, **kwargs):
                return self.response

        allowed_url = "https://cdn.discordapp.com/attachments/1/file.png"
        with patch.object(
            attachment_repair.httpx,
            "Client",
            return_value=FakeClient(FakeResponse(status_code=302)),
        ):
            with self.assertRaises(ValueError):
                attachment_repair.download_attachment(allowed_url)

        with patch.object(
            attachment_repair.httpx,
            "Client",
            return_value=FakeClient(
                FakeResponse(
                    headers={
                        "content-length": str(attachment_repair.MAX_ATTACHMENT_DOWNLOAD_BYTES + 1),
                    }
                )
            ),
        ):
            with self.assertRaises(ValueError):
                attachment_repair.download_attachment(allowed_url)

    def test_google_sheet_export_fetch_rejects_redirects_and_declared_large_files(self):
        import asyncio
        from unittest.mock import patch

        from app import bookkeeping

        class FakeAsyncResponse:
            def __init__(self, *, status_code: int = 200, headers: dict[str, str] | None = None):
                self.status_code = status_code
                self.headers = headers or {}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def raise_for_status(self):
                return None

            async def aiter_bytes(self):
                yield b"x"

        class FakeAsyncClient:
            def __init__(self, response: FakeAsyncResponse):
                self.response = response

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def stream(self, *args, **kwargs):
                return self.response

        export_url = "https://docs.google.com/spreadsheets/d/abc/export?format=xlsx"
        with patch.object(
            bookkeeping.httpx,
            "AsyncClient",
            return_value=FakeAsyncClient(FakeAsyncResponse(status_code=302)),
        ):
            with self.assertRaises(ValueError):
                asyncio.run(bookkeeping.fetch_google_sheet_export(export_url))

        with patch.object(
            bookkeeping.httpx,
            "AsyncClient",
            return_value=FakeAsyncClient(
                FakeAsyncResponse(
                    headers={
                        "content-length": str(bookkeeping.MAX_GOOGLE_SHEET_EXPORT_BYTES + 1),
                    }
                )
            ),
        ):
            with self.assertRaises(ValueError):
                asyncio.run(bookkeeping.fetch_google_sheet_export(export_url))


if __name__ == "__main__":
    unittest.main()
