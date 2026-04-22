"""Regression test — middleware stack order.

Bug: attach_current_user was registered AFTER SessionMiddleware, which made
it the outermost middleware in Starlette's stack (add_middleware inserts at
position 0). That meant attach_current_user ran BEFORE SessionMiddleware on
every request, so `request.scope["session"]` was empty when it tried to
resolve the logged-in user from the cookie. The ops side limped along via a
fallback inside `require_role_response`, but the team portal's stricter
`_require_employee` gate redirected every authenticated user to
`/team/login`, producing a redirect loop.

This test locks in the correct order: SessionMiddleware MUST be outermost.
"""
from __future__ import annotations

import os
import unittest

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")


class MiddlewareOrderTests(unittest.TestCase):
    def test_session_middleware_runs_before_attach_current_user(self):
        # Importing app.main wires up the middleware stack as a side effect.
        from starlette.middleware.sessions import SessionMiddleware
        from starlette.middleware.base import BaseHTTPMiddleware
        import app.main as m

        stack = m.app.user_middleware
        classes = [mw.cls for mw in stack]

        self.assertIn(SessionMiddleware, classes, "SessionMiddleware missing from stack")
        self.assertIn(BaseHTTPMiddleware, classes, "attach_current_user middleware missing")

        session_idx = classes.index(SessionMiddleware)
        base_idx = classes.index(BaseHTTPMiddleware)

        # Starlette stores user_middleware with the OUTERMOST at position 0.
        # SessionMiddleware must be outermost so scope["session"] is populated
        # by the time attach_current_user runs.
        self.assertLess(
            session_idx,
            base_idx,
            "SessionMiddleware must be outer (position 0) relative to attach_current_user; "
            f"stack order is {classes}",
        )


if __name__ == "__main__":
    unittest.main()
