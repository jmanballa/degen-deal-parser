import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import scripts.tiktok_backfill as backfill_module


class FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class FakeSession:
    def __init__(self, saved_auth):
        self.saved_auth = saved_auth
        self.exec_calls = []

    def exec(self, query):
        self.exec_calls.append(query)
        return FakeResult(self.saved_auth)

    def commit(self):
        return None


class FakeHttpxClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TikTokBackfillTests(unittest.TestCase):
    def test_main_reuses_saved_auth_for_automatic_pull(self) -> None:
        saved_auth = SimpleNamespace(
            access_token="saved-access-token",
            refresh_token="saved-refresh-token",
            tiktok_shop_id="shop-1",
            shop_cipher="cipher-1",
        )
        fake_session = FakeSession(saved_auth)
        captured = {}

        @contextmanager
        def fake_managed_session():
            yield fake_session

        with patch.object(backfill_module, "require_env", side_effect=lambda name: {"TIKTOK_APP_KEY": "app-key", "TIKTOK_APP_SECRET": "app-secret"}[name]), patch.object(
            backfill_module,
            "optional_env",
            return_value="",
        ), patch.object(
            backfill_module,
            "resolve_shop_api_base_url",
            return_value="https://open-api.tiktokglobalshop.com",
        ), patch.object(
            backfill_module,
            "parse_args",
            return_value=SimpleNamespace(
                since=None,
                limit=None,
                dry_run=False,
                shop_id=None,
                shop_cipher=None,
                access_token=None,
                refresh_token=None,
                auth_code=None,
                products=False,
            ),
        ), patch.object(
            backfill_module,
            "init_db",
            return_value=None,
        ), patch.object(
            backfill_module,
            "managed_session",
            side_effect=fake_managed_session,
        ), patch.object(
            backfill_module,
            "httpx",
            SimpleNamespace(Client=FakeHttpxClient),
        ), patch.object(
            backfill_module,
            "backfill_tiktok_orders",
            side_effect=lambda *args, **kwargs: captured.update(kwargs) or backfill_module.TikTokPullSummary(
                fetched=1,
                inserted=1,
                updated=0,
                failed=0,
                detail_calls=0,
                auth_updated=0,
            ),
        ) as backfill_mock:
            exit_code = backfill_module.main()

        self.assertEqual(exit_code, 0)
        backfill_mock.assert_called_once()
        self.assertEqual(captured["access_token"], "saved-access-token")
        self.assertEqual(captured["shop_id"], "shop-1")
        self.assertEqual(captured["shop_cipher"], "cipher-1")
        self.assertEqual(captured["base_url"], "https://open-api.tiktokglobalshop.com")


if __name__ == "__main__":
    unittest.main()
