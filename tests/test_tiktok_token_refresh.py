import asyncio
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from sqlmodel import SQLModel, Session, create_engine

from app.tiktok.tiktok_auth_refresh import refresh_tiktok_auth_if_needed
from app.models import TikTokAuth


def _utcnow():
    return datetime.now(timezone.utc)


def _future(minutes=60):
    return _utcnow() + timedelta(minutes=minutes)


def _past(minutes=60):
    return _utcnow() - timedelta(minutes=minutes)


class RefreshTiktokAuthTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _seed_auth(self, session, access_token="tok", refresh_token="rtok",
                   access_token_expires_at=None, app_key="key"):
        auth = TikTokAuth(
            app_key=app_key,
            tiktok_shop_id="shop-1",
            access_token=access_token,
            refresh_token=refresh_token,
            access_token_expires_at=access_token_expires_at or _future(60),
            created_at=_utcnow(),
            updated_at=_utcnow(),
            source="oauth",
        )
        session.add(auth)
        session.commit()
        session.refresh(auth)
        return auth

    def test_no_auth_row_returns_none(self):
        with Session(self.engine) as session:
            result = refresh_tiktok_auth_if_needed(
                session,
                runtime_name="test",
                resolve_base_url=lambda: "https://example.com",
            )
        self.assertIsNone(result)

    def test_valid_token_not_expiring_skips_refresh(self):
        """Token not close to expiry — no refresh needed."""
        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_future(60))
            with patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )
        self.assertIsNone(result)

    def test_missing_app_secret_returns_none(self):
        with Session(self.engine) as session:
            self._seed_auth(session)
            with patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = ""  # missing
                mock_settings.tiktok_refresh_token = "rtok"
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )
        self.assertIsNone(result)

    def test_expired_token_triggers_refresh(self):
        fake_result = {
            "data": {
                "access_token": "new-tok",
                "refresh_token": "new-rtok",
                "access_token_expire_in": 86400,
            }
        }

        def fake_refresh_fn(client, *, base_url, app_key, app_secret, refresh_token):
            return fake_result

        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_past(5))
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", fake_refresh_fn), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings, \
                 patch("app.tiktok.tiktok_auth_refresh.upsert_tiktok_auth_from_callback",
                       return_value=("inserted", {"tiktok_shop_id": "shop-1"})) as mock_upsert:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                result = refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                )

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "inserted")

    def test_update_state_called_when_provided(self):
        fake_result = {"data": {"access_token": "t", "refresh_token": "r", "access_token_expire_in": 3600}}

        def fake_refresh_fn(client, *, base_url, app_key, app_secret, refresh_token):
            return fake_result

        update_state_calls = []

        def fake_update_state(**kwargs):
            update_state_calls.append(kwargs)

        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_past(5))
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", fake_refresh_fn), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings, \
                 patch("app.tiktok.tiktok_auth_refresh.upsert_tiktok_auth_from_callback",
                       return_value=("inserted", {"tiktok_shop_id": "shop-1"})):
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                refresh_tiktok_auth_if_needed(
                    session,
                    runtime_name="test",
                    resolve_base_url=lambda: "https://example.com",
                    update_state=fake_update_state,
                )

        self.assertEqual(len(update_state_calls), 1)

    def test_exception_during_refresh_propagates(self):
        def exploding_refresh(client, *, base_url, app_key, app_secret, refresh_token):
            raise RuntimeError("network error")

        with Session(self.engine) as session:
            self._seed_auth(session, access_token_expires_at=_past(5))
            with patch("app.tiktok.tiktok_auth_refresh._refresh_fn", exploding_refresh), \
                 patch("app.tiktok.tiktok_auth_refresh.settings") as mock_settings:
                mock_settings.tiktok_app_key = "key"
                mock_settings.tiktok_app_secret = "secret"
                mock_settings.tiktok_refresh_token = ""
                mock_settings.tiktok_redirect_uri = ""
                mock_settings.tiktok_shop_id = ""
                with self.assertRaises(RuntimeError):
                    refresh_tiktok_auth_if_needed(
                        session,
                        runtime_name="test",
                        resolve_base_url=lambda: "https://example.com",
                    )


class PeriodicLoopTests(unittest.TestCase):
    def test_loop_exits_when_stop_event_set(self):
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        async def run():
            stop = asyncio.Event()
            stop.set()  # pre-set so the loop exits without sleeping
            with patch("app.discord.worker_service.settings") as mock_settings:
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001  # very short
                # Loop should exit cleanly after stop_event is set
                task = asyncio.create_task(periodic_tiktok_token_refresh_loop(stop))
                await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(run())

    def test_loop_catches_exceptions_without_crashing(self):
        """The per-iteration try/except swallows errors so the loop can keep running."""
        from app.discord.worker_service import periodic_tiktok_token_refresh_loop

        error_log = []
        sleep_calls = [0]

        async def fake_to_thread(fn, *args, **kwargs):
            raise RuntimeError("simulated network error")

        async def fake_sleep(seconds):
            sleep_calls[0] += 1
            # On second sleep, the loop body has already run once and caught the error.
            # Set stop so the loop exits cleanly on its next iteration check.
            if sleep_calls[0] >= 2:
                stop.set()

        async def run():
            nonlocal stop
            stop = asyncio.Event()
            with patch("app.discord.worker_service.settings") as mock_settings, \
                 patch("asyncio.to_thread", fake_to_thread), \
                 patch("app.discord.worker_service.managed_session"), \
                 patch("asyncio.sleep", fake_sleep), \
                 patch("builtins.print", lambda *a, **kw: error_log.append(str(a))):
                mock_settings.tiktok_token_refresh_interval_minutes = 0.001
                mock_settings.runtime_name = "test"
                task = asyncio.create_task(periodic_tiktok_token_refresh_loop(stop))
                await asyncio.wait_for(task, timeout=2.0)

        stop = None
        asyncio.run(run())
        # The error should have been printed (swallowed, not re-raised)
        self.assertTrue(any("simulated network error" in str(entry) for entry in error_log))


if __name__ == "__main__":
    unittest.main()
