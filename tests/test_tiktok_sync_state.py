import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine, select

from app.models import TikTokSyncState


def _utcnow():
    return datetime.now(timezone.utc)


class TikTokSyncStateTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _make_managed_session(self):
        from contextlib import contextmanager

        @contextmanager
        def _managed_session():
            with Session(self.engine) as session:
                yield session

        return _managed_session

    # ------------------------------------------------------------------
    # _persist_tiktok_state  (called by update_tiktok_integration_state)
    # ------------------------------------------------------------------

    def test_update_state_persists_last_error(self):
        import app.main as main_module

        with patch("app.main.managed_session", self._make_managed_session()):
            main_module.update_tiktok_integration_state(last_error="boom")

        with Session(self.engine) as session:
            row = session.get(TikTokSyncState, 1)
        self.assertIsNotNone(row)
        self.assertEqual(row.last_error, "boom")

    def test_update_state_persists_pull_timestamps(self):
        import app.main as main_module

        now = _utcnow()
        with patch("app.main.managed_session", self._make_managed_session()):
            main_module.update_tiktok_integration_state(last_pull_at=now)

        with Session(self.engine) as session:
            row = session.get(TikTokSyncState, 1)
        self.assertIsNotNone(row)
        self.assertIsNotNone(row.last_pull_at)

    def test_update_state_upserts_singleton(self):
        """Two calls should result in exactly one row, not two."""
        import app.main as main_module

        with patch("app.main.managed_session", self._make_managed_session()):
            main_module.update_tiktok_integration_state(last_error="first")
            main_module.update_tiktok_integration_state(last_error="second")

        with Session(self.engine) as session:
            rows = session.exec(select(TikTokSyncState)).all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].last_error, "second")

    def test_is_pull_running_persisted(self):
        import app.main as main_module

        with patch("app.main.managed_session", self._make_managed_session()):
            main_module.update_tiktok_integration_state(is_pull_running=True)

        with Session(self.engine) as session:
            row = session.get(TikTokSyncState, 1)
        self.assertTrue(row.is_pull_running)

    # ------------------------------------------------------------------
    # _load_tiktok_state_from_db
    # ------------------------------------------------------------------

    def test_load_from_db_restores_last_error(self):
        import app.main as main_module

        # Seed a row directly
        with Session(self.engine) as session:
            session.merge(TikTokSyncState(id=1, last_error="saved_error"))
            session.commit()

        # Reset in-memory state first
        with main_module._tiktok_state_lock:
            main_module._tiktok_state["last_error"] = None

        with patch("app.main.managed_session", self._make_managed_session()):
            main_module._load_tiktok_state_from_db()

        state = main_module.read_tiktok_integration_state()
        self.assertEqual(state["last_error"], "saved_error")

    def test_load_from_db_resets_is_pull_running(self):
        """is_pull_running should always be False after load (restart means idle)."""
        import app.main as main_module

        with Session(self.engine) as session:
            session.merge(TikTokSyncState(id=1, is_pull_running=True))
            session.commit()

        with patch("app.main.managed_session", self._make_managed_session()):
            main_module._load_tiktok_state_from_db()

        state = main_module.read_tiktok_integration_state()
        self.assertFalse(state["is_pull_running"])

    def test_load_from_db_no_row_is_noop(self):
        """If no DB row exists, in-memory state stays unchanged."""
        import app.main as main_module

        with main_module._tiktok_state_lock:
            main_module._tiktok_state["last_error"] = "preexisting"

        with patch("app.main.managed_session", self._make_managed_session()):
            main_module._load_tiktok_state_from_db()

        state = main_module.read_tiktok_integration_state()
        self.assertEqual(state["last_error"], "preexisting")

    def tearDown(self):
        # Reset in-memory state after each test to avoid cross-test contamination
        import app.main as main_module
        with main_module._tiktok_state_lock:
            main_module._tiktok_state.update({
                "last_authorization_at": None,
                "last_callback": None,
                "last_webhook_at": None,
                "last_webhook": None,
                "is_pull_running": False,
                "last_pull_started_at": None,
                "last_pull_finished_at": None,
                "last_pull_at": None,
                "last_pull": None,
                "last_error": None,
            })
        self.engine.dispose()


if __name__ == "__main__":
    unittest.main()
