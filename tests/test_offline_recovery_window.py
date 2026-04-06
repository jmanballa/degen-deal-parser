import asyncio
import shutil
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import discord
from sqlmodel import Session, SQLModel, create_engine, select

import app.discord_ingest as discord_ingest_module
from app.discord_ingest import DealIngestBot, mark_message_deleted
from app.models import DiscordMessage, ParseAttempt, PARSE_PARSED, PARSE_PENDING, PARSE_IGNORED, utcnow
from app.worker import OFFLINE_EDIT_REPARSE_ERROR, reconcile_offline_audit_rows


class FakeAuthor:
    def __init__(self, author_id: int = 1, name: str = "tester") -> None:
        self.id = author_id
        self.name = name
        self.bot = False
        self.display_name = name

    def __str__(self) -> str:
        return self.name


class FakeGuild:
    def __init__(self, guild_id: int = 1) -> None:
        self.id = guild_id


class FakeChannel:
    def __init__(self, channel_id: int, name: str, messages: list[object] | None = None) -> None:
        self.id = channel_id
        self.name = name
        self._messages = list(messages or [])

    async def history(self, limit=None, oldest_first=True, after=None, before=None):
        for message in self._messages:
            yield message


class FakeDiscordMessage(SimpleNamespace):
    pass


class OfflineRecoveryWindowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_offline_recovery_window" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "offline_recovery_window.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def managed_session_override(self):
        with Session(self.engine) as session:
            yield session

    def run_write_with_retry_sqlite(self, operation, *, attempts=4, initial_delay_seconds=0.35):
        """Use isolated SQLite; real run_write_with_retry uses global DATABASE_URL (e.g. Postgres)."""
        with Session(self.engine) as session:
            result = operation(session)
            session.commit()
            return result

    def make_message(
        self,
        *,
        discord_message_id: str,
        channel_id: str,
        content: str,
        created_at: datetime,
    ) -> DiscordMessage:
        return DiscordMessage(
            discord_message_id=discord_message_id,
            channel_id=channel_id,
            channel_name=f"chan-{channel_id}",
            author_id="42",
            author_name="tester",
            content=content,
            created_at=created_at,
            parse_status=PARSE_PARSED,
            parse_attempts=0,
            needs_review=False,
        )

    def make_discord_message(
        self,
        *,
        discord_message_id: int,
        channel_id: int,
        content: str,
        created_at: datetime,
    ) -> FakeDiscordMessage:
        return FakeDiscordMessage(
            id=discord_message_id,
            guild=FakeGuild(99),
            channel=FakeChannel(channel_id, f"chan-{channel_id}"),
            author=FakeAuthor(),
            content=content,
            attachments=[],
            created_at=created_at,
        )

    def test_startup_backfill_uses_bounded_lookback_window(self) -> None:
        fixed_now = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
        backfill_mock = AsyncMock(return_value={"ok": True})

        with patch.object(discord_ingest_module, "utcnow", return_value=fixed_now), patch.object(
            discord_ingest_module, "persist_available_discord_channels", lambda *_args, **_kwargs: None
        ), patch.object(discord_ingest_module, "list_available_discord_channels", return_value=[]), patch.object(
            discord_ingest_module.settings, "startup_backfill_enabled", True
        ), patch.object(
            discord_ingest_module.settings, "startup_offline_audit_enabled", False
        ), patch.object(
            discord_ingest_module.settings, "startup_backfill_lookback_hours", 12.0
        ), patch.object(
            DealIngestBot, "backfill_enabled_channels", backfill_mock
        ):
            bot = DealIngestBot(intents=discord.Intents.none())
            asyncio.run(bot.on_ready())

        self.assertEqual(backfill_mock.await_count, 1)
        kwargs = backfill_mock.await_args.kwargs
        self.assertEqual(kwargs["limit_per_channel"], discord_ingest_module.settings.startup_backfill_limit_per_channel)
        self.assertTrue(kwargs["oldest_first"])
        self.assertEqual(kwargs["after"], fixed_now - timedelta(hours=12))

    def test_startup_recent_audit_uses_bounded_lookback_window(self) -> None:
        fixed_now = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
        audit_mock = AsyncMock(return_value={"ok": True, "results": []})

        with patch.object(discord_ingest_module, "utcnow", return_value=fixed_now), patch.object(
            discord_ingest_module.settings, "startup_offline_audit_lookback_hours", 6.0
        ), patch.object(
            discord_ingest_module.settings, "startup_offline_audit_limit_per_channel", 111
        ), patch.object(
            discord_ingest_module, "audit_recent_enabled_channels", audit_mock
        ):
            bot = DealIngestBot(intents=discord.Intents.none())
            asyncio.run(bot.run_startup_recent_audit())

        self.assertEqual(audit_mock.await_count, 1)
        kwargs = audit_mock.await_args.kwargs
        self.assertEqual(kwargs["limit_per_channel"], 111)
        self.assertTrue(kwargs["oldest_first"])
        self.assertEqual(kwargs["after"], fixed_now - timedelta(hours=6))

    def test_recent_audit_replays_offline_edit_within_window(self) -> None:
        now = utcnow()
        channel_id = 1398068861932408902
        message_id = 1488253008243331174

        with Session(self.engine) as session:
            row = self.make_message(
                discord_message_id=str(message_id),
                channel_id=str(channel_id),
                content="original text",
                created_at=now - timedelta(minutes=5),
            )
            session.add(row)
            session.commit()

        edited_message = self.make_discord_message(
            discord_message_id=message_id,
            channel_id=channel_id,
            content="edited text",
            created_at=now - timedelta(minutes=4),
        )
        fake_channel = FakeChannel(channel_id, "store-buys", messages=[edited_message])
        client = SimpleNamespace(get_channel=lambda _channel_id: fake_channel)

        with patch.object(discord_ingest_module, "managed_session", self.managed_session_override), patch.object(
            discord_ingest_module, "run_write_with_retry", self.run_write_with_retry_sqlite
        ), patch.object(
            discord_ingest_module, "get_enabled_channel_ids", return_value={channel_id}
        ):
            result = asyncio.run(
                discord_ingest_module.audit_recent_channel_history(
                    client,
                    channel_id=channel_id,
                    limit=50,
                    oldest_first=True,
                    after=now - timedelta(hours=1),
                )
            )

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
            self.assertIsNotNone(row)
            self.assertEqual(row.content, "edited text")
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertIsNotNone(row.edited_at)

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["deleted"], 0)

    def test_recent_audit_marks_missing_recent_row_deleted_within_window(self) -> None:
        now = utcnow()
        channel_id = 1398043001393254400
        message_id = 1461825187883520020

        with Session(self.engine) as session:
            row = self.make_message(
                discord_message_id=str(message_id),
                channel_id=str(channel_id),
                content="will be deleted offline",
                created_at=now - timedelta(minutes=10),
            )
            session.add(row)
            session.commit()

        fake_channel = FakeChannel(channel_id, "store-sales-and-trades", messages=[])
        fake_channel.fetch_message = AsyncMock(  # type: ignore[attr-defined]
            side_effect=discord.NotFound(
                response=SimpleNamespace(status=404, reason="Not Found", text="Not Found"),
                message="Not Found",
            )
        )
        client = SimpleNamespace(get_channel=lambda _channel_id: fake_channel)

        with patch.object(discord_ingest_module, "managed_session", self.managed_session_override), patch.object(
            discord_ingest_module, "run_write_with_retry", self.run_write_with_retry_sqlite
        ), patch.object(
            discord_ingest_module, "get_enabled_channel_ids", return_value={channel_id}
        ):
            result = asyncio.run(
                discord_ingest_module.audit_recent_channel_history(
                    client,
                    channel_id=channel_id,
                    limit=50,
                    oldest_first=True,
                    after=now - timedelta(hours=1),
                )
            )

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
            self.assertIsNotNone(row)
            self.assertTrue(row.is_deleted)
            self.assertEqual(row.parse_status, PARSE_IGNORED)
            self.assertIsNotNone(row.deleted_at)

        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 1)

    def test_background_audit_requeues_edited_and_deleted_rows(self) -> None:
        now = utcnow()

        with Session(self.engine) as session:
            edited_row = self.make_message(
                discord_message_id="offline-edited-row",
                channel_id="1398068861932408902",
                content="edited offline later",
                created_at=now - timedelta(minutes=20),
            )
            edited_row.edited_at = now - timedelta(minutes=1)
            deleted_row = self.make_message(
                discord_message_id="offline-deleted-row",
                channel_id="1398043001393254400",
                content="deleted offline later",
                created_at=now - timedelta(minutes=15),
            )
            deleted_row.is_deleted = True
            session.add(edited_row)
            session.add(deleted_row)
            session.commit()

            changed = reconcile_offline_audit_rows(session, batch_size=10)
            session.refresh(edited_row)
            session.refresh(deleted_row)

        self.assertEqual(changed, 2)
        self.assertEqual(edited_row.parse_status, PARSE_PENDING)
        self.assertEqual(edited_row.last_error, OFFLINE_EDIT_REPARSE_ERROR)
        self.assertEqual(deleted_row.parse_status, PARSE_IGNORED)
        self.assertEqual(deleted_row.last_error, "message deleted")

    def test_background_audit_only_requeues_recent_risky_rows_within_batch(self) -> None:
        now = utcnow()

        with Session(self.engine) as session:
            risky_recent = self.make_message(
                discord_message_id="risky-recent",
                channel_id="1398068861932408902",
                content="recently edited and risky",
                created_at=now - timedelta(minutes=8),
            )
            risky_recent.edited_at = now - timedelta(minutes=1)

            safe_recent = self.make_message(
                discord_message_id="safe-recent",
                channel_id="1398068861932408902",
                content="edited but already reprocessed",
                created_at=now - timedelta(minutes=7),
            )
            safe_recent.edited_at = now - timedelta(minutes=1)

            old_risky = self.make_message(
                discord_message_id="old-risky",
                channel_id="1398068861932408902",
                content="old edited row",
                created_at=now - timedelta(minutes=60),
            )
            old_risky.edited_at = now - timedelta(minutes=1)

            session.add(risky_recent)
            session.add(safe_recent)
            session.add(old_risky)
            session.commit()

            for row, attempt_finished_at in [
                (risky_recent, now - timedelta(minutes=20)),
                (safe_recent, now - timedelta(seconds=30)),
                (old_risky, now - timedelta(minutes=20)),
            ]:
                session.add(
                    ParseAttempt(
                        message_id=row.id,
                        attempt_number=1,
                        started_at=attempt_finished_at - timedelta(seconds=10),
                        finished_at=attempt_finished_at,
                        success=True,
                    )
                )
            session.commit()

            changed = reconcile_offline_audit_rows(session, batch_size=2)
            session.refresh(risky_recent)
            session.refresh(safe_recent)
            session.refresh(old_risky)

        self.assertEqual(changed, 1)
        self.assertEqual(risky_recent.parse_status, PARSE_PENDING)
        self.assertEqual(risky_recent.last_error, OFFLINE_EDIT_REPARSE_ERROR)
        self.assertEqual(safe_recent.parse_status, PARSE_PARSED)
        self.assertIsNone(safe_recent.last_error)
        self.assertEqual(old_risky.parse_status, PARSE_PARSED)
        self.assertIsNone(old_risky.last_error)

    def test_live_delete_event_marks_row_deleted(self) -> None:
        now = utcnow()
        channel_id = 1398043001393254400
        message_id = 1461825187883520020

        with Session(self.engine) as session:
            row = self.make_message(
                discord_message_id=str(message_id),
                channel_id=str(channel_id),
                content="will be deleted",
                created_at=now - timedelta(minutes=10),
            )
            session.add(row)
            session.commit()

        message = self.make_discord_message(
            discord_message_id=message_id,
            channel_id=channel_id,
            content="will be deleted",
            created_at=now - timedelta(minutes=10),
        )

        with patch.object(discord_ingest_module, "managed_session", self.managed_session_override):
            self.assertTrue(mark_message_deleted(message))

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
            self.assertIsNotNone(row)
            self.assertTrue(row.is_deleted)
            self.assertEqual(row.parse_status, PARSE_IGNORED)


if __name__ == "__main__":
    unittest.main()
