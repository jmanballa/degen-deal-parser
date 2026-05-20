import asyncio
import shutil
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlmodel import Session, SQLModel, create_engine, select

from app.attachment_repair import AttachmentRepairCandidate
import app.discord.discord_ingest as discord_ingest
from app.discord.discord_ingest import (
    audit_recent_channel_history,
    run_periodic_attachment_repair_once,
    run_periodic_recent_audit_once,
)
from app.shared import find_nearby_image_candidates
from app.models import AttachmentAsset, DiscordMessage, PARSE_PARSED, WatchedChannel


class _FakeAttachment:
    def __init__(self, url: str, *, filename: str = "recovered.png", content_type: str = "image/png"):
        self.url = url
        self.filename = filename
        self.content_type = content_type


class _FakeAuthor:
    def __init__(self, name: str):
        self.id = 42
        self.name = name
        self.display_name = name
        self.bot = False

    def __str__(self) -> str:
        return self.name


class _FakeMessage:
    def __init__(
        self,
        *,
        message_id: str,
        channel_id: str,
        channel_name: str,
        author_name: str,
        content: str,
        created_at: datetime,
        attachments: list[_FakeAttachment],
        edited_at: datetime | None = None,
    ) -> None:
        self.id = int(message_id)
        self.content = content
        self.created_at = created_at
        self.edited_at = edited_at
        self.attachments = attachments
        self.channel = SimpleNamespace(id=int(channel_id), name=channel_name)
        self.guild = None
        self.author = _FakeAuthor(author_name)


class _FakeHistoryChannel:
    def __init__(self, *, channel_id: int, name: str, history_rows: list[_FakeMessage]):
        self.id = channel_id
        self.name = name
        self._history_rows = history_rows

    def history(self, *, limit=None, oldest_first=True, after=None):
        async def _gen():
            for row in self._history_rows:
                if after is not None and row.created_at < after:
                    continue
                yield row

        return _gen()


class _FakeDiscordClient:
    def __init__(self, channel: _FakeHistoryChannel):
        self._channel = channel

    def is_closed(self):
        return False

    def is_ready(self):
        return True

    def get_channel(self, channel_id: int):
        return self._channel if self._channel.id == channel_id else None


class _FakeHTTPResponse:
    def __init__(self, content: bytes):
        self.content = content
        self.headers = {"content-type": "image/png"}

    def raise_for_status(self):
        return None


class _FakeHTTPClient:
    def __init__(self, content: bytes):
        self._content = content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url: str):
        return _FakeHTTPResponse(self._content)


class AttachmentRepairAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_attachment_repair_audit" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "attachment_repair_audit.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)
        self.cache_dir = self.temp_dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_run_periodic_recent_audit_once_uses_configured_lookback_window(self) -> None:
        client = SimpleNamespace(
            is_closed=lambda: False,
            is_ready=lambda: True,
        )
        fixed_now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)

        with patch.object(discord_ingest.settings, "periodic_offline_audit_lookback_hours", 24.0), patch.object(
            discord_ingest.settings,
            "periodic_offline_audit_limit_per_channel",
            17,
        ), patch.object(discord_ingest, "utcnow", return_value=fixed_now), patch.object(
            discord_ingest,
            "audit_recent_enabled_channels",
            new=AsyncMock(return_value={"ok": True, "results": [], "total_inserted": 0, "total_updated": 0, "total_skipped": 0, "total_deleted": 0}),
        ) as audit_mock:
            result = asyncio.run(run_periodic_recent_audit_once(client))

        self.assertIsNotNone(result)
        self.assertEqual(audit_mock.await_count, 1)
        call = audit_mock.await_args
        self.assertEqual(call.args[0], client)
        self.assertEqual(call.kwargs["limit_per_channel"], 17)
        self.assertTrue(call.kwargs["oldest_first"])
        self.assertEqual(call.kwargs["after"], fixed_now - timedelta(hours=24))

    def test_run_periodic_attachment_repair_once_uses_bounded_recent_candidates(self) -> None:
        client = SimpleNamespace(
            is_closed=lambda: False,
            is_ready=lambda: True,
        )
        fixed_now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        candidate = AttachmentRepairCandidate(
            message_id=123,
            channel_id="1",
            discord_message_id="discord-123",
            created_at=fixed_now - timedelta(hours=1),
            attachment_urls=["https://cdn.example.com/a.png"],
            existing_assets=[],
            missing_cache_asset_ids=[],
        )

        @contextmanager
        def temp_managed_session():
            yield None

        with patch.object(discord_ingest.settings, "periodic_attachment_repair_lookback_hours", 48.0), patch.object(
            discord_ingest.settings,
            "periodic_attachment_repair_limit",
            12,
        ), patch.object(
            discord_ingest.settings,
            "periodic_attachment_repair_min_age_minutes",
            15,
        ), patch.object(discord_ingest, "utcnow", return_value=fixed_now), patch.object(
            discord_ingest,
            "attachment_repair_candidate_query",
            return_value=[candidate],
        ) as query_mock, patch.object(
            discord_ingest,
            "get_enabled_channel_ids",
            return_value={1},
        ), patch.object(
            discord_ingest,
            "managed_session",
            temp_managed_session,
        ), patch.object(
            discord_ingest,
            "restore_missing_assets_from_urls",
            return_value=(1, 0),
        ) as restore_mock, patch.object(
            discord_ingest,
            "row_status_snapshot",
            return_value=(1, 1),
        ), patch.object(
            discord_ingest,
            "recover_attachment_assets_for_message",
            new=AsyncMock(return_value=False),
        ) as recover_mock:
            result = asyncio.run(run_periodic_attachment_repair_once(client))

        self.assertIsNotNone(result)
        self.assertEqual(query_mock.call_count, 1)
        self.assertEqual(query_mock.call_args.kwargs["limit"], 12)
        self.assertEqual(query_mock.call_args.kwargs["since"], fixed_now - timedelta(hours=48))
        self.assertEqual(query_mock.call_args.kwargs["before"], fixed_now - timedelta(minutes=15))
        restore_mock.assert_called_once_with(123, ["https://cdn.example.com/a.png"])
        recover_mock.assert_not_awaited()
        self.assertEqual(result["processed_candidates"], 1)
        self.assertEqual(result["repaired_rows"], 1)
        self.assertEqual(result["failed_rows"], 0)

    def test_sync_attachment_assets_retries_cleanly_after_transient_write_retry(self) -> None:
        payloads = [
            {
                "url": "https://cdn.example.com/retry.png",
                "filename": "retry.png",
                "content_type": "image/png",
                "is_image": True,
            }
        ]
        attempts = {"count": 0}

        @contextmanager
        def temp_managed_session():
            with Session(self.engine) as session:
                yield session

        def fake_run_write_with_retry(operation, **kwargs):
            for _ in range(2):
                with Session(self.engine) as session:
                    result = operation(session)
                    if attempts["count"] == 0:
                        attempts["count"] += 1
                        session.rollback()
                        continue
                    session.commit()
                    return result
            raise AssertionError("write retry did not succeed")

        with patch.object(discord_ingest, "managed_session", temp_managed_session), patch.object(
            discord_ingest,
            "run_write_with_retry",
            side_effect=fake_run_write_with_retry,
        ), patch.object(
            discord_ingest,
            "download_attachment",
            return_value=(b"retry-bytes", "image/png"),
        ), patch.object(
            discord_ingest,
            "write_attachment_cache_file",
            side_effect=lambda *args, **kwargs: None,
        ), patch.object(
            discord_ingest,
            "delete_attachment_cache_file",
            side_effect=lambda *args, **kwargs: None,
        ):
            discord_ingest.sync_attachment_assets(1, payloads)

        with Session(self.engine) as session:
            assets = session.exec(select(AttachmentAsset).where(AttachmentAsset.message_id == 1)).all()

        self.assertEqual(attempts["count"], 1)
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].source_url, "https://cdn.example.com/retry.png")
        self.assertEqual(assets[0].data, b"retry-bytes")

    def test_run_periodic_attachment_repair_once_uses_discord_fallback_after_url_failures(self) -> None:
        client = SimpleNamespace(
            is_closed=lambda: False,
            is_ready=lambda: True,
        )
        fixed_now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        candidate = AttachmentRepairCandidate(
            message_id=456,
            channel_id="2",
            discord_message_id="discord-456",
            created_at=fixed_now - timedelta(hours=2),
            attachment_urls=["https://cdn.example.com/b.png"],
            existing_assets=[],
            missing_cache_asset_ids=[],
        )

        @contextmanager
        def temp_managed_session():
            yield None

        with patch.object(discord_ingest.settings, "periodic_attachment_repair_lookback_hours", 24.0), patch.object(
            discord_ingest.settings,
            "periodic_attachment_repair_limit",
            10,
        ), patch.object(
            discord_ingest.settings,
            "periodic_attachment_repair_min_age_minutes",
            10,
        ), patch.object(discord_ingest, "utcnow", return_value=fixed_now), patch.object(
            discord_ingest,
            "attachment_repair_candidate_query",
            return_value=[candidate],
        ), patch.object(
            discord_ingest,
            "get_enabled_channel_ids",
            return_value={2},
        ), patch.object(
            discord_ingest,
            "managed_session",
            temp_managed_session,
        ), patch.object(
            discord_ingest,
            "restore_missing_assets_from_urls",
            return_value=(0, 1),
        ), patch.object(
            discord_ingest,
            "row_status_snapshot",
            return_value=(1, 1),
        ), patch.object(
            discord_ingest,
            "recover_attachment_assets_for_message",
            new=AsyncMock(return_value=True),
        ) as recover_mock:
            result = asyncio.run(run_periodic_attachment_repair_once(client))

        recover_mock.assert_awaited_once_with(
            channel_id="2",
            discord_message_id="discord-456",
            message_row_id=456,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["discord_fallback_rows"], 1)
        self.assertEqual(result["repaired_rows"], 1)
        self.assertEqual(result["failed_rows"], 0)

    def test_audit_recent_channel_history_refreshes_attachment_assets_for_edited_message(self) -> None:
        now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        channel_id = "1398068861932408902"
        old_url = "https://cdn.example.com/old.png"
        new_url = "https://cdn.example.com/new.png"

        fake_message = _FakeMessage(
            message_id="1488253008243331174",
            channel_id=channel_id,
            channel_name="watched",
            author_name="tester",
            content="cards sold for cash",
            created_at=now - timedelta(minutes=10),
            edited_at=now,
            attachments=[_FakeAttachment(new_url)],
        )
        fake_channel = _FakeHistoryChannel(
            channel_id=int(channel_id),
            name="watched",
            history_rows=[fake_message],
        )
        client = _FakeDiscordClient(fake_channel)
        existing = DiscordMessage(
            id=1,
            discord_message_id="1488253008243331174",
            channel_id=channel_id,
            channel_name="watched",
            author_name="tester",
            content="cards sold for cash",
            created_at=now - timedelta(minutes=10),
            edited_at=now - timedelta(minutes=9),
            parse_status=PARSE_PARSED,
            attachment_urls_json=f'["{old_url}"]',
        )

        @contextmanager
        def temp_managed_session():
            with Session(self.engine) as session:
                yield session

        with patch.object(discord_ingest, "get_enabled_channel_ids", return_value={int(channel_id)}), patch.object(
            discord_ingest,
            "managed_session",
            temp_managed_session,
        ), patch.object(discord_ingest, "get_message_row", return_value=existing), patch.object(
            discord_ingest,
            "insert_or_update_message",
            return_value=(True, "updated"),
        ) as update_mock:
            result = asyncio.run(
                audit_recent_channel_history(
                    client,
                    channel_id=int(channel_id),
                    limit=10,
                    oldest_first=True,
                    after=now - timedelta(hours=1),
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["updated"], 1)
        self.assertEqual(update_mock.call_count, 1)
        self.assertTrue(update_mock.call_args.kwargs["is_edit"])
        self.assertEqual(update_mock.call_args.args[0].id, fake_message.id)

    def test_find_nearby_image_candidates_prefers_closest_valid_candidate(self) -> None:
        now = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        channel_id = "1398068861932408902"

        with Session(self.engine) as session:
            target = DiscordMessage(
                discord_message_id="target",
                channel_id=channel_id,
                channel_name="watched",
                author_name="tester",
                content="sold sealed product",
                created_at=now,
                parse_status=PARSE_PARSED,
                attachment_urls_json="[]",
            )
            deleted_candidate = DiscordMessage(
                discord_message_id="deleted",
                channel_id=channel_id,
                channel_name="watched",
                author_name="tester",
                content="image candidate",
                created_at=now - timedelta(seconds=4),
                parse_status=PARSE_PARSED,
                is_deleted=True,
                attachment_urls_json='["https://cdn.example.com/deleted.png"]',
            )
            stitched_candidate = DiscordMessage(
                discord_message_id="stitched",
                channel_id=channel_id,
                channel_name="watched",
                author_name="tester",
                content="image candidate",
                created_at=now - timedelta(seconds=6),
                parse_status=PARSE_PARSED,
                stitched_group_id="group-1",
                attachment_urls_json='["https://cdn.example.com/stitched.png"]',
            )
            closest_valid = DiscordMessage(
                discord_message_id="closest",
                channel_id=channel_id,
                channel_name="watched",
                author_name="tester",
                content="image candidate",
                created_at=now - timedelta(seconds=8),
                parse_status=PARSE_PARSED,
                attachment_urls_json='["https://cdn.example.com/closest.png"]',
            )
            farther_valid = DiscordMessage(
                discord_message_id="farther",
                channel_id=channel_id,
                channel_name="watched",
                author_name="tester",
                content="image candidate",
                created_at=now - timedelta(seconds=18),
                parse_status=PARSE_PARSED,
                attachment_urls_json='["https://cdn.example.com/farther.png"]',
            )
            session.add(target)
            session.add(deleted_candidate)
            session.add(stitched_candidate)
            session.add(closest_valid)
            session.add(farther_valid)
            session.commit()
            session.refresh(target)
            session.refresh(deleted_candidate)
            session.refresh(stitched_candidate)
            session.refresh(closest_valid)
            session.refresh(farther_valid)

            for row, source_url in (
                (deleted_candidate, "https://cdn.example.com/deleted.png"),
                (stitched_candidate, "https://cdn.example.com/stitched.png"),
                (closest_valid, "https://cdn.example.com/closest.png"),
                (farther_valid, "https://cdn.example.com/farther.png"),
            ):
                session.add(
                    AttachmentAsset(
                        message_id=row.id,
                        source_url=source_url,
                        filename=f"{row.discord_message_id}.png",
                        content_type="image/png",
                        is_image=True,
                        data=f"{row.discord_message_id}-bytes".encode(),
                    )
                )
            session.commit()

            result = find_nearby_image_candidates(session, [target])

            closest_asset = session.exec(
                select(AttachmentAsset).where(AttachmentAsset.message_id == closest_valid.id)
            ).first()

        self.assertIn(target.id, result)
        self.assertEqual(result[target.id]["message_id"], closest_valid.id)
        self.assertEqual(result[target.id]["image_url"], f"/attachments/{closest_asset.id}")
        self.assertEqual(result[target.id]["delta_seconds"], 8)


if __name__ == "__main__":
    unittest.main()
