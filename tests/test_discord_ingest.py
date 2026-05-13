import asyncio
import json
import shutil
import types
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine, select

import app.discord.discord_ingest as discord_ingest_module
from app.discord.discord_ingest import (
    get_attachment_payloads,
    invalidate_available_channels_cache,
    insert_or_update_message,
    list_available_discord_channels,
    mark_message_deleted_row,
    persist_available_discord_channels,
)
from app.discord.channels import get_available_channel_choices
from app.models import (
    AvailableDiscordChannel,
    DiscordMessage,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    WatchedChannel,
)


def _utcnow():
    return datetime.now(timezone.utc)


def _fake_author(author_id, author_name):
    class FakeAuthor:
        def __init__(self):
            self.id = int(author_id)
            self.bot = False
        def __str__(self):
            return author_name
    return FakeAuthor()


def _fake_message(
    msg_id="111",
    content="$50 buy",
    channel_id="999",
    channel_name="deals",
    guild_id="888",
    author_id="777",
    author_name="Trader#0001",
    attachments=None,
    edited_at=None,
):
    attachment_list = attachments or []
    return types.SimpleNamespace(
        id=int(msg_id),
        content=content,
        channel=types.SimpleNamespace(id=int(channel_id), name=channel_name),
        guild=types.SimpleNamespace(id=int(guild_id)),
        author=_fake_author(author_id, author_name),
        attachments=attachment_list,
        created_at=_utcnow(),
        edited_at=edited_at,
    )


def _fake_attachment(url="https://cdn.discord.com/a.png", filename="a.png", content_type="image/png"):
    return types.SimpleNamespace(url=url, filename=filename, content_type=content_type)


class InsertOrUpdateMessageTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_discord_ingest" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "ingest.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def _patch(self):
        return [
            patch("app.discord.discord_ingest.managed_session", self._managed_session),
            patch("app.discord.discord_ingest.sync_attachment_assets"),
            patch("app.discord.discord_ingest.ingest_log"),
        ]

    _WATCHED_CHANNEL_IDS = {999}

    def test_new_message_stored_as_pending(self):
        msg = _fake_message()
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            tracked, action = insert_or_update_message(msg, is_edit=False, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        self.assertTrue(tracked)
        self.assertEqual(action, "inserted")
        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertIsNotNone(row)
        self.assertEqual(row.parse_status, PARSE_PENDING)
        self.assertEqual(row.content, "$50 buy")

    def test_new_message_stores_channel_and_author(self):
        msg = _fake_message(channel_name="trades", author_name="Jeff#1234")
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            insert_or_update_message(msg, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertEqual(row.channel_name, "trades")
        self.assertEqual(row.author_name, "Jeff#1234")

    def test_edit_resets_parsed_row_to_pending(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                parse_attempts=2,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()

        msg = _fake_message(msg_id="111", content="$55 buy edited")
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            tracked, action = insert_or_update_message(msg, is_edit=True, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(action, "updated")
        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertEqual(row.parse_status, PARSE_PENDING)
        self.assertEqual(row.parse_attempts, 0)
        self.assertIsNone(row.last_error)

    def test_non_edit_update_preserves_parse_status(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                parse_attempts=1,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()

        msg = _fake_message(msg_id="111")
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            insert_or_update_message(msg, is_edit=False, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertEqual(row.parse_status, PARSE_PARSED)


class MarkMessageDeletedTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _make_row(self, parse_status=PARSE_PARSED):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="222",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=parse_status,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return row.id

    def test_sets_is_deleted_and_ignored(self):
        row_id = self._make_row()
        with Session(self.engine) as session:
            row = session.get(DiscordMessage, row_id)
            with patch("app.discord.discord_ingest.sync_transaction_from_message"), \
                 patch("app.discord.discord_ingest.ingest_log"):
                result = mark_message_deleted_row(session, row)

        self.assertTrue(result)
        with Session(self.engine) as session:
            row = session.get(DiscordMessage, row_id)
        self.assertTrue(row.is_deleted)
        self.assertEqual(row.parse_status, PARSE_IGNORED)

    def test_double_delete_is_noop(self):
        row_id = self._make_row()
        with Session(self.engine) as session:
            row = session.get(DiscordMessage, row_id)
            with patch("app.discord.discord_ingest.sync_transaction_from_message"), \
                 patch("app.discord.discord_ingest.ingest_log"):
                first = mark_message_deleted_row(session, row)
                second = mark_message_deleted_row(session, row)

        self.assertTrue(first)
        self.assertFalse(second)


class GetAttachmentPayloadsTests(unittest.TestCase):
    def test_returns_empty_for_no_attachments(self):
        msg = _fake_message(attachments=[])
        self.assertEqual(get_attachment_payloads(msg), [])

    def test_extracts_image_by_content_type(self):
        att = _fake_attachment(url="https://cdn/img.png", filename="img.png", content_type="image/png")
        msg = _fake_message(attachments=[att])
        payloads = get_attachment_payloads(msg)
        self.assertEqual(len(payloads), 1)
        self.assertTrue(payloads[0]["is_image"])
        self.assertEqual(payloads[0]["url"], "https://cdn/img.png")

    def test_extracts_image_by_filename_extension(self):
        att = _fake_attachment(url="https://cdn/photo.jpg", filename="photo.jpg", content_type=None)
        msg = _fake_message(attachments=[att])
        payloads = get_attachment_payloads(msg)
        self.assertTrue(payloads[0]["is_image"])

    def test_non_image_attachment_is_false(self):
        att = _fake_attachment(url="https://cdn/doc.pdf", filename="doc.pdf", content_type="application/pdf")
        msg = _fake_message(attachments=[att])
        payloads = get_attachment_payloads(msg)
        self.assertFalse(payloads[0]["is_image"])

    def test_multiple_attachments(self):
        atts = [
            _fake_attachment(url="https://cdn/a.png", filename="a.png", content_type="image/png"),
            _fake_attachment(url="https://cdn/b.pdf", filename="b.pdf", content_type="application/pdf"),
        ]
        msg = _fake_message(attachments=atts)
        payloads = get_attachment_payloads(msg)
        self.assertEqual(len(payloads), 2)
        self.assertTrue(payloads[0]["is_image"])
        self.assertFalse(payloads[1]["is_image"])


class AvailableDiscordChannelInventoryTests(unittest.TestCase):
    def setUp(self):
        invalidate_available_channels_cache()

    def tearDown(self):
        invalidate_available_channels_cache()

    def _fake_client(self, guild=None):
        class FakeLoop:
            def is_closed(self):
                return False

        return types.SimpleNamespace(
            guilds=[guild or types.SimpleNamespace(id=1, name="Degen", text_channels=[])],
            loop=FakeLoop(),
            is_closed=lambda: False,
            is_ready=lambda: True,
        )

    def _fake_rest_channel(self, channel_id="222", name="2026-may-9-10-eastbaycardshow"):
        guild = types.SimpleNamespace(id=111, name="Degen Guild", text_channels=[])
        channel = types.SimpleNamespace(
            id=int(channel_id),
            name=name,
            created_at=datetime(2026, 5, 9, tzinfo=timezone.utc),
            last_message_id=None,
        )
        return guild, channel

    def _run_coroutine_threadsafe_immediately(self, coro, _loop):
        class FakeFuture:
            def __init__(self, value):
                self.value = value
                self.cancelled = False

            def result(self, timeout=None):
                return self.value

            def cancel(self):
                self.cancelled = True

        return FakeFuture(asyncio.run(coro))

    def test_normal_cache_miss_uses_rest_channel_inventory(self):
        guild, channel = self._fake_rest_channel()

        async def fake_fetch(_client):
            return [(guild, channel, "Show Deals")], True

        with patch("app.discord.discord_ingest.get_discord_client", return_value=self._fake_client(guild)), patch(
            "app.discord.discord_ingest._fetch_live_guild_channels_rest", side_effect=fake_fetch
        ) as fetch_mock, patch(
            "app.discord.discord_ingest.asyncio.run_coroutine_threadsafe",
            side_effect=self._run_coroutine_threadsafe_immediately,
        ), patch(
            "app.discord.discord_ingest.persist_available_discord_channels"
        ) as persist_mock, patch(
            "app.discord.discord_ingest.get_cached_available_discord_channels", return_value=[]
        ):
            channels = list_available_discord_channels()

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual([row["channel_id"] for row in channels], ["222"])
        self.assertEqual(channels[0]["label"], "Show Deals / #2026-may-9-10-eastbaycardshow")
        persist_mock.assert_called_once()
        self.assertTrue(persist_mock.call_args.kwargs["remove_missing"])

    def test_in_memory_cache_prevents_repeated_rest_fetch_until_forced(self):
        guild, channel = self._fake_rest_channel()

        async def fake_fetch(_client):
            return [(guild, channel, "Show Deals")], True

        with patch("app.discord.discord_ingest.get_discord_client", return_value=self._fake_client(guild)), patch(
            "app.discord.discord_ingest._fetch_live_guild_channels_rest", side_effect=fake_fetch
        ) as fetch_mock, patch(
            "app.discord.discord_ingest.asyncio.run_coroutine_threadsafe",
            side_effect=self._run_coroutine_threadsafe_immediately,
        ), patch(
            "app.discord.discord_ingest.persist_available_discord_channels"
        ), patch(
            "app.discord.discord_ingest.get_cached_available_discord_channels", return_value=[]
        ):
            first = list_available_discord_channels()
            second = list_available_discord_channels()
            forced = list_available_discord_channels(force_refresh=True)

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(first, second)
        self.assertEqual(forced, first)

    def test_non_authoritative_fallback_keeps_persisted_inventory(self):
        guild, channel = self._fake_rest_channel(channel_id="333", name="offline-deals")
        cached_private_channel = {
            "guild_id": "111",
            "guild_name": "Degen Guild",
            "channel_id": "444",
            "channel_name": "2026-may-9-10-eastbaycardshow",
            "category_name": "Show Deals",
            "label": "Show Deals / #2026-may-9-10-eastbaycardshow",
            "created_at": None,
            "last_message_at": None,
        }

        async def fake_fetch(_client):
            return [(guild, channel, "Offline Deals")], False

        with patch("app.discord.discord_ingest.get_discord_client", return_value=self._fake_client(guild)), patch(
            "app.discord.discord_ingest._fetch_live_guild_channels_rest", side_effect=fake_fetch
        ), patch(
            "app.discord.discord_ingest.asyncio.run_coroutine_threadsafe",
            side_effect=self._run_coroutine_threadsafe_immediately,
        ), patch(
            "app.discord.discord_ingest.persist_available_discord_channels"
        ) as persist_mock, patch(
            "app.discord.discord_ingest.get_cached_available_discord_channels", return_value=[cached_private_channel]
        ):
            channels = list_available_discord_channels()

        self.assertEqual({row["channel_id"] for row in channels}, {"333", "444"})
        self.assertFalse(persist_mock.call_args.kwargs["remove_missing"])

    def test_rest_fetch_resolves_category_name_from_fetched_categories(self):
        class FakeCategory:
            def __init__(self):
                self.id = 10
                self.name = "Show Deals"

        class FakeTextChannel:
            def __init__(self):
                self.id = 555
                self.name = "2026-may-9-10-eastbaycardshow"
                self.category = None
                self.category_id = 10

        category = FakeCategory()
        text_channel = FakeTextChannel()

        class FakeGuild:
            id = 111
            text_channels = []

            async def fetch_channels(self):
                return [category, text_channel]

        guild = FakeGuild()
        with patch.object(discord_ingest_module.discord, "CategoryChannel", FakeCategory), patch.object(
            discord_ingest_module.discord, "TextChannel", FakeTextChannel
        ):
            pairs, authoritative = asyncio.run(
                discord_ingest_module._fetch_live_guild_channels_rest(
                    types.SimpleNamespace(guilds=[guild])
                )
            )

        self.assertTrue(authoritative)
        self.assertEqual(pairs, [(guild, text_channel, "Show Deals")])


class AvailableDiscordChannelPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_channel_inventory" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "channels.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def _channel(self, channel_id: str, *, category_name: str, channel_name: str) -> dict:
        return {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": channel_id,
            "channel_name": channel_name,
            "category_name": category_name,
            "label": f"{category_name} / #{channel_name}",
            "created_at": None,
            "last_message_at": None,
        }

    def _persist(self, channels: list[dict]) -> None:
        with patch("app.discord.discord_ingest.managed_session", self._managed_session):
            persist_available_discord_channels(channels)

    def test_auto_adds_new_show_deals_channel_as_backfill_ready(self):
        self._persist([
            self._channel("1001", category_name="Show Deals", channel_name="2026-may-eastbaycardshow")
        ])

        with Session(self.engine) as session:
            available = session.exec(select(AvailableDiscordChannel)).one()
            watched = session.exec(select(WatchedChannel)).one()

        self.assertEqual(available.channel_id, "1001")
        self.assertEqual(watched.channel_id, "1001")
        self.assertEqual(watched.channel_name, "Show Deals / #2026-may-eastbaycardshow")
        self.assertTrue(watched.is_enabled)
        self.assertTrue(watched.backfill_enabled)
        self.assertIsNone(watched.backfill_after)
        self.assertIsNone(watched.backfill_before)

    def test_does_not_auto_add_other_deal_categories(self):
        self._persist([
            self._channel("2001", category_name="Past Shows", channel_name="2025-show-deals"),
            self._channel("2002", category_name="Offline Deals", channel_name="offline-deals"),
            self._channel("2003", category_name="Employees", channel_name="employee-deals"),
        ])

        with Session(self.engine) as session:
            watched_rows = session.exec(select(WatchedChannel)).all()
            available_rows = session.exec(select(AvailableDiscordChannel)).all()

        self.assertEqual(watched_rows, [])
        self.assertEqual({row.channel_id for row in available_rows}, {"2001", "2002", "2003"})

    def test_preserves_existing_channel_flags_and_backfill_windows(self):
        after = datetime(2026, 4, 1, tzinfo=timezone.utc)
        before = datetime(2026, 4, 30, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                WatchedChannel(
                    channel_id="3001",
                    channel_name="Old Label",
                    is_enabled=False,
                    backfill_enabled=False,
                    backfill_after=after,
                    backfill_before=before,
                )
            )
            session.commit()

        self._persist([
            self._channel("3001", category_name="Show Deals", channel_name="renamed-cardshow-deals")
        ])

        with Session(self.engine) as session:
            watched = session.exec(select(WatchedChannel)).one()

        self.assertEqual(watched.channel_name, "Show Deals / #renamed-cardshow-deals")
        self.assertFalse(watched.is_enabled)
        self.assertFalse(watched.backfill_enabled)
        self.assertEqual(watched.backfill_after, after.replace(tzinfo=None))
        self.assertEqual(watched.backfill_before, before.replace(tzinfo=None))

    def test_show_deals_auto_add_is_idempotent(self):
        channel = self._channel("4001", category_name="Show Deals", channel_name="2026-show-deals")

        self._persist([channel])
        self._persist([channel])

        with Session(self.engine) as session:
            watched_rows = session.exec(select(WatchedChannel)).all()
            available_rows = session.exec(select(AvailableDiscordChannel)).all()

        self.assertEqual(len(watched_rows), 1)
        self.assertEqual(watched_rows[0].channel_id, "4001")
        self.assertEqual(len(available_rows), 1)


class ShowDealsAutoWatchMessageTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_channel_inventory" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "message_auto_watch.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)
        invalidate_available_channels_cache()

    def tearDown(self):
        invalidate_available_channels_cache()
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def test_new_show_deals_message_auto_watches_channel_before_ingest(self):
        guild = types.SimpleNamespace(id=111, name="Degen Guild")
        channel = types.SimpleNamespace(
            id=5001,
            name="2026-may-16-westgate",
            guild=guild,
            category=types.SimpleNamespace(name="Show Deals"),
            category_id=10,
            created_at=datetime(2026, 5, 16, tzinfo=timezone.utc),
            last_message_id=9001,
        )
        message = _fake_message(
            msg_id="9001",
            content="Buy $190",
            channel_id="5001",
            channel_name="2026-may-16-westgate",
            attachments=[_fake_attachment()],
        )
        message.channel = channel

        async def noop_auto_import(_message):
            return None

        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.maybe_auto_import_bookkeeping_message",
            side_effect=noop_auto_import,
        ), patch("app.discord.discord_ingest.sync_attachment_assets"):
            bot = discord_ingest_module.DealIngestBot(
                intents=discord_ingest_module.discord.Intents.none()
            )
            asyncio.run(bot.on_message(message))

        with Session(self.engine) as session:
            watched = session.exec(select(WatchedChannel)).one_or_none()
            available = session.exec(select(AvailableDiscordChannel)).one_or_none()
            stored_message = session.exec(select(DiscordMessage)).one_or_none()

        self.assertIsNotNone(watched)
        self.assertEqual(watched.channel_id, "5001")
        self.assertTrue(watched.is_enabled)
        self.assertTrue(watched.backfill_enabled)
        self.assertEqual(watched.channel_name, "Show Deals / #2026-may-16-westgate")
        self.assertIsNotNone(available)
        self.assertEqual(available.channel_name, "2026-may-16-westgate")
        self.assertIsNotNone(stored_message)
        self.assertEqual(stored_message.channel_id, "5001")


class AvailableChannelChoiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_admin_choices_merge_live_and_cached_available_inventory(self):
        live_channel = {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": "111",
            "channel_name": "offline-deals",
            "category_name": "Offline Deals",
            "label": "Offline Deals / #offline-deals",
            "created_at": None,
            "last_message_at": None,
        }
        cached_channel = {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": "222",
            "channel_name": "2026-may-9-10-eastbaycardshow",
            "category_name": "Show Deals",
            "label": "Show Deals / #2026-may-9-10-eastbaycardshow",
            "created_at": None,
            "last_message_at": None,
        }

        with Session(self.engine) as session, patch(
            "app.discord.channels.list_available_discord_channels", return_value=[live_channel]
        ), patch("app.discord.channels.get_cached_available_discord_channels", return_value=[cached_channel]):
            choices, has_live = get_available_channel_choices(session)

        self.assertTrue(has_live)
        self.assertEqual({row["channel_id"] for row in choices}, {"111", "222"})

    def test_admin_choices_use_cached_available_inventory_before_generic_fallback(self):
        cached_channel = {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": "222",
            "channel_name": "2026-may-9-10-eastbaycardshow",
            "category_name": "Show Deals",
            "label": "Show Deals / #2026-may-9-10-eastbaycardshow",
            "created_at": None,
            "last_message_at": None,
        }

        with Session(self.engine) as session, patch(
            "app.discord.channels.list_available_discord_channels", return_value=[]
        ), patch("app.discord.channels.get_cached_available_discord_channels", return_value=[cached_channel]):
            choices, has_live = get_available_channel_choices(session)

        self.assertFalse(has_live)
        self.assertEqual(choices, [cached_channel])


if __name__ == "__main__":
    unittest.main()
