import shutil
import unittest
import uuid
from datetime import timezone
from pathlib import Path

from sqlmodel import Session, SQLModel, create_engine, select

from app.discord.discord_ingest import mark_message_deleted_row, recent_message_needs_refresh
from app.models import DiscordMessage, Transaction, PARSE_PARSED, utcnow
from app.discord.transactions import sync_transaction_from_message


class _FakeAttachment:
    def __init__(self, url: str):
        self.url = url
        self.filename = url.rsplit("/", 1)[-1]
        self.content_type = "image/png"


class _FakeMessage:
    def __init__(self, *, content: str, attachment_urls: list[str] | None = None, edited_at=None):
        self.content = content
        self.attachments = [_FakeAttachment(url) for url in (attachment_urls or [])]
        self.edited_at = edited_at


class OfflineMessageRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_offline_message_recovery" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "offline_message_recovery.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_recent_message_needs_refresh_when_content_changes(self) -> None:
        row = DiscordMessage(
            discord_message_id="123",
            channel_id="1",
            content="old text",
            attachment_urls_json='["https://cdn.example.com/a.png"]',
            created_at=utcnow(),
        )
        message = _FakeMessage(
            content="new text",
            attachment_urls=["https://cdn.example.com/a.png"],
        )
        self.assertTrue(recent_message_needs_refresh(row, message))

    def test_recent_message_needs_refresh_when_edited_timestamp_advances(self) -> None:
        row = DiscordMessage(
            discord_message_id="123",
            channel_id="1",
            content="same",
            attachment_urls_json="[]",
            created_at=utcnow(),
            edited_at=utcnow().replace(tzinfo=timezone.utc),
        )
        message = _FakeMessage(
            content="same",
            edited_at=utcnow().replace(tzinfo=timezone.utc),
        )
        self.assertTrue(recent_message_needs_refresh(row, message))

    def test_mark_message_deleted_row_removes_existing_transaction(self) -> None:
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="123",
                channel_id="1",
                channel_name="deals",
                author_name="tester",
                content="sold card $10",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                deal_type="sell",
                entry_kind="sale",
                amount=10.0,
                money_in=10.0,
                is_deleted=False,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            sync_transaction_from_message(session, row)
            session.commit()
            self.assertIsNotNone(
                session.exec(select(Transaction).where(Transaction.source_message_id == row.id)).first()
            )

            changed = mark_message_deleted_row(session, row, channel_name="deals", reason="deleted in offline audit")
            self.assertTrue(changed)

            transaction = session.exec(select(Transaction).where(Transaction.source_message_id == row.id)).first()
            self.assertIsNone(transaction)
            self.assertTrue(row.is_deleted)
            self.assertEqual(row.parse_status, "ignored")


if __name__ == "__main__":
    unittest.main()
