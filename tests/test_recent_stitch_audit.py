import shutil
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine, select

import app.discord.worker as worker_module
from app.models import DiscordMessage, ParseAttempt, WatchedChannel, PARSE_PARSED, PARSE_PENDING, utcnow
from app.discord.worker import queue_recent_stitch_audit_candidates


class RecentStitchAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_recent_stitch_audit" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "recent_stitch_audit.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def make_row(
        self,
        *,
        discord_message_id: str,
        channel_id: str,
        content: str,
        created_at,
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
            parse_attempts=1,
            confidence=0.99,
            needs_review=False,
        )

    def add_attempt(self, session: Session, row: DiscordMessage, *, finished_at) -> None:
        session.add(
            ParseAttempt(
                message_id=row.id,
                attempt_number=row.parse_attempts,
                started_at=finished_at - timedelta(seconds=10),
                finished_at=finished_at,
                success=True,
            )
        )

    def test_recent_stitch_audit_requeues_fragment_like_recent_watched_row(self) -> None:
        now = utcnow()
        watched_channel_id = "1398068861932408902"
        other_channel_id = "1398043001393254400"

        with Session(self.engine) as session:
            session.add(WatchedChannel(channel_id=watched_channel_id, channel_name="watched", is_enabled=True))

            watched_row = self.make_row(
                discord_message_id="watched-fragment",
                channel_id=watched_channel_id,
                content="zelle 15",
                created_at=now - timedelta(minutes=30),
            )
            other_row = self.make_row(
                discord_message_id="other-fragment",
                channel_id=other_channel_id,
                content="zelle 15",
                created_at=now - timedelta(minutes=30),
            )
            session.add(watched_row)
            session.add(other_row)
            session.commit()

            self.add_attempt(session, watched_row, finished_at=now - timedelta(minutes=20))
            self.add_attempt(session, other_row, finished_at=now - timedelta(minutes=20))
            session.commit()

            with (
                patch.object(worker_module.settings, "periodic_stitch_audit_limit", 10),
                patch.object(worker_module.settings, "periodic_stitch_audit_lookback_hours", 24.0),
                patch.object(worker_module.settings, "periodic_stitch_audit_min_age_minutes", 10),
                patch.object(worker_module.settings, "parser_reprocess_interval_hours", 0.25),
            ):
                queued = queue_recent_stitch_audit_candidates(session, batch_size=10)

            session.refresh(watched_row)
            session.refresh(other_row)

        self.assertEqual(queued, 1)
        self.assertEqual(watched_row.parse_status, PARSE_PENDING)
        self.assertEqual(watched_row.last_error, "recent stitch audit: fragment-like")
        self.assertEqual(other_row.parse_status, PARSE_PARSED)

    def test_recent_stitch_audit_requeues_recent_edit_after_last_parse(self) -> None:
        now = utcnow()
        watched_channel_id = "1398068861932408902"

        with Session(self.engine) as session:
            session.add(WatchedChannel(channel_id=watched_channel_id, channel_name="watched", is_enabled=True))
            row = self.make_row(
                discord_message_id="edited-after-parse",
                channel_id=watched_channel_id,
                content="cards sold for cash",
                created_at=now - timedelta(minutes=40),
            )
            row.edited_at = now - timedelta(minutes=5)
            session.add(row)
            session.commit()

            self.add_attempt(session, row, finished_at=now - timedelta(minutes=20))
            session.commit()

            with (
                patch.object(worker_module.settings, "periodic_stitch_audit_limit", 10),
                patch.object(worker_module.settings, "periodic_stitch_audit_lookback_hours", 24.0),
                patch.object(worker_module.settings, "periodic_stitch_audit_min_age_minutes", 10),
                patch.object(worker_module.settings, "parser_reprocess_interval_hours", 1.0),
            ):
                queued = queue_recent_stitch_audit_candidates(session, batch_size=10)

            session.refresh(row)

        self.assertEqual(queued, 1)
        self.assertEqual(row.parse_status, PARSE_PENDING)
        self.assertEqual(row.last_error, "recent stitch audit: edited after last parse")

    def test_recent_stitch_audit_uses_nearby_sibling_signal_but_skips_recent_attempts(self) -> None:
        now = utcnow()
        watched_channel_id = "1398068861932408902"

        with Session(self.engine) as session:
            session.add(WatchedChannel(channel_id=watched_channel_id, channel_name="watched", is_enabled=True))
            sibling_row = self.make_row(
                discord_message_id="nearby-sibling",
                channel_id=watched_channel_id,
                content="sold sealed product",
                created_at=now - timedelta(minutes=35),
            )
            recent_attempt_row = self.make_row(
                discord_message_id="recent-attempt",
                channel_id=watched_channel_id,
                content="sold sealed product",
                created_at=now - timedelta(minutes=35),
            )
            session.add(sibling_row)
            session.add(recent_attempt_row)
            session.commit()

            self.add_attempt(session, sibling_row, finished_at=now - timedelta(minutes=20))
            self.add_attempt(session, recent_attempt_row, finished_at=now - timedelta(minutes=2))
            session.commit()

            with (
                patch.object(worker_module.settings, "periodic_stitch_audit_limit", 10),
                patch.object(worker_module.settings, "periodic_stitch_audit_lookback_hours", 24.0),
                patch.object(worker_module.settings, "periodic_stitch_audit_min_age_minutes", 10),
                patch.object(worker_module.settings, "parser_reprocess_interval_hours", 0.25),
                patch.object(worker_module, "row_has_nearby_siblings", side_effect=lambda _session, row: row.id == sibling_row.id),
            ):
                queued = queue_recent_stitch_audit_candidates(session, batch_size=10)

            session.refresh(sibling_row)
            session.refresh(recent_attempt_row)

        self.assertEqual(queued, 1)
        self.assertEqual(sibling_row.parse_status, PARSE_PENDING)
        self.assertEqual(sibling_row.last_error, "recent stitch audit: nearby siblings")
        self.assertEqual(recent_attempt_row.parse_status, PARSE_PARSED)


if __name__ == "__main__":
    unittest.main()
