import asyncio
import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlmodel import Session, SQLModel, create_engine, select
from starlette.requests import Request

from app.main import attachment_asset, deal_detail_page, message_attachment_fallback
from app.models import AttachmentAsset, DiscordMessage, PARSE_PARSED, WatchedChannel, utcnow


def make_request(path: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": path, "headers": []})


class _TupleAllResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        normalized = []
        for row in self._rows:
            if isinstance(row, (tuple, list)):
                normalized.append(tuple(row))
            else:
                normalized.append((row,))
        return normalized


class AttachmentRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_attachment_routes" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "attachment_routes.db"
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

    def test_attachment_asset_rehydrates_missing_disk_cache_from_blob(self) -> None:
        with Session(self.engine) as session:
            asset = AttachmentAsset(
                message_id=1,
                source_url="https://cdn.example.com/a.png",
                filename="deal.png",
                content_type="image/png",
                is_image=True,
                data=b"cached-image-bytes",
            )
            session.add(asset)
            session.commit()
            session.refresh(asset)

            expected_path = self.cache_dir / f"{asset.id}-deal.png"
            req = make_request(f"/attachments/{asset.id}")
            with patch("app.main.require_role_response", return_value=None), patch(
                "app.main.attachment_cache_path", return_value=expected_path,
            ), patch(
                "app.main.write_attachment_cache_file",
                side_effect=lambda asset_id, filename, content_type, data: expected_path.write_bytes(data) or expected_path,
            ):
                response = attachment_asset(request=req, asset_id=asset.id, session=session)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "public, max-age=31536000, immutable")
        self.assertIn("etag", response.headers)
        self.assertTrue(expected_path.exists())
        self.assertEqual(expected_path.read_bytes(), b"cached-image-bytes")

    def test_message_attachment_fallback_redirects_to_cached_attachment_route(self) -> None:
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="msg-1",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="deal with image",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                attachment_urls_json='["https://cdn.example.com/a.png"]',
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            asset = AttachmentAsset(
                message_id=row.id,
                source_url="https://cdn.example.com/a.png",
                filename="deal.png",
                content_type="image/png",
                is_image=True,
                data=b"cached-image-bytes",
            )
            session.add(asset)
            session.commit()
            session.refresh(asset)

            original_exec = session.exec

            def wrapped_exec(statement, *args, **kwargs):
                result = original_exec(statement, *args, **kwargs)
                if "attachmentasset.id" in str(statement).lower():
                    return _TupleAllResult(result.all())
                return result

            req = make_request(f"/messages/{row.id}/attachments/0")
            with patch("app.main.require_role_response", return_value=None), patch.object(session, "exec", side_effect=wrapped_exec):
                response = asyncio.run(message_attachment_fallback(request=req, message_id=row.id, attachment_index=0, session=session))

        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], f"/attachments/{asset.id}")

    def test_message_attachment_fallback_recovers_missing_assets_then_redirects(self) -> None:
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="msg-2",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="deal with missing cache",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                attachment_urls_json='["https://cdn.example.com/b.png"]',
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            async def fake_recover_attachment_assets_for_message(*, channel_id, discord_message_id, message_row_id):
                with Session(self.engine) as recovery_session:
                    recovered_asset = AttachmentAsset(
                        message_id=message_row_id,
                        source_url="https://cdn.example.com/b.png",
                        filename="recovered.png",
                        content_type="image/png",
                        is_image=True,
                        data=b"recovered-image-bytes",
                    )
                    recovery_session.add(recovered_asset)
                    recovery_session.commit()
                    recovery_session.refresh(recovered_asset)
                return True

            original_exec = session.exec

            def wrapped_exec(statement, *args, **kwargs):
                result = original_exec(statement, *args, **kwargs)
                if "attachmentasset.id" in str(statement).lower():
                    return _TupleAllResult(result.all())
                return result

            req = make_request(f"/messages/{row.id}/attachments/0")
            with patch("app.main.require_role_response", return_value=None), patch(
                "app.main.recover_attachment_assets_for_message",
                new=AsyncMock(side_effect=fake_recover_attachment_assets_for_message),
            ), patch.object(
                session,
                "exec",
                side_effect=wrapped_exec,
            ):
                response = asyncio.run(message_attachment_fallback(request=req, message_id=row.id, attachment_index=0, session=session))

        with Session(self.engine) as session:
            recovered_asset = session.exec(
                select(AttachmentAsset).where(AttachmentAsset.message_id == row.id)
            ).first()

        self.assertIsNotNone(recovered_asset)
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], f"/attachments/{recovered_asset.id}")

    def test_deal_detail_page_inherits_stitched_child_attachment_urls(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None), patch(
            "app.main.get_watched_channels",
            return_value=[WatchedChannel(channel_id="chan-1", channel_name="deals", is_enabled=True)],
        ):
            now = utcnow()
            primary = DiscordMessage(
                discord_message_id="primary",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="primary stitched message",
                created_at=now,
                parse_status=PARSE_PARSED,
                stitched_group_id="group-1",
                stitched_primary=True,
                stitched_message_ids_json="[]",
            )
            child = DiscordMessage(
                discord_message_id="child",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="child image message",
                created_at=now,
                parse_status=PARSE_PARSED,
                stitched_group_id="group-1",
                stitched_primary=False,
                stitched_message_ids_json="[]",
            )
            session.add(primary)
            session.add(child)
            session.commit()
            session.refresh(primary)
            session.refresh(child)

            primary.stitched_message_ids_json = f"[{primary.id}, {child.id}]"
            session.add(primary)
            session.commit()

            child_asset = AttachmentAsset(
                message_id=child.id,
                source_url="https://cdn.example.com/grouped.png",
                filename="grouped.png",
                content_type="image/png",
                is_image=True,
                data=b"grouped-image-bytes",
            )
            session.add(child_asset)
            session.commit()
            session.refresh(child_asset)

            response = deal_detail_page(
                message_id=primary.id,
                request=make_request(f"/deals/{primary.id}"),
                channel_id=None,
                entry_kind=None,
                after=None,
                before=None,
                page=1,
                limit=25,
                session=session,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(f"/attachments/{child_asset.id}", response.context["deal"]["attachment_urls"])
        self.assertIn(f"/attachments/{child_asset.id}", response.context["deal"]["image_urls"])
        self.assertEqual(response.context["deal"]["first_image_url"], f"/attachments/{child_asset.id}")


if __name__ == "__main__":
    unittest.main()
