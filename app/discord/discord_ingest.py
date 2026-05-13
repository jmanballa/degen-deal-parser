import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

import discord
from sqlalchemy.exc import ProgrammingError
from sqlmodel import Session, select

from ..attachment_repair import (
    attachment_repair_candidate_query,
    download_attachment,
    restore_missing_assets_from_urls,
    row_status_snapshot,
)
from ..attachment_storage import delete_attachment_cache_file, write_attachment_cache_file
from .bookkeeping import auto_import_public_google_sheet, extract_google_sheet_url
from ..config import get_settings
from ..db import engine, managed_session, run_write_with_retry
from ..models import (
    AttachmentAsset,
    AvailableDiscordChannel,
    DiscordMessage,
    PARSE_IGNORED,
    PARSE_PENDING,
    WatchedChannel,
)
from .ops_log import write_operations_log
from ..runtime_logging import structured_log_line
from .transactions import sync_transaction_from_message

settings = get_settings()

discord_client_instance = None
discord_runtime_state = {
    "status": "stopped",
    "error": None,
    "last_recent_audit_at": None,
    "last_recent_audit_summary": None,
    "last_attachment_repair_at": None,
    "last_attachment_repair_summary": None,
}
_available_channels_cache_lock = threading.Lock()
_available_channels_cache = {
    "expires_at": 0.0,
    "channels": [],
}
AVAILABLE_CHANNELS_CACHE_SECONDS = 30.0
AVAILABLE_CHANNELS_REST_TIMEOUT_SECONDS = 15.0
DISCORD_RETRY_MIN_SECONDS = 15
DISCORD_RETRY_MAX_SECONDS = 900
ALLOWED_CHANNEL_CATEGORIES = {
    "Employees",
    "Show Deals",
    "Past Shows",
    "Offline Deals",
}
AUTO_WATCHED_CHANNEL_CATEGORY = "Show Deals"
# Backfill cancellation is enforced from progress callbacks, so keep this
# cadence small enough to react promptly without adding a database check for
# every single message fetched from Discord history.
BACKFILL_PROGRESS_EVERY_MESSAGES = 5
RECENT_AUDIT_PROGRESS_EVERY_MESSAGES = 5
TRANSACTION_CHANNEL_NAME_HINTS = (
    "deal",
    "deals",
    "trade",
    "trades",
    "buy",
    "buys",
    "sell",
    "sells",
    "show",
    "offline",
    "cardshow",
    "expo",
)
IMAGE_ATTACHMENT_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ingest_log(
    *,
    action: str,
    level: str = "info",
    success: bool | None = None,
    error: str | None = None,
    session: Session | None = None,
    **details,
) -> None:
    print(
        structured_log_line(
            runtime="worker",
            action=action,
            success=success,
            error=error,
            **details,
        )
    )
    if session is None:
        return
    write_operations_log(
        session,
        event_type=f"ingest.{action}",
        level="error" if level == "error" else level,
        source="worker",
        message=action,
        details={
            "runtime": "worker",
            "action": action,
            "success": success,
            "error": error,
            **details,
        },
    )


def parse_iso_datetime(value: Optional[str], *, end_of_day: bool = False) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    if len(value) == 10:
        dt = datetime.fromisoformat(value)
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.replace(tzinfo=timezone.utc)

    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_enabled_channel_ids() -> set[int]:
    with managed_session() as session:
        rows = session.exec(
            select(WatchedChannel).where(WatchedChannel.is_enabled == True)
        ).all()
        return {int(r.channel_id) for r in rows}


def get_backfill_channel_ids() -> set[int]:
    with managed_session() as session:
        rows = session.exec(
            select(WatchedChannel).where(
                WatchedChannel.is_enabled == True,
                WatchedChannel.backfill_enabled == True,
            )
        ).all()
        return {int(r.channel_id) for r in rows}


def get_backfill_channels() -> list[WatchedChannel]:
    with managed_session() as session:
        return session.exec(
            select(WatchedChannel)
            .where(
                WatchedChannel.is_enabled == True,
                WatchedChannel.backfill_enabled == True,
            )
            .order_by(WatchedChannel.channel_name, WatchedChannel.channel_id)
        ).all()


def seed_channels_from_env() -> None:
    with managed_session() as session:
        existing = session.exec(select(WatchedChannel)).all()
        existing_by_id = {row.channel_id: row for row in existing}

        for channel_id in settings.channel_ids:
            cid = str(channel_id)
            if cid in existing_by_id:
                row = existing_by_id[cid]
                if not row.channel_name:
                    row.channel_name = cid
                    row.updated_at = utcnow()
                    session.add(row)
                continue

            session.add(
                WatchedChannel(
                    channel_id=cid,
                    channel_name=cid,
                    is_enabled=True,
                    backfill_enabled=False,
                )
            )

        session.commit()


def get_attachment_payloads(message: discord.Message) -> list[dict]:
    payloads: list[dict] = []
    for attachment in message.attachments:
        filename = getattr(attachment, "filename", None) or ""
        content_type = getattr(attachment, "content_type", None)
        is_image = bool(
            (content_type and content_type.startswith("image/"))
            or filename.lower().endswith(IMAGE_ATTACHMENT_EXTENSIONS)
        )
        payloads.append(
            {
                "url": attachment.url,
                "filename": filename or None,
                "content_type": content_type,
                "is_image": is_image,
            }
        )
    return payloads


def _show_deals_watched_channel_name(channel: dict) -> str:
    return (
        str(channel.get("label") or "").strip()
        or f"{channel.get('category_name')} / #{channel.get('channel_name')}"
    )


def _sync_show_deals_watched_channels(
    session: Session,
    channels: list[dict],
    *,
    now: datetime,
) -> None:
    show_deals_channels = [
        channel
        for channel in channels
        if str(channel.get("category_name") or "").strip() == AUTO_WATCHED_CHANNEL_CATEGORY
        and looks_like_transaction_channel(
            str(channel.get("channel_name") or ""),
            str(channel.get("category_name") or ""),
        )
    ]
    if not show_deals_channels:
        return

    existing_rows = session.exec(select(WatchedChannel)).all()
    watched_by_channel_id = {row.channel_id: row for row in existing_rows}
    changed_count = 0
    created_count = 0

    for channel in show_deals_channels:
        channel_id = str(channel.get("channel_id") or "").strip()
        if not channel_id:
            continue

        channel_name = _show_deals_watched_channel_name(channel)
        existing = watched_by_channel_id.get(channel_id)
        if existing:
            if channel_name and existing.channel_name != channel_name:
                existing.channel_name = channel_name
                existing.updated_at = now
                session.add(existing)
                changed_count += 1
            continue

        row = WatchedChannel(
            channel_id=channel_id,
            channel_name=channel_name,
            is_enabled=True,
            backfill_enabled=True,
            created_at=now,
            updated_at=now,
        )
        watched_by_channel_id[channel_id] = row
        session.add(row)
        created_count += 1

    if created_count or changed_count:
        print(
            "[discord] synced Show Deals watched channels: "
            f"created={created_count}, renamed={changed_count}"
        )


def persist_available_discord_channels(channels: list[dict], *, remove_missing: bool = True) -> None:
    now = utcnow()
    try:
        with managed_session() as session:
            existing_rows = session.exec(select(AvailableDiscordChannel)).all()
            existing_by_channel_id = {row.channel_id: row for row in existing_rows}
            keep_channel_ids = {channel["channel_id"] for channel in channels}

            if remove_missing:
                for row in existing_rows:
                    if row.channel_id not in keep_channel_ids:
                        session.delete(row)

            for channel in channels:
                row = existing_by_channel_id.get(channel["channel_id"])
                if not row:
                    row = AvailableDiscordChannel(channel_id=channel["channel_id"])
                row.channel_name = channel["channel_name"]
                row.guild_id = channel.get("guild_id")
                row.guild_name = channel.get("guild_name")
                row.category_name = channel.get("category_name")
                row.label = channel["label"]
                row.created_at_discord = parse_iso_datetime(channel.get("created_at"))
                row.last_message_at = parse_iso_datetime(channel.get("last_message_at"))
                row.updated_at = now
                session.add(row)

            _sync_show_deals_watched_channels(session, channels, now=now)
            session.commit()
    except ProgrammingError:
        return


def merge_available_discord_channel_rows(*channel_lists: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for channels in channel_lists:
        for channel in channels:
            channel_id = str(channel.get("channel_id") or "").strip()
            if not channel_id:
                continue
            if channel_id not in merged:
                merged[channel_id] = dict(channel)

    rows = list(merged.values())
    rows.sort(
        key=lambda x: (
            (x.get("guild_name") or "").lower(),
            (x.get("category_name") or "").lower(),
            (x.get("channel_name") or x.get("label") or "").lower(),
        )
    )
    return rows


def get_cached_available_discord_channels() -> list[dict]:
    try:
        with managed_session() as session:
            rows = session.exec(
                select(AvailableDiscordChannel).order_by(
                    AvailableDiscordChannel.guild_name,
                    AvailableDiscordChannel.category_name,
                    AvailableDiscordChannel.channel_name,
                )
            ).all()
    except ProgrammingError:
        return []

    return [
        {
            "guild_id": row.guild_id or "",
            "guild_name": row.guild_name or "",
            "channel_id": row.channel_id,
            "channel_name": row.channel_name,
            "category_name": row.category_name,
            "label": row.label,
            "created_at": row.created_at_discord.isoformat() if row.created_at_discord else None,
            "last_message_at": row.last_message_at.isoformat() if row.last_message_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for row in rows
    ]


def _download_missing_attachments(missing_payloads: list[dict], message_id: int) -> list[dict]:
    """Download attachment files synchronously (intended to run in a thread)."""
    downloaded: list[dict] = []
    if not missing_payloads:
        return downloaded
    for payload in missing_payloads:
        try:
            data, content_type = download_attachment(payload["url"])
        except Exception as exc:
            ingest_log(
                action="attachment_cache_failed",
                level="error",
                success=False,
                error=str(exc),
                message_id=message_id,
                attachment_url=payload["url"],
            )
            continue
        downloaded.append(
            {
                "source_url": payload["url"],
                "filename": payload.get("filename"),
                "content_type": payload.get("content_type") or content_type,
                "is_image": bool(payload.get("is_image")),
                "data": data,
            }
        )
    return downloaded


def sync_attachment_assets(message_id: int, attachment_payloads: list[dict]) -> None:
    with managed_session() as session:
        existing_urls = {
            asset.source_url
            for asset in session.exec(
                select(AttachmentAsset).where(AttachmentAsset.message_id == message_id)
            ).all()
        }

    keep_urls = {payload["url"] for payload in attachment_payloads}
    missing_payloads = [
        payload
        for payload in attachment_payloads
        if payload["url"] not in existing_urls
    ]

    downloaded_payloads = _download_missing_attachments(missing_payloads, message_id)

    def write_assets(session: Session) -> tuple[list[tuple[int, str | None, str | None]], list[tuple[int, str | None, str | None, bytes]]]:
        existing_assets = session.exec(
            select(AttachmentAsset).where(AttachmentAsset.message_id == message_id)
        ).all()
        existing_by_url = {asset.source_url: asset for asset in existing_assets}
        deleted_assets: list[tuple[int, str | None, str | None]] = []
        new_assets: list[tuple[int, str | None, str | None, bytes]] = []

        for asset in existing_assets:
            if asset.source_url not in keep_urls:
                if asset.id is not None:
                    deleted_assets.append((asset.id, asset.filename, asset.content_type))
                session.delete(asset)

        for payload in downloaded_payloads:
            if payload["source_url"] in existing_by_url:
                continue
            asset = AttachmentAsset(
                message_id=message_id,
                source_url=payload["source_url"],
                filename=payload["filename"],
                content_type=payload["content_type"],
                is_image=bool(payload["is_image"]),
                data=payload["data"],
            )
            session.add(asset)
            session.flush()
            if asset.id is not None:
                new_assets.append((asset.id, asset.filename, asset.content_type, asset.data))
            existing_by_url[payload["source_url"]] = asset

        return deleted_assets, new_assets

    deleted_assets, new_assets = run_write_with_retry(write_assets)

    for asset_id, filename, content_type in deleted_assets:
        delete_attachment_cache_file(
            asset_id,
            filename=filename,
            content_type=content_type,
        )

    for asset_id, filename, content_type, data in new_assets:
        write_attachment_cache_file(
            asset_id,
            filename=filename,
            content_type=content_type,
            data=data,
        )


def get_message_row(session: Session, discord_message_id: str) -> Optional[DiscordMessage]:
    return session.exec(
        select(DiscordMessage).where(
            DiscordMessage.discord_message_id == discord_message_id
        )
    ).first()


def coerce_aware_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def recent_message_needs_refresh(existing: DiscordMessage, message: discord.Message) -> bool:
    existing_edited_at = coerce_aware_datetime(existing.edited_at)
    incoming_edited_at = coerce_aware_datetime(getattr(message, "edited_at", None))
    if incoming_edited_at and (existing_edited_at is None or incoming_edited_at > existing_edited_at):
        return True

    incoming_content = message.content or ""
    incoming_attachment_urls = json.dumps([payload["url"] for payload in get_attachment_payloads(message)])
    if existing.content != incoming_content:
        return True
    if existing.attachment_urls_json != incoming_attachment_urls:
        return True

    return False


def mark_message_deleted_row(
    session: Session,
    row: DiscordMessage,
    *,
    channel_name: Optional[str] = None,
    reason: str = "message deleted",
) -> bool:
    if row.is_deleted:
        return False

    row.is_deleted = True
    row.edited_at = utcnow()
    row.deleted_at = utcnow()
    row.parse_status = PARSE_IGNORED
    row.last_error = reason
    session.add(row)
    sync_transaction_from_message(session, row)
    session.commit()
    ingest_log(
        action="message_deleted",
        level="warning",
        success=True,
        session=session,
        message_id=row.id,
        discord_message_id=row.discord_message_id,
        channel_id=row.channel_id,
        channel=channel_name or row.channel_name,
        current_state=row.parse_status,
    )
    return True


async def recover_attachment_assets_for_message(
    *,
    channel_id: str,
    discord_message_id: str,
    message_row_id: int,
) -> bool:
    client = get_discord_client()
    if client is None or client.is_closed() or not client.is_ready():
        return False

    try:
        channel = client.get_channel(int(channel_id))
        if channel is None:
            channel = await client.fetch_channel(int(channel_id))
        message = await channel.fetch_message(int(discord_message_id))
    except Exception as exc:
        ingest_log(
            action="attachment_recovery_failed",
            level="error",
            success=False,
            error=str(exc),
            discord_message_id=discord_message_id,
            channel_id=channel_id,
            message_id=message_row_id,
        )
        return False

    await asyncio.to_thread(sync_attachment_assets, message_row_id, get_attachment_payloads(message))
    return True


def is_watched_channel(channel_id: int, watched_channel_ids: Optional[set[int]] = None) -> bool:
    if watched_channel_ids is None:
        watched_channel_ids = get_enabled_channel_ids()
    return channel_id in watched_channel_ids


def should_track_message(
    message: discord.Message,
    watched_channel_ids: Optional[set[int]] = None,
) -> bool:
    if message.author.bot:
        return False

    if not is_watched_channel(message.channel.id, watched_channel_ids):
        return False

    if not message.content and not message.attachments:
        return False

    return True


def insert_or_update_message(
    message: discord.Message,
    *,
    is_edit: bool = False,
    watched_channel_ids: Optional[set[int]] = None,
) -> tuple[bool, str]:
    if watched_channel_ids is None:
        ensure_available_discord_channel(message.channel)

    if not should_track_message(message, watched_channel_ids):
        return False, "ignored"

    attachment_payloads = get_attachment_payloads(message)
    attachment_urls = [payload["url"] for payload in attachment_payloads]

    with managed_session() as session:
        existing = get_message_row(session, str(message.id))

        if existing:
            existing.guild_id = str(message.guild.id) if message.guild else None
            existing.channel_id = str(message.channel.id)
            existing.channel_name = getattr(message.channel, "name", None)
            existing.author_id = str(message.author.id)
            existing.author_name = str(message.author)
            existing.content = message.content or ""
            existing.attachment_urls_json = json.dumps(attachment_urls)
            existing.last_seen_at = utcnow()
            existing.is_deleted = False
            existing.deleted_at = None

            discord_edited_at = getattr(message, "edited_at", None)
            if is_edit:
                existing.edited_at = discord_edited_at or utcnow()
                existing.parse_status = PARSE_PENDING
                existing.parse_attempts = 0
                existing.last_error = None
            elif discord_edited_at is not None:
                existing.edited_at = discord_edited_at

            session.add(existing)
            session.commit()
            if existing.id is not None:
                sync_attachment_assets(existing.id, attachment_payloads)
            ingest_log(
                action="message_updated",
                success=True,
                session=session,
                message_id=existing.id,
                discord_message_id=str(message.id),
                channel_id=str(message.channel.id),
                channel=getattr(message.channel, "name", None),
                current_state=existing.parse_status,
                is_edit=is_edit,
            )
            return True, "updated"

        row = DiscordMessage(
            discord_message_id=str(message.id),
            guild_id=str(message.guild.id) if message.guild else None,
            channel_id=str(message.channel.id),
            channel_name=getattr(message.channel, "name", None),
            author_id=str(message.author.id),
            author_name=str(message.author),
            content=message.content or "",
            attachment_urls_json=json.dumps(attachment_urls),
            created_at=message.created_at,
            last_seen_at=utcnow(),
            edited_at=getattr(message, "edited_at", None),
            parse_status=PARSE_PENDING,
            is_deleted=False,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        if row.id is not None:
            sync_attachment_assets(row.id, attachment_payloads)
        ingest_log(
            action="message_queued",
            success=True,
            session=session,
            message_id=row.id,
            discord_message_id=str(message.id),
            channel_id=str(message.channel.id),
            channel=getattr(message.channel, "name", None),
            current_state=row.parse_status,
            is_edit=is_edit,
        )
        return True, "inserted"


async def maybe_auto_import_bookkeeping_message(message: discord.Message) -> None:
    sheet_url = extract_google_sheet_url(message.content or "")
    if not sheet_url:
        return

    try:
        imported_id = await auto_import_public_google_sheet(
            message_text=message.content or "",
            created_at=message.created_at,
            sheet_url=sheet_url,
        )
        if imported_id:
            ingest_log(
                action="bookkeeping_auto_imported",
                success=True,
                discord_message_id=str(message.id),
                channel_id=str(message.channel.id),
                import_id=imported_id,
            )
    except Exception as exc:
        ingest_log(
            action="bookkeeping_auto_import_failed",
            level="error",
            success=False,
            error=str(exc),
            discord_message_id=str(message.id),
            channel_id=str(message.channel.id),
        )


def mark_message_deleted(message: discord.Message) -> bool:
    with managed_session() as session:
        existing = get_message_row(session, str(message.id))
        if not existing:
            return False
        return mark_message_deleted_row(
            session,
            existing,
            channel_name=getattr(message.channel, "name", None),
        )


async def audit_recent_channel_history(
    client: discord.Client,
    *,
    channel_id: int,
    limit: int,
    oldest_first: bool,
    after: Optional[datetime],
) -> dict:
    watched_channel_ids = get_enabled_channel_ids()
    if channel_id not in watched_channel_ids:
        return {
            "ok": False,
            "channel_id": channel_id,
            "error": "Channel is not currently enabled for ingestion",
        }

    channel = client.get_channel(channel_id)
    if channel is None:
        try:
            channel = await client.fetch_channel(channel_id)
        except Exception as exc:
            return {
                "ok": False,
                "channel_id": channel_id,
                "error": f"Unable to fetch channel: {exc}",
            }

    if not hasattr(channel, "history"):
        return {
            "ok": False,
            "channel_id": channel_id,
            "error": "Channel does not support message history auditing",
        }

    inserted_count = 0
    updated_count = 0
    skipped_count = 0
    deleted_count = 0
    processed_count = 0
    seen_message_ids: set[str] = set()

    if after is not None and after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)

    ingest_log(
        action="recent_audit_channel_started",
        success=True,
        channel_id=channel_id,
        channel_name=getattr(channel, "name", None),
        after=after.isoformat() if after else None,
        limit=limit,
        oldest_first=oldest_first,
    )

    try:
        async for message in channel.history(
            limit=limit,
            oldest_first=oldest_first,
            after=after,
        ):
            processed_count += 1
            seen_message_ids.add(str(message.id))

            with managed_session() as session:
                existing = get_message_row(session, str(message.id))

            should_refresh = bool(existing and recent_message_needs_refresh(existing, message))
            if existing is not None and not should_refresh:
                skipped_count += 1
            else:
                ok, action = insert_or_update_message(
                    message,
                    is_edit=should_refresh,
                    watched_channel_ids=watched_channel_ids,
                )
                if not ok:
                    skipped_count += 1
                elif action == "inserted":
                    inserted_count += 1
                else:
                    updated_count += 1

        with managed_session() as session:
            stmt = (
                select(DiscordMessage)
                .where(DiscordMessage.channel_id == str(channel_id))
                .where(DiscordMessage.is_deleted == False)  # noqa: E712
                .order_by(DiscordMessage.created_at.asc(), DiscordMessage.id.asc())
            )
            if after is not None:
                stmt = stmt.where(DiscordMessage.created_at >= after)
            recent_rows = session.exec(stmt.limit(limit)).all()

        for row in recent_rows:
            if row.discord_message_id in seen_message_ids:
                continue

            try:
                fetched_message = await channel.fetch_message(int(row.discord_message_id))
            except discord.NotFound:
                with managed_session() as session:
                    db_row = session.get(DiscordMessage, row.id) if row.id is not None else None
                    if db_row is not None and mark_message_deleted_row(
                        session,
                        db_row,
                        channel_name=getattr(channel, "name", None),
                        reason="message deleted during recent offline audit",
                    ):
                        deleted_count += 1
                continue
            except discord.Forbidden as exc:
                ingest_log(
                    action="recent_audit_message_fetch_forbidden",
                    level="warning",
                    success=False,
                    error=str(exc),
                    message_id=row.id,
                    discord_message_id=row.discord_message_id,
                    channel_id=row.channel_id,
                    channel_name=getattr(channel, "name", None),
                )
                skipped_count += 1
                continue
            except Exception as exc:
                ingest_log(
                    action="recent_audit_message_fetch_failed",
                    level="warning",
                    success=False,
                    error=str(exc),
                    message_id=row.id,
                    discord_message_id=row.discord_message_id,
                    channel_id=row.channel_id,
                    channel_name=getattr(channel, "name", None),
                )
                skipped_count += 1
                continue

            with managed_session() as session:
                existing = session.get(DiscordMessage, row.id) if row.id is not None else None

            if existing is None:
                continue

            should_refresh = recent_message_needs_refresh(existing, fetched_message)
            if should_refresh:
                ok, action = insert_or_update_message(
                    fetched_message,
                    is_edit=True,
                    watched_channel_ids=watched_channel_ids,
                )
                if not ok:
                    skipped_count += 1
                elif action == "inserted":
                    inserted_count += 1
                else:
                    updated_count += 1
            else:
                skipped_count += 1

    except Exception as exc:
        ingest_log(
            action="recent_audit_channel_failed",
            level="error",
            success=False,
            error=str(exc),
            channel_id=channel_id,
            channel_name=getattr(channel, "name", None),
            after=after.isoformat() if after else None,
            limit=limit,
            oldest_first=oldest_first,
        )
        return {
            "ok": False,
            "channel_id": channel_id,
            "channel_name": getattr(channel, "name", None),
            "error": f"Recent audit failed while reading channel history: {exc}",
        }

    ingest_log(
        action="recent_audit_channel_completed",
        success=True,
        channel_id=channel_id,
        channel_name=getattr(channel, "name", None),
        after=after.isoformat() if after else None,
        limit=limit,
        oldest_first=oldest_first,
        processed_count=processed_count,
        inserted_count=inserted_count,
        updated_count=updated_count,
        skipped_count=skipped_count,
        deleted_count=deleted_count,
    )
    return {
        "ok": True,
        "channel_id": channel_id,
        "channel_name": getattr(channel, "name", None),
        "inserted": inserted_count,
        "updated": updated_count,
        "skipped": skipped_count,
        "deleted": deleted_count,
        "processed": processed_count,
        "limit": limit,
        "after": after.isoformat() if after else None,
    }


async def audit_recent_enabled_channels(
    client: discord.Client,
    *,
    limit_per_channel: int,
    oldest_first: bool,
    after: Optional[datetime],
) -> dict:
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    total_deleted = 0
    results = []
    all_ok = True

    for channel_id in sorted(get_enabled_channel_ids()):
        result = await audit_recent_channel_history(
            client,
            channel_id=channel_id,
            limit=limit_per_channel,
            oldest_first=oldest_first,
            after=after,
        )
        results.append(result)
        if result.get("ok"):
            total_inserted += result.get("inserted", 0)
            total_updated += result.get("updated", 0)
            total_skipped += result.get("skipped", 0)
            total_deleted += result.get("deleted", 0)
        else:
            all_ok = False

    return {
        "ok": all_ok,
        "results": results,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "total_skipped": total_skipped,
        "total_deleted": total_deleted,
        "after": after.isoformat() if after else None,
    }


async def run_periodic_recent_audit_once(client: discord.Client) -> Optional[dict]:
    if client is None or client.is_closed() or not client.is_ready():
        return None

    recent_after = None
    if settings.periodic_offline_audit_lookback_hours > 0:
        recent_after = utcnow() - timedelta(hours=settings.periodic_offline_audit_lookback_hours)

    result = await audit_recent_enabled_channels(
        client,
        limit_per_channel=settings.periodic_offline_audit_limit_per_channel,
        oldest_first=True,
        after=recent_after,
    )
    discord_runtime_state["last_recent_audit_at"] = utcnow().isoformat()
    discord_runtime_state["last_recent_audit_summary"] = result
    ingest_log(
        action="periodic_recent_audit_completed",
        success=bool(result.get("ok")),
        after=result.get("after"),
        total_inserted=result.get("total_inserted", 0),
        total_updated=result.get("total_updated", 0),
        total_skipped=result.get("total_skipped", 0),
        total_deleted=result.get("total_deleted", 0),
    )
    return result


async def run_periodic_attachment_repair_once(client: discord.Client) -> Optional[dict]:
    if client is None or client.is_closed() or not client.is_ready():
        return None

    repair_since = None
    if settings.periodic_attachment_repair_lookback_hours > 0:
        repair_since = utcnow() - timedelta(hours=settings.periodic_attachment_repair_lookback_hours)

    repair_before = None
    if settings.periodic_attachment_repair_min_age_minutes > 0:
        repair_before = utcnow() - timedelta(minutes=settings.periodic_attachment_repair_min_age_minutes)

    watched_channel_ids = {str(channel_id) for channel_id in get_enabled_channel_ids()}
    with managed_session() as session:
        candidates = [
            candidate
            for candidate in attachment_repair_candidate_query(
                session,
                since=repair_since,
                before=repair_before,
                limit=settings.periodic_attachment_repair_limit,
            )
            if candidate.missing_attachment_count > 0
            and (not watched_channel_ids or candidate.channel_id in watched_channel_ids)
        ]

    restored_url_assets = 0
    repaired_rows = 0
    failed_rows = 0
    discord_fallback_rows = 0

    for candidate in candidates:
        restored_count, failed_count = restore_missing_assets_from_urls(
            candidate.message_id,
            candidate.missing_attachment_urls,
        )
        restored_url_assets += restored_count

        recovered_via_discord = False
        if failed_count > 0:
            recovered_via_discord = await recover_attachment_assets_for_message(
                channel_id=candidate.channel_id,
                discord_message_id=candidate.discord_message_id,
                message_row_id=candidate.message_id,
            )
            if recovered_via_discord:
                discord_fallback_rows += 1

        asset_count, _cached_count = row_status_snapshot(candidate.message_id)
        if asset_count >= len(candidate.attachment_urls):
            repaired_rows += 1
        else:
            failed_rows += 1
            ingest_log(
                action="periodic_attachment_repair_incomplete",
                level="warning",
                success=False,
                message_id=candidate.message_id,
                channel_id=candidate.channel_id,
                discord_message_id=candidate.discord_message_id,
                expected_assets=len(candidate.attachment_urls),
                actual_assets=asset_count,
                restored_url_assets=restored_count,
                used_discord_fallback=recovered_via_discord,
            )

    result = {
        "processed_candidates": len(candidates),
        "repaired_rows": repaired_rows,
        "failed_rows": failed_rows,
        "discord_fallback_rows": discord_fallback_rows,
        "restored_url_assets": restored_url_assets,
        "lookback_hours": settings.periodic_attachment_repair_lookback_hours,
        "min_age_minutes": settings.periodic_attachment_repair_min_age_minutes,
        "limit": settings.periodic_attachment_repair_limit,
        "since": repair_since.isoformat() if repair_since else None,
        "before": repair_before.isoformat() if repair_before else None,
    }
    discord_runtime_state["last_attachment_repair_at"] = utcnow().isoformat()
    discord_runtime_state["last_attachment_repair_summary"] = result
    ingest_log(
        action="periodic_attachment_repair_completed",
        success=failed_rows == 0,
        processed_candidates=result["processed_candidates"],
        repaired_rows=repaired_rows,
        failed_rows=failed_rows,
        discord_fallback_rows=discord_fallback_rows,
        restored_url_assets=restored_url_assets,
        since=result["since"],
        before=result["before"],
        limit=result["limit"],
    )
    return result


async def recent_message_audit_loop(stop_event: asyncio.Event, get_client) -> None:
    if not settings.periodic_offline_audit_enabled:
        return

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=max(settings.periodic_offline_audit_interval_minutes, 1) * 60,
            )
            break
        except asyncio.TimeoutError:
            pass

        client = get_client()
        try:
            await run_periodic_recent_audit_once(client)
        except Exception as exc:
            ingest_log(
                action="periodic_recent_audit_failed",
                level="error",
                success=False,
                error=str(exc),
            )


async def periodic_attachment_repair_loop(stop_event: asyncio.Event, get_client) -> None:
    if not settings.periodic_attachment_repair_enabled:
        return

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=max(settings.periodic_attachment_repair_interval_minutes, 1) * 60,
            )
            break
        except asyncio.TimeoutError:
            pass

        client = get_client()
        try:
            await run_periodic_attachment_repair_once(client)
        except Exception as exc:
            ingest_log(
                action="periodic_attachment_repair_failed",
                level="error",
                success=False,
                error=str(exc),
            )



class DealIngestBot(discord.Client):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ready_event = asyncio.Event()
        self.startup_backfill_done = False
        self.startup_recent_audit_done = False
        self.startup_recent_audit_task: Optional[asyncio.Task] = None

    async def run_startup_recent_audit(self) -> None:
        try:
            recent_after = None
            if settings.startup_offline_audit_lookback_hours > 0:
                recent_after = utcnow() - timedelta(hours=settings.startup_offline_audit_lookback_hours)
                print(
                    "[discord] running startup offline audit "
                    f"for the last {settings.startup_offline_audit_lookback_hours:g}h"
                )

            result = await audit_recent_enabled_channels(
                self,
                limit_per_channel=settings.startup_offline_audit_limit_per_channel,
                oldest_first=settings.startup_offline_audit_oldest_first,
                after=recent_after,
            )
            discord_runtime_state["last_recent_audit_at"] = utcnow().isoformat()
            discord_runtime_state["last_recent_audit_summary"] = result
            ingest_log(
                action="startup_recent_audit_completed",
                success=bool(result.get("ok")),
                after=result.get("after"),
                total_inserted=result.get("total_inserted", 0),
                total_updated=result.get("total_updated", 0),
                total_skipped=result.get("total_skipped", 0),
                total_deleted=result.get("total_deleted", 0),
            )
        except Exception as exc:
            ingest_log(
                action="startup_recent_audit_failed",
                level="error",
                success=False,
                error=str(exc),
            )

    async def on_ready(self):
        print(f"[discord] logged in as {self.user}")
        discord_runtime_state["status"] = "ready"
        discord_runtime_state["error"] = None
        try:
            guild_channel_pairs, authoritative = await _fetch_live_guild_channels_rest(self)
            if not guild_channel_pairs:
                guild_channel_pairs = _gateway_guild_channel_pairs(self)
                authoritative = False
            channels = _build_available_discord_channel_rows(guild_channel_pairs)
            if not authoritative:
                channels = merge_available_discord_channel_rows(
                    channels,
                    get_cached_available_discord_channels(),
                )
            _cache_and_persist_available_discord_channels(
                channels,
                remove_missing=authoritative,
            )
        except Exception as exc:
            print(f"[discord] failed to cache channel inventory: {exc}")

        if not self.startup_backfill_done and settings.startup_backfill_enabled:
            self.startup_backfill_done = True
            try:
                startup_after = None
                if settings.startup_backfill_lookback_hours > 0:
                    startup_after = utcnow() - timedelta(hours=settings.startup_backfill_lookback_hours)
                    print(
                        "[discord] running startup catch-up backfill "
                        f"for the last {settings.startup_backfill_lookback_hours:g}h"
                    )
                await self.backfill_enabled_channels(
                    limit_per_channel=settings.startup_backfill_limit_per_channel,
                    oldest_first=settings.startup_backfill_oldest_first,
                    after=startup_after,
                )
            except Exception as exc:
                discord_runtime_state["status"] = "degraded"
                discord_runtime_state["error"] = f"startup backfill failed: {exc}"
                ingest_log(
                    action="startup_backfill_failed",
                    level="error",
                    success=False,
                    error=str(exc),
                )

        if not self.startup_recent_audit_done and settings.startup_offline_audit_enabled:
            self.startup_recent_audit_done = True
            self.startup_recent_audit_task = asyncio.create_task(
                self.run_startup_recent_audit(),
                name="discord-recent-audit",
            )

        self.ready_event.set()

    async def on_message(self, message: discord.Message):
        ok, action = insert_or_update_message(message, is_edit=False)
        if ok and action == "inserted":
            print(f"[discord] live ingested message {message.id}")
            asyncio.create_task(maybe_auto_import_bookkeeping_message(message))
        elif ok and action == "updated":
            print(f"[discord] refreshed existing message {message.id}")
            asyncio.create_task(maybe_auto_import_bookkeeping_message(message))

    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        ok, action = insert_or_update_message(after, is_edit=True)
        if ok:
            print(f"[discord] edited message {after.id} -> {action}")
            asyncio.create_task(maybe_auto_import_bookkeeping_message(after))

    async def on_message_delete(self, message: discord.Message):
        ok = mark_message_deleted(message)
        if ok:
            print(f"[discord] deleted message {message.id}")

    async def on_guild_channel_create(self, channel):
        ensure_available_discord_channel(channel, force=True)

    async def on_guild_channel_update(self, before, after):
        ensure_available_discord_channel(after, force=True)

    async def backfill_channel(
        self,
        channel_id: int,
        limit: Optional[int] = None,
        oldest_first: bool = True,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> dict:
        watched_channel_ids = get_enabled_channel_ids()
        if channel_id not in watched_channel_ids:
            return {
                "ok": False,
                "channel_id": channel_id,
                "error": "Channel is not currently enabled for ingestion",
            }

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception as e:
                return {
                    "ok": False,
                    "channel_id": channel_id,
                    "error": f"Unable to fetch channel: {e}",
                }

        if not hasattr(channel, "history"):
            return {
                "ok": False,
                "channel_id": channel_id,
                "error": "Channel does not support message history backfill",
            }

        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        processed_count = 0

        if progress_callback:
            await progress_callback(
                {
                    "event_type": "backfill_channel_started",
                    "message": f"Started channel backfill for {getattr(channel, 'name', channel_id)}",
                    "details": {
                        "channel_id": channel_id,
                        "channel_name": getattr(channel, "name", None),
                        "after": after.isoformat() if after else None,
                        "before": before.isoformat() if before else None,
                        "limit": limit,
                        "oldest_first": oldest_first,
                    },
                }
            )

        try:
            async for message in channel.history(
                limit=limit,
                oldest_first=oldest_first,
                after=after,
                before=before,
            ):
                processed_count += 1
                if not should_track_message(message, watched_channel_ids):
                    skipped_count += 1
                else:
                    with managed_session() as session:
                        existing = get_message_row(session, str(message.id))
                    should_refresh = bool(existing and recent_message_needs_refresh(existing, message))
                    ok, action = insert_or_update_message(
                        message,
                        is_edit=should_refresh,
                        watched_channel_ids=watched_channel_ids,
                    )
                    if not ok:
                        skipped_count += 1
                    elif action == "inserted":
                        inserted_count += 1
                    else:
                        updated_count += 1

                # Cancellation is checked inside the progress callback. We emit
                # progress on the first message and then every few messages so
                # smaller backfills and long-running imports can stop promptly.
                if progress_callback and (
                    processed_count == 1
                    or processed_count % BACKFILL_PROGRESS_EVERY_MESSAGES == 0
                ):
                    preview = (message.content or "").strip().replace("\n", " ")
                    if len(preview) > 140:
                        preview = f"{preview[:137]}..."
                    await progress_callback(
                        {
                            "event_type": "backfill_channel_progress",
                            "message": (
                                f"{getattr(channel, 'name', channel_id)}: processed {processed_count} messages "
                                f"(inserted={inserted_count}, updated={updated_count}, skipped={skipped_count})"
                            ),
                            "details": {
                                "channel_id": channel_id,
                                "channel_name": getattr(channel, "name", None),
                                "processed_count": processed_count,
                                "inserted_count": inserted_count,
                                "updated_count": updated_count,
                                "skipped_count": skipped_count,
                                "last_message_id": str(message.id),
                                "last_message_author": getattr(message.author, "display_name", None) or getattr(message.author, "name", None),
                                "last_message_created_at": message.created_at.isoformat() if getattr(message, "created_at", None) else None,
                                "last_message_preview": preview,
                            },
                        }
                    )
        except Exception as e:
            if progress_callback:
                await progress_callback(
                    {
                        "event_type": "backfill_channel_failed",
                        "level": "error",
                        "message": f"Channel backfill failed for {getattr(channel, 'name', channel_id)}: {e}",
                        "details": {
                            "channel_id": channel_id,
                            "channel_name": getattr(channel, "name", None),
                            "processed_count": processed_count,
                            "inserted_count": inserted_count,
                            "updated_count": updated_count,
                            "skipped_count": skipped_count,
                            "error": str(e),
                        },
                    }
                )
            return {
                "ok": False,
                "channel_id": channel_id,
                "channel_name": getattr(channel, "name", None),
                "error": f"Backfill failed while reading channel history: {e}",
            }

        if progress_callback:
            await progress_callback(
                {
                    "event_type": "backfill_channel_completed",
                    "message": (
                        f"Completed channel backfill for {getattr(channel, 'name', channel_id)}: "
                        f"processed={processed_count}, inserted={inserted_count}, updated={updated_count}, skipped={skipped_count}"
                    ),
                    "details": {
                        "channel_id": channel_id,
                        "channel_name": getattr(channel, "name", None),
                        "processed_count": processed_count,
                        "inserted_count": inserted_count,
                        "updated_count": updated_count,
                        "skipped_count": skipped_count,
                    },
                }
            )

        return {
            "ok": True,
            "channel_id": channel_id,
            "channel_name": getattr(channel, "name", None),
            "inserted": inserted_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "limit": limit,
            "after": after.isoformat() if after else None,
            "before": before.isoformat() if before else None,
        }

    async def backfill_enabled_channels(
        self,
        limit_per_channel: Optional[int] = None,
        oldest_first: bool = True,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> dict:
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        results = []
        all_ok = True

        for watched_channel in get_backfill_channels():
            channel_after = after if after is not None else watched_channel.backfill_after
            channel_before = before if before is not None else watched_channel.backfill_before

            if after is None and before is None and channel_after is None and channel_before is None:
                if progress_callback:
                    await progress_callback(
                        {
                            "event_type": "backfill_channel_skipped",
                            "message": f"Skipped channel {watched_channel.channel_name or watched_channel.channel_id}: no backfill range configured",
                            "details": {
                                "channel_id": int(watched_channel.channel_id),
                                "channel_name": watched_channel.channel_name,
                                "skipped_reason": "no backfill range configured",
                            },
                        }
                    )
                results.append(
                    {
                        "ok": True,
                        "channel_id": int(watched_channel.channel_id),
                        "channel_name": watched_channel.channel_name,
                        "inserted": 0,
                        "updated": 0,
                        "skipped": 0,
                        "skipped_reason": "no backfill range configured",
                    }
                )
                continue

            result = await self.backfill_channel(
                channel_id=int(watched_channel.channel_id),
                limit=limit_per_channel,
                oldest_first=oldest_first,
                after=channel_after,
                before=channel_before,
                progress_callback=progress_callback,
            )
            results.append(result)

            if result.get("ok"):
                total_inserted += result.get("inserted", 0)
                total_updated += result.get("updated", 0)
                total_skipped += result.get("skipped", 0)
            else:
                all_ok = False

        return {
            "ok": all_ok,
            "results": results,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "total_skipped": total_skipped,
            "after": after.isoformat() if after else None,
            "before": before.isoformat() if before else None,
        }


async def run_discord_bot(stop_event: asyncio.Event):
    global discord_client_instance

    if not settings.discord_ingest_enabled:
        discord_runtime_state["status"] = "disabled"
        discord_runtime_state["error"] = None
        print("[discord] ingestion disabled by configuration")
        return

    if not settings.discord_bot_token.strip():
        discord_runtime_state["status"] = "disabled"
        discord_runtime_state["error"] = "missing DISCORD_BOT_TOKEN"
        print("[discord] ingestion disabled because DISCORD_BOT_TOKEN is empty")
        return

    intents = discord.Intents.default()
    intents.message_content = True
    intents.guilds = True
    intents.messages = True

    retry_delay = DISCORD_RETRY_MIN_SECONDS
    discord_runtime_state["status"] = "starting"
    discord_runtime_state["error"] = None

    try:
        while not stop_event.is_set():
            client = DealIngestBot(intents=intents)
            discord_client_instance = client
            discord_runtime_state["status"] = "starting"
            discord_runtime_state["error"] = None

            try:
                async with client:
                    bot_task = asyncio.create_task(client.start(settings.discord_bot_token))
                    stop_task = asyncio.create_task(stop_event.wait())
                    done, pending = await asyncio.wait(
                        {bot_task, stop_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for task in pending:
                        task.cancel()

                    if stop_task in done:
                        await client.close()
                        await asyncio.gather(bot_task, return_exceptions=True)
                        break

                    result = await asyncio.gather(bot_task, return_exceptions=True)
                    exc = result[0]
                    if isinstance(exc, Exception):
                        raise exc
                    retry_delay = DISCORD_RETRY_MIN_SECONDS
            except asyncio.CancelledError:
                raise
            except discord.HTTPException as exc:
                status_code = getattr(exc, "status", None)
                if status_code == 429:
                    discord_runtime_state["status"] = "rate_limited"
                    discord_runtime_state["error"] = f"Discord rate limited startup/connect; retrying in {retry_delay}s"
                    ingest_log(
                        action="discord_rate_limited",
                        level="warning",
                        success=False,
                        error=str(exc),
                        status_code=status_code,
                        retry_delay_seconds=retry_delay,
                    )
                else:
                    discord_runtime_state["status"] = "degraded"
                    discord_runtime_state["error"] = f"Discord HTTP error ({status_code}): {exc}; retrying in {retry_delay}s"
                    ingest_log(
                        action="discord_http_error",
                        level="error",
                        success=False,
                        error=str(exc),
                        status_code=status_code,
                        retry_delay_seconds=retry_delay,
                    )
                retry_delay = min(retry_delay * 2, DISCORD_RETRY_MAX_SECONDS)
            except Exception as exc:
                discord_runtime_state["status"] = "degraded"
                discord_runtime_state["error"] = f"Discord connection failed: {exc}; retrying in {retry_delay}s"
                ingest_log(
                    action="discord_connection_failed",
                    level="error",
                    success=False,
                    error=str(exc),
                    retry_delay_seconds=retry_delay,
                )
                retry_delay = min(retry_delay * 2, DISCORD_RETRY_MAX_SECONDS)
            finally:
                discord_client_instance = None

            if stop_event.is_set():
                break

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=retry_delay)
            except asyncio.TimeoutError:
                pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        discord_runtime_state["status"] = "error"
        discord_runtime_state["error"] = str(exc)
        ingest_log(
            action="discord_task_crashed",
            level="error",
            success=False,
            error=str(exc),
        )
    finally:
        if discord_runtime_state["status"] not in {"disabled", "error", "rate_limited", "degraded"}:
            discord_runtime_state["status"] = "stopped"
            discord_runtime_state["error"] = None
        discord_client_instance = None


def get_discord_client() -> Optional[DealIngestBot]:
    return discord_client_instance


def snowflake_to_datetime(snowflake_id: Optional[int]) -> Optional[datetime]:
    if not snowflake_id:
        return None
    discord_epoch = 1420070400000
    timestamp_ms = (int(snowflake_id) >> 22) + discord_epoch
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def looks_like_transaction_channel(channel_name: str, category_name: Optional[str]) -> bool:
    lower_name = (channel_name or "").lower()
    lower_category = (category_name or "").lower()

    if lower_category in {"show deals", "past shows", "offline deals"}:
        return True

    return any(token in lower_name for token in TRANSACTION_CHANNEL_NAME_HINTS)


def invalidate_available_channels_cache() -> None:
    with _available_channels_cache_lock:
        _available_channels_cache["expires_at"] = 0.0
        _available_channels_cache["channels"] = []


def _resolve_channel_category_name(channel, category_by_id: dict | None = None) -> Optional[str]:
    category = getattr(channel, "category", None)
    if category is not None:
        name = getattr(category, "name", None)
        if name:
            return name

    category_id = getattr(channel, "category_id", None)
    if category_id is None:
        return None

    if category_by_id:
        category = category_by_id.get(category_id) or category_by_id.get(str(category_id))
        if category is not None:
            name = getattr(category, "name", None)
            if name:
                return name

    guild = getattr(channel, "guild", None)
    get_channel = getattr(guild, "get_channel", None)
    if callable(get_channel):
        try:
            category = get_channel(int(category_id))
        except (TypeError, ValueError):
            category = get_channel(category_id)
        if category is not None:
            return getattr(category, "name", None)

    return None


def _gateway_guild_channel_pairs(client) -> list[tuple]:
    return [
        (guild, channel, _resolve_channel_category_name(channel))
        for guild in list(client.guilds)
        for channel in getattr(guild, "text_channels", [])
    ]


def _build_available_discord_channel_rows(guild_channel_pairs: list[tuple]) -> list[dict]:
    channels: list[dict] = []
    for guild, channel, category_name in guild_channel_pairs:
        if category_name not in ALLOWED_CHANNEL_CATEGORIES:
            continue
        if not looks_like_transaction_channel(channel.name, category_name):
            continue

        channels.append(
            {
                "guild_id": str(guild.id),
                "guild_name": guild.name,
                "channel_id": str(channel.id),
                "channel_name": channel.name,
                "category_name": category_name,
                "label": f"{category_name} / #{channel.name}",
                "created_at": channel.created_at.isoformat() if getattr(channel, "created_at", None) else None,
                "last_message_at": (
                    snowflake_to_datetime(getattr(channel, "last_message_id", None)).isoformat()
                    if getattr(channel, "last_message_id", None) else None
                ),
            }
        )

    return merge_available_discord_channel_rows(channels)


def _available_channel_cache_contains(channel_id: str) -> bool:
    now = time.monotonic()
    with _available_channels_cache_lock:
        if float(_available_channels_cache["expires_at"]) <= now:
            return False
        return any(
            str(channel.get("channel_id") or "") == channel_id
            for channel in _available_channels_cache["channels"]
        )


def ensure_available_discord_channel(channel, *, force: bool = False) -> bool:
    """Persist one visible Discord channel before watched-channel filtering.

    New private Show Deals channels can receive messages before the worker has
    rebuilt its full Discord inventory. Persisting the single channel here lets
    the existing Show Deals auto-watch sync run before should_track_message()
    checks WatchedChannel.
    """
    channel_id = str(getattr(channel, "id", "") or "").strip()
    guild = getattr(channel, "guild", None)
    if not channel_id or guild is None:
        return False

    if not force and _available_channel_cache_contains(channel_id):
        return True

    category_name = _resolve_channel_category_name(channel)
    channel_rows = _build_available_discord_channel_rows([(guild, channel, category_name)])
    if not channel_rows:
        return False

    channels = merge_available_discord_channel_rows(
        channel_rows,
        get_cached_available_discord_channels(),
    )
    _cache_and_persist_available_discord_channels(
        channels,
        remove_missing=False,
    )
    return True


def _cache_and_persist_available_discord_channels(
    channels: list[dict],
    *,
    now: float | None = None,
    remove_missing: bool,
) -> list[dict]:
    cache_now = time.monotonic() if now is None else now
    with _available_channels_cache_lock:
        _available_channels_cache["channels"] = list(channels)
        _available_channels_cache["expires_at"] = cache_now + AVAILABLE_CHANNELS_CACHE_SECONDS
    try:
        persist_available_discord_channels(channels, remove_missing=remove_missing)
    except Exception as exc:
        print(f"[discord] failed to cache channel inventory: {exc}")
    return channels


async def _fetch_live_guild_channels_rest(client) -> tuple[list[tuple], bool]:
    """REST-authoritative channel walk. Bypasses discord.py's gateway cache,
    which can miss private channels the bot was added to after creation
    (no GUILD_CHANNEL_CREATE was delivered at creation time)."""
    live: list = []
    authoritative = True
    for guild in list(client.guilds):
        try:
            fetched = await guild.fetch_channels()
        except Exception as exc:
            authoritative = False
            print(f"[discord] fetch_channels failed for guild {guild.id}: {exc}")
            # Fall back to cached text_channels for this guild so we don't
            # regress when one guild's REST call hiccups.
            for channel in getattr(guild, "text_channels", []):
                live.append((guild, channel, _resolve_channel_category_name(channel)))
            continue

        category_by_id = {
            getattr(channel, "id", None): channel
            for channel in fetched
            if isinstance(channel, discord.CategoryChannel)
        }
        category_by_id.update(
            {
                str(channel_id): channel
                for channel_id, channel in list(category_by_id.items())
                if channel_id is not None
            }
        )
        for channel in fetched:
            # Only text-channel-like objects carry .category and a name we
            # can match against our hints. Skip voice/stage/forum/etc.
            if not isinstance(channel, discord.TextChannel):
                continue
            live.append((guild, channel, _resolve_channel_category_name(channel, category_by_id)))
    return live, authoritative


def list_available_discord_channels(*, force_refresh: bool = False) -> list[dict]:
    now = time.monotonic()
    if not force_refresh:
        with _available_channels_cache_lock:
            if float(_available_channels_cache["expires_at"]) > now:
                return list(_available_channels_cache["channels"])

    client = get_discord_client()
    if client is None or client.is_closed() or not client.is_ready():
        cached_channels = get_cached_available_discord_channels()
        with _available_channels_cache_lock:
            _available_channels_cache["channels"] = list(cached_channels)
            _available_channels_cache["expires_at"] = now + AVAILABLE_CHANNELS_CACHE_SECONDS
        return cached_channels

    guild_channel_pairs: list = []
    authoritative = False
    client_loop = getattr(client, "loop", None)
    running_on_client_loop = False
    try:
        running_on_client_loop = asyncio.get_running_loop() is client_loop
    except RuntimeError:
        running_on_client_loop = False

    # Normal admin/table loads also use REST once the in-memory cache expires.
    # The gateway cache can miss private channels that were created before the
    # bot gained access, while Guild.fetch_channels reflects current grants.
    if (
        client_loop is not None
        and not client_loop.is_closed()
        and not running_on_client_loop
    ):
        future = None
        try:
            future = asyncio.run_coroutine_threadsafe(
                _fetch_live_guild_channels_rest(client),
                client_loop,
            )
            guild_channel_pairs, authoritative = future.result(timeout=AVAILABLE_CHANNELS_REST_TIMEOUT_SECONDS)
        except Exception as exc:
            if future is not None:
                future.cancel()
            print(f"[discord] REST channel inventory failed, falling back to gateway cache: {exc}")
            guild_channel_pairs = []
            authoritative = False

    if not guild_channel_pairs:
        # Default / fallback: walk the gateway-cached guilds in-process.
        guild_channel_pairs = _gateway_guild_channel_pairs(client)

    channels = _build_available_discord_channel_rows(guild_channel_pairs)
    if not authoritative:
        channels = merge_available_discord_channel_rows(
            channels,
            get_cached_available_discord_channels(),
        )
    return _cache_and_persist_available_discord_channels(
        channels,
        now=now,
        remove_missing=authoritative,
    )
