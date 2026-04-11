import asyncio
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

import discord
import httpx
from sqlalchemy.exc import ProgrammingError
from sqlmodel import Session, select

from .attachment_repair import (
    attachment_repair_candidate_query,
    restore_missing_assets_from_urls,
    row_status_snapshot,
)
from .attachment_storage import delete_attachment_cache_file, write_attachment_cache_file
from .bookkeeping import auto_import_public_google_sheet, extract_google_sheet_url
from .config import get_settings
from .db import engine, managed_session, run_write_with_retry
from .models import (
    AttachmentAsset,
    AvailableDiscordChannel,
    DiscordMessage,
    PARSE_IGNORED,
    PARSE_PENDING,
    WatchedChannel,
)
from .ops_log import write_operations_log
from .runtime_logging import structured_log_line
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
DISCORD_RETRY_MIN_SECONDS = 15
DISCORD_RETRY_MAX_SECONDS = 900
ALLOWED_CHANNEL_CATEGORIES = {
    "Employees",
    "Show Deals",
    "Past Shows",
    "Offline Deals",
}
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


def persist_available_discord_channels(channels: list[dict]) -> None:
    now = utcnow()
    try:
        with managed_session() as session:
            existing_rows = session.exec(select(AvailableDiscordChannel)).all()
            existing_by_channel_id = {row.channel_id: row for row in existing_rows}
            keep_channel_ids = {channel["channel_id"] for channel in channels}

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

            session.commit()
    except ProgrammingError:
        return


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
    with httpx.Client(follow_redirects=True, timeout=20.0) as client:
        for payload in missing_payloads:
            try:
                response = client.get(payload["url"])
                response.raise_for_status()
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
                    "content_type": payload.get("content_type") or response.headers.get("content-type"),
                    "is_image": bool(payload.get("is_image")),
                    "data": response.content,
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
            persist_available_discord_channels(list_available_discord_channels())
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


def list_available_discord_channels() -> list[dict]:
    now = time.monotonic()
    with _available_channels_cache_lock:
        if float(_available_channels_cache["expires_at"]) > now:
            return list(_available_channels_cache["channels"])

    client = get_discord_client()
    if client is None or client.is_closed() or not client.is_ready():
        cached_channels = get_cached_available_discord_channels()
        with _available_channels_cache_lock:
            _available_channels_cache["channels"] = list(cached_channels)
            _available_channels_cache["expires_at"] = now + 30.0
        return cached_channels

    channels: list[dict] = []

    for guild in client.guilds:
        for channel in guild.text_channels:
            category = getattr(channel, "category", None)
            category_name = category.name if category else None

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

    channels.sort(
        key=lambda x: (
            x["guild_name"].lower(),
            (x["category_name"] or "").lower(),
            x["channel_name"].lower(),
        )
    )
    with _available_channels_cache_lock:
        _available_channels_cache["channels"] = list(channels)
        _available_channels_cache["expires_at"] = now + 30.0
    try:
        persist_available_discord_channels(channels)
    except Exception as exc:
        print(f"[discord] failed to cache channel inventory: {exc}")
    return channels
