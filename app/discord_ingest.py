import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import discord
from sqlmodel import Session, select

from .bookkeeping import auto_import_public_google_sheet, extract_google_sheet_url
from .config import get_settings
from .db import engine
from .models import DiscordMessage, WatchedChannel

settings = get_settings()

discord_client_instance = None
discord_runtime_state = {
    "status": "stopped",
    "error": None,
}
DISCORD_RETRY_MIN_SECONDS = 15
DISCORD_RETRY_MAX_SECONDS = 900
ALLOWED_CHANNEL_CATEGORIES = {
    "Employees",
    "Show Deals",
    "Past Shows",
    "Offline Deals",
}
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


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
    with Session(engine) as session:
        rows = session.exec(
            select(WatchedChannel).where(WatchedChannel.is_enabled == True)
        ).all()
        return {int(r.channel_id) for r in rows}


def get_backfill_channel_ids() -> set[int]:
    with Session(engine) as session:
        rows = session.exec(
            select(WatchedChannel).where(
                WatchedChannel.is_enabled == True,
                WatchedChannel.backfill_enabled == True,
            )
        ).all()
        return {int(r.channel_id) for r in rows}


def get_backfill_channels() -> list[WatchedChannel]:
    with Session(engine) as session:
        return session.exec(
            select(WatchedChannel)
            .where(
                WatchedChannel.is_enabled == True,
                WatchedChannel.backfill_enabled == True,
            )
            .order_by(WatchedChannel.channel_name, WatchedChannel.channel_id)
        ).all()


def seed_channels_from_env() -> None:
    with Session(engine) as session:
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


def get_attachment_urls(message: discord.Message) -> list[str]:
    return [a.url for a in message.attachments]


def get_message_row(session: Session, discord_message_id: str) -> Optional[DiscordMessage]:
    return session.exec(
        select(DiscordMessage).where(
            DiscordMessage.discord_message_id == discord_message_id
        )
    ).first()


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

    attachment_urls = get_attachment_urls(message)

    with Session(engine) as session:
        existing = get_message_row(session, str(message.id))

        if existing:
            existing.guild_id = str(message.guild.id) if message.guild else None
            existing.channel_id = str(message.channel.id)
            existing.channel_name = getattr(message.channel, "name", None)
            existing.author_id = str(message.author.id)
            existing.author_name = str(message.author)
            existing.content = message.content or ""
            existing.attachment_urls_json = json.dumps(attachment_urls)
            existing.is_deleted = False

            if is_edit:
                existing.edited_at = utcnow()
                existing.parse_status = "queued"
                existing.last_error = None

            session.add(existing)
            session.commit()
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
            parse_status="queued",
            is_deleted=False,
        )
        session.add(row)
        session.commit()
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
            print(f"[bookkeeping] auto-imported Google Sheet from message {message.id} -> import {imported_id}")
    except Exception as exc:
        print(f"[bookkeeping] auto-import failed for message {message.id}: {exc}")


def mark_message_deleted(message: discord.Message) -> bool:
    with Session(engine) as session:
        existing = get_message_row(session, str(message.id))
        if not existing:
            return False

        existing.is_deleted = True
        existing.edited_at = utcnow()
        existing.parse_status = "deleted"
        session.add(existing)
        session.commit()
        return True


class DealIngestBot(discord.Client):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ready_event = asyncio.Event()
        self.startup_backfill_done = False

    async def on_ready(self):
        print(f"[discord] logged in as {self.user}")
        discord_runtime_state["status"] = "ready"
        discord_runtime_state["error"] = None

        if not self.startup_backfill_done and settings.startup_backfill_enabled:
            self.startup_backfill_done = True
            try:
                await self.backfill_enabled_channels(
                    limit_per_channel=settings.startup_backfill_limit_per_channel,
                    oldest_first=settings.startup_backfill_oldest_first,
                )
            except Exception as exc:
                discord_runtime_state["status"] = "degraded"
                discord_runtime_state["error"] = f"startup backfill failed: {exc}"
                print(f"[discord] startup backfill failed: {exc}")

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

        try:
            async for message in channel.history(
                limit=limit,
                oldest_first=oldest_first,
                after=after,
                before=before,
            ):
                if not should_track_message(message, watched_channel_ids):
                    skipped_count += 1
                    continue

                ok, action = insert_or_update_message(
                    message,
                    is_edit=False,
                    watched_channel_ids=watched_channel_ids,
                )
                if not ok:
                    skipped_count += 1
                elif action == "inserted":
                    inserted_count += 1
                else:
                    updated_count += 1
        except Exception as e:
            return {
                "ok": False,
                "channel_id": channel_id,
                "channel_name": getattr(channel, "name", None),
                "error": f"Backfill failed while reading channel history: {e}",
            }

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
                    print(f"[discord] rate limited ({exc}); retrying in {retry_delay}s")
                else:
                    discord_runtime_state["status"] = "degraded"
                    discord_runtime_state["error"] = f"Discord HTTP error ({status_code}): {exc}; retrying in {retry_delay}s"
                    print(f"[discord] HTTP error ({status_code}): {exc}; retrying in {retry_delay}s")
            except Exception as exc:
                discord_runtime_state["status"] = "degraded"
                discord_runtime_state["error"] = f"Discord connection failed: {exc}; retrying in {retry_delay}s"
                print(f"[discord] bot stopped with error: {exc}; retrying in {retry_delay}s")
            finally:
                discord_client_instance = None

            if stop_event.is_set():
                break

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=retry_delay)
            except asyncio.TimeoutError:
                pass

            retry_delay = min(retry_delay * 2, DISCORD_RETRY_MAX_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        discord_runtime_state["status"] = "error"
        discord_runtime_state["error"] = str(exc)
        print(f"[discord] bot task crashed: {exc}")
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


def list_available_discord_channels() -> list[dict]:
    client = get_discord_client()
    if client is None or client.is_closed() or not client.is_ready():
        return []

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
    return channels
