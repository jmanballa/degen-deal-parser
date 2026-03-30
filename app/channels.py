from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Session, select

from .discord_ingest import list_available_discord_channels
from .models import DiscordMessage, WatchedChannel, utcnow


def get_watched_channels(session: Session) -> list[WatchedChannel]:
    return session.exec(
        select(WatchedChannel).order_by(WatchedChannel.channel_name, WatchedChannel.channel_id)
    ).all()


def resolve_channel_label(channel_id: str, preferred_name: Optional[str] = None) -> str:
    if preferred_name and preferred_name.strip():
        return preferred_name.strip()

    available = list_available_discord_channels()
    matched = next((channel for channel in available if channel["channel_id"] == channel_id), None)
    if matched:
        return matched["label"]

    return channel_id


def normalize_channel_ids(channel_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for channel_id in channel_ids:
        cleaned = (channel_id or "").strip()
        if not cleaned or not cleaned.isdigit() or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)

    return normalized


def upsert_watched_channel(
    session: Session,
    *,
    channel_id: str,
    channel_name: Optional[str] = None,
    is_enabled: bool = True,
    backfill_enabled: bool = False,
    backfill_after: Optional[datetime] = None,
    backfill_before: Optional[datetime] = None,
) -> WatchedChannel:
    channel_id = channel_id.strip()
    existing = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    now = utcnow()

    if existing:
        existing.channel_name = resolve_channel_label(
            channel_id,
            channel_name or existing.channel_name,
        )
        existing.is_enabled = is_enabled
        existing.backfill_enabled = backfill_enabled
        if backfill_after is not None:
            existing.backfill_after = backfill_after
        if backfill_before is not None:
            existing.backfill_before = backfill_before
        existing.updated_at = now
        session.add(existing)
        session.commit()
        session.refresh(existing)
        return existing

    resolved_name = resolve_channel_label(channel_id, channel_name)
    row = WatchedChannel(
        channel_id=channel_id,
        channel_name=resolved_name,
        is_enabled=is_enabled,
        backfill_enabled=backfill_enabled,
        backfill_after=backfill_after,
        backfill_before=backfill_before,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def update_backfill_window(
    session: Session,
    *,
    channel_id: str,
    backfill_after: Optional[datetime],
    backfill_before: Optional[datetime],
) -> Optional[WatchedChannel]:
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()
    if not row:
        return None

    row.backfill_after = backfill_after
    row.backfill_before = backfill_before
    row.updated_at = utcnow()
    session.add(row)
    return row


def get_channel_filter_choices(session: Session) -> list[dict]:
    watched = get_watched_channels(session)
    choices: dict[str, dict] = {
        channel.channel_id: {
            "channel_id": channel.channel_id,
            "channel_name": channel.channel_name or channel.channel_id,
        }
        for channel in watched
    }

    stored_channels = session.exec(
        select(DiscordMessage.channel_id, DiscordMessage.channel_name).distinct()
    ).all()
    for channel_id, channel_name in stored_channels:
        if not channel_id:
            continue
        choices.setdefault(
            channel_id,
            {
                "channel_id": channel_id,
                "channel_name": channel_name or channel_id,
            },
        )

    return sorted(choices.values(), key=lambda row: row["channel_name"].lower())
