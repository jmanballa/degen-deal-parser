import argparse
import asyncio
import sys
from pathlib import Path
from datetime import timedelta
from typing import Awaitable, Callable, Optional

import discord
from sqlmodel import select

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.attachment_repair import (
    AttachmentRepairCandidate,
    attachment_repair_candidate_query,
    restore_missing_assets_from_urls,
    row_status_snapshot,
)
from app.attachment_storage import attachment_cache_path, write_attachment_cache_file
from app.config import get_settings
from app.db import managed_session
from app.discord.discord_ingest import DealIngestBot, recover_attachment_assets_for_message
import app.discord.discord_ingest as discord_ingest
from app.models import AttachmentAsset


BATCH_SIZE = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Warm the disk-backed attachment cache for older Discord messages."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of uncached message rows to process.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be processed without writing cache files or fetching Discord.",
    )
    parser.add_argument(
        "--since-hours",
        type=float,
        default=None,
        help="Only process messages seen or edited within the last N hours.",
    )
    return parser.parse_args()


def log(message: str) -> None:
    print(message, flush=True)


def find_uncached_candidates(
    before_id: Optional[int],
    batch_size: int,
    *,
    since_hours: Optional[float],
) -> tuple[list[AttachmentRepairCandidate], Optional[int]]:
    with managed_session() as session:
        since = None
        if since_hours is not None and since_hours > 0:
            since = discord.utils.utcnow() - timedelta(hours=since_hours)
        candidates = attachment_repair_candidate_query(
            session,
            since=since,
            before_message_id=before_id,
            limit=batch_size,
        )
        if not candidates:
            return [], None

    next_cursor = min((candidate.message_id for candidate in candidates), default=None)
    return candidates, next_cursor


def restore_missing_cache_files(message_id: int, asset_ids: list[int], dry_run: bool) -> tuple[int, int]:
    restored = 0
    skipped = 0
    for asset_id in asset_ids:
        with managed_session() as session:
            asset = session.get(AttachmentAsset, asset_id)
            if not asset or asset.id is None:
                skipped += 1
                continue
            cache_path = attachment_cache_path(
                asset.id,
                filename=asset.filename,
                content_type=asset.content_type,
            )
            if cache_path.exists():
                skipped += 1
                continue
            if dry_run:
                restored += 1
                continue
            write_attachment_cache_file(
                asset.id,
                filename=asset.filename,
                content_type=asset.content_type,
                data=asset.data,
            )
            restored += 1
    return restored, skipped


async def start_recovery_client_if_needed(dry_run: bool) -> tuple[Optional[DealIngestBot], Optional[asyncio.Task]]:
    if dry_run:
        return None, None

    settings = get_settings()
    if not settings.discord_bot_token.strip():
        log("[setup] DISCORD_BOT_TOKEN is empty. Discord recovery is unavailable.")
        return None, None

    intents = discord.Intents.default()
    intents.message_content = True
    intents.guilds = True
    intents.messages = True

    client = DealIngestBot(intents=intents)
    client.startup_backfill_done = True
    discord_ingest.discord_client_instance = client
    discord_ingest.discord_runtime_state["status"] = "starting"
    discord_ingest.discord_runtime_state["error"] = None

    task = asyncio.create_task(client.start(settings.discord_bot_token))
    try:
        await asyncio.wait_for(client.ready_event.wait(), timeout=60)
        log("[setup] Discord recovery client is ready.")
        return client, task
    except Exception as exc:
        log(f"[setup] Failed to start Discord recovery client: {exc}")
        try:
            await client.close()
        finally:
            await asyncio.gather(task, return_exceptions=True)
            discord_ingest.discord_client_instance = None
        return None, None


async def shutdown_recovery_client(
    client: Optional[DealIngestBot],
    task: Optional[asyncio.Task],
) -> None:
    if client is None or task is None:
        return
    try:
        await client.close()
    finally:
        await asyncio.gather(task, return_exceptions=True)
        discord_ingest.discord_client_instance = None


async def process_candidate(
    candidate: AttachmentRepairCandidate,
    *,
    dry_run: bool,
    ensure_discord_client_ready: Callable[[], Awaitable[bool]],
) -> str:
    current_asset_count, current_cached_count = row_status_snapshot(candidate.message_id)
    target_count = len(candidate.attachment_urls)

    if current_asset_count >= target_count and current_cached_count >= current_asset_count:
        log(
            f"[skip] message_id={candidate.message_id} channel={candidate.channel_id} "
            "already fully cached"
        )
        return "skipped"

    try:
        restored_count, _skipped_assets = restore_missing_cache_files(
            candidate.message_id,
            candidate.missing_cache_asset_ids,
            dry_run=dry_run,
        )
        if dry_run:
            url_restored_count = len(candidate.missing_attachment_urls)
            url_failed_count = 0
        else:
            url_restored_count, url_failed_count = restore_missing_assets_from_urls(
                candidate.message_id,
                candidate.missing_attachment_urls,
            )

        if url_failed_count and candidate.is_incomplete:
            if dry_run:
                log(
                    f"[dry-run] message_id={candidate.message_id} channel={candidate.channel_id} "
                    f"would use Discord fallback for {url_failed_count} attachment URL failure(s)"
                )
            elif await ensure_discord_client_ready():
                recovered = await recover_attachment_assets_for_message(
                    channel_id=candidate.channel_id,
                    discord_message_id=candidate.discord_message_id,
                    message_row_id=candidate.message_id,
                )
                if not recovered:
                    log(
                        f"[failed] message_id={candidate.message_id} channel={candidate.channel_id} "
                        "Discord recovery returned no assets"
                    )
            else:
                log(
                    f"[warn] message_id={candidate.message_id} channel={candidate.channel_id} "
                    "Discord recovery client is not available for fallback"
                )

        if dry_run:
            log(
                f"[dry-run] message_id={candidate.message_id} channel={candidate.channel_id} "
                f"assets={current_asset_count}/{target_count} "
                f"missing_cache_files={len(candidate.missing_cache_asset_ids)} "
                f"url_repaired={url_restored_count}"
            )
            return "succeeded"

        final_asset_count, final_cached_count = row_status_snapshot(candidate.message_id)
        if final_asset_count >= target_count and final_cached_count >= final_asset_count:
            log(
                f"[success] message_id={candidate.message_id} channel={candidate.channel_id} "
                f"cached={final_cached_count}/{target_count} restored_files={restored_count} "
                f"url_assets={url_restored_count}"
            )
            return "succeeded"

        log(
            f"[failed] message_id={candidate.message_id} channel={candidate.channel_id} "
            f"still incomplete after warmup assets={final_asset_count}/{target_count} "
            f"cached_files={final_cached_count}/{final_asset_count}"
        )
        return "failed"
    except Exception as exc:
        log(
            f"[failed] message_id={candidate.message_id} channel={candidate.channel_id} "
            f"unexpected_error={exc}"
        )
        return "failed"


async def async_main() -> int:
    args = parse_args()

    processed = 0
    succeeded = 0
    failed = 0
    skipped = 0
    cursor_id: Optional[int] = None

    client: Optional[DealIngestBot] = None
    client_task: Optional[asyncio.Task] = None
    client_start_attempted = False

    async def ensure_discord_client_ready() -> bool:
        nonlocal client, client_task, client_start_attempted
        if client is not None:
            return True
        if client_start_attempted:
            return False
        client_start_attempted = True
        client, client_task = await start_recovery_client_if_needed(dry_run=False)
        return client is not None

    try:
        while True:
            try:
                candidates, next_cursor = find_uncached_candidates(
                    cursor_id,
                    BATCH_SIZE,
                    since_hours=args.since_hours,
                )
            except OperationalError as exc:
                log(f"[summary] database_connection_failed={exc}")
                return 2
            if next_cursor is None:
                break
            cursor_id = next_cursor
            if not candidates:
                continue

            for candidate in candidates:
                if args.limit is not None and processed >= args.limit:
                    break

                status = await process_candidate(
                    candidate,
                    dry_run=args.dry_run,
                    ensure_discord_client_ready=ensure_discord_client_ready,
                )
                processed += 1
                if status == "succeeded":
                    succeeded += 1
                elif status == "failed":
                    failed += 1
                else:
                    skipped += 1

            if args.limit is not None and processed >= args.limit:
                break
    finally:
        await shutdown_recovery_client(client, client_task)

    log(
        "[summary] "
        f"processed={processed} succeeded={succeeded} failed={failed} skipped={skipped}"
    )
    return 0 if failed == 0 else 1


def main() -> int:
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        log("[summary] interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
