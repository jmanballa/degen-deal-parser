import asyncio
import signal
import socket
import threading

from ..inventory.ai_resolver import ai_review_resolver_loop
from .backfill_requests import backfill_request_loop, requeue_interrupted_backfill_requests
from ..config import get_settings
from ..db import init_db, managed_session
from .discord_ingest import (
    discord_runtime_state,
    get_discord_client,
    periodic_attachment_repair_loop,
    recent_message_audit_loop,
    run_discord_bot,
    seed_channels_from_env,
)
from .ops_log import write_operations_log
from ..runtime_logging import setup_runtime_file_logging
from ..runtime_monitor import runtime_heartbeat_loop
from ..tiktok.tiktok_auth_refresh import refresh_tiktok_auth_if_needed
from .worker import (
    parser_loop,
    periodic_stitch_audit_loop,
)

settings = get_settings()
setup_runtime_file_logging("worker.log")


def _resolve_tiktok_base_url() -> str:
    explicit = (settings.tiktok_shop_api_base_url or "").strip()
    if explicit:
        return explicit
    generic = (settings.tiktok_api_base_url or "").strip()
    if generic and "open-api" in generic:
        return generic
    return "https://open-api.tiktokglobalshop.com"


async def periodic_tiktok_token_refresh_loop(stop_event: asyncio.Event) -> None:
    interval = max(settings.tiktok_token_refresh_interval_minutes, 1.0)
    while not stop_event.is_set():
        await asyncio.sleep(interval * 60)
        if stop_event.is_set():
            break
        try:
            with managed_session() as session:
                await asyncio.to_thread(
                    refresh_tiktok_auth_if_needed,
                    session,
                    runtime_name=settings.runtime_name,
                    resolve_base_url=_resolve_tiktok_base_url,
                    update_state=None,
                )
        except Exception as exc:
            print(f"[worker] tiktok-token-refresh error: {exc}")


def worker_runtime_details() -> dict:
    return {
        "discord_status": discord_runtime_state.get("status"),
        "discord_error": discord_runtime_state.get("error"),
        "parser_worker_enabled": settings.parser_worker_enabled,
        "ai_resolver_enabled": settings.ai_resolver_enabled,
        "ai_resolver_interval_minutes": settings.ai_resolver_interval_minutes,
        "discord_ingest_enabled": settings.discord_ingest_enabled,
        "periodic_stitch_audit_enabled": settings.periodic_stitch_audit_enabled,
        "periodic_stitch_audit_interval_minutes": settings.periodic_stitch_audit_interval_minutes,
        "periodic_attachment_repair_enabled": settings.periodic_attachment_repair_enabled,
        "periodic_attachment_repair_interval_minutes": settings.periodic_attachment_repair_interval_minutes,
        "periodic_attachment_repair_lookback_hours": settings.periodic_attachment_repair_lookback_hours,
        "periodic_attachment_repair_limit": settings.periodic_attachment_repair_limit,
        "periodic_attachment_repair_min_age_minutes": settings.periodic_attachment_repair_min_age_minutes,
        "service_mode": "worker-host",
        "last_recent_audit_at": discord_runtime_state.get("last_recent_audit_at"),
        "last_recent_audit_summary": discord_runtime_state.get("last_recent_audit_summary"),
        "last_attachment_repair_at": discord_runtime_state.get("last_attachment_repair_at"),
        "last_attachment_repair_summary": discord_runtime_state.get("last_attachment_repair_summary"),
    }


def _recreate_task(task_name: str, stop_event: asyncio.Event) -> asyncio.Task | None:
    """Re-create a crashed background task by name."""
    factories = {
        "discord-ingest": lambda: asyncio.create_task(run_discord_bot(stop_event), name="discord-ingest"),
        "backfill-queue": lambda: asyncio.create_task(
            backfill_request_loop(stop_event, get_discord_client), name="backfill-queue"
        ),
        "recent-message-audit": lambda: asyncio.create_task(
            recent_message_audit_loop(stop_event, get_discord_client), name="recent-message-audit"
        ),
        "stitch-audit": lambda: asyncio.create_task(
            periodic_stitch_audit_loop(stop_event), name="stitch-audit"
        ),
        "attachment-repair-audit": lambda: asyncio.create_task(
            periodic_attachment_repair_loop(stop_event, get_discord_client), name="attachment-repair-audit"
        ),
        "parser-worker": lambda: asyncio.create_task(parser_loop(stop_event), name="parser-worker"),
        "tiktok-token-refresh": lambda: asyncio.create_task(
            periodic_tiktok_token_refresh_loop(stop_event), name="tiktok-token-refresh"
        ),
        "ai-review-resolver": lambda: asyncio.create_task(
            ai_review_resolver_loop(stop_event), name="ai-review-resolver"
        ),
    }
    factory = factories.get(task_name)
    return factory() if factory else None


async def run_worker_service() -> None:
    if not settings.discord_ingest_enabled and not settings.parser_worker_enabled:
        raise SystemExit("Worker service has nothing to run. Enable DISCORD_INGEST_ENABLED and/or PARSER_WORKER_ENABLED.")

    init_db()
    seed_channels_from_env()

    with managed_session() as session:
        requeue_interrupted_backfill_requests(session)
        if not settings.discord_ingest_enabled:
            write_operations_log(
                session,
                event_type="backfill_executor_disabled",
                level="warning",
                source="worker",
                message=(
                    "Backfill queue execution is disabled on this runtime because "
                    "DISCORD_INGEST_ENABLED is false."
                ),
                details={
                    "runtime_name": settings.runtime_name,
                    "discord_ingest_enabled": settings.discord_ingest_enabled,
                    "parser_worker_enabled": settings.parser_worker_enabled,
                },
            )

    stop_event = asyncio.Event()
    heartbeat_stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=runtime_heartbeat_loop,
        kwargs={
            "stop_event": heartbeat_stop_event,
            "runtime_name": settings.runtime_name,
            "host_name": socket.gethostname(),
            "details_provider": worker_runtime_details,
        },
        name="worker-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()

    background_tasks: list[asyncio.Task] = []
    if settings.discord_ingest_enabled:
        background_tasks.append(asyncio.create_task(run_discord_bot(stop_event), name="discord-ingest"))
        background_tasks.append(
            asyncio.create_task(
                backfill_request_loop(stop_event, get_discord_client),
                name="backfill-queue",
            )
        )
        if settings.periodic_offline_audit_enabled:
            background_tasks.append(
                asyncio.create_task(
                    recent_message_audit_loop(stop_event, get_discord_client),
                    name="recent-message-audit",
                )
            )
    if settings.discord_ingest_enabled and settings.parser_worker_enabled:
        background_tasks.append(
            asyncio.create_task(
                periodic_stitch_audit_loop(stop_event),
                name="stitch-audit",
            )
        )
    if settings.discord_ingest_enabled and settings.periodic_attachment_repair_enabled:
        background_tasks.append(
            asyncio.create_task(
                periodic_attachment_repair_loop(stop_event, get_discord_client),
                name="attachment-repair-audit",
            )
        )
    if settings.parser_worker_enabled:
        background_tasks.append(asyncio.create_task(parser_loop(stop_event), name="parser-worker"))
    if settings.parser_worker_enabled and settings.ai_resolver_enabled:
        background_tasks.append(
            asyncio.create_task(
                ai_review_resolver_loop(stop_event),
                name="ai-review-resolver",
            )
        )
    if settings.tiktok_token_refresh_enabled:
        background_tasks.append(
            asyncio.create_task(
                periodic_tiktok_token_refresh_loop(stop_event),
                name="tiktok-token-refresh",
            )
        )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    stop_waiter = asyncio.create_task(stop_event.wait(), name="worker-stop-waiter")
    try:
        while not stop_event.is_set():
            waitables = [stop_waiter] + [t for t in background_tasks if not t.done()]
            if not waitables or (len(waitables) == 1 and waitables[0] is stop_waiter):
                break
            done, _pending = await asyncio.wait(waitables, return_when=asyncio.FIRST_COMPLETED)
            if stop_waiter in done:
                break
            for task in done:
                if task is stop_waiter:
                    continue
                task_name = task.get_name()
                exc = task.exception() if not task.cancelled() else None
                if exc:
                    print(f"[worker] task {task_name} crashed: {exc}; restarting in 5s")
                else:
                    print(f"[worker] task {task_name} exited unexpectedly; restarting in 5s")
                background_tasks.remove(task)
                await asyncio.sleep(5)
                if stop_event.is_set():
                    break
                new_task = _recreate_task(task_name, stop_event)
                if new_task:
                    background_tasks.append(new_task)
                    print(f"[worker] task {task_name} restarted")
                else:
                    print(f"[worker] task {task_name} has no restart factory; not restarted")
    finally:
        stop_event.set()
        heartbeat_stop_event.set()
        for task in background_tasks:
            if not task.done():
                task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        stop_waiter.cancel()
        await asyncio.gather(stop_waiter, return_exceptions=True)
        heartbeat_thread.join(timeout=5)


def main() -> None:
    asyncio.run(run_worker_service())


if __name__ == "__main__":
    main()
