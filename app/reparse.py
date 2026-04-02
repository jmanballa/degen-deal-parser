from __future__ import annotations

import argparse
import asyncio

from sqlmodel import Session

from .db import managed_session
from .models import DiscordMessage
from .reparse_runs import safe_create_reparse_run, safe_finalize_reparse_run_queue
from .reporting import parse_report_datetime
from .transactions import sync_transaction_from_message
from .worker import process_once, queue_reparse_range, reset_for_reprocess


def reparse_message_rows(
    session: Session,
    rows: list[DiscordMessage],
    *,
    reason: str,
    reset_attempts: bool = True,
) -> int:
    updated = 0
    for row in rows:
        reset_for_reprocess(row, reason=reason, reset_attempts=reset_attempts)
        row.active_reparse_run_id = None
        session.add(row)
        sync_transaction_from_message(session, row)
        updated += 1
    if updated:
        session.commit()
    return updated


def reparse_message_row(
    session: Session,
    message_id: int,
    *,
    reason: str,
    reset_attempts: bool = True,
) -> bool:
    row = session.get(DiscordMessage, message_id)
    if not row:
        return False
    reparse_message_rows(session, [row], reason=reason, reset_attempts=reset_attempts)
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Queue a safe parser reparse for an explicit DiscordMessage date range.",
    )
    parser.add_argument("--after", help="Inclusive UTC date/datetime, for example 2026-03-01 or 2026-03-01T00:00:00")
    parser.add_argument("--before", help="Inclusive UTC date/datetime, for example 2026-03-31 or 2026-03-31T23:59:59")
    parser.add_argument("--channel-id", help="Optional Discord channel id filter")
    parser.add_argument("--include-failed", action="store_true", help="Also queue failed rows in the range")
    parser.add_argument("--include-ignored", action="store_true", help="Also queue ignored rows in the range")
    parser.add_argument("--include-reviewed", action="store_true", help="Also queue rows that were manually reviewed")
    parser.add_argument(
        "--force-reviewed",
        action="store_true",
        help="Required with --include-reviewed to confirm overwriting reviewed parser output.",
    )
    parser.add_argument(
        "--process-now",
        action="store_true",
        help="After queueing the range, run one worker pass immediately in this process.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    start = parse_report_datetime(args.after)
    end = parse_report_datetime(args.before, end_of_day=True)
    if start is None and end is None:
        parser.error("provide --after and/or --before to define a reparse range")
    if args.include_reviewed and not args.force_reviewed:
        parser.error("--include-reviewed requires --force-reviewed to avoid overwriting manual review corrections")

    include_statuses = ["parsed", "needs_review"]
    if args.include_failed:
        include_statuses.append("failed")
    if args.include_ignored:
        include_statuses.append("ignored")

    run_id = safe_create_reparse_run(
        source="cli",
        reason="cli range reparse",
        range_after=start,
        range_before=end,
        channel_id=args.channel_id or None,
        include_reviewed=args.include_reviewed,
        force_reviewed=args.force_reviewed,
        requested_statuses=include_statuses,
    )

    with managed_session() as session:
        result = queue_reparse_range(
            session,
            start=start,
            end=end,
            channel_id=args.channel_id or None,
            include_statuses=include_statuses,
            include_reviewed=args.include_reviewed,
            reason="cli range reparse",
            reparse_run_id=run_id,
        )
    safe_finalize_reparse_run_queue(
        run_id=run_id,
        selected_count=result["matched"],
        queued_count=result["queued"],
        already_queued_count=result["already_queued"],
        skipped_reviewed_count=result["skipped_reviewed"],
        first_message_id=result["first_message_id"],
        last_message_id=result["last_message_id"],
        first_message_created_at=result["first_message_created_at"],
        last_message_created_at=result["last_message_created_at"],
    )

    print(
        "Queued {queued} of {matched} matched rows for range reparse (run_id={run_id}).".format(
            queued=result["queued"],
            matched=result["matched"],
            run_id=run_id or "unavailable",
        )
    )

    if args.process_now:
        asyncio.run(process_once())
        print("Ran one parser worker pass for queued rows.")


if __name__ == "__main__":
    main()
