"""Find and re-queue DiscordMessage rows affected by recent parser bugs.

Targets two classes of misparses introduced before commits that added
`_normalize_amount_text` and fixed `looks_like_internal_cash_transfer`:

 1. Amount parsing: text with comma-thousands (``$11,050``) or k/M
    suffixes (``6k``, ``1.5k``, ``2M``) was producing far smaller
    amounts (e.g. ``$50`` instead of ``$11,050``).

 2. Loan detection: "Give company X cash (owe me)" was being parsed
    as a buy/reimbursement instead of being ignored as an internal
    cash transfer.

Usage (from the project root):

    # dry run - scan and report, don't touch anything
    python -m scripts.reparse_misparsed_rows --dry-run

    # scan the last 7 days, requeue matches
    python -m scripts.reparse_misparsed_rows --days 7

    # include rows that have been manually reviewed (DANGEROUS - only do
    # this if you are sure your corrections are also wrong)
    python -m scripts.reparse_misparsed_rows --include-reviewed --force

The script is idempotent: rows already in ``pending`` state are left
alone so they don't get bounced back into the queue mid-processing.
"""
from __future__ import annotations

import argparse
import io
import re
import sys
from datetime import timedelta

# Force UTF-8 stdout on Windows so channel names with unicode box chars
# (e.g. U+2551) don't blow up on cp1252.
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from sqlmodel import select

from app.db import managed_session
from app.models import (
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_REVIEW_REQUIRED,
    DiscordMessage,
    utcnow,
)
from app.reparse import reparse_message_rows

# -- heuristics used to find suspected-bad rows ---------------------------

# A comma-thousand number with at least 4 digits of magnitude (>= $1,000).
# We ignore small e.g. "1,250" -> could be a short typo but still valid.
COMMA_THOUSANDS_PATTERN = re.compile(r"\$?\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?")

# A k/M suffix on a number.
K_SUFFIX_PATTERN = re.compile(r"\b\d+(?:\.\d+)?[kKmM]\b")

# "Give/gave/loaned/handed/brought/put ... company/shop/store/business"
LOAN_COMPANY_PATTERN = re.compile(
    r"\b(give|gave|hand(?:ed)?|brought|put|loan(?:ed|ing)?|floated?)\b"
    r".{0,80}?"
    r"\b(company|shop|store|business)\b",
    re.IGNORECASE,
)


def text_has_misparsed_amount(text: str, parsed_amount: float | None) -> bool:
    if not text:
        return False
    if parsed_amount is None:
        return False

    # Look for the largest plausible amount in the raw text via our two
    # patterns. If it is much larger than the parsed amount, the parse
    # was probably truncated.
    largest_in_text: float | None = None

    for match in COMMA_THOUSANDS_PATTERN.finditer(text):
        try:
            value = float(match.group(0).replace("$", "").replace(",", ""))
        except ValueError:
            continue
        if largest_in_text is None or value > largest_in_text:
            largest_in_text = value

    for match in K_SUFFIX_PATTERN.finditer(text):
        token = match.group(0)
        try:
            base = float(token[:-1])
        except ValueError:
            continue
        multiplier = 1000 if token[-1].lower() == "k" else 1_000_000
        value = base * multiplier
        if largest_in_text is None or value > largest_in_text:
            largest_in_text = value

    if largest_in_text is None:
        return False

    # Parsed amount is less than half of the largest number in the text
    # -> almost certainly a truncation bug.
    return parsed_amount < largest_in_text * 0.5


def text_looks_like_company_loan(text: str) -> bool:
    if not text:
        return False
    return bool(LOAN_COMPANY_PATTERN.search(text))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Only scan rows created within the last N days (default 30).",
    )
    parser.add_argument(
        "--include-reviewed",
        action="store_true",
        help="Include rows that have already been manually reviewed. "
             "DANGEROUS. Requires --force.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Required with --include-reviewed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and report only; do not modify anything.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max rows to re-queue per run (default 500). Dry run always scans all.",
    )
    args = parser.parse_args()

    if args.include_reviewed and not args.force:
        print(
            "--include-reviewed requires --force to confirm you want to "
            "overwrite reviewed rows.",
            file=sys.stderr,
        )
        sys.exit(2)

    cutoff = utcnow() - timedelta(days=max(args.days, 1))

    # Skip rows the worker is actively processing.
    skip_statuses = {PARSE_PENDING}

    with managed_session() as session:
        stmt = (
            select(DiscordMessage)
            .where(DiscordMessage.is_deleted == False)  # noqa: E712
            .where(DiscordMessage.created_at >= cutoff)
            .where(DiscordMessage.parse_status.in_([
                PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED,
            ]))
            .order_by(DiscordMessage.created_at.desc())
        )
        rows = session.exec(stmt).all()

        amount_bugs: list[DiscordMessage] = []
        loan_bugs: list[DiscordMessage] = []
        for row in rows:
            if row.parse_status in skip_statuses:
                continue
            if row.reviewed_at is not None and not args.include_reviewed:
                continue

            text = row.content or ""

            # Loan bug: text matches company-loan pattern AND the row
            # currently has a deal_type (meaning the parser thought
            # it was a buy/sell/trade). If it's already PARSE_IGNORED
            # with no deal_type, the new logic probably already got it.
            if text_looks_like_company_loan(text) and row.deal_type is not None:
                loan_bugs.append(row)
                continue

            # Amount bug: parsed amount is way smaller than the largest
            # number in the text.
            if text_has_misparsed_amount(text, row.amount):
                amount_bugs.append(row)

        print(f"Scanned {len(rows):,} rows created since {cutoff.isoformat()}")
        print(f"  suspected amount bugs: {len(amount_bugs)}")
        print(f"  suspected loan bugs:   {len(loan_bugs)}")

        total = amount_bugs + loan_bugs
        if not total:
            print("Nothing to re-queue. Done.")
            return

        # Print a sample of the affected rows for visibility.
        print()
        print("=== sample of suspected misparses ===")
        for row in (amount_bugs[:5] + loan_bugs[:5]):
            txt = (row.content or "").replace("\n", " ")[:80]
            print(
                f"  id={row.id:<6} status={row.parse_status:<16}"
                f" type={row.deal_type or '-':<7} amount={row.amount}"
                f" ch={row.channel_name or '-':<25} text={txt!r}"
            )

        if args.dry_run:
            print("\n--dry-run set; not modifying anything.")
            return

        # Keep runs bounded so a mis-triggered run doesn't drain the queue.
        capped = total[: max(args.limit, 0)]
        if len(capped) < len(total):
            print(f"\nlimiting to first {len(capped)} rows "
                  f"(of {len(total)} matches). Use --limit to raise.")

        updated = reparse_message_rows(
            session,
            capped,
            reason="cleanup: reparse after amount + loan parser fix",
            reset_attempts=True,
        )
        print(f"\nRe-queued {updated} row(s). "
              "The worker will process them on the next parser loop tick.")


if __name__ == "__main__":
    main()
