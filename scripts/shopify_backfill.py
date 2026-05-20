from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

load_dotenv(ROOT_DIR / ".env")

from app.db import init_db, managed_session
from app.inventory.shopify_ingest import backfill_shopify_orders


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Shopify orders into shopify_orders.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of orders to fetch.")
    parser.add_argument("--since", type=str, default=None, help="Only fetch orders created on or after this ISO datetime/date.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and normalize orders without storing them.")
    return parser.parse_args()


def require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def main() -> int:
    args = parse_args()
    store_domain = require_env("SHOPIFY_STORE_DOMAIN")
    api_key = require_env("SHOPIFY_API_KEY")
    init_db()

    with managed_session() as session:
        summary = backfill_shopify_orders(
            session,
            store_domain=store_domain,
            api_key=api_key,
            since=args.since,
            limit=args.limit,
            dry_run=args.dry_run,
        )

    print(
        "Shopify backfill summary: "
        f"total fetched={summary.fetched}, "
        f"inserted={summary.inserted}, "
        f"updated={summary.updated}, "
        f"failed={summary.failed}, "
        f"dry_run={args.dry_run}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
