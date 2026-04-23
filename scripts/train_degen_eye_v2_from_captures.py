"""Add confirmed employee scan captures to the Degen Eye v2 pHash index.

This is the offline version of the website training trigger:
  1. employees scan cards normally
  2. batch review confirms or corrects the label
  3. this script hashes the real shop photo crop and adds it as an exemplar

Only confirmed captures are used by default. Unconfirmed scanner predictions
are useful evaluation data, but they are not trusted as training labels.

Example:
  .\\.venv\\Scripts\\python.exe scripts\\train_degen_eye_v2_from_captures.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT))

from app.degen_eye_v2_training import train_confirmed_captures  # noqa: E402

_RAW_INDEX_PATH = os.getenv("DEGEN_EYE_V2_INDEX_PATH") or str(_ROOT / "data" / "phash_index.sqlite")
DEFAULT_INDEX_PATH = Path(_RAW_INDEX_PATH)
if not DEFAULT_INDEX_PATH.is_absolute():
    DEFAULT_INDEX_PATH = _ROOT / DEFAULT_INDEX_PATH


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", default=str(DEFAULT_INDEX_PATH), help="Path to phash_index.sqlite")
    parser.add_argument("--limit", type=int, default=200, help="Only process the first N eligible captures")
    parser.add_argument("--include-indexed", action="store_true", help="Reprocess captures already marked indexed")
    parser.add_argument("--dry-run", action="store_true", help="Hash and count captures without writing to SQLite")
    args = parser.parse_args()
    summary = train_confirmed_captures(
        index_path=args.index,
        limit=args.limit,
        include_indexed=args.include_indexed,
        dry_run=args.dry_run,
    )
    print(
        "captures considered={captures_considered} indexed={indexed} skipped={skipped} "
        "dry_run={dry_run} elapsed_ms={elapsed_ms}".format(**summary)
    )
    for error in summary.get("errors") or []:
        print(f"skip {error.get('capture_id')}: {error.get('error')}")
    if summary.get("indexed") and not summary.get("dry_run"):
        print("Reload the running web process with POST /degen_eye/v2/reload-index or restart it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
