"""Inject the mobile bottom nav partial into every top-level workspace template.

A template is a candidate if:
  - It includes '_workspace_nav.html' (the sticky top workspace header)
  - AND it has a '</body>' closing tag
  - AND it is not 'tiktok_streamer.html' (exempt: has its own fixed-bottom
    chat drawer toggle that would collide with the bottom nav)

Idempotent: if the mobile-bottom-nav include is already present, skip.

Re-run:
    python scripts/inject-mobile-bottom-nav.py
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

INCLUDE_LINE = "    {% include '_mobile_bottom_nav.html' %}\n"
INCLUDE_MARKER = "_mobile_bottom_nav.html"
WORKSPACE_NAV_MARKER = "_workspace_nav.html"
BODY_CLOSE_RE = re.compile(r"([ \t]*)</body>", re.IGNORECASE)

EXEMPT = {"tiktok_streamer.html"}


def _inject(text: str) -> tuple[str, bool]:
    if INCLUDE_MARKER in text:
        return text, False
    match = BODY_CLOSE_RE.search(text)
    if not match:
        return text, False
    insert_at = match.start()
    new_text = text[:insert_at] + INCLUDE_LINE + text[insert_at:]
    return new_text, True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--templates-dir",
        default=str(Path(__file__).resolve().parent.parent / "app" / "templates"),
    )
    args = parser.parse_args()

    templates_dir = Path(args.templates_dir)
    stats = {
        "scanned": 0,
        "injected": 0,
        "already": 0,
        "skipped_no_workspace_nav": 0,
        "skipped_no_body_close": 0,
        "skipped_exempt": 0,
    }

    for path in sorted(templates_dir.rglob("*.html")):
        stats["scanned"] += 1
        if path.name.startswith("_"):
            # Partials never get the include themselves.
            continue
        if path.name in EXEMPT:
            stats["skipped_exempt"] += 1
            print(f"exempt {path.relative_to(templates_dir)}")
            continue
        original = path.read_text(encoding="utf-8")
        if WORKSPACE_NAV_MARKER not in original:
            stats["skipped_no_workspace_nav"] += 1
            continue
        if not BODY_CLOSE_RE.search(original):
            stats["skipped_no_body_close"] += 1
            continue
        if INCLUDE_MARKER in original:
            stats["already"] += 1
            continue
        new_text, injected = _inject(original)
        if injected:
            path.write_text(new_text, encoding="utf-8")
            stats["injected"] += 1
            print(f"patched {path.relative_to(templates_dir)}")

    print()
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
