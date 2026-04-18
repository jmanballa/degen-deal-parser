"""Inject PWA meta tags into every Jinja template that has a <head>.

This is idempotent:
  - If the PWA marker comment ('<!-- PWA -->') is already in the <head>, skip.
  - Otherwise insert the block right after <meta charset="...">
    (or at the top of <head> if no charset tag is present).
  - Also upgrades the viewport meta tag to include 'viewport-fit=cover'.

Templates without a <head> (partials, fragments) are left alone.

Re-run:
    python scripts/inject-pwa-meta.py
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

PWA_BLOCK = """    <!-- PWA -->
    <link rel="manifest" href="/static/manifest.webmanifest">
    <meta name="theme-color" content="#C8102E">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-title" content="Degen">
    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
    <meta name="mobile-web-app-capable" content="yes">
    <link rel="apple-touch-icon" href="/static/icons/apple-touch-icon-180.png">
    <link rel="icon" type="image/png" sizes="192x192" href="/static/icons/icon-192.png">
    <link rel="icon" type="image/png" sizes="512x512" href="/static/icons/icon-512.png">
"""

PWA_MARKER = "<!-- PWA -->"

CHARSET_RE = re.compile(r'(<meta\s+charset=["\'][^"\']*["\']\s*/?>)', re.IGNORECASE)
HEAD_OPEN_RE = re.compile(r'(<head\b[^>]*>)', re.IGNORECASE)
VIEWPORT_RE = re.compile(
    r'<meta\s+name=["\']viewport["\']\s+content=["\']([^"\']*)["\']\s*/?>',
    re.IGNORECASE,
)


def _upgrade_viewport(text: str) -> tuple[str, bool]:
    match = VIEWPORT_RE.search(text)
    if not match:
        return text, False
    content = match.group(1)
    if "viewport-fit=cover" in content:
        return text, False
    new_content = content.rstrip().rstrip(",").rstrip()
    new_content = f"{new_content}, viewport-fit=cover"
    replacement = f'<meta name="viewport" content="{new_content}">'
    return text.replace(match.group(0), replacement, 1), True


def _inject(text: str) -> tuple[str, bool, bool]:
    """Returns (new_text, injected_pwa_block, upgraded_viewport)."""
    injected = False
    if PWA_MARKER not in text and HEAD_OPEN_RE.search(text):
        charset_match = CHARSET_RE.search(text)
        if charset_match:
            insert_at = charset_match.end()
            # preserve the trailing newline/whitespace after the charset tag
            if insert_at < len(text) and text[insert_at] == "\n":
                insert_at += 1
            new_text = text[:insert_at] + PWA_BLOCK + text[insert_at:]
        else:
            head_match = HEAD_OPEN_RE.search(text)
            assert head_match is not None
            insert_at = head_match.end()
            if insert_at < len(text) and text[insert_at] == "\n":
                insert_at += 1
            new_text = text[:insert_at] + PWA_BLOCK + text[insert_at:]
        text = new_text
        injected = True

    text, viewport_upgraded = _upgrade_viewport(text)
    return text, injected, viewport_upgraded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--templates-dir",
        default=str(Path(__file__).resolve().parent.parent / "app" / "templates"),
    )
    args = parser.parse_args()

    templates_dir = Path(args.templates_dir)
    stats = {"scanned": 0, "injected": 0, "viewport_upgraded": 0, "skipped_no_head": 0, "already": 0}

    for path in sorted(templates_dir.rglob("*.html")):
        stats["scanned"] += 1
        original = path.read_text(encoding="utf-8")
        if not HEAD_OPEN_RE.search(original):
            stats["skipped_no_head"] += 1
            continue
        new_text, injected, viewport_upgraded = _inject(original)
        if injected:
            stats["injected"] += 1
        elif PWA_MARKER in original:
            stats["already"] += 1
        if viewport_upgraded:
            stats["viewport_upgraded"] += 1
        if new_text != original:
            path.write_text(new_text, encoding="utf-8")
            print(f"patched {path.relative_to(templates_dir)}")

    print()
    for key, value in stats.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
