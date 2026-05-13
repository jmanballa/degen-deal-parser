#!/usr/bin/env python3
"""Reorganize app/ into vertical subdirectories.

Run from the repo root:
    python scripts/migrate_app_structure.py
"""
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).parent.parent
APP  = REPO / "app"
TESTS = REPO / "tests"

# Modules that stay at app/ root (never moved)
ROOT_MODULES = {
    "main", "config", "db", "models", "shared", "auth", "cache", "csrf",
    "permissions", "rate_limit", "schemas", "runtime_logging", "runtime_monitor",
    "attachment_storage", "attachment_repair", "display_media", "reporting", "ai_client",
}

# old_name -> (subpackage, new_name)
MOVES = {
    # ── discord ───────────────────────────────────────────────────────────────
    "discord_ingest":          ("discord",   "discord_ingest"),
    "channels":                ("discord",   "channels"),
    "worker":                  ("discord",   "worker"),
    "worker_service":          ("discord",   "worker_service"),
    "parser":                  ("discord",   "parser"),
    "transactions":            ("discord",   "transactions"),
    "bookkeeping":             ("discord",   "bookkeeping"),
    "financials":              ("discord",   "financials"),
    "corrections":             ("discord",   "corrections"),
    "reparse":                 ("discord",   "reparse"),
    "reparse_runs":            ("discord",   "reparse_runs"),
    "backfill_requests":       ("discord",   "backfill_requests"),
    "bank_reconciliation":     ("discord",   "bank_reconciliation"),
    "ops_log":                 ("discord",   "ops_log"),
    "plaid_bank_feed":         ("discord",   "plaid_bank_feed"),
    "username_scraper_client": ("discord",   "username_scraper_client"),
    # ── tiktok ────────────────────────────────────────────────────────────────
    "tiktok_ingest":           ("tiktok",    "tiktok_ingest"),
    "tiktok_auth_refresh":     ("tiktok",    "tiktok_auth_refresh"),
    "tiktok_live_chat":        ("tiktok",    "tiktok_live_chat"),
    "tiktok_alerts":           ("tiktok",    "tiktok_alerts"),
    # ── inventory (some renamed) ──────────────────────────────────────────────
    "inventory":               ("inventory", "routes"),
    "inventory_barcode":       ("inventory", "barcode"),
    "inventory_pricing":       ("inventory", "pricing"),
    "inventory_shopify":       ("inventory", "shopify"),
    "inventory_price_updates": ("inventory", "price_updates"),
    "card_scanner":            ("inventory", "card_scanner"),
    "pokemon_scanner":         ("inventory", "pokemon_scanner"),
    "cert_lookup":             ("inventory", "cert_lookup"),
    "card_detect":             ("inventory", "card_detect"),
    "phash_scanner":           ("inventory", "phash_scanner"),
    "degen_eye_v2":            ("inventory", "degen_eye_v2"),
    "degen_eye_v2_training":   ("inventory", "degen_eye_v2_training"),
    "price_cache":             ("inventory", "price_cache"),
    "tcgplayer_sales":         ("inventory", "tcgplayer_sales"),
    "shopify_ingest":          ("inventory", "shopify_ingest"),
    "ai_resolver":             ("inventory", "ai_resolver"),
    # ── team ──────────────────────────────────────────────────────────────────
    "pii":                     ("team",      "pii"),
    "clockify":                ("team",      "clockify"),
    "sms":                     ("team",      "sms"),
    "supply_deals":            ("team",      "supply_deals"),
    "team_notifications":      ("team",      "team_notifications"),
}

# ─── regex for standard relative imports ─────────────────────────────────────
# Matches: [indent] from .X import ... (or from ..X import ...)
IMPORT_RE = re.compile(
    r'^(\s*from\s+)(\.+)([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*)?(\s+import\b.*)',
    re.DOTALL,
)

# Matches: [indent] from . import X as Y  (no module path after dots)
FROM_DOT_IMPORT_RE = re.compile(
    r'^(\s*from\s+\.\s+import\s+)([A-Za-z_]\w*.*)',
    re.DOTALL,
)


def transform_relative_line(line: str, file_subpkg: str | None) -> str:
    """
    Transform one import line.

    file_subpkg:
      None      — file stays at app/ root
      "routers" — file is in app/routers/ (uses .. to reach app/)
      "discord" | "tiktok" | "inventory" | "team" — file moved to that subpackage
    """
    bare = line.rstrip("\n")

    # ── Handle "from . import X as Y" form ───────────────────────────────────
    m2 = FROM_DOT_IMPORT_RE.match(bare)
    if m2:
        if file_subpkg is None:
            # Root file: "from . import pii" → "from .team import pii"
            prefix   = m2.group(1)   # "from . import "
            rest_str = m2.group(2)   # "pii as _pii"
            first    = rest_str.strip().split()[0].rstrip(",")
            if first in MOVES:
                subpkg, _ = MOVES[first]
                new_line = f"from .{subpkg} import {rest_str}"
                indent   = len(bare) - len(bare.lstrip())
                return " " * indent + new_line + ("\n" if line.endswith("\n") else "")
        return line

    # ── Handle standard "from .X import Y" form ──────────────────────────────
    m = IMPORT_RE.match(bare)
    if not m:
        return line

    leading  = m.group(1)   # "from " or "    from "
    dots     = m.group(2)   # "." / ".." / "..."
    modpath  = m.group(3)   # "discord_ingest" or "models.X" (may be None)
    rest     = m.group(4)   # " import foo, bar"
    ndots    = len(dots)
    nl       = "\n" if line.endswith("\n") else ""

    if modpath is None:
        return line

    top_mod      = modpath.split(".")[0]
    sub_modpath  = ".".join(modpath.split(".")[1:])  # anything after first component

    def rebuild(new_dots: int, new_path: str) -> str:
        return f"{leading}{'.' * new_dots}{new_path}{rest}{nl}"

    # ── Root-level file (single-dot imports reference app.X) ─────────────────
    if file_subpkg is None:
        if ndots == 1 and top_mod in MOVES:
            subpkg, new_name = MOVES[top_mod]
            new_path = f"{subpkg}.{new_name}" + (f".{sub_modpath}" if sub_modpath else "")
            return rebuild(1, new_path)
        return line

    # ── Router file (double-dot imports reference app.X) ─────────────────────
    if file_subpkg == "routers":
        if ndots == 2 and top_mod in MOVES:
            subpkg, new_name = MOVES[top_mod]
            new_path = f"{subpkg}.{new_name}" + (f".{sub_modpath}" if sub_modpath else "")
            return rebuild(2, new_path)
        return line

    # ── Vertical file (single-dot imports may reference root or other verticals)
    if ndots == 1:
        if top_mod in ROOT_MODULES:
            # Root module: bump one extra dot
            return rebuild(2, modpath)
        if top_mod in MOVES:
            dest_subpkg, dest_name = MOVES[top_mod]
            new_local = dest_name + (f".{sub_modpath}" if sub_modpath else "")
            if dest_subpkg == file_subpkg:
                # Same vertical — just fix renamed module
                return rebuild(1, new_local)
            else:
                # Cross-vertical
                return rebuild(2, f"{dest_subpkg}.{new_local}")
    return line


def process_file(path: Path, file_subpkg: str | None) -> None:
    text  = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    new_lines = [transform_relative_line(line, file_subpkg) for line in lines]
    new_text = "".join(new_lines)
    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
        print(f"  updated {path.relative_to(REPO)}")


def process_test_file(path: Path) -> None:
    """Fix absolute imports in tests/ (from app.X import → from app.vertical.X_new import)."""
    text = path.read_text(encoding="utf-8")
    changed = False

    for old_name, (subpkg, new_name) in MOVES.items():
        # "from app.X import" → "from app.subpkg.new_name import"
        old_pat = rf'\bfrom\s+app\.{re.escape(old_name)}\b'
        new_str = f"from app.{subpkg}.{new_name}"
        new_text, n = re.subn(old_pat, new_str, text)
        if n:
            text = new_text
            changed = True

    if changed:
        path.write_text(text, encoding="utf-8")
        print(f"  updated {path.relative_to(REPO)}")


def main() -> None:
    # ── Step 1: Create directories ────────────────────────────────────────────
    print("Creating directories...")
    for subpkg in ("discord", "tiktok", "inventory", "team"):
        d = APP / subpkg
        d.mkdir(exist_ok=True)
        init = d / "__init__.py"
        if not init.exists():
            init.write_text("")
            print(f"  created {init.relative_to(REPO)}")

    # ── Step 2: Move files with git mv ────────────────────────────────────────
    print("\nMoving files...")
    for old_name, (subpkg, new_name) in MOVES.items():
        src = APP / f"{old_name}.py"
        dst = APP / subpkg / f"{new_name}.py"
        if src.exists() and not dst.exists():
            subprocess.run(["git", "mv", str(src), str(dst)], check=True, cwd=REPO)
            print(f"  {src.relative_to(REPO)} → {dst.relative_to(REPO)}")
        elif not src.exists():
            print(f"  SKIP {src.relative_to(REPO)} (not found)")
        elif dst.exists():
            print(f"  SKIP {old_name} (destination already exists)")

    # ── Step 3: Update relative imports in app/ ───────────────────────────────
    print("\nUpdating imports in app/...")

    # Root-level files
    for py in sorted(APP.glob("*.py")):
        if py.name != "__init__.py":
            process_file(py, None)

    # Vertical files
    for subpkg in ("discord", "tiktok", "inventory", "team"):
        for py in sorted((APP / subpkg).glob("*.py")):
            if py.name != "__init__.py":
                process_file(py, subpkg)

    # Router files
    for py in sorted((APP / "routers").glob("*.py")):
        if py.name != "__init__.py":
            process_file(py, "routers")

    # ── Step 4: Update absolute imports in tests/ ─────────────────────────────
    print("\nUpdating imports in tests/...")
    for py in sorted(TESTS.glob("*.py")):
        process_test_file(py)

    print("\nDone. Next: python -m compileall app tests")


if __name__ == "__main__":
    main()
