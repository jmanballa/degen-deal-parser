#!/usr/bin/env python3
"""Smoke test for Degen Eye scanner across all supported TCGs.

Hits live APIs — Scryfall, OPTCG, Lorcast, YGOPRODeck, TCGTracking.
Exits 0 if every expected game PASSES, 1 otherwise.

Usage:
    python3 scripts/smoke_test_degen_eye.py [--include-optional]
        --include-optional: also require Yu-Gi-Oh + Lorcana to pass
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from dataclasses import asdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import pokemon_scanner as ps  # noqa: E402
from app.pokemon_scanner import ExtractedFields, ScoredCandidate  # noqa: E402


RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"


def _c(color: str, msg: str) -> str:
    if not sys.stdout.isatty():
        return msg
    return f"{color}{msg}{RESET}"


def _short(v, n: int = 72) -> str:
    s = "" if v is None else str(v)
    return s if len(s) <= n else s[: n - 1] + "…"


async def _run_one(
    label: str,
    category_id: str,
    fields_kwargs: dict,
    *,
    price_required: bool = True,
    must_contain_set: str | None = None,
) -> tuple[str, str]:
    """Call the scanner routing + price enrichment. Return (status, note).

    status: "PASS" | "WARN" | "FAIL"

    If ``must_contain_set`` is given, the test only requires that at least
    one candidate's ``set_name`` contains that substring (case-insensitive) —
    the real pipeline's scorer picks the right candidate; the raw API ordering
    isn't meaningful for multi-print games like Pokemon.
    """
    fields = ExtractedFields(**fields_kwargs)
    t0 = time.monotonic()
    try:
        cands = await ps._lookup_candidates_by_category(fields, category_id)
    except Exception as exc:
        return "FAIL", f"lookup raised: {exc!r}"
    dt = (time.monotonic() - t0) * 1000

    if not cands:
        return "FAIL", f"0 candidates in {dt:.0f}ms"

    # If caller pinned a target set, pick that candidate for enrichment
    # instead of relying on API ordering.
    pick = cands[0]
    if must_contain_set:
        needle = must_contain_set.lower()
        matches = [c for c in cands if needle in (c.set_name or "").lower()]
        if not matches:
            return "FAIL", (
                f"{len(cands)} cands but none match set {must_contain_set!r}; "
                f"top={cands[0].name!r} set={cands[0].set_name!r}"
            )
        pick = matches[0]

    missing: list[str] = []
    if not pick.name:
        missing.append("name")
    if not pick.image_url:
        missing.append("image_url")
    if not pick.set_name:
        missing.append("set_name")
    if missing:
        return "FAIL", f"{len(cands)} cands but picked missing {missing}; picked={pick.name!r}"

    # Run price enrichment on a ScoredCandidate copy
    scored = ScoredCandidate(**asdict(pick))
    t1 = time.monotonic()
    try:
        await ps._enrich_price_fast(scored, ptcg_key="", category_id=category_id)
    except Exception as exc:
        return "WARN", (
            f"lookup OK ({len(cands)} cands, img ok) but enrich raised: {exc!r}"
        )
    enrich_dt = (time.monotonic() - t1) * 1000

    has_price = scored.market_price is not None or any(
        v.get("price") is not None for v in (scored.available_variants or [])
    )
    tcgp = scored.tcgplayer_url or pick.tcgplayer_url

    note = (
        f"{len(cands)} cands in {dt:.0f}ms; pick={pick.name!r} "
        f"set={pick.set_name!r} num={pick.number!r} "
        f"img={'Y' if pick.image_url else 'N'} "
        f"tcgp={'Y' if tcgp else 'N'} "
        f"price={'Y' if has_price else 'N'} (enrich {enrich_dt:.0f}ms)"
    )
    if not has_price:
        return ("FAIL" if price_required else "WARN"), note
    return "PASS", note


CASES = [
    # (label, category_id, fields_kwargs, required, must_contain_set)
    ("Magic — Lightning Bolt 4ED 208", "1", {
        "card_name": "Lightning Bolt",
        "set_name": "Fourth Edition",
        "collector_number": "208",
    }, True, None),
    ("Magic — Sheoldred DMU 107", "1", {
        "card_name": "Sheoldred, the Apocalypse",
        "set_name": "Dominaria United",
        "collector_number": "107",
    }, True, None),
    ("One Piece — Luffy OP01-003", "68", {
        "card_name": "Monkey D. Luffy",
        "set_name": "Romance Dawn",
        "collector_number": "OP01-003",
    }, True, None),
    ("One Piece — Zoro OP01-025", "68", {
        "card_name": "Roronoa Zoro",
        "set_name": "Romance Dawn",
        "collector_number": "OP01-025",
    }, True, None),
    ("Riftbound — Annie Fiery OGS 001", "89", {
        "card_name": "Annie",
        "set_name": "Origins: Proving Grounds",
        "collector_number": "001/024",
    }, True, None),
    ("Riftbound — Firestorm OGS 002", "89", {
        "card_name": "Firestorm",
        "set_name": "Origins: Proving Grounds",
        "collector_number": "002/024",
    }, True, None),
    ("Yu-Gi-Oh — Dark Magician", "2", {
        "card_name": "Dark Magician",
    }, False, None),
    ("Lorcana — Elsa", "71", {
        "card_name": "Elsa",
    }, False, None),
    # Regression for Pokemon — real pipeline's scorer picks the right set,
    # so we just need the right set to be *among* the candidates.
    ("Pokemon — Pikachu 25 S&V:151", "3", {
        "card_name": "Pikachu",
        "set_name": "Scarlet & Violet: 151",
        "collector_number": "25",
    }, True, "151"),
]


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--include-optional",
        action="store_true",
        help="Fail the run if Yu-Gi-Oh/Lorcana don't pass (lower-priority games).",
    )
    args = parser.parse_args()

    results: list[tuple[str, str, str, bool]] = []
    for label, cat_id, kwargs, price_required, must_contain_set in CASES:
        print(_c(BLUE, f"→ {label} (cat={cat_id})"), flush=True)
        status, note = await _run_one(
            label, cat_id, kwargs,
            price_required=price_required,
            must_contain_set=must_contain_set,
        )
        results.append((label, status, note, price_required))
        color = {"PASS": GREEN, "WARN": YELLOW, "FAIL": RED}[status]
        print(f"  {_c(color, status)}: {note}\n", flush=True)

    print("=" * 72)
    print("Summary:")
    required_failed = 0
    optional_failed = 0
    for label, status, _note, required in results:
        color = {"PASS": GREEN, "WARN": YELLOW, "FAIL": RED}[status]
        tag = "[req]" if required else "[opt]"
        print(f"  {_c(color, status):>4} {tag} {label}")
        if status == "FAIL":
            if required:
                required_failed += 1
            else:
                optional_failed += 1

    print()
    if required_failed == 0 and (optional_failed == 0 or not args.include_optional):
        print(_c(GREEN, "ALL REQUIRED GAMES PASSED"))
        if optional_failed:
            print(_c(YELLOW, f"{optional_failed} optional game(s) failed (Yu-Gi-Oh/Lorcana)"))
        return 0

    print(_c(RED, f"{required_failed} required game(s) failed"))
    if optional_failed:
        print(_c(YELLOW, f"{optional_failed} optional game(s) failed"))
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
