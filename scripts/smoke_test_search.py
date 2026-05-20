#!/usr/bin/env python3
"""Smoke test for Degen Eye free-text search.

Runs a handful of real human queries through ``text_search_cards`` and
judges each against a target set / card name. Writes a markdown report.

Usage:
    python3 scripts/smoke_test_search.py [--out PATH]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.inventory import pokemon_scanner as ps  # noqa: E402


# (label, query, category_id, expectations)
# expectations:
#   expect_name_contains:  case-insensitive substring expected in best_match.name
#                          (string or tuple-of-strings — any match wins)
#   expect_set_contains:   any-of substring list that best_match.set_name must
#                          contain (case-insensitive). None = don't check.
#   expect_number:         exact collector number (first slot before /) or None
#   allow_any_candidate:   when True, pass if any of top 8 candidates match
#                          (used when multiple legitimate answers exist).
CASES = [
    ("charizard 151", "charizard 151", "3", {
        "expect_name_contains": "charizard",
        "expect_set_contains": ["151"],
    }),
    ("pikachu 25", "pikachu 25", "3", {
        "expect_name_contains": "pikachu",
        "expect_set_contains": ["151"],
        "allow_any_candidate": True,
    }),
    ("dragonite fossil", "dragonite fossil", "3", {
        "expect_name_contains": "dragonite",
        "expect_set_contains": ["fossil"],
    }),
    ("base set charizard", "base set charizard", "3", {
        "expect_name_contains": "charizard",
        "expect_set_contains": ["base set", "base"],
    }),
    ("mew ex 151", "mew ex 151", "3", {
        "expect_name_contains": "mew",
        "expect_set_contains": ["151"],
    }),
    ("moonbreon", "moonbreon", "3", {
        "expect_name_contains": ("umbreon",),
        "expect_set_contains": ["evolving skies"],
    }),
    ("giratina v alt art", "giratina v alt art", "3", {
        "expect_name_contains": "giratina",
        "expect_set_contains": ["lost origin", "lost"],
        "allow_any_candidate": True,
    }),
    ("eevee", "eevee", "3", {
        "expect_name_contains": "eevee",
    }),
    ("151 zapdos", "151 zapdos", "3", {
        "expect_name_contains": "zapdos",
        "expect_set_contains": ["151"],
    }),
    ("charizard vmax 20/189", "charizard vmax 20/189", "3", {
        "expect_name_contains": "charizard",
        "expect_number": "20",
    }),
    # Magic
    ("mtg — lightning bolt 4ed", "lightning bolt 4ed", "1", {
        "expect_name_contains": "lightning bolt",
        "expect_set_contains": ["fourth", "4th", "4ed"],
    }),
    ("mtg — sheoldred the apocalypse", "sheoldred the apocalypse", "1", {
        "expect_name_contains": "sheoldred",
    }),
    ("mtg — black lotus", "black lotus", "1", {
        "expect_name_contains": "black lotus",
    }),
    # One Piece
    # OP01-001 is not a real card (the iconic OP01 Luffy is OP01-003).
    # Use OP01-003 so the smoke test reflects a query a real user would type
    # and expect to work.
    ("op — luffy op01-003", "luffy op01-003", "68", {
        "expect_name_contains": "luffy",
        "expect_number": "op01-003",
        "allow_any_candidate": True,
    }),
    ("op — romance dawn luffy", "romance dawn luffy", "68", {
        "expect_name_contains": "luffy",
    }),
    # Riftbound
    ("rb — annie fiery", "annie fiery", "89", {
        "expect_name_contains": "annie",
    }),
]


RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"


def _c(color: str, msg: str) -> str:
    if not sys.stdout.isatty():
        return msg
    return f"{color}{msg}{RESET}"


def _name_matches(name: str, expected) -> bool:
    name = (name or "").lower()
    if isinstance(expected, str):
        return expected.lower() in name
    return any(e.lower() in name for e in expected)


def _set_matches(set_name: str, options) -> bool:
    s = (set_name or "").lower()
    return any(opt.lower() in s for opt in options)


def _number_matches(number: str, expected: str) -> bool:
    if not number:
        return False
    base = number.split("/")[0].strip().lower()
    return base == expected.strip().lower()


def _judge(result: dict, exp: dict) -> tuple[str, str]:
    status = result.get("status")
    if status in ("ERROR", "NO_MATCH"):
        return "FAIL", f"status={status} err={result.get('error')!r}"

    candidates = result.get("candidates") or []
    best = result.get("best_match") or (candidates[0] if candidates else None)
    if not best:
        return "FAIL", "no best_match"

    expect_name = exp.get("expect_name_contains")
    expect_set = exp.get("expect_set_contains")
    expect_num = exp.get("expect_number")
    allow_any = exp.get("allow_any_candidate", False)

    pool = candidates if allow_any else [best]

    def _ok(c: dict) -> bool:
        if expect_name and not _name_matches(c.get("name", ""), expect_name):
            return False
        if expect_set and not _set_matches(c.get("set_name", ""), expect_set):
            return False
        if expect_num and not _number_matches(c.get("number", ""), expect_num):
            return False
        return True

    if any(_ok(c) for c in pool):
        where = "top" if _ok(best) else "cand"
        return "PASS", (
            f"{where} {best.get('name')!r} set={best.get('set_name')!r} "
            f"#{best.get('number')} ({len(candidates)} cands)"
        )

    return "FAIL", (
        f"top={best.get('name')!r} set={best.get('set_name')!r} "
        f"#{best.get('number')} ({len(candidates)} cands)"
    )


async def _run_one(label: str, query: str, cat_id: str, exp: dict):
    t0 = time.monotonic()
    try:
        result = await ps.text_search_cards(query, category_id=cat_id)
    except Exception as exc:
        return "FAIL", f"raised {exc!r}", None, 0.0
    dt_ms = (time.monotonic() - t0) * 1000
    status, note = _judge(result, exp)
    return status, note, result, dt_ms


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=".forge-scratch/search-report.md")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    rows: list[tuple[str, str, str, str, dict, float]] = []
    for label, query, cat_id, exp in CASES:
        print(_c(BLUE, f"→ {label} (cat={cat_id}) q={query!r}"), flush=True)
        status, note, result, dt_ms = await _run_one(label, query, cat_id, exp)
        color = {"PASS": GREEN, "WARN": YELLOW, "FAIL": RED}.get(status, RED)
        print(f"  {_c(color, status)}: {note} ({dt_ms:.0f}ms)\n", flush=True)
        rows.append((label, query, cat_id, status, result or {}, dt_ms))

    passes = sum(1 for r in rows if r[3] == "PASS")
    total = len(rows)

    lines: list[str] = []
    lines.append(f"# Degen Eye search smoke report\n")
    lines.append(f"**Result:** {passes}/{total} PASS\n")
    lines.append(f"| # | Query | Cat | Status | Top name | Top set | # | Cands | ms |\n")
    lines.append("|---|---|---|---|---|---|---|---|---|\n")
    for i, (label, query, cat_id, status, result, dt_ms) in enumerate(rows, 1):
        best = (result.get("best_match") or {}) if result else {}
        cands = result.get("candidates") or [] if result else []
        lines.append(
            f"| {i} | `{query}` | {cat_id} | **{status}** | "
            f"{best.get('name','')} | {best.get('set_name','')} | "
            f"{best.get('number','')} | {len(cands)} | {dt_ms:.0f} |\n"
        )
    lines.append("\n## Details\n")
    for i, (label, query, cat_id, status, result, dt_ms) in enumerate(rows, 1):
        lines.append(f"\n### {i}. {label} — {status}\n")
        lines.append(f"- query: `{query}` (cat {cat_id})\n")
        if result:
            fields = result.get("extracted_fields") or {}
            lines.append(
                f"- parsed: name={fields.get('card_name')!r} "
                f"set={fields.get('set_name')!r} "
                f"num={fields.get('collector_number')!r} "
                f"via={fields.get('extraction_method')}\n"
            )
            cands = result.get("candidates") or []
            for c in cands[:5]:
                lines.append(
                    f"  - {c.get('name','')} · {c.get('set_name','')} "
                    f"#{c.get('number','')}\n"
                )

    with open(args.out, "w") as f:
        f.writelines(lines)

    print(f"\nWrote {args.out} — {passes}/{total} PASS", flush=True)
    return 0 if passes == total else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
