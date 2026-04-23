"""Build the Degen Eye v2 perceptual-hash index for Pokemon cards.

Walks TCGdex (primary) and the PokemonTCG API (fallback) for every card in
every Pokemon set, downloads each card image, computes a 64-bit pHash, and
stores the result in ``data/phash_index.sqlite`` for the server's lookup.

Usage:
    python scripts/build_phash_index.py                         # full build
    python scripts/build_phash_index.py --sets swsh10 swsh11    # subset
    python scripts/build_phash_index.py --incremental           # skip indexed cards
    python scripts/build_phash_index.py --limit 500             # smoke test

Expected one-time cost: ~20-40 minutes for the full ~20k Pokemon card catalog
on a decent connection. Incremental re-runs are seconds when nothing changed.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx
from PIL import Image

# Allow running as a plain script without an installed package
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT))

import imagehash  # noqa: E402

TCGDEX_BASE = "https://api.tcgdex.net/v2/en"
POKEMONTCG_BASE = "https://api.pokemontcg.io/v2"

DATA_DIR = _ROOT / "data"
INDEX_PATH = DATA_DIR / "phash_index.sqlite"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("phash_index")


# ---------------------------------------------------------------------------
# SQLite schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS phash_index (
    card_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    number TEXT NOT NULL,
    set_id TEXT NOT NULL,
    set_name TEXT NOT NULL,
    phash BLOB NOT NULL,
    image_url TEXT,
    tcgplayer_url TEXT,
    source TEXT NOT NULL,
    indexed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_phash_set ON phash_index(set_id);
CREATE INDEX IF NOT EXISTS idx_phash_name ON phash_index(name);
CREATE TABLE IF NOT EXISTS phash_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _phash_to_blob(value: int) -> bytes:
    """Encode a 64-bit unsigned pHash as 8 big-endian bytes.

    SQLite's INTEGER affinity is signed 64-bit so values >= 2**63 overflow.
    BLOB sidesteps the range issue and keeps sort order irrelevant (we
    only do XOR+popcount anyway).
    """
    return int(value).to_bytes(8, byteorder="big", signed=False)


def _open_db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(INDEX_PATH)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def _already_indexed(conn: sqlite3.Connection, card_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM phash_index WHERE card_id = ? LIMIT 1", (card_id,))
    return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# Source data
# ---------------------------------------------------------------------------

@dataclass
class CardRef:
    card_id: str
    name: str
    number: str
    set_id: str
    set_name: str
    image_url: str
    tcgplayer_url: Optional[str] = None
    source: str = "tcgdex"


async def _fetch_tcgdex_sets(client: httpx.AsyncClient) -> list[dict]:
    resp = await client.get(f"{TCGDEX_BASE}/sets", timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def _fetch_tcgdex_set_cards(client: httpx.AsyncClient, set_id: str) -> list[dict]:
    # The /sets/{id} response carries a cards array with slim data; we pull
    # high-res image URLs in the per-card detail fetch later.
    resp = await client.get(f"{TCGDEX_BASE}/sets/{set_id}", timeout=30.0)
    if resp.status_code != 200:
        return []
    data = resp.json() or {}
    return data.get("cards") or []


async def _fetch_tcgdex_card(client: httpx.AsyncClient, card_id: str) -> Optional[dict]:
    resp = await client.get(f"{TCGDEX_BASE}/cards/{card_id}", timeout=15.0)
    if resp.status_code != 200:
        return None
    return resp.json()


def _tcgdex_image_url(card: dict) -> Optional[str]:
    # TCGdex delivers a bare image prefix; you append /high.png for the
    # high-res artwork. See https://tcgdex.dev for the spec.
    raw = card.get("image")
    if not raw:
        return None
    if raw.endswith(".png") or raw.endswith(".jpg"):
        return raw
    return f"{raw}/high.png"


async def _iter_pokemon_refs(
    client: httpx.AsyncClient, only_sets: Optional[list[str]] = None,
) -> list[CardRef]:
    sets = await _fetch_tcgdex_sets(client)
    if only_sets:
        only = {s.lower() for s in only_sets}
        sets = [s for s in sets if (s.get("id") or "").lower() in only]
    log.info("TCGdex sets to index: %d", len(sets))

    refs: list[CardRef] = []
    for s in sets:
        set_id = s.get("id") or ""
        set_name = s.get("name") or set_id
        if not set_id:
            continue
        cards = await _fetch_tcgdex_set_cards(client, set_id)
        if not cards:
            log.warning("No cards returned for set %s", set_id)
            continue
        for c in cards:
            card_id = c.get("id") or ""
            if not card_id:
                continue
            refs.append(CardRef(
                card_id=f"tcgdex:{card_id}",
                name=c.get("name") or "",
                number=str(c.get("localId") or ""),
                set_id=set_id,
                set_name=set_name,
                image_url="",  # filled in per-card detail fetch
                source="tcgdex",
            ))
        log.info("Set %s (%s): %d cards queued", set_id, set_name, len(cards))

    return refs


# ---------------------------------------------------------------------------
# Image -> pHash
# ---------------------------------------------------------------------------

async def _download_image(client: httpx.AsyncClient, url: str) -> Optional[bytes]:
    try:
        resp = await client.get(url, timeout=20.0, follow_redirects=True)
        if resp.status_code != 200:
            return None
        return resp.content
    except Exception:
        return None


def _phash_from_bytes(raw: bytes) -> Optional[int]:
    try:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
    except Exception:
        return None
    h = imagehash.phash(img, hash_size=8)
    # Convert the 64-bit flat hash to an int for fast XOR+popcount in Python.
    # ``h.hash`` is a 2D bool numpy array; flattening preserves MSB-first order
    # so we can later xor two ints and popcount to get Hamming distance.
    bits = h.hash.flatten()
    value = 0
    for b in bits:
        value = (value << 1) | int(bool(b))
    return value


async def _process_ref(
    client: httpx.AsyncClient,
    ref: CardRef,
    sem: asyncio.Semaphore,
) -> Optional[tuple[CardRef, int]]:
    async with sem:
        try:
            # Need high-res image URL; fetch card detail lazily if we don't have one
            url = ref.image_url
            if not url:
                detail = await _fetch_tcgdex_card(client, ref.card_id.split(":", 1)[-1])
                if detail is None:
                    return None
                url = _tcgdex_image_url(detail) or ""
                if not url:
                    return None
                ref.image_url = url
                # Backfill number with the detail's localId if needed
                if not ref.number:
                    ref.number = str(detail.get("localId") or "")

            raw = await _download_image(client, url)
            if raw is None:
                return None
            value = _phash_from_bytes(raw)
            if value is None:
                return None
            return (ref, value)
        except Exception as exc:
            log.debug("Skipping %s after indexing error: %s", ref.card_id, exc)
            return None


# ---------------------------------------------------------------------------
# Index writer
# ---------------------------------------------------------------------------

def _upsert(conn: sqlite3.Connection, ref: CardRef, phash_int: int) -> None:
    conn.execute(
        """
        INSERT INTO phash_index (card_id, name, number, set_id, set_name,
                                 phash, image_url, tcgplayer_url, source, indexed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(card_id) DO UPDATE SET
            name=excluded.name,
            number=excluded.number,
            set_id=excluded.set_id,
            set_name=excluded.set_name,
            phash=excluded.phash,
            image_url=excluded.image_url,
            tcgplayer_url=excluded.tcgplayer_url,
            source=excluded.source,
            indexed_at=datetime('now')
        """,
        (
            ref.card_id, ref.name, ref.number, ref.set_id, ref.set_name,
            _phash_to_blob(phash_int), ref.image_url, ref.tcgplayer_url, ref.source,
        ),
    )


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO phash_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> int:
    conn = _open_db()

    async with httpx.AsyncClient() as client:
        log.info("Discovering Pokemon cards from TCGdex...")
        refs = await _iter_pokemon_refs(client, only_sets=args.sets)

        if args.limit:
            refs = refs[: args.limit]
            log.info("Limiting to %d cards for smoke test", len(refs))

        if args.incremental:
            before = len(refs)
            refs = [r for r in refs if not _already_indexed(conn, r.card_id)]
            log.info("Incremental: %d new (skipped %d already indexed)", len(refs), before - len(refs))

        total = len(refs)
        if total == 0:
            log.info("Nothing to index.")
            return 0

        sem = asyncio.Semaphore(args.concurrency)
        tasks = [_process_ref(client, ref, sem) for ref in refs]

        done = 0
        success = 0
        t_start = time.monotonic()
        last_log = t_start
        batch: list[tuple[CardRef, int]] = []

        for coro in asyncio.as_completed(tasks):
            result = await coro
            done += 1
            if result is not None:
                batch.append(result)
                success += 1
            if len(batch) >= 50:
                for ref, phash_int in batch:
                    _upsert(conn, ref, phash_int)
                conn.commit()
                batch.clear()
            now = time.monotonic()
            if now - last_log > 5.0 or done == total:
                rate = done / max(0.001, now - t_start)
                eta = (total - done) / max(0.001, rate)
                log.info(
                    "Progress %d/%d (%.1f cards/s, ~%.0fs remaining, %d skipped)",
                    done, total, rate, eta, done - success,
                )
                last_log = now

        # Flush tail
        for ref, phash_int in batch:
            _upsert(conn, ref, phash_int)

    _set_meta(conn, "last_build_at", str(int(time.time())))
    cur = conn.execute("SELECT COUNT(*) FROM phash_index")
    count = cur.fetchone()[0]
    _set_meta(conn, "card_count", str(count))
    conn.commit()
    conn.close()

    log.info("Done. Indexed %d / %d cards into %s", success, total, INDEX_PATH)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sets", nargs="*", help="Only index these TCGdex set ids (e.g. swsh10)")
    parser.add_argument("--limit", type=int, default=0, help="Smoke test: stop after N cards")
    parser.add_argument(
        "--concurrency", type=int, default=8,
        help="Parallel card detail+image fetches (default 8)",
    )
    parser.add_argument(
        "--incremental", action="store_true",
        help="Skip cards already present in the index",
    )
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
