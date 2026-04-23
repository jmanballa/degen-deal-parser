"""Degen Eye v2 — perceptual-hash card lookup.

Owns the in-memory copy of ``data/phash_index.sqlite`` and offers a
sub-100ms nearest-neighbor lookup by Hamming distance.

No OCR, no LLM, no cloud call. The index is built offline by
``scripts/build_phash_index.py``; this module just reads it once per
process and stays in memory for fast scans.

Confidence banding (64-bit pHash, hash_size=8):
    distance 0-6  -> HIGH   (same-art match, often the exact printing)
    distance 7-12 -> MEDIUM (likely correct but verify — e.g. near-reprint)
    distance >12  -> LOW    (probably wrong; fall back to Ximilar)
"""
from __future__ import annotations

import io
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Optional

import imagehash
from PIL import Image

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_INDEX_PATH = _ROOT / "data" / "phash_index.sqlite"


# ---------------------------------------------------------------------------
# In-memory index
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PhashEntry:
    card_id: str
    name: str
    number: str
    set_id: str
    set_name: str
    phash: int
    image_url: Optional[str]
    tcgplayer_url: Optional[str]
    source: str


@dataclass
class PhashMatch:
    entry: PhashEntry
    distance: int
    confidence: str  # "HIGH" | "MEDIUM" | "LOW"


_INDEX: Optional[list[PhashEntry]] = None
_INDEX_LOCK = Lock()
_INDEX_METADATA: dict[str, str] = {}
_INDEX_PATH: Path = _DEFAULT_INDEX_PATH


HIGH_THRESHOLD = 6
MEDIUM_THRESHOLD = 12


def _band(distance: int) -> str:
    if distance <= HIGH_THRESHOLD:
        return "HIGH"
    if distance <= MEDIUM_THRESHOLD:
        return "MEDIUM"
    return "LOW"


def set_index_path(path: Path | str) -> None:
    """Override the index location (tests and alternate deployments)."""
    global _INDEX_PATH, _INDEX, _INDEX_METADATA
    with _INDEX_LOCK:
        _INDEX_PATH = Path(path)
        _INDEX = None
        _INDEX_METADATA = {}


def _load_index(force: bool = False) -> list[PhashEntry]:
    """Lazily load (or reload) the index from SQLite into memory."""
    global _INDEX, _INDEX_METADATA
    with _INDEX_LOCK:
        if _INDEX is not None and not force:
            return _INDEX
        if not _INDEX_PATH.exists():
            logger.warning(
                "[phash_scanner] Index not found at %s — lookup will return empty "
                "until scripts/build_phash_index.py has run.",
                _INDEX_PATH,
            )
            _INDEX = []
            _INDEX_METADATA = {}
            return _INDEX

        t_start = time.monotonic()
        conn = sqlite3.connect(f"file:{_INDEX_PATH}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "SELECT card_id, name, number, set_id, set_name, phash, "
                "image_url, tcgplayer_url, source FROM phash_index"
            ).fetchall()
            meta_rows = conn.execute("SELECT key, value FROM phash_meta").fetchall()
        finally:
            conn.close()
        def _decode_phash(raw) -> int:
            # Backwards compatible: earlier drafts stored INTEGER; current
            # schema uses BLOB to dodge SQLite's signed INT64 range limit.
            if isinstance(raw, (bytes, bytearray, memoryview)):
                return int.from_bytes(bytes(raw), byteorder="big", signed=False)
            return int(raw) & ((1 << 64) - 1)

        _INDEX = [
            PhashEntry(
                card_id=r[0], name=r[1], number=r[2], set_id=r[3], set_name=r[4],
                phash=_decode_phash(r[5]), image_url=r[6], tcgplayer_url=r[7], source=r[8],
            )
            for r in rows
        ]
        _INDEX_METADATA = {k: v for (k, v) in meta_rows}
        logger.info(
            "[phash_scanner] Loaded %d pHash entries from %s in %.0fms",
            len(_INDEX), _INDEX_PATH, (time.monotonic() - t_start) * 1000,
        )
        return _INDEX


def get_index_stats() -> dict:
    entries = _load_index()
    return {
        "index_path": str(_INDEX_PATH),
        "card_count": len(entries),
        "sets": sorted({e.set_id for e in entries}),
        "metadata": dict(_INDEX_METADATA),
        "index_exists": _INDEX_PATH.exists(),
    }


def reload_index() -> int:
    """Force a reload — call after a rebuild so the server picks up new data."""
    return len(_load_index(force=True))


# ---------------------------------------------------------------------------
# pHash compute
# ---------------------------------------------------------------------------

def compute_phash(image_bytes: bytes) -> Optional[int]:
    """Compute a 64-bit pHash (hash_size=8) for the given image bytes.

    Returns None on decode failure so callers can surface a clean error.
    """
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception as exc:
        logger.warning("[phash_scanner] compute_phash decode failed: %s", exc)
        return None
    h = imagehash.phash(img, hash_size=8)
    bits = h.hash.flatten()
    value = 0
    for b in bits:
        value = (value << 1) | int(bool(b))
    return value


def _hamming(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------

def lookup(
    image_bytes: bytes,
    *,
    top_n: int = 5,
    set_filter: Optional[str] = None,
) -> tuple[Optional[int], list[PhashMatch]]:
    """Find the closest index entries for ``image_bytes``.

    Returns ``(phash_value, matches)``. ``matches`` is sorted by Hamming
    distance ascending. If the index is empty or the image can't be decoded,
    the list is empty but the function doesn't raise.

    ``set_filter``: optional TCGdex set id to constrain the search (used
    when the caller already knows the set, e.g. from Ximilar context).
    """
    index = _load_index()
    if not index:
        return (None, [])

    value = compute_phash(image_bytes)
    if value is None:
        return (None, [])

    t_start = time.monotonic()
    matches: list[PhashMatch] = []
    # Plain linear scan — 20k entries * XOR+popcount = a few ms in Python.
    for entry in index:
        if set_filter and entry.set_id != set_filter:
            continue
        d = _hamming(value, entry.phash)
        matches.append(PhashMatch(entry=entry, distance=d, confidence=_band(d)))

    matches.sort(key=lambda m: m.distance)
    top = matches[:top_n]

    elapsed = (time.monotonic() - t_start) * 1000
    if top:
        logger.info(
            "[phash_scanner] Lookup: best=%s #%s d=%d (%s), searched=%d in %.1fms",
            top[0].entry.name, top[0].entry.number, top[0].distance,
            top[0].confidence, len(index), elapsed,
        )
    else:
        logger.info("[phash_scanner] Lookup: no entries searched (set_filter=%s)", set_filter)

    return (value, top)


def has_index() -> bool:
    """Cheap check for callers that want to skip v2 if the index isn't built."""
    return _INDEX_PATH.exists() and len(_load_index()) > 0
