"""Degen Eye v2 employee scan capture storage.

This module keeps a durable, local dataset of real employee scan photos plus
the scanner prediction and the later batch-review confirmation label. The
capture path is intentionally best-effort: failures are logged but must never
block the scanner.
"""
from __future__ import annotations

import base64
import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Optional

from .config import get_settings

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_WRITE_LOCK = Lock()
_CAPTURE_ID_SEP = "_"
_MAX_CANDIDATE_SUMMARIES = 5
_DEFAULT_INDEX_PATH = _ROOT / "data" / "phash_index.sqlite"

_PHASH_SCHEMA = """
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso() -> str:
    return _utc_now().isoformat(timespec="seconds")


def _capture_enabled() -> bool:
    return bool(getattr(get_settings(), "degen_eye_v2_capture_enabled", True))


def capture_root() -> Path:
    raw = str(getattr(get_settings(), "degen_eye_v2_capture_dir", "data/v2_training_scans") or "").strip()
    path = Path(raw or "data/v2_training_scans")
    if not path.is_absolute():
        path = _ROOT / path
    return path


def default_index_path() -> Path:
    raw = str(getattr(get_settings(), "degen_eye_v2_index_path", "data/phash_index.sqlite") or "").strip()
    path = Path(raw or "data/phash_index.sqlite")
    if not path.is_absolute():
        path = _ROOT / path
    return path


def _make_capture_id(now: datetime) -> str:
    return f"{now.strftime('%Y%m%d')}{_CAPTURE_ID_SEP}{uuid.uuid4().hex}"


def _date_dir_from_id(capture_id: str) -> Optional[str]:
    prefix = (capture_id or "").split(_CAPTURE_ID_SEP, 1)[0]
    if len(prefix) != 8 or not prefix.isdigit():
        return None
    return f"{prefix[:4]}-{prefix[4:6]}-{prefix[6:8]}"


def _strip_data_url(image_b64: str) -> str:
    value = (image_b64 or "").strip()
    if "," in value:
        value = value.split(",", 1)[1]
    return value.strip()


def _decode_image(image_b64: str) -> bytes:
    return base64.b64decode(_strip_data_url(image_b64), validate=False)


def _image_kind(raw: bytes) -> tuple[str, str]:
    if raw.startswith(b"\xff\xd8\xff"):
        return ("jpg", "image/jpeg")
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return ("png", "image/png")
    if raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
        return ("webp", "image/webp")
    return ("img", "application/octet-stream")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(_ROOT).as_posix()
    except ValueError:
        return str(path)


def _candidate_summary(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    return {
        "id": candidate.get("id"),
        "name": candidate.get("name"),
        "number": candidate.get("number"),
        "set_id": candidate.get("set_id"),
        "set_name": candidate.get("set_name"),
        "source": candidate.get("source"),
        "confidence": candidate.get("confidence"),
        "score": candidate.get("score"),
        "market_price": candidate.get("market_price"),
        "image_url": candidate.get("image_url"),
        "tcgplayer_url": candidate.get("tcgplayer_url"),
    }


def _prediction_summary(result: dict[str, Any]) -> dict[str, Any]:
    debug = result.get("debug") or {}
    v2 = debug.get("v2") or {}
    phash = v2.get("phash") or {}
    return {
        "captured_at": _utc_iso(),
        "status": result.get("status"),
        "processing_time_ms": result.get("processing_time_ms"),
        "game": result.get("game"),
        "best_match": _candidate_summary(result.get("best_match") or {}),
        "candidates": [
            _candidate_summary(candidate)
            for candidate in (result.get("candidates") or [])[:_MAX_CANDIDATE_SUMMARIES]
            if isinstance(candidate, dict)
        ],
        "debug": {
            "mode": debug.get("mode"),
            "engines_used": debug.get("engines_used"),
            "pipeline_tier": debug.get("pipeline_tier"),
            "extraction_method": debug.get("extraction_method"),
            "phash": {
                "source": phash.get("source"),
                "selected": phash.get("selected"),
                "top": (phash.get("top") or [])[:_MAX_CANDIDATE_SUMMARIES],
                "exactness": phash.get("exactness"),
            },
        },
        "error": result.get("error"),
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)
    except OSError:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _metadata_path(capture_id: str) -> Optional[Path]:
    capture_id = (capture_id or "").strip()
    if not capture_id:
        return None
    date_dir = _date_dir_from_id(capture_id)
    if date_dir:
        path = capture_root() / date_dir / f"{capture_id}.json"
        if path.exists():
            return path
    matches = list(capture_root().glob(f"**/{capture_id}.json"))
    return matches[0] if matches else None


def _load_metadata(capture_id: str) -> tuple[Optional[Path], Optional[dict[str, Any]]]:
    path = _metadata_path(capture_id)
    if path is None:
        return (None, None)
    try:
        return (path, json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        logger.warning("[degen_eye_v2_training] failed to read capture metadata %s", capture_id, exc_info=True)
        return (path, None)


def _update_metadata(capture_id: str, updater) -> bool:
    path, payload = _load_metadata(capture_id)
    if path is None or payload is None:
        return False
    try:
        updated = updater(dict(payload))
        updated["updated_at"] = _utc_iso()
        with _WRITE_LOCK:
            _write_json_atomic(path, updated)
        return True
    except Exception:
        logger.warning("[degen_eye_v2_training] failed to update capture %s", capture_id, exc_info=True)
        return False


def create_scan_capture(
    image_b64: str,
    *,
    source: str,
    category_id: str = "3",
    employee: Optional[dict[str, Any]] = None,
    scan_id: Optional[str] = None,
    request_meta: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Persist one full-resolution employee scan photo and initial metadata."""
    if not _capture_enabled():
        return None
    try:
        raw = _decode_image(image_b64)
        if not raw:
            return None
        ext, content_type = _image_kind(raw)
        now = _utc_now()
        capture_id = _make_capture_id(now)
        day_dir = capture_root() / now.strftime("%Y-%m-%d")
        image_path = day_dir / f"{capture_id}.{ext}"
        meta_path = day_dir / f"{capture_id}.json"
        relative_image_path = _display_path(image_path)
        payload = {
            "capture_id": capture_id,
            "created_at": now.isoformat(timespec="seconds"),
            "updated_at": now.isoformat(timespec="seconds"),
            "source": source,
            "category_id": str(category_id or "3"),
            "scan_id": scan_id,
            "employee": employee or {},
            "request": request_meta or {},
            "image": {
                "path": relative_image_path,
                "bytes": len(raw),
                "content_type": content_type,
            },
            "prediction": None,
            "confirmed_label": None,
            "confirmed_at": None,
            "confirmed_by": None,
            "inventory_item_id": None,
            "training": {
                "eligible": False,
                "indexed_at": None,
                "index_path": None,
                "phash_source": None,
            },
        }
        with _WRITE_LOCK:
            day_dir.mkdir(parents=True, exist_ok=True)
            tmp_image = image_path.with_name(f"{image_path.name}.{uuid.uuid4().hex}.tmp")
            try:
                tmp_image.write_bytes(raw)
                tmp_image.replace(image_path)
            except OSError:
                try:
                    tmp_image.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            _write_json_atomic(meta_path, payload)
        return capture_id
    except Exception:
        logger.warning("[degen_eye_v2_training] failed to create scan capture", exc_info=True)
        return None


def attach_prediction(capture_id: Optional[str], result: dict[str, Any]) -> bool:
    if not capture_id:
        return False

    summary = _prediction_summary(result or {})

    def _apply(payload: dict[str, Any]) -> dict[str, Any]:
        payload["prediction"] = summary
        return payload

    return _update_metadata(capture_id, _apply)


def attach_confirmed_label(
    capture_id: Optional[str],
    label: dict[str, Any],
    *,
    inventory_item_id: Optional[int] = None,
    confirmed_by: Optional[dict[str, Any]] = None,
) -> bool:
    if not capture_id:
        return False
    clean_label = {
        "card_name": (label.get("card_name") or "").strip(),
        "game": (label.get("game") or "").strip(),
        "set_id": (label.get("set_id") or "").strip(),
        "set_name": (label.get("set_name") or "").strip(),
        "card_number": (label.get("card_number") or "").strip(),
        "condition": (label.get("condition") or "").strip(),
        "variant": (label.get("variant") or "").strip(),
        "is_foil": bool(label.get("is_foil")),
        "image_url": (label.get("image_url") or "").strip(),
        "auto_price": label.get("auto_price"),
        "notes": (label.get("notes") or "").strip(),
    }

    eligible = bool(clean_label["card_name"] and (clean_label["card_number"] or clean_label["set_name"]))

    def _apply(payload: dict[str, Any]) -> dict[str, Any]:
        payload["confirmed_label"] = clean_label
        payload["confirmed_at"] = _utc_iso()
        payload["confirmed_by"] = confirmed_by or {}
        payload["inventory_item_id"] = inventory_item_id
        training = dict(payload.get("training") or {})
        training["eligible"] = eligible
        payload["training"] = training
        return payload

    return _update_metadata(capture_id, _apply)


def mark_training_indexed(
    capture_id: str,
    *,
    index_path: str,
    phash_source: str,
) -> bool:
    def _apply(payload: dict[str, Any]) -> dict[str, Any]:
        training = dict(payload.get("training") or {})
        training.update({
            "eligible": True,
            "indexed_at": _utc_iso(),
            "index_path": index_path,
            "phash_source": phash_source,
        })
        payload["training"] = training
        return payload

    return _update_metadata(capture_id, _apply)


def _iter_metadata_paths() -> Iterable[Path]:
    root = capture_root()
    if not root.exists():
        return []
    return root.glob("**/*.json")


def capture_stats() -> dict[str, Any]:
    root = capture_root()
    total = 0
    labeled = 0
    indexed = 0
    bytes_total = 0
    status_counts: dict[str, int] = {}
    if root.exists():
        for path in root.glob("**/*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            total += 1
            image = payload.get("image") or {}
            bytes_total += int(image.get("bytes") or 0)
            if payload.get("confirmed_label"):
                labeled += 1
            training = payload.get("training") or {}
            if training.get("indexed_at"):
                indexed += 1
            prediction = payload.get("prediction") or {}
            status = str(prediction.get("status") or "UNSCANNED")
            status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "enabled": _capture_enabled(),
        "root": str(root),
        "captures": total,
        "labeled": labeled,
        "indexed": indexed,
        "unlabeled": max(0, total - labeled),
        "bytes": bytes_total,
        "status_counts": status_counts,
    }


def iter_confirmed_captures(*, include_indexed: bool = False) -> list[dict[str, Any]]:
    """Return confirmed capture metadata rows for offline training scripts."""
    rows: list[dict[str, Any]] = []
    root = capture_root()
    if not root.exists():
        return rows
    for path in root.glob("**/*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        label = payload.get("confirmed_label") or {}
        if not label.get("card_name"):
            continue
        training = payload.get("training") or {}
        if training.get("indexed_at") and not include_indexed:
            continue
        image = payload.get("image") or {}
        image_path = Path(str(image.get("path") or ""))
        if not image_path.is_absolute():
            image_path = _ROOT / image_path
        payload["_metadata_path"] = str(path)
        payload["_image_path"] = str(image_path)
        rows.append(payload)
    rows.sort(key=lambda row: row.get("created_at") or "")
    return rows


def _phash_to_blob(value: int) -> bytes:
    return int(value).to_bytes(8, byteorder="big", signed=False)


def _norm_training_value(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("'", "").replace("\u2019", "").split())


def _number_core(value: Any) -> str:
    raw = str(value or "").split("/", 1)[0].strip().lower()
    return raw.lstrip("0") or raw


def _open_training_db(index_path: Path) -> sqlite3.Connection:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(index_path)
    conn.executescript(_PHASH_SCHEMA)
    conn.commit()
    return conn


def _set_training_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO phash_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _resolve_canonical_index_row(conn: sqlite3.Connection, label: dict[str, Any]) -> dict[str, str]:
    name = _norm_training_value(label.get("card_name"))
    if not name:
        return {}
    number = _number_core(label.get("card_number"))
    set_id = _norm_training_value(label.get("set_id"))
    set_name = _norm_training_value(label.get("set_name"))
    rows = conn.execute(
        """
        SELECT card_id, number, set_id, set_name, image_url, tcgplayer_url, source
        FROM phash_index
        WHERE lower(name) = ?
        """,
        (name,),
    ).fetchall()
    if not rows:
        return {}
    if number:
        rows = [r for r in rows if _number_core(r[1]) == number]
    if not rows:
        return {}

    non_capture = [r for r in rows if str(r[6] or "") != "employee_capture"] or rows
    if set_id:
        exact = [r for r in non_capture if _norm_training_value(r[2]) == set_id]
        if exact:
            non_capture = exact
    elif set_name:
        exact = [r for r in non_capture if _norm_training_value(r[3]) == set_name]
        if exact:
            non_capture = exact

    row = non_capture[0]
    return {
        "set_id": row[2] or "",
        "set_name": row[3] or "",
        "image_url": row[4] or "",
        "tcgplayer_url": row[5] or "",
    }


def _hash_capture_image(image_path: Path) -> tuple[Optional[int], str]:
    from .card_detect import detect_and_crop
    from .phash_scanner import compute_phash

    raw = image_path.read_bytes()
    crop_bytes, debug = detect_and_crop(raw)
    source = "crop" if crop_bytes else "raw"
    phash = compute_phash(crop_bytes or raw)
    reason = str((debug or {}).get("reason") or source)
    return phash, f"{source}:{reason}"


def _upsert_capture_exemplar(
    conn: sqlite3.Connection,
    *,
    capture_id: str,
    label: dict[str, Any],
    canonical: dict[str, str],
    phash_int: int,
) -> None:
    name = (label.get("card_name") or "").strip()
    number = (label.get("card_number") or "").strip()
    set_id = (label.get("set_id") or canonical.get("set_id") or "employee_capture").strip()
    set_name = (label.get("set_name") or canonical.get("set_name") or set_id).strip()
    image_url = (canonical.get("image_url") or label.get("image_url") or "").strip()
    tcgplayer_url = (canonical.get("tcgplayer_url") or "").strip()
    conn.execute(
        """
        INSERT INTO phash_index (card_id, name, number, set_id, set_name,
                                 phash, image_url, tcgplayer_url, source, indexed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'employee_capture', datetime('now'))
        ON CONFLICT(card_id) DO UPDATE SET
            name=excluded.name,
            number=excluded.number,
            set_id=excluded.set_id,
            set_name=excluded.set_name,
            phash=excluded.phash,
            image_url=excluded.image_url,
            tcgplayer_url=excluded.tcgplayer_url,
            source='employee_capture',
            indexed_at=datetime('now')
        """,
        (
            f"employee_capture:{capture_id}",
            name,
            number,
            set_id,
            set_name,
            _phash_to_blob(phash_int),
            image_url,
            tcgplayer_url,
        ),
    )


def train_confirmed_captures(
    *,
    index_path: Optional[Path | str] = None,
    limit: int = 200,
    include_indexed: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Promote confirmed employee captures into the local pHash index."""
    resolved_index = Path(index_path).resolve() if index_path else default_index_path().resolve()
    rows = iter_confirmed_captures(include_indexed=include_indexed)
    if limit:
        rows = rows[: max(0, int(limit))]

    conn = _open_training_db(resolved_index)
    considered = len(rows)
    indexed = 0
    skipped = 0
    errors: list[dict[str, str]] = []
    t0 = time.monotonic()
    try:
        for row in rows:
            capture_id = str(row.get("capture_id") or "").strip()
            label = row.get("confirmed_label") or {}
            image_path = Path(str(row.get("_image_path") or ""))
            if not capture_id or not label.get("card_name") or not image_path.exists():
                skipped += 1
                continue
            try:
                phash_int, phash_source = _hash_capture_image(image_path)
                if phash_int is None:
                    skipped += 1
                    continue
                canonical = _resolve_canonical_index_row(conn, label)
                if not dry_run:
                    _upsert_capture_exemplar(
                        conn,
                        capture_id=capture_id,
                        label=label,
                        canonical=canonical,
                        phash_int=phash_int,
                    )
                    mark_training_indexed(
                        capture_id,
                        index_path=str(resolved_index),
                        phash_source=phash_source,
                    )
                indexed += 1
                if indexed % 50 == 0:
                    conn.commit()
            except Exception as exc:
                skipped += 1
                if len(errors) < 10:
                    errors.append({"capture_id": capture_id, "error": str(exc)[:300]})

        if not dry_run:
            _set_training_meta(conn, "last_employee_training_at", str(int(time.time())))
            cur = conn.execute("SELECT COUNT(*) FROM phash_index WHERE source = 'employee_capture'")
            _set_training_meta(conn, "employee_training_count", str(cur.fetchone()[0]))
            cur = conn.execute("SELECT COUNT(*) FROM phash_index")
            _set_training_meta(conn, "card_count", str(cur.fetchone()[0]))
            conn.commit()
    finally:
        conn.close()

    return {
        "index_path": str(resolved_index),
        "captures_considered": considered,
        "indexed": indexed,
        "skipped": skipped,
        "dry_run": dry_run,
        "include_indexed": include_indexed,
        "limit": limit,
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "errors": errors,
    }
