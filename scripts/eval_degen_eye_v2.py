"""Evaluate Degen Eye v2 against a labeled scan manifest.

Manifest formats:
  CSV with headers:
    image_path,expected_name,expected_number,expected_set_id,expected_set_name,category_id

  JSON / JSONL with the same keys. ``image_path`` is resolved relative to the
  manifest file. ``image_b64`` may be provided instead of ``image_path``.

Example:
  .\\.venv\\Scripts\\python.exe scripts\\eval_degen_eye_v2.py ^
      --manifest data\\v2_eval\\manifest.csv ^
      --output data\\v2_eval\\results.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT))

os.environ.setdefault("SESSION_SECRET", "eval")
os.environ.setdefault("DATABASE_URL", "sqlite:///data/v2_eval.db")

from app.degen_eye_v2 import run_v2_pipeline  # noqa: E402


def _norm_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("'", "").replace("’", "").split())


def _number_core(value: Any) -> str:
    raw = str(value or "").split("/", 1)[0].strip().lower()
    return raw.lstrip("0") or raw


def _row_expected(row: dict[str, Any]) -> dict[str, str]:
    return {
        "name": _norm_text(row.get("expected_name") or row.get("name")),
        "number": _number_core(row.get("expected_number") or row.get("number")),
        "set_id": _norm_text(row.get("expected_set_id") or row.get("set_id")),
        "set_name": _norm_text(row.get("expected_set_name") or row.get("set_name")),
    }


def _candidate_matches(candidate: dict[str, Any], expected: dict[str, str]) -> bool:
    if not candidate:
        return False
    name_ok = not expected["name"] or _norm_text(candidate.get("name")) == expected["name"]
    number_ok = not expected["number"] or _number_core(candidate.get("number")) == expected["number"]
    if expected["set_id"]:
        set_ok = _norm_text(candidate.get("set_id")) == expected["set_id"]
    elif expected["set_name"]:
        set_ok = _norm_text(candidate.get("set_name")) == expected["set_name"]
    else:
        set_ok = True
    return name_ok and number_ok and set_ok


def _name_number_matches(candidate: dict[str, Any], expected: dict[str, str]) -> bool:
    if not candidate:
        return False
    name_ok = not expected["name"] or _norm_text(candidate.get("name")) == expected["name"]
    number_ok = not expected["number"] or _number_core(candidate.get("number")) == expected["number"]
    return name_ok and number_ok


def _load_manifest(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("JSON manifest must be a list of row objects")
        return [dict(row) for row in data]
    if suffix in (".jsonl", ".ndjson"):
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows
    raise ValueError("Manifest must be .csv, .json, or .jsonl")


async def _image_b64(row: dict[str, Any], manifest_dir: Path, allow_remote: bool) -> str:
    if row.get("image_b64"):
        return str(row["image_b64"]).split(",", 1)[-1]
    image_path = str(row.get("image_path") or row.get("path") or "").strip()
    if not image_path:
        raise ValueError("row is missing image_path or image_b64")
    parsed = urlparse(image_path)
    if parsed.scheme in ("http", "https"):
        if not allow_remote:
            raise ValueError("remote image URL requires --allow-remote")
        async with httpx.AsyncClient(follow_redirects=True, timeout=20.0) as client:
            resp = await client.get(image_path)
            resp.raise_for_status()
            raw = resp.content
    else:
        p = Path(image_path)
        if not p.is_absolute():
            p = manifest_dir / p
        raw = p.read_bytes()
    return base64.b64encode(raw).decode("ascii")


def _percent(n: int, total: int) -> float:
    return round((n / total) * 100, 2) if total else 0.0


def _latency_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"avg_ms": None, "p50_ms": None, "p95_ms": None}
    ordered = sorted(values)
    p95_idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return {
        "avg_ms": round(sum(values) / len(values), 1),
        "p50_ms": round(statistics.median(values), 1),
        "p95_ms": round(ordered[p95_idx], 1),
    }


async def run_eval(args: argparse.Namespace) -> dict[str, Any]:
    manifest = Path(args.manifest).resolve()
    rows = _load_manifest(manifest)
    if args.limit:
        rows = rows[: args.limit]
    out_path = Path(args.output).resolve() if args.output else (
        _ROOT / "data" / "v2_eval" / f"results_{int(time.time())}.jsonl"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "total": 0,
        "errors": 0,
        "top1_exact": 0,
        "top1_name_number": 0,
        "top3_exact": 0,
        "top3_name_number": 0,
        "matched_status": 0,
        "ambiguous_status": 0,
        "ximilar_fallback": 0,
        "same_art_reprint_risk": 0,
        "latencies": [],
    }

    with out_path.open("w", encoding="utf-8") as out:
        for idx, row in enumerate(rows, start=1):
            expected = _row_expected(row)
            detail: dict[str, Any] = {"row": idx, "expected": expected}
            try:
                b64 = await _image_b64(row, manifest.parent, args.allow_remote)
                category_id = str(row.get("category_id") or args.category_id or "3")
                result = await run_v2_pipeline(b64, category_id=category_id)
                candidates = result.get("candidates") or []
                best = result.get("best_match") or {}
                top3 = candidates[:3]
                exact = _candidate_matches(best, expected)
                name_number = _name_number_matches(best, expected)
                top3_exact = any(_candidate_matches(c, expected) for c in top3)
                top3_name_number = any(_name_number_matches(c, expected) for c in top3)
                debug = result.get("debug") or {}
                v2 = debug.get("v2") or {}
                exactness = ((v2.get("phash") or {}).get("exactness") or {})

                detail.update({
                    "status": result.get("status"),
                    "top1_exact": exact,
                    "top1_name_number": name_number,
                    "top3_exact": top3_exact,
                    "top3_name_number": top3_name_number,
                    "best_match": {
                        "name": best.get("name"),
                        "number": best.get("number"),
                        "set_id": best.get("set_id"),
                        "set_name": best.get("set_name"),
                        "confidence": best.get("confidence"),
                        "source": best.get("source"),
                    },
                    "processing_time_ms": result.get("processing_time_ms"),
                    "engines_used": debug.get("engines_used"),
                    "phash_exactness": exactness,
                    "error": result.get("error"),
                })

                stats["total"] += 1
                stats["top1_exact"] += int(exact)
                stats["top1_name_number"] += int(name_number)
                stats["top3_exact"] += int(top3_exact)
                stats["top3_name_number"] += int(top3_name_number)
                stats["matched_status"] += int(result.get("status") == "MATCHED")
                stats["ambiguous_status"] += int(result.get("status") == "AMBIGUOUS")
                stats["ximilar_fallback"] += int("ximilar" in (debug.get("engines_used") or []))
                stats["same_art_reprint_risk"] += int(bool(exactness.get("same_art_reprint_risk")))
                if isinstance(result.get("processing_time_ms"), (int, float)):
                    stats["latencies"].append(float(result["processing_time_ms"]))
            except Exception as exc:
                stats["total"] += 1
                stats["errors"] += 1
                detail.update({"status": "ERROR", "error": str(exc)})
            out.write(json.dumps(detail, default=str) + "\n")

    total = stats["total"]
    summary = {
        "manifest": str(manifest),
        "output": str(out_path),
        "total": total,
        "errors": stats["errors"],
        "top1_exact_pct": _percent(stats["top1_exact"], total),
        "top1_name_number_pct": _percent(stats["top1_name_number"], total),
        "top3_exact_pct": _percent(stats["top3_exact"], total),
        "top3_name_number_pct": _percent(stats["top3_name_number"], total),
        "matched_status_pct": _percent(stats["matched_status"], total),
        "ambiguous_status_pct": _percent(stats["ambiguous_status"], total),
        "ximilar_fallback_pct": _percent(stats["ximilar_fallback"], total),
        "same_art_reprint_risk_pct": _percent(stats["same_art_reprint_risk"], total),
        **_latency_summary(stats["latencies"]),
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="CSV/JSON/JSONL labeled scan manifest")
    parser.add_argument("--output", help="JSONL details output path")
    parser.add_argument("--limit", type=int, default=0, help="Only evaluate the first N rows")
    parser.add_argument("--category-id", default="3", help="Default TCGTracking category id")
    parser.add_argument("--allow-remote", action="store_true", help="Allow remote image URLs in manifest")
    args = parser.parse_args()
    summary = asyncio.run(run_eval(args))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
