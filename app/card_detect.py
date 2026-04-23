"""Degen Eye v2 — card detection and perspective rectification.

Finds a trading card in a phone photo, crops it out, and returns a
canonical 300x420 portrait image ready for perceptual-hash lookup.

The goal is to give the downstream pHash a clean, tight crop regardless
of where the user holds the card or how they frame it. Consumer card
scanners (CardEye, Collectr, etc.) all do this preprocessing step.

Usage:
    from app.card_detect import detect_and_crop
    crop_bytes, debug = detect_and_crop(raw_bytes)
    if crop_bytes is None:
        # Fall back to the raw image — still usable, just less robust.
        crop_bytes = raw_bytes

If OpenCV is unavailable (import fails at runtime for any reason), the
function returns ``(None, {"reason": "opencv_unavailable"})`` so callers
can degrade gracefully instead of crashing.
"""
from __future__ import annotations

import hashlib
import io
import logging
from typing import Any, Optional

import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

_CROP_W = 300
_CROP_H = 420
_MIN_AREA_FRAC = 0.08   # at least 8% of the image
_ASPECT_MIN = 0.55      # card aspect tolerance (w/h)
_ASPECT_MAX = 0.85

try:
    import cv2  # type: ignore
    _HAVE_CV2 = True
except Exception as exc:  # pragma: no cover
    cv2 = None  # type: ignore
    _HAVE_CV2 = False
    logger.warning("[card_detect] OpenCV unavailable, falling back to raw image: %s", exc)


def _order_corners(pts: np.ndarray) -> np.ndarray:
    """Return corners in (top-left, top-right, bottom-right, bottom-left) order."""
    pts = pts.reshape(4, 2).astype(np.float32)
    rect = np.zeros((4, 2), dtype=np.float32)
    s = pts.sum(axis=1)
    rect[0] = pts[np.argmin(s)]   # top-left has smallest x+y
    rect[2] = pts[np.argmax(s)]   # bottom-right has largest x+y
    diff = np.diff(pts, axis=1)
    rect[1] = pts[np.argmin(diff)]  # top-right has smallest y-x
    rect[3] = pts[np.argmax(diff)]  # bottom-left has largest y-x
    return rect


def _score_quadrilateral(approx: np.ndarray, img_area: float) -> Optional[tuple[float, np.ndarray]]:
    """Return (score, ordered_corners) for a candidate contour, or None if rejected.

    Score rewards card-like aspect ratios and large area. We don't just
    pick ``largest`` because desks and keyboards often form the largest
    rectangle in the frame; aspect-ratio sanity guards against that.
    """
    if len(approx) != 4:
        return None
    area = cv2.contourArea(approx)
    if area < img_area * _MIN_AREA_FRAC:
        return None

    rect = _order_corners(approx)
    (tl, tr, br, bl) = rect
    width_top = np.linalg.norm(tr - tl)
    width_bot = np.linalg.norm(br - bl)
    height_l = np.linalg.norm(bl - tl)
    height_r = np.linalg.norm(br - tr)
    width = max(width_top, width_bot)
    height = max(height_l, height_r)
    if width < 30 or height < 30:
        return None

    # Orient portrait (cards are taller than wide)
    aspect = min(width, height) / max(width, height)
    if aspect < _ASPECT_MIN or aspect > _ASPECT_MAX:
        return None

    # Penalize non-rectangular (skew too extreme)
    top_bot_ratio = min(width_top, width_bot) / max(width_top, width_bot)
    l_r_ratio = min(height_l, height_r) / max(height_l, height_r)
    if top_bot_ratio < 0.75 or l_r_ratio < 0.75:
        return None

    # Score: area fraction * aspect-closeness to 0.714 (ideal Pokemon card ratio)
    aspect_bonus = 1.0 - abs(aspect - 0.714) * 2.0
    area_frac = area / img_area
    score = area_frac * max(0.1, aspect_bonus)
    return (score, rect)


def _warp_to_portrait(img: np.ndarray, rect: np.ndarray) -> np.ndarray:
    """Warp the four-corner region to a canonical portrait crop."""
    dst = np.array([
        [0, 0],
        [_CROP_W - 1, 0],
        [_CROP_W - 1, _CROP_H - 1],
        [0, _CROP_H - 1],
    ], dtype=np.float32)
    # If the detected quad is landscape-oriented, rotate the destination
    # so the long axis becomes the card's long axis.
    (tl, tr, br, bl) = rect
    width = max(np.linalg.norm(tr - tl), np.linalg.norm(br - bl))
    height = max(np.linalg.norm(bl - tl), np.linalg.norm(br - tr))
    if width > height:
        # landscape capture — rotate the destination corners clockwise once
        dst = np.array([
            [_CROP_W - 1, 0],
            [_CROP_W - 1, _CROP_H - 1],
            [0, _CROP_H - 1],
            [0, 0],
        ], dtype=np.float32)
    transform = cv2.getPerspectiveTransform(rect, dst)
    return cv2.warpPerspective(img, transform, (_CROP_W, _CROP_H))


def detect_and_crop(raw_bytes: bytes) -> tuple[Optional[bytes], dict[str, Any]]:
    """Find the card in ``raw_bytes`` and return a JPEG-encoded portrait crop.

    Returns ``(jpeg_bytes, debug)`` on success or ``(None, debug)`` on
    failure. ``debug`` always contains a ``reason`` key explaining either
    success parameters or why the detection failed.
    """
    if not _HAVE_CV2:
        return (None, {"reason": "opencv_unavailable"})

    debug: dict[str, Any] = {}

    try:
        arr = np.frombuffer(raw_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception as exc:
        return (None, {"reason": f"imdecode_failed: {exc}"})
    if img is None:
        return (None, {"reason": "imdecode_returned_none"})

    h, w = img.shape[:2]
    img_area = float(h * w)
    debug["input_size"] = [w, h]

    # Downscale for edge detection: works just as well on 800px and is ~3x faster
    max_dim = max(h, w)
    scale = 1.0
    if max_dim > 900:
        scale = 900.0 / max_dim
        work = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
    else:
        work = img
    gray = cv2.cvtColor(work, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    # Adaptive Canny thresholds from the image's median brightness — more
    # robust to varied lighting than a fixed 50/150.
    v = float(np.median(blurred))
    lo = int(max(0, 0.66 * v))
    hi = int(min(255, 1.33 * v))
    edges = cv2.Canny(blurred, lo, hi)
    # Dilate so near-connected edges form closed contours
    edges = cv2.dilate(edges, np.ones((3, 3), dtype=np.uint8), iterations=1)

    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    work_area = float(work.shape[0] * work.shape[1])

    best: Optional[tuple[float, np.ndarray]] = None
    for c in sorted(contours, key=cv2.contourArea, reverse=True)[:8]:
        peri = cv2.arcLength(c, True)
        approx = cv2.approxPolyDP(c, 0.02 * peri, True)
        scored = _score_quadrilateral(approx, work_area)
        if scored is None:
            continue
        if best is None or scored[0] > best[0]:
            best = scored

    if best is None:
        debug["reason"] = "no_quadrilateral_found"
        debug["contours_tried"] = min(8, len(contours))
        return (None, debug)

    # Scale the corners back to the full-resolution image
    score, rect = best
    rect = rect / max(0.0001, scale)

    # If the detected quad sits flush against the image edges the input is
    # effectively a pre-cropped card (TCGdex image, card_scanner upload, etc.)
    # and warping it would only introduce tiny resampling artifacts that hurt
    # the pHash. Bail out so the caller uses the raw bytes for hashing.
    xs = rect[:, 0]
    ys = rect[:, 1]
    left_margin = float(xs.min())
    right_margin = float(w - xs.max())
    top_margin = float(ys.min())
    bottom_margin = float(h - ys.max())
    margins = [
        left_margin / w if w else 0,
        right_margin / w if w else 0,
        top_margin / h if h else 0,
        bottom_margin / h if h else 0,
    ]
    edge_margin_frac = min(margins)
    max_edge_margin_frac = max(margins)
    # Treat the image as pre-cropped only when the detected card is flush on
    # all sides. A real camera frame often has one edge near the border; using
    # the minimum margin alone caused those captures to skip rectification.
    if max_edge_margin_frac < 0.015:
        debug["reason"] = "image_already_cropped"
        debug["edge_margin_frac"] = round(edge_margin_frac, 4)
        debug["max_edge_margin_frac"] = round(max_edge_margin_frac, 4)
        return (None, debug)

    warped = _warp_to_portrait(img, rect.astype(np.float32))

    # Encode as JPEG
    ok, buf = cv2.imencode(".jpg", warped, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
    if not ok:
        debug["reason"] = "jpeg_encode_failed"
        return (None, debug)

    debug.update({
        "reason": "ok",
        "score": round(float(score), 4),
        "corners": rect.tolist(),
        "crop_size": [_CROP_W, _CROP_H],
    })
    return (bytes(buf), debug)


def detect_box(raw_bytes: bytes) -> dict[str, Any]:
    """Lightweight variant used by Phase B auto-capture.

    Returns ``{found, box?, stability_hash?, score?, reason}`` without the
    expensive perspective warp — useful when the caller just needs to know
    whether a card is present and where its corners sit.
    """
    if not _HAVE_CV2:
        return {"found": False, "reason": "opencv_unavailable"}

    try:
        arr = np.frombuffer(raw_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception:
        return {"found": False, "reason": "imdecode_failed"}
    if img is None:
        return {"found": False, "reason": "imdecode_returned_none"}

    h, w = img.shape[:2]
    max_dim = max(h, w)
    scale = 1.0
    if max_dim > 600:
        scale = 600.0 / max_dim
        img = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_AREA)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    v = float(np.median(blurred))
    edges = cv2.Canny(blurred, int(max(0, 0.66 * v)), int(min(255, 1.33 * v)))
    edges = cv2.dilate(edges, np.ones((3, 3), dtype=np.uint8), iterations=1)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    img_area = float(img.shape[0] * img.shape[1])

    best: Optional[tuple[float, np.ndarray]] = None
    for c in sorted(contours, key=cv2.contourArea, reverse=True)[:8]:
        peri = cv2.arcLength(c, True)
        approx = cv2.approxPolyDP(c, 0.02 * peri, True)
        scored = _score_quadrilateral(approx, img_area)
        if scored is None:
            continue
        if best is None or scored[0] > best[0]:
            best = scored

    if best is None:
        return {"found": False, "reason": "no_quadrilateral_found"}

    score, rect = best
    # Quantize corners to 10px grid so small camera jitter still counts as "stable"
    quantized = [(int(round(x / 10)) * 10, int(round(y / 10)) * 10) for x, y in rect.tolist()]
    stability_key = "|".join(f"{x}:{y}" for x, y in quantized)
    stability_hash = hashlib.sha1(stability_key.encode("ascii")).hexdigest()[:12]
    xs = [p[0] for p in rect.tolist()]
    ys = [p[1] for p in rect.tolist()]
    return {
        "found": True,
        "score": round(float(score), 4),
        "box": [round(min(xs), 1), round(min(ys), 1), round(max(xs) - min(xs), 1), round(max(ys) - min(ys), 1)],
        "corners": rect.tolist(),
        "stability_hash": stability_hash,
        "reason": "ok",
    }
