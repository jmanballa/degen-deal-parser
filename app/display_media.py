from __future__ import annotations

import base64
import io
import json
import logging
from typing import Iterable

from sqlalchemy import select
from sqlmodel import Session

from .models import AttachmentAsset, DiscordMessage

logger = logging.getLogger(__name__)


IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"]

# Bedrock-hosted Anthropic models reject any single image source whose
# BASE64-encoded payload exceeds 5 MiB (5,242,880 bytes). Base64 expands
# bytes by 4/3, so raw image bytes must stay under 3,932,160 for the
# encoded form to fit. We target 3.6 MB of raw bytes so the base64
# output is ~4.8 MB, comfortably under the cap and leaving room for the
# "data:<mime>;base64," MIME framing prefix.
VISION_IMAGE_MAX_BYTES = 3_600_000


def sniff_image_mime_type(data: bytes) -> str | None:
    """Return the true MIME type of an image by inspecting its magic bytes.

    Bedrock validates that the declared MIME type matches the actual
    bytes: "The image was specified using the image/jpeg media type,
    but the image appears to be a image/png image" -> HTTP 400. The
    content_type stored on AttachmentAsset comes from Discord CDN
    headers or a filename-based guess, which is sometimes wrong (e.g.
    a PNG renamed with a .jpg extension).

    Returns None if the bytes don't match any recognized image header.
    """
    if not data or len(data) < 12:
        return None
    head = data[:16]
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if head.startswith(b"GIF87a") or head.startswith(b"GIF89a"):
        return "image/gif"
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return "image/webp"
    if head.startswith(b"BM"):
        return "image/bmp"
    return None


def stable_attachment_url_key(url: str) -> str:
    """Strip volatile query-string signing so cached Discord URLs still match.

    Discord CDN URLs contain ephemeral ``?ex=...&is=...&hm=...`` signatures
    that Discord rotates periodically. The part before ``?`` (host + path +
    attachment id + filename) stays stable, so we key any cache lookup by
    that to survive URL re-signing.
    """
    return (url or "").split("?", 1)[0].strip()


def shrink_image_to_limit(
    data: bytes,
    content_type: str,
    max_bytes: int = VISION_IMAGE_MAX_BYTES,
) -> tuple[bytes, str] | None:
    """Re-encode an oversized image so it fits under ``max_bytes``.

    Tries decreasing JPEG quality first (preserves detail, cheap), then
    downscales the image in steps. Returns ``(bytes, content_type)`` on
    success, or ``None`` if the image could not be compressed (or Pillow
    isn't available). The caller can then skip the image rather than
    fail the whole model call.

    Images already under ``max_bytes`` are returned as-is so small PNGs
    don't get needlessly re-encoded as JPEG.
    """
    if not data:
        return None
    if len(data) <= max_bytes:
        return data, content_type

    try:
        from PIL import Image
    except ImportError:
        logger.warning(
            "display_media: image is %d bytes (> %d) and Pillow is not "
            "installed; cannot downscale",
            len(data),
            max_bytes,
        )
        return None

    try:
        img = Image.open(io.BytesIO(data))
        # JPEG encoder cannot handle RGBA / palette modes
        if img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")

        for quality in (85, 75, 65, 55):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            if buf.tell() <= max_bytes:
                return buf.getvalue(), "image/jpeg"

        for scale in (0.75, 0.60, 0.45, 0.30):
            width, height = img.size
            resized = img.resize(
                (max(1, int(width * scale)), max(1, int(height * scale))),
                Image.LANCZOS,
            )
            buf = io.BytesIO()
            resized.save(buf, format="JPEG", quality=70, optimize=True)
            if buf.tell() <= max_bytes:
                return buf.getvalue(), "image/jpeg"

        logger.warning(
            "display_media: could not shrink image below %d bytes "
            "even after aggressive resize; skipping",
            max_bytes,
        )
        return None
    except Exception as exc:
        logger.warning("display_media: image compression failed: %s", exc)
        return None


def encode_bytes_as_vision_data_url(
    data: bytes,
    content_type: str,
    max_bytes: int = VISION_IMAGE_MAX_BYTES,
) -> str | None:
    """Return a ``data:<mime>;base64,<payload>`` URL suitable for vision APIs.

    Automatically shrinks the input so the payload fits under ``max_bytes``
    before encoding. The MIME type in the returned data URL is derived
    from the actual bytes (magic-byte sniff), not the caller-supplied
    ``content_type``, because Bedrock validates the declared MIME
    against the bytes and a mismatch returns HTTP 400.

    Returns ``None`` if the image is missing, cannot be compressed, or
    encoding fails.
    """
    shrunk = shrink_image_to_limit(data, content_type, max_bytes=max_bytes)
    if shrunk is None:
        return None
    image_bytes, resolved_content_type = shrunk

    # If we passed through the original bytes without re-encoding, the
    # caller-supplied content_type may be wrong. Always trust the magic
    # bytes when we can read them.
    sniffed = sniff_image_mime_type(image_bytes)
    if sniffed:
        resolved_content_type = sniffed

    try:
        encoded = base64.b64encode(image_bytes).decode("ascii")
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("display_media: base64 encode failed: %s", exc)
        return None
    mime = (resolved_content_type or "").strip() or "image/jpeg"
    return f"data:{mime};base64,{encoded}"


def encode_attachment_asset_as_vision_data_url(
    asset: AttachmentAsset,
    max_bytes: int = VISION_IMAGE_MAX_BYTES,
) -> str | None:
    """Convenience wrapper: take an AttachmentAsset and return a vision data URL."""
    if not asset or not asset.data:
        return None
    content_type = (asset.content_type or "").strip() or "image/jpeg"
    return encode_bytes_as_vision_data_url(asset.data, content_type, max_bytes=max_bytes)


def parse_attachment_urls_json(value: str | None) -> list[str]:
    try:
        loaded = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    cleaned: list[str] = []
    for entry in loaded:
        if isinstance(entry, str) and entry.strip():
            cleaned.append(entry.strip())
    return cleaned


def extract_image_urls(attachment_urls: list[str]) -> list[str]:
    return [
        url for url in attachment_urls
        if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
    ]


def get_cached_attachment_map(session: Session, message_ids: list[int]) -> dict[int, dict[str, list[str]]]:
    valid_ids = [message_id for message_id in message_ids if message_id is not None]
    if not valid_ids:
        return {}

    assets = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.message_id, AttachmentAsset.is_image)
        .where(AttachmentAsset.message_id.in_(valid_ids))
        .order_by(AttachmentAsset.message_id.asc(), AttachmentAsset.id.asc())
    ).all()

    results: dict[int, dict[str, list[str]]] = {}
    for asset_id, message_id, is_image in assets:
        if asset_id is None:
            continue
        bucket = results.setdefault(
            message_id,
            {"all_urls": [], "image_urls": []},
        )
        asset_url = f"/attachments/{asset_id}"
        bucket["all_urls"].append(asset_url)
        if is_image:
            bucket["image_urls"].append(asset_url)

    return results


def normalize_attachment_urls_for_row(
    row: DiscordMessage,
    cached_assets: dict[str, list[str]] | None = None,
) -> tuple[list[str], list[str]]:
    if cached_assets:
        return list(cached_assets["all_urls"]), list(cached_assets["image_urls"])

    attachment_urls = parse_attachment_urls_json(row.attachment_urls_json)
    if row.id is None:
        return attachment_urls, extract_image_urls(attachment_urls)

    proxy_urls = [
        f"/messages/{row.id}/attachments/{index}"
        for index, _url in enumerate(attachment_urls)
    ]
    image_proxy_urls = [
        proxy_urls[index]
        for index, url in enumerate(attachment_urls)
        if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
    ]
    return proxy_urls, image_proxy_urls


def row_has_images(
    row: DiscordMessage,
    *,
    cached_assets: dict[str, list[str]] | None = None,
) -> bool:
    _, image_urls = normalize_attachment_urls_for_row(row, cached_assets)
    return bool(image_urls)


def merge_display_attachment_urls(
    *attachment_groups: Iterable[str] | None,
    image_groups: Iterable[Iterable[str] | None] | None = None,
) -> tuple[list[str], list[str]]:
    merged_urls: list[str] = []
    seen: set[str] = set()

    for attachment_group in attachment_groups:
        if not attachment_group:
            continue
        for url in attachment_group:
            if not url or url in seen:
                continue
            seen.add(url)
            merged_urls.append(url)

    if image_groups is None:
        return merged_urls, extract_image_urls(merged_urls)

    merged_image_urls: list[str] = []
    seen_images: set[str] = set()
    for image_group in image_groups:
        if not image_group:
            continue
        for url in image_group:
            if not url or url in seen_images:
                continue
            seen_images.add(url)
            merged_image_urls.append(url)

    return merged_urls, merged_image_urls
