import io
import logging
import mimetypes
import re
from pathlib import Path
from typing import Optional

from .config import BASE_DIR

logger = logging.getLogger(__name__)

ATTACHMENT_CACHE_DIR = BASE_DIR / "data" / "attachments"
THUMBNAIL_CACHE_DIR = BASE_DIR / "data" / "attachments" / "thumbs"
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
THUMB_MAX_SIZE = (240, 240)


def ensure_attachment_cache_dir() -> Path:
    ATTACHMENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return ATTACHMENT_CACHE_DIR


def guess_attachment_suffix(filename: Optional[str], content_type: Optional[str]) -> str:
    if filename:
        suffix = Path(filename).suffix.strip()
        if suffix:
            return suffix
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if guessed:
            return guessed
    return ".bin"


def attachment_cache_path(
    asset_id: int,
    *,
    filename: Optional[str],
    content_type: Optional[str],
) -> Path:
    suffix = guess_attachment_suffix(filename, content_type)
    safe_stem = SAFE_FILENAME_RE.sub("-", Path(filename or "attachment").stem).strip("-") or "attachment"
    return ensure_attachment_cache_dir() / f"{asset_id}-{safe_stem}{suffix}"


def write_attachment_cache_file(
    asset_id: int,
    *,
    filename: Optional[str],
    content_type: Optional[str],
    data: bytes,
) -> Path:
    path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
    if not path.exists():
        path.write_bytes(data)
    return path


def delete_attachment_cache_file(
    asset_id: int,
    *,
    filename: Optional[str],
    content_type: Optional[str],
) -> None:
    path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
    if path.exists():
        path.unlink()
    thumb = thumbnail_cache_path(asset_id)
    if thumb.exists():
        thumb.unlink()


def ensure_thumbnail_cache_dir() -> Path:
    THUMBNAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return THUMBNAIL_CACHE_DIR


def thumbnail_cache_path(asset_id: int) -> Path:
    return ensure_thumbnail_cache_dir() / f"{asset_id}.jpg"


def warm_attachment_cache(session) -> tuple[int, int]:
    """Extract attachment blobs from DB to disk cache. Returns (extracted, already_cached)."""
    from sqlalchemy import select as sa_select
    from .models import AttachmentAsset

    already_cached = 0
    extracted = 0
    offset = 0
    batch_size = 100

    while True:
        rows = session.exec(
            sa_select(
                AttachmentAsset.id,
                AttachmentAsset.filename,
                AttachmentAsset.content_type,
                AttachmentAsset.is_image,
            )
            .order_by(AttachmentAsset.id.asc())
            .offset(offset)
            .limit(batch_size)
        ).all()
        if not rows:
            break

        needs_extract: list[tuple[int, str | None, str | None, bool]] = []
        for asset_id, filename, content_type, is_image in rows:
            if asset_id is None:
                continue
            path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
            if path.exists():
                already_cached += 1
                if is_image:
                    generate_thumbnail(path, asset_id)
            else:
                needs_extract.append((asset_id, filename, content_type, is_image))

        for asset_id, filename, content_type, is_image in needs_extract:
            asset = session.get(AttachmentAsset, asset_id)
            if asset and asset.data:
                file_path = write_attachment_cache_file(
                    asset_id, filename=asset.filename,
                    content_type=asset.content_type, data=asset.data,
                )
                extracted += 1
                if is_image:
                    generate_thumbnail(file_path, asset_id)

        offset += batch_size

    return extracted, already_cached


def generate_thumbnail(source_path: Path, asset_id: int) -> Optional[Path]:
    try:
        from PIL import Image
    except ImportError:
        return None

    thumb_path = thumbnail_cache_path(asset_id)
    if thumb_path.exists():
        return thumb_path

    try:
        with Image.open(source_path) as img:
            img.thumbnail(THUMB_MAX_SIZE, Image.LANCZOS)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=80, optimize=True)
            thumb_path.write_bytes(buf.getvalue())
        return thumb_path
    except Exception:
        logger.debug("thumbnail generation failed for asset %s", asset_id, exc_info=True)
        return None
