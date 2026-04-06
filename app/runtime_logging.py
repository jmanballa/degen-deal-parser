import atexit
import json
import os
import sys
from pathlib import Path
from typing import TextIO

from .config import BASE_DIR, get_settings
from .models import utcnow

LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5


class RotatingStream:
    """File-like stream that rotates when exceeding max_bytes."""

    def __init__(self, path: Path, max_bytes: int = LOG_MAX_BYTES, backup_count: int = LOG_BACKUP_COUNT):
        self._path = path
        self._max_bytes = max_bytes
        self._backup_count = backup_count
        self._handle: TextIO = open(path, "a", encoding="utf-8", buffering=1)
        self.encoding = "utf-8"

    def write(self, data: str) -> int:
        if self._handle.closed:
            return 0
        try:
            self._handle.write(data)
            if self._path.exists() and self._path.stat().st_size >= self._max_bytes:
                self._rotate()
        except (OSError, ValueError):
            pass
        return len(data)

    def flush(self) -> None:
        try:
            if not self._handle.closed:
                self._handle.flush()
        except (OSError, ValueError):
            pass

    def close(self) -> None:
        try:
            self._handle.close()
        except (OSError, ValueError):
            pass

    @property
    def closed(self) -> bool:
        return self._handle.closed

    def _rotate(self) -> None:
        self._handle.close()
        for i in range(self._backup_count - 1, 0, -1):
            src = self._path.with_suffix(f".log.{i}")
            dst = self._path.with_suffix(f".log.{i + 1}")
            if src.exists():
                try:
                    dst.unlink(missing_ok=True)
                    src.rename(dst)
                except OSError:
                    pass
        first_backup = self._path.with_suffix(f".log.1")
        try:
            first_backup.unlink(missing_ok=True)
            self._path.rename(first_backup)
        except OSError:
            pass
        self._handle = open(self._path, "a", encoding="utf-8", buffering=1)


class TeeStream:
    def __init__(self, primary: TextIO, mirror: TextIO):
        self._primary = primary
        self._mirror = mirror
        self.encoding = getattr(primary, "encoding", "utf-8")

    def write(self, data: str) -> int:
        self._primary.write(data)
        self._mirror.write(data)
        return len(data)

    def flush(self) -> None:
        try:
            self._primary.flush()
        except (OSError, ValueError):
            pass
        if getattr(self._mirror, "closed", False):
            return
        try:
            self._mirror.flush()
        except (OSError, ValueError):
            pass

    def isatty(self) -> bool:
        return bool(getattr(self._primary, "isatty", lambda: False)())

    def fileno(self) -> int:
        return self._primary.fileno()


def _close_runtime_log_handle(handle: TextIO) -> None:
    try:
        handle.flush()
    except (OSError, ValueError):
        pass
    try:
        handle.close()
    except (OSError, ValueError):
        pass


def setup_runtime_file_logging(default_name: str = "app.log") -> Path | None:
    settings = get_settings()
    if not settings.log_to_file:
        return None

    resolved_dir = Path(settings.log_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = BASE_DIR / resolved_dir
    resolved_dir.mkdir(parents=True, exist_ok=True)

    log_path = resolved_dir / default_name
    sentinel = f"_degen_log_path_{default_name.replace('.', '_')}"
    if getattr(sys, sentinel, None) == str(log_path):
        return log_path

    handle = RotatingStream(log_path)
    setattr(sys, sentinel, str(log_path))

    if not getattr(sys.stdout, "_degen_tee_wrapped", False):
        sys.stdout = TeeStream(sys.stdout, handle)
        setattr(sys.stdout, "_degen_tee_wrapped", True)

    if not getattr(sys.stderr, "_degen_tee_wrapped", False):
        sys.stderr = TeeStream(sys.stderr, handle)
        setattr(sys.stderr, "_degen_tee_wrapped", True)

    atexit.register(_close_runtime_log_handle, handle)
    print(f"[logging] writing runtime output to {os.path.normpath(str(log_path))}")
    return log_path


def resolve_runtime_log_path(default_name: str) -> Path:
    settings = get_settings()
    resolved_dir = Path(settings.log_dir)
    if not resolved_dir.is_absolute():
        resolved_dir = BASE_DIR / resolved_dir
    return resolved_dir / default_name


def structured_log_line(
    *,
    runtime: str,
    action: str,
    success: bool | None = None,
    error: str | None = None,
    **details,
) -> str:
    payload = {
        "timestamp": utcnow().isoformat(),
        "runtime": runtime,
        "action": action,
        "success": success,
        "error": error,
    }
    payload.update(details)
    return json.dumps(payload, default=str, sort_keys=True)
