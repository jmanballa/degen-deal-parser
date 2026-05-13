from __future__ import annotations

import base64
import hashlib
from typing import List, Optional

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from ..config import get_settings

_settings = get_settings()
_multi_fernet: Optional[MultiFernet] = None


class PIIDecryptError(ValueError):
    """Raised when ciphertext cannot be decrypted with any configured key."""


def _validate_and_build(keys_raw: str) -> MultiFernet:
    raw_keys = [k.strip() for k in (keys_raw or "").split(",") if k.strip()]
    if not raw_keys:
        raise SystemExit(
            "[pii] EMPLOYEE_PORTAL_ENABLED=true but EMPLOYEE_PII_KEY is missing — fail-closed."
        )
    fernets: List[Fernet] = []
    for key in raw_keys:
        try:
            decoded = base64.urlsafe_b64decode(key.encode("ascii"))
        except Exception as exc:
            raise SystemExit(f"[pii] EMPLOYEE_PII_KEY is not valid urlsafe base64: {exc}")
        if len(decoded) != 32:
            raise SystemExit(
                "[pii] EMPLOYEE_PII_KEY must decode to exactly 32 bytes (raw AES-128 + HMAC-SHA256)."
            )
        fernets.append(Fernet(key.encode("ascii")))
    return MultiFernet(fernets)


def _init_if_needed() -> None:
    global _multi_fernet
    if _multi_fernet is not None:
        return
    if not _settings.employee_portal_enabled:
        return
    if not (_settings.employee_email_hash_salt or "").strip():
        raise SystemExit(
            "[pii] EMPLOYEE_PORTAL_ENABLED=true but EMPLOYEE_EMAIL_HASH_SALT is missing — fail-closed."
        )
    _multi_fernet = _validate_and_build(_settings.employee_pii_key)


# Eagerly validate at import time if enabled — fail-closed on boot.
if _settings.employee_portal_enabled:
    try:
        _init_if_needed()
    except SystemExit:
        # Let it propagate so the process exits.
        raise


def _fernet() -> MultiFernet:
    _init_if_needed()
    if _multi_fernet is None:
        raise RuntimeError(
            "PII helpers invoked with EMPLOYEE_PORTAL_ENABLED=false; enable the portal or guard callers."
        )
    return _multi_fernet


def encrypt_pii(plaintext: Optional[str]) -> Optional[bytes]:
    if plaintext is None:
        return None
    return _fernet().encrypt(plaintext.encode("utf-8"))


def decrypt_pii(blob: Optional[bytes]) -> Optional[str]:
    if blob is None:
        return None
    try:
        return _fernet().decrypt(bytes(blob)).decode("utf-8")
    except InvalidToken as exc:
        raise PIIDecryptError("PII ciphertext failed to decrypt with any configured key") from exc


def email_lookup_hash(email: str) -> str:
    salt = _settings.employee_email_hash_salt or ""
    if not salt:
        # Validated at module init when the portal is enabled; this is a
        # defensive fallback for mis-configured callers with portal disabled.
        raise ValueError("EMPLOYEE_EMAIL_HASH_SALT is not configured")
    normalized = (email or "").strip().lower()
    return hashlib.sha256((salt + normalized).encode("utf-8")).hexdigest()
