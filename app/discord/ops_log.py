import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import func
from sqlmodel import Session, select

from ..models import OperationsLog


REDACTION_MARKER = "[REDACTED]"

# Keys whose values are treated as credentials and replaced with [REDACTED].
# Matched case-insensitively as a substring against the key name so things
# like "PasswordHint", "access_token", and "X-API-Key" are all caught.
_SENSITIVE_KEY_FRAGMENTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "auth_header",
    "signature",
    "received_sig",
    "candidate_digest",
    "candidate_digests",
    "tiktok_signature",
    "tt_signature",
    "x_signature",
    "session_id",
    "cookie",
    "private_key",
    "client_secret",
    "credential",
    "shop_cipher",
)

# Bearer / token-shaped substrings sometimes leak into free-text fields.
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-_\.=]+")
_SECRET_TOKEN_RE = re.compile(
    r"(?i)\b(?:tok|rt|sk|pk|ghp|gho|ghs|github_pat|xox[baprs])[-_A-Za-z0-9]{10,}\b"
)
_SENSITIVE_TEXT_RE = re.compile(
    r"(?i)([\"']?\b(?:access[_-]?token|refresh[_-]?token|auth[_-]?code|app[_-]?secret|"
    r"client[_-]?secret|api[_-]?key|shop[_-]?cipher|x-tts-access-token|authorization|"
    r"(?:x[_-]?)?(?:tik[_-]?tok|tt)[_-]?signature|x[_-]?signature)\b[\"']?\s*[:=]\s*[\"']?)"
    r"([^\s&\"'}]+)([\"']?)"
)
_SENSITIVE_HEADER_TEXT_RE = re.compile(
    r"(?i)(\b(?:authorization|(?:x[-_])?(?:tik[-_]?tok|tt)[-_]?signature|x[-_]?signature)\b\s*:\s*)"
    r"([^\s\"'}]+)"
)


def _key_is_sensitive(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    lowered = key.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    compact = normalized.replace("_", "")
    if normalized.endswith("_at") or normalized.endswith("_expires_at"):
        return False
    return any(
        fragment in lowered or fragment in normalized or fragment.replace("_", "") in compact
        for fragment in _SENSITIVE_KEY_FRAGMENTS
    )


def _redact_text(value: str) -> str:
    value = _BEARER_RE.sub(f"Bearer {REDACTION_MARKER}", value)
    value = _SENSITIVE_HEADER_TEXT_RE.sub(lambda match: f"{match.group(1)}{REDACTION_MARKER}", value)
    value = _SENSITIVE_TEXT_RE.sub(lambda match: f"{match.group(1)}{REDACTION_MARKER}{match.group(3)}", value)
    return _SECRET_TOKEN_RE.sub(REDACTION_MARKER, value)


def redact_log_details(details: Any, *, _top_level: bool = True) -> Any:
    """Return a deep copy of ``details`` with sensitive values replaced.

    Recursively walks dicts/lists. Keys matching known sensitive fragments
    (token, password, secret, api_key, authorization, ...) have their
    values replaced with ``[REDACTED]``. String values are also scanned
    for inline Bearer tokens which are masked in-place.
    """
    if details is None:
        return {} if _top_level else None
    if isinstance(details, dict):
        cleaned: dict[Any, Any] = {}
        for key, value in details.items():
            if _key_is_sensitive(key):
                # Sensitive subtree — replace the entire value. Nested keys like
                # {"credentials": {"value": "..."}} must not leak just because
                # child names are generic.
                cleaned[key] = REDACTION_MARKER
            else:
                # Non-sensitive key: recurse so nested sensitive children still
                # get redacted while keeping diagnostic shape useful.
                cleaned[key] = redact_log_details(value, _top_level=False)
        return cleaned
    if isinstance(details, list):
        return [redact_log_details(item, _top_level=False) for item in details]
    if isinstance(details, tuple):
        return tuple(redact_log_details(item, _top_level=False) for item in details)
    if isinstance(details, str):
        return _redact_text(details)
    return details


def write_operations_log(
    session: Session,
    *,
    event_type: str,
    message: str,
    level: str = "info",
    source: str = "system",
    details: Optional[dict] = None,
) -> OperationsLog:
    safe_details = redact_log_details(details or {})
    row = OperationsLog(
        event_type=event_type,
        level=level,
        source=source,
        message=message,
        details_json=json.dumps(safe_details),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def list_operations_logs(
    session: Session,
    *,
    limit: int = 200,
    event_type_prefix: Optional[str] = None,
    level: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list[OperationsLog]:
    stmt = select(OperationsLog)
    if event_type_prefix:
        stmt = stmt.where(OperationsLog.event_type.startswith(event_type_prefix))
    if level:
        stmt = stmt.where(OperationsLog.level == level)
    if since:
        stmt = stmt.where(OperationsLog.created_at >= since)
    if until:
        stmt = stmt.where(OperationsLog.created_at <= until)
    return session.exec(
        stmt.order_by(OperationsLog.created_at.desc(), OperationsLog.id.desc())
        .limit(limit)
    ).all()


def count_recent_errors(session: Session, *, since_minutes: int = 60) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
    stmt = (
        select(func.count())
        .select_from(OperationsLog)
        .where(OperationsLog.level == "error")
        .where(OperationsLog.created_at >= cutoff)
    )
    return int(session.exec(stmt).one())


def parse_operations_log_details(row: OperationsLog) -> dict:
    try:
        raw = json.loads(row.details_json or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    return redact_log_details(raw)


def list_operations_logs_for_backfill_request(
    session: Session,
    *,
    request_id: int,
    limit: int = 300,
) -> list[OperationsLog]:
    rows = list_operations_logs(session, limit=max(limit * 4, 500))
    filtered: list[OperationsLog] = []
    for row in rows:
        details = parse_operations_log_details(row)
        if details.get("request_id") == request_id:
            filtered.append(row)
        if len(filtered) >= limit:
            break
    return filtered
