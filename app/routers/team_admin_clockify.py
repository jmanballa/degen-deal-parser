"""/team/admin/clockify - Clockify setup and employee mapping tools."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import or_
from sqlmodel import Session, select

from ..clockify import (
    ClockifyClient,
    ClockifyApiError,
    ClockifyConfigError,
    ClockifyWeekSummary,
    build_week_summary,
    clockify_week_bounds,
    clockify_client_from_settings,
    clockify_is_configured,
    format_hours,
    parse_clockify_datetime,
    parse_iso_duration_seconds,
)
from ..auth import is_draft_user
from ..config import get_settings
from ..csrf import issue_token, require_csrf
from ..db import get_session
from ..models import (
    AuditLog,
    ClockifyTimeEntry,
    ClockifyWebhookEvent,
    EmployeeProfile,
    User,
    utcnow,
)
from ..pii import decrypt_pii
from ..shared import templates
from .team_admin import _admin_gate, _permission_gate
from .team_admin_employees import (
    COMPENSATION_TYPE_HOURLY,
    COMPENSATION_TYPE_LABELS,
    COMPENSATION_TYPE_MONTHLY,
    COMPENSATION_TYPE_UNPAID,
    _decrypt_hourly_rate_cents,
    _decrypt_monthly_salary_cents,
    _normalize_compensation_type,
    _salary_cost_for_period,
)

router = APIRouter()


CLOCKIFY_NAME_OVERRIDES = {
    # Store nickname differences that are safe enough to auto-link.
    "alex": ("mod alex",),
    "dat david": ("david",),
}
_CLOCKIFY_WEEK_CACHE: dict[tuple[str, date], tuple[float, ClockifyWeekSummary]] = {}
_CLOCKIFY_WEEK_CACHE_TTL_SECONDS = 60.0
_BREAK_KEYWORDS = ("break", "lunch", "meal", "rest")
_LABOR_STATS_DEFAULT_RANGE = "this_week"
_LABOR_STATS_MAX_DAYS = 366
_LABOR_STATS_PRESETS = (
    {"key": "today", "label": "Today"},
    {"key": "yesterday", "label": "Yesterday"},
    {"key": "this_week", "label": "This week"},
    {"key": "last_week", "label": "Last week"},
    {"key": "last_7", "label": "Last 7 days"},
    {"key": "this_month", "label": "This month"},
    {"key": "last_month", "label": "Last month"},
    {"key": "last_30", "label": "Last 30 days"},
    {"key": "custom", "label": "Custom"},
)


def _mask_id(value: str) -> str:
    value = (value or "").strip()
    if len(value) <= 8:
        return value or "-"
    return f"{value[:4]}...{value[-4:]}"


def _clockify_user_id(row: dict[str, Any]) -> str:
    return str(row.get("id") or "").strip()


def _clockify_user_name(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    if name:
        return name
    email = str(row.get("email") or "").strip()
    if email:
        return email
    return _mask_id(_clockify_user_id(row))


def _clockify_user_email(row: dict[str, Any]) -> str:
    return str(row.get("email") or "").strip()


def _mask_email(email: str) -> str:
    email = (email or "").strip()
    if "@" not in email:
        return f"{email[:3]}***" if email else ""
    local, domain = email.split("@", 1)
    return f"{local[:3]}***@{domain}" if domain else f"{local[:3]}***"


def _clockify_display_name(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    if name:
        return _mask_email(name) if "@" in name else name
    email = _clockify_user_email(row)
    if email:
        return _mask_email(email)
    return _mask_id(_clockify_user_id(row))


def _masked_clockify_user(row: dict[str, Any]) -> dict[str, Any]:
    masked = dict(row)
    masked["email"] = _mask_email(_clockify_user_email(row))
    name = str(masked.get("name") or "").strip()
    if "@" in name:
        masked["name"] = _mask_email(name)
    return masked


def _is_matchable_team_user(user: Optional[User]) -> bool:
    return bool(user and (user.is_active or is_draft_user(user)))


def _normalize_match_name(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def _clockify_match_keys(row: dict[str, Any]) -> list[str]:
    raw_name = _clockify_user_name(row)
    full_key = _normalize_match_name(raw_name)
    keys: list[str] = []
    overrides = CLOCKIFY_NAME_OVERRIDES.get(full_key, ())
    for override in overrides:
        key = _normalize_match_name(override)
        if key and key not in keys:
            keys.append(key)
    if overrides:
        return keys
    for inner in re.findall(r"\(([^)]+)\)", raw_name):
        key = _normalize_match_name(inner)
        if key and key not in keys:
            keys.append(key)
    if full_key and full_key not in keys:
        keys.append(full_key)
    return keys


def _safe_decrypt_name(profile: Optional[EmployeeProfile]) -> str:
    if profile is None or not profile.legal_name_enc:
        return ""
    try:
        return (decrypt_pii(profile.legal_name_enc) or "").strip()
    except ValueError:
        return ""


def _employee_match_keys(user: User, profile: Optional[EmployeeProfile]) -> list[str]:
    values = [
        user.display_name,
        _safe_decrypt_name(profile),
    ]
    if not is_draft_user(user):
        values.append(user.username)
    keys: list[str] = []
    for value in values:
        key = _normalize_match_name(value or "")
        if key and key not in keys:
            keys.append(key)
    return keys


def _employee_clockify_counts(employee_rows: list[dict[str, Any]]) -> dict[str, int]:
    matchable_profiles = [row.get("profile") for row in employee_rows if row.get("profile")]
    mapped = sum(1 for profile in matchable_profiles if profile.clockify_user_id)
    with_email = sum(1 for profile in matchable_profiles if profile.email_ciphertext)
    return {
        "active_profiles": len(matchable_profiles),
        "mapped": mapped,
        "unmapped": max(0, len(matchable_profiles) - mapped),
        "with_email": with_email,
    }


def _employee_rows(
    session: Session,
    *,
    include_inactive: bool = False,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    stmt = (
        select(User, EmployeeProfile)
        .join(EmployeeProfile, EmployeeProfile.user_id == User.id, isouter=True)
        .order_by(User.display_name, User.username)
    )
    if not include_inactive:
        stmt = stmt.where((User.is_active == True) | (User.password_hash == ""))  # noqa: E712
    result = session.exec(stmt).all()
    for employee, profile in result:
        out.append(
            {
                "user": employee,
                "profile": profile,
                "clockify_user_id": (profile.clockify_user_id or "").strip()
                if profile
                else "",
                "is_draft": is_draft_user(employee),
            }
        )
    return out


def _cached_user_week_summary(
    client: ClockifyClient,
    clockify_user_id: str,
    *,
    today: date,
    settings=None,
) -> ClockifyWeekSummary:
    week_start_local, _week_end_local = clockify_week_bounds(today, settings=settings)
    key = (clockify_user_id, week_start_local.date())
    now = time.time()
    cached = _CLOCKIFY_WEEK_CACHE.get(key)
    if cached is not None:
        cached_at, summary = cached
        if now - cached_at < _CLOCKIFY_WEEK_CACHE_TTL_SECONDS:
            return summary
    summary = client.user_week_summary(
        clockify_user_id,
        today=today,
        settings=settings,
    )
    _CLOCKIFY_WEEK_CACHE[key] = (now, summary)
    return summary


def _employee_link_map(employee_rows: list[dict[str, Any]]) -> dict[str, User]:
    linked: dict[str, User] = {}
    for row in employee_rows:
        clockify_id = (row.get("clockify_user_id") or "").strip()
        user = row.get("user")
        if clockify_id and isinstance(user, User):
            linked[clockify_id] = user
    return linked


def _clockify_users_by_email(clockify_users: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_email: dict[str, dict[str, Any]] = {}
    for row in clockify_users:
        email = str(row.get("email") or "").strip().lower()
        user_id = str(row.get("id") or "").strip()
        if email and user_id:
            by_email.setdefault(email, row)
    return by_email


def _clockify_users_by_name(
    clockify_users: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    by_name: dict[str, dict[str, Any]] = {}
    ambiguous: set[str] = set()
    for row in clockify_users:
        if not _clockify_user_id(row):
            continue
        for key in _clockify_match_keys(row):
            if key in ambiguous:
                continue
            if key in by_name and _clockify_user_id(by_name[key]) != _clockify_user_id(row):
                by_name.pop(key, None)
                ambiguous.add(key)
                continue
            by_name[key] = row
    return by_name, ambiguous


def _find_clockify_name_match(
    user: User,
    profile: Optional[EmployeeProfile],
    by_name: dict[str, dict[str, Any]],
    ambiguous_names: set[str],
) -> tuple[Optional[dict[str, Any]], bool]:
    matches: dict[str, dict[str, Any]] = {}
    saw_ambiguous = False
    for key in _employee_match_keys(user, profile):
        if key in ambiguous_names:
            saw_ambiguous = True
            continue
        match = by_name.get(key)
        if match is not None:
            matches[_clockify_user_id(match)] = match
    if len(matches) == 1:
        return next(iter(matches.values())), False
    if len(matches) > 1:
        return None, True
    return None, saw_ambiguous


def build_clockify_roster_preview(
    clockify_users: list[dict[str, Any]],
    *,
    client: Optional[ClockifyClient] = None,
    settings=None,
    include_hours: bool = False,
    today: Optional[date] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Build admin-safe rows for Clockify people and optional hour previews."""
    rows: list[dict[str, Any]] = []
    today = today or date.today()
    for row in clockify_users[: max(0, limit)]:
        clockify_id = _clockify_user_id(row)
        preview = {
            "id": clockify_id,
            "id_masked": _mask_id(clockify_id),
            "name": _clockify_display_name(row),
            "email": _mask_email(_clockify_user_email(row)),
            "status": str(row.get("status") or "").strip() or "-",
            "raw": _masked_clockify_user(row),
            "has_data": False,
            "hours_label": "-",
            "entry_count": 0,
            "running_count": 0,
            "data_error": "",
        }
        if include_hours and client is not None and clockify_id:
            try:
                summary = _cached_user_week_summary(
                    client,
                    clockify_id,
                    today=today,
                    settings=settings,
                )
                preview["has_data"] = bool(summary.entries or summary.total_seconds)
                preview["hours_label"] = format_hours(summary.total_seconds)
                preview["entry_count"] = len(summary.entries)
                preview["running_count"] = summary.running_count
            except (ClockifyApiError, ClockifyConfigError) as exc:
                preview["data_error"] = str(exc)
        rows.append(preview)
    return rows


def _clockify_day_bounds(
    today: Optional[date] = None,
    *,
    settings=None,
) -> tuple[datetime, datetime]:
    day = today or date.today()
    week_start_local, _week_end_local = clockify_week_bounds(day, settings=settings)
    day_offset = (day - week_start_local.date()).days
    start_local = week_start_local + timedelta(days=day_offset)
    return start_local, start_local + timedelta(days=1)


def _format_clockify_time(value: Optional[datetime]) -> str:
    if value is None:
        return "-"
    return value.strftime("%I:%M %p").lstrip("0")


def _clockify_entry_search_text(entry: Any) -> str:
    pieces: list[str] = []
    if hasattr(entry, "description"):
        pieces.append(str(getattr(entry, "description") or ""))
    if hasattr(entry, "entry_type"):
        pieces.append(str(getattr(entry, "entry_type") or ""))
    if isinstance(entry, dict):
        for key in ("description", "projectName", "taskName", "type", "entry_type"):
            pieces.append(str(entry.get(key) or ""))
        for key in ("project", "task"):
            nested = entry.get(key)
            if isinstance(nested, dict):
                pieces.append(str(nested.get("name") or ""))
    return " ".join(piece for piece in pieces if piece).lower()


def _clockify_entry_is_break(entry: Any) -> bool:
    text = _clockify_entry_search_text(entry)
    return any(re.search(rf"\b{re.escape(keyword)}\b", text) for keyword in _BREAK_KEYWORDS)


def _json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return "{}"


def _json_loads_body(raw_body: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw_body.decode("utf-8") if raw_body else "{}")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Clockify webhook body must be JSON.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Clockify webhook body must be a JSON object.")
    return payload


def _clockify_webhook_secret() -> str:
    env_value = (os.getenv("CLOCKIFY_WEBHOOK_SECRET") or "").strip()
    if env_value:
        return env_value
    return _env_file_value("CLOCKIFY_WEBHOOK_SECRET")


def _split_secret_values(raw: str) -> list[str]:
    values: list[str] = []
    for value in re.split(r"[\s,;]+", raw or ""):
        value = value.strip().strip('"').strip("'")
        if value and value not in values:
            values.append(value)
    return values


def _clockify_webhook_signing_secrets() -> list[str]:
    values: list[str] = []
    for key_name in ("CLOCKIFY_WEBHOOK_SIGNING_SECRET", "CLOCKIFY_WEBHOOK_SIGNING_SECRETS"):
        for value in _split_secret_values(os.getenv(key_name) or ""):
            if value not in values:
                values.append(value)
        for value in _split_secret_values(_env_file_value(key_name)):
            if value not in values:
                values.append(value)
    return values


def _env_file_value(key_name: str) -> str:
    try:
        for line in Path(".env").read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() == key_name:
                return value.strip().strip('"').strip("'")
    except OSError:
        return ""
    return ""


def _strip_bearer(value: str) -> str:
    value = (value or "").strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


def _clockify_url_secret_candidates(request: Request, supplied_secret: str) -> list[str]:
    return [
        (supplied_secret or "").strip(),
        request.headers.get("X-Degen-Webhook-Secret", ""),
        request.headers.get("X-Webhook-Secret", ""),
    ]


def _clockify_signing_secret_candidates(request: Request) -> list[str]:
    raw_values = [
        request.headers.get("Authorization", ""),
        request.headers.get("X-Clockify-Webhook-Token", ""),
        request.headers.get("Clockify-Webhook-Token", ""),
        request.headers.get("X-Clockify-Webhook-Signature", ""),
        request.headers.get("Clockify-Webhook-Signature", ""),
        request.headers.get("X-Clockify-Signature", ""),
        request.headers.get("Clockify-Signature", ""),
        request.headers.get("X-Webhook-Signature", ""),
        request.headers.get("Webhook-Signature", ""),
        request.headers.get("X-Webhook-Token", ""),
        request.headers.get("Webhook-Token", ""),
        request.headers.get("X-Auth-Token", ""),
        request.headers.get("Auth-Token", ""),
        request.headers.get("authToken", ""),
    ]
    return [_strip_bearer(value) for value in raw_values if (value or "").strip()]


def _require_clockify_webhook_secret(request: Request, supplied_secret: str) -> None:
    expected_url_secret = _clockify_webhook_secret()
    expected_signing_secrets = _clockify_webhook_signing_secrets()
    if not expected_url_secret and not expected_signing_secrets:
        raise HTTPException(status_code=503, detail="Clockify webhook secret is not configured.")

    url_secret_ok = bool(
        expected_url_secret
        and any(
            hmac.compare_digest(expected_url_secret, (candidate or "").strip())
            for candidate in _clockify_url_secret_candidates(request, supplied_secret)
        )
    )
    signing_candidates = _clockify_signing_secret_candidates(request)
    signing_secret_ok = bool(
        expected_signing_secrets
        and any(
            hmac.compare_digest(expected, candidate)
            for expected in expected_signing_secrets
            for candidate in signing_candidates
        )
    )
    if not url_secret_ok and not signing_secret_ok:
        raise HTTPException(status_code=403, detail="Invalid Clockify webhook secret.")


def _first_string(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _nested_dict(value: Any, *keys: str) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current if isinstance(current, dict) else None


def _clockify_event_type(payload: dict[str, Any]) -> str:
    event_obj = payload.get("event")
    if isinstance(event_obj, dict):
        nested = _first_string(
            event_obj.get("type"),
            event_obj.get("eventType"),
            event_obj.get("name"),
            event_obj.get("action"),
        )
        if nested:
            return nested.upper()
    return _first_string(
        payload.get("eventType"),
        payload.get("event"),
        payload.get("type"),
        payload.get("action"),
        payload.get("name"),
    ).upper() or "UNKNOWN"


def _clockify_entry_payload(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    for keys in (
        ("timeEntry",),
        ("time_entry",),
        ("entry",),
        ("entity",),
        ("data", "timeEntry"),
        ("data", "time_entry"),
        ("data", "entry"),
        ("data", "entity"),
    ):
        nested = _nested_dict(payload, *keys)
        if nested:
            return nested
    if isinstance(payload.get("timeInterval"), dict) and payload.get("id"):
        return payload
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("timeInterval"), dict):
        return data
    return None


def _clockify_entry_id(payload: dict[str, Any], entry: Optional[dict[str, Any]] = None) -> str:
    entry = entry or {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    return _first_string(
        entry.get("id"),
        entry.get("timeEntryId"),
        payload.get("timeEntryId"),
        payload.get("time_entry_id"),
        payload.get("entityId"),
        data.get("timeEntryId") if isinstance(data, dict) else "",
        data.get("id") if isinstance(data, dict) else "",
    )


def _clockify_user_id_from_entry(payload: dict[str, Any], entry: Optional[dict[str, Any]] = None) -> str:
    entry = entry or {}
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    user_obj = entry.get("user") if isinstance(entry.get("user"), dict) else {}
    return _first_string(
        entry.get("userId"),
        entry.get("user_id"),
        user_obj.get("id") if isinstance(user_obj, dict) else "",
        payload.get("userId"),
        payload.get("user_id"),
        data.get("userId") if isinstance(data, dict) else "",
    )


def _clockify_workspace_id_from_payload(payload: dict[str, Any], settings=None) -> str:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    return _first_string(
        payload.get("workspaceId"),
        payload.get("workspace_id"),
        data.get("workspaceId") if isinstance(data, dict) else "",
        getattr(settings, "clockify_workspace_id", ""),
    )


def _clockify_event_id(payload: dict[str, Any]) -> str:
    event_obj = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    return _first_string(
        payload.get("eventId"),
        payload.get("event_id"),
        payload.get("webhookEventId"),
        payload.get("id") if not isinstance(payload.get("timeInterval"), dict) else "",
        event_obj.get("id") if isinstance(event_obj, dict) else "",
    )


def _is_delete_event(event_type: str) -> bool:
    normalized = (event_type or "").upper()
    return "DELETE" in normalized or "DELETED" in normalized or normalized.endswith("_REMOVED")


def _entry_interval_payload(entry: dict[str, Any]) -> dict[str, Any]:
    interval = entry.get("timeInterval") if isinstance(entry.get("timeInterval"), dict) else {}
    return interval if isinstance(interval, dict) else {}


def _entry_duration_from_payload(entry: dict[str, Any], start_at: Optional[datetime], end_at: Optional[datetime]) -> int:
    interval = _entry_interval_payload(entry)
    parsed = parse_iso_duration_seconds(interval.get("duration"))
    if parsed is not None:
        return max(0, parsed)
    if start_at and end_at:
        return max(0, int((end_at - start_at).total_seconds()))
    duration = entry.get("duration")
    if isinstance(duration, (int, float)):
        return max(0, int(duration))
    return 0


def _portal_user_id_for_clockify(session: Session, clockify_user_id: str) -> Optional[int]:
    clockify_user_id = (clockify_user_id or "").strip()
    if not clockify_user_id:
        return None
    profile = session.exec(
        select(EmployeeProfile).where(EmployeeProfile.clockify_user_id == clockify_user_id)
    ).first()
    return profile.user_id if profile is not None else None


def _upsert_clockify_time_entry_from_payload(
    session: Session,
    payload: dict[str, Any],
    *,
    source_event: str,
    settings=None,
    received_at: Optional[datetime] = None,
) -> Optional[ClockifyTimeEntry]:
    entry = _clockify_entry_payload(payload)
    entry_id = _clockify_entry_id(payload, entry)
    if not entry_id:
        return None
    received_at = received_at or utcnow()
    existing = session.exec(
        select(ClockifyTimeEntry).where(ClockifyTimeEntry.clockify_entry_id == entry_id)
    ).first()
    if _is_delete_event(source_event):
        if existing is None:
            existing = ClockifyTimeEntry(
                clockify_entry_id=entry_id,
                clockify_user_id=_clockify_user_id_from_entry(payload, entry),
                workspace_id=_clockify_workspace_id_from_payload(payload, settings),
                is_deleted=True,
                source_event=source_event,
                received_at=received_at,
                updated_at=received_at,
            )
        existing.is_deleted = True
        existing.is_running = False
        existing.source_event = source_event
        existing.raw_payload = _json_dumps_safe(payload)
        existing.updated_at = received_at
        session.add(existing)
        return existing
    if entry is None:
        return existing

    interval = _entry_interval_payload(entry)
    start_at = parse_clockify_datetime(interval.get("start") or entry.get("start"))
    end_at = parse_clockify_datetime(interval.get("end") or entry.get("end"))
    clockify_user_id = _clockify_user_id_from_entry(payload, entry)
    if existing is None:
        existing = ClockifyTimeEntry(
            clockify_entry_id=entry_id,
            clockify_user_id=clockify_user_id,
            received_at=received_at,
        )
    existing.clockify_user_id = clockify_user_id or existing.clockify_user_id
    existing.user_id = _portal_user_id_for_clockify(session, existing.clockify_user_id)
    existing.workspace_id = _clockify_workspace_id_from_payload(payload, settings)
    existing.description = str(entry.get("description") or "").strip()
    existing.project_id = _first_string(entry.get("projectId"), entry.get("project_id")) or None
    existing.task_id = _first_string(entry.get("taskId"), entry.get("task_id")) or None
    existing.entry_type = _first_string(entry.get("type"), entry.get("entry_type"), "REGULAR").upper()
    existing.start_at = start_at
    existing.end_at = end_at
    existing.duration_seconds = _entry_duration_from_payload(entry, start_at, end_at)
    existing.is_running = bool(start_at and end_at is None and not existing.is_deleted)
    existing.is_deleted = False
    existing.source_event = source_event
    existing.raw_payload = _json_dumps_safe(entry)
    existing.updated_at = received_at
    session.add(existing)
    return existing


def _clockify_time_entry_to_raw(row: ClockifyTimeEntry) -> dict[str, Any]:
    def iso(dt: Optional[datetime]) -> Optional[str]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "id": row.clockify_entry_id,
        "userId": row.clockify_user_id,
        "description": row.description,
        "projectId": row.project_id,
        "taskId": row.task_id,
        "type": row.entry_type,
        "timeInterval": {
            "start": iso(row.start_at),
            "end": iso(row.end_at),
        },
    }


def _cached_clockify_entries_by_user(
    session: Session,
    clockify_user_ids: list[str],
    *,
    start_local: datetime,
    end_local: datetime,
) -> dict[str, list[dict[str, Any]]]:
    if not clockify_user_ids:
        return {}
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    rows = session.exec(
        select(ClockifyTimeEntry).where(
            ClockifyTimeEntry.clockify_user_id.in_(clockify_user_ids),
            ClockifyTimeEntry.is_deleted == False,  # noqa: E712
            ClockifyTimeEntry.start_at < end_utc,
            or_(ClockifyTimeEntry.end_at == None, ClockifyTimeEntry.end_at > start_utc),  # noqa: E711
        )
    ).all()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.clockify_user_id, []).append(_clockify_time_entry_to_raw(row))
    return grouped


def process_clockify_webhook_payload(
    session: Session,
    payload: dict[str, Any],
    *,
    raw_body: bytes,
    settings=None,
) -> dict[str, Any]:
    received_at = utcnow()
    payload_hash = hashlib.sha256(raw_body).hexdigest()
    event_type = _clockify_event_type(payload)
    entry_payload = _clockify_entry_payload(payload)
    entry_id = _clockify_entry_id(payload, entry_payload) or None
    clockify_user_id = _clockify_user_id_from_entry(payload, entry_payload) or None
    event_id = _clockify_event_id(payload)
    dedupe_key = event_id or payload_hash

    existing_event = session.exec(
        select(ClockifyWebhookEvent).where(ClockifyWebhookEvent.dedupe_key == dedupe_key)
    ).first()
    if existing_event is not None and existing_event.processed:
        return {
            "ok": True,
            "duplicate": True,
            "event_type": existing_event.event_type,
            "entry_id": existing_event.clockify_entry_id,
        }

    webhook_event = existing_event or ClockifyWebhookEvent(
        dedupe_key=dedupe_key,
        received_at=received_at,
    )
    webhook_event.event_type = event_type
    webhook_event.clockify_entry_id = entry_id
    webhook_event.clockify_user_id = clockify_user_id
    webhook_event.payload_sha256 = payload_hash
    webhook_event.payload_json = _json_dumps_safe(payload)

    fetched_entry = False
    error = ""
    if entry_payload is None and entry_id and not _is_delete_event(event_type):
        try:
            if clockify_is_configured(settings):
                fetched = clockify_client_from_settings(settings).get_time_entry(entry_id)
                if fetched:
                    payload = dict(payload)
                    payload["timeEntry"] = fetched
                    entry_payload = fetched
                    clockify_user_id = _clockify_user_id_from_entry(payload, entry_payload) or clockify_user_id
                    fetched_entry = True
        except (ClockifyApiError, ClockifyConfigError) as exc:
            error = str(exc)

    cached_entry = _upsert_clockify_time_entry_from_payload(
        session,
        payload,
        source_event=event_type,
        settings=settings,
        received_at=received_at,
    )
    webhook_event.clockify_entry_id = (
        cached_entry.clockify_entry_id
        if cached_entry is not None
        else webhook_event.clockify_entry_id
    )
    webhook_event.clockify_user_id = (
        cached_entry.clockify_user_id
        if cached_entry is not None
        else clockify_user_id
    )
    webhook_event.processed = not bool(error)
    webhook_event.error = error or None
    webhook_event.processed_at = utcnow()
    session.add(webhook_event)
    session.commit()
    return {
        "ok": True,
        "duplicate": False,
        "event_type": event_type,
        "entry_id": webhook_event.clockify_entry_id,
        "clockify_user_id": webhook_event.clockify_user_id,
        "cached": cached_entry is not None,
        "fetched_entry": fetched_entry,
        "warning": error,
    }


def build_clockify_live_status(
    session: Session,
    client: Optional[ClockifyClient],
    *,
    settings=None,
    today: Optional[date] = None,
    now: Optional[datetime] = None,
    employee_rows: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Build current Clockify timer status for mapped portal employees."""
    day = today or date.today()
    start_local, end_local = _clockify_day_bounds(day, settings=settings)
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    rows: list[dict[str, Any]] = []
    employees = employee_rows if employee_rows is not None else _employee_rows(session)
    eligible_rows = [row for row in employees if row.get("profile")]
    mapped_rows = [
        row for row in eligible_rows if (row.get("clockify_user_id") or "").strip()
    ]
    mapped_clockify_ids = [
        (row.get("clockify_user_id") or "").strip()
        for row in mapped_rows
        if (row.get("clockify_user_id") or "").strip()
    ]
    cached_entries_by_user = _cached_clockify_entries_by_user(
        session,
        mapped_clockify_ids,
        start_local=start_local,
        end_local=end_local,
    )

    for row in mapped_rows:
        employee = row.get("user")
        clockify_user_id = (row.get("clockify_user_id") or "").strip()
        display_name = (
            getattr(employee, "display_name", None)
            or getattr(employee, "username", None)
            or "Employee"
        )
        base = {
            "employee": employee,
            "profile": row.get("profile"),
            "employee_name": display_name,
            "clockify_user_id": clockify_user_id,
            "clockify_user_id_masked": _mask_id(clockify_user_id),
            "status": "Not clocked in",
            "status_key": "not_clocked_in",
            "status_color": "var(--lx-muted)",
            "current_start_label": "-",
            "running_duration_label": "-",
            "today_total_label": "0m",
            "break_label": "No time today",
            "break_color": "var(--lx-muted)",
            "entry_count": 0,
            "error": "",
            "today_total_seconds": 0,
            "break_seconds": 0,
            "running_seconds": 0,
            "pay_type_label": "Not paid",
            "labor_cost_label": "$0.00",
            "labor_cost_cents": 0,
            "rank": 4,
        }
        try:
            raw_entries = cached_entries_by_user.get(clockify_user_id)
            if raw_entries is None:
                if client is None:
                    raw_entries = []
                else:
                    raw_entries = client.get_user_time_entries(
                        clockify_user_id,
                        start_utc=start_local.astimezone(timezone.utc),
                        end_utc=end_local.astimezone(timezone.utc),
                    )
            summary = build_week_summary(
                raw_entries,
                week_start_local=start_local,
                week_end_local=end_local,
                settings=settings,
                now=now_utc,
            )
        except (ClockifyApiError, ClockifyConfigError) as exc:
            base.update(
                {
                    "status": "Clockify error",
                    "status_key": "error",
                    "status_color": "#fca5a5",
                    "break_label": "-",
                    "error": str(exc),
                    "rank": 5,
                }
            )
            rows.append(base)
            continue

        running_entries = [entry for entry in summary.entries if entry.running]
        running_entry = running_entries[-1] if running_entries else None
        break_entries = [entry for entry in summary.entries if _clockify_entry_is_break(entry)]
        break_seconds = sum(entry.duration_seconds for entry in break_entries)
        work_seconds = sum(
            entry.duration_seconds
            for entry in summary.entries
            if not _clockify_entry_is_break(entry)
        )
        break_taken = any(entry.duration_seconds > 0 for entry in break_entries)
        running_is_break = bool(running_entry and _clockify_entry_is_break(running_entry))
        base["today_total_seconds"] = work_seconds
        base["break_seconds"] = break_seconds
        base["today_total_label"] = format_hours(work_seconds)
        base["entry_count"] = len(summary.entries)

        if running_entry is not None:
            base["running_seconds"] = running_entry.duration_seconds
            base["current_start_label"] = _format_clockify_time(running_entry.start_local)
            base["running_duration_label"] = format_hours(running_entry.duration_seconds)
            if running_is_break:
                base["status"] = "On break"
                base["status_key"] = "on_break"
                base["status_color"] = "#facc15"
                base["break_label"] = f"On break now ({format_hours(break_seconds)})"
                base["break_color"] = "#facc15"
                base["rank"] = 0
            else:
                base["status"] = "Clocked in"
                base["status_key"] = "clocked_in"
                base["status_color"] = "#86efac"
                base["break_label"] = (
                    f"Taken ({format_hours(break_seconds)})"
                    if break_taken
                    else "No break yet"
                )
                base["break_color"] = "#86efac" if break_taken else "var(--lx-muted)"
                base["rank"] = 1
        elif summary.total_seconds > 0:
            base["status"] = "Clocked out"
            base["status_key"] = "clocked_out"
            base["status_color"] = "var(--lx-text)"
            base["break_label"] = (
                f"Taken ({format_hours(break_seconds)})" if break_taken else "No break"
            )
            base["break_color"] = "#86efac" if break_taken else "var(--lx-muted)"
            base["rank"] = 3

        rows.append(base)

    rows.sort(key=lambda item: (item["rank"], str(item["employee_name"]).lower()))
    generated_at = now_utc.astimezone(start_local.tzinfo)
    timezone_name = str(getattr(start_local.tzinfo, "key", None) or start_local.tzinfo)
    return {
        "rows": rows,
        "mapped_count": len(mapped_rows),
        "unmapped_count": max(0, len(eligible_rows) - len(mapped_rows)),
        "timezone_name": timezone_name,
        "date_label": day.strftime("%b %d, %Y").replace(" 0", " "),
        "generated_at_label": _format_clockify_time(generated_at),
    }


def _format_money_label(cents: int) -> str:
    return f"${Decimal(cents) / Decimal(100):,.2f}"


def _cents_for_seconds(seconds: int, rate_cents: int) -> int:
    if seconds <= 0 or rate_cents <= 0:
        return 0
    amount = (Decimal(seconds) / Decimal(3600)) * Decimal(rate_cents)
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def build_shift_tracker_pay_summary(
    session: Session,
    live: dict[str, Any],
    *,
    employee_rows: Optional[list[dict[str, Any]]] = None,
    today: Optional[date] = None,
) -> dict[str, Any]:
    """Attach pay data to cached Clockify rows and build today's labor total."""
    day = today or date.today()
    employees = employee_rows if employee_rows is not None else _employee_rows(session)
    salary_today_cents = 0
    hourly_today_cents = 0
    salaried_count = 0
    hourly_count = 0
    unpaid_count = 0
    missing_salary_count = 0
    missing_hourly_rate_count = 0

    for employee_row in employees:
        user = employee_row.get("user")
        profile = employee_row.get("profile")
        if not isinstance(user, User) or not isinstance(profile, EmployeeProfile):
            continue
        compensation_type = _normalize_compensation_type(profile.compensation_type or "")
        if compensation_type == COMPENSATION_TYPE_UNPAID:
            unpaid_count += 1
            continue
        if compensation_type == COMPENSATION_TYPE_MONTHLY:
            salaried_count += 1
            salary_cents = _decrypt_monthly_salary_cents(profile)
            if salary_cents is None:
                missing_salary_count += 1
                continue
            salary_today_cents += _salary_cost_for_period(
                salary_cents=salary_cents,
                user=user,
                profile=profile,
                start_day=day,
                end_day=day,
            )
        elif compensation_type == COMPENSATION_TYPE_HOURLY:
            hourly_count += 1
            if _decrypt_hourly_rate_cents(profile) is None:
                missing_hourly_rate_count += 1

    clocked_in_count = 0
    on_break_count = 0
    total_seconds = 0
    for row in live.get("rows", []):
        profile = row.get("profile")
        compensation_type = _normalize_compensation_type(
            profile.compensation_type if isinstance(profile, EmployeeProfile) else ""
        )
        row["pay_type_label"] = COMPENSATION_TYPE_LABELS.get(
            compensation_type, "Hourly"
        )
        row["labor_cost_cents"] = 0
        row["labor_cost_label"] = "$0.00"
        if row.get("status_key") == "clocked_in":
            clocked_in_count += 1
        elif row.get("status_key") == "on_break":
            on_break_count += 1
        seconds = int(row.get("today_total_seconds") or 0)
        total_seconds += seconds
        if compensation_type == COMPENSATION_TYPE_HOURLY:
            rate_cents = _decrypt_hourly_rate_cents(profile)
            if rate_cents is not None:
                cost_cents = _cents_for_seconds(seconds, rate_cents)
                row["labor_cost_cents"] = cost_cents
                row["labor_cost_label"] = _format_money_label(cost_cents)
                hourly_today_cents += cost_cents
        elif compensation_type == COMPENSATION_TYPE_MONTHLY:
            row["labor_cost_label"] = "Included in salary"

    total_today_cents = salary_today_cents + hourly_today_cents
    return {
        "clocked_in_count": clocked_in_count,
        "on_break_count": on_break_count,
        "tracked_hours_label": format_hours(total_seconds),
        "hourly_today_label": _format_money_label(hourly_today_cents),
        "salary_today_label": _format_money_label(salary_today_cents),
        "total_today_label": _format_money_label(total_today_cents),
        "salaried_count": salaried_count,
        "hourly_count": hourly_count,
        "unpaid_count": unpaid_count,
        "missing_salary_count": missing_salary_count,
        "missing_hourly_rate_count": missing_hourly_rate_count,
        "basis_label": "Webhook cache for hourly timers + daily salary accrual",
    }


def _parse_boolish(value: Optional[str], *, default: bool = False) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def _parse_labor_day(value: Optional[str]) -> Optional[date]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _labor_stats_window(
    range_key: str,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    today: Optional[date] = None,
) -> dict[str, Any]:
    today = today or date.today()
    key = (range_key or _LABOR_STATS_DEFAULT_RANGE).strip().lower()
    preset_keys = {row["key"] for row in _LABOR_STATS_PRESETS}
    if key not in preset_keys:
        key = _LABOR_STATS_DEFAULT_RANGE

    if key == "today":
        start_day = end_day = today
    elif key == "yesterday":
        start_day = end_day = today - timedelta(days=1)
    elif key == "this_week":
        start_day = today - timedelta(days=today.weekday())
        end_day = today
    elif key == "last_week":
        this_week_start = today - timedelta(days=today.weekday())
        start_day = this_week_start - timedelta(days=7)
        end_day = this_week_start - timedelta(days=1)
    elif key == "last_7":
        start_day = today - timedelta(days=6)
        end_day = today
    elif key == "this_month":
        start_day = today.replace(day=1)
        end_day = today
    elif key == "last_month":
        month_start = today.replace(day=1)
        end_day = month_start - timedelta(days=1)
        start_day = end_day.replace(day=1)
    elif key == "last_30":
        start_day = today - timedelta(days=29)
        end_day = today
    else:
        start_day = _parse_labor_day(start) or today
        end_day = _parse_labor_day(end) or start_day
        key = "custom"

    if end_day > today:
        end_day = today
    if start_day > end_day:
        start_day = end_day
    if (end_day - start_day).days >= _LABOR_STATS_MAX_DAYS:
        start_day = end_day - timedelta(days=_LABOR_STATS_MAX_DAYS - 1)
    return {
        "range_key": key,
        "start_day": start_day,
        "end_day": end_day,
        "start_value": start_day.isoformat(),
        "end_value": end_day.isoformat(),
        "range_label": _format_labor_date_range(start_day, end_day),
    }


def _format_labor_date(value: date) -> str:
    return value.strftime("%b %d, %Y").replace(" 0", " ")


def _format_labor_date_range(start_day: date, end_day: date) -> str:
    if start_day == end_day:
        return _format_labor_date(start_day)
    return f"{_format_labor_date(start_day)} - {_format_labor_date(end_day)}"


def _format_labor_datetime(value: Optional[datetime], tzinfo) -> str:
    if value is None:
        return "No cache yet"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local = value.astimezone(tzinfo)
    return (
        local.strftime("%b %d, %Y %I:%M %p")
        .replace(" 0", " ")
        .replace(" 0", " ")
    )


def _format_rate_label(cents_per_hour: int) -> str:
    if cents_per_hour <= 0:
        return "-"
    return f"{_format_money_label(cents_per_hour)}/hr"


def _format_percent_label(part: int, total: int) -> str:
    if part <= 0 or total <= 0:
        return "0%"
    pct = (Decimal(part) / Decimal(total) * Decimal(100)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return f"{pct}%"


def _clockify_range_bounds(
    start_day: date,
    end_day: date,
    *,
    settings=None,
) -> tuple[datetime, datetime]:
    start_local, _ = _clockify_day_bounds(start_day, settings=settings)
    _, end_local = _clockify_day_bounds(end_day, settings=settings)
    return start_local, end_local


def _labor_stats_clockify_rows(
    session: Session,
    clockify_user_ids: list[str],
    *,
    start_local: datetime,
    end_local: datetime,
) -> list[ClockifyTimeEntry]:
    if not clockify_user_ids:
        return []
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    return list(
        session.exec(
            select(ClockifyTimeEntry).where(
                ClockifyTimeEntry.clockify_user_id.in_(clockify_user_ids),
                ClockifyTimeEntry.is_deleted == False,  # noqa: E712
                ClockifyTimeEntry.start_at < end_utc,
                or_(ClockifyTimeEntry.end_at == None, ClockifyTimeEntry.end_at > start_utc),  # noqa: E711
            )
        ).all()
    )


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clockify_seconds_by_day(
    entry: ClockifyTimeEntry,
    *,
    start_local: datetime,
    end_local: datetime,
    now_utc: datetime,
) -> dict[date, int]:
    start_utc = _as_utc(entry.start_at)
    if start_utc is None:
        return {}
    end_utc = _as_utc(entry.end_at)
    if end_utc is None and entry.is_running:
        end_utc = now_utc
    if end_utc is None and entry.duration_seconds > 0:
        end_utc = start_utc + timedelta(seconds=entry.duration_seconds)
    if end_utc is None or end_utc <= start_utc:
        return {}

    tzinfo = start_local.tzinfo
    entry_start = start_utc.astimezone(tzinfo)
    entry_end = end_utc.astimezone(tzinfo)
    clipped_start = max(entry_start, start_local)
    clipped_end = min(entry_end, end_local)
    if clipped_end <= clipped_start:
        return {}

    out: dict[date, int] = {}
    cursor = clipped_start.date()
    while cursor < end_local.date():
        day_start = datetime.combine(cursor, datetime.min.time(), tzinfo=tzinfo)
        day_end = day_start + timedelta(days=1)
        overlap_start = max(clipped_start, day_start)
        overlap_end = min(clipped_end, day_end)
        if overlap_end > overlap_start:
            out[cursor] = out.get(cursor, 0) + int(
                (overlap_end - overlap_start).total_seconds()
            )
        if day_end >= clipped_end:
            break
        cursor += timedelta(days=1)
    return out


def _labor_employee_active_on(
    user: User,
    profile: Optional[EmployeeProfile],
    day: date,
    *,
    include_inactive: bool,
) -> bool:
    if profile is not None:
        if profile.hire_date and day < profile.hire_date:
            return False
        if profile.termination_date and day > profile.termination_date:
            return False
    if user.is_active or is_draft_user(user):
        return True
    if not include_inactive:
        return False
    return bool(profile and (profile.hire_date or profile.termination_date))


def _labor_salary_cost_for_day(
    *,
    salary_cents: int,
    user: User,
    profile: EmployeeProfile,
    day: date,
    include_inactive: bool,
) -> int:
    if salary_cents <= 0:
        return 0
    if not include_inactive:
        return _salary_cost_for_period(
            salary_cents=salary_cents,
            user=user,
            profile=profile,
            start_day=day,
            end_day=day,
        )
    if not _labor_employee_active_on(
        user, profile, day, include_inactive=include_inactive
    ):
        return 0
    days_in_month = monthrange(day.year, day.month)[1]
    amount = Decimal(salary_cents) / Decimal(days_in_month)
    return int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _blank_labor_day(day: date) -> dict[str, Any]:
    return {
        "day": day,
        "day_label": _format_labor_date(day),
        "work_seconds": 0,
        "break_seconds": 0,
        "hourly_cents": 0,
        "salary_cents": 0,
        "total_cents": 0,
        "entry_count": 0,
    }


def _blank_labor_employee_row(
    user: User,
    profile: EmployeeProfile,
    clockify_user_id: str,
) -> dict[str, Any]:
    compensation_type = _normalize_compensation_type(profile.compensation_type or "")
    hourly_rate_cents = (
        _decrypt_hourly_rate_cents(profile)
        if compensation_type == COMPENSATION_TYPE_HOURLY
        else None
    )
    salary_cents = (
        _decrypt_monthly_salary_cents(profile)
        if compensation_type == COMPENSATION_TYPE_MONTHLY
        else None
    )
    return {
        "employee": user,
        "profile": profile,
        "employee_name": user.display_name or user.username or "Employee",
        "clockify_user_id": clockify_user_id,
        "clockify_user_id_masked": _mask_id(clockify_user_id),
        "compensation_type": compensation_type,
        "pay_type_label": COMPENSATION_TYPE_LABELS.get(compensation_type, "Hourly"),
        "hourly_rate_cents": hourly_rate_cents or 0,
        "salary_cents": salary_cents or 0,
        "rate_label": _format_rate_label(hourly_rate_cents or 0),
        "salary_label": _format_money_label(salary_cents or 0)
        if salary_cents
        else "-",
        "work_seconds": 0,
        "break_seconds": 0,
        "hourly_cents": 0,
        "salary_cents_total": 0,
        "total_cents": 0,
        "entry_count": 0,
        "work_entry_count": 0,
        "break_entry_count": 0,
        "missing_rate": compensation_type == COMPENSATION_TYPE_HOURLY
        and hourly_rate_cents is None,
        "missing_salary": compensation_type == COMPENSATION_TYPE_MONTHLY
        and salary_cents is None,
    }


def _finalize_labor_rows(
    *,
    daily_rows: list[dict[str, Any]],
    employee_rows: list[dict[str, Any]],
    total_cents: int,
) -> None:
    for row in daily_rows:
        row["total_cents"] = row["hourly_cents"] + row["salary_cents"]
        row["work_hours_label"] = format_hours(row["work_seconds"])
        row["break_hours_label"] = format_hours(row["break_seconds"])
        row["hourly_label"] = _format_money_label(row["hourly_cents"])
        row["salary_label"] = _format_money_label(row["salary_cents"])
        row["total_label"] = _format_money_label(row["total_cents"])
        row["share_label"] = _format_percent_label(row["total_cents"], total_cents)

    for row in employee_rows:
        row["total_cents"] = row["hourly_cents"] + row["salary_cents_total"]
        row["work_hours_label"] = format_hours(row["work_seconds"])
        row["break_hours_label"] = format_hours(row["break_seconds"])
        row["hourly_label"] = _format_money_label(row["hourly_cents"])
        row["salary_cost_label"] = _format_money_label(row["salary_cents_total"])
        row["total_label"] = _format_money_label(row["total_cents"])
        row["share_label"] = _format_percent_label(row["total_cents"], total_cents)
    employee_rows.sort(
        key=lambda row: (
            -int(row["total_cents"]),
            -int(row["work_seconds"]),
            str(row["employee_name"]).lower(),
        )
    )


def _labor_stats_core(
    session: Session,
    *,
    start_day: date,
    end_day: date,
    settings=None,
    employee_rows: Optional[list[dict[str, Any]]] = None,
    include_inactive: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    employees = (
        employee_rows
        if employee_rows is not None
        else _employee_rows(session, include_inactive=include_inactive)
    )
    start_local, end_local = _clockify_range_bounds(
        start_day, end_day, settings=settings
    )
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    eligible_rows = [
        row
        for row in employees
        if isinstance(row.get("user"), User)
        and isinstance(row.get("profile"), EmployeeProfile)
    ]
    mapped_rows = [
        row
        for row in eligible_rows
        if (row.get("clockify_user_id") or "").strip()
    ]
    mapped_clockify_ids = [
        (row.get("clockify_user_id") or "").strip() for row in mapped_rows
    ]
    raw_entries = _labor_stats_clockify_rows(
        session,
        mapped_clockify_ids,
        start_local=start_local,
        end_local=end_local,
    )
    entries_by_clockify: dict[str, list[ClockifyTimeEntry]] = {}
    for entry in raw_entries:
        entries_by_clockify.setdefault(entry.clockify_user_id, []).append(entry)

    daily_by_day: dict[date, dict[str, Any]] = {}
    cursor = start_day
    while cursor <= end_day:
        daily_by_day[cursor] = _blank_labor_day(cursor)
        cursor += timedelta(days=1)

    employee_stats: dict[str, dict[str, Any]] = {}
    total_hourly_cents = 0
    total_salary_cents = 0
    total_work_seconds = 0
    total_break_seconds = 0
    active_timer_count = 0
    missing_hourly_rate_count = 0
    missing_salary_count = 0
    salary_people_count = 0
    hourly_people_count = 0
    unpaid_people_count = 0
    latest_cache_at: Optional[datetime] = None

    for source_row in eligible_rows:
        user = source_row["user"]
        profile = source_row["profile"]
        clockify_user_id = (source_row.get("clockify_user_id") or "").strip()
        employee_key = clockify_user_id or f"user:{user.id}"
        row = _blank_labor_employee_row(user, profile, clockify_user_id)
        compensation_type = row["compensation_type"]
        employee_stats[employee_key] = row

        if compensation_type == COMPENSATION_TYPE_UNPAID:
            unpaid_people_count += 1
        elif compensation_type == COMPENSATION_TYPE_HOURLY:
            hourly_people_count += 1
            if row["missing_rate"]:
                missing_hourly_rate_count += 1
        elif compensation_type == COMPENSATION_TYPE_MONTHLY:
            salary_people_count += 1
            if row["missing_salary"]:
                missing_salary_count += 1
            else:
                day_cursor = start_day
                while day_cursor <= end_day:
                    salary_cost = _labor_salary_cost_for_day(
                        salary_cents=int(row["salary_cents"] or 0),
                        user=user,
                        profile=profile,
                        day=day_cursor,
                        include_inactive=include_inactive,
                    )
                    if salary_cost:
                        row["salary_cents_total"] += salary_cost
                        daily_by_day[day_cursor]["salary_cents"] += salary_cost
                        total_salary_cents += salary_cost
                    day_cursor += timedelta(days=1)

        for entry in entries_by_clockify.get(clockify_user_id, []):
            cache_at = _as_utc(entry.updated_at or entry.received_at)
            if cache_at and (latest_cache_at is None or cache_at > latest_cache_at):
                latest_cache_at = cache_at
            if entry.is_running:
                active_timer_count += 1
            seconds_by_day = _clockify_seconds_by_day(
                entry,
                start_local=start_local,
                end_local=end_local,
                now_utc=now_utc,
            )
            if not seconds_by_day:
                continue
            is_break = _clockify_entry_is_break(entry)
            row["entry_count"] += 1
            if is_break:
                row["break_entry_count"] += 1
            else:
                row["work_entry_count"] += 1
            for day_key, seconds in seconds_by_day.items():
                day_row = daily_by_day.get(day_key)
                if day_row is None:
                    continue
                day_row["entry_count"] += 1
                if is_break:
                    row["break_seconds"] += seconds
                    day_row["break_seconds"] += seconds
                    total_break_seconds += seconds
                    continue
                row["work_seconds"] += seconds
                day_row["work_seconds"] += seconds
                total_work_seconds += seconds
                if compensation_type == COMPENSATION_TYPE_HOURLY and row["hourly_rate_cents"]:
                    hourly_cost = _cents_for_seconds(seconds, int(row["hourly_rate_cents"]))
                    row["hourly_cents"] += hourly_cost
                    day_row["hourly_cents"] += hourly_cost
                    total_hourly_cents += hourly_cost

    total_cents = total_hourly_cents + total_salary_cents
    daily_rows = list(daily_by_day.values())
    employee_rows_out = [
        row
        for row in employee_stats.values()
        if row["hourly_cents"]
        or row["salary_cents_total"]
        or row["total_cents"]
        or row["work_seconds"]
        or row["break_seconds"]
        or row["missing_rate"]
        or row["missing_salary"]
    ]
    _finalize_labor_rows(
        daily_rows=daily_rows,
        employee_rows=employee_rows_out,
        total_cents=total_cents,
    )
    effective_cents = (
        int(
            (Decimal(total_cents) * Decimal(3600) / Decimal(total_work_seconds)).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
        )
        if total_work_seconds > 0
        else 0
    )
    timezone_name = str(getattr(start_local.tzinfo, "key", None) or start_local.tzinfo)
    return {
        "start_day": start_day,
        "end_day": end_day,
        "start_local": start_local,
        "end_local": end_local,
        "timezone_name": timezone_name,
        "range_label": _format_labor_date_range(start_day, end_day),
        "daily_rows": daily_rows,
        "employee_rows": employee_rows_out,
        "entry_count": len(raw_entries),
        "mapped_count": len(mapped_rows),
        "unmapped_count": max(0, len(eligible_rows) - len(mapped_rows)),
        "hourly_people_count": hourly_people_count,
        "salary_people_count": salary_people_count,
        "unpaid_people_count": unpaid_people_count,
        "missing_hourly_rate_count": missing_hourly_rate_count,
        "missing_salary_count": missing_salary_count,
        "active_timer_count": active_timer_count,
        "work_seconds": total_work_seconds,
        "break_seconds": total_break_seconds,
        "hourly_cents": total_hourly_cents,
        "salary_cents": total_salary_cents,
        "total_cents": total_cents,
        "work_hours_label": format_hours(total_work_seconds),
        "break_hours_label": format_hours(total_break_seconds),
        "hourly_label": _format_money_label(total_hourly_cents),
        "salary_label": _format_money_label(total_salary_cents),
        "total_label": _format_money_label(total_cents),
        "effective_rate_label": _format_rate_label(effective_cents),
        "latest_cache_label": _format_labor_datetime(latest_cache_at, start_local.tzinfo),
        "basis_label": "Actual Clockify work entries plus compensation settings",
    }


def build_labor_stats_summary(
    session: Session,
    *,
    start_day: date,
    end_day: date,
    settings=None,
    employee_rows: Optional[list[dict[str, Any]]] = None,
    include_inactive: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    today = (now.astimezone(timezone.utc).date() if now else date.today())
    selected = _labor_stats_core(
        session,
        start_day=start_day,
        end_day=end_day,
        settings=settings,
        employee_rows=employee_rows,
        include_inactive=include_inactive,
        now=now,
    )
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    week_to_date = _labor_stats_core(
        session,
        start_day=week_start,
        end_day=today,
        settings=settings,
        employee_rows=employee_rows,
        include_inactive=include_inactive,
        now=now,
    )
    full_week_salary_cents = 0
    employees = (
        employee_rows
        if employee_rows is not None
        else _employee_rows(session, include_inactive=include_inactive)
    )
    for source_row in employees:
        user = source_row.get("user")
        profile = source_row.get("profile")
        if not isinstance(user, User) or not isinstance(profile, EmployeeProfile):
            continue
        if _normalize_compensation_type(profile.compensation_type or "") != COMPENSATION_TYPE_MONTHLY:
            continue
        salary_cents = _decrypt_monthly_salary_cents(profile)
        if salary_cents is None:
            continue
        cursor = week_start
        while cursor <= week_end:
            full_week_salary_cents += _labor_salary_cost_for_day(
                salary_cents=salary_cents,
                user=user,
                profile=profile,
                day=cursor,
                include_inactive=include_inactive,
            )
            cursor += timedelta(days=1)
    estimated_week_cents = week_to_date["hourly_cents"] + full_week_salary_cents
    selected["week_estimate"] = {
        "range_label": _format_labor_date_range(week_start, week_end),
        "total_label": _format_money_label(estimated_week_cents),
        "hourly_to_date_label": _format_money_label(week_to_date["hourly_cents"]),
        "salary_week_label": _format_money_label(full_week_salary_cents),
        "basis_label": "Clocked hourly so far plus full-week salary accrual",
    }
    return selected


def refresh_clockify_labor_cache(
    session: Session,
    client: ClockifyClient,
    *,
    start_day: date,
    end_day: date,
    settings=None,
    employee_rows: Optional[list[dict[str, Any]]] = None,
    include_inactive: bool = False,
    source_event: str = "LABOR_STATS_REFRESH",
) -> dict[str, Any]:
    start_local, end_local = _clockify_range_bounds(
        start_day, end_day, settings=settings
    )
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    employees = (
        employee_rows
        if employee_rows is not None
        else _employee_rows(session, include_inactive=include_inactive)
    )
    mapped_rows = [
        row for row in employees if (row.get("clockify_user_id") or "").strip()
    ]
    refreshed_users = 0
    cached_entries = 0
    errors: list[str] = []
    received_at = utcnow()

    for row in mapped_rows:
        clockify_user_id = (row.get("clockify_user_id") or "").strip()
        try:
            entries = client.get_user_time_entries(
                clockify_user_id,
                start_utc=start_utc,
                end_utc=end_utc,
                page_size=1000,
                max_pages=20,
            )
        except (ClockifyApiError, ClockifyConfigError) as exc:
            employee = row.get("user")
            employee_name = (
                getattr(employee, "display_name", None)
                or getattr(employee, "username", None)
                or _mask_id(clockify_user_id)
            )
            errors.append(f"{employee_name}: {exc}")
            continue

        seen_entry_ids: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_payload = dict(entry)
            entry_payload.setdefault("userId", clockify_user_id)
            entry_id = str(entry.get("id") or "").strip()
            if entry_id:
                seen_entry_ids.add(entry_id)
            cached = _upsert_clockify_time_entry_from_payload(
                session,
                {
                    "workspaceId": getattr(settings, "clockify_workspace_id", ""),
                    "timeEntry": entry_payload,
                },
                source_event=source_event,
                settings=settings,
                received_at=received_at,
            )
            if cached is not None:
                cached_entries += 1

        existing_rows = session.exec(
            select(ClockifyTimeEntry).where(
                ClockifyTimeEntry.clockify_user_id == clockify_user_id,
                ClockifyTimeEntry.is_deleted == False,  # noqa: E712
                ClockifyTimeEntry.start_at < end_utc,
                or_(ClockifyTimeEntry.end_at == None, ClockifyTimeEntry.end_at > start_utc),  # noqa: E711
            )
        ).all()
        for existing in existing_rows:
            if existing.clockify_entry_id not in seen_entry_ids:
                existing.is_deleted = True
                existing.is_running = False
                existing.updated_at = received_at
                session.add(existing)
        refreshed_users += 1

    session.commit()
    return {
        "mapped_users": len(mapped_rows),
        "refreshed_users": refreshed_users,
        "cached_entries": cached_entries,
        "error_count": len(errors),
        "errors": errors,
        "range_label": _format_labor_date_range(start_day, end_day),
    }


def refresh_clockify_shift_tracker_cache(
    session: Session,
    client: ClockifyClient,
    *,
    settings=None,
    today: Optional[date] = None,
    employee_rows: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    day = today or date.today()
    start_local, end_local = _clockify_day_bounds(day, settings=settings)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    employees = employee_rows if employee_rows is not None else _employee_rows(session)
    mapped_rows = [
        row for row in employees if (row.get("clockify_user_id") or "").strip()
    ]
    refreshed_users = 0
    cached_entries = 0
    errors: list[str] = []
    received_at = utcnow()

    for row in mapped_rows:
        clockify_user_id = (row.get("clockify_user_id") or "").strip()
        try:
            entries = client.get_user_time_entries(
                clockify_user_id,
                start_utc=start_utc,
                end_utc=end_utc,
            )
        except (ClockifyApiError, ClockifyConfigError) as exc:
            employee = row.get("user")
            employee_name = (
                getattr(employee, "display_name", None)
                or getattr(employee, "username", None)
                or _mask_id(clockify_user_id)
            )
            errors.append(f"{employee_name}: {exc}")
            continue

        seen_entry_ids: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_payload = dict(entry)
            entry_payload.setdefault("userId", clockify_user_id)
            entry_id = str(entry.get("id") or "").strip()
            if entry_id:
                seen_entry_ids.add(entry_id)
            cached = _upsert_clockify_time_entry_from_payload(
                session,
                {
                    "workspaceId": getattr(settings, "clockify_workspace_id", ""),
                    "timeEntry": entry_payload,
                },
                source_event="MANUAL_REFRESH",
                settings=settings,
                received_at=received_at,
            )
            if cached is not None:
                cached_entries += 1

        existing_rows = session.exec(
            select(ClockifyTimeEntry).where(
                ClockifyTimeEntry.clockify_user_id == clockify_user_id,
                ClockifyTimeEntry.is_deleted == False,  # noqa: E712
                ClockifyTimeEntry.start_at < end_utc,
                or_(ClockifyTimeEntry.end_at == None, ClockifyTimeEntry.end_at > start_utc),  # noqa: E711
            )
        ).all()
        for existing in existing_rows:
            if existing.clockify_entry_id not in seen_entry_ids:
                existing.is_deleted = True
                existing.is_running = False
                existing.updated_at = received_at
                session.add(existing)
        refreshed_users += 1

    session.commit()
    return {
        "mapped_users": len(mapped_rows),
        "refreshed_users": refreshed_users,
        "cached_entries": cached_entries,
        "error_count": len(errors),
        "errors": errors,
    }


def set_employee_clockify_user_id(
    session: Session,
    *,
    current_user: User,
    user_id: int,
    clockify_user_id: str,
    ip_address: Optional[str] = None,
) -> tuple[bool, str]:
    employee = session.get(User, user_id)
    if employee is None:
        raise ValueError("employee_not_found")

    clockify_user_id = (clockify_user_id or "").strip()
    if clockify_user_id:
        existing = session.exec(
            select(EmployeeProfile).where(
                EmployeeProfile.clockify_user_id == clockify_user_id,
                EmployeeProfile.user_id != user_id,
            )
        ).first()
        if existing is not None:
            other_user = session.get(User, existing.user_id)
            other_name = (
                other_user.display_name or other_user.username
                if other_user is not None
                else f"employee {existing.user_id}"
            )
            return False, f"That Clockify user is already linked to {other_name}."

    profile = session.get(EmployeeProfile, user_id)
    if profile is None:
        profile = EmployeeProfile(user_id=user_id)
        session.add(profile)
        session.flush()

    old_value = (profile.clockify_user_id or "").strip()
    if old_value == clockify_user_id:
        return True, "No change."

    profile.clockify_user_id = clockify_user_id or None
    profile.updated_at = utcnow()
    session.add(profile)
    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            target_user_id=user_id,
            action="admin.clockify.manual_link",
            resource_key="admin.employees.edit",
            details_json=json.dumps(
                {
                    "old_clockify_user_id": old_value,
                    "new_clockify_user_id": clockify_user_id,
                },
                sort_keys=True,
            ),
            ip_address=ip_address,
        )
    )
    session.commit()
    if clockify_user_id:
        return True, "Clockify user linked."
    return True, "Clockify user unlinked."


def sync_clockify_user_ids_by_email(
    session: Session,
    *,
    current_user: User,
    clockify_users: list[dict[str, Any]],
    ip_address: Optional[str] = None,
) -> dict[str, int]:
    """Link local employee profiles to Clockify ids by email or safe name match.

    Existing conflicting Clockify ids are left untouched. Draft employees are
    included because they are inactive until onboarding. Counts are audited;
    raw email addresses and employee names are never written to AuditLog.
    """
    by_email = _clockify_users_by_email(clockify_users)
    by_name, ambiguous_names = _clockify_users_by_name(clockify_users)
    profiles = list(session.exec(select(EmployeeProfile)).all())
    users = {
        row.id: row
        for row in session.exec(select(User)).all()
        if row.id is not None
    }
    linked_clockify_ids = {
        (profile.clockify_user_id or "").strip(): profile.user_id
        for profile in profiles
        if (profile.clockify_user_id or "").strip()
    }
    now = utcnow()
    counts = {
        "checked": 0,
        "mapped": 0,
        "email_matched": 0,
        "name_matched": 0,
        "already_mapped": 0,
        "conflicts": 0,
        "missing_email": 0,
        "email_decrypt_failed": 0,
        "no_clockify_match": 0,
        "ambiguous_name_match": 0,
    }

    for profile in profiles:
        user = users.get(profile.user_id)
        if not _is_matchable_team_user(user):
            continue
        counts["checked"] += 1
        match: Optional[dict[str, Any]] = None
        match_method = ""
        if not profile.email_ciphertext:
            counts["missing_email"] += 1
        else:
            try:
                email = (decrypt_pii(profile.email_ciphertext) or "").strip().lower()
            except ValueError:
                counts["email_decrypt_failed"] += 1
                email = ""
            if not email:
                counts["missing_email"] += 1
            else:
                match = by_email.get(email)
                if match is not None:
                    match_method = "email"

        if match is None:
            match, ambiguous = _find_clockify_name_match(
                user,
                profile,
                by_name,
                ambiguous_names,
            )
            if ambiguous:
                counts["ambiguous_name_match"] += 1
            if match is not None:
                match_method = "name"

        if match is None:
            counts["no_clockify_match"] += 1
            continue
        match_id = str(match.get("id") or "").strip()
        if not match_id:
            counts["no_clockify_match"] += 1
            continue
        existing = (profile.clockify_user_id or "").strip()
        if existing == match_id:
            counts["already_mapped"] += 1
            continue
        if existing and existing != match_id:
            counts["conflicts"] += 1
            continue
        linked_user_id = linked_clockify_ids.get(match_id)
        if linked_user_id is not None and linked_user_id != profile.user_id:
            counts["conflicts"] += 1
            continue
        profile.clockify_user_id = match_id
        profile.updated_at = now
        session.add(profile)
        linked_clockify_ids[match_id] = profile.user_id
        counts["mapped"] += 1
        if match_method == "email":
            counts["email_matched"] += 1
        elif match_method == "name":
            counts["name_matched"] += 1

    session.add(
        AuditLog(
            actor_user_id=current_user.id,
            action="admin.clockify.sync_users",
            resource_key="admin.employees.edit",
            details_json=json.dumps(counts, sort_keys=True),
            ip_address=ip_address,
        )
    )
    session.commit()
    return counts


@router.post("/webhooks/clockify")
async def clockify_webhook(
    request: Request,
    secret: str = Query(default=""),
    session: Session = Depends(get_session),
):
    _require_clockify_webhook_secret(request, secret)
    raw_body = await request.body()
    try:
        payload = _json_loads_body(raw_body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = process_clockify_webhook_payload(
        session,
        payload,
        raw_body=raw_body,
        settings=get_settings(),
    )
    status_code = 200 if not result.get("warning") else 202
    return JSONResponse(result, status_code=status_code)


@router.get("/team/admin/clockify", response_class=HTMLResponse)
def admin_clockify_page(
    request: Request,
    flash: Optional[str] = None,
    error: Optional[str] = None,
    include_hours: str = Query(default="0"),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    settings = get_settings()
    configured = clockify_is_configured(settings)
    workspace = None
    status_error = None
    clockify_users: list[dict[str, Any]] = []
    clockify_users_display: list[dict[str, Any]] = []
    roster_preview: list[dict[str, Any]] = []
    clockify_user_map: dict[str, dict[str, Any]] = {}
    preview_capped = False
    client: Optional[ClockifyClient] = None
    include_hour_preview = include_hours not in ("0", "false", "no", "off")
    if configured:
        try:
            client = clockify_client_from_settings(settings)
            workspace = client.workspace_info()
            clockify_users = client.list_workspace_users(status="ALL")
            clockify_users_display = [_masked_clockify_user(row) for row in clockify_users]
            clockify_user_map = {
                _clockify_user_id(row): row
                for row in clockify_users_display
                if _clockify_user_id(row)
            }
            roster_preview = build_clockify_roster_preview(
                clockify_users,
                client=client,
                settings=settings,
                include_hours=include_hour_preview,
            )
            preview_capped = len(clockify_users) > len(roster_preview)
        except (ClockifyApiError, ClockifyConfigError) as exc:
            status_error = str(exc)
    employees = _employee_rows(session)
    linked_by_clockify = _employee_link_map(employees)
    counts = _employee_clockify_counts(employees)
    return templates.TemplateResponse(
        request,
        "team/admin/clockify.html",
        {
            "request": request,
            "title": "Clockify",
            "current_user": user,
            "configured": configured,
            "workspace": workspace,
            "workspace_id_masked": _mask_id(settings.clockify_workspace_id),
            "status_error": status_error,
            "clockify_users": clockify_users_display,
            "clockify_user_map": clockify_user_map,
            "roster_preview": roster_preview,
            "preview_capped": preview_capped,
            "include_hours": include_hour_preview,
            "employees": employees,
            "linked_by_clockify": linked_by_clockify,
            "counts": counts,
            "can_sync": user.role == "admin",
            "mask_id": _mask_id,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
        },
    )


@router.get("/team/admin/shift-tracker", response_class=HTMLResponse)
def admin_shift_tracker_page(
    request: Request,
    flash: Optional[str] = None,
    error: Optional[str] = None,
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    settings = get_settings()
    configured = clockify_is_configured(settings)
    employees = _employee_rows(session)
    counts = _employee_clockify_counts(employees)
    live = build_clockify_live_status(
        session,
        None,
        settings=settings,
        employee_rows=employees,
    )
    pay_summary = build_shift_tracker_pay_summary(
        session,
        live,
        employee_rows=employees,
    )
    return templates.TemplateResponse(
        request,
        "team/admin/shift_tracker.html",
        {
            "request": request,
            "title": "Shift Tracker",
            "current_user": user,
            "configured": configured,
            "counts": counts,
            "live": live,
            "pay_summary": pay_summary,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
            "can_refresh": user.role == "admin",
        },
    )


@router.post(
    "/team/admin/shift-tracker/refresh",
    dependencies=[Depends(require_csrf)],
)
async def admin_shift_tracker_refresh(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, _user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    settings = get_settings()
    if not clockify_is_configured(settings):
        return RedirectResponse(
            "/team/admin/shift-tracker?"
            + urlencode(
                {"error": "CLOCKIFY_API_KEY and CLOCKIFY_WORKSPACE_ID are required."}
            ),
            status_code=303,
        )
    try:
        result = refresh_clockify_shift_tracker_cache(
            session,
            clockify_client_from_settings(settings),
            settings=settings,
        )
    except (ClockifyApiError, ClockifyConfigError) as exc:
        return RedirectResponse(
            "/team/admin/shift-tracker?" + urlencode({"error": str(exc)}),
            status_code=303,
        )
    if result["error_count"]:
        flash = (
            f"Refreshed {result['refreshed_users']} of {result['mapped_users']} "
            f"mapped employee(s). {result['error_count']} error(s); "
            f"{result['cached_entries']} entries cached."
        )
    else:
        flash = (
            f"Refreshed {result['refreshed_users']} mapped employee(s); "
            f"{result['cached_entries']} entries cached."
        )
    return RedirectResponse(
        "/team/admin/shift-tracker?" + urlencode({"flash": flash}),
        status_code=303,
    )


@router.get("/team/admin/labor-stats", response_class=HTMLResponse)
def admin_labor_stats_page(
    request: Request,
    range_key: str = Query(default=_LABOR_STATS_DEFAULT_RANGE, alias="range"),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    show_inactive: Optional[str] = Query(default=None),
    flash: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    denial, user = _permission_gate(request, session, "admin.employees.view")
    if denial:
        return denial
    settings = get_settings()
    configured = clockify_is_configured(settings)
    include_inactive = _parse_boolish(show_inactive, default=True)
    window = _labor_stats_window(range_key, start=start, end=end)
    employees = _employee_rows(session, include_inactive=include_inactive)
    stats = build_labor_stats_summary(
        session,
        start_day=window["start_day"],
        end_day=window["end_day"],
        settings=settings,
        employee_rows=employees,
        include_inactive=include_inactive,
    )
    return templates.TemplateResponse(
        request,
        "team/admin/labor_stats.html",
        {
            "request": request,
            "title": "Labor Stats",
            "current_user": user,
            "configured": configured,
            "labor_presets": _LABOR_STATS_PRESETS,
            "window": window,
            "stats": stats,
            "include_inactive": include_inactive,
            "flash": flash,
            "error": error,
            "csrf_token": issue_token(request),
            "can_refresh": user.role == "admin",
        },
    )


@router.post(
    "/team/admin/labor-stats/refresh",
    dependencies=[Depends(require_csrf)],
)
async def admin_labor_stats_refresh(
    request: Request,
    range_key: str = Form(default=_LABOR_STATS_DEFAULT_RANGE),
    start: str = Form(default=""),
    end: str = Form(default=""),
    show_inactive: str = Form(default="1"),
    session: Session = Depends(get_session),
):
    denial, _user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    settings = get_settings()
    include_inactive = _parse_boolish(show_inactive, default=True)
    window = _labor_stats_window(range_key, start=start, end=end)
    qs_base = {
        "range": window["range_key"],
        "start": window["start_value"],
        "end": window["end_value"],
        "show_inactive": "1" if include_inactive else "0",
    }
    if not clockify_is_configured(settings):
        qs = dict(qs_base)
        qs["error"] = "CLOCKIFY_API_KEY and CLOCKIFY_WORKSPACE_ID are required."
        return RedirectResponse(
            "/team/admin/labor-stats?" + urlencode(qs),
            status_code=303,
        )
    employees = _employee_rows(session, include_inactive=include_inactive)
    try:
        result = refresh_clockify_labor_cache(
            session,
            clockify_client_from_settings(settings),
            start_day=window["start_day"],
            end_day=window["end_day"],
            settings=settings,
            employee_rows=employees,
            include_inactive=include_inactive,
        )
    except (ClockifyApiError, ClockifyConfigError) as exc:
        qs = dict(qs_base)
        qs["error"] = str(exc)
        return RedirectResponse(
            "/team/admin/labor-stats?" + urlencode(qs),
            status_code=303,
        )
    if result["error_count"]:
        flash = (
            f"Refreshed {result['refreshed_users']} of {result['mapped_users']} "
            f"mapped employee(s) for {result['range_label']}. "
            f"{result['error_count']} error(s); {result['cached_entries']} entries cached."
        )
    else:
        flash = (
            f"Refreshed {result['refreshed_users']} mapped employee(s) for "
            f"{result['range_label']}; {result['cached_entries']} entries cached."
        )
    qs = dict(qs_base)
    qs["flash"] = flash
    return RedirectResponse(
        "/team/admin/labor-stats?" + urlencode(qs),
        status_code=303,
    )


@router.post(
    "/team/admin/clockify/sync-users",
    dependencies=[Depends(require_csrf)],
)
async def admin_clockify_sync_users(
    request: Request,
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    settings = get_settings()
    if not clockify_is_configured(settings):
        return RedirectResponse(
            "/team/admin/clockify?error=CLOCKIFY_API_KEY+and+CLOCKIFY_WORKSPACE_ID+are+required.",
            status_code=303,
        )
    try:
        clockify_users = clockify_client_from_settings(settings).list_workspace_users(
            status="ALL"
        )
        counts = sync_clockify_user_ids_by_email(
            session,
            current_user=user,
            clockify_users=clockify_users,
            ip_address=(request.client.host if request.client else None),
        )
    except (ClockifyApiError, ClockifyConfigError) as exc:
        return RedirectResponse(
            "/team/admin/clockify?" + urlencode({"error": str(exc)}),
            status_code=303,
        )
    flash = (
        f"Mapped {counts['mapped']} employee(s) "
        f"({counts['email_matched']} by email, {counts['name_matched']} by name). "
        f"{counts['already_mapped']} already linked, "
        f"{counts['conflicts']} conflict(s), "
        f"{counts['no_clockify_match']} without a Clockify match."
    )
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({"flash": flash}),
        status_code=303,
    )


@router.post(
    "/team/admin/clockify/manual-link",
    dependencies=[Depends(require_csrf)],
)
async def admin_clockify_manual_link(
    request: Request,
    user_id: int = Form(...),
    clockify_user_id: str = Form(default=""),
    session: Session = Depends(get_session),
):
    denial, user = _admin_gate(request, session, "admin.employees.edit")
    if denial:
        return denial
    try:
        ok, message = set_employee_clockify_user_id(
            session,
            current_user=user,
            user_id=user_id,
            clockify_user_id=clockify_user_id,
            ip_address=(request.client.host if request.client else None),
        )
    except ValueError:
        return RedirectResponse(
            "/team/admin/clockify?" + urlencode({"error": "Employee not found."}),
            status_code=303,
        )
    key = "flash" if ok else "error"
    return RedirectResponse(
        "/team/admin/clockify?" + urlencode({key: message}),
        status_code=303,
    )
