"""Clockify API helpers for the employee portal.

The integration is intentionally read-first. Clockify remains the source of
truth for time entries; the portal pulls summaries and stores only the
Clockify user id on EmployeeProfile.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from ..config import Settings, get_settings


DEFAULT_CLOCKIFY_BASE_URL = "https://api.clockify.me/api/v1"
DEFAULT_CLOCKIFY_TIMEZONE = "America/Los_Angeles"
DEFAULT_CLOCKIFY_ACCOUNT_STATUSES = (
    "ACTIVE",
    "PENDING_EMAIL_VERIFICATION",
    "NOT_REGISTERED",
    "LIMITED",
)


class ClockifyConfigError(RuntimeError):
    """Raised when the Clockify integration is not configured enough to call."""


class ClockifyApiError(RuntimeError):
    """Raised for Clockify transport/API failures with a safe message."""


@dataclass(frozen=True)
class ClockifyEntryView:
    id: str
    description: str
    start_local: Optional[datetime]
    end_local: Optional[datetime]
    duration_seconds: int
    running: bool


@dataclass(frozen=True)
class ClockifyDailyTotal:
    day: date
    duration_seconds: int


@dataclass(frozen=True)
class ClockifyWeekSummary:
    week_start: date
    week_end_exclusive: date
    timezone_name: str
    total_seconds: int
    running_count: int
    daily_totals: list[ClockifyDailyTotal]
    entries: list[ClockifyEntryView]

    @property
    def week_end_inclusive(self) -> date:
        return self.week_end_exclusive - timedelta(days=1)


def clockify_is_configured(settings: Optional[Settings] = None) -> bool:
    settings = settings or get_settings()
    return bool(
        (settings.clockify_api_key or "").strip()
        and (settings.clockify_workspace_id or "").strip()
    )


def _clockify_timezone(settings: Optional[Settings] = None) -> ZoneInfo:
    settings = settings or get_settings()
    name = (settings.clockify_timezone or DEFAULT_CLOCKIFY_TIMEZONE).strip()
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_CLOCKIFY_TIMEZONE)


def _clockify_timezone_name(settings: Optional[Settings] = None) -> str:
    tz = _clockify_timezone(settings)
    return str(tz.key)


def clockify_today(
    *,
    settings: Optional[Settings] = None,
    now: Optional[datetime] = None,
) -> date:
    """Return today's date in the configured business/Clockify timezone."""
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return now_utc.astimezone(_clockify_timezone(settings)).date()


def clockify_week_bounds(
    day: Optional[date] = None,
    *,
    settings: Optional[Settings] = None,
) -> tuple[datetime, datetime]:
    """Return Monday-to-Monday local week bounds converted to aware datetimes."""
    tz = _clockify_timezone(settings)
    day = day or clockify_today(settings=settings)
    week_start = day - timedelta(days=day.weekday())
    start_local = datetime.combine(week_start, time.min, tzinfo=tz)
    end_local = start_local + timedelta(days=7)
    return start_local, end_local


def _to_clockify_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_clockify_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


_DURATION_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def parse_iso_duration_seconds(value: Any) -> Optional[int]:
    if not isinstance(value, str):
        return None
    match = _DURATION_RE.match(value.strip())
    if not match:
        return None
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def format_hours(total_seconds: int) -> str:
    total_minutes = max(0, int(round(total_seconds / 60)))
    hours, minutes = divmod(total_minutes, 60)
    if hours and minutes:
        return f"{hours}h {minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def _entry_interval(entry: dict[str, Any], now_utc: datetime) -> tuple[Optional[datetime], Optional[datetime], bool]:
    interval = entry.get("timeInterval") if isinstance(entry, dict) else None
    if not isinstance(interval, dict):
        interval = {}
    start = parse_clockify_datetime(interval.get("start"))
    end = parse_clockify_datetime(interval.get("end"))
    running = bool(start and end is None)
    if running:
        end = now_utc
    return start, end, running


def _entry_duration_seconds(entry: dict[str, Any], now_utc: datetime) -> int:
    start, end, running = _entry_interval(entry, now_utc)
    if start and end:
        return max(0, int((end - start).total_seconds()))
    interval = entry.get("timeInterval") if isinstance(entry, dict) else None
    if isinstance(interval, dict):
        parsed = parse_iso_duration_seconds(interval.get("duration"))
        if parsed is not None:
            return parsed
    return 0 if not running else 0


def _entry_intersects_range(
    entry: dict[str, Any],
    start_utc: datetime,
    end_utc: datetime,
    now_utc: datetime,
) -> bool:
    start, end, _running = _entry_interval(entry, now_utc)
    if start is None:
        return False
    end = end or start
    return start < end_utc and end > start_utc


def _daily_totals_for(
    entry_views: list[ClockifyEntryView],
    week_start: date,
    week_end_exclusive: date,
    tz: ZoneInfo,
) -> list[ClockifyDailyTotal]:
    totals: dict[date, int] = {
        week_start + timedelta(days=offset): 0 for offset in range(7)
    }
    for entry in entry_views:
        if entry.start_local is None:
            continue
        start = entry.start_local
        end = entry.end_local or start
        cursor_day = start.date()
        while cursor_day < week_end_exclusive:
            day_start = datetime.combine(cursor_day, time.min, tzinfo=tz)
            day_end = day_start + timedelta(days=1)
            overlap_start = max(start, day_start)
            overlap_end = min(end, day_end)
            if overlap_end > overlap_start and cursor_day in totals:
                totals[cursor_day] += int((overlap_end - overlap_start).total_seconds())
            if day_end >= end:
                break
            cursor_day += timedelta(days=1)
    return [
        ClockifyDailyTotal(day=day, duration_seconds=seconds)
        for day, seconds in sorted(totals.items())
    ]


def build_week_summary(
    entries: list[dict[str, Any]],
    *,
    week_start_local: datetime,
    week_end_local: datetime,
    settings: Optional[Settings] = None,
    now: Optional[datetime] = None,
) -> ClockifyWeekSummary:
    tz = _clockify_timezone(settings)
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start_utc = week_start_local.astimezone(timezone.utc)
    end_utc = week_end_local.astimezone(timezone.utc)
    views: list[ClockifyEntryView] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if not _entry_intersects_range(entry, start_utc, end_utc, now_utc):
            continue
        start_utc_entry, end_utc_entry, running = _entry_interval(entry, now_utc)
        if start_utc_entry and end_utc_entry:
            duration_start = max(start_utc_entry, start_utc)
            duration_end = min(end_utc_entry, end_utc)
            duration = max(0, int((duration_end - duration_start).total_seconds()))
        else:
            duration = _entry_duration_seconds(entry, now_utc)
        start_local = start_utc_entry.astimezone(tz) if start_utc_entry else None
        end_local = end_utc_entry.astimezone(tz) if end_utc_entry else None
        views.append(
            ClockifyEntryView(
                id=str(entry.get("id") or ""),
                description=str(entry.get("description") or "Clockify entry").strip()
                or "Clockify entry",
                start_local=start_local,
                end_local=end_local,
                duration_seconds=duration,
                running=running,
            )
        )
    views.sort(key=lambda row: row.start_local or datetime.min.replace(tzinfo=tz))
    week_start = week_start_local.date()
    week_end = week_end_local.date()
    daily_totals = _daily_totals_for(views, week_start, week_end, tz)
    return ClockifyWeekSummary(
        week_start=week_start,
        week_end_exclusive=week_end,
        timezone_name=str(tz.key),
        total_seconds=sum(row.duration_seconds for row in views),
        running_count=sum(1 for row in views if row.running),
        daily_totals=daily_totals,
        entries=views,
    )


class ClockifyClient:
    def __init__(
        self,
        *,
        api_key: str,
        workspace_id: str,
        base_url: str = DEFAULT_CLOCKIFY_BASE_URL,
        timeout_seconds: float = 12.0,
    ):
        self.api_key = (api_key or "").strip()
        self.workspace_id = (workspace_id or "").strip()
        self.base_url = (base_url or DEFAULT_CLOCKIFY_BASE_URL).rstrip("/")
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_settings(cls, settings: Optional[Settings] = None) -> "ClockifyClient":
        settings = settings or get_settings()
        return cls(
            api_key=settings.clockify_api_key,
            workspace_id=settings.clockify_workspace_id,
            base_url=settings.clockify_base_url,
            timeout_seconds=settings.clockify_timeout_seconds,
        )

    def _require_config(self) -> None:
        if not self.api_key:
            raise ClockifyConfigError("CLOCKIFY_API_KEY is not set.")
        if not self.workspace_id:
            raise ClockifyConfigError("CLOCKIFY_WORKSPACE_ID is not set.")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Any:
        self._require_config()
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {"Accept": "application/json", "X-Api-Key": self.api_key}
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        try:
            with httpx.Client(
                timeout=self.timeout_seconds,
                follow_redirects=True,
                headers=headers,
            ) as client:
                response = client.request(method, url, params=params, json=json_body)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            body = (exc.response.text or "").strip()[:240]
            detail = f" Clockify said: {body}" if body else ""
            raise ClockifyApiError(f"Clockify request failed with HTTP {status}.{detail}") from exc
        except httpx.HTTPError as exc:
            raise ClockifyApiError(f"Clockify request failed: {exc}") from exc
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ClockifyApiError("Clockify returned a non-JSON response.") from exc

    def workspace_info(self) -> dict[str, Any]:
        data = self._request("GET", f"/workspaces/{self.workspace_id}")
        return data if isinstance(data, dict) else {}

    def list_workspace_users(
        self,
        *,
        status: str = "ALL",
        account_statuses: Optional[tuple[str, ...] | list[str] | str] = (
            DEFAULT_CLOCKIFY_ACCOUNT_STATUSES
        ),
        page_size: int = 100,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page_size = max(1, min(page_size, 1000))
        for page in range(1, max_pages + 1):
            params: dict[str, Any] = {
                "page": page,
                "page-size": page_size,
            }
            if status:
                params["status"] = status
            if account_statuses:
                if isinstance(account_statuses, str):
                    account_statuses_value = account_statuses
                else:
                    account_statuses_value = ",".join(
                        str(value).strip()
                        for value in account_statuses
                        if str(value).strip()
                    )
                if account_statuses_value:
                    params["account-statuses"] = account_statuses_value
            data = self._request(
                "GET",
                f"/workspaces/{self.workspace_id}/users",
                params=params,
            )
            if not isinstance(data, list):
                break
            out.extend(row for row in data if isinstance(row, dict))
            if len(data) < page_size:
                break
        return out

    def find_user_by_email(self, email: str) -> Optional[dict[str, Any]]:
        email_clean = (email or "").strip().lower()
        if not email_clean:
            return None
        data = self._request(
            "GET",
            f"/workspaces/{self.workspace_id}/users",
            params={
                "email": email_clean,
                "status": "ALL",
                "account-statuses": ",".join(DEFAULT_CLOCKIFY_ACCOUNT_STATUSES),
                "page-size": 25,
            },
        )
        if not isinstance(data, list):
            return None
        for row in data:
            if not isinstance(row, dict):
                continue
            if str(row.get("email") or "").strip().lower() == email_clean:
                return row
        return None

    def get_user_time_entries(
        self,
        user_id: str,
        *,
        start_utc: datetime,
        end_utc: datetime,
        page_size: int = 50,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        user_id = (user_id or "").strip()
        if not user_id:
            raise ClockifyConfigError("Clockify user id is not set for this employee.")
        page_size = max(1, min(page_size, 1000))
        out: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            data = self._request(
                "GET",
                f"/workspaces/{self.workspace_id}/user/{user_id}/time-entries",
                params={
                    "start": _to_clockify_iso(start_utc),
                    "page": page,
                    "page-size": page_size,
                },
            )
            if not isinstance(data, list):
                break
            out.extend(row for row in data if isinstance(row, dict))
            if len(data) < page_size:
                break
        now_utc = datetime.now(timezone.utc)
        return [
            row
            for row in out
            if _entry_intersects_range(row, start_utc, end_utc, now_utc)
        ]

    def get_time_entry(self, entry_id: str, *, hydrated: bool = True) -> dict[str, Any]:
        entry_id = (entry_id or "").strip()
        if not entry_id:
            raise ClockifyConfigError("Clockify time entry id is not set.")
        data = self._request(
            "GET",
            f"/workspaces/{self.workspace_id}/time-entries/{entry_id}",
            params={"hydrated": str(bool(hydrated)).lower()},
        )
        return data if isinstance(data, dict) else {}

    def user_week_summary(
        self,
        user_id: str,
        *,
        today: Optional[date] = None,
        settings: Optional[Settings] = None,
    ) -> ClockifyWeekSummary:
        week_start_local, week_end_local = clockify_week_bounds(today, settings=settings)
        entries = self.get_user_time_entries(
            user_id,
            start_utc=week_start_local.astimezone(timezone.utc),
            end_utc=week_end_local.astimezone(timezone.utc),
        )
        return build_week_summary(
            entries,
            week_start_local=week_start_local,
            week_end_local=week_end_local,
            settings=settings,
        )


def clockify_client_from_settings(settings: Optional[Settings] = None) -> ClockifyClient:
    return ClockifyClient.from_settings(settings or get_settings())
