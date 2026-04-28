"""Small SMS sending adapter for employee portal invite links."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Optional

import httpx

from .config import Settings, get_settings


class SmsSendError(RuntimeError):
    """Raised when an outbound SMS provider rejects or cannot send a message."""


@dataclass(frozen=True)
class SmsSendResult:
    provider: str
    status: str
    message_id: str = ""
    dry_run: bool = False
    error: str = ""

    @property
    def success(self) -> bool:
        return not self.error


def normalize_sms_phone(value: str) -> Optional[str]:
    """Normalize a US-friendly phone string to E.164-ish format for SMS APIs."""
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.startswith("+"):
        digits = re.sub(r"\D", "", raw)
        if 10 <= len(digits) <= 15:
            return f"+{digits}"
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def mask_sms_phone(e164_phone: str) -> str:
    digits = re.sub(r"\D", "", e164_phone or "")
    if len(digits) <= 4:
        return "****"
    return f"***-***-{digits[-4:]}"


def sms_phone_fingerprint(e164_phone: str) -> str:
    normalized = re.sub(r"\D", "", e164_phone or "")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def send_sms(
    *,
    to_phone: str,
    body: str,
    settings: Optional[Settings] = None,
) -> SmsSendResult:
    settings = settings or get_settings()
    provider = (settings.sms_provider or "dry_run").strip().lower()
    if provider in {"", "dryrun", "dry_run", "log", "console"}:
        return SmsSendResult(provider="dry_run", status="dry_run", dry_run=True)
    if provider in {"disabled", "off", "none"}:
        return SmsSendResult(
            provider=provider,
            status="disabled",
            error="SMS_PROVIDER is disabled.",
        )
    if provider != "twilio":
        return SmsSendResult(
            provider=provider,
            status="unsupported_provider",
            error=f"Unsupported SMS_PROVIDER: {provider}",
        )

    account_sid = (settings.sms_twilio_account_sid or "").strip()
    auth_token = (settings.sms_twilio_auth_token or "").strip()
    from_number = (settings.sms_from_number or "").strip()
    messaging_service_sid = (settings.sms_twilio_messaging_service_sid or "").strip()
    if not account_sid or not auth_token:
        return SmsSendResult(
            provider="twilio",
            status="not_configured",
            error="TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN are required.",
        )
    if not from_number and not messaging_service_sid:
        return SmsSendResult(
            provider="twilio",
            status="not_configured",
            error="SMS_FROM_NUMBER or TWILIO_MESSAGING_SERVICE_SID is required.",
        )

    payload = {"To": to_phone, "Body": body}
    if messaging_service_sid:
        payload["MessagingServiceSid"] = messaging_service_sid
    else:
        payload["From"] = from_number
    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    try:
        with httpx.Client(timeout=settings.sms_timeout_seconds) as client:
            response = client.post(url, data=payload, auth=(account_sid, auth_token))
    except httpx.HTTPError as exc:
        return SmsSendResult(
            provider="twilio",
            status="transport_error",
            error=str(exc),
        )

    try:
        data = response.json()
    except ValueError:
        data = {}
    if response.status_code >= 400:
        message = str(data.get("message") or response.text or "Twilio send failed")
        return SmsSendResult(
            provider="twilio",
            status=f"http_{response.status_code}",
            error=message[:240],
        )
    return SmsSendResult(
        provider="twilio",
        status=str(data.get("status") or "queued"),
        message_id=str(data.get("sid") or ""),
    )
