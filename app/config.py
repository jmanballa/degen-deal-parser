import ipaddress

from functools import lru_cache
from pathlib import Path
from typing import List
from urllib.parse import urlparse

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB = BASE_DIR / "data" / "degen_live.db"
DEFAULT_SESSION_SECRET = "degen-dev-session-secret"
DEFAULT_ADMIN_PASSWORD = "degen1234"


def _resolve_root(value: str, fallback_subdir: str) -> Path:
    """Resolve a configured root: absolute paths used as-is, relative paths
    joined to BASE_DIR. Empty value falls back to BASE_DIR / fallback_subdir."""
    raw = (value or "").strip()
    if not raw:
        return BASE_DIR / fallback_subdir
    path = Path(raw)
    return path if path.is_absolute() else (BASE_DIR / path)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Degen Live Deal Parser"
    database_url: str = Field(default=f"sqlite:///{DEFAULT_DB.as_posix()}", alias="DATABASE_URL")
    session_secret: str = Field(default=DEFAULT_SESSION_SECRET, alias="SESSION_SECRET")
    public_base_url: str = Field(default="http://127.0.0.1:8000", alias="PUBLIC_BASE_URL")
    session_cookie_name: str = Field(default="degen_session", alias="SESSION_COOKIE_NAME")
    session_https_only: bool = Field(default=True, alias="SESSION_HTTPS_ONLY")
    session_same_site: str = Field(default="strict", alias="SESSION_SAME_SITE")
    session_domain: str = Field(default="", alias="SESSION_DOMAIN")
    # 30 days — keeps iOS PWA launches logged in across Safari cookie sweeps.
    session_max_age_seconds: int = Field(default=30 * 24 * 60 * 60, alias="SESSION_MAX_AGE_SECONDS")
    log_to_file: bool = Field(default=True, alias="LOG_TO_FILE")
    log_dir: str = Field(default="logs", alias="LOG_DIR")

    # Durable runtime data root. Defaults to repo-relative `data/` for local
    # dev; Green/Linux production should set DATA_ROOT=/opt/degen/data so
    # uploaded attachments, hit images, and v2 training captures persist
    # outside the app checkout. MEDIA_ROOT optionally splits user-uploaded
    # media (attachments, hit images) from operational data; when unset it
    # falls back to DATA_ROOT.
    data_root: str = Field(default="data", alias="DATA_ROOT")
    media_root: str = Field(default="", alias="MEDIA_ROOT")

    discord_bot_token: str = Field(default="", alias="DISCORD_BOT_TOKEN")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    ai_provider: str = Field(default="nvidia", alias="AI_PROVIDER")
    nvidia_api_key: str = Field(default="", alias="NVIDIA_API_KEY")
    # inference-api.nvidia.com is the documented OpenAI-compatible endpoint for
    # the NVIDIA catalog — required for multimodal (image) chat completions.
    # integrate.api.nvidia.com accepts text-only chat but 404s on multimodal.
    nvidia_base_url: str = Field(default="https://inference-api.nvidia.com/v1", alias="NVIDIA_BASE_URL")
    nvidia_model: str = Field(default="aws/anthropic/bedrock-claude-opus-4-7", alias="NVIDIA_MODEL")
    nvidia_fast_model: str = Field(default="aws/anthropic/claude-haiku-4-5-v1", alias="NVIDIA_FAST_MODEL")
    # Tiebreaker is only invoked when Ximilar and the vision model disagree on a
    # LOW/MEDIUM-confidence scan, so there is no per-scan cost when scans go
    # smoothly. Shares the NVIDIA endpoint; no separate API key required.
    nvidia_tiebreaker_model: str = Field(default="gcp/google/gemini-3.1-pro-preview", alias="NVIDIA_TIEBREAKER_MODEL")
    # Used by Balanced scanner mode — runs in parallel with Haiku (via
    # NVIDIA_FAST_MODEL) for a 3-way majority vote on non-HIGH Ximilar scans.
    nvidia_gemini_flash_model: str = Field(default="gcp/google/gemini-3-flash-preview", alias="NVIDIA_GEMINI_FLASH_MODEL")
    discord_channel_ids: str = Field(default="", alias="DISCORD_CHANNEL_IDS")
    discord_ingest_enabled: bool = Field(default=True, alias="DISCORD_INGEST_ENABLED")
    parser_worker_enabled: bool = Field(default=True, alias="PARSER_WORKER_ENABLED")
    disable_external_warmups: bool = Field(default=False, alias="DISABLE_EXTERNAL_WARMUPS")

    parser_poll_seconds: float = Field(default=2.0, alias="PARSER_POLL_SECONDS")
    parser_batch_size: int = Field(default=10, alias="PARSER_BATCH_SIZE")
    parser_max_attempts: int = Field(default=3, alias="PARSER_MAX_ATTEMPTS")
    parser_reprocess_enabled: bool = Field(default=True, alias="PARSER_REPROCESS_ENABLED")
    parser_reprocess_interval_hours: float = Field(default=4.0, alias="PARSER_REPROCESS_INTERVAL_HOURS")
    parser_reprocess_batch_size: int = Field(default=20, alias="PARSER_REPROCESS_BATCH_SIZE")
    parser_reprocess_min_age_minutes: int = Field(default=15, alias="PARSER_REPROCESS_MIN_AGE_MINUTES")
    parser_reprocess_lookback_days: int = Field(default=14, alias="PARSER_REPROCESS_LOOKBACK_DAYS")

    startup_backfill_enabled: bool = Field(default=True, alias="STARTUP_BACKFILL_ENABLED")
    startup_backfill_limit_per_channel: int = Field(default=500, alias="STARTUP_BACKFILL_LIMIT_PER_CHANNEL")
    startup_backfill_oldest_first: bool = Field(default=True, alias="STARTUP_BACKFILL_OLDEST_FIRST")
    startup_backfill_lookback_hours: float = Field(default=24.0, alias="STARTUP_BACKFILL_LOOKBACK_HOURS")
    startup_offline_audit_enabled: bool = Field(default=True, alias="STARTUP_OFFLINE_AUDIT_ENABLED")
    startup_offline_audit_limit_per_channel: int = Field(
        default=250,
        alias="STARTUP_OFFLINE_AUDIT_LIMIT_PER_CHANNEL",
    )
    startup_offline_audit_oldest_first: bool = Field(
        default=True,
        alias="STARTUP_OFFLINE_AUDIT_OLDEST_FIRST",
    )
    startup_offline_audit_lookback_hours: float = Field(
        default=24.0,
        alias="STARTUP_OFFLINE_AUDIT_LOOKBACK_HOURS",
    )
    periodic_offline_audit_enabled: bool = Field(default=True, alias="PERIODIC_OFFLINE_AUDIT_ENABLED")
    periodic_offline_audit_interval_minutes: float = Field(
        default=30.0,
        alias="PERIODIC_OFFLINE_AUDIT_INTERVAL_MINUTES",
    )
    periodic_offline_audit_limit_per_channel: int = Field(
        default=75,
        alias="PERIODIC_OFFLINE_AUDIT_LIMIT_PER_CHANNEL",
    )
    periodic_offline_audit_lookback_hours: float = Field(
        default=24.0,
        alias="PERIODIC_OFFLINE_AUDIT_LOOKBACK_HOURS",
    )
    periodic_stitch_audit_enabled: bool = Field(default=True, alias="PERIODIC_STITCH_AUDIT_ENABLED")
    periodic_stitch_audit_interval_minutes: float = Field(
        default=45.0,
        alias="PERIODIC_STITCH_AUDIT_INTERVAL_MINUTES",
    )
    periodic_stitch_audit_limit: int = Field(default=50, alias="PERIODIC_STITCH_AUDIT_LIMIT")
    periodic_stitch_audit_lookback_hours: float = Field(
        default=24.0,
        alias="PERIODIC_STITCH_AUDIT_LOOKBACK_HOURS",
    )
    periodic_stitch_audit_min_age_minutes: int = Field(
        default=10,
        alias="PERIODIC_STITCH_AUDIT_MIN_AGE_MINUTES",
    )
    periodic_attachment_repair_enabled: bool = Field(
        default=True,
        alias="PERIODIC_ATTACHMENT_REPAIR_ENABLED",
    )
    periodic_attachment_repair_interval_minutes: float = Field(
        default=60.0,
        alias="PERIODIC_ATTACHMENT_REPAIR_INTERVAL_MINUTES",
    )
    periodic_attachment_repair_limit: int = Field(
        default=50,
        alias="PERIODIC_ATTACHMENT_REPAIR_LIMIT",
    )
    periodic_attachment_repair_lookback_hours: float = Field(
        default=24.0,
        alias="PERIODIC_ATTACHMENT_REPAIR_LOOKBACK_HOURS",
    )
    periodic_attachment_repair_min_age_minutes: int = Field(
        default=10,
        alias="PERIODIC_ATTACHMENT_REPAIR_MIN_AGE_MINUTES",
    )

    auto_promote_enabled: bool = Field(default=True, alias="AUTO_PROMOTE_ENABLED")
    auto_promote_min_count: int = Field(default=5, alias="AUTO_PROMOTE_MIN_COUNT")
    auto_promote_min_confidence: float = Field(default=0.85, alias="AUTO_PROMOTE_MIN_CONFIDENCE")
    auto_promote_interval_minutes: float = Field(default=30.0, alias="AUTO_PROMOTE_INTERVAL_MINUTES")

    # AI review-resolver agent: a background loop that scans
    # review_required rows and asks a heavy model to resolve them using
    # author history, nearby siblings, and prior corrections as context.
    ai_resolver_enabled: bool = Field(default=True, alias="AI_RESOLVER_ENABLED")
    ai_resolver_interval_minutes: float = Field(default=10.0, alias="AI_RESOLVER_INTERVAL_MINUTES")
    ai_resolver_batch_size: int = Field(default=25, alias="AI_RESOLVER_BATCH_SIZE")
    ai_resolver_min_age_minutes: int = Field(default=5, alias="AI_RESOLVER_MIN_AGE_MINUTES")
    ai_resolver_auto_confidence: float = Field(default=0.95, alias="AI_RESOLVER_AUTO_CONFIDENCE")
    ai_resolver_max_context_messages: int = Field(default=10, alias="AI_RESOLVER_MAX_CONTEXT_MESSAGES")
    ai_resolver_max_correction_hints: int = Field(default=5, alias="AI_RESOLVER_MAX_CORRECTION_HINTS")

    tiktok_token_refresh_enabled: bool = Field(default=True, alias="TIKTOK_TOKEN_REFRESH_ENABLED")
    tiktok_token_refresh_interval_minutes: float = Field(
        default=30.0,
        alias="TIKTOK_TOKEN_REFRESH_INTERVAL_MINUTES",
    )

    stitch_enabled: bool = Field(default=True, alias="STITCH_ENABLED")
    stitch_window_seconds: int = Field(default=30, alias="STITCH_WINDOW_SECONDS")
    stitch_max_messages: int = Field(default=3, alias="STITCH_MAX_MESSAGES")

    sqlite_busy_timeout_ms: int = Field(default=15000, alias="SQLITE_BUSY_TIMEOUT_MS")
    sqlite_enable_wal: bool = Field(default=True, alias="SQLITE_ENABLE_WAL")
    admin_username: str = Field(default="admin", alias="ADMIN_USERNAME")
    admin_password: str = Field(default=DEFAULT_ADMIN_PASSWORD, alias="ADMIN_PASSWORD")
    admin_display_name: str = Field(default="Admin", alias="ADMIN_DISPLAY_NAME")
    reviewer_username: str = Field(default="", alias="REVIEWER_USERNAME")
    reviewer_password: str = Field(default="", alias="REVIEWER_PASSWORD")
    reviewer_display_name: str = Field(default="Reviewer", alias="REVIEWER_DISPLAY_NAME")
    viewer_accounts: str = Field(default="", alias="VIEWER_ACCOUNTS")
    auth_reseed_passwords: bool = Field(default=False, alias="AUTH_RESEED_PASSWORDS")
    runtime_name: str = Field(default="local_ingest", alias="RUNTIME_NAME")
    runtime_label: str = Field(default="Ingest Worker", alias="RUNTIME_LABEL")
    worker_runtime_name: str = Field(default="", alias="WORKER_RUNTIME_NAME")
    worker_runtime_label: str = Field(default="Ingest Worker", alias="WORKER_RUNTIME_LABEL")
    shopify_webhook_secret: str = Field(default="", alias="SHOPIFY_WEBHOOK_SECRET")
    shopify_api_key: str = Field(default="", alias="SHOPIFY_API_KEY")
    shopify_store_domain: str = Field(default="", alias="SHOPIFY_STORE_DOMAIN")
    tiktok_app_key: str = Field(default="", alias="TIKTOK_APP_KEY")
    tiktok_app_secret: str = Field(default="", alias="TIKTOK_APP_SECRET")
    tiktok_redirect_uri: str = Field(default="", alias="TIKTOK_REDIRECT_URI")
    tiktok_shop_id: str = Field(default="", alias="TIKTOK_SHOP_ID")
    tiktok_shop_cipher: str = Field(default="", alias="TIKTOK_SHOP_CIPHER")
    tiktok_access_token: str = Field(default="", alias="TIKTOK_ACCESS_TOKEN")
    tiktok_refresh_token: str = Field(default="", alias="TIKTOK_REFRESH_TOKEN")
    tiktok_api_base_url: str = Field(default="https://open.tiktokapis.com", alias="TIKTOK_BASE_URL")
    tiktok_shop_api_base_url: str = Field(default="", alias="TIKTOK_SHOP_API_BASE_URL")
    tiktok_sync_enabled: bool = Field(default=True, alias="TIKTOK_SYNC_ENABLED")
    tiktok_sync_interval_minutes: int = Field(default=15, alias="TIKTOK_SYNC_INTERVAL_MINUTES")
    tiktok_sync_lookback_hours: float = Field(default=24.0, alias="TIKTOK_SYNC_LOOKBACK_HOURS")
    tiktok_sync_limit: int = Field(default=100, alias="TIKTOK_SYNC_LIMIT")
    tiktok_startup_backfill_days: int = Field(default=30, alias="TIKTOK_STARTUP_BACKFILL_DAYS")
    tiktok_live_api_key: str = Field(default="", alias="TIKTOK_LIVE_API_KEY")
    tiktok_live_username: str = Field(default="", alias="TIKTOK_LIVE_USERNAME")

    # Optional external username-resolver service (scripts/scraper_server.py
    # running on OpenClaw or another Linux host). When base_url + api_key are
    # set the app will attempt to resolve buyer @usernames from order IDs.
    username_scraper_base_url: str = Field(default="", alias="USERNAME_SCRAPER_BASE_URL")
    username_scraper_api_key: str = Field(default="", alias="USERNAME_SCRAPER_API_KEY")
    username_scraper_timeout_seconds: float = Field(default=8.0, alias="USERNAME_SCRAPER_TIMEOUT_SECONDS")

    # Inventory
    shopify_access_token: str = Field(default="", alias="SHOPIFY_ACCESS_TOKEN")
    scrydex_api_key: str = Field(default="", alias="SCRYDEX_API_KEY")
    scrydex_base_url: str = Field(default="https://api.scrydex.io", alias="SCRYDEX_BASE_URL")
    inventory_auto_price_enabled: bool = Field(default=True, alias="INVENTORY_AUTO_PRICE_ENABLED")
    inventory_auto_shopify_push: bool = Field(default=False, alias="INVENTORY_AUTO_SHOPIFY_PUSH")
    inventory_price_refresh_interval_hours: float = Field(default=6.0, alias="INVENTORY_PRICE_REFRESH_INTERVAL_HOURS")
    inventory_price_stale_hours: float = Field(default=24.0, alias="INVENTORY_PRICE_STALE_HOURS")
    # Card scanning
    ximilar_api_token: str = Field(default="", alias="XIMILAR_API_TOKEN")
    psa_api_key: str = Field(default="", alias="PSA_API_KEY")
    pokemon_tcg_api_key: str = Field(default="", alias="POKEMON_TCG_API_KEY")
    degen_eye_v2_capture_enabled: bool = Field(default=True, alias="DEGEN_EYE_V2_CAPTURE_ENABLED")
    degen_eye_v2_capture_dir: str = Field(default="data/v2_training_scans", alias="DEGEN_EYE_V2_CAPTURE_DIR")
    degen_eye_v2_index_path: str = Field(default="data/phash_index.sqlite", alias="DEGEN_EYE_V2_INDEX_PATH")

    # Firecrawl (web scraping)
    firecrawl_api_key: str = Field(default="", alias="FIRECRAWL_API_KEY")

    # Debug: write webhook capture files to logs/ on signature mismatch
    debug_webhook_capture: bool = Field(default=False, alias="DEBUG_WEBHOOK_CAPTURE")

    # Employee portal (Phase 1)
    employee_portal_enabled: bool = Field(default=False, alias="EMPLOYEE_PORTAL_ENABLED")
    employee_pii_key: str = Field(default="", alias="EMPLOYEE_PII_KEY")
    employee_email_hash_salt: str = Field(default="", alias="EMPLOYEE_EMAIL_HASH_SALT")
    employee_token_hmac_key: str = Field(default="", alias="EMPLOYEE_TOKEN_HMAC_KEY")
    sms_provider: str = Field(default="dry_run", alias="SMS_PROVIDER")
    sms_from_number: str = Field(default="", alias="SMS_FROM_NUMBER")
    sms_twilio_account_sid: str = Field(default="", alias="TWILIO_ACCOUNT_SID")
    sms_twilio_auth_token: str = Field(default="", alias="TWILIO_AUTH_TOKEN")
    sms_twilio_messaging_service_sid: str = Field(default="", alias="TWILIO_MESSAGING_SERVICE_SID")
    sms_timeout_seconds: float = Field(default=10.0, alias="SMS_TIMEOUT_SECONDS")
    trust_x_forwarded_for: bool = Field(default=False, alias="TRUST_X_FORWARDED_FOR")
    trusted_proxy_ips: str = Field(default="127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16", alias="TRUSTED_PROXY_IPS")
    clockify_api_key: str = Field(default="", alias="CLOCKIFY_API_KEY")
    clockify_workspace_id: str = Field(default="", alias="CLOCKIFY_WORKSPACE_ID")
    clockify_base_url: str = Field(default="https://api.clockify.me/api/v1", alias="CLOCKIFY_BASE_URL")
    clockify_timezone: str = Field(default="America/Los_Angeles", alias="CLOCKIFY_TIMEZONE")
    clockify_timeout_seconds: float = Field(default=12.0, alias="CLOCKIFY_TIMEOUT_SECONDS")
    session_hours: int = Field(default=8, alias="SESSION_HOURS")

    @property
    def data_root_path(self) -> Path:
        return _resolve_root(self.data_root, "data")

    @property
    def media_root_path(self) -> Path:
        # Empty MEDIA_ROOT inherits DATA_ROOT so a single env var is enough
        # for the common case (Green sets DATA_ROOT=/opt/degen/data).
        if (self.media_root or "").strip():
            return _resolve_root(self.media_root, "data")
        return self.data_root_path

    def data_path(self, *parts: str) -> Path:
        return self.data_root_path.joinpath(*parts)

    def media_path(self, *parts: str) -> Path:
        return self.media_root_path.joinpath(*parts)

    @property
    def channel_ids(self) -> List[int]:
        channel_ids: list[int] = []
        seen: set[int] = set()

        for raw_value in self.discord_channel_ids.split(","):
            cleaned = raw_value.strip()
            if not cleaned:
                continue
            if not cleaned.isdigit():
                print(f"[config] ignoring invalid DISCORD_CHANNEL_IDS value: {cleaned!r}")
                continue

            channel_id = int(cleaned)
            if channel_id in seen:
                continue

            seen.add(channel_id)
            channel_ids.append(channel_id)

        return channel_ids

    @property
    def effective_session_domain(self) -> str:
        raw = (self.session_domain or "").strip().lower()
        if not raw or raw == "none":
            return ""
        return self.session_domain.strip()

    @property
    def public_host_mode(self) -> bool:
        parsed = urlparse(self.public_base_url or "")
        hostname = (parsed.hostname or "").lower()
        is_local_host = hostname in {"", "127.0.0.1", "localhost"}
        return bool(
            self.session_https_only
            or self.effective_session_domain
            or (hostname and not is_local_host)
        )

    @property
    def effective_worker_runtime_name(self) -> str:
        explicit_name = (self.worker_runtime_name or "").strip()
        if explicit_name:
            return explicit_name
        if self.runtime_name.endswith("_web"):
            return f"{self.runtime_name.removesuffix('_web')}_worker"
        return self.runtime_name

    @property
    def effective_worker_runtime_label(self) -> str:
        return (self.worker_runtime_label or "").strip() or "Ingest Worker"

    @property
    def trusted_proxy_networks(self) -> list[ipaddress._BaseNetwork]:
        networks: list[ipaddress._BaseNetwork] = []
        for raw_value in self.trusted_proxy_ips.split(","):
            cleaned = raw_value.strip()
            if not cleaned:
                continue
            try:
                if "/" in cleaned:
                    network = ipaddress.ip_network(cleaned, strict=False)
                else:
                    address = ipaddress.ip_address(cleaned)
                    bits = 32 if address.version == 4 else 128
                    network = ipaddress.ip_network(f"{cleaned}/{bits}", strict=False)
            except ValueError:
                continue
            networks.append(network)
        return networks

    def is_trusted_proxy(self, host: str) -> bool:
        try:
            address = ipaddress.ip_address((host or "").strip())
        except ValueError:
            return False
        return any(address in network for network in self.trusted_proxy_networks)

    def validate_runtime_secrets(self) -> None:
        requires_hardened_runtime = self.public_host_mode or self.employee_portal_enabled
        if not requires_hardened_runtime:
            return

        insecure_fields: list[str] = []
        if not self.session_secret or self.session_secret == DEFAULT_SESSION_SECRET:
            insecure_fields.append("SESSION_SECRET")
        if not self.admin_password or self.admin_password == DEFAULT_ADMIN_PASSWORD:
            insecure_fields.append("ADMIN_PASSWORD")
        if self.employee_portal_enabled:
            if not self.employee_token_hmac_key:
                insecure_fields.append("EMPLOYEE_TOKEN_HMAC_KEY")
            elif self.employee_token_hmac_key == self.session_secret:
                insecure_fields.append("EMPLOYEE_TOKEN_HMAC_KEY (must differ from SESSION_SECRET)")

        if insecure_fields:
            fields_text = ", ".join(insecure_fields)
            mode_label = "employee portal" if self.employee_portal_enabled else "public host mode"
            raise RuntimeError(
                f"Insecure configuration for {mode_label}: set real values for {fields_text} in .env before booting."
            )


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.validate_runtime_secrets()
    return settings
