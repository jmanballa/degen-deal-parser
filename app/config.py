from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB = BASE_DIR / "data" / "degen_live.db"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Degen Live Deal Parser"
    database_url: str = Field(default=f"sqlite:///{DEFAULT_DB.as_posix()}", alias="DATABASE_URL")
    session_secret: str = Field(default="degen-dev-session-secret", alias="SESSION_SECRET")
    public_base_url: str = Field(default="http://127.0.0.1:8000", alias="PUBLIC_BASE_URL")
    session_cookie_name: str = Field(default="degen_session", alias="SESSION_COOKIE_NAME")
    session_https_only: bool = Field(default=False, alias="SESSION_HTTPS_ONLY")
    session_same_site: str = Field(default="lax", alias="SESSION_SAME_SITE")
    session_domain: str = Field(default="", alias="SESSION_DOMAIN")

    discord_bot_token: str = Field(default="", alias="DISCORD_BOT_TOKEN")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    discord_channel_ids: str = Field(default="", alias="DISCORD_CHANNEL_IDS")
    discord_ingest_enabled: bool = Field(default=True, alias="DISCORD_INGEST_ENABLED")
    parser_worker_enabled: bool = Field(default=True, alias="PARSER_WORKER_ENABLED")

    parser_poll_seconds: float = Field(default=2.0, alias="PARSER_POLL_SECONDS")
    parser_batch_size: int = Field(default=10, alias="PARSER_BATCH_SIZE")
    parser_max_attempts: int = Field(default=3, alias="PARSER_MAX_ATTEMPTS")

    startup_backfill_enabled: bool = Field(default=True, alias="STARTUP_BACKFILL_ENABLED")
    startup_backfill_limit_per_channel: int = Field(default=500, alias="STARTUP_BACKFILL_LIMIT_PER_CHANNEL")
    startup_backfill_oldest_first: bool = Field(default=True, alias="STARTUP_BACKFILL_OLDEST_FIRST")

    stitch_enabled: bool = Field(default=True, alias="STITCH_ENABLED")
    stitch_window_seconds: int = Field(default=30, alias="STITCH_WINDOW_SECONDS")
    stitch_max_messages: int = Field(default=3, alias="STITCH_MAX_MESSAGES")

    sqlite_busy_timeout_ms: int = Field(default=5000, alias="SQLITE_BUSY_TIMEOUT_MS")
    sqlite_enable_wal: bool = Field(default=True, alias="SQLITE_ENABLE_WAL")
    admin_username: str = Field(default="admin", alias="ADMIN_USERNAME")
    admin_password: str = Field(default="degen1234", alias="ADMIN_PASSWORD")
    admin_display_name: str = Field(default="Admin", alias="ADMIN_DISPLAY_NAME")
    reviewer_username: str = Field(default="", alias="REVIEWER_USERNAME")
    reviewer_password: str = Field(default="", alias="REVIEWER_PASSWORD")
    reviewer_display_name: str = Field(default="Reviewer", alias="REVIEWER_DISPLAY_NAME")
    auth_reseed_passwords: bool = Field(default=False, alias="AUTH_RESEED_PASSWORDS")
    runtime_name: str = Field(default="local_ingest", alias="RUNTIME_NAME")
    runtime_label: str = Field(default="Ingest Worker", alias="RUNTIME_LABEL")

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


@lru_cache
def get_settings() -> Settings:
    return Settings()
