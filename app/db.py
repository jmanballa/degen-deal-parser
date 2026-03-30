from contextlib import contextmanager
import threading
import time
from pathlib import Path

import psycopg
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel, Session, create_engine

from .config import get_settings
from . import models as _models  # noqa: F401

settings = get_settings()
_db_failure_state_lock = threading.Lock()
_db_failure_state = {
    "failed_at": 0.0,
}


def normalize_database_url(raw_database_url: str) -> str:
    if raw_database_url.startswith("postgres://"):
        return raw_database_url.replace("postgres://", "postgresql+psycopg://", 1)
    if raw_database_url.startswith("postgresql://") and "+psycopg" not in raw_database_url:
        return raw_database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_database_url


database_url = normalize_database_url(settings.database_url)


def is_postgres_database_url(url: str) -> bool:
    return url.startswith("postgresql+psycopg://")

if database_url.startswith("sqlite:///"):
    db_path = database_url.replace("sqlite:///", "", 1)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {
    "connect_timeout": 15,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
    "application_name": "degen-live-parser",
}

engine_kwargs = {
    "echo": False,
    "connect_args": connect_args,
    "pool_pre_ping": True,
}
if database_url.startswith("sqlite"):
    engine_kwargs["pool_recycle"] = -1
else:
    # Render-hosted Postgres is more reliable here when we avoid reusing
    # long-lived pooled SSL connections from local worker/web processes.
    engine_kwargs["poolclass"] = NullPool

engine = create_engine(database_url, **engine_kwargs)


if database_url.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def configure_sqlite_connection(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute(f"PRAGMA busy_timeout={max(settings.sqlite_busy_timeout_ms, 0)}")
        if settings.sqlite_enable_wal:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()


SQLITE_ADDITIVE_MIGRATIONS = {
    "discordmessage": {
        "guild_id": "TEXT",
        "edited_at": "TIMESTAMP",
        "is_deleted": "BOOLEAN DEFAULT 0",
        "stitched_group_id": "TEXT",
        "stitched_primary": "BOOLEAN DEFAULT 0",
        "stitched_message_ids_json": "TEXT DEFAULT '[]'",
        "entry_kind": "TEXT",
        "money_in": "REAL",
        "money_out": "REAL",
        "expense_category": "TEXT",
        "reviewed_by": "TEXT",
        "reviewed_at": "TIMESTAMP",
    },
    "watchedchannel": {
        "backfill_enabled": "BOOLEAN DEFAULT 1",
        "backfill_after": "TIMESTAMP",
        "backfill_before": "TIMESTAMP",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "bookkeepingentry": {
        "sheet_name": "TEXT",
    },
    "parseattempt": {
        "input_tokens": "INTEGER",
        "cached_input_tokens": "INTEGER",
        "output_tokens": "INTEGER",
        "total_tokens": "INTEGER",
        "estimated_cost_usd": "REAL",
    },
}


SQLITE_INDEX_MIGRATIONS = [
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_is_enabled ON watchedchannel (is_enabled)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_enabled ON watchedchannel (backfill_enabled)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_after ON watchedchannel (backfill_after)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_before ON watchedchannel (backfill_before)",
]


def ensure_sqlite_schema() -> None:
    if not database_url.startswith("sqlite"):
        return

    with engine.begin() as connection:
        for table_name, columns in SQLITE_ADDITIVE_MIGRATIONS.items():
            existing = {
                row[1]
                for row in connection.execute(text(f"PRAGMA table_info({table_name})"))
            }
            for column_name, column_type in columns.items():
                if column_name in existing:
                    continue
                connection.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"ADD COLUMN {column_name} {column_type}"
                    )
                )

        for statement in SQLITE_INDEX_MIGRATIONS:
            connection.execute(text(statement))


def postgres_schema_ready() -> bool:
    if not is_postgres_database_url(database_url):
        return False

    required_tables = set(SQLModel.metadata.tables.keys())
    try:
        with psycopg.connect(settings.database_url, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select table_name
                    from information_schema.tables
                    where table_schema = 'public'
                      and table_name = any(%s)
                    """,
                    (list(required_tables),),
                )
                existing = {row[0] for row in cur.fetchall()}
        return required_tables.issubset(existing)
    except Exception:
        return False


def init_db() -> None:
    attempts = 1 if database_url.startswith("sqlite") else 6
    delay_seconds = 1.0
    last_error: OperationalError | None = None

    for attempt in range(1, attempts + 1):
        try:
            SQLModel.metadata.create_all(engine)
            break
        except OperationalError as exc:
            last_error = exc
            if attempt >= attempts:
                if postgres_schema_ready():
                    print("[db] metadata create_all failed, but existing Postgres schema was detected; continuing startup")
                    break
                raise
            time.sleep(delay_seconds)
            delay_seconds *= 2
    ensure_sqlite_schema()


def get_session():
    with managed_session() as session:
        yield session


def dispose_engine() -> None:
    engine.dispose()


def mark_db_failure() -> None:
    with _db_failure_state_lock:
        _db_failure_state["failed_at"] = time.monotonic()


def clear_db_failure() -> None:
    with _db_failure_state_lock:
        _db_failure_state["failed_at"] = 0.0


def recent_db_failure(window_seconds: float = 8.0) -> bool:
    with _db_failure_state_lock:
        failed_at = float(_db_failure_state["failed_at"])
    return failed_at > 0 and (time.monotonic() - failed_at) < window_seconds


@contextmanager
def managed_session():
    attempts = 1 if database_url.startswith("sqlite") else 5
    delay_seconds = 0.75
    if is_postgres_database_url(database_url) and recent_db_failure():
        raise OperationalError("Database circuit open", None, None)

    for attempt in range(1, attempts + 1):
        session = Session(engine)
        try:
            if not database_url.startswith("sqlite"):
                session.exec(text("SELECT 1"))
            clear_db_failure()
            yield session
            return
        except OperationalError:
            mark_db_failure()
            dispose_engine()
            if attempt >= attempts:
                raise
            time.sleep(delay_seconds)
            delay_seconds *= 2
        finally:
            session.close()
