import time
from pathlib import Path

from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel, Session, create_engine

from .config import get_settings

settings = get_settings()


def normalize_database_url(raw_database_url: str) -> str:
    if raw_database_url.startswith("postgres://"):
        return raw_database_url.replace("postgres://", "postgresql+psycopg://", 1)
    if raw_database_url.startswith("postgresql://") and "+psycopg" not in raw_database_url:
        return raw_database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_database_url


database_url = normalize_database_url(settings.database_url)

if database_url.startswith("sqlite:///"):
    db_path = database_url.replace("sqlite:///", "", 1)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}

engine_kwargs = {
    "echo": False,
    "connect_args": connect_args,
}
if database_url.startswith("sqlite"):
    engine_kwargs["pool_recycle"] = -1
else:
    # NullPool avoids reusing stale SSL connections across requests/tasks.
    # pool_pre_ping is intentionally omitted: it is a no-op with NullPool.
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


def init_db() -> None:
    attempts = 1 if database_url.startswith("sqlite") else 5
    delay_seconds = 1.0

    for attempt in range(1, attempts + 1):
        try:
            SQLModel.metadata.create_all(engine)
            break
        except OperationalError:
            if attempt >= attempts:
                raise
            time.sleep(delay_seconds)
            delay_seconds *= 2
    ensure_sqlite_schema()


def get_session():
    with Session(engine) as session:
        yield session


def dispose_engine() -> None:
    # With NullPool this is a no-op; kept for forward-compat if pool strategy changes.
    engine.dispose()
