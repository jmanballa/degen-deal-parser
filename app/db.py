from pathlib import Path

from sqlalchemy import event, text
from sqlmodel import SQLModel, Session, create_engine

from .config import get_settings

settings = get_settings()
database_url = settings.database_url

if database_url.startswith("sqlite:///"):
    db_path = database_url.replace("sqlite:///", "", 1)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}

engine = create_engine(
    database_url,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True,
)


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
    SQLModel.metadata.create_all(engine)
    ensure_sqlite_schema()


def get_session():
    with Session(engine) as session:
        yield session
