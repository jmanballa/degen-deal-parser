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
LEGACY_SHOPIFY_TABLE = "shopifyorder"
SHOPIFY_TABLE = "shopify_orders"


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
        "last_seen_at": "TIMESTAMP",
        "edited_at": "TIMESTAMP",
        "deleted_at": "TIMESTAMP",
        "is_deleted": "BOOLEAN DEFAULT 0",
        "stitched_group_id": "TEXT",
        "stitched_primary": "BOOLEAN DEFAULT 0",
        "stitched_message_ids_json": "TEXT DEFAULT '[]'",
        "last_stitched_at": "TIMESTAMP",
        "entry_kind": "TEXT",
        "money_in": "REAL",
        "money_out": "REAL",
        "expense_category": "TEXT",
        "reviewed_by": "TEXT",
        "reviewed_at": "TIMESTAMP",
        "active_reparse_run_id": "TEXT",
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
    "reviewcorrection": {
        "pattern_type": "TEXT",
        "parsed_before_json": "TEXT DEFAULT '{}'",
        "corrected_after_json": "TEXT DEFAULT '{}'",
        "field_diffs_json": "TEXT DEFAULT '{}'",
        "features_json": "TEXT DEFAULT '{}'",
    },
    SHOPIFY_TABLE: {
        "customer_name": "TEXT",
        "customer_email": "TEXT",
        "total_tax": "REAL",
        "subtotal_ex_tax": "REAL",
        "financial_status": "TEXT DEFAULT ''",
        "fulfillment_status": "TEXT",
        "line_items_json": "TEXT DEFAULT '[]'",
        "line_items_summary_json": "TEXT DEFAULT '[]'",
        "raw_payload": "TEXT DEFAULT '{}'",
        "source": "TEXT DEFAULT 'webhook'",
        "received_at": "TIMESTAMP",
    },
}


SQLITE_INDEX_MIGRATIONS = [
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_is_enabled ON watchedchannel (is_enabled)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_enabled ON watchedchannel (backfill_enabled)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_after ON watchedchannel (backfill_after)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_before ON watchedchannel (backfill_before)",
    "CREATE INDEX IF NOT EXISTS ix_reviewcorrection_pattern_type ON reviewcorrection (pattern_type)",
    "CREATE INDEX IF NOT EXISTS ix_discordmessage_active_reparse_run_id ON discordmessage (active_reparse_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_discordmessage_last_stitched_at ON discordmessage (last_stitched_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_shopify_orders_shopify_order_id ON shopify_orders (shopify_order_id)",
    "CREATE INDEX IF NOT EXISTS ix_shopify_orders_created_at ON shopify_orders (created_at)",
]


TRANSACTION_PARSE_STATUS_ALIAS_FIXUPS = {
    "needs_review": "review_required",
    "queued": "pending",
    "deleted": "ignored",
}


POSTGRES_ADDITIVE_MIGRATIONS = {
    "reviewcorrection": {
        "pattern_type": "TEXT",
        "parsed_before_json": "TEXT DEFAULT '{}'",
        "corrected_after_json": "TEXT DEFAULT '{}'",
        "field_diffs_json": "TEXT DEFAULT '{}'",
        "features_json": "TEXT DEFAULT '{}'",
    },
    "discordmessage": {
        "last_seen_at": "TIMESTAMP",
        "active_reparse_run_id": "TEXT",
        "deleted_at": "TIMESTAMP",
        "last_stitched_at": "TIMESTAMP",
    },
    SHOPIFY_TABLE: {
        "customer_name": "TEXT",
        "customer_email": "TEXT",
        "total_tax": "DOUBLE PRECISION",
        "subtotal_ex_tax": "DOUBLE PRECISION",
        "financial_status": "TEXT DEFAULT ''",
        "fulfillment_status": "TEXT",
        "line_items_json": "TEXT DEFAULT '[]'",
        "line_items_summary_json": "TEXT DEFAULT '[]'",
        "raw_payload": "TEXT DEFAULT '{}'",
        "source": "TEXT DEFAULT 'webhook'",
        "received_at": "TIMESTAMP",
    },
}


def sqlite_table_exists(connection, table_name: str) -> bool:
    row = connection.execute(
        text("SELECT name FROM sqlite_master WHERE type = 'table' AND name = :table_name"),
        {"table_name": table_name},
    ).first()
    return row is not None


def migrate_legacy_sqlite_shopify_orders(connection) -> None:
    if not sqlite_table_exists(connection, LEGACY_SHOPIFY_TABLE):
        return
    if not sqlite_table_exists(connection, SHOPIFY_TABLE):
        return
    target_count = connection.execute(
        text(f"SELECT COUNT(*) FROM {SHOPIFY_TABLE}")
    ).scalar_one()
    if int(target_count or 0) > 0:
        return

    legacy_rows = connection.execute(
        text(
            f"""
            SELECT
                shopify_order_id,
                order_number,
                created_at,
                updated_at,
                customer_name,
                customer_email,
                total_price,
                subtotal_price,
                financial_status,
                fulfillment_status,
                line_items_json,
                raw_payload,
                source,
                received_at
            FROM {LEGACY_SHOPIFY_TABLE}
            """
        )
    ).mappings().all()

    for row in legacy_rows:
        connection.execute(
            text(
                f"""
                INSERT OR REPLACE INTO {SHOPIFY_TABLE} (
                    id,
                    shopify_order_id,
                    order_number,
                    created_at,
                    updated_at,
                    customer_name,
                    customer_email,
                    total_price,
                    subtotal_price,
                    financial_status,
                    fulfillment_status,
                    line_items_json,
                    raw_payload,
                    source,
                    received_at
                ) VALUES (
                    (SELECT id FROM {SHOPIFY_TABLE} WHERE shopify_order_id = :shopify_order_id),
                    :shopify_order_id,
                    :order_number,
                    :created_at,
                    :updated_at,
                    :customer_name,
                    :customer_email,
                    :total_price,
                    :subtotal_price,
                    :financial_status,
                    :fulfillment_status,
                    :line_items_json,
                    :raw_payload,
                    :source,
                    :received_at
                )
                """
            ),
            dict(row),
        )


def postgres_table_exists(connection, table_name: str) -> bool:
    row = connection.execute(
        text("SELECT to_regclass(:table_name)"),
        {"table_name": table_name},
    ).first()
    return bool(row and row[0])


def migrate_legacy_postgres_shopify_orders(connection) -> None:
    if not postgres_table_exists(connection, LEGACY_SHOPIFY_TABLE):
        return
    if not postgres_table_exists(connection, SHOPIFY_TABLE):
        return
    target_count = connection.execute(
        text(f"SELECT COUNT(*) FROM {SHOPIFY_TABLE}")
    ).scalar_one()
    if int(target_count or 0) > 0:
        return

    connection.execute(
        text(
            f"""
            INSERT INTO {SHOPIFY_TABLE} (
                shopify_order_id,
                order_number,
                created_at,
                updated_at,
                customer_name,
                customer_email,
                total_price,
                subtotal_price,
                financial_status,
                fulfillment_status,
                line_items_json,
                raw_payload,
                source,
                received_at
            )
            SELECT
                shopify_order_id,
                order_number,
                created_at,
                updated_at,
                customer_name,
                customer_email,
                total_price,
                subtotal_price,
                financial_status,
                fulfillment_status,
                line_items_json,
                raw_payload,
                source,
                received_at
            FROM {LEGACY_SHOPIFY_TABLE}
            ON CONFLICT (shopify_order_id) DO UPDATE SET
                order_number = EXCLUDED.order_number,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                customer_name = EXCLUDED.customer_name,
                customer_email = EXCLUDED.customer_email,
                total_price = EXCLUDED.total_price,
                subtotal_price = EXCLUDED.subtotal_price,
                financial_status = EXCLUDED.financial_status,
                fulfillment_status = EXCLUDED.fulfillment_status,
                line_items_json = EXCLUDED.line_items_json,
                raw_payload = EXCLUDED.raw_payload,
                source = EXCLUDED.source,
                received_at = EXCLUDED.received_at
            """
        )
    )


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
        migrate_legacy_sqlite_shopify_orders(connection)


def fixup_transaction_parse_status_aliases() -> None:
    updates = [
        (
            legacy_status,
            canonical_status,
            text(
                'UPDATE "transaction" '
                "SET parse_status = :canonical_status "
                "WHERE parse_status = :legacy_status"
            ),
        )
        for legacy_status, canonical_status in TRANSACTION_PARSE_STATUS_ALIAS_FIXUPS.items()
    ]

    try:
        with engine.begin() as connection:
            for legacy_status, canonical_status, statement in updates:
                connection.execute(
                    statement,
                    {
                        "legacy_status": legacy_status,
                        "canonical_status": canonical_status,
                    },
                )
    except OperationalError as exc:
        if database_url.startswith("sqlite"):
            print(f"[db] transaction parse-status alias fixup skipped because SQLite was busy: {exc}")
            return
        raise


def ensure_postgres_schema() -> None:
    if not is_postgres_database_url(database_url):
        return

    with engine.begin() as connection:
        for table_name, columns in POSTGRES_ADDITIVE_MIGRATIONS.items():
            for column_name, column_type in columns.items():
                connection.execute(
                    text(
                        f"ALTER TABLE {table_name} "
                        f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                    )
                )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_discordmessage_last_stitched_at "
                "ON discordmessage (last_stitched_at)"
            )
        )
        connection.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_shopify_orders_shopify_order_id "
                "ON shopify_orders (shopify_order_id)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_shopify_orders_created_at "
                "ON shopify_orders (created_at)"
            )
        )
        migrate_legacy_postgres_shopify_orders(connection)


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
    ensure_postgres_schema()
    ensure_sqlite_schema()
    fixup_transaction_parse_status_aliases()
    try:
        from .backfill_requests import repair_backfill_request_state_rows

        with Session(engine) as session:
            repair_backfill_request_state_rows(session)
    except Exception as exc:
        print(f"[db] backfill request state repair skipped: {exc}")
    try:
        from .shopify_ingest import repair_shopify_tax_fields
        from .shopify_ingest import repair_shopify_line_item_summaries

        with Session(engine) as session:
            updated = repair_shopify_tax_fields(session)
            if updated:
                print(f"[db] repaired Shopify tax fields for {updated} order rows")
            updated_line_items = repair_shopify_line_item_summaries(session)
            if updated_line_items:
                print(
                    f"[db] repaired Shopify line-item summaries for {updated_line_items} order rows"
                )
    except Exception as exc:
        print(f"[db] Shopify repair skipped: {exc}")


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


def is_sqlite_lock_error(exc: Exception) -> bool:
    return database_url.startswith("sqlite") and "database is locked" in str(exc).lower()


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


def run_write_with_retry(
    operation,
    *,
    attempts: int = 4,
    initial_delay_seconds: float = 0.35,
):
    if not database_url.startswith("sqlite"):
        with managed_session() as session:
            result = operation(session)
            session.commit()
            return result

    delay_seconds = initial_delay_seconds
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            with managed_session() as session:
                result = operation(session)
                session.commit()
                return result
        except OperationalError as exc:
            if not is_sqlite_lock_error(exc) or attempt >= attempts:
                raise
            time.sleep(delay_seconds)
            delay_seconds *= 2
