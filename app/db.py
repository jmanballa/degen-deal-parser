from contextlib import contextmanager
import threading
import time
from pathlib import Path

import psycopg
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool, QueuePool
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
TIKTOK_AUTH_TABLE = "tiktok_auth"
TIKTOK_ORDERS_TABLE = "tiktok_orders"


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
    "options": "-c timezone=UTC -c statement_timeout=120000",
}

if database_url.startswith("sqlite"):
    _poolclass = NullPool
    _pool_kwargs: dict = {}
else:
    _poolclass = QueuePool
    _pool_kwargs = {"pool_size": 5, "max_overflow": 10, "pool_timeout": 30, "pool_recycle": 600}

engine_kwargs = {
    "echo": False,
    "connect_args": connect_args,
    "pool_pre_ping": True,
    "poolclass": _poolclass,
    **_pool_kwargs,
}

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
        "parse_disagreement_json": "TEXT",
        "ai_resolver_reasoning_json": "TEXT",
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
    TIKTOK_AUTH_TABLE: {
        "shop_cipher": "TEXT",
        "seller_id": "TEXT",
        "open_id": "TEXT",
        "shop_name": "TEXT",
        "shop_region": "TEXT",
        "seller_name": "TEXT",
        "app_key": "TEXT",
        "redirect_uri": "TEXT",
        "access_token": "TEXT",
        "refresh_token": "TEXT",
        "access_token_expires_at": "TIMESTAMP",
        "refresh_token_expires_at": "TIMESTAMP",
        "scopes_json": "TEXT DEFAULT '[]'",
        "raw_payload": "TEXT DEFAULT '{}'",
        "source": "TEXT DEFAULT 'oauth'",
        "received_at": "TIMESTAMP",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
        "creator_access_token": "TEXT",
        "creator_refresh_token": "TEXT",
        "creator_token_expires_at": "TIMESTAMP",
    },
    TIKTOK_ORDERS_TABLE: {
        "shop_id": "TEXT",
        "shop_cipher": "TEXT",
        "seller_id": "TEXT",
        "customer_name": "TEXT",
        "customer_email": "TEXT",
        "total_tax": "REAL",
        "subtotal_ex_tax": "REAL",
        "financial_status": "TEXT DEFAULT ''",
        "fulfillment_status": "TEXT",
        "order_status": "TEXT",
        "currency": "TEXT",
        "line_items_json": "TEXT DEFAULT '[]'",
        "line_items_summary_json": "TEXT DEFAULT '[]'",
        "raw_payload": "TEXT DEFAULT '{}'",
        "source": "TEXT DEFAULT 'webhook'",
        "received_at": "TIMESTAMP",
    },
    "live_hits": {
        "order_value": "REAL",
        "image_filename": "TEXT",
    },
    "stream_schedules": {
        "stream_account_id": "INTEGER",
        "is_overnight": "BOOLEAN DEFAULT 0",
    },
    "user": {
        "password_salt": "TEXT",
        "is_schedulable": "BOOLEAN DEFAULT 0",
        "staff_kind": "TEXT DEFAULT 'storefront'",
        "password_changed_at": "TIMESTAMP",
    },
    "streamers": {
        "user_id": "INTEGER",
    },
    "shift_entry": {
        # Multi-shift-per-day support: sort_order decides the vertical
        # stacking order of shifts that share the same (user_id,
        # shift_date). Legacy single-shift rows default to 0. The
        # matching UNIQUE constraint drop for SQLite happens in
        # ensure_sqlite_schema() via a table rebuild.
        "sort_order": "INTEGER DEFAULT 0",
    },
    "invitetoken": {
        "token_lookup_hmac": "BLOB",
        "target_user_id": "INTEGER",
    },
    "passwordresettoken": {
        "token_lookup_hmac": "BLOB",
    },
    "employeeprofile": {
        "compensation_type": "TEXT DEFAULT 'hourly'",
        "monthly_salary_cents_enc": "BLOB",
        "monthly_salary_pay_day": "INTEGER",
        "payment_method": "TEXT DEFAULT 'cash'",
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
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_tiktok_auth_tiktok_shop_id ON tiktok_auth (tiktok_shop_id)",
    "CREATE INDEX IF NOT EXISTS ix_tiktok_auth_app_key ON tiktok_auth (app_key)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_tiktok_orders_tiktok_order_id ON tiktok_orders (tiktok_order_id)",
    "CREATE INDEX IF NOT EXISTS ix_tiktok_orders_created_at ON tiktok_orders (created_at)",
    # Indexed columns added via SQLITE_ADDITIVE_MIGRATIONS (create_all does not backfill indexes on ALTER-only DBs)
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_guild_id ON discordmessage (guild_id)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_last_seen_at ON discordmessage (last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_deleted_at ON discordmessage (deleted_at)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_is_deleted ON discordmessage (is_deleted)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_stitched_group_id ON discordmessage (stitched_group_id)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_stitched_primary ON discordmessage (stitched_primary)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_entry_kind ON discordmessage (entry_kind)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_expense_category ON discordmessage (expense_category)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_reviewed_by ON discordmessage (reviewed_by)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_reviewed_at ON discordmessage (reviewed_at)",
    "CREATE INDEX IF NOT EXISTS idx_watchedchannel_created_at ON watchedchannel (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_watchedchannel_updated_at ON watchedchannel (updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_bookkeepingentry_sheet_name ON bookkeepingentry (sheet_name)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_customer_name ON shopify_orders (customer_name)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_customer_email ON shopify_orders (customer_email)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_financial_status ON shopify_orders (financial_status)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_fulfillment_status ON shopify_orders (fulfillment_status)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_source ON shopify_orders (source)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_received_at ON shopify_orders (received_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_shop_id ON tiktok_orders (shop_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_shop_cipher ON tiktok_orders (shop_cipher)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_seller_id ON tiktok_orders (seller_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_customer_name ON tiktok_orders (customer_name)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_customer_email ON tiktok_orders (customer_email)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_financial_status ON tiktok_orders (financial_status)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_fulfillment_status ON tiktok_orders (fulfillment_status)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_order_status ON tiktok_orders (order_status)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_currency ON tiktok_orders (currency)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_source ON tiktok_orders (source)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_received_at ON tiktok_orders (received_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_shop_cipher ON tiktok_auth (shop_cipher)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_seller_id ON tiktok_auth (seller_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_open_id ON tiktok_auth (open_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_shop_name ON tiktok_auth (shop_name)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_shop_region ON tiktok_auth (shop_region)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_seller_name ON tiktok_auth (seller_name)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_access_token_expires_at ON tiktok_auth (access_token_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_refresh_token_expires_at ON tiktok_auth (refresh_token_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_source ON tiktok_auth (source)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_received_at ON tiktok_auth (received_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_created_at ON tiktok_auth (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_updated_at ON tiktok_auth (updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_stream_schedules_stream_account_id ON stream_schedules (stream_account_id)",
    "CREATE INDEX IF NOT EXISTS idx_user_is_schedulable ON user (is_schedulable)",
    "CREATE INDEX IF NOT EXISTS idx_user_staff_kind ON user (staff_kind)",
    "CREATE INDEX IF NOT EXISTS idx_streamers_user_id ON streamers (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_invitetoken_token_lookup_hmac ON invitetoken (token_lookup_hmac)",
    "CREATE INDEX IF NOT EXISTS idx_invitetoken_target_user_id ON invitetoken (target_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_passwordresettoken_token_lookup_hmac ON passwordresettoken (token_lookup_hmac)",
    "CREATE INDEX IF NOT EXISTS idx_employeeprofile_compensation_type ON employeeprofile (compensation_type)",
    "CREATE INDEX IF NOT EXISTS idx_employeeprofile_monthly_salary_pay_day ON employeeprofile (monthly_salary_pay_day)",
    "CREATE INDEX IF NOT EXISTS idx_employeeprofile_payment_method ON employeeprofile (payment_method)",
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
        "guild_id": "TEXT",
        "last_seen_at": "TIMESTAMP",
        "edited_at": "TIMESTAMP",
        "deleted_at": "TIMESTAMP",
        "is_deleted": "BOOLEAN DEFAULT FALSE",
        "stitched_group_id": "TEXT",
        "stitched_primary": "BOOLEAN DEFAULT FALSE",
        "stitched_message_ids_json": "TEXT DEFAULT '[]'",
        "last_stitched_at": "TIMESTAMP",
        "entry_kind": "TEXT",
        "money_in": "DOUBLE PRECISION",
        "money_out": "DOUBLE PRECISION",
        "expense_category": "TEXT",
        "reviewed_by": "TEXT",
        "reviewed_at": "TIMESTAMP",
        "active_reparse_run_id": "TEXT",
        "parse_disagreement_json": "TEXT",
        "ai_resolver_reasoning_json": "TEXT",
    },
    "watchedchannel": {
        "backfill_enabled": "BOOLEAN DEFAULT TRUE",
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
        "estimated_cost_usd": "DOUBLE PRECISION",
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
    TIKTOK_AUTH_TABLE: {
        "shop_cipher": "TEXT",
        "seller_id": "TEXT",
        "open_id": "TEXT",
        "shop_name": "TEXT",
        "shop_region": "TEXT",
        "seller_name": "TEXT",
        "app_key": "TEXT",
        "redirect_uri": "TEXT",
        "access_token": "TEXT",
        "refresh_token": "TEXT",
        "access_token_expires_at": "TIMESTAMP",
        "refresh_token_expires_at": "TIMESTAMP",
        "scopes_json": "TEXT DEFAULT '[]'",
        "raw_payload": "TEXT DEFAULT '{}'",
        "source": "TEXT DEFAULT 'oauth'",
        "received_at": "TIMESTAMP",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
        "creator_access_token": "TEXT",
        "creator_refresh_token": "TEXT",
        "creator_token_expires_at": "TIMESTAMP",
    },
    TIKTOK_ORDERS_TABLE: {
        "shop_id": "TEXT",
        "shop_cipher": "TEXT",
        "seller_id": "TEXT",
        "customer_name": "TEXT",
        "customer_email": "TEXT",
        "total_tax": "DOUBLE PRECISION",
        "subtotal_ex_tax": "DOUBLE PRECISION",
        "financial_status": "TEXT DEFAULT ''",
        "fulfillment_status": "TEXT",
        "order_status": "TEXT",
        "currency": "TEXT",
        "line_items_json": "TEXT DEFAULT '[]'",
        "line_items_summary_json": "TEXT DEFAULT '[]'",
        "raw_payload": "TEXT DEFAULT '{}'",
        "source": "TEXT DEFAULT 'webhook'",
        "received_at": "TIMESTAMP",
    },
    "live_hits": {
        "order_value": "DOUBLE PRECISION",
        "image_filename": "TEXT",
    },
    "stream_schedules": {
        "stream_account_id": "INTEGER",
        "is_overnight": "BOOLEAN DEFAULT FALSE",
    },
    "user": {
        "password_salt": "TEXT",
        "is_schedulable": "BOOLEAN DEFAULT FALSE",
        "staff_kind": "TEXT DEFAULT 'storefront'",
        "password_changed_at": "TIMESTAMP",
    },
    "streamers": {
        "user_id": "INTEGER",
    },
    "shift_entry": {
        # Multi-shift-per-day support: sort_order decides the vertical
        # stacking order of shifts that share the same (user_id,
        # shift_date). Legacy single-shift rows default to 0. The
        # matching UNIQUE constraint drop happens in ensure_postgres_schema().
        "sort_order": "INTEGER DEFAULT 0",
    },
    "invitetoken": {
        "token_lookup_hmac": "BYTEA",
        "target_user_id": "INTEGER",
    },
    "passwordresettoken": {
        "token_lookup_hmac": "BYTEA",
    },
    "employeeprofile": {
        "compensation_type": "TEXT DEFAULT 'hourly'",
        "monthly_salary_cents_enc": "BYTEA",
        "monthly_salary_pay_day": "INTEGER",
        "payment_method": "TEXT DEFAULT 'cash'",
    },
}


POSTGRES_INDEX_MIGRATIONS = [
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_is_enabled ON watchedchannel (is_enabled)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_enabled ON watchedchannel (backfill_enabled)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_after ON watchedchannel (backfill_after)",
    "CREATE INDEX IF NOT EXISTS ix_watchedchannel_backfill_before ON watchedchannel (backfill_before)",
    "CREATE INDEX IF NOT EXISTS ix_reviewcorrection_pattern_type ON reviewcorrection (pattern_type)",
    "CREATE INDEX IF NOT EXISTS ix_discordmessage_active_reparse_run_id ON discordmessage (active_reparse_run_id)",
    "CREATE INDEX IF NOT EXISTS ix_discordmessage_last_stitched_at ON discordmessage (last_stitched_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_shopify_orders_shopify_order_id ON shopify_orders (shopify_order_id)",
    "CREATE INDEX IF NOT EXISTS ix_shopify_orders_created_at ON shopify_orders (created_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_tiktok_auth_tiktok_shop_id ON tiktok_auth (tiktok_shop_id)",
    "CREATE INDEX IF NOT EXISTS ix_tiktok_auth_app_key ON tiktok_auth (app_key)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ix_tiktok_orders_tiktok_order_id ON tiktok_orders (tiktok_order_id)",
    "CREATE INDEX IF NOT EXISTS ix_tiktok_orders_created_at ON tiktok_orders (created_at)",
    # Indexed columns added via POSTGRES_ADDITIVE_MIGRATIONS (create_all does not backfill indexes on ALTER-only DBs)
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_guild_id ON discordmessage (guild_id)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_last_seen_at ON discordmessage (last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_deleted_at ON discordmessage (deleted_at)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_is_deleted ON discordmessage (is_deleted)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_stitched_group_id ON discordmessage (stitched_group_id)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_stitched_primary ON discordmessage (stitched_primary)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_entry_kind ON discordmessage (entry_kind)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_expense_category ON discordmessage (expense_category)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_reviewed_by ON discordmessage (reviewed_by)",
    "CREATE INDEX IF NOT EXISTS idx_discordmessage_reviewed_at ON discordmessage (reviewed_at)",
    "CREATE INDEX IF NOT EXISTS idx_watchedchannel_created_at ON watchedchannel (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_watchedchannel_updated_at ON watchedchannel (updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_bookkeepingentry_sheet_name ON bookkeepingentry (sheet_name)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_customer_name ON shopify_orders (customer_name)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_customer_email ON shopify_orders (customer_email)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_financial_status ON shopify_orders (financial_status)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_fulfillment_status ON shopify_orders (fulfillment_status)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_source ON shopify_orders (source)",
    "CREATE INDEX IF NOT EXISTS idx_shopify_orders_received_at ON shopify_orders (received_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_shop_id ON tiktok_orders (shop_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_shop_cipher ON tiktok_orders (shop_cipher)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_seller_id ON tiktok_orders (seller_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_customer_name ON tiktok_orders (customer_name)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_customer_email ON tiktok_orders (customer_email)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_financial_status ON tiktok_orders (financial_status)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_fulfillment_status ON tiktok_orders (fulfillment_status)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_order_status ON tiktok_orders (order_status)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_currency ON tiktok_orders (currency)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_source ON tiktok_orders (source)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_orders_received_at ON tiktok_orders (received_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_shop_cipher ON tiktok_auth (shop_cipher)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_seller_id ON tiktok_auth (seller_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_open_id ON tiktok_auth (open_id)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_shop_name ON tiktok_auth (shop_name)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_shop_region ON tiktok_auth (shop_region)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_seller_name ON tiktok_auth (seller_name)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_access_token_expires_at ON tiktok_auth (access_token_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_refresh_token_expires_at ON tiktok_auth (refresh_token_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_source ON tiktok_auth (source)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_received_at ON tiktok_auth (received_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_created_at ON tiktok_auth (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_tiktok_auth_updated_at ON tiktok_auth (updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_stream_schedules_stream_account_id ON stream_schedules (stream_account_id)",
    'CREATE INDEX IF NOT EXISTS idx_user_is_schedulable ON "user" (is_schedulable)',
    'CREATE INDEX IF NOT EXISTS idx_user_staff_kind ON "user" (staff_kind)',
    "CREATE INDEX IF NOT EXISTS idx_streamers_user_id ON streamers (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_invitetoken_token_lookup_hmac ON invitetoken (token_lookup_hmac)",
    "CREATE INDEX IF NOT EXISTS idx_invitetoken_target_user_id ON invitetoken (target_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_passwordresettoken_token_lookup_hmac ON passwordresettoken (token_lookup_hmac)",
    "CREATE INDEX IF NOT EXISTS idx_employeeprofile_compensation_type ON employeeprofile (compensation_type)",
    "CREATE INDEX IF NOT EXISTS idx_employeeprofile_monthly_salary_pay_day ON employeeprofile (monthly_salary_pay_day)",
    "CREATE INDEX IF NOT EXISTS idx_employeeprofile_payment_method ON employeeprofile (payment_method)",
]


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


def _migrate_shift_entry_drop_unique(connection) -> None:
    """Drop the legacy UNIQUE(user_id, shift_date) index on shift_entry.

    The original schema had a UniqueConstraint so every (employee, day)
    cell held exactly one ShiftEntry row. Multi-shift-per-day support
    requires allowing N rows per (employee, day), so we drop the unique
    index. SQLite can't actually remove a table-level UniqueConstraint
    without rebuilding the table, but we don't need to: when SQLModel
    sees an existing table on `create_all`, it leaves the schema alone.
    The UniqueConstraint wasn't re-declared on the model (it was dropped
    in the same refactor), so newly-created DBs never get it; older DBs
    already in the wild carry it as an auto-generated unique index
    named `sqlite_autoindex_shift_entry_1` (or a named one) which we
    drop here.

    Idempotent: DROP INDEX IF EXISTS is a no-op on clean databases.
    """
    if not sqlite_table_exists(connection, "shift_entry"):
        return
    # Find any unique index covering (user_id, shift_date) and drop it.
    # Named constraints land in sqlite_master as regular indexes; the
    # implicit ones SQLite auto-generates are prefixed sqlite_autoindex_
    # and can't be DROPped directly — a full table rebuild is the only
    # way to remove those. Detect that case and rebuild.
    indexes = list(
        connection.execute(
            text("PRAGMA index_list('shift_entry')")
        )
    )
    # PRAGMA index_list returns: (seq, name, unique, origin, partial)
    for row in indexes:
        name = row[1]
        is_unique = int(row[2]) == 1
        if not is_unique:
            continue
        info = list(
            connection.execute(text(f"PRAGMA index_info('{name}')"))
        )
        cols = {r[2] for r in info}  # column names
        if cols != {"user_id", "shift_date"}:
            continue
        if name.startswith("sqlite_autoindex_"):
            # Can't drop implicit constraint indexes — must rebuild table.
            _rebuild_shift_entry_without_unique(connection)
            return
        try:
            connection.execute(text(f'DROP INDEX IF EXISTS "{name}"'))
        except Exception as exc:
            print(f"[db] shift_entry unique-index drop skipped ({name}): {exc}")


def _rebuild_shift_entry_without_unique(connection) -> None:
    """Recreate shift_entry without the implicit UNIQUE constraint.

    SQLite doesn't support ALTER TABLE DROP CONSTRAINT, so we use the
    officially-sanctioned rename-copy-drop-rename dance:
      1. CREATE TABLE shift_entry_new (...) without the unique
      2. INSERT INTO shift_entry_new SELECT ... FROM shift_entry
      3. DROP TABLE shift_entry
      4. ALTER TABLE shift_entry_new RENAME TO shift_entry
      5. Recreate the non-unique indexes we still want
    Foreign key checks are temporarily disabled so step 3 doesn't
    cascade-delete rows in tables referencing shift_entry.id.
    """
    # Collect existing columns so the INSERT copies everything forward
    # even if the old DB is missing recent additive columns (e.g.
    # sort_order hasn't run yet because _ensure_sqlite_schema runs the
    # column-adds BEFORE this migration). We recreate with the full
    # current shape.
    existing_cols = [
        row[1]
        for row in connection.execute(text("PRAGMA table_info('shift_entry')"))
    ]
    has_sort_order = "sort_order" in existing_cols
    # FK checks off for the duration of the rebuild. `PRAGMA
    # foreign_keys` only takes effect when NOT inside a transaction on
    # some SQLite versions, so this is best-effort; rebuilding a
    # leaf-like table like shift_entry is safe even with FKs on.
    connection.execute(text("PRAGMA foreign_keys = OFF"))
    try:
        connection.execute(text("DROP TABLE IF EXISTS shift_entry_new"))
        connection.execute(
            text(
                """
                CREATE TABLE shift_entry_new (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    shift_date DATE NOT NULL,
                    label TEXT NOT NULL DEFAULT '',
                    kind TEXT NOT NULL DEFAULT 'blank',
                    notes TEXT NOT NULL DEFAULT '',
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    created_by_user_id INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES user(id),
                    FOREIGN KEY (created_by_user_id) REFERENCES user(id)
                )
                """
            )
        )
        # Build the SELECT list so missing columns are substituted with
        # defaults rather than failing the copy.
        copy_cols = (
            "id, user_id, shift_date, label, kind, notes, "
            + ("sort_order" if has_sort_order else "0")
            + ", created_by_user_id, created_at, updated_at"
        )
        connection.execute(
            text(
                "INSERT INTO shift_entry_new "
                "(id, user_id, shift_date, label, kind, notes, sort_order, "
                "created_by_user_id, created_at, updated_at) "
                f"SELECT {copy_cols} FROM shift_entry"
            )
        )
        connection.execute(text("DROP TABLE shift_entry"))
        connection.execute(text("ALTER TABLE shift_entry_new RENAME TO shift_entry"))
        # Recreate the non-unique indexes SQLModel declared.
        for stmt in (
            "CREATE INDEX IF NOT EXISTS ix_shift_entry_user_id ON shift_entry(user_id)",
            "CREATE INDEX IF NOT EXISTS ix_shift_entry_shift_date ON shift_entry(shift_date)",
            "CREATE INDEX IF NOT EXISTS ix_shift_entry_kind ON shift_entry(kind)",
            "CREATE INDEX IF NOT EXISTS ix_shift_entry_sort_order ON shift_entry(sort_order)",
        ):
            try:
                connection.execute(text(stmt))
            except Exception as exc:
                print(f"[db] shift_entry index recreate skipped: {exc}")
    finally:
        try:
            connection.execute(text("PRAGMA foreign_keys = ON"))
        except Exception:
            pass


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
        _migrate_shift_entry_drop_unique(connection)


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


def _pg_migrate_statement(stmt: str, label: str) -> None:
    """Run a single migration DDL statement in its own short transaction with a lock timeout.

    If the lock can't be acquired within 5 seconds, the statement is skipped rather
    than blocking the entire app startup (and cascading into a full server hang).
    """
    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text(stmt))
    except Exception as exc:
        print(f"[db] migration skipped ({label}): {exc}")


def ensure_postgres_schema() -> None:
    if not is_postgres_database_url(database_url):
        return

    for table_name, columns in POSTGRES_ADDITIVE_MIGRATIONS.items():
        pg_table = f'"{table_name}"' if table_name == "user" else table_name
        for column_name, column_type in columns.items():
            _pg_migrate_statement(
                f"ALTER TABLE {pg_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}",
                f"{table_name}.{column_name}",
            )
    for idx_stmt in POSTGRES_INDEX_MIGRATIONS:
        _pg_migrate_statement(idx_stmt, idx_stmt[:60])
    # Multi-shift-per-day: the original shift_entry table had a UNIQUE
    # constraint on (user_id, shift_date). Drop it so a single employee
    # can have multiple shifts on the same day. Safe to run repeatedly
    # (IF EXISTS) and harmless on fresh databases where the constraint
    # was never created.
    _pg_migrate_statement(
        "ALTER TABLE shift_entry DROP CONSTRAINT IF EXISTS uq_shift_entry_user_date",
        "shift_entry.drop_uq_user_date",
    )
    with engine.begin() as connection:
        migrate_legacy_postgres_shopify_orders(connection)


def postgres_schema_ready() -> bool:
    if not is_postgres_database_url(database_url):
        return False

    required_tables = set(SQLModel.metadata.tables.keys())
    psycopg_url = settings.database_url
    for prefix in ("postgresql+psycopg://", "postgresql://"):
        if psycopg_url.startswith(prefix):
            psycopg_url = "postgresql://" + psycopg_url[len(prefix):]
            break
    try:
        with psycopg.connect(psycopg_url, connect_timeout=15) as conn:
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
    except Exception as exc:
        print(f"[db] postgres_schema_ready check failed: {exc}")
        return False


DEFAULT_ROLE_PERMISSIONS: tuple[tuple[str, str, bool], ...] = tuple(
    (role, key, allowed)
    for key, row in (
        ("page.dashboard", (True, True, True, True, True)),
        ("page.profile", (True, True, True, True, True)),
        ("page.policies", (True, True, True, True, True)),
        ("page.hours", (True, False, True, True, True)),
        ("page.schedule", (True, False, True, True, True)),
        ("page.announcements", (True, True, True, True, True)),
        ("page.timeoff", (True, True, True, True, True)),
        ("page.supply_requests", (True, False, True, True, True)),
        ("page.admin.employees", (False, False, False, False, True)),
        ("page.admin.invites", (False, False, False, False, True)),
        ("page.admin.permissions", (False, False, False, False, True)),
        ("page.admin.supply", (False, False, True, True, True)),
        ("widget.dashboard.hours_this_week", (True, False, True, True, True)),
        ("widget.dashboard.estimated_pay", (True, False, False, False, True)),
        ("widget.dashboard.todays_tasks", (True, False, True, True, True)),
        ("widget.dashboard.upcoming_shifts", (True, False, True, True, True)),
        ("widget.dashboard.supply_queue_count", (False, False, True, True, True)),
        ("action.timeoff.submit", (True, True, True, True, True)),
        ("action.supply_request.submit", (True, False, True, True, True)),
        ("action.supply_request.approve", (False, False, True, True, True)),
        ("action.pii.reveal", (False, False, False, False, True)),
        ("action.password.reset_issued", (False, False, False, False, True)),
        ("action.employee.terminate", (False, False, False, False, True)),
        ("action.employee.purge", (False, False, False, False, True)),
        ("admin.permissions.view", (False, False, False, False, True)),
        ("admin.permissions.edit", (False, False, False, False, True)),
        # Wave 4 — employee management, invites, supply queue
        ("admin.employees.view", (False, False, True, False, True)),
        ("admin.employees.reveal_pii", (False, False, False, False, True)),
        ("admin.employees.reset_password", (False, False, False, False, True)),
        ("admin.employees.terminate", (False, False, False, False, True)),
        ("admin.employees.purge", (False, False, False, False, True)),
        ("admin.invites.view", (False, False, False, False, True)),
        ("admin.invites.issue", (False, False, False, False, True)),
        ("admin.supply.view", (False, False, True, True, True)),
        ("admin.supply.approve", (False, False, True, True, True)),
        ("admin.timeoff.view", (False, False, True, True, True)),
        ("admin.timeoff.approve", (False, False, True, False, True)),
        # Wave 4.5 — edit key distinct from view
        ("admin.employees.edit", (False, False, False, False, True)),
        ("admin.employee_roster.edit", (False, False, True, False, True)),
        # Wave 4.7 — schedule admin surface. Managers can also view/edit
        # since scheduling is typically a floor-manager responsibility;
        # admin is the one required role.
        ("admin.schedule.view", (False, False, True, False, True)),
        ("admin.schedule.edit", (False, False, True, False, True)),
        # Payroll exports expose compensation totals, so they stay admin-only
        # unless the permissions matrix explicitly grants them.
        ("admin.labor_financials.view", (False, False, False, False, True)),
        ("admin.payroll.view", (False, False, False, False, True)),
        ("admin.payroll.lock", (False, False, False, False, True)),
        ("legacy.ops.view", (False, False, False, True, True)),
        # Wave C — announcements comms hub.
        ("admin.announcements.view", (False, False, True, True, True)),
        ("admin.announcements.create", (False, False, True, False, True)),
        ("admin.policies.view", (False, False, False, False, True)),
        ("admin.policies.create", (False, False, False, False, True)),
        ("page.admin.schedule", (False, False, True, False, True)),
    )
    for role, allowed in zip(
        ("employee", "viewer", "manager", "reviewer", "admin"), row
    )
)


DEFAULT_DASHBOARD_WIDGETS: tuple[dict, ...] = (
    {
        "widget_key": "dashboard.hours_this_week",
        "title": "Hours This Week",
        "description": "Clockify hours for the current week.",
        "default_roles_csv": "employee,manager,reviewer,admin",
        "display_order": 10,
    },
    {
        "widget_key": "dashboard.estimated_pay",
        "title": "Estimated Pay",
        "description": "Hours × hourly rate (current week).",
        "default_roles_csv": "employee,admin",
        "display_order": 20,
    },
    {
        "widget_key": "dashboard.todays_tasks",
        "title": "Today's Tasks",
        "description": "Policies to acknowledge, reminders, etc.",
        "default_roles_csv": "employee,manager,reviewer,admin",
        "display_order": 30,
    },
    {
        "widget_key": "dashboard.upcoming_shifts",
        "title": "Upcoming Shifts",
        "description": "Read-only upcoming schedule.",
        "default_roles_csv": "employee,manager,reviewer,admin",
        "display_order": 40,
    },
    {
        "widget_key": "dashboard.supply_queue_count",
        "title": "Supply Queue",
        "description": "Count of pending supply approvals.",
        "default_roles_csv": "manager,reviewer,admin",
        "display_order": 50,
    },
)


def seed_employee_portal_defaults(session: Session) -> None:
    """Idempotent seed for RolePermission grid + DashboardWidget registry."""
    from datetime import datetime as _dt, timezone as _tz
    from sqlmodel import select as _select

    from .models import DashboardWidget, RolePermission

    now = _dt.now(_tz.utc)
    existing_perms = {
        (row.role, row.resource_key)
        for row in session.exec(_select(RolePermission)).all()
    }
    added_any = False
    for role, resource_key, is_allowed in DEFAULT_ROLE_PERMISSIONS:
        if (role, resource_key) in existing_perms:
            continue
        session.add(
            RolePermission(
                role=role,
                resource_key=resource_key,
                is_allowed=is_allowed,
                updated_at=now,
            )
        )
        added_any = True

    existing_widgets = {
        row.widget_key for row in session.exec(_select(DashboardWidget)).all()
    }
    for spec in DEFAULT_DASHBOARD_WIDGETS:
        if spec["widget_key"] in existing_widgets:
            continue
        session.add(DashboardWidget(created_at=now, **spec))
        added_any = True

    if added_any:
        session.commit()

    # Wave 4.5 MAJ-1: older deployments seeded reviewer=False for the two
    # admin.supply.* keys before the spec was reconciled. Force-true on every
    # boot for just these two rows. Idempotent: only UPDATEs when still False.
    fixup_keys = ("admin.supply.view", "admin.supply.approve")
    stale = session.exec(
        _select(RolePermission).where(
            RolePermission.role == "reviewer",
            RolePermission.resource_key.in_(fixup_keys),
            RolePermission.is_allowed == False,  # noqa: E712 (SQLModel needs ==)
        )
    ).all()
    if stale:
        for row in stale:
            row.is_allowed = True
            row.updated_at = now
            session.add(row)
        session.commit()


def seed_staff_schedule_defaults(session: Session) -> None:
    """One-time backfill for new Wave 4.8 staff columns.

    Adds two protections against re-running and clobbering admin edits:
      * `is_schedulable` is only auto-flipped on if NO user has
        is_schedulable=True yet. Once any admin has curated the list,
        we stop.
      * `staff_kind` is only auto-set to 'stream' if NO user has
        staff_kind='stream' yet. Same logic.

    Additionally, on every boot we fill in missing Streamer.user_id by
    matching Streamer.name / Streamer.display_name to User.display_name
    (case-insensitive). This is safe to re-run because we only fill
    NULLs and never overwrite an existing link.
    """
    from datetime import datetime as _dt, timezone as _tz

    from sqlmodel import select as _select

    from .models import (
        STAFF_KIND_STREAM,
        ScheduleRosterMember,
        ShiftEntry,
        Streamer,
        User,
    )

    now = _dt.now(_tz.utc)
    changed = False

    # 1) Link Streamers -> Users by case-insensitive name match.
    streamers = list(
        session.exec(
            _select(Streamer).where(Streamer.user_id.is_(None))
        ).all()
    )
    if streamers:
        users = list(session.exec(_select(User)).all())
        name_to_user: dict[str, User] = {}
        for u in users:
            for candidate in (u.display_name, u.username):
                key = (candidate or "").strip().lower()
                if key and key not in name_to_user:
                    name_to_user[key] = u
        for s in streamers:
            for candidate in (s.display_name, s.name):
                key = (candidate or "").strip().lower()
                if not key:
                    continue
                match = name_to_user.get(key)
                if match and match.id:
                    s.user_id = match.id
                    s.updated_at = now
                    session.add(s)
                    changed = True
                    break

    # 2) Auto-classify linked users as 'stream' — but only if admin
    #    hasn't started curating yet.
    stream_users_exist = session.exec(
        _select(User).where(User.staff_kind == STAFF_KIND_STREAM).limit(1)
    ).first() is not None
    if not stream_users_exist:
        linked_user_ids = {
            s.user_id
            for s in session.exec(_select(Streamer).where(Streamer.is_active == True)).all()  # noqa: E712
            if s.user_id is not None
        }
        for uid in linked_user_ids:
            u = session.get(User, uid)
            if u is None:
                continue
            if u.staff_kind != STAFF_KIND_STREAM:
                u.staff_kind = STAFF_KIND_STREAM
                u.updated_at = now
                session.add(u)
                changed = True

    # 3) Auto-enable is_schedulable for everyone who's actually been
    #    put on a schedule, again only if no curation has happened.
    schedulable_exists = session.exec(
        _select(User).where(User.is_schedulable == True).limit(1)  # noqa: E712
    ).first() is not None
    if not schedulable_exists:
        rostered_ids = {
            row.user_id
            for row in session.exec(_select(ScheduleRosterMember)).all()
            if row.user_id is not None
        }
        shifted_ids = {
            row.user_id
            for row in session.exec(_select(ShiftEntry)).all()
            if row.user_id is not None
        }
        for uid in rostered_ids | shifted_ids:
            u = session.get(User, uid)
            if u is None or u.is_schedulable:
                continue
            u.is_schedulable = True
            u.updated_at = now
            session.add(u)
            changed = True

    if changed:
        session.commit()


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
    if settings.employee_portal_enabled:
        try:
            with Session(engine) as session:
                seed_employee_portal_defaults(session)
        except Exception as exc:
            print(f"[db] employee portal seed skipped: {exc}")
        try:
            with Session(engine) as session:
                seed_staff_schedule_defaults(session)
        except Exception as exc:
            print(f"[db] staff/schedule backfill skipped: {exc}")

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


def recent_db_failure(window_seconds: float = 2.0) -> bool:
    with _db_failure_state_lock:
        failed_at = float(_db_failure_state["failed_at"])
    return failed_at > 0 and (time.monotonic() - failed_at) < window_seconds


def is_sqlite_lock_error(exc: Exception) -> bool:
    return database_url.startswith("sqlite") and "database is locked" in str(exc).lower()


@contextmanager
def managed_session():
    sqlite_mode = database_url.startswith("sqlite")
    attempts = 3 if sqlite_mode else 5
    delay_seconds = 0.5 if sqlite_mode else 0.75
    if is_postgres_database_url(database_url) and recent_db_failure():
        raise OperationalError("Database circuit open", None, None)

    for attempt in range(1, attempts + 1):
        session = Session(engine)
        try:
            if not sqlite_mode:
                session.exec(text("SELECT 1"))
            clear_db_failure()
            yield session
            return
        except OperationalError as exc:
            if sqlite_mode and is_sqlite_lock_error(exc) and attempt < attempts:
                session.close()
                time.sleep(delay_seconds)
                delay_seconds *= 2
                continue
            if not sqlite_mode:
                mark_db_failure()
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
