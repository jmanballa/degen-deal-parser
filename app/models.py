from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Iterable
from typing import Optional
from sqlalchemy import Column, LargeBinary
from sqlmodel import SQLModel, Field

PARSE_PENDING = "pending"
PARSE_PROCESSING = "processing"
PARSE_PARSED = "parsed"
PARSE_FAILED = "failed"
PARSE_REVIEW_REQUIRED = "review_required"
PARSE_IGNORED = "ignored"

LEGACY_PARSE_STATUS_ALIASES = {
    "queued": PARSE_PENDING,
    "needs_review": PARSE_REVIEW_REQUIRED,
    "deleted": PARSE_IGNORED,
}

ACTIVE_PARSE_STATUSES = {PARSE_PENDING, PARSE_PROCESSING}
TERMINAL_PARSE_STATUSES = {PARSE_PARSED, PARSE_FAILED, PARSE_REVIEW_REQUIRED, PARSE_IGNORED}
ALL_PARSE_STATUSES = ACTIVE_PARSE_STATUSES | TERMINAL_PARSE_STATUSES

BACKFILL_QUEUED = "queued"
BACKFILL_PROCESSING = "processing"
BACKFILL_COMPLETED = "completed"
BACKFILL_CANCELLED = "cancelled"
BACKFILL_FAILED = "failed"
BACKFILL_TERMINAL_STATUSES = {BACKFILL_COMPLETED, BACKFILL_CANCELLED, BACKFILL_FAILED}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_parse_status(
    value: Optional[str],
    *,
    is_deleted: bool = False,
    needs_review: bool = False,
) -> str:
    if is_deleted:
        return PARSE_IGNORED
    if needs_review:
        return PARSE_REVIEW_REQUIRED

    normalized = (value or "").strip().lower()
    if not normalized:
        return PARSE_PENDING
    return LEGACY_PARSE_STATUS_ALIASES.get(normalized, normalized)


def is_pending_parse_status(value: Optional[str]) -> bool:
    return normalize_parse_status(value) == PARSE_PENDING


def is_review_required_status(value: Optional[str], *, needs_review: bool = False) -> bool:
    return normalize_parse_status(value, needs_review=needs_review) == PARSE_REVIEW_REQUIRED


def expand_parse_status_filter_values(values: Iterable[str]) -> set[str]:
    expanded: set[str] = set()
    requested = {normalize_parse_status(value) for value in values if value}

    for raw_value, canonical_value in LEGACY_PARSE_STATUS_ALIASES.items():
        if canonical_value in requested:
            expanded.add(raw_value)

    expanded.update(requested)
    return expanded


def normalize_money_value(value: Optional[float]) -> float:
    if value is None:
        return 0.0
    return round(float(value), 2)


def signed_money_delta(money_in: Optional[float], money_out: Optional[float]) -> float:
    return round(normalize_money_value(money_in) - normalize_money_value(money_out), 2)


class WatchedChannel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    channel_id: str = Field(index=True, unique=True)
    channel_name: Optional[str] = Field(default=None, index=True)

    is_enabled: bool = Field(default=True, index=True)
    backfill_enabled: bool = Field(default=True, index=True)
    backfill_after: Optional[datetime] = Field(default=None, index=True)
    backfill_before: Optional[datetime] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class AvailableDiscordChannel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    channel_id: str = Field(index=True, unique=True)
    channel_name: str = Field(index=True)
    guild_id: Optional[str] = Field(default=None, index=True)
    guild_name: Optional[str] = Field(default=None, index=True)
    category_name: Optional[str] = Field(default=None, index=True)
    label: str = Field(index=True)
    created_at_discord: Optional[datetime] = Field(default=None, index=True)
    last_message_at: Optional[datetime] = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class DiscordMessage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    discord_message_id: str = Field(index=True, unique=True)
    guild_id: Optional[str] = Field(default=None, index=True)
    channel_id: str = Field(index=True)
    channel_name: Optional[str] = Field(default=None, index=True)

    author_id: Optional[str] = Field(default=None)
    author_name: Optional[str] = Field(default=None)

    content: str = ""
    attachment_urls_json: str = "[]"

    created_at: datetime = Field(index=True)
    ingested_at: datetime = Field(default_factory=utcnow, index=True)
    last_seen_at: Optional[datetime] = Field(default=None, index=True)
    edited_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = Field(default=None, index=True)
    is_deleted: bool = Field(default=False, index=True)

    stitched_group_id: Optional[str] = Field(default=None, index=True)
    stitched_primary: bool = Field(default=False, index=True)
    stitched_message_ids_json: str = "[]"
    last_stitched_at: Optional[datetime] = Field(default=None, index=True)

    parse_status: str = Field(default=PARSE_PENDING, index=True)
    parse_attempts: int = Field(default=0)
    last_error: Optional[str] = None
    active_reparse_run_id: Optional[str] = Field(default=None, index=True)

    deal_type: Optional[str] = None
    amount: Optional[float] = None
    payment_method: Optional[str] = None
    cash_direction: Optional[str] = None
    category: Optional[str] = None

    item_names_json: str = "[]"
    items_in_json: str = "[]"
    items_out_json: str = "[]"

    trade_summary: Optional[str] = None
    notes: Optional[str] = None
    confidence: Optional[float] = None
    needs_review: bool = Field(default=False)
    image_summary: Optional[str] = None
    reviewed_by: Optional[str] = Field(default=None, index=True)
    reviewed_at: Optional[datetime] = Field(default=None, index=True)

    entry_kind: Optional[str] = Field(default=None, index=True)
    money_in: Optional[float] = None
    money_out: Optional[float] = None
    expense_category: Optional[str] = Field(default=None, index=True)


class AttachmentAsset(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(index=True, foreign_key="discordmessage.id")
    source_url: str = Field(index=True)
    filename: Optional[str] = None
    content_type: Optional[str] = None
    is_image: bool = Field(default=False, index=True)
    data: bytes = Field(sa_column=Column(LargeBinary, nullable=False))
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ParseAttempt(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(index=True, foreign_key="discordmessage.id")
    attempt_number: int
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    model_used: Optional[str] = None
    input_tokens: Optional[int] = None
    cached_input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    estimated_cost_usd: Optional[float] = None


class ReparseRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, unique=True)
    source: str = Field(default="unknown", index=True)
    reason: Optional[str] = Field(default=None)

    requested_at: datetime = Field(default_factory=utcnow, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)
    duration_ms: Optional[int] = None

    range_after: Optional[datetime] = Field(default=None, index=True)
    range_before: Optional[datetime] = Field(default=None, index=True)
    channel_id: Optional[str] = Field(default=None, index=True)
    requested_statuses_json: str = "[]"

    include_reviewed: bool = Field(default=False)
    force_reviewed: bool = Field(default=False)

    selected_count: int = Field(default=0)
    queued_count: int = Field(default=0)
    already_queued_count: int = Field(default=0)
    skipped_reviewed_count: int = Field(default=0)
    succeeded_count: int = Field(default=0)
    failed_count: int = Field(default=0)

    first_message_id: Optional[int] = None
    last_message_id: Optional[int] = None
    first_message_created_at: Optional[datetime] = Field(default=None, index=True)
    last_message_created_at: Optional[datetime] = Field(default=None, index=True)

    status: str = Field(default="queued", index=True)
    error_message: Optional[str] = None


class Transaction(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source_message_id: int = Field(index=True, unique=True, foreign_key="discordmessage.id")

    discord_message_id: Optional[str] = Field(default=None, index=True)
    guild_id: Optional[str] = Field(default=None, index=True)
    channel_id: Optional[str] = Field(default=None, index=True)
    channel_name: Optional[str] = Field(default=None, index=True)
    author_name: Optional[str] = Field(default=None, index=True)
    occurred_at: datetime = Field(index=True)

    parse_status: Optional[str] = Field(default=None, index=True)
    deal_type: Optional[str] = Field(default=None, index=True)
    entry_kind: Optional[str] = Field(default=None, index=True)
    payment_method: Optional[str] = Field(default=None, index=True)
    cash_direction: Optional[str] = Field(default=None, index=True)
    category: Optional[str] = Field(default=None, index=True)
    expense_category: Optional[str] = Field(default=None, index=True)

    amount: Optional[float] = None
    money_in: Optional[float] = None
    money_out: Optional[float] = None

    needs_review: bool = Field(default=False, index=True)
    confidence: Optional[float] = None
    notes: Optional[str] = None
    trade_summary: Optional[str] = None
    source_content: str = ""
    is_deleted: bool = Field(default=False, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class TransactionItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    transaction_id: int = Field(index=True, foreign_key="transaction.id")
    direction: str = Field(index=True)
    item_name: str
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ReviewCorrection(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    source_message_id: int = Field(index=True, unique=True, foreign_key="discordmessage.id")
    normalized_text: str = Field(index=True)
    pattern_type: Optional[str] = Field(default=None, index=True)

    deal_type: Optional[str] = Field(default=None, index=True)
    amount: Optional[float] = None
    payment_method: Optional[str] = Field(default=None, index=True)
    cash_direction: Optional[str] = Field(default=None, index=True)
    category: Optional[str] = Field(default=None, index=True)
    entry_kind: Optional[str] = Field(default=None, index=True)
    expense_category: Optional[str] = Field(default=None, index=True)

    notes: Optional[str] = None
    trade_summary: Optional[str] = None
    items_in_json: str = "[]"
    items_out_json: str = "[]"
    item_names_json: str = "[]"
    confidence: Optional[float] = None
    correction_source: str = Field(default="manual_edit", index=True)
    parsed_before_json: str = "{}"
    corrected_after_json: str = "{}"
    field_diffs_json: str = "{}"
    features_json: str = "{}"

    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class BookkeepingImport(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    show_label: str = Field(index=True)
    show_date: Optional[datetime] = Field(default=None, index=True)
    range_start: Optional[datetime] = Field(default=None, index=True)
    range_end: Optional[datetime] = Field(default=None, index=True)
    source_kind: str = Field(default="upload", index=True)
    source_name: Optional[str] = None
    source_url: Optional[str] = None
    row_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class BookkeepingEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    import_id: int = Field(index=True, foreign_key="bookkeepingimport.id")
    row_index: int = Field(index=True)
    sheet_name: Optional[str] = Field(default=None, index=True)
    occurred_at: Optional[datetime] = Field(default=None, index=True)
    entry_kind: Optional[str] = Field(default=None, index=True)
    amount: Optional[float] = None
    payment_method: Optional[str] = Field(default=None, index=True)
    category: Optional[str] = Field(default=None, index=True)
    notes: Optional[str] = None
    raw_row_json: str = "{}"
    matched_transaction_id: Optional[int] = Field(default=None, index=True)
    match_status: Optional[str] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ShopifyOrder(SQLModel, table=True):
    __tablename__ = "shopify_orders"

    id: Optional[int] = Field(default=None, primary_key=True)
    shopify_order_id: str = Field(index=True, unique=True)
    order_number: str = Field(index=True)
    created_at: datetime = Field(index=True)
    updated_at: datetime = Field(index=True)
    customer_name: Optional[str] = Field(default=None, index=True)
    customer_email: Optional[str] = Field(default=None, index=True)
    total_price: float = 0.0
    subtotal_price: float = 0.0
    total_tax: Optional[float] = None
    subtotal_ex_tax: Optional[float] = None
    financial_status: str = Field(default="", index=True)
    fulfillment_status: Optional[str] = Field(default=None, index=True)
    line_items_json: str = "[]"
    line_items_summary_json: str = "[]"
    raw_payload: str = "{}"
    source: str = Field(default="webhook", index=True)
    received_at: datetime = Field(default_factory=utcnow, index=True)


class TikTokAuth(SQLModel, table=True):
    __tablename__ = "tiktok_auth"

    id: Optional[int] = Field(default=None, primary_key=True)
    tiktok_shop_id: str = Field(index=True, unique=True)
    shop_cipher: Optional[str] = Field(default=None, index=True)
    seller_id: Optional[str] = Field(default=None, index=True)
    open_id: Optional[str] = Field(default=None, index=True)
    shop_name: Optional[str] = Field(default=None, index=True)
    shop_region: Optional[str] = Field(default=None, index=True)
    seller_name: Optional[str] = Field(default=None, index=True)
    app_key: Optional[str] = Field(default=None, index=True)
    redirect_uri: Optional[str] = None
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    access_token_expires_at: Optional[datetime] = Field(default=None, index=True)
    refresh_token_expires_at: Optional[datetime] = Field(default=None, index=True)
    scopes_json: str = Field(default="[]")
    raw_payload: str = Field(default="{}")
    source: str = Field(default="oauth", index=True)
    received_at: Optional[datetime] = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)
    creator_access_token: Optional[str] = Field(default=None)
    creator_refresh_token: Optional[str] = Field(default=None)
    creator_token_expires_at: Optional[datetime] = Field(default=None)


class TikTokOrder(SQLModel, table=True):
    __tablename__ = "tiktok_orders"

    id: Optional[int] = Field(default=None, primary_key=True)
    tiktok_order_id: str = Field(index=True, unique=True)
    shop_id: Optional[str] = Field(default=None, index=True)
    shop_cipher: Optional[str] = Field(default=None, index=True)
    seller_id: Optional[str] = Field(default=None, index=True)
    order_number: str = Field(index=True)
    created_at: datetime = Field(index=True)
    updated_at: datetime = Field(index=True)
    customer_name: Optional[str] = Field(default=None, index=True)
    customer_email: Optional[str] = Field(default=None, index=True)
    total_price: float = 0.0
    subtotal_price: float = 0.0
    total_tax: Optional[float] = None
    subtotal_ex_tax: Optional[float] = None
    financial_status: str = Field(default="", index=True)
    fulfillment_status: Optional[str] = Field(default=None, index=True)
    order_status: Optional[str] = Field(default=None, index=True)
    currency: Optional[str] = Field(default=None, index=True)
    line_items_json: str = "[]"
    line_items_summary_json: str = "[]"
    raw_payload: str = "{}"
    source: str = Field(default="webhook", index=True)
    received_at: datetime = Field(default_factory=utcnow, index=True)


class TikTokProduct(SQLModel, table=True):
    __tablename__ = "tiktok_products"

    id: Optional[int] = Field(default=None, primary_key=True)
    tiktok_product_id: str = Field(index=True, unique=True)
    shop_id: Optional[str] = Field(default=None, index=True)
    shop_cipher: Optional[str] = Field(default=None, index=True)
    title: str = Field(index=True)
    description: Optional[str] = None
    status: Optional[str] = Field(default=None, index=True)
    audit_status: Optional[str] = Field(default=None, index=True)
    category_id: Optional[str] = Field(default=None, index=True)
    category_name: Optional[str] = None
    brand_id: Optional[str] = None
    brand_name: Optional[str] = None
    main_image_url: Optional[str] = None
    images_json: str = "[]"
    skus_json: str = "[]"
    sales_attributes_json: str = "[]"
    product_attributes_json: str = "[]"
    raw_payload: str = "{}"
    source: str = Field(default="sync", index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)
    synced_at: datetime = Field(default_factory=utcnow, index=True)


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str
    password_salt: str = Field(default="")
    display_name: str = Field(default="")
    role: str = Field(default="viewer", index=True)
    is_active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class RuntimeHeartbeat(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    runtime_name: str = Field(index=True, unique=True)
    host_name: Optional[str] = Field(default=None, index=True)
    status: str = Field(default="unknown", index=True)
    details_json: str = "{}"
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class BackfillRequest(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    channel_id: Optional[str] = Field(default=None, index=True)
    after: Optional[datetime] = Field(default=None, index=True)
    before: Optional[datetime] = Field(default=None, index=True)
    limit_per_channel: Optional[int] = None
    oldest_first: bool = Field(default=True, index=True)
    status: str = Field(default=BACKFILL_QUEUED, index=True)
    requested_by: Optional[str] = Field(default=None, index=True)
    result_json: str = "{}"
    error_message: Optional[str] = None
    inserted_count: int = Field(default=0)
    skipped_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    started_at: Optional[datetime] = Field(default=None, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)


class OperationsLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    event_type: str = Field(index=True)
    level: str = Field(default="info", index=True)
    source: str = Field(default="system", index=True)
    message: str
    details_json: str = "{}"
    created_at: datetime = Field(default_factory=utcnow, index=True)


# ---------------------------------------------------------------------------
# Live Hit Tracker
# ---------------------------------------------------------------------------

# Fallback streamer list — used only if no Streamer profiles exist in the DB.
STREAMERS: list[str] = [
    "Jeff",
    "Streamer 2",
    "Streamer 3",
]

PLATFORMS: list[str] = ["TikTok", "Whatnot", "Shopify Live", "YouTube", "Other"]

BIG_HIT_THRESHOLD: float = 50.0

STREAMER_COLORS: list[str] = [
    "#fe2c55", "#25f4ee", "#3ecf8e", "#f59e0b", "#8b5cf6",
    "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1",
]


class Streamer(SQLModel, table=True):
    __tablename__ = "streamers"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    display_name: Optional[str] = Field(default=None)
    color: Optional[str] = Field(default=None)
    avatar_emoji: Optional[str] = Field(default=None)
    is_active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class StreamAccount(SQLModel, table=True):
    __tablename__ = "stream_accounts"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    platform: str = Field(default="TikTok")
    handle: Optional[str] = Field(default=None)
    is_default: bool = Field(default=False)
    is_active: bool = Field(default=True, index=True)
    sort_order: int = Field(default=0)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class StreamSchedule(SQLModel, table=True):
    __tablename__ = "stream_schedules"

    id: Optional[int] = Field(default=None, primary_key=True)
    streamer_id: int = Field(index=True)
    stream_account_id: Optional[int] = Field(default=None, index=True)
    date: str = Field(index=True)  # YYYY-MM-DD
    start_time: str  # HH:MM (24h)
    end_time: str    # HH:MM (24h)
    is_overnight: bool = Field(default=False)
    title: Optional[str] = Field(default=None)
    notes: Optional[str] = Field(default=None)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class LiveHit(SQLModel, table=True):
    __tablename__ = "live_hits"

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow)
    is_deleted: bool = Field(default=False, index=True)

    hit_at: datetime = Field(default_factory=utcnow, index=True)
    streamer_name: str = Field(index=True)
    customer_name: Optional[str] = Field(default=None, index=True)
    order_number: Optional[str] = Field(default=None, index=True)
    hit_note: str
    estimated_value: Optional[float] = Field(default=None)
    order_value: Optional[float] = Field(default=None)
    platform: Optional[str] = Field(default=None)
    stream_label: Optional[str] = Field(default=None)
    notes: Optional[str] = Field(default=None)
    created_by: Optional[str] = Field(default=None)
    image_filename: Optional[str] = Field(default=None)


class TikTokSyncState(SQLModel, table=True):
    """Singleton row (id=1) that persists the TikTok integration runtime state across restarts."""

    __tablename__ = "tiktok_sync_state"

    id: int = Field(default=1, primary_key=True)
    last_authorization_at: Optional[datetime] = Field(default=None, index=True)
    last_callback_json: str = Field(default="{}")
    last_webhook_at: Optional[datetime] = Field(default=None, index=True)
    last_webhook_json: str = Field(default="{}")
    is_pull_running: bool = Field(default=False)
    last_pull_started_at: Optional[datetime] = Field(default=None, index=True)
    last_pull_finished_at: Optional[datetime] = Field(default=None, index=True)
    last_pull_at: Optional[datetime] = Field(default=None, index=True)
    last_pull_json: str = Field(default="{}")
    last_error: Optional[str] = Field(default=None)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class AppSetting(SQLModel, table=True):
    __tablename__ = "app_settings"
    key: str = Field(primary_key=True)
    value: str = Field(default="")


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

INVENTORY_IN_STOCK = "in_stock"
INVENTORY_LISTED = "listed"
INVENTORY_SOLD = "sold"
INVENTORY_HELD = "held"
ALL_INVENTORY_STATUSES = {INVENTORY_IN_STOCK, INVENTORY_LISTED, INVENTORY_SOLD, INVENTORY_HELD}

ITEM_TYPE_SINGLE = "single"
ITEM_TYPE_SLAB = "slab"

GRADING_COMPANIES: list[str] = ["PSA", "BGS", "CGC", "SGC"]
GAMES: list[str] = [
    "Pokemon",
    "MTG",
    "Sports - Baseball",
    "Sports - Basketball",
    "Sports - Football",
    "Sports - Hockey",
    "Other",
]
CONDITIONS: list[str] = ["NM", "LP", "MP", "HP", "DMG"]
PRICE_SOURCES: list[str] = ["scrydex", "130point", "alt", "card_ladder", "manual"]


class InventoryItem(SQLModel, table=True):
    __tablename__ = "inventory_items"

    id: Optional[int] = Field(default=None, primary_key=True)
    barcode: str = Field(unique=True, index=True)          # e.g. "DGN-000001"
    item_type: str = Field(index=True)                     # "single" | "slab"
    game: str = Field(index=True)                          # "Pokemon" | "MTG" | ...
    card_name: str = Field(index=True)
    set_name: Optional[str] = Field(default=None)
    set_code: Optional[str] = Field(default=None)          # for API lookup
    card_number: Optional[str] = Field(default=None)
    language: str = Field(default="English")
    condition: Optional[str] = Field(default=None)         # singles: NM/LP/MP/HP/DMG
    quantity: int = Field(default=1)

    # Slab-specific
    grading_company: Optional[str] = Field(default=None, index=True)  # PSA | BGS | CGC | SGC
    grade: Optional[str] = Field(default=None)             # "10" | "9.5" | "9" ...
    cert_number: Optional[str] = Field(default=None, index=True)
    sub_grades_json: str = Field(default="{}")             # BGS sub-grades

    # Pricing
    cost_basis: Optional[float] = Field(default=None)     # what we paid
    auto_price: Optional[float] = Field(default=None)     # latest price lookup
    list_price: Optional[float] = Field(default=None)     # manual override
    last_priced_at: Optional[datetime] = Field(default=None, index=True)

    # Shopify
    shopify_product_id: Optional[str] = Field(default=None, index=True)
    shopify_variant_id: Optional[str] = Field(default=None)

    # Status & tracking
    status: str = Field(default=INVENTORY_IN_STOCK, index=True)
    notes: Optional[str] = Field(default=None)
    image_url: Optional[str] = Field(default=None)
    sold_at: Optional[datetime] = Field(default=None, index=True)
    sold_price: Optional[float] = Field(default=None)

    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: Optional[datetime] = Field(default=None)


class PriceHistory(SQLModel, table=True):
    __tablename__ = "price_history"

    id: Optional[int] = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="inventory_items.id", index=True)
    source: str = Field(index=True)                        # scrydex | 130point | alt | ...
    market_price: Optional[float] = Field(default=None)
    low_price: Optional[float] = Field(default=None)
    high_price: Optional[float] = Field(default=None)
    fetched_at: datetime = Field(default_factory=utcnow, index=True)
    raw_response_json: str = Field(default="{}")
