from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import Column, LargeBinary
from sqlmodel import SQLModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
    edited_at: Optional[datetime] = None
    is_deleted: bool = Field(default=False, index=True)

    stitched_group_id: Optional[str] = Field(default=None, index=True)
    stitched_primary: bool = Field(default=False, index=True)
    stitched_message_ids_json: str = "[]"

    parse_status: str = Field(default="queued", index=True)
    parse_attempts: int = Field(default=0)
    last_error: Optional[str] = None

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


class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str
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
