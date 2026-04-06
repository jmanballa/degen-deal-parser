from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from threading import Lock
from typing import Any, Optional

import httpx
from sqlmodel import Session, select

from .models import ShopifyOrder, utcnow
from .runtime_logging import structured_log_line

SHOPIFY_API_VERSION = "2024-01"
SHOPIFY_ORDERS_PATH = f"/admin/api/{SHOPIFY_API_VERSION}/orders.json"
SHOPIFY_PROGRESS_INTERVAL = 50
_backfill_state_lock = Lock()
_backfill_state = {
    "is_running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_since": None,
    "last_limit": None,
    "last_summary": None,
    "last_error": None,
}


@dataclass
class ShopifyBackfillSummary:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed: int = 0


def read_shopify_backfill_state() -> dict[str, Any]:
    with _backfill_state_lock:
        return dict(_backfill_state)


def update_shopify_backfill_state(**changes: Any) -> dict[str, Any]:
    with _backfill_state_lock:
        _backfill_state.update(changes)
        return dict(_backfill_state)


def parse_shopify_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return utcnow()
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def money_to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return round(float(Decimal(str(value))), 2)
    except (InvalidOperation, ValueError, TypeError):
        return 0.0


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def _safe_json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not value:
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def normalize_shopify_line_items(line_items: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in _safe_json_list(line_items):
        title = str(item.get("title") or "").strip()
        if not title:
            continue

        quantity_raw = item.get("quantity")
        try:
            quantity = int(quantity_raw or 0)
        except (TypeError, ValueError):
            quantity = 0

        normalized.append(
            {
                "title": title,
                "quantity": quantity if quantity > 0 else 1,
                "sku": str(item.get("sku") or "").strip() or None,
                "product_id": str(item.get("product_id") or "").strip() or None,
                "variant_id": str(item.get("variant_id") or "").strip() or None,
                "unit_price": money_to_float(item.get("price")),
            }
        )
    return normalized


def build_shopify_reconciliation_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    normalized_line_items = normalize_shopify_line_items(record.get("line_items_json"))
    return {
        "shopify_order_id": record.get("shopify_order_id"),
        "order_number": record.get("order_number"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "customer_name": record.get("customer_name"),
        "customer_email": record.get("customer_email"),
        "financial_status": record.get("financial_status"),
        "total_price": record.get("total_price"),
        "subtotal_ex_tax": record.get("subtotal_ex_tax"),
        "line_item_count": len(normalized_line_items),
        "line_items": normalized_line_items,
    }


def normalize_customer_name(payload: dict[str, Any]) -> Optional[str]:
    customer = payload.get("customer") or {}
    billing_address = payload.get("billing_address") or {}
    shipping_address = payload.get("shipping_address") or {}

    first_name = (
        customer.get("first_name")
        or billing_address.get("first_name")
        or shipping_address.get("first_name")
        or ""
    ).strip()
    last_name = (
        customer.get("last_name")
        or billing_address.get("last_name")
        or shipping_address.get("last_name")
        or ""
    ).strip()
    full_name = " ".join(part for part in (first_name, last_name) if part).strip()
    return full_name or None


def normalize_customer_email(payload: dict[str, Any]) -> Optional[str]:
    customer = payload.get("customer") or {}
    return (
        customer.get("email")
        or payload.get("email")
        or (payload.get("contact_email") or "")
    ) or None


def normalize_order_number(payload: dict[str, Any]) -> str:
    name = str(payload.get("name") or "").strip()
    if name:
        return name
    number = payload.get("order_number")
    if number not in (None, ""):
        return f"#{number}"
    return str(payload.get("id") or "")


def extract_order_tax_fields(payload: dict[str, Any]) -> tuple[Optional[float], Optional[float], bool]:
    total_price = money_to_float(payload.get("total_price"))
    raw_total_tax = payload.get("total_tax")
    if raw_total_tax not in (None, ""):
        total_tax = money_to_float(raw_total_tax)
        return total_tax, round(total_price - total_tax, 2), False

    tax_lines = payload.get("tax_lines") or []
    if isinstance(tax_lines, list) and tax_lines:
        tax_total = round(
            sum(money_to_float(line.get("price")) for line in tax_lines if isinstance(line, dict)),
            2,
        )
        return tax_total, round(total_price - tax_total, 2), False

    return None, None, True


def validate_shopify_webhook(*, raw_body: bytes, shared_secret: str, received_hmac: Optional[str]) -> bool:
    if not shared_secret or not received_hmac:
        return False
    digest = hmac.new(shared_secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    encoded_digest = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(encoded_digest, received_hmac.strip())


def order_record_from_payload(
    payload: dict[str, Any],
    *,
    source: str,
    received_at: Optional[datetime] = None,
    runtime_name: str = "shopify_ingest",
) -> dict[str, Any]:
    order_id = payload.get("id")
    if order_id in (None, ""):
        raise ValueError("Shopify payload is missing id")

    created_at = parse_shopify_datetime(payload.get("created_at"))
    updated_at = parse_shopify_datetime(payload.get("updated_at") or payload.get("created_at"))
    total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
    if missing_tax:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="shopify.tax_fields.missing",
                success=False,
                error="No tax field available on Shopify order payload",
                shopify_order_id=payload.get("id"),
                order_number=payload.get("name") or payload.get("order_number"),
            )
        )
    return {
        "shopify_order_id": str(order_id),
        "order_number": normalize_order_number(payload),
        "created_at": created_at,
        "updated_at": updated_at,
        "customer_name": normalize_customer_name(payload),
        "customer_email": normalize_customer_email(payload),
        "total_price": money_to_float(payload.get("total_price")),
        "subtotal_price": money_to_float(payload.get("subtotal_price")),
        "total_tax": total_tax,
        "subtotal_ex_tax": subtotal_ex_tax,
        "financial_status": str(payload.get("financial_status") or "").strip(),
        "fulfillment_status": str(payload.get("fulfillment_status") or "").strip() or None,
        "line_items_json": json_dumps(payload.get("line_items") or []),
        "line_items_summary_json": json_dumps(
            normalize_shopify_line_items(payload.get("line_items") or [])
        ),
        "raw_payload": json_dumps(payload),
        "source": source,
        "received_at": received_at or utcnow(),
    }


def upsert_shopify_order(
    session: Session,
    payload: dict[str, Any],
    *,
    source: str,
    received_at: Optional[datetime] = None,
    dry_run: bool = False,
    runtime_name: str = "shopify_ingest",
) -> str:
    record = order_record_from_payload(
        payload,
        source=source,
        received_at=received_at,
        runtime_name=runtime_name,
    )
    existing = session.exec(
        select(ShopifyOrder).where(ShopifyOrder.shopify_order_id == record["shopify_order_id"])
    ).first()

    if existing is None:
        if not dry_run:
            session.add(ShopifyOrder(**record))
        return "inserted"

    for field_name, value in record.items():
        setattr(existing, field_name, value)
    if not dry_run:
        session.add(existing)
    return "updated"


def build_shopify_orders_url(store_domain: str) -> str:
    normalized = (store_domain or "").strip().rstrip("/")
    if normalized.startswith("https://"):
        return f"{normalized}{SHOPIFY_ORDERS_PATH}"
    if normalized.startswith("http://"):
        normalized = normalized.replace("http://", "https://", 1)
        return f"{normalized}{SHOPIFY_ORDERS_PATH}"
    return f"https://{normalized}{SHOPIFY_ORDERS_PATH}"


def parse_shopify_link_header(link_header: str | None) -> dict[str, str]:
    links: dict[str, str] = {}
    if not link_header:
        return links

    for raw_part in link_header.split(","):
        part = raw_part.strip()
        if not part.startswith("<") or ">;" not in part:
            continue
        url_part, _, meta_part = part.partition(">;")
        url = url_part[1:]
        rel = ""
        for item in meta_part.split(";"):
            key, _, value = item.strip().partition("=")
            if key == "rel":
                rel = value.strip().strip('"')
                break
        if rel:
            links[rel] = url
    return links


def extract_next_page_info(link_header: str | None) -> Optional[str]:
    links = parse_shopify_link_header(link_header)
    next_url = links.get("next")
    if not next_url or "page_info=" not in next_url:
        return None
    _, _, tail = next_url.partition("page_info=")
    return tail.split("&", 1)[0]


def fetch_shopify_orders_page(
    client: httpx.Client,
    *,
    store_domain: str,
    api_key: str,
    since: Optional[str] = None,
    page_info: Optional[str] = None,
    limit: int = 250,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    headers = {
        "X-Shopify-Access-Token": api_key,
        "Accept": "application/json",
    }
    params: dict[str, Any] = {
        "status": "any",
        "limit": max(1, min(limit, 250)),
    }
    if page_info:
        params = {
            "limit": max(1, min(limit, 250)),
            "page_info": page_info,
        }
    elif since:
        params["created_at_min"] = since

    url = build_shopify_orders_url(store_domain)
    max_attempts = 3
    backoff = 0.5
    response: httpx.Response | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, headers=headers, params=params)
            if response.status_code == 429 or response.status_code >= 500:
                wait_s = backoff
                ra = response.headers.get("Retry-After")
                if ra:
                    try:
                        wait_s = max(wait_s, float(ra))
                    except ValueError:
                        pass
                if attempt < max_attempts:
                    time.sleep(wait_s)
                    backoff *= 2
                    continue
            response.raise_for_status()
            break
        except (httpx.TimeoutException, httpx.TransportError):
            if attempt >= max_attempts:
                raise
            time.sleep(backoff)
            backoff *= 2
    if response is None:
        raise RuntimeError("Shopify fetch failed without response")
    payload = response.json()
    orders = payload.get("orders") or []
    if not isinstance(orders, list):
        raise ValueError("Shopify API response did not include an orders list")
    next_page_info = extract_next_page_info(response.headers.get("Link"))
    return orders, next_page_info


def backfill_shopify_orders(
    session: Session,
    *,
    store_domain: str,
    api_key: str,
    since: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    runtime_name: str = "shopify_backfill",
) -> ShopifyBackfillSummary:
    summary = ShopifyBackfillSummary()
    if limit == 0:
        return summary
    page_info: Optional[str] = None
    remaining = limit if limit and limit > 0 else None

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        while True:
            page_limit = min(250, remaining) if remaining else 250
            orders, page_info = fetch_shopify_orders_page(
                client,
                store_domain=store_domain,
                api_key=api_key,
                since=since,
                page_info=page_info,
                limit=page_limit,
            )
            if not orders:
                break

            for payload in orders:
                if remaining is not None and remaining <= 0:
                    break
                summary.fetched += 1
                try:
                    result = upsert_shopify_order(
                        session,
                        payload,
                        source="backfill",
                        received_at=utcnow(),
                        dry_run=dry_run,
                        runtime_name=runtime_name,
                    )
                    if result == "inserted":
                        summary.inserted += 1
                    else:
                        summary.updated += 1
                    if not dry_run:
                        session.commit()
                    elif session.in_transaction():
                        session.rollback()
                except Exception as exc:
                    summary.failed += 1
                    if session.in_transaction():
                        session.rollback()
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="shopify.backfill.order_failed",
                            success=False,
                            error=str(exc),
                            shopify_order_id=payload.get("id"),
                            order_number=payload.get("name") or payload.get("order_number"),
                        )
                    )

                if summary.fetched % SHOPIFY_PROGRESS_INTERVAL == 0:
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="shopify.backfill.progress",
                            success=True,
                            fetched=summary.fetched,
                            inserted=summary.inserted,
                            updated=summary.updated,
                            failed=summary.failed,
                            dry_run=dry_run,
                        )
                    )
                if remaining is not None:
                    remaining -= 1

            if remaining is not None and remaining <= 0:
                break
            if not page_info:
                break

    return summary


def repair_shopify_tax_fields(session: Session) -> int:
    rows = session.exec(
        select(ShopifyOrder).where(
            (ShopifyOrder.total_tax == None) | (ShopifyOrder.subtotal_ex_tax == None)
        )
    ).all()
    updated = 0
    for row in rows:
        try:
            payload = json.loads(row.raw_payload or "{}")
        except json.JSONDecodeError:
            continue
        total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
        if missing_tax:
            continue
        row.total_tax = total_tax
        row.subtotal_ex_tax = subtotal_ex_tax
        session.add(row)
        updated += 1
    if updated:
        session.commit()
    return updated


def repair_shopify_line_item_summaries(session: Session) -> int:
    rows = session.exec(
        select(ShopifyOrder).where(ShopifyOrder.line_items_summary_json == "[]")
    ).all()
    updated = 0
    for row in rows:
        try:
            payload = json.loads(row.raw_payload or "{}")
        except json.JSONDecodeError:
            continue
        normalized = normalize_shopify_line_items(payload.get("line_items") or row.line_items_json)
        if not normalized:
            continue
        row.line_items_summary_json = json_dumps(normalized)
        session.add(row)
        updated += 1
    if updated:
        session.commit()
    return updated
