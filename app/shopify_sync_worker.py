from __future__ import annotations

import asyncio
from threading import Event
from typing import Optional

from sqlmodel import Session, select

from .config import get_settings
from .db import managed_session
from .inventory.pricing import effective_price
from .inventory.shopify import (
    apply_shopify_variant_ref,
    find_shopify_variant_by_sku,
    get_shopify_inventory_item_location_id,
    get_shopify_primary_location_id,
    push_item_to_shopify,
    resolve_shopify_access_token,
    shopify_admin_configured,
    sync_shopify_inventory_quantity,
    update_shopify_variant_price,
)
from .models import INVENTORY_LISTED, InventoryItem, ShopifySyncJob, utcnow
from .runtime_logging import structured_log_line
from .shopify_sync import (
    SHOPIFY_SYNC_ERROR,
    SHOPIFY_SYNC_ISSUE_SYNC_ERROR,
    SHOPIFY_SYNC_JOB_DONE,
    SHOPIFY_SYNC_JOB_ERROR,
    SHOPIFY_SYNC_JOB_PENDING,
    SHOPIFY_SYNC_LINKED,
    SHOPIFY_SYNC_SYNCED,
    mark_shopify_item_synced,
    record_shopify_sync_issue,
)

settings = get_settings()
SHOPIFY_SYNC_INTERVAL_SECONDS = 20.0
SHOPIFY_SYNC_BATCH_SIZE = 10


def _sync_location_id_for_item(item: InventoryItem) -> str:
    return (
        (item.shopify_location_id or "").strip()
        or (settings.shopify_location_id or "").strip()
    )


def _shopify_access_token() -> str:
    return resolve_shopify_access_token(settings)


async def _ensure_sync_location_id(item: InventoryItem) -> str:
    location_id = _sync_location_id_for_item(item)
    if location_id:
        return location_id
    if item.shopify_inventory_item_id:
        discovered_for_item = await get_shopify_inventory_item_location_id(
            item.shopify_inventory_item_id,
            store_domain=settings.shopify_store_domain,
            access_token=_shopify_access_token(),
        )
        if discovered_for_item:
            item.shopify_location_id = discovered_for_item
            return discovered_for_item
    discovered = await get_shopify_primary_location_id(
        store_domain=settings.shopify_store_domain,
        access_token=_shopify_access_token(),
    )
    if discovered:
        item.shopify_location_id = discovered
        return discovered
    return ""


def _apply_shopify_ids_response(item: InventoryItem, ids_resp: dict) -> None:
    item.shopify_product_id = str(ids_resp.get("shopify_product_id") or item.shopify_product_id or "")
    item.shopify_variant_id = str(ids_resp.get("shopify_variant_id") or item.shopify_variant_id or "")
    inventory_item_id = str(ids_resp.get("shopify_inventory_item_id") or "").strip()
    if inventory_item_id:
        item.shopify_inventory_item_id = inventory_item_id
    sku = str(ids_resp.get("shopify_sku") or "").strip()
    if sku:
        item.shopify_sku = sku
    if settings.shopify_location_id and not item.shopify_location_id:
        item.shopify_location_id = settings.shopify_location_id.strip()


async def sync_inventory_item_to_shopify(
    session: Session,
    item: InventoryItem,
    *,
    source: str = "Shopify Sync Worker",
) -> tuple[bool, str]:
    access_token = _shopify_access_token()
    if not settings.shopify_store_domain or not access_token:
        return False, "SHOPIFY_STORE_DOMAIN and a Shopify Admin token must be configured."

    try:
        if not item.shopify_variant_id:
            existing = await find_shopify_variant_by_sku(
                item.barcode,
                store_domain=settings.shopify_store_domain,
                access_token=access_token,
            )
            if existing:
                apply_shopify_variant_ref(item, existing)
                item.shopify_sync_status = SHOPIFY_SYNC_LINKED
                item.updated_at = utcnow()
                session.add(item)
                session.commit()

        if not item.shopify_variant_id:
            ids_resp = await push_item_to_shopify(
                item,
                store_domain=settings.shopify_store_domain,
                access_token=access_token,
            )
            if ids_resp:
                _apply_shopify_ids_response(item, ids_resp)
                item.status = INVENTORY_LISTED
                item.updated_at = utcnow()
                session.add(item)
                session.commit()

        if not item.shopify_variant_id:
            return False, "Could not find or create a Shopify variant for this item."

        price_ok = await update_shopify_variant_price(
            item,
            store_domain=settings.shopify_store_domain,
            access_token=access_token,
        )
        if not price_ok:
            return False, "Shopify price update failed."

        location_id = await _ensure_sync_location_id(item) if item.shopify_inventory_item_id else ""
        if item.shopify_inventory_item_id and location_id:
            quantity_ok, quantity_error = await sync_shopify_inventory_quantity(
                item,
                store_domain=settings.shopify_store_domain,
                access_token=access_token,
                location_id=location_id,
            )
            if not quantity_ok:
                return False, quantity_error or "Shopify quantity sync failed."

        item.status = INVENTORY_LISTED if item.quantity > 0 else item.status
        mark_shopify_item_synced(
            session,
            item,
            status=SHOPIFY_SYNC_SYNCED,
            payload={"source": source, "quantity": item.quantity, "price": effective_price(item)},
        )
        session.commit()
        return True, ""
    except Exception as exc:
        session.rollback()
        return False, str(exc)


async def process_pending_shopify_sync_jobs_once(
    *,
    limit: int = SHOPIFY_SYNC_BATCH_SIZE,
    runtime_name: str = "shopify_sync",
) -> tuple[int, int]:
    processed = 0
    failed = 0
    with managed_session() as session:
        jobs = session.exec(
            select(ShopifySyncJob)
            .where(ShopifySyncJob.status == SHOPIFY_SYNC_JOB_PENDING)
            .order_by(ShopifySyncJob.created_at.asc())
            .limit(limit)
        ).all()
        for job in jobs:
            item = session.get(InventoryItem, job.item_id)
            job.attempts = (job.attempts or 0) + 1
            job.updated_at = utcnow()
            if item is None or item.archived_at is not None:
                job.status = SHOPIFY_SYNC_JOB_ERROR
                job.last_error = "Inventory item is missing or archived."
                job.processed_at = utcnow()
                session.add(job)
                failed += 1
                continue
            ok, error = await sync_inventory_item_to_shopify(
                session,
                item,
                source=job.source or runtime_name,
            )
            job.status = SHOPIFY_SYNC_JOB_DONE if ok else SHOPIFY_SYNC_JOB_ERROR
            job.last_error = error or None
            job.processed_at = utcnow()
            job.updated_at = utcnow()
            session.add(job)
            if ok:
                processed += 1
            else:
                failed += 1
                item = session.get(InventoryItem, job.item_id)
                if item:
                    mark_shopify_item_synced(session, item, status=SHOPIFY_SYNC_ERROR, error=error)
                    record_shopify_sync_issue(
                        session,
                        issue_type=SHOPIFY_SYNC_ISSUE_SYNC_ERROR,
                        inventory_item_id=item.id,
                        shopify_product_id=item.shopify_product_id,
                        shopify_variant_id=item.shopify_variant_id,
                        shopify_inventory_item_id=item.shopify_inventory_item_id,
                        shopify_sku=item.shopify_sku or item.barcode,
                        shopify_title=item.card_name,
                        message=error or "Shopify sync failed.",
                        payload={"job_id": job.id, "action": job.action},
                    )
            session.commit()
    return processed, failed


async def periodic_shopify_sync_loop(stop_event: Event) -> None:
    runtime_name = f"{settings.runtime_name}_shopify_sync"
    while not stop_event.is_set():
        try:
            if shopify_admin_configured(settings):
                processed, failed = await process_pending_shopify_sync_jobs_once(
                    runtime_name=runtime_name,
                )
                if processed or failed:
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="shopify.inventory_sync.cycle",
                            success=failed == 0,
                            processed=processed,
                            failed=failed,
                        )
                    )
        except Exception as exc:
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="shopify.inventory_sync.failed",
                    success=False,
                    error=str(exc),
                )
            )
        await asyncio.sleep(SHOPIFY_SYNC_INTERVAL_SECONDS)
