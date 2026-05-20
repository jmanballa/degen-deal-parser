"""Shared inventory price update and slab resticker alert helpers."""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlmodel import Session

from ..config import get_settings
from .pricing import (
    apply_slab_resticker_alert,
    effective_price,
    price_result_to_json,
)
from ..models import InventoryItem, PriceHistory, utcnow
from ..team.team_notifications import notify_active_employees

logger = logging.getLogger(__name__)
settings = get_settings()


def record_inventory_price_result(
    session: Session,
    item: InventoryItem,
    result: dict[str, Any],
    *,
    request: Any = None,
    actor_user_id: Optional[int] = None,
    notify: bool = True,
) -> tuple[PriceHistory, str]:
    """Persist a price lookup result and update slab resticker state."""
    previous_effective = effective_price(item)
    source = str(result.get("source") or "unknown")
    market_price = result.get("market_price")

    item.auto_price = market_price
    item.last_priced_at = utcnow()
    item.updated_at = utcnow()

    alert_event = "none"
    if settings.inventory_slab_resticker_alert_enabled and source in {"card_ladder", "slab_comps"}:
        alert_event = apply_slab_resticker_alert(
            item,
            suggested_price=market_price,
            previous_effective_price=previous_effective,
            min_percent=settings.inventory_slab_resticker_threshold_percent,
            min_dollars=settings.inventory_slab_resticker_threshold_dollars,
            source=source,
        )

    session.add(item)
    history = PriceHistory(
        item_id=item.id,
        source=source,
        market_price=market_price,
        low_price=result.get("low_price"),
        high_price=result.get("high_price"),
        raw_response_json=price_result_to_json(result),
    )
    session.add(history)

    if notify and alert_event in {"created", "updated"}:
        notify_slab_resticker_alert(
            session,
            item,
            request=request,
            actor_user_id=actor_user_id,
        )

    return history, alert_event


def notify_slab_resticker_alert(
    session: Session,
    item: InventoryItem,
    *,
    request: Any = None,
    actor_user_id: Optional[int] = None,
) -> None:
    """Create portal notifications for an active slab resticker alert."""
    if not item.id or not item.resticker_alert_active:
        return
    try:
        target = item.resticker_alert_price
        reference = item.resticker_reference_price
        body = item.resticker_alert_reason or "Slab comps moved above the current sticker price."
        if target is not None and reference is not None:
            body = f"Sticker ${reference:,.2f} -> slab comps ${target:,.2f}. Update the slab label."
        notify_active_employees(
            session,
            actor_user_id=actor_user_id,
            kind="inventory_resticker",
            title=f"Resticker slab: {item.card_name}",
            body=body[:700],
            link_path=f"/inventory/{item.id}",
            request=request,
            send_text=settings.inventory_slab_resticker_sms_enabled,
        )
    except Exception as exc:
        logger.warning("[inventory] resticker notification failed for item %s: %s", item.id, exc)
