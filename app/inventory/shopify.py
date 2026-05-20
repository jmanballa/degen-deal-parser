"""
Outbound Shopify Admin API integration for inventory.

Handles creating Shopify products from inventory items, updating prices,
and marking items sold when a Shopify order arrives with a matching SKU.

Requires SHOPIFY_ACCESS_TOKEN (or the existing SHOPIFY_API_KEY env var used by
the older Shopify order sync) with write_products and write_inventory scopes.
SHOPIFY_STORE_DOMAIN must also be set.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Optional
from uuid import uuid4

import httpx

from ..models import InventoryItem, ITEM_TYPE_SEALED, ITEM_TYPE_SLAB, utcnow
from .pricing import effective_price
from ..shopify_api import SHOPIFY_API_VERSION

logger = logging.getLogger(__name__)


@dataclass
class ShopifyVariantRef:
    sku: str
    product_id: Optional[str] = None
    variant_id: Optional[str] = None
    inventory_item_id: Optional[str] = None
    product_gid: Optional[str] = None
    variant_gid: Optional[str] = None
    inventory_item_gid: Optional[str] = None
    location_gid: Optional[str] = None
    title: Optional[str] = None
    product_title: Optional[str] = None
    product_handle: Optional[str] = None
    product_status: Optional[str] = None


@dataclass
class ShopifyInventorySyncResult:
    ok: bool
    sku: str
    quantity: int
    location_gid: Optional[str] = None
    variant: Optional[ShopifyVariantRef] = None
    user_errors: list[dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


def _shopify_headers(access_token: str) -> dict[str, str]:
    return {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def resolve_shopify_access_token(settings_obj: Any) -> str:
    """Return the configured Shopify Admin token without forcing a new env name."""
    explicit = getattr(settings_obj, "shopify_access_token", "")
    legacy = getattr(settings_obj, "shopify_api_key", "")
    explicit_text = explicit.strip() if isinstance(explicit, str) else ""
    legacy_text = legacy.strip() if isinstance(legacy, str) else ""
    return explicit_text or legacy_text


def shopify_admin_configured(settings_obj: Any) -> bool:
    store_domain = getattr(settings_obj, "shopify_store_domain", "")
    store_text = store_domain.strip() if isinstance(store_domain, str) else ""
    return bool(store_text and resolve_shopify_access_token(settings_obj))


def _shopify_base(store_domain: str) -> str:
    domain = (store_domain or "").strip().rstrip("/")
    if not domain.startswith("http"):
        domain = f"https://{domain}"
    return f"{domain}/admin/api/{SHOPIFY_API_VERSION}"


def _shopify_graphql_url(store_domain: str) -> str:
    return f"{_shopify_base(store_domain)}/graphql.json"


def _shopify_gid(resource: str, value: str | int | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("gid://shopify/"):
        return text
    return f"gid://shopify/{resource}/{text}"


def _shopify_legacy_id(value: Any) -> str:
    text = str(value or "").strip()
    if "/" in text:
        return text.rsplit("/", 1)[-1]
    return text


async def _graphql_post(
    client: httpx.AsyncClient,
    *,
    store_domain: str,
    access_token: str,
    query: str,
    variables: dict[str, Any],
) -> dict[str, Any]:
    response = await client.post(
        _shopify_graphql_url(store_domain),
        json={"query": query, "variables": variables},
        headers=_shopify_headers(access_token),
    )
    response.raise_for_status()
    return response.json()


def _variant_ref_from_graphql_node(node: dict[str, Any]) -> ShopifyVariantRef:
    product = node.get("product") or {}
    inventory_item = node.get("inventoryItem") or {}
    inventory_levels = (inventory_item.get("inventoryLevels") or {}).get("nodes") or []
    location = (inventory_levels[0] or {}).get("location") if inventory_levels else {}
    product_gid = str(product.get("id") or "")
    variant_gid = str(node.get("id") or "")
    inventory_item_gid = str(inventory_item.get("id") or "")
    location_gid = str((location or {}).get("id") or "")
    return ShopifyVariantRef(
        sku=str(node.get("sku") or ""),
        product_id=_shopify_legacy_id(product_gid),
        variant_id=_shopify_legacy_id(variant_gid),
        inventory_item_id=_shopify_legacy_id(inventory_item_gid),
        product_gid=product_gid or None,
        variant_gid=variant_gid or None,
        inventory_item_gid=inventory_item_gid or None,
        location_gid=location_gid or None,
        title=str(node.get("title") or ""),
        product_title=str(product.get("title") or ""),
        product_handle=str(product.get("handle") or ""),
        product_status=str(product.get("status") or ""),
    )


async def find_shopify_variant_by_sku(
    sku: str,
    *,
    store_domain: str,
    access_token: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[ShopifyVariantRef]:
    sku = (sku or "").strip()
    if not sku or not store_domain or not access_token:
        return None

    query = """
    query FindVariantBySku($query: String!) {
      productVariants(first: 5, query: $query) {
        nodes {
          id
          sku
          title
          inventoryItem {
            id
          }
          product {
            id
            title
            handle
            status
          }
        }
      }
    }
    """

    async def _run(active_client: httpx.AsyncClient) -> Optional[ShopifyVariantRef]:
        try:
            payload = await _graphql_post(
                active_client,
                store_domain=store_domain,
                access_token=access_token,
                query=query,
                variables={"query": f"sku:{sku}"},
            )
        except Exception as exc:
            logger.warning("[shopify-inventory] SKU lookup failed for %s: %s", sku, exc)
            return None
        nodes = ((payload.get("data") or {}).get("productVariants") or {}).get("nodes") or []
        exact = [node for node in nodes if str(node.get("sku") or "").strip() == sku]
        if len(exact) != 1:
            return None
        return _variant_ref_from_graphql_node(exact[0])

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=20.0) as active_client:
        return await _run(active_client)


async def list_shopify_product_variants(
    *,
    store_domain: str,
    access_token: str,
    client: Optional[httpx.AsyncClient] = None,
    limit: int = 250,
) -> list[ShopifyVariantRef]:
    if not store_domain or not access_token:
        return []
    url = f"{_shopify_base(store_domain)}/products.json"
    params = {
        "limit": max(1, min(int(limit or 250), 250)),
        "fields": "id,title,handle,status,variants",
    }

    async def _run(active_client: httpx.AsyncClient) -> list[ShopifyVariantRef]:
        response = await active_client.get(
            url,
            params=params,
            headers=_shopify_headers(access_token),
        )
        response.raise_for_status()
        rows: list[ShopifyVariantRef] = []
        for product in response.json().get("products") or []:
            if not isinstance(product, dict):
                continue
            for variant in product.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                rows.append(
                    ShopifyVariantRef(
                        sku=str(variant.get("sku") or ""),
                        product_id=str(product.get("id") or ""),
                        variant_id=str(variant.get("id") or ""),
                        inventory_item_id=str(variant.get("inventory_item_id") or ""),
                        product_gid=_shopify_gid("Product", product.get("id")),
                        variant_gid=_shopify_gid("ProductVariant", variant.get("id")),
                        inventory_item_gid=_shopify_gid("InventoryItem", variant.get("inventory_item_id")),
                        title=str(variant.get("title") or ""),
                        product_title=str(product.get("title") or ""),
                        product_handle=str(product.get("handle") or ""),
                        product_status=str(product.get("status") or ""),
                    )
                )
        return rows

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=30.0) as active_client:
        return await _run(active_client)


async def get_shopify_primary_location_id(
    *,
    store_domain: str,
    access_token: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[str]:
    if not store_domain or not access_token:
        return None
    url = f"{_shopify_base(store_domain)}/locations.json"

    async def _run(active_client: httpx.AsyncClient) -> Optional[str]:
        try:
            response = await active_client.get(url, headers=_shopify_headers(access_token))
            response.raise_for_status()
        except Exception as exc:
            logger.warning("[shopify-inventory] location lookup failed: %s", exc)
            return None
        locations = response.json().get("locations") or []
        active_locations = [row for row in locations if row and row.get("active", True)]
        selected = active_locations[0] if active_locations else (locations[0] if locations else None)
        return _shopify_legacy_id((selected or {}).get("id")) if selected else None

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=20.0) as active_client:
        return await _run(active_client)


async def get_shopify_inventory_item_location_id(
    inventory_item_id: str,
    *,
    store_domain: str,
    access_token: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[str]:
    inventory_item_id = _shopify_legacy_id(inventory_item_id)
    if not inventory_item_id or not store_domain or not access_token:
        return None
    url = f"{_shopify_base(store_domain)}/inventory_levels.json"
    params = {"inventory_item_ids": inventory_item_id, "limit": 1}

    async def _run(active_client: httpx.AsyncClient) -> Optional[str]:
        try:
            response = await active_client.get(
                url,
                params=params,
                headers=_shopify_headers(access_token),
            )
            response.raise_for_status()
        except Exception as exc:
            logger.warning(
                "[shopify-inventory] inventory level lookup failed for %s: %s",
                inventory_item_id,
                exc,
            )
            return None
        levels = response.json().get("inventory_levels") or []
        first_level = levels[0] if levels else None
        return _shopify_legacy_id((first_level or {}).get("location_id")) if first_level else None

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=20.0) as active_client:
        return await _run(active_client)


def apply_shopify_variant_ref(item: InventoryItem, variant: ShopifyVariantRef) -> None:
    item.shopify_product_id = variant.product_id or item.shopify_product_id
    item.shopify_variant_id = variant.variant_id or item.shopify_variant_id
    item.shopify_inventory_item_id = variant.inventory_item_id or item.shopify_inventory_item_id
    item.shopify_location_id = _shopify_legacy_id(variant.location_gid) or item.shopify_location_id
    item.shopify_sku = variant.sku or item.shopify_sku or item.barcode
    item.shopify_product_handle = variant.product_handle or item.shopify_product_handle
    item.shopify_product_status = variant.product_status or item.shopify_product_status


def _build_product_title(item: InventoryItem) -> str:
    parts = [item.card_name]
    if item.set_name:
        parts.append(item.set_name)
    if item.item_type == ITEM_TYPE_SLAB and item.grading_company and item.grade:
        parts.append(f"{item.grading_company} {item.grade}")
    elif item.item_type == ITEM_TYPE_SEALED and item.sealed_product_kind:
        parts.append(item.sealed_product_kind)
    else:
        if item.variant:
            parts.append(item.variant)
        if item.condition:
            parts.append(item.condition)
    if item.language and item.language != "English":
        parts.append(item.language)
    return " — ".join(parts)


def _build_product_body(item: InventoryItem) -> str:
    lines = []
    if item.game:
        lines.append(f"Game: {item.game}")
    if item.set_name:
        lines.append(f"Set: {item.set_name}")
    if item.card_number:
        lines.append(f"Card #: {item.card_number}")
    if item.item_type == ITEM_TYPE_SLAB:
        if item.grading_company:
            lines.append(f"Grading Company: {item.grading_company}")
        if item.grade:
            lines.append(f"Grade: {item.grade}")
        if item.cert_number:
            lines.append(f"Cert #: {item.cert_number}")
    elif item.item_type == ITEM_TYPE_SEALED:
        if item.sealed_product_kind:
            lines.append(f"Product Type: {item.sealed_product_kind}")
        if item.upc:
            lines.append(f"UPC: {item.upc}")
        if item.location:
            lines.append(f"Location: {item.location}")
    else:
        if item.variant:
            lines.append(f"Variant: {item.variant}")
        if item.condition:
            lines.append(f"Condition: {item.condition}")
    if item.language and item.language != "English":
        lines.append(f"Language: {item.language}")
    if item.notes:
        lines.append(f"Notes: {item.notes}")
    return "<br>".join(lines)


def _build_product_tags(item: InventoryItem) -> list[str]:
    tags = [item.game, item.item_type]
    if item.item_type == ITEM_TYPE_SLAB:
        if item.grading_company:
            tags.append(item.grading_company)
        if item.grade:
            tags.append(f"Grade {item.grade}")
    elif item.item_type == ITEM_TYPE_SEALED:
        if item.sealed_product_kind:
            tags.append(item.sealed_product_kind)
    else:
        if item.variant:
            tags.append(item.variant)
        if item.condition:
            tags.append(item.condition)
    if item.set_name:
        tags.append(item.set_name)
    return [t for t in tags if t]


def build_shopify_product_payload(item: InventoryItem) -> dict[str, Any]:
    price = effective_price(item)
    price_str = f"{price:.2f}" if price is not None else "0.00"
    return {
        "product": {
            "title": _build_product_title(item),
            "body_html": _build_product_body(item),
            "product_type": "Sealed Product" if item.item_type == ITEM_TYPE_SEALED else ("Slabs" if item.item_type == ITEM_TYPE_SLAB else "Singles"),
            "tags": ", ".join(_build_product_tags(item)),
            "variants": [
                {
                    "price": price_str,
                    "sku": item.barcode,
                    "inventory_quantity": item.quantity,
                    "inventory_management": "shopify",
                    "fulfillment_service": "manual",
                }
            ],
        }
    }


async def push_item_to_shopify(
    item: InventoryItem,
    *,
    store_domain: str,
    access_token: str,
) -> Optional[dict[str, Any]]:
    """
    Create a new Shopify product for the inventory item.

    Returns a dict with shopify_product_id and shopify_variant_id on success,
    or None on failure.
    """
    if not store_domain or not access_token:
        logger.warning("[shopify-inventory] Shopify store domain or Admin token is not set")
        return None

    url = f"{_shopify_base(store_domain)}/products.json"
    payload = build_shopify_product_payload(item)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                url,
                json=payload,
                headers=_shopify_headers(access_token),
            )
            resp.raise_for_status()
            data = resp.json()
            product = data.get("product") or {}
            variants = product.get("variants") or [{}]
            variant = variants[0] if variants else {}
            return {
                "shopify_product_id": str(product.get("id") or ""),
                "shopify_variant_id": str(variant.get("id") or ""),
                "shopify_inventory_item_id": str(
                    variant.get("inventory_item_id")
                    or ((variant.get("inventory_item") or {}).get("id") if isinstance(variant.get("inventory_item"), dict) else "")
                    or ""
                ),
                "shopify_sku": str(variant.get("sku") or item.barcode),
            }
    except httpx.HTTPStatusError as exc:
        logger.error(
            "[shopify-inventory] create product failed for item %s: %s %s",
            item.barcode,
            exc.response.status_code,
            exc.response.text[:200],
        )
        return None
    except Exception as exc:
        logger.error("[shopify-inventory] create product error for item %s: %s", item.barcode, exc)
        return None


async def update_shopify_variant_price(
    item: InventoryItem,
    *,
    store_domain: str,
    access_token: str,
) -> bool:
    """Push the current effective_price to the Shopify variant. Returns True on success."""
    if not item.shopify_variant_id or not store_domain or not access_token:
        return False

    price = effective_price(item)
    if price is None:
        return False

    url = f"{_shopify_base(store_domain)}/variants/{item.shopify_variant_id}.json"
    payload = {"variant": {"id": item.shopify_variant_id, "price": f"{price:.2f}"}}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.put(
                url,
                json=payload,
                headers=_shopify_headers(access_token),
            )
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error(
            "[shopify-inventory] price update failed for variant %s: %s",
            item.shopify_variant_id,
            exc,
        )
        return False


async def sync_shopify_inventory_quantity(
    item: InventoryItem,
    *,
    store_domain: str,
    access_token: str,
    location_id: str = "",
    client: Optional[httpx.AsyncClient] = None,
    idempotency_key: Optional[str] = None,
) -> tuple[bool, Optional[str]]:
    """Set Shopify's available quantity to the local Degen quantity."""
    if not store_domain or not access_token:
        return False, "SHOPIFY_STORE_DOMAIN and a Shopify Admin token must be configured."
    inventory_item_id = (item.shopify_inventory_item_id or "").strip()
    if not inventory_item_id:
        return False, "Item is missing a Shopify inventory item ID."
    resolved_location_id = (location_id or item.shopify_location_id or "").strip()
    if not resolved_location_id:
        return False, "Item is missing a Shopify location ID."

    query = """
    mutation SyncInventoryQuantity($idempotencyKey: String!, $input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
        inventoryAdjustmentGroup {
          changes {
            quantityAfterChange
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        "idempotencyKey": idempotency_key or f"inventory-sync-{item.id or item.barcode}-{uuid4()}",
        "input": {
            "name": "available",
            "reason": "correction",
            "referenceDocumentUri": f"degen://inventory/{item.id or item.barcode}",
            "ignoreCompareQuantity": True,
            "quantities": [
                {
                    "inventoryItemId": _shopify_gid("InventoryItem", inventory_item_id),
                    "locationId": _shopify_gid("Location", resolved_location_id),
                    "quantity": max(0, int(item.quantity or 0)),
                    "changeFromQuantity": None,
                }
            ],
        },
    }

    async def _run(active_client: httpx.AsyncClient) -> tuple[bool, Optional[str]]:
        try:
            payload = await _graphql_post(
                active_client,
                store_domain=store_domain,
                access_token=access_token,
                query=query,
                variables=variables,
            )
        except Exception as exc:
            return False, str(exc)
        if payload.get("errors"):
            return False, "; ".join(str(err.get("message") or err) for err in payload["errors"])
        result = ((payload.get("data") or {}).get("inventorySetQuantities") or {})
        user_errors = result.get("userErrors") or []
        if user_errors:
            return False, "; ".join(str(err.get("message") or err) for err in user_errors)
        return True, None

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient(timeout=20.0) as active_client:
        return await _run(active_client)
