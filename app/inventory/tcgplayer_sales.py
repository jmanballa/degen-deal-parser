"""TCGplayer public product sales helpers.

TCGplayer's official API is not generally available for new app keys, but the
product page itself reads a few public JSON endpoints for the "View More Data"
panel. This module keeps that browser-facing integration isolated so buylist
routes can show latest solds and 3-month snapshot data without mixing endpoint
details into UI handlers.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import re
import time
from typing import Any, Optional
from urllib.parse import parse_qsl, unquote, urlparse

import httpx

logger = logging.getLogger(__name__)

TCGPLAYER_MPF_VERSION = "5143"
TCGPLAYER_SALES_CACHE_TTL_SECONDS = 300.0
TCGPLAYER_PRODUCT_RE = re.compile(
    r"(?:https?://)?(?:www\.)?tcgplayer\.com/product/(\d+)",
    flags=re.IGNORECASE,
)

_SALES_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_SALES_CACHE_MAX = 128

_CONDITION_SEARCH_ALIASES: dict[str, tuple[str, ...]] = {
    "NM": ("NM", "NEAR MINT", "NEARMINT"),
    "LP": ("LP", "LIGHTLY PLAYED", "LIGHTPLAYED", "LIGHT PLAY", "EXCELLENT"),
    "MP": ("MP", "MODERATELY PLAYED", "MODERATE PLAY", "PLAYED"),
    "HP": ("HP", "HEAVILY PLAYED", "HEAVY PLAY"),
    "DMG": ("DMG", "DM", "DAMAGED"),
    "SEALED": ("SEALED", "UNOPENED", "UNOPENED PRODUCT"),
}


def tcgplayer_product_id_from_url(value: str | None) -> str:
    """Extract a TCGplayer product id from direct or affiliate-wrapped URLs."""
    if not value:
        return ""
    seen: set[str] = set()
    queue = [str(value)]
    while queue:
        raw = queue.pop(0).strip()
        if not raw or raw in seen:
            continue
        seen.add(raw)
        for candidate in (raw, unquote(raw)):
            match = TCGPLAYER_PRODUCT_RE.search(candidate)
            if match:
                return match.group(1)
            parsed = urlparse(candidate)
            for _key, query_value in parse_qsl(parsed.query, keep_blank_values=True):
                if query_value and query_value not in seen:
                    queue.append(query_value)
    return ""


def tcgplayer_product_url(product_id: str, product_url: str | None = None) -> str:
    extracted = tcgplayer_product_id_from_url(product_url)
    if extracted == str(product_id):
        decoded = unquote(str(product_url or ""))
        match = TCGPLAYER_PRODUCT_RE.search(decoded)
        if match:
            return decoded[match.start():].split("&", 1)[0]
    return f"https://www.tcgplayer.com/product/{product_id}"


def normalize_tcgplayer_condition(value: str | None) -> str:
    raw = str(value or "NM").strip().upper()
    compact = re.sub(r"[^A-Z]", "", raw)
    if raw == "DM":
        return "DMG"
    for canonical, aliases in _CONDITION_SEARCH_ALIASES.items():
        normalized_aliases = {re.sub(r"[^A-Z]", "", alias.upper()) for alias in aliases}
        if compact in normalized_aliases:
            return canonical
        if any(len(alias) >= 6 and alias in compact for alias in normalized_aliases):
            return canonical
    return raw or "NM"


def _condition_tokens(value: str | None) -> set[str]:
    canonical = normalize_tcgplayer_condition(value)
    aliases = _CONDITION_SEARCH_ALIASES.get(canonical, (canonical,))
    return {re.sub(r"[^A-Z]", "", alias.upper()) for alias in aliases if alias}


def _text_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _to_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number <= 0:
        return None
    return round(number, 2)


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _money_range(values: list[Any]) -> tuple[Optional[float], Optional[float]]:
    prices = [price for price in (_to_float(value) for value in values) if price is not None]
    if not prices:
        return None, None
    return round(min(prices), 2), round(max(prices), 2)


def _row_score(
    row: dict[str, Any],
    *,
    selected_condition: str,
    selected_variant: str,
    selected_language: str,
) -> int:
    score = 0
    row_condition = _text_key(row.get("condition"))
    if row_condition and row_condition in _condition_tokens(selected_condition):
        score += 40
    row_variant = _text_key(row.get("variant"))
    selected_variant_key = _text_key(selected_variant)
    if selected_variant_key and row_variant == selected_variant_key:
        score += 25
    row_language = _text_key(row.get("language"))
    selected_language_key = _text_key(selected_language)
    if selected_language_key and row_language == selected_language_key:
        score += 15
    if normalize_tcgplayer_condition(selected_condition) == "SEALED":
        if row_condition in _condition_tokens("Sealed"):
            score += 20
        elif not selected_variant_key:
            score += 5
    return score


def _best_matching_row(
    rows: list[dict[str, Any]],
    *,
    selected_condition: str,
    selected_variant: str,
    selected_language: str,
) -> Optional[dict[str, Any]]:
    if not rows:
        return None
    scored = [
        (
            _row_score(
                row,
                selected_condition=selected_condition,
                selected_variant=selected_variant,
                selected_language=selected_language,
            ),
            index,
            row,
        )
        for index, row in enumerate(rows)
        if isinstance(row, dict)
    ]
    if not scored:
        return None
    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[0][2]


def _sale_matches(
    sale: dict[str, Any],
    *,
    selected_condition: str,
    selected_variant: str,
    selected_language: str,
) -> bool:
    if not isinstance(sale, dict):
        return False
    condition_key = _text_key(sale.get("condition"))
    if condition_key and condition_key not in _condition_tokens(selected_condition):
        return False
    variant_key = _text_key(sale.get("variant"))
    selected_variant_key = _text_key(selected_variant)
    if selected_variant_key and variant_key and variant_key != selected_variant_key:
        return False
    language_key = _text_key(sale.get("language"))
    selected_language_key = _text_key(selected_language)
    if selected_language_key and language_key and language_key != selected_language_key:
        return False
    return True


def _normalize_sale(sale: dict[str, Any]) -> dict[str, Any]:
    purchase = _to_float(sale.get("purchasePrice")) or 0.0
    shipping = _to_float(sale.get("shippingPrice")) or 0.0
    return {
        "condition": str(sale.get("condition") or "").strip(),
        "variant": str(sale.get("variant") or "").strip(),
        "language": str(sale.get("language") or "").strip(),
        "quantity": max(1, _to_int(sale.get("quantity"))),
        "title": str(sale.get("title") or "").strip(),
        "listing_type": str(sale.get("listingType") or "").strip(),
        "purchase_price": round(purchase, 2),
        "shipping_price": round(shipping, 2),
        "total_price": round(purchase + shipping, 2),
        "order_date": str(sale.get("orderDate") or "").strip(),
    }


def _snapshot_from_history_row(row: dict[str, Any]) -> dict[str, Any]:
    buckets = [bucket for bucket in row.get("buckets") or [] if isinstance(bucket, dict)]
    low_sale, high_sale = _money_range([bucket.get("lowSalePrice") for bucket in buckets])
    bucket_high_low, bucket_high = _money_range([bucket.get("highSalePrice") for bucket in buckets])
    if high_sale is None:
        high_sale = bucket_high
    elif bucket_high is not None:
        high_sale = max(high_sale, bucket_high)
    if low_sale is None and bucket_high_low is not None:
        low_sale = bucket_high_low

    latest_market = None
    for bucket in buckets:
        latest_market = _to_float(bucket.get("marketPrice"))
        if latest_market is not None:
            break

    return {
        "sku_id": str(row.get("skuId") or "").strip(),
        "condition": str(row.get("condition") or "").strip(),
        "variant": str(row.get("variant") or "").strip(),
        "language": str(row.get("language") or "").strip(),
        "average_daily_quantity_sold": _to_float(row.get("averageDailyQuantitySold")) or 0.0,
        "average_daily_transaction_count": _to_float(row.get("averageDailyTransactionCount")) or 0.0,
        "total_quantity_sold": _to_int(row.get("totalQuantitySold")),
        "total_transaction_count": _to_int(row.get("totalTransactionCount")),
        "low_sale_price": low_sale,
        "high_sale_price": high_sale,
        "market_price": latest_market,
        "bucket_count": len(buckets),
        "latest_bucket_date": str((buckets[0] or {}).get("bucketStartDate") or "").strip() if buckets else "",
        "oldest_bucket_date": str((buckets[-1] or {}).get("bucketStartDate") or "").strip() if buckets else "",
    }


def normalize_tcgplayer_sales_payload(
    product_id: str,
    *,
    selected_condition: str,
    selected_variant: str = "",
    selected_language: str = "English",
    sales_payload: dict[str, Any] | None = None,
    history_payload: dict[str, Any] | None = None,
    pricepoints_payload: list[dict[str, Any]] | None = None,
    volatility_payload: dict[str, Any] | None = None,
    product_url: str | None = None,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    raw_history_rows = (history_payload or {}).get("result", []) if isinstance(history_payload, dict) else []
    if not isinstance(raw_history_rows, list):
        raw_history_rows = []
    history_rows = [
        row for row in raw_history_rows
        if isinstance(row, dict)
    ]
    selected_history = _best_matching_row(
        history_rows,
        selected_condition=selected_condition,
        selected_variant=selected_variant,
        selected_language=selected_language,
    )
    snapshot = _snapshot_from_history_row(selected_history) if selected_history else None

    raw_sales = [
        row for row in (sales_payload or {}).get("data", [])
        if isinstance(row, dict)
    ]
    matching_sales = [
        _normalize_sale(sale)
        for sale in raw_sales
        if _sale_matches(
            sale,
            selected_condition=selected_condition,
            selected_variant=selected_variant,
            selected_language=selected_language,
        )
    ]

    pricepoint = None
    if pricepoints_payload:
        sku_id = str((snapshot or {}).get("sku_id") or "")
        for row in pricepoints_payload:
            if str(row.get("skuId") or "") == sku_id:
                pricepoint = row
                break
        if pricepoint is None and pricepoints_payload:
            pricepoint = pricepoints_payload[0]
    if snapshot and pricepoint:
        snapshot.update(
            {
                "active_market_price": _to_float(pricepoint.get("marketPrice")),
                "active_low_price": _to_float(pricepoint.get("lowestPrice")),
                "active_high_price": _to_float(pricepoint.get("highestPrice")),
                "active_listing_count": _to_int(pricepoint.get("priceCount")),
                "active_prices_calculated_at": str(pricepoint.get("calculatedAt") or "").strip(),
            }
        )
    if snapshot and volatility_payload:
        snapshot["volatility"] = str(volatility_payload.get("volatility") or "").strip()
        snapshot["volatility_z_score"] = _to_float(volatility_payload.get("zScore"))

    return {
        "ok": bool(snapshot or matching_sales),
        "source": "tcgplayer_public_web",
        "product_id": str(product_id),
        "product_url": tcgplayer_product_url(str(product_id), product_url),
        "selected": {
            "condition": selected_condition,
            "variant": selected_variant,
            "language": selected_language,
        },
        "snapshot": snapshot,
        "last_sales": matching_sales[:5],
        "latest_sales_count": len(raw_sales),
        "matching_latest_sales_count": len(matching_sales),
        "errors": errors or [],
        "fetched_at": int(time.time()),
    }


def _browser_headers(product_url: str) -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.tcgplayer.com",
        "Referer": product_url,
    }


async def _response_json(
    response: httpx.Response,
    *,
    label: str,
    errors: list[str],
) -> dict[str, Any] | list[dict[str, Any]]:
    if response.status_code >= 400:
        errors.append(f"{label} returned HTTP {response.status_code}")
        return {}
    try:
        parsed = response.json()
    except ValueError:
        errors.append(f"{label} returned invalid JSON")
        return {}
    if isinstance(parsed, (dict, list)):
        return parsed
    errors.append(f"{label} returned an unexpected payload")
    return {}


def _cache_get(key: str) -> Optional[dict[str, Any]]:
    cached = _SALES_CACHE.get(key)
    if not cached:
        return None
    expires_at, payload = cached
    if expires_at <= time.monotonic():
        _SALES_CACHE.pop(key, None)
        return None
    result = copy.deepcopy(payload)
    result["cached"] = True
    return result


def _cache_set(key: str, payload: dict[str, Any]) -> None:
    if len(_SALES_CACHE) >= _SALES_CACHE_MAX:
        oldest = min(_SALES_CACHE, key=lambda item: _SALES_CACHE[item][0])
        _SALES_CACHE.pop(oldest, None)
    cached = copy.deepcopy(payload)
    cached["cached"] = False
    _SALES_CACHE[key] = (time.monotonic() + TCGPLAYER_SALES_CACHE_TTL_SECONDS, cached)


async def fetch_tcgplayer_public_sales(
    product_id: str,
    *,
    selected_condition: str = "NM",
    selected_variant: str = "",
    selected_language: str = "English",
    product_url: str | None = None,
) -> dict[str, Any]:
    """Fetch latest solds and quarter snapshot for a TCGplayer product."""
    product_id = str(product_id or "").strip()
    if not re.fullmatch(r"\d{1,12}", product_id):
        return {
            "ok": False,
            "source": "tcgplayer_public_web",
            "product_id": product_id,
            "snapshot": None,
            "last_sales": [],
            "errors": ["Invalid TCGplayer product id"],
        }

    normalized_condition = normalize_tcgplayer_condition(selected_condition)
    selected_variant = str(selected_variant or "").strip()
    selected_language = str(selected_language or "").strip() or "English"
    url = tcgplayer_product_url(product_id, product_url)
    cache_key = "|".join([product_id, normalized_condition, selected_variant.lower(), selected_language.lower()])
    cached = _cache_get(cache_key)
    if cached:
        return cached

    errors: list[str] = []
    latest_sales_url = (
        f"https://mpapi.tcgplayer.com/v2/product/{product_id}/latestsales"
        f"?mpfev={TCGPLAYER_MPF_VERSION}"
    )
    history_url = f"https://infinite-api.tcgplayer.com/price/history/{product_id}/detailed?range=quarter"

    try:
        async with httpx.AsyncClient(headers=_browser_headers(url), timeout=12.0) as client:
            sales_response, history_response = await asyncio.gather(
                client.post(latest_sales_url, json={}),
                client.get(history_url),
            )
            sales_payload = await _response_json(sales_response, label="Latest sales", errors=errors)
            history_payload = await _response_json(history_response, label="3-month history", errors=errors)

            raw_history_rows = (history_payload or {}).get("result", []) if isinstance(history_payload, dict) else []
            if not isinstance(raw_history_rows, list):
                raw_history_rows = []
            selected_history = _best_matching_row(
                [row for row in raw_history_rows if isinstance(row, dict)],
                selected_condition=normalized_condition,
                selected_variant=selected_variant,
                selected_language=selected_language,
            )
            sku_id = str((selected_history or {}).get("skuId") or "").strip()
            pricepoints_payload: list[dict[str, Any]] = []
            volatility_payload: dict[str, Any] = {}
            if sku_id.isdigit():
                pricepoints_url = (
                    "https://mpgateway.tcgplayer.com/v1/pricepoints/marketprice/skus/search"
                    f"?mpfev={TCGPLAYER_MPF_VERSION}"
                )
                volatility_url = (
                    f"https://mpgateway.tcgplayer.com/v1/pricepoints/marketprice/skus/{sku_id}/volatility"
                    f"?mpfev={TCGPLAYER_MPF_VERSION}"
                )
                pricepoints_response, volatility_response = await asyncio.gather(
                    client.post(pricepoints_url, json={"skuIds": [int(sku_id)]}),
                    client.get(volatility_url),
                )
                parsed_pricepoints = await _response_json(
                    pricepoints_response,
                    label="Active listings",
                    errors=errors,
                )
                parsed_volatility = await _response_json(
                    volatility_response,
                    label="Volatility",
                    errors=errors,
                )
                if isinstance(parsed_pricepoints, list):
                    pricepoints_payload = [row for row in parsed_pricepoints if isinstance(row, dict)]
                if isinstance(parsed_volatility, dict):
                    volatility_payload = parsed_volatility

            payload = normalize_tcgplayer_sales_payload(
                product_id,
                selected_condition=normalized_condition,
                selected_variant=selected_variant,
                selected_language=selected_language,
                sales_payload=sales_payload if isinstance(sales_payload, dict) else {},
                history_payload=history_payload if isinstance(history_payload, dict) else {},
                pricepoints_payload=pricepoints_payload,
                volatility_payload=volatility_payload,
                product_url=url,
                errors=errors,
            )
    except httpx.HTTPError as exc:
        logger.warning("[tcgplayer_sales] TCGplayer sales lookup failed for %s: %s", product_id, exc)
        payload = normalize_tcgplayer_sales_payload(
            product_id,
            selected_condition=normalized_condition,
            selected_variant=selected_variant,
            selected_language=selected_language,
            product_url=url,
            errors=[f"TCGplayer request failed: {exc.__class__.__name__}"],
        )
    except Exception as exc:
        logger.exception("[tcgplayer_sales] Unexpected TCGplayer sales lookup error for %s", product_id)
        payload = normalize_tcgplayer_sales_payload(
            product_id,
            selected_condition=normalized_condition,
            selected_variant=selected_variant,
            selected_language=selected_language,
            product_url=url,
            errors=[f"TCGplayer lookup failed: {exc.__class__.__name__}"],
        )

    _cache_set(cache_key, payload)
    return copy.deepcopy(payload)
