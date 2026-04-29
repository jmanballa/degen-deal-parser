"""Supply deal finder catalog and marketplace search links."""
from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin

import httpx


@dataclass(frozen=True)
class SupplyDealItem:
    key: str
    label: str
    query: str
    unit_label: str
    buy_hint: str
    candidate_urls: tuple[str, ...] = ()


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
CACHE_PATH = Path("data") / "supply_deal_cache.json"


SUPPLY_DEAL_ITEMS: tuple[SupplyDealItem, ...] = (
    SupplyDealItem(
        key="boxes-6x6x6",
        label="6x6x6 boxes",
        query="6x6x6 shipping boxes corrugated",
        unit_label="box",
        buy_hint="Compare 25, 50, and 100 packs.",
        candidate_urls=(
            "https://www.ebay.com/itm/184611993882?_skw=6x6x+6+boxes&epid=8055134757&itmmeta=01KQBPEGX9AX6G70NSXK237XV6&hash=item2afbbb791a:g:eksAAeSwmBVpmOP8&itmprp=enc%3AAQALAAAA0GfYFPkwiKCW4ZNSs2u11xCUpBolx4MR6FFt%2BwbGwk3e3SPhjHmdgjCl5ouHprI5S8MWHKtcBSgvQxqmphGFujVQzYLMHEJNFQUQFpp%2FqbXGrXdh4ydfKfX5gbtdqzqUYDM%2BTXWxtAmSdeaQS9vnvjQ730PQjfA26pwU4pJy6mi1NnprQUgPcRyDwVId1BvyKLPmsf48Pn%2BCZZrscFkYmUziUhVoF7GbzVsAtjTb8W4CncFqMtm8YmxpyuuHXzhxn8nQtafgJm4%2Flt8M1AEI5yc%3D%7Ctkp%3ABk9SR_yOuva6Zw",
        ),
    ),
    SupplyDealItem(
        key="boxes-8x8x8",
        label="8x8x8 boxes",
        query="8x8x8 shipping boxes corrugated",
        unit_label="box",
        buy_hint="Usually cheaper in 50+ packs.",
    ),
    SupplyDealItem(
        key="packing-paper",
        label="Packing paper",
        query="packing paper sheets for moving shipping",
        unit_label="sheet",
        buy_hint="Use sheet count or pounds consistently.",
    ),
    SupplyDealItem(
        key="boxes-15x12x10",
        label="15x12x10 boxes",
        query="15x12x10 shipping boxes corrugated",
        unit_label="box",
        buy_hint="Watch dimensional weight and bundle size.",
    ),
    SupplyDealItem(
        key="boxes-12x10x8",
        label="12x10x8 boxes",
        query="12x10x8 shipping boxes corrugated",
        unit_label="box",
        buy_hint="Good middle-size default for bulk orders.",
    ),
    SupplyDealItem(
        key="mailers-6x10",
        label="6x10 bubble mailers",
        query="6x10 bubble mailers padded envelopes",
        unit_label="mailer",
        buy_hint="Compare #0 mailers if the listing uses mailer sizes.",
    ),
    SupplyDealItem(
        key="mailers-4x8",
        label="4x8 bubble mailers",
        query="4x8 bubble mailers padded envelopes",
        unit_label="mailer",
        buy_hint="Good for small singles and lightweight shipments.",
    ),
    SupplyDealItem(
        key="bubble-wrap",
        label="Bubble wrap",
        query="small bubble wrap roll shipping",
        unit_label="sq ft",
        buy_hint="Compare by square feet, not just roll length.",
    ),
    SupplyDealItem(
        key="packing-tape",
        label="Packing tape",
        query="packing tape rolls shipping clear 2 inch",
        unit_label="roll",
        buy_hint="Check roll length; 55 yd and 110 yd are not the same deal.",
    ),
    SupplyDealItem(
        key="scotch-tape",
        label="Scotch tape",
        query="Scotch tape refill rolls",
        unit_label="roll",
        buy_hint="Name-brand rolls vary a lot by pack size.",
    ),
)


def supply_item_by_key(key: str) -> SupplyDealItem | None:
    return next((item for item in SUPPLY_DEAL_ITEMS if item.key == key), None)


def marketplace_links(query: str) -> list[dict[str, str]]:
    encoded = quote_plus(query.strip())
    staples_query = quote_plus(query.replace("corrugated", "").strip())
    return [
        {
            "name": "eBay",
            "url": f"https://www.ebay.com/sch/i.html?_nkw={encoded}&LH_BIN=1&_sop=15",
            "hint": "Buy It Now, lowest price + shipping first",
        },
        {
            "name": "Amazon",
            "url": f"https://www.amazon.com/s?k={encoded}&s=price-asc-rank",
            "hint": "Price low to high search",
        },
        {
            "name": "Temu",
            "url": f"https://www.temu.com/search_result.html?search_key={encoded}",
            "hint": "Marketplace search",
        },
        {
            "name": "Walmart",
            "url": f"https://www.walmart.com/search?q={encoded}&sort=price_low",
            "hint": "Price low search with shipping badges",
        },
        {
            "name": "Staples",
            "url": f"https://www.staples.com/{staples_query}/directory_{staples_query}",
            "hint": "Business supplies search",
        },
        {
            "name": "The Boxery",
            "url": f"https://www.theboxery.com/SearchResults.asp?Search={encoded}",
            "hint": "Boxery site search",
        },
        {
            "name": "ValueMailers",
            "url": f"https://www.valuemailers.com/search?q={encoded}",
            "hint": "Bulk mailers and packaging search",
        },
        {
            "name": "Paper Mart",
            "url": f"https://www.papermart.com/search?q={encoded}",
            "hint": "Paper Mart search",
        },
        {
            "name": "Uline",
            "url": f"https://www.uline.com/Search?keywords={encoded}",
            "hint": "Uline catalog search",
        },
        {
            "name": "SupplyLand",
            "url": f"https://www.supplyland.com/search?query={encoded}",
            "hint": "SupplyLand search",
        },
    ]


def _ebay_search_urls(item: SupplyDealItem) -> list[str]:
    queries = [item.query]
    if item.key.startswith("boxes-") or item.key.startswith("mailers-"):
        queries.extend(
            [
                f"100 {item.query}",
                f"50 {item.query}",
                f"25 {item.query}",
            ]
        )
    elif item.key == "packing-tape":
        queries.extend([f"36 rolls {item.query}", f"24 rolls {item.query}"])
    elif item.key == "scotch-tape":
        queries.extend([f"12 rolls {item.query}", f"6 rolls {item.query}"])
    elif item.key == "packing-paper":
        queries.extend([f"500 sheets {item.query}", f"1000 sheets {item.query}"])

    urls: list[str] = []
    seen: set[str] = set()
    for query in queries:
        encoded = quote_plus(query.strip())
        url = f"https://www.ebay.com/sch/i.html?_nkw={encoded}&LH_BIN=1&_sop=15"
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def supply_deal_catalog() -> list[dict[str, object]]:
    return [
        {
            "key": item.key,
            "label": item.label,
            "query": item.query,
            "unit_label": item.unit_label,
            "buy_hint": item.buy_hint,
            "links": marketplace_links(item.query),
        }
        for item in SUPPLY_DEAL_ITEMS
    ]


def _clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _parse_money(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", value)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"([0-9][0-9,]*)", value)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def estimate_units(title: str, unit_label: str) -> int | None:
    lowered = title.lower()
    def valid_count(raw_value: str) -> int | None:
        value = int(raw_value)
        if 0 < value <= 5000:
            return value
        return None

    patterns = (
        r"^\s*(\d{1,5})\s+\d+(?:\.\d+)?\s*x\s*\d+(?:\.\d+)?\s*x\s*\d+(?:\.\d+)?",
        r"(?:pack|case|set|bundle|box|carton)\s+of\s+(\d{1,5})\b",
        r"\b(\d{1,5})\s*/\s*(?:pack|carton|case|box)\b",
        r"\b(\d{1,5})\s*(?:pack|pk|count|ct|pc|pcs|piece|pieces|sheets|rolls|mailer|mailers|boxes|box)\b",
        r"\b(\d{1,5})\s*-\s*(?:pack|count|ct|piece|roll)",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if match:
            value = valid_count(match.group(1))
            if value is not None:
                return value
    if unit_label == "mailer":
        mailer_patterns = (
            r"\b(\d{1,5})\s*#\d+\b",
            r"\b(\d{1,5})\s+(?:poly|kraft|white|black|purple|pink|blue|bubble|padded)\b",
            r"^\s*(\d{1,5})\s+(?:poly|kraft|white|black|purple|pink|blue|bubble|padded)\b",
            r"\((\d{1,5})\)\s*$",
        )
        for pattern in mailer_patterns:
            match = re.search(pattern, lowered)
            if match:
                value = valid_count(match.group(1))
                if value is not None:
                    return value
    if unit_label == "sq ft":
        sq_ft_match = re.search(r"(\d{2,5})\s*(?:sq\.?\s*ft|square\s+feet)", lowered)
        if sq_ft_match:
            return int(sq_ft_match.group(1))
        roll_match = re.search(r"(\d{1,3})(?:\"|in|inch)\s*x\s*(\d{2,5})(?:'|ft|feet)", lowered)
        if roll_match:
            width_inches = int(roll_match.group(1))
            length_feet = int(roll_match.group(2))
            return max(1, round((width_inches / 12) * length_feet))
    return None


def _best_unit_count(title: str, unit_label: str, fallback: int | None = None) -> int | None:
    return estimate_units(title, unit_label) or fallback


def _normalize_listing_units(row: dict[str, Any], item: SupplyDealItem) -> dict[str, Any]:
    title = str(row.get("title") or "")
    if row.get("units") is None or int(row.get("units") or 0) > 5000:
        row["units"] = _best_unit_count(title, item.unit_label)
        row["unit_price"] = None
    row.setdefault("unit_label", item.unit_label)
    if row.get("unit_price") is None and row.get("units"):
        delivered = row.get("delivered_price")
        if delivered is None:
            delivered = (row.get("price") or 0) + (row.get("shipping") or 0)
            row["delivered_price"] = delivered
        try:
            row["unit_price"] = float(delivered) / int(row["units"])
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return row


def _looks_like_quantity_variation(title: str) -> bool:
    lowered = title.lower()
    if re.match(r"^\s*\d{1,5}\s+\d{1,5}\s+\d{1,5}\b", lowered):
        return True
    leading_counts = re.findall(r"^\s*((?:\d{1,5}\s+){2,}\d{1,5})\s+(?:pieces|pcs|pack|count|ct)\b", lowered)
    if leading_counts:
        return True
    return "choose quantity" in lowered or "select quantity" in lowered


def _score_listing(row: dict[str, Any]) -> tuple[float, int, float]:
    unit_price = row.get("unit_price")
    if unit_price is None:
        unit_price = 999999.0
    rating_count = int(row.get("rating_count") or 0)
    rating = float(row.get("rating") or 0)
    risk_penalty = {"high": 9999, "medium": 2, "low": 0}.get(str(row.get("risk_level") or "medium"), 2)
    return (risk_penalty, float(unit_price), -rating_count, -rating)


def _median(values: list[float]) -> float | None:
    values = sorted(value for value in values if value > 0)
    if not values:
        return None
    midpoint = len(values) // 2
    if len(values) % 2:
        return values[midpoint]
    return (values[midpoint - 1] + values[midpoint]) / 2


def _market_reference_unit_price(rows: list[dict[str, Any]]) -> float | None:
    values = sorted(
        float(row["unit_price"])
        for row in rows
        if row.get("unit_price") is not None
        and (row.get("is_saved_deal") or int(row.get("sold_count") or 0) >= 100)
    )
    if values:
        return values[0]
    values = sorted(float(row["unit_price"]) for row in rows if row.get("unit_price") is not None)
    return values[0] if values else None


def _add_reliability(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    source_trust = {
        "Amazon": 14,
        "Walmart": 14,
        "Staples": 18,
        "eBay": 4,
    }
    median_unit = _median([float(row["unit_price"]) for row in rows if row.get("unit_price") is not None])
    reference_unit = _market_reference_unit_price(rows)
    for row in rows:
        score = source_trust.get(str(row.get("source")), 6)
        reasons: list[str] = []
        warnings: list[str] = []

        rating = row.get("rating")
        rating_count = int(row.get("rating_count") or 0)
        if rating_count >= 250:
            score += 10
            reasons.append(f"{rating_count:,} reviews")
        elif rating_count >= 25:
            score += 6
            reasons.append(f"{rating_count:,} reviews")
        elif rating_count > 0:
            score -= 6
            warnings.append(f"Only {rating_count:,} review{'s' if rating_count != 1 else ''}")
        else:
            score -= 8
            warnings.append("No review data")

        if rating is not None:
            if float(rating) >= 4.5:
                score += 8
                reasons.append(f"{float(rating):.1f} stars")
            elif float(rating) >= 4.0:
                score += 4
                reasons.append(f"{float(rating):.1f} stars")
            else:
                score -= 14
                warnings.append(f"Low rating: {float(rating):.1f} stars")

        if row.get("shipping_speed"):
            score += 4
            reasons.append("Shipping speed listed")
        else:
            score -= 5
            warnings.append("Shipping speed missing")

        if row.get("shipping") is None and not row.get("shipping_label"):
            score -= 4
            warnings.append("Shipping price unclear")
        else:
            score += 3

        unit_price = row.get("unit_price")
        if reference_unit and unit_price and float(unit_price) > reference_unit * 1.65:
            score -= 20
            warnings.append("Expensive versus the best parsed unit price")
        if median_unit and unit_price:
            has_strong_seller_proof = (
                row.get("seller_positive") is not None
                and float(row.get("seller_positive") or 0) >= 98
                and int(row.get("sold_count") or 0) >= 100
            )
            if float(unit_price) < median_unit * 0.45 and not has_strong_seller_proof:
                score -= 18
                warnings.append("Price is far below market; verify listing before ordering")
            elif float(unit_price) <= median_unit * 0.85:
                score += 5
                reasons.append("Below typical unit cost")

        if row.get("units") is None:
            score -= 5
            warnings.append("Pack size not detected")

        seller_positive = row.get("seller_positive")
        if seller_positive is not None:
            seller_positive = float(seller_positive)
            if seller_positive >= 99:
                score += 10
                reasons.append(f"{seller_positive:.1f}% seller positive")
            elif seller_positive >= 97:
                score += 6
                reasons.append(f"{seller_positive:.1f}% seller positive")
            else:
                score -= 8
                warnings.append(f"Seller positive only {seller_positive:.1f}%")
        sold_count = row.get("sold_count")
        if sold_count:
            score += 3
            reasons.append(f"{int(sold_count):,}+ sold")
            if int(sold_count) >= 1000:
                score += 8
                reasons.append("High-volume seller")
        if row.get("is_saved_deal"):
            score += 12
            reasons.append("Saved supply deal")

        if score >= 28:
            risk_level = "low"
            badge = "Reliable"
        elif score >= 5:
            risk_level = "medium"
            badge = "Check details"
        else:
            risk_level = "high"
            badge = "Risky"
        row["reliability_score"] = score
        row["risk_level"] = risk_level
        row["reliability_badge"] = badge
        row["reliability_reasons"] = reasons[:3]
        row["risk_warnings"] = warnings[:4]
    ranked = sorted(rows, key=_score_listing)
    best_low_risk = next((row for row in ranked if row.get("risk_level") == "low"), None)
    if best_low_risk is not None:
        best_low_risk["reliability_badge"] = "Best reliable value"
    return rows


def _title_matches_item(title: str, item: SupplyDealItem) -> bool:
    lowered = title.lower()
    compact = re.sub(r"[\s\"'.,-]+", "", lowered)
    if _looks_like_quantity_variation(title):
        return False
    if item.key.startswith("boxes-"):
        dims = item.key.removeprefix("boxes-")
        target_dim = dims.replace("x", "")
        found_dims = {
            "".join(match)
            for match in re.findall(
                r"\b(\d{1,2})(?:\.\d+)?\s*x\s*(\d{1,2})(?:\.\d+)?\s*x\s*(\d{1,2})(?:\.\d+)?\b",
                lowered,
            )
        }
        if found_dims and any(found_dim != target_dim for found_dim in found_dims):
            return False
        compact_no_x = compact.replace("x", "")
        return (dims in compact or target_dim in compact_no_x) and "box" in lowered
    if item.key == "mailers-6x10":
        found_dims = [
            (float(width), float(height))
            for width, height in re.findall(r"\b(\d{1,2}(?:\.\d+)?)\s*x\s*(\d{1,2}(?:\.\d+)?)\b", lowered)
        ]
        has_target_dim = any(5.8 <= width <= 6.7 and 9.8 <= height <= 10.3 for width, height in found_dims)
        has_wrong_dim = any(not (5.8 <= width <= 6.7 and 9.8 <= height <= 10.3) for width, height in found_dims)
        return ((has_target_dim and not has_wrong_dim) or ("#0" in lowered and not has_wrong_dim)) and "mailer" in lowered
    if item.key == "mailers-4x8":
        found_dims = [
            (float(width), float(height))
            for width, height in re.findall(r"\b(\d{1,2}(?:\.\d+)?)\s*x\s*(\d{1,2}(?:\.\d+)?)\b", lowered)
        ]
        has_target_dim = any(3.8 <= width <= 4.3 and 7.8 <= height <= 8.3 for width, height in found_dims)
        has_wrong_dim = any(not (3.8 <= width <= 4.3 and 7.8 <= height <= 8.3) for width, height in found_dims)
        return has_target_dim and not has_wrong_dim and ("mailer" in lowered or "envelope" in lowered)
    if item.key == "packing-paper":
        return "paper" in lowered and ("packing" in lowered or "shipping" in lowered)
    if item.key == "bubble-wrap":
        return "bubble" in lowered and ("wrap" in lowered or "cushion" in lowered)
    if item.key == "packing-tape":
        return "tape" in lowered and ("packing" in lowered or "shipping" in lowered or "packaging" in lowered)
    if item.key == "scotch-tape":
        return "tape" in lowered and ("scotch" in lowered or "transparent" in lowered or "office" in lowered)
    return True


def _dedupe_and_rank(
    rows: list[dict[str, Any]],
    limit: int,
    *,
    item: SupplyDealItem | None = None,
    balance_sources: bool = False,
) -> list[dict[str, Any]]:
    def row_identity(row: dict[str, Any]) -> str:
        return f"{row.get('source')}:{str(row.get('title') or '').lower()}"

    seen: set[str] = set()
    unique = []
    for row in rows:
        if item is not None:
            row = _normalize_listing_units(row, item)
        if item is not None and not _title_matches_item(str(row.get("title") or ""), item):
            continue
        if item is not None and row.get("unit_price") is None:
            continue
        identity = row_identity(row)
        if not identity or identity in seen:
            continue
        seen.add(str(identity))
        unique.append(row)
    ranked = sorted(_add_reliability(unique), key=_score_listing)
    if not balance_sources:
        return ranked[:limit]

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    per_source_target = max(2, min(8, limit // 6))
    if ranked:
        first_identity = row_identity(ranked[0])
        selected.append(ranked[0])
        selected_ids.add(first_identity)
    sources = sorted({str(row.get("source") or "") for row in ranked if row.get("source")})
    for source in sources:
        source_rows = [row for row in ranked if row.get("source") == source]
        for row in source_rows[:per_source_target]:
            identity = row_identity(row)
            if identity not in selected_ids:
                selected.append(row)
                selected_ids.add(identity)
    for row in ranked:
        if len(selected) >= limit:
            break
        identity = row_identity(row)
        if identity not in selected_ids:
            selected.append(row)
            selected_ids.add(identity)
    return selected[:limit]


def _listing_identity(row: dict[str, Any]) -> str:
    url = str(row.get("url") or "").split("?", 1)[0].strip().lower()
    if url:
        return f"{row.get('source')}:{url}"
    return f"{row.get('source')}:{str(row.get('title') or '').strip().lower()}"


def _read_cache() -> dict[str, Any]:
    try:
        if not CACHE_PATH.exists():
            return {}
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_cache(cache: dict[str, Any]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CACHE_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(CACHE_PATH)


def get_cached_supply_deals(item: SupplyDealItem) -> dict[str, Any] | None:
    cached = _read_cache().get(item.key)
    if not isinstance(cached, dict):
        return None
    cached["listings"] = _dedupe_and_rank(cached.get("listings", []) or [], 48, item=item, balance_sources=True)
    cached["from_cache"] = True
    cached["refreshing"] = False
    cached.setdefault("cache_status", "Showing saved results")
    return cached


def _merge_cached_and_live(
    item: SupplyDealItem,
    cached: dict[str, Any] | None,
    live: dict[str, Any],
    *,
    limit: int,
) -> dict[str, Any]:
    combined: dict[str, dict[str, Any]] = {}
    for row in (cached or {}).get("listings", []) or []:
        if isinstance(row, dict):
            old_row = dict(row)
            old_row.setdefault("cached_listing", True)
            combined[_listing_identity(old_row)] = old_row
    for row in live.get("listings", []) or []:
        if isinstance(row, dict):
            new_row = dict(row)
            new_row["cached_listing"] = False
            combined[_listing_identity(new_row)] = new_row
    listings = _dedupe_and_rank(list(combined.values()), limit, item=item, balance_sources=True)
    merged = dict(live)
    merged["listings"] = listings
    merged["from_cache"] = False
    merged["refreshing"] = False
    merged["cache_status"] = "Updated just now"
    merged["cached_at"] = datetime.now(timezone.utc).isoformat()
    return merged


async def refresh_supply_deal_cache(item: SupplyDealItem, *, limit: int = 48) -> dict[str, Any]:
    cached = get_cached_supply_deals(item)
    live = await lookup_supply_deals(item, limit=limit)
    merged = _merge_cached_and_live(item, cached, live, limit=limit)
    cache = _read_cache()
    cache[item.key] = merged
    _write_cache(cache)
    return merged


def _parse_amazon_results(raw_html: str, item: SupplyDealItem, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocks = raw_html.split('data-component-type="s-search-result"')
    for block in blocks[1:]:
        chunk = block[:18000]
        asin_match = re.search(r'data-asin="([^"]+)"', chunk)
        title_match = re.search(r"<h2[^>]*aria-label=\"([^\"]+)\"", chunk)
        price_match = re.search(r'<span class="a-offscreen">(\$[0-9][^<]+)</span>', chunk)
        href_match = re.search(r'<a[^>]+href="([^"]*/dp/[^"]+)"[^>]*>\s*<h2', chunk)
        rating_match = re.search(r"([0-9.]+)\s+out of 5 stars", html.unescape(chunk))
        count_match = re.search(r'aria-label="([0-9,]+)\s+ratings?"', chunk)
        delivery_parts = [
            _clean_text(part)
            for part in re.findall(
                r'<div class="a-row a-color-base udm-[^"]*delivery-message"><div class="a-column a-span12">(.*?)</div></div>',
                chunk,
                re.S,
            )
        ]
        shipping_text = " | ".join(part for part in delivery_parts if part)
        shipping_lower = shipping_text.lower()
        if "free delivery" in shipping_lower:
            shipping = 0.0
        elif "delivery" in shipping_lower:
            shipping = _parse_money(shipping_text)
        else:
            shipping = None
        if not (asin_match and title_match and price_match):
            continue
        title = _clean_text(title_match.group(1))
        price = _parse_money(price_match.group(1))
        if not title or price is None:
            continue
        units = _best_unit_count(title, item.unit_label)
        unit_price = (price / units) if units else None
        href = href_match.group(1) if href_match else f"/dp/{asin_match.group(1)}"
        rows.append(
            {
                "source": "Amazon",
                "title": title,
                "price": price,
                "shipping": shipping,
                "shipping_label": "Free" if shipping == 0.0 else (f"${shipping:.2f}" if shipping else None),
                "shipping_speed": shipping_text or None,
                "delivered_price": price + (shipping or 0),
                "units": units,
                "unit_label": item.unit_label,
                "unit_price": ((price + (shipping or 0)) / units) if units else unit_price,
                "rating": float(rating_match.group(1)) if rating_match else None,
                "rating_count": _parse_int(count_match.group(1) if count_match else None),
                "url": urljoin("https://www.amazon.com", html.unescape(href)),
            }
        )
    return _dedupe_and_rank(rows, limit, item=item)


def _parse_ebay_results(raw_html: str, item: SupplyDealItem, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for block in re.findall(r'<li[^>]+class="[^"]*s-item[^"]*"[^>]*>(.*?)</li>', raw_html, re.S):
        title_match = re.search(r'<div[^>]+class="[^"]*s-item__title[^"]*"[^>]*>(.*?)</div>', block, re.S)
        price_match = re.search(r'<span[^>]+class="[^"]*s-item__price[^"]*"[^>]*>(.*?)</span>', block, re.S)
        href_match = re.search(r'<a[^>]+class="[^"]*s-item__link[^"]*"[^>]+href="([^"]+)"', block)
        shipping_match = re.search(r'<span[^>]+class="[^"]*s-item__shipping[^"]*"[^>]*>(.*?)</span>', block, re.S)
        if not (title_match and price_match and href_match):
            continue
        title = _clean_text(title_match.group(1))
        if not title or title.lower() == "shop on ebay":
            continue
        price = _parse_money(_clean_text(price_match.group(1)))
        shipping_text = _clean_text(shipping_match.group(1)) if shipping_match else ""
        shipping = 0.0 if "free" in shipping_text.lower() else _parse_money(shipping_text)
        if price is None:
            continue
        delivered = price + (shipping or 0)
        units = _best_unit_count(title, item.unit_label)
        rows.append(
            {
                "source": "eBay",
                "title": title,
                "price": price,
                "shipping": shipping,
                "shipping_label": "Free" if shipping == 0.0 else (f"${shipping:.2f}" if shipping else None),
                "shipping_speed": shipping_text or None,
                "delivered_price": delivered,
                "units": units,
                "unit_label": item.unit_label,
                "unit_price": (delivered / units) if units else None,
                "rating": None,
                "rating_count": None,
                "url": html.unescape(href_match.group(1)),
            }
        )
    return _dedupe_and_rank(rows, limit, item=item)


def _parse_ebay_browser_text_rows(browser_rows: list[dict[str, str]], item: SupplyDealItem, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for scraped in browser_rows:
        title = _clean_text(scraped.get("alt") or "")
        text = _clean_text(scraped.get("text") or "")
        href = scraped.get("href") or ""
        if not title or title.lower() == "shop on ebay" or not href:
            continue
        text_for_parse = text
        title_index = text.lower().find(title.lower()[:60])
        if title_index >= 0:
            text_for_parse = text[title_index : title_index + 700]
        price = _parse_money(text_for_parse)
        if price is None:
            continue
        shipping_match = re.search(r"(free delivery[^|]*|free shipping[^|]*|\$[0-9.,]+\s+(?:delivery|shipping)[^|]*)", text_for_parse, re.I)
        shipping_text = _clean_text(shipping_match.group(1)) if shipping_match else None
        shipping = 0.0 if shipping_text and "free" in shipping_text.lower() else _parse_money(shipping_text)
        units = _best_unit_count(title, item.unit_label)
        seller_match = re.search(r"([0-9.]+)%\s+positive", text_for_parse, re.I)
        sold_match = re.search(r"([0-9][0-9,.]*)\+?\s+sold", text_for_parse, re.I)
        delivered = price + (shipping or 0)
        rows.append(
            {
                "source": "eBay",
                "title": title,
                "price": price,
                "shipping": shipping,
                "shipping_label": "Free" if shipping == 0.0 else (f"${shipping:.2f}" if shipping else None),
                "shipping_speed": shipping_text,
                "delivered_price": delivered,
                "units": units,
                "unit_label": item.unit_label,
                "unit_price": (delivered / units) if units else None,
                "rating": None,
                "rating_count": None,
                "seller_positive": float(seller_match.group(1)) if seller_match else None,
                "sold_count": _parse_int(sold_match.group(1) if sold_match else None),
                "url": href,
            }
        )
    return _dedupe_and_rank(rows, limit, item=item)


async def _browser_scrape_ebay(url: str, item: SupplyDealItem, limit: int) -> list[dict[str, Any]]:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        return []
    browser = None
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent=USER_AGENT,
                locale="en-US",
                viewport={"width": 1365, "height": 900},
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=25000)
            await page.wait_for_timeout(2500)
            browser_rows = await page.evaluate(
                """
                () => Array.from(new Set(Array.from(document.querySelectorAll('a[href*="/itm/"]')).map(a => a.href)))
                    .slice(0, 32)
                    .map((href) => {
                        const a = Array.from(document.querySelectorAll(`a[href="${href}"]`))[0];
                        const img = a?.querySelector('img') || a?.parentElement?.querySelector('img');
                        let chosen = "";
                        let node = a;
                        for (let i = 0; i < 14 && node; i += 1, node = node.parentElement) {
                            const text = (node.innerText || "").trim();
                            if (text.includes("$")) {
                                chosen = text;
                                break;
                            }
                            if (text.length > chosen.length) chosen = text;
                        }
                        return { href, alt: img?.alt || "", text: chosen.slice(0, 1800) };
                    })
                """
            )
            await browser.close()
            return _parse_ebay_browser_text_rows(browser_rows, item, limit)
    except Exception:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
        return []


async def _browser_scrape_ebay_item(url: str, item: SupplyDealItem) -> dict[str, Any] | None:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        return None
    browser = None
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent=USER_AGENT,
                locale="en-US",
                viewport={"width": 1365, "height": 900},
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=25000)
            await page.wait_for_timeout(2500)
            page_title = await page.title()
            body_text = await page.locator("body").inner_text(timeout=8000)
            await browser.close()
        title = _clean_text(page_title.split("| eBay", 1)[0])
        if not title or not _title_matches_item(title, item):
            return None
        price_match = re.search(r"US\s*\$([0-9][0-9,.]*)", body_text)
        if price_match is None:
            price_match = re.search(r"\$([0-9][0-9,.]*)", body_text)
        price = _parse_money(price_match.group(0) if price_match else None)
        if price is None:
            return None
        shipping_match = re.search(r"(free delivery|free shipping|\$[0-9.,]+\s+(?:delivery|shipping))", body_text, re.I)
        shipping_text = _clean_text(shipping_match.group(1)) if shipping_match else None
        shipping = 0.0 if shipping_text and "free" in shipping_text.lower() else _parse_money(shipping_text)
        units = _best_unit_count(title, item.unit_label)
        seller_match = re.search(r"([0-9.]+)%\s*positive", body_text, re.I)
        sold_values = [
            _parse_int(value)
            for value in re.findall(r"([0-9][0-9,]*)\s+sold", body_text, re.I)
        ]
        sold_values = [value for value in sold_values if value is not None]
        delivered = price + (shipping or 0)
        return {
            "source": "eBay",
            "title": title,
            "price": price,
            "shipping": shipping,
            "shipping_label": "Free" if shipping == 0.0 else (f"${shipping:.2f}" if shipping else None),
            "shipping_speed": shipping_text,
            "delivered_price": delivered,
            "units": units,
            "unit_label": item.unit_label,
            "unit_price": (delivered / units) if units else None,
            "rating": None,
            "rating_count": None,
            "seller_positive": float(seller_match.group(1)) if seller_match else None,
            "sold_count": max(sold_values) if sold_values else None,
            "url": url,
        }
    except Exception:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
    return None


async def _browser_scrape_generic_source(
    source: str,
    url: str,
    item: SupplyDealItem,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        return [], "Browser scrape unavailable"
    browser = None
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent=USER_AGENT,
                locale="en-US",
                viewport={"width": 1365, "height": 900},
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=18000)
            await page.wait_for_timeout(2500)
            title = await page.title()
            body_text = await page.locator("body").inner_text(timeout=6000)
            browser_rows = await page.evaluate(
                """
                () => Array.from(document.querySelectorAll('a[href]'))
                    .map((a) => {
                        let node = a;
                        let chosen = (a.innerText || "").trim();
                        for (let i = 0; i < 8 && node; i += 1, node = node.parentElement) {
                            const text = (node.innerText || "").trim();
                            if (text.includes("$") && text.length < 1200) {
                                chosen = text;
                                break;
                            }
                        }
                        return { href: a.href, text: chosen };
                    })
                    .filter((row) => row.href && row.text && row.text.includes("$"))
                    .slice(0, 40)
                """
            )
            await browser.close()
    except Exception as exc:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
        return [], f"Browser scrape failed: {exc.__class__.__name__}"

    lowered_body = body_text.lower()
    lowered_title = title.lower()
    if "cloudflare" in lowered_title or "blocked" in lowered_body or "access denied" in lowered_title:
        return [], "Browser checked; site blocked automation"
    if "page not found" in lowered_title or "page not found" in lowered_body:
        return [], "Browser checked; supplier search page returned not found"

    parsed: list[dict[str, Any]] = []
    for row in browser_rows:
        text = _clean_text(row.get("text") or "")
        if not text:
            continue
        lines = [line.strip() for line in re.split(r"\s{2,}|\n|\|", text) if line.strip()]
        title = next((line for line in lines if "$" not in line and len(line) > 8), "")
        if not title or not _title_matches_item(title, item):
            continue
        price = _parse_money(text)
        if price is None:
            continue
        shipping_text = None
        shipping = None
        shipping_match = re.search(r"(free shipping|free delivery|\$[0-9.,]+\s+(?:shipping|delivery)|ships? [^|]{0,80})", text, re.I)
        if shipping_match:
            shipping_text = _clean_text(shipping_match.group(1))
            shipping = 0.0 if "free" in shipping_text.lower() else _parse_money(shipping_text)
        units = _best_unit_count(title, item.unit_label)
        delivered = price + (shipping or 0)
        parsed.append(
            {
                "source": source,
                "title": title,
                "price": price,
                "shipping": shipping,
                "shipping_label": "Free" if shipping == 0.0 else (f"${shipping:.2f}" if shipping else None),
                "shipping_speed": shipping_text,
                "delivered_price": delivered,
                "units": units,
                "unit_label": item.unit_label,
                "unit_price": (delivered / units) if units else None,
                "rating": None,
                "rating_count": None,
                "url": row.get("href") or url,
            }
        )
    parsed = _dedupe_and_rank(parsed, limit, item=item)
    if parsed:
        return parsed, f"{len(parsed)} listings parsed by browser"
    if body_text.strip():
        return [], "Browser checked; no matching product cards found"
    return [], "Browser checked; no readable page content"


def _saved_deal_fallback(url: str, item: SupplyDealItem) -> dict[str, Any] | None:
    if item.key == "boxes-6x6x6" and "184611993882" in url:
        price = 36.40
        units = 100
        return {
            "source": "eBay",
            "title": "100 6x6x6 Cardboard Paper Boxes Mailing Packing Shipping Box Corrugated",
            "price": price,
            "shipping": 0.0,
            "shipping_label": "Free",
            "shipping_speed": "Free delivery listed on saved eBay deal",
            "delivered_price": price,
            "units": units,
            "unit_label": item.unit_label,
            "unit_price": price / units,
            "rating": None,
            "rating_count": None,
            "seller_positive": 100.0,
            "sold_count": 9154,
            "url": url,
            "is_saved_deal": True,
        }
    return None


def _parse_walmart_results(raw_html: str, item: SupplyDealItem, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocks = re.split(r'<div role="group" data-item-id=', raw_html)[1:]
    for raw_block in blocks:
        block = raw_block[:22000]
        item_id = raw_block.split('"', 1)[0]
        title_match = re.search(r'data-automation-id="product-title"[^>]*>(.*?)</h3>', block, re.S)
        if not title_match:
            title_match = re.search(r'<h3>(.*?)</h3>', block, re.S)
        href_match = re.search(r'<a[^>]+href="([^"]+)"', block)
        price_match = re.search(r'current price\s*(\$[0-9][0-9.,]*)', _clean_text(block))
        unit_price_match = re.search(r"([0-9.]+)\s*¢/ea|\$([0-9.]+)\s*/\s*ea", _clean_text(block), re.I)
        fulfillment_parts = [
            _clean_text(part)
            for part in re.findall(r'<div class="ff-text-wrapper">(.*?)</div>', block, re.S)
        ]
        shipping_text = " | ".join(part for part in fulfillment_parts if part)
        if not (title_match and price_match):
            continue
        title = _clean_text(title_match.group(1))
        price = _parse_money(price_match.group(1))
        units = _best_unit_count(title, item.unit_label)
        parsed_unit_price = None
        if unit_price_match:
            if unit_price_match.group(1):
                parsed_unit_price = float(unit_price_match.group(1)) / 100
            elif unit_price_match.group(2):
                parsed_unit_price = float(unit_price_match.group(2))
        if price is None:
            continue
        shipping = 0.0 if "free shipping" in shipping_text.lower() or "shipping arrives" in shipping_text.lower() else None
        rows.append(
            {
                "source": "Walmart",
                "title": title,
                "price": price,
                "shipping": shipping,
                "shipping_label": "Free/see page" if shipping == 0.0 else None,
                "shipping_speed": shipping_text or None,
                "delivered_price": price + (shipping or 0),
                "units": units,
                "unit_label": item.unit_label,
                "unit_price": parsed_unit_price or ((price + (shipping or 0)) / units if units else None),
                "rating": None,
                "rating_count": None,
                "url": urljoin("https://www.walmart.com", html.unescape(href_match.group(1) if href_match else f"/ip/{item_id}")),
            }
        )
    return _dedupe_and_rank(rows, limit, item=item)


def _parse_staples_results(raw_html: str, item: SupplyDealItem, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    blocks = re.split(r'<div id="" data-id="tile-', raw_html)[1:]
    for raw_block in blocks:
        block = raw_block[:24000]
        title_match = re.search(r'class="standard-tile__title[^"]*"[^>]+href="([^"]+)">(.*?)</a>', block, re.S)
        price_match = re.search(r'standard-tile__final_price.*?(\$[0-9][0-9.,]*)', block, re.S)
        unit_match = re.search(r"Price per unit\s*(\$[0-9.]+)\s*/\s*([^<]+)", _clean_text(block), re.I)
        count_match = re.search(r"([0-9,]+)\s+Reviews?", _clean_text(block), re.I)
        delivery_match = re.search(
            r'standard-tile__delivery_date_wrapper.*?<span[^>]*>([^<]*delivery[^<]*)</span>.*?<span class="standard-tile__delivery_info">(.*?)</span>',
            block,
            re.S | re.I,
        )
        if not (title_match and price_match):
            continue
        title = _clean_text(title_match.group(2))
        price = _parse_money(price_match.group(1))
        if price is None:
            continue
        units = _best_unit_count(title, item.unit_label)
        unit_price = _parse_money(unit_match.group(1) if unit_match else None)
        shipping_label = None
        shipping_speed = None
        if delivery_match:
            delivery_label = _clean_text(delivery_match.group(1))
            delivery_date = _clean_text(delivery_match.group(2))
            shipping_label = "Free" if "free" in delivery_label.lower() else None
            shipping_speed = f"{delivery_label} {delivery_date}".strip()
        rows.append(
            {
                "source": "Staples",
                "title": title,
                "price": price,
                "shipping": 0.0 if shipping_label == "Free" else None,
                "shipping_label": shipping_label,
                "shipping_speed": shipping_speed,
                "delivered_price": price,
                "units": units,
                "unit_label": item.unit_label,
                "unit_price": unit_price or (price / units if units else None),
                "rating": None,
                "rating_count": _parse_int(count_match.group(1) if count_match else None),
                "url": urljoin("https://www.staples.com", html.unescape(title_match.group(1))),
            }
        )
    return _dedupe_and_rank(rows, limit, item=item)


async def _fetch_text(client: httpx.AsyncClient, url: str) -> tuple[int, str]:
    response = await client.get(url, headers=REQUEST_HEADERS)
    return response.status_code, response.text


async def lookup_supply_deals(item: SupplyDealItem, *, limit: int = 48) -> dict[str, Any]:
    links = marketplace_links(item.query)
    status_by_source: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=18, follow_redirects=True) as client:
        parsers = {
            "Amazon": _parse_amazon_results,
            "eBay": _parse_ebay_results,
            "Walmart": _parse_walmart_results,
            "Staples": _parse_staples_results,
        }
        for link in links:
            source = link["name"]
            parser = parsers.get(source)
            if parser is None:
                continue
            source_urls = _ebay_search_urls(item) if source == "eBay" else [link["url"]]
            source_rows: list[dict[str, Any]] = []
            source_statuses: list[str] = []
            for source_url in source_urls:
                try:
                    status_code, body = await _fetch_text(client, source_url)
                except Exception as exc:
                    source_statuses.append(f"fetch failed: {exc.__class__.__name__}")
                    if source == "eBay":
                        browser_rows = await _browser_scrape_ebay(source_url, item, limit)
                        if browser_rows:
                            source_rows.extend(browser_rows)
                            source_statuses.append(f"{len(browser_rows)} browser listings")
                    continue
                if status_code >= 400:
                    source_statuses.append(f"HTTP {status_code}")
                    if source == "eBay":
                        browser_rows = await _browser_scrape_ebay(source_url, item, limit)
                        if browser_rows:
                            source_rows.extend(browser_rows)
                            source_statuses.append(f"{len(browser_rows)} browser listings")
                    continue
                parsed = parser(body, item, limit)
                if parsed:
                    source_rows.extend(parsed)
                    source_statuses.append(f"{len(parsed)} listings")
                elif source == "eBay":
                    browser_rows = await _browser_scrape_ebay(source_url, item, limit)
                    if browser_rows:
                        source_rows.extend(browser_rows)
                        source_statuses.append(f"{len(browser_rows)} browser listings")
                    else:
                        source_statuses.append("no parseable listings")
                else:
                    source_statuses.append("no parseable listings")
            if source_rows:
                deduped_source_rows = _dedupe_and_rank(source_rows, limit, item=item)
                rows.extend(deduped_source_rows)
                if source == "eBay" and len(source_urls) > 1:
                    status_by_source[source] = (
                        f"{len(deduped_source_rows)} listings parsed across {len(source_urls)} eBay searches"
                    )
                else:
                    status_by_source[source] = f"{len(deduped_source_rows)} listings parsed"
            else:
                status_by_source[source] = "; ".join(source_statuses[:3]) or "No parseable listings returned"
        candidate_count = 0
        for candidate_url in item.candidate_urls:
            if "ebay.com/itm/" in candidate_url:
                candidate = await _browser_scrape_ebay_item(candidate_url, item)
                if candidate is None:
                    candidate = _saved_deal_fallback(candidate_url, item)
                if candidate:
                    rows.append(candidate)
                    candidate_count += 1
        if candidate_count:
            existing = status_by_source.get("eBay", "")
            suffix = f"{candidate_count} saved deal checked"
            status_by_source["eBay"] = f"{existing}; {suffix}" if existing else suffix
        for link in links:
            source = link["name"]
            if source in parsers or source == "Temu":
                continue
            parsed, status = await _browser_scrape_generic_source(source, link["url"], item, max(4, limit // 2))
            if parsed:
                rows.extend(parsed)
            status_by_source[source] = status
    for link in links:
        status_by_source.setdefault(link["name"], "Open site search")
    return {
        "item": {
            "key": item.key,
            "label": item.label,
            "query": item.query,
            "unit_label": item.unit_label,
            "buy_hint": item.buy_hint,
        },
        "links": links,
        "listings": _dedupe_and_rank(rows, limit, item=item, balance_sources=True),
        "sources": status_by_source,
        "looked_up_at": datetime.now(timezone.utc).isoformat(),
    }
