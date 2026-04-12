"""
Graded slab certificate lookup + last-sold price data.

Supports PSA, BGS (Beckett), CGC, and SGC cert numbers.
Fetches last-sold prices from 130point and Card Ladder, then suggests a
recency-weighted median price.

PSA:  Official API (free key from psacard.com/api) with HTML scrape fallback.
BGS:  HTML scrape from Beckett public cert verification page.
CGC:  HTML scrape from CGC public cert lookup page.
SGC:  HTML scrape from SGC public cert lookup page.

130point + Card Ladder: best-effort HTTP requests; endpoints may require
updates if the sites change their response structure.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def lookup_cert(
    cert_number: str,
    grading_company: str,
    *,
    psa_api_key: str = "",
    timeout: float = 12.0,
) -> dict[str, Any]:
    """
    Look up a graded card by its certificate number.

    Returns:
        {
          "cert_number": str,
          "grading_company": str,
          "grade": str | None,
          "card_name": str | None,
          "set_name": str | None,
          "series": str | None,
          "last_solds": [{"date": str, "price": float, "source": str}],
          "suggested_price": float | None,
          "data_points": int,
          "error": str | None,   # present only on failure
        }
    """
    result: dict[str, Any] = {
        "cert_number": cert_number,
        "grading_company": grading_company.upper(),
        "grade": None,
        "card_name": None,
        "set_name": None,
        "series": None,
        "last_solds": [],
        "suggested_price": None,
        "data_points": 0,
    }

    company = grading_company.upper()

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DegenCollectibles/1.0)"},
        follow_redirects=True,
    ) as client:
        # -- Step 1: get card details from the grading company --
        try:
            if company == "PSA":
                await _lookup_psa(client, cert_number, result, psa_api_key=psa_api_key)
            elif company == "BGS":
                await _lookup_bgs(client, cert_number, result)
            elif company == "CGC":
                await _lookup_cgc(client, cert_number, result)
            elif company == "SGC":
                await _lookup_sgc(client, cert_number, result)
            else:
                result["error"] = f"Unsupported grading company: {company}"
                return result
        except Exception as exc:
            logger.warning("[cert_lookup] %s cert fetch failed for %s: %s", company, cert_number, exc)
            result.setdefault("error", f"Cert lookup failed: {exc}")

        # -- Step 2: fetch last-sold prices (requires card_name + grade) --
        card_name = result.get("card_name")
        grade = result.get("grade")

        if card_name:
            last_solds: list[dict] = []
            try:
                solds_130 = await _fetch_130point_sales(
                    client, card_name, company=company, grade=grade
                )
                last_solds.extend(solds_130)
            except Exception as exc:
                logger.debug("[cert_lookup] 130point fetch failed: %s", exc)

            if len(last_solds) < 3:
                try:
                    solds_cl = await _fetch_card_ladder_sales(
                        client, card_name, company=company, grade=grade
                    )
                    # Merge — deduplicate by (date, price)
                    seen = {(s["date"], s["price"]) for s in last_solds}
                    for s in solds_cl:
                        key = (s["date"], s["price"])
                        if key not in seen:
                            last_solds.append(s)
                            seen.add(key)
                except Exception as exc:
                    logger.debug("[cert_lookup] Card Ladder fetch failed: %s", exc)

            # Sort newest first
            last_solds.sort(key=lambda s: s.get("date", ""), reverse=True)
            result["last_solds"] = last_solds[:20]
            result["data_points"] = len(result["last_solds"])
            result["suggested_price"] = _suggest_price(result["last_solds"])

    return result


# ---------------------------------------------------------------------------
# Grading company cert lookups
# ---------------------------------------------------------------------------

async def _lookup_psa(
    client: httpx.AsyncClient,
    cert_number: str,
    result: dict,
    *,
    psa_api_key: str = "",
) -> None:
    """Try PSA Public API first, fall back to HTML scrape."""
    if psa_api_key:
        try:
            resp = await client.get(
                f"https://api.psacard.com/publicapi/cert/GetByCertNumber/{cert_number}",
                headers={"PSAAuth": psa_api_key, "Accept": "application/json"},
            )
            if resp.status_code == 200:
                data = resp.json()
                cert = data.get("PSACert") or {}
                result["grade"] = str(cert.get("CardGrade") or cert.get("Grade") or "")
                result["card_name"] = cert.get("Subject") or cert.get("CardName")
                result["set_name"] = cert.get("CardNumber") or cert.get("Year")
                result["series"] = cert.get("Brand") or cert.get("Set")
                return
        except Exception as exc:
            logger.debug("[cert_lookup] PSA API error: %s", exc)

    # HTML scrape fallback
    resp = await client.get(f"https://www.psacard.com/cert/{cert_number}")
    if resp.status_code != 200:
        raise RuntimeError(f"PSA cert page returned {resp.status_code}")
    _parse_psa_html(resp.text, result)


def _parse_psa_html(html: str, result: dict) -> None:
    """Extract grade, card name, set from PSA cert HTML."""
    # Grade
    grade_m = re.search(r'<td[^>]*class="[^"]*grade[^"]*"[^>]*>\s*([^<]+)', html, re.I)
    if not grade_m:
        grade_m = re.search(r'Grade\s*</[^>]+>\s*<[^>]+>\s*([^\s<]+)', html, re.I)
    if grade_m:
        result["grade"] = grade_m.group(1).strip()

    # Card name (Subject field)
    name_m = re.search(r'Subject\s*</[^>]+>\s*<[^>]+>\s*([^<]+)', html, re.I)
    if not name_m:
        name_m = re.search(r'<td[^>]*class="[^"]*subject[^"]*"[^>]*>\s*([^<]+)', html, re.I)
    if name_m:
        result["card_name"] = name_m.group(1).strip()

    # Set / series
    set_m = re.search(r'Brand\s*</[^>]+>\s*<[^>]+>\s*([^<]+)', html, re.I)
    if set_m:
        result["series"] = set_m.group(1).strip()
    year_m = re.search(r'Year\s*</[^>]+>\s*<[^>]+>\s*([^<]+)', html, re.I)
    if year_m:
        result["set_name"] = year_m.group(1).strip()


async def _lookup_bgs(
    client: httpx.AsyncClient,
    cert_number: str,
    result: dict,
) -> None:
    """Scrape Beckett grading public cert page."""
    resp = await client.get(
        f"https://www.beckett.com/grading/certificate/{cert_number}"
    )
    if resp.status_code != 200:
        raise RuntimeError(f"BGS cert page returned {resp.status_code}")
    html = resp.text

    # Grade — Beckett shows "9.5" or "10" in a prominent element
    grade_m = re.search(r'Overall\s+Grade[^0-9]*([0-9]+\.?[0-9]*)', html, re.I)
    if not grade_m:
        grade_m = re.search(r'class="[^"]*grade[^"]*"[^>]*>\s*([0-9]+\.?[0-9]*)', html, re.I)
    if grade_m:
        result["grade"] = grade_m.group(1).strip()

    # Card name
    name_m = re.search(r'<h[12][^>]*>\s*([^<]{5,100})</h[12]>', html, re.I)
    if name_m:
        result["card_name"] = name_m.group(1).strip()

    # Set / year in meta or structured block
    set_m = re.search(r'(?:Set|Series|Brand)[^\w].*?<[^>]+>\s*([^<]+)', html, re.I)
    if set_m:
        result["set_name"] = set_m.group(1).strip()


async def _lookup_cgc(
    client: httpx.AsyncClient,
    cert_number: str,
    result: dict,
) -> None:
    """Scrape CGC Cards public cert lookup."""
    resp = await client.get(
        f"https://www.cgccards.com/certlookup/{cert_number}/"
    )
    if resp.status_code != 200:
        raise RuntimeError(f"CGC cert page returned {resp.status_code}")
    html = resp.text

    # CGC renders JSON in a <script type="application/ld+json"> block
    ld_m = re.search(r'application/ld\+json">\s*(\{.*?\})\s*</script>', html, re.S)
    if ld_m:
        try:
            ld = json.loads(ld_m.group(1))
            result["card_name"] = ld.get("name")
            result["grade"] = str(ld.get("ratingValue") or "")
        except Exception as exc:
            logger.warning(
                "cert_lookup._lookup_cgc: ld+json parse failed (cert=%s): %s",
                cert_number,
                exc,
                exc_info=True,
            )

    if not result["card_name"]:
        name_m = re.search(r'<h[12][^>]*>\s*([^<]{5,120})</h[12]>', html, re.I)
        if name_m:
            result["card_name"] = name_m.group(1).strip()

    if not result["grade"]:
        grade_m = re.search(r'Grade[^\d]*([0-9]+\.?[0-9]*)', html, re.I)
        if grade_m:
            result["grade"] = grade_m.group(1).strip()


async def _lookup_sgc(
    client: httpx.AsyncClient,
    cert_number: str,
    result: dict,
) -> None:
    """Scrape SGC public cert lookup."""
    resp = await client.get(
        f"https://www.sgccard.com/cert-lookup/?cert={cert_number}"
    )
    if resp.status_code != 200:
        raise RuntimeError(f"SGC cert page returned {resp.status_code}")
    html = resp.text

    grade_m = re.search(r'Grade[^\d]*([0-9]+\.?[0-9]*)', html, re.I)
    if grade_m:
        result["grade"] = grade_m.group(1).strip()

    name_m = re.search(r'<h[12][^>]*>\s*([^<]{5,120})</h[12]>', html, re.I)
    if name_m:
        result["card_name"] = name_m.group(1).strip()

    set_m = re.search(r'(?:Set|Year|Brand)[^\w].*?<[^>]+>\s*([^<]+)', html, re.I)
    if set_m:
        result["set_name"] = set_m.group(1).strip()


# ---------------------------------------------------------------------------
# Last-sold data sources
# ---------------------------------------------------------------------------

async def _fetch_130point_sales(
    client: httpx.AsyncClient,
    card_name: str,
    *,
    company: str = "",
    grade: Optional[str] = None,
) -> list[dict]:
    """
    Query 130point for recent sales of a graded card.

    130point exposes a JSON search endpoint used by their public site.
    We build a query from card_name + grading company + grade.
    """
    query_parts = [card_name]
    if company:
        query_parts.append(company)
    if grade:
        query_parts.append(str(grade))

    params = {
        "q": " ".join(query_parts),
        "page": "1",
        "per_page": "20",
    }

    resp = await client.get(
        "https://130point.com/sales",
        params=params,
        headers={"Accept": "application/json, text/javascript, */*"},
    )
    if resp.status_code != 200:
        return []

    # 130point may return HTML or JSON depending on Accept header
    content_type = resp.headers.get("content-type", "")
    if "json" in content_type:
        data = resp.json()
        return _parse_130point_json(data)

    # Fallback: parse any embedded JSON array in the HTML response
    return _parse_sales_from_html(resp.text, source="130point")


def _parse_130point_json(data: Any) -> list[dict]:
    """Parse 130point JSON response into normalised last-sold list."""
    sales = []
    items = data if isinstance(data, list) else data.get("sales") or data.get("results") or []
    for item in items[:20]:
        price = _safe_float(item.get("sale_price") or item.get("price"))
        date_str = _normalise_date(item.get("date") or item.get("sold_date") or "")
        if price and date_str:
            sales.append({"date": date_str, "price": price, "source": "130point"})
    return sales


async def _fetch_card_ladder_sales(
    client: httpx.AsyncClient,
    card_name: str,
    *,
    company: str = "",
    grade: Optional[str] = None,
) -> list[dict]:
    """
    Query Card Ladder for recent sales.

    Card Ladder uses a REST-ish search API at /cards/search.
    """
    query = card_name
    if company:
        query += f" {company}"
    if grade:
        query += f" {grade}"

    params = {"q": query, "limit": "20"}

    resp = await client.get(
        "https://www.cardladder.com/cards/search",
        params=params,
        headers={"Accept": "application/json"},
    )
    if resp.status_code != 200:
        return []

    try:
        data = resp.json()
    except Exception:
        return _parse_sales_from_html(resp.text, source="cardladder")

    sales = []
    items = data if isinstance(data, list) else data.get("sales") or data.get("data") or []
    for item in items[:20]:
        price = _safe_float(item.get("price") or item.get("sale_price"))
        date_str = _normalise_date(item.get("date") or item.get("sold_at") or "")
        if price and date_str:
            sales.append({"date": date_str, "price": price, "source": "cardladder"})
    return sales


def _parse_sales_from_html(html: str, source: str) -> list[dict]:
    """
    Best-effort: find any JSON array of sales embedded in an HTML page.
    Looks for patterns like: [{..."price":...,"date":...}]
    """
    # Try to find a JSON array in the page source
    array_m = re.search(r'\[(\s*\{[^\[\]]{20,}\}[\s,]*)+\]', html)
    if not array_m:
        return []
    try:
        items = json.loads(array_m.group(0))
        sales = []
        for item in items[:20]:
            if not isinstance(item, dict):
                continue
            price = _safe_float(
                item.get("price") or item.get("sale_price") or item.get("amount")
            )
            date_str = _normalise_date(
                item.get("date") or item.get("sold_date") or item.get("sold_at") or ""
            )
            if price and date_str:
                sales.append({"date": date_str, "price": price, "source": source})
        return sales
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Price suggestion
# ---------------------------------------------------------------------------

def _suggest_price(last_solds: list[dict]) -> Optional[float]:
    """
    Weighted median of the most recent sales.

    Most-recent sale gets weight 2; all others weight 1.
    Uses up to 5 most recent data points.
    """
    if not last_solds:
        return None

    recent = last_solds[:5]
    weighted: list[float] = []
    for i, sale in enumerate(recent):
        price = sale.get("price")
        if price is None:
            continue
        weight = 2 if i == 0 else 1
        weighted.extend([float(price)] * weight)

    if not weighted:
        return None

    weighted.sort()
    mid = len(weighted) // 2
    if len(weighted) % 2 == 0:
        median = (weighted[mid - 1] + weighted[mid]) / 2
    else:
        median = weighted[mid]

    return round(median, 2)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        # Strip currency symbols
        cleaned = re.sub(r'[^\d.]', '', str(value))
        f = float(cleaned)
        return round(f, 2) if f > 0 else None
    except (TypeError, ValueError):
        return None


def _normalise_date(raw: str) -> str:
    """Return ISO date string (YYYY-MM-DD) or empty string."""
    if not raw:
        return ""
    raw = raw.strip()
    # Already ISO
    if re.match(r'^\d{4}-\d{2}-\d{2}', raw):
        return raw[:10]
    # MM/DD/YYYY or M/D/YYYY
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})', raw)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    # Month DD, YYYY
    try:
        dt = datetime.strptime(raw[:20], "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        dt = datetime.strptime(raw[:11], "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    return raw[:10]
