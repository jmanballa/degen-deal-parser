"""
Barcode generation for inventory items.

Generates Code 128 barcodes as SVG strings using python-barcode.
The barcode value for each item is its DGN-XXXXXX code, which USB
barcode scanners send as keyboard input followed by Enter.
"""
from __future__ import annotations

import io
from typing import TYPE_CHECKING

try:
    import barcode
    from barcode.writer import SVGWriter
    _BARCODE_AVAILABLE = True
except ImportError:
    _BARCODE_AVAILABLE = False

if TYPE_CHECKING:
    from ..models import InventoryItem


def _money(value: float | None) -> str:
    if value is None:
        return ""
    return f"${value:,.2f}"


def _label_price(item: "InventoryItem") -> tuple[str, str]:
    """Return the customer-facing price text plus the source used."""
    if item.list_price is not None:
        return _money(round(item.list_price, 2)), "Manual price"
    if item.auto_price is not None:
        return _money(round(item.auto_price, 2)), "Market price"
    return "Price not set", ""


def _label_product_type(item: "InventoryItem") -> str:
    item_type = (item.item_type or "").strip().lower()
    if item_type == "sealed":
        return item.sealed_product_kind or "Sealed"
    if item_type == "slab":
        return "Graded card"
    if item_type == "single":
        return "Single"
    return item.item_type or "Inventory"


def generate_barcode_value(item_id: int) -> str:
    """Return the canonical barcode string for an item, e.g. 'DGN-000042'."""
    return f"DGN-{item_id:06d}"


def render_barcode_svg(barcode_value: str) -> str:
    """
    Render a Code 128 barcode as an SVG string.

    Returns an SVG string ready to embed in HTML or serve as a response.
    Falls back to a minimal placeholder SVG when python-barcode is not installed.
    """
    if not _BARCODE_AVAILABLE:
        return _fallback_svg(barcode_value)

    Code128 = barcode.get_barcode_class("code128")
    buf = io.BytesIO()
    writer = SVGWriter()
    code = Code128(barcode_value, writer=writer)
    # write() returns SVG bytes; write_options control size
    code.write(
        buf,
        options={
            "module_height": 10.0,   # mm, controls bar height
            "module_width": 0.2,     # mm, controls bar width
            "font_size": 6,
            "text_distance": 3,
            "quiet_zone": 4.0,
        },
    )
    return buf.getvalue().decode("utf-8")


def _fallback_svg(barcode_value: str) -> str:
    """Minimal placeholder SVG returned when python-barcode is unavailable."""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="60">'
        f'<rect width="200" height="60" fill="#f5f5f5" stroke="#ccc"/>'
        f'<text x="100" y="35" text-anchor="middle" font-size="11" font-family="monospace">{barcode_value}</text>'
        "</svg>"
    )


def label_context_for_items(items: list) -> list[dict]:
    """
    Build a list of label context dicts for use in the print-labels template.
    Each dict contains barcode_value, barcode_svg, and display fields.
    """
    labels = []
    for item in items:
        barcode_value = item.barcode
        svg = render_barcode_svg(barcode_value)
        grade_or_condition = (
            f"{item.grading_company} {item.grade}" if item.grading_company and item.grade
            else item.condition or ""
        )
        price_text, price_source = _label_price(item)
        labels.append({
            "item": item,
            "barcode_value": barcode_value,
            "barcode_svg": svg,
            "grade_or_condition": grade_or_condition,
            "product_type": _label_product_type(item),
            "price_text": price_text,
            "price_source": price_source,
        })
    return labels
