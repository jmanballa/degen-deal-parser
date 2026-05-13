from app.inventory.barcode import (
    _BARCODE_AVAILABLE,
    generate_barcode_value,
    label_context_for_items,
    render_barcode_svg,
)
from app.models import InventoryItem
from app.shared import templates


class _FakeUrl:
    path = "/inventory/labels"

    def include_query_params(self, **params):
        return "/inventory/labels?" + "&".join(f"{key}={value}" for key, value in params.items())


class _FakeRequest:
    url = _FakeUrl()


class _FakeUser:
    role = "admin"
    display_name = "Admin"
    username = "admin"


def test_generate_barcode_value_uses_stable_shop_prefix():
    assert generate_barcode_value(42) == "DGN-000042"


def test_render_barcode_svg_uses_real_code128_dependency():
    assert _BARCODE_AVAILABLE is True
    svg = render_barcode_svg("DGN-000042")
    assert "<svg" in svg
    assert "DGN-000042" in svg
    assert "fill:#000000" in svg or 'fill="black"' in svg or "fill:black" in svg


def test_label_context_includes_product_type_and_customer_price():
    item = InventoryItem(
        id=42,
        barcode="DGN-000042",
        item_type="sealed",
        game="Pokemon",
        card_name="Prismatic Evolutions Super Premium Collection",
        set_name="SV Prismatic Evolutions",
        sealed_product_kind="Super Premium Collection",
        condition="Sealed",
        auto_price=129.99,
        list_price=139.99,
    )

    label = label_context_for_items([item])[0]

    assert label["barcode_value"] == "DGN-000042"
    assert label["product_type"] == "Super Premium Collection"
    assert label["grade_or_condition"] == "Sealed"
    assert label["price_text"] == "$139.99"
    assert label["price_source"] == "Manual price"


def test_label_context_falls_back_to_market_price():
    item = InventoryItem(
        id=43,
        barcode="DGN-000043",
        item_type="single",
        game="Pokemon",
        card_name="Pikachu",
        condition="NM",
        auto_price=12.345,
    )

    label = label_context_for_items([item])[0]

    assert label["product_type"] == "Single"
    assert label["price_text"] == "$12.35"
    assert label["price_source"] == "Market price"


def test_label_template_has_sheet_and_thermal_layouts():
    item = InventoryItem(
        id=44,
        barcode="DGN-000044",
        item_type="sealed",
        game="Pokemon",
        card_name="Test Booster Box",
        sealed_product_kind="Booster Box",
        auto_price=119.99,
    )
    label = label_context_for_items([item])[0]

    html = templates.env.get_template("inventory_labels.html").render(
        request=_FakeRequest(),
        current_user=_FakeUser(),
        csrf_token="",
        labels=[label],
        layout="thermal",
    )

    assert 'class="label-layout-thermal"' in html
    assert "2.25&quot; Thermal" in html
    assert "$119.99" in html
    assert "Test Booster Box" in html
