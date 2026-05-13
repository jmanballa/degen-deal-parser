import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from app.routers import team_admin, team_buylist
from app.routers.team_buylist import DEFAULT_BUYLIST_CONFIG, calculate_buylist_offer
from app.models import AppSetting, AuditLog, BuylistSubmission, InventoryItem, RolePermission, User


class FakeJsonRequest:
    def __init__(self, body, user):
        self._body = body
        self.state = SimpleNamespace(current_user=user)
        self.client = SimpleNamespace(host="127.0.0.1")

    async def json(self):
        return deepcopy(self._body)


def _memory_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine, Session(engine)


def _user(user_id=1, role="admin"):
    return User(
        id=user_id,
        username=f"user{user_id}",
        password_hash="x",
        password_salt="x",
        display_name=f"User {user_id}",
        role=role,
        is_active=True,
    )
from app.inventory.tcgplayer_sales import normalize_tcgplayer_sales_payload, tcgplayer_product_id_from_url


def test_staff_buylist_uses_cash_and_trade_ranges():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)

    offer = calculate_buylist_offer(
        config,
        market_price=10.0,
        condition="NM",
        language="English",
        printing="Normal",
        product={"name": "Test Card"},
    )

    assert offer["cash_offer"] == 5.0
    assert offer["trade_offer"] == 6.0
    assert offer["cash_rule"] == "50%"
    assert offer["trade_rule"] == "60%"


def test_staff_buylist_applies_modifiers_and_darklist_blocks():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)
    config["darklist_rules"] = [{"pattern": "Blocked Card", "percent": 100}]

    offer = calculate_buylist_offer(
        config,
        market_price=20.0,
        condition="LP",
        language="Japanese",
        printing="Reverse Holofoil",
        product={"name": "Blocked Card"},
    )

    assert offer["blocked"] is True
    assert offer["cash_offer"] == 0.0
    assert offer["trade_offer"] == 0.0
    assert "Not buying" in offer["notes"]


def test_staff_buylist_can_use_tcgplayer_condition_market():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)
    config["condition_pricing_mode"] = team_buylist.CONDITION_PRICING_TCGPLAYER

    offer = calculate_buylist_offer(
        config,
        market_price=100.0,
        condition_market_price=70.0,
        condition="LP",
        language="English",
        printing="Normal",
        product={"name": "Test Card"},
    )

    assert offer["market_price"] == 70.0
    assert offer["base_market_price"] == 100.0
    assert offer["cash_offer"] == 42.0
    assert offer["trade_offer"] == 49.0
    assert offer["condition_price_source"] == team_buylist.CONDITION_PRICING_TCGPLAYER
    assert "LP TCGPlayer market" in offer["notes"]


def test_staff_buylist_card_payload_includes_tcgplayer_listing_metrics():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)
    config["condition_pricing_mode"] = team_buylist.CONDITION_PRICING_TCGPLAYER
    candidate = {
        "id": "sv08-238",
        "name": "Pikachu ex",
        "set_name": "Surging Sparks",
        "number": "238/191",
        "tcgplayer_url": (
            "https://partner.tcgplayer.com/c/6207277/1830156/21018?"
            "u=https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F590027%2Fpokemon-test"
        ),
        "available_variants": [
            {
                "name": "Holofoil",
                "price": 333.59,
                "conditions": {
                    "NM": {"mkt": 339.46, "low": 253.99, "hi": 599.99, "cnt": 25, "sku_id": "8299137"},
                    "LP": {"mkt": 242.14, "low": 214.75, "cnt": 6},
                },
            }
        ],
    }

    payload = team_buylist._candidate_payload(candidate, config, category_id="3")

    assert payload["tcgplayer_product_id"] == "590027"
    assert payload["product_id"] == "590027"
    assert payload["condition_market_prices"]["NM"] == 339.46
    assert payload["condition_price_metrics"]["NM"] == {
        "market": 339.46,
        "low": 253.99,
        "high": 599.99,
        "listing_count": 25,
        "sku_id": "8299137",
    }
    assert payload["market_price"] == 339.46


def test_staff_buylist_payload_prefers_normal_when_tcgtracking_returns_foil_first():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)
    candidate = {
        "id": "magic-630917",
        "name": "Summon Bahamut",
        "set_name": "FINAL FANTASY",
        "number": "1",
        "available_variants": [
            {
                "name": "Foil",
                "price": 22.89,
                "conditions": {"NM": {"mkt": 22.81, "low": 19.67, "cnt": 25}},
            },
            {
                "name": "Normal",
                "price": 19.62,
                "conditions": {
                    "NM": {"mkt": 19.12, "low": 13.99, "cnt": 25},
                    "LP": {"mkt": 17.96, "low": 16.54, "cnt": 25},
                },
            },
        ],
    }

    payload = team_buylist._candidate_payload(candidate, config, category_id="1")

    assert payload["variant"] == "Normal"
    assert payload["base_market_price"] == 19.62
    assert payload["market_price"] == 19.12
    assert payload["condition_market_prices"]["LP"] == 17.96
    assert "NM TCGPlayer market" in payload["pricing_notes"]


def test_staff_buylist_tcgplayer_mode_falls_back_to_modifier_table():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)
    config["condition_pricing_mode"] = team_buylist.CONDITION_PRICING_TCGPLAYER

    offer = calculate_buylist_offer(
        config,
        market_price=100.0,
        condition="LP",
        language="English",
        printing="Normal",
        product={"name": "Test Card"},
    )

    assert offer["market_price"] == 100.0
    assert offer["cash_offer"] == 55.25
    assert offer["trade_offer"] == 63.75
    assert offer["condition_price_source"] == "modifier_fallback"
    assert "LP modifier fallback 85%" in offer["notes"]


def test_staff_buylist_blocks_missing_market_price():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)

    offer = calculate_buylist_offer(
        config,
        market_price=0.0,
        condition="NM",
        language="English",
        printing="Normal",
        product={"name": "Unpriced Card"},
    )

    assert offer["blocked"] is True
    assert offer["cash_offer"] == 0.0
    assert offer["trade_offer"] == 0.0
    assert team_buylist.NO_MARKET_PRICE_NOTE in offer["notes"]
    assert "Not buying" in offer["notes"]


def test_staff_buylist_json_loads_logs_corrupt_payload(caplog):
    with caplog.at_level("WARNING", logger="app.routers.team_buylist"):
        parsed = team_buylist._json_loads("{bad-json", [], label="buylist_submission.99.lines_json")

    assert parsed == []
    assert "Invalid buylist_submission.99.lines_json" in caplog.text


def test_tcgplayer_sales_payload_normalizes_snapshot_and_latest_solds():
    payload = normalize_tcgplayer_sales_payload(
        "590027",
        selected_condition="NM",
        selected_variant="Holofoil",
        selected_language="English",
        sales_payload={
            "data": [
                {
                    "condition": "Near Mint",
                    "variant": "Holofoil",
                    "language": "English",
                    "quantity": 1,
                    "title": "Pikachu ex - 238/191",
                    "purchasePrice": 293.29,
                    "shippingPrice": 0,
                    "orderDate": "2026-05-09T22:27:36.807+00:00",
                },
                {
                    "condition": "Lightly Played",
                    "variant": "Holofoil",
                    "language": "English",
                    "quantity": 1,
                    "title": "Pikachu ex - 238/191",
                    "purchasePrice": 200,
                    "shippingPrice": 0,
                    "orderDate": "2026-05-08T22:27:36.807+00:00",
                },
            ],
        },
        history_payload={
            "result": [
                {
                    "skuId": "8299137",
                    "variant": "Holofoil",
                    "language": "English",
                    "condition": "Near Mint",
                    "averageDailyQuantitySold": "2",
                    "averageDailyTransactionCount": "2",
                    "totalQuantitySold": "210",
                    "totalTransactionCount": "197",
                    "buckets": [
                        {
                            "marketPrice": "334.58",
                            "quantitySold": "6",
                            "lowSalePrice": "278.3",
                            "highSalePrice": "424",
                            "bucketStartDate": "2026-05-07",
                        },
                        {
                            "marketPrice": "319.53",
                            "quantitySold": "12",
                            "lowSalePrice": "292.3",
                            "highSalePrice": "329.95",
                            "bucketStartDate": "2026-05-04",
                        },
                    ],
                }
            ],
        },
        pricepoints_payload=[
            {
                "skuId": 8299137,
                "marketPrice": 339.91,
                "lowestPrice": 278.3,
                "highestPrice": 599.99,
                "priceCount": 25,
            }
        ],
        volatility_payload={"skuId": 8299137, "volatility": "MED", "zScore": 0.44},
    )

    assert payload["ok"] is True
    assert payload["snapshot"]["total_quantity_sold"] == 210
    assert payload["snapshot"]["low_sale_price"] == 278.3
    assert payload["snapshot"]["high_sale_price"] == 424.0
    assert payload["snapshot"]["active_low_price"] == 278.3
    assert payload["snapshot"]["active_listing_count"] == 25
    assert payload["snapshot"]["volatility"] == "MED"
    assert len(payload["last_sales"]) == 1
    assert payload["last_sales"][0]["purchase_price"] == 293.29


def test_tcgplayer_sales_payload_tolerates_null_history_result():
    payload = normalize_tcgplayer_sales_payload(
        "678671",
        selected_condition="NM",
        history_payload={"result": None},
        sales_payload={"data": []},
    )

    assert payload["ok"] is False
    assert payload["snapshot"] is None
    assert payload["last_sales"] == []


def test_tcgplayer_product_id_from_affiliate_url():
    assert (
        tcgplayer_product_id_from_url(
            "https://partner.tcgplayer.com/c/6207277/1830156/21018?"
            "u=https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F590027%2Fpokemon-test"
        )
        == "590027"
    )


def test_staff_buylist_sealed_products_use_ranges_without_condition_modifiers():
    config = deepcopy(DEFAULT_BUYLIST_CONFIG)

    offer = calculate_buylist_offer(
        config,
        market_price=100.0,
        condition="LP",
        language="Japanese",
        printing="Reverse Holofoil",
        product={"name": "Test Booster Box", "item_type": "sealed"},
    )

    assert offer["cash_offer"] == 65.0
    assert offer["trade_offer"] == 75.0
    assert offer["condition_price_source"] == team_buylist.BUYLIST_PRODUCT_TYPE_SEALED
    assert offer["notes"] == ["Sealed product TCGPlayer market"]


def test_staff_buylist_search_uses_fast_options_and_cache(monkeypatch):
    calls = []

    async def fake_text_search_cards(query, **kwargs):
        calls.append((query, kwargs))
        return {
            "status": "MATCHED",
            "processing_time_ms": 123,
            "candidates": [
                {
                    "id": "abc",
                    "name": "Test Card",
                    "set_name": "Test Set",
                    "number": "1",
                    "available_variants": [{"name": "Normal", "price": 10.0}],
                }
            ],
        }

    monkeypatch.setattr(team_buylist, "_require_team_user", lambda request, session: (None, SimpleNamespace(id=1)))
    monkeypatch.setattr(team_buylist, "get_buylist_config", lambda session: deepcopy(DEFAULT_BUYLIST_CONFIG))
    monkeypatch.setattr(team_buylist, "text_search_cards", fake_text_search_cards)
    team_buylist._BUYLIST_SEARCH_CACHE.clear()

    first = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(),
            q=" test card ",
            game="Pokemon",
            product_type="card",
            session=None,
        )
    )
    second = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(),
            q="test   card",
            game="Pokemon",
            product_type="card",
            session=None,
        )
    )

    first_body = json.loads(first.body.decode("utf-8"))
    second_body = json.loads(second.body.decode("utf-8"))

    assert first_body["cards"][0]["name"] == "Test Card"
    assert second_body["cached"] is True
    assert len(calls) == 1
    assert calls[0][0] == "test card"
    assert calls[0][1]["max_results"] == team_buylist.BUYLIST_SEARCH_RESULT_LIMIT
    assert calls[0][1]["include_pokemontcg_supplement"] is False
    assert calls[0][1]["allow_cross_category_pricing"] is False
    assert calls[0][1]["allow_pokemontcg_price_fallback"] is False


def test_staff_buylist_search_rejects_overlong_query(monkeypatch):
    async def fake_text_search_cards(query, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("search should not run for overlong queries")

    monkeypatch.setattr(team_buylist, "_require_team_user", lambda request, session: (None, SimpleNamespace(id=1)))
    monkeypatch.setattr(team_buylist, "get_buylist_config", lambda session: deepcopy(DEFAULT_BUYLIST_CONFIG))
    monkeypatch.setattr(team_buylist, "text_search_cards", fake_text_search_cards)

    response = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(),
            q="x" * (team_buylist.BUYLIST_SEARCH_MAX_CHARS + 1),
            game="Pokemon",
            product_type="card",
            session=None,
        )
    )
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 400
    assert body["ok"] is False
    assert "Search is too long" in body["error"]


def test_staff_buylist_search_all_games_value_uses_configured_default(monkeypatch):
    calls = []

    async def fake_text_search_cards(query, **kwargs):
        calls.append((query, kwargs))
        category_id = kwargs["category_id"]
        candidates = []
        if category_id == "3":
            candidates = [
                {
                    "id": "pokemon-pikachu",
                    "product_id": "pokemon-pikachu",
                    "name": "Pikachu",
                    "set_name": "151",
                    "number": "025/165",
                    "available_variants": [{"name": "Normal", "price": 2.0}],
                }
            ]
        return {
            "status": "MATCHED" if candidates else "NO_MATCH",
            "processing_time_ms": 50,
            "candidates": candidates,
        }

    monkeypatch.setattr(team_buylist, "_require_team_user", lambda request, session: (None, SimpleNamespace(id=1)))
    monkeypatch.setattr(team_buylist, "get_buylist_config", lambda session: deepcopy(DEFAULT_BUYLIST_CONFIG))
    monkeypatch.setattr(team_buylist, "text_search_cards", fake_text_search_cards)
    team_buylist._BUYLIST_SEARCH_CACHE.clear()

    response = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(),
            q="pikachu",
            game=team_buylist.BUYLIST_ALL_GAMES_VALUE,
            product_type="card",
            session=None,
        )
    )
    body = json.loads(response.body.decode("utf-8"))

    assert body["game"] == "Pokemon"
    assert body["category_id"] == "3"
    assert body["cards"][0]["game"] == "Pokemon"
    assert body["cards"][0]["name"] == "Pikachu"
    assert [call[1]["category_id"] for call in calls] == ["3"]


def test_staff_buylist_dedupes_same_card_with_short_and_full_numbers():
    payloads = [
        {"game": "Pokemon", "name": "Pikachu", "set_name": "151", "number": "025/165", "id": "tcgdex"},
        {"game": "Pokemon", "name": "Pikachu", "set_name": "151", "number": "25", "id": "pokemontcg"},
        {"game": "Pokemon", "name": "Pikachu", "set_name": "151", "number": "173", "id": "sir"},
    ]

    deduped = team_buylist._dedupe_payloads(payloads, limit=12)

    assert [row["id"] for row in deduped] == ["tcgdex", "sir"]


def test_staff_buylist_search_can_return_sealed_products(monkeypatch):
    calls = []

    async def fake_sealed_search(query, **kwargs):
        calls.append((query, kwargs))
        return [
            {
                "name": "Test Booster Box",
                "set_name": "Test Set",
                "kind": "Booster Box",
                "external_id": "123",
                "category_id": "3",
                "game": "Pokemon",
                "market_price": 100.0,
                "image_url": "https://example.test/box.jpg",
            }
        ], ""

    monkeypatch.setattr(team_buylist, "_require_team_user", lambda request, session: (None, SimpleNamespace(id=1)))
    monkeypatch.setattr(team_buylist, "get_buylist_config", lambda session: deepcopy(DEFAULT_BUYLIST_CONFIG))
    monkeypatch.setattr(team_buylist, "_search_buylist_sealed_products", fake_sealed_search)
    team_buylist._BUYLIST_SEARCH_CACHE.clear()

    response = asyncio.run(
        team_buylist.staff_buylist_search(
            SimpleNamespace(),
            q="test booster box",
            game="Pokemon",
            product_type="sealed",
            session=None,
        )
    )
    body = json.loads(response.body.decode("utf-8"))

    assert body["product_type"] == "sealed"
    assert body["cards"][0]["item_type"] == "sealed"
    assert body["cards"][0]["sealed_product_kind"] == "Booster Box"
    assert body["cards"][0]["cash_offer"] == 65.0
    assert calls == [("test booster box", {"game": "Pokemon", "limit": team_buylist.BUYLIST_SEARCH_RESULT_LIMIT})]


def test_staff_buylist_save_creates_submission(monkeypatch):
    engine, session = _memory_session()
    try:
        user = _user(10, role="employee")
        session.add(user)
        session.commit()
        monkeypatch.setattr(team_buylist, "_require_team_user", lambda request, sess: (None, user))
        request = FakeJsonRequest(
            {
                "customer_name": "Ash",
                "customer_contact": "ash@example.test",
                "payment_view": "cash",
                "notes": "ID checked",
                "items": [
                    {
                        "id": "card-1",
                        "item_type": "card",
                        "game": "Pokemon",
                        "name": "Test Card",
                        "set_name": "Test Set",
                        "number": "1",
                        "variant": "Normal",
                        "condition": "NM",
                        "language": "English",
                        "market_price": 10.0,
                        "base_market_price": 10.0,
                        "quantity": 2,
                    }
                ],
            },
            user,
        )

        response = asyncio.run(team_buylist.staff_buylist_save(request, session=session))
        body = json.loads(response.body.decode("utf-8"))
        row = session.get(BuylistSubmission, body["submission_id"])
        lines = json.loads(row.lines_json)

        assert body["message"] == "Buylist submitted"
        assert row.status == "submitted"
        assert row.customer_name == "Ash"
        assert json.loads(row.totals_json)["cash"] == 10.0
        assert lines[0]["unit_cash"] == 5.0
        audit = session.exec(
            select(AuditLog).where(AuditLog.action == "staff_buylist.quote_saved")
        ).one()
        audit_details = json.loads(audit.details_json)
        assert audit_details["buylist_submission_id"] == row.id
        assert audit_details["line_count"] == 1
        assert audit_details["has_customer_name"] is True
        assert audit_details["has_customer_contact"] is True
        assert audit_details["has_notes"] is True
        assert "customer_name" not in audit_details
        assert "customer_contact" not in audit_details
        assert "notes" not in audit_details
        assert "lines" not in audit_details
        assert "Ash" not in audit.details_json
        assert "ash@example.test" not in audit.details_json
        assert "ID checked" not in audit.details_json
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_save_rejects_manager_review_lines(monkeypatch):
    engine, session = _memory_session()
    try:
        user = _user(11, role="employee")
        session.add(user)
        session.commit()
        monkeypatch.setattr(team_buylist, "_require_team_user", lambda request, sess: (None, user))
        request = FakeJsonRequest(
            {
                "customer_name": "Brock",
                "payment_view": "cash",
                "items": [
                    {
                        "id": "card-zero",
                        "item_type": "card",
                        "game": "Magic",
                        "name": "Unpriced Card",
                        "set_name": "Test Set",
                        "number": "1",
                        "variant": "Normal",
                        "condition": "NM",
                        "language": "English",
                        "market_price": 0.0,
                        "base_market_price": 0.0,
                        "quantity": 1,
                    }
                ],
            },
            user,
        )

        response = asyncio.run(team_buylist.staff_buylist_save(request, session=session))
        body = json.loads(response.body.decode("utf-8"))

        assert response.status_code == 400
        assert body["ok"] is False
        assert "manager-review" in body["error"]
        assert session.exec(select(BuylistSubmission)).all() == []
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_status_counts_surface_unknown_statuses():
    engine, session = _memory_session()
    try:
        session.add(
            BuylistSubmission(
                submitted_by_user_id=1,
                customer_name="Known",
                status="submitted",
                totals_json="{}",
                lines_json="[]",
            )
        )
        session.add(
            BuylistSubmission(
                submitted_by_user_id=1,
                customer_name="Odd",
                status="stuck",
                totals_json="{}",
                lines_json="[]",
            )
        )
        session.commit()

        counts, unknown_count = team_buylist._buylist_submission_status_counts(session)

        assert counts["submitted"] == 1
        assert "stuck" not in counts
        assert unknown_count == 1
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_admin_denies_employee_role(monkeypatch):
    engine, session = _memory_session()
    try:
        employee = _user(12, role="employee")
        session.add(employee)
        session.commit()
        request = SimpleNamespace(
            state=SimpleNamespace(current_user=employee),
            client=SimpleNamespace(host="127.0.0.1"),
        )
        monkeypatch.setattr(
            team_admin,
            "get_settings",
            lambda: SimpleNamespace(employee_portal_enabled=True),
        )

        response = team_buylist.admin_buylist_submissions_page(request, session=session)

        assert response.status_code == 403
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_admin_save_requires_dedicated_edit_permission(monkeypatch):
    engine, session = _memory_session()
    try:
        manager = _user(13, role="manager")
        session.add(manager)
        session.add(
            RolePermission(
                role="manager",
                resource_key="admin.supply.view",
                is_allowed=True,
            )
        )
        session.add(
            RolePermission(
                role="manager",
                resource_key=team_buylist.BUYLIST_EDIT_PERMISSION,
                is_allowed=False,
            )
        )
        session.commit()
        request = SimpleNamespace(
            state=SimpleNamespace(current_user=manager),
            client=SimpleNamespace(host="127.0.0.1"),
        )
        monkeypatch.setattr(
            team_admin,
            "get_settings",
            lambda: SimpleNamespace(employee_portal_enabled=True),
        )

        response = asyncio.run(
            team_buylist.staff_buylist_admin_save(
                request,
                enabled_games=["Pokemon"],
                default_game="Pokemon",
                session=session,
            )
        )

        assert response.status_code == 403
        assert session.get(AppSetting, team_buylist.BUYLIST_CONFIG_KEY) is None
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_approval_receives_inventory(monkeypatch):
    engine, session = _memory_session()
    try:
        actor = _user(20, role="admin")
        submitter = _user(21, role="employee")
        session.add(actor)
        session.add(submitter)
        session.add(
            BuylistSubmission(
                submitted_by_user_id=submitter.id,
                customer_name="Misty",
                payment_view="cash",
                status="submitted",
                totals_json=json.dumps({"cash": 5.0, "trade": 6.0, "quantity": 1, "items": 1}),
                lines_json=json.dumps(
                    [
                        {
                            "id": "card-1",
                            "item_type": "card",
                            "game": "Pokemon",
                            "name": "Test Card",
                            "set_name": "Test Set",
                            "number": "1",
                            "variant": "Normal",
                            "condition": "NM",
                            "language": "English",
                            "market_price": 10.0,
                            "base_market_price": 10.0,
                            "unit_cash": 5.0,
                            "unit_trade": 6.0,
                            "quantity": 1,
                        }
                    ],
                    sort_keys=True,
                ),
            )
        )
        session.commit()
        submission = session.exec(select(BuylistSubmission)).one()
        monkeypatch.setattr(team_buylist, "_permission_gate", lambda request, sess, key: (None, actor))

        response = asyncio.run(
            team_buylist.admin_buylist_submission_approve(
                FakeJsonRequest({}, actor),
                submission_id=submission.id,
                location="Case A",
                session=session,
            )
        )
        refreshed = session.get(BuylistSubmission, submission.id)
        item = session.exec(select(InventoryItem).where(InventoryItem.card_name == "Test Card")).one()
        result = json.loads(refreshed.inventory_result_json)

        assert response.status_code == 303
        assert refreshed.status == "approved"
        assert item.quantity == 1
        assert item.cost_basis == 5.0
        assert item.location == "Case A"
        assert result["items"][0]["inventory_item_id"] == item.id
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_approval_defaults_missing_jp_language(monkeypatch):
    engine, session = _memory_session()
    try:
        actor = _user(30, role="admin")
        submitter = _user(31, role="employee")
        session.add(actor)
        session.add(submitter)
        session.add(
            BuylistSubmission(
                submitted_by_user_id=submitter.id,
                customer_name="Erika",
                payment_view="cash",
                status="submitted",
                totals_json=json.dumps({"cash": 5.0, "trade": 6.0, "quantity": 1, "items": 1}),
                lines_json=json.dumps(
                    [
                        {
                            "id": "jp-card-1",
                            "item_type": "card",
                            "game": "Pokemon JP",
                            "name": "Gengar VMAX",
                            "set_name": "Gengar VMAX High-Class Deck",
                            "number": "002/019",
                            "variant": "Holofoil",
                            "condition": "NM",
                            "market_price": 10.0,
                            "base_market_price": 10.0,
                            "unit_cash": 5.0,
                            "unit_trade": 6.0,
                            "quantity": 1,
                        }
                    ],
                    sort_keys=True,
                ),
            )
        )
        session.commit()
        submission = session.exec(select(BuylistSubmission)).one()
        monkeypatch.setattr(team_buylist, "_permission_gate", lambda request, sess, key: (None, actor))

        asyncio.run(
            team_buylist.admin_buylist_submission_approve(
                FakeJsonRequest({}, actor),
                submission_id=submission.id,
                location="Case JP",
                session=session,
            )
        )
        item = session.exec(select(InventoryItem).where(InventoryItem.card_name == "Gengar VMAX")).one()

        assert item.language == "Japanese"
    finally:
        session.close()
        engine.dispose()


def test_staff_buylist_reject_and_mark_paid_flows(monkeypatch):
    engine, session = _memory_session()
    try:
        actor = _user(40, role="admin")
        submitter = _user(41, role="employee")
        session.add(actor)
        session.add(submitter)
        session.add(
            BuylistSubmission(
                submitted_by_user_id=submitter.id,
                customer_name="Reject Me",
                payment_view="cash",
                status="submitted",
                totals_json="{}",
                lines_json="[]",
            )
        )
        session.add(
            BuylistSubmission(
                submitted_by_user_id=submitter.id,
                customer_name="Pay Me",
                payment_view="cash",
                status="approved",
                totals_json="{}",
                lines_json="[]",
            )
        )
        session.commit()
        rows = session.exec(select(BuylistSubmission).order_by(BuylistSubmission.id)).all()
        reject_row, pay_row = rows
        monkeypatch.setattr(team_buylist, "_permission_gate", lambda request, sess, key: (None, actor))

        reject_response = asyncio.run(
            team_buylist.admin_buylist_submission_reject(
                FakeJsonRequest({}, actor),
                submission_id=reject_row.id,
                decision_notes="Customer passed",
                session=session,
            )
        )
        paid_response = asyncio.run(
            team_buylist.admin_buylist_submission_mark_paid(
                FakeJsonRequest({}, actor),
                submission_id=pay_row.id,
                session=session,
            )
        )
        session.refresh(reject_row)
        session.refresh(pay_row)

        assert reject_response.status_code == 303
        assert paid_response.status_code == 303
        assert reject_row.status == "rejected"
        assert reject_row.rejected_by_user_id == actor.id
        assert reject_row.decision_notes == "Customer passed"
        assert pay_row.status == "paid"
        assert pay_row.paid_by_user_id == actor.id
    finally:
        session.close()
        engine.dispose()
