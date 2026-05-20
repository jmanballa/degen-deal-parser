import shutil
import asyncio
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlmodel import Session, SQLModel, create_engine, select
from starlette.requests import Request

from app.inventory.routes import (
    _ADD_STOCK_SINGLE_CACHE,
    _add_stock_category_ids_for_game,
    _add_stock_single_category_id,
    _add_stock_query_looks_sealed,
    _best_sealed_product_match,
    _build_bulk_sealed_preview,
    _cached_add_stock_single_search,
    _inventory_sealed_template_context,
    _normalize_add_stock_game,
    _normalize_add_stock_search_type,
    _pokemon_set_search_queries,
    _receive_sealed_stock,
    _receive_single_stock,
    _sealed_catalog_category_ids_for_query,
    _sealed_catalog_product_match_score,
    _sealed_set_search_queries,
    _sealed_catalog_suggestions,
    _sealed_kind_hints_from_query,
    _single_lookup_suggestions,
    _tcgtracking_sealed_price,
    _tcgtracking_sealed_product,
    inventory_sealed_receive,
    inventory_singles_receive,
)
from app.models import (
    INVENTORY_SOLD,
    InventoryItem,
    InventoryStockMovement,
    ITEM_TYPE_SEALED,
    ITEM_TYPE_SINGLE,
    PriceHistory,
)


class SealedInventoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_sealed_inventory" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "sealed_inventory.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_receive_creates_sealed_item_and_stock_movement(self) -> None:
        with Session(self.engine) as session:
            item, movement, created = _receive_sealed_stock(
                session,
                product_name="Pokemon 151 Booster Bundle",
                set_name="Scarlet & Violet 151",
                sealed_product_kind="Booster Bundle",
                upc="123456789012",
                quantity=4,
                unit_cost=24.5,
                list_price=44.99,
                location="Shelf A",
                source="Distributor",
                notes="Case restock",
                actor_label="counter",
            )

            self.assertTrue(created)
            self.assertEqual(item.item_type, ITEM_TYPE_SEALED)
            self.assertEqual(item.quantity, 4)
            self.assertEqual(item.cost_basis, 24.5)
            self.assertEqual(item.list_price, 44.99)
            self.assertEqual(item.location, "Shelf A")
            self.assertEqual(item.upc, "123456789012")
            self.assertTrue(item.barcode.startswith("DGN-"))
            self.assertEqual(movement.quantity_before, 0)
            self.assertEqual(movement.quantity_after, 4)
            self.assertEqual(movement.total_cost, 98.0)
            self.assertEqual(movement.created_by, "counter")

    def test_receive_again_matches_upc_and_weighted_average_cost(self) -> None:
        with Session(self.engine) as session:
            first, _, _ = _receive_sealed_stock(
                session,
                product_name="Prismatic Evolutions Elite Trainer Box",
                set_name="SV: Prismatic Evolutions",
                sealed_product_kind="Elite Trainer Box",
                upc="196214105133",
                quantity=2,
                unit_cost=90.0,
                location="Wall",
            )
            second, movement, created = _receive_sealed_stock(
                session,
                product_name="Prismatic Evolutions ETB",
                set_name="SV: Prismatic Evolutions",
                sealed_product_kind="Elite Trainer Box",
                upc="196214105133",
                quantity=1,
                unit_cost=120.0,
                location="Wall",
            )

            self.assertFalse(created)
            self.assertEqual(second.id, first.id)
            self.assertEqual(second.quantity, 3)
            self.assertEqual(second.cost_basis, 100.0)
            self.assertEqual(movement.quantity_before, 2)
            self.assertEqual(movement.quantity_after, 3)
            rows = session.exec(select(InventoryStockMovement)).all()
            self.assertEqual(len(rows), 2)

    def test_receive_single_tracks_variant_condition_and_price_history(self) -> None:
        with Session(self.engine) as session:
            item, movement, created = _receive_single_stock(
                session,
                card_name="Charizard ex",
                set_name="151",
                set_code="sv03.5",
                card_number="199/165",
                variant="Holofoil",
                condition="LP",
                image_url="https://example.test/charizard.jpg",
                quantity=2,
                unit_cost=250.0,
                list_price=386.73,
                auto_price=386.73,
                low_price=332.49,
                location="Case A",
                source="Manual Lookup",
                price_payload={
                    "selected_variant": {
                        "name": "Holofoil",
                        "condition_prices": {
                            "NM": {"market": 437.05, "low": 396.69},
                            "LP": {"market": 386.73, "low": 332.49},
                        },
                    }
                },
                actor_label="counter",
            )

            self.assertTrue(created)
            self.assertEqual(item.item_type, ITEM_TYPE_SINGLE)
            self.assertEqual(item.variant, "Holofoil")
            self.assertEqual(item.condition, "LP")
            self.assertEqual(item.quantity, 2)
            self.assertEqual(item.auto_price, 386.73)
            self.assertEqual(item.list_price, 386.73)
            self.assertEqual(movement.quantity_after, 2)
            history = session.exec(select(PriceHistory).where(PriceHistory.item_id == item.id)).one()
            self.assertEqual(history.source, "tcgtracking")
            self.assertEqual(history.market_price, 386.73)

    def test_catalog_suggestions_skip_already_stocked_product(self) -> None:
        with Session(self.engine) as session:
            item, _, _ = _receive_sealed_stock(
                session,
                product_name="Evolving Skies Booster Box",
                set_name="SWSH07: Evolving Skies",
                sealed_product_kind="Booster Box",
                upc="",
                quantity=1,
            )
            api_products = [
                {
                    "name": "Evolving Skies Booster Box",
                    "set_name": "SWSH07: Evolving Skies",
                    "kind": "Booster Box",
                    "upc": "",
                    "image_url": "https://example.test/booster-box.jpg",
                },
                {
                    "name": "Evolving Skies Elite Trainer Box",
                    "set_name": "SWSH07: Evolving Skies",
                    "kind": "Elite Trainer Box",
                    "upc": "",
                    "image_url": "https://example.test/etb.jpg",
                },
            ]
            suggestions = _sealed_catalog_suggestions(api_products, [item], limit=20)

            names = {row["name"] for row in suggestions}
            self.assertNotIn("Evolving Skies Booster Box", names)
            self.assertIn("Evolving Skies Elite Trainer Box", names)

    def test_tcgtracking_product_filter_keeps_sealed_with_image(self) -> None:
        set_info = {"id": 2848, "name": "SWSH07: Evolving Skies"}
        code_card = _tcgtracking_sealed_product(
            product={
                "id": 248026,
                "clean_name": "Code Card Evolving Skies Booster Pack",
                "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/248026_200w.jpg",
            },
            set_info=set_info,
            category_id="3",
            query_kind_hints=set(),
        )
        booster_box = _tcgtracking_sealed_product(
            product={
                "id": 242436,
                "clean_name": "Evolving Skies Booster Box",
                "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/242436_200w.jpg",
                "tcgplayer_url": "https://www.tcgplayer.com/product/242436",
            },
            set_info=set_info,
            category_id="3",
            query_kind_hints=set(),
            pricing={
                "242436": {
                    "tcg": {
                        "Normal": {
                            "low": 1200.00,
                            "market": 1345.67,
                        }
                    }
                }
            },
        )

        self.assertIsNone(code_card)
        self.assertIsNotNone(booster_box)
        assert booster_box is not None
        self.assertEqual(booster_box["kind"], "Booster Box")
        self.assertEqual(booster_box["set_name"], "SWSH07: Evolving Skies")
        self.assertEqual(
            booster_box["image_url"],
            "https://tcgplayer-cdn.tcgplayer.com/product/242436_400w.jpg",
        )
        self.assertEqual(booster_box["market_price"], 1345.67)
        self.assertEqual(booster_box["market_price_source"], "TCGPlayer Market")
        self.assertEqual(booster_box["set_id"], "2848")

    def test_tcgtracking_sealed_price_falls_back_to_low_when_market_missing(self) -> None:
        price, source = _tcgtracking_sealed_price(
            "131867",
            {"131867": {"tcg": {"Normal": {"low": 199.99}}}},
        )

        self.assertEqual(price, 199.99)
        self.assertEqual(source, "TCGPlayer Low")

    def test_old_collection_box_matches_by_product_name_without_set_query(self) -> None:
        product = _tcgtracking_sealed_product(
            product={
                "id": 131867,
                "clean_name": "Kingdra EX Box",
                "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/131867_200w.jpg",
                "tcgplayer_url": "https://www.tcgplayer.com/product/131867/pokemon-sm-base-set-kingdra-ex-box",
            },
            set_info={"id": 1863, "name": "SM Base Set"},
            category_id="3",
            query_kind_hints=set(),
            pricing={"131867": {"tcg": {"Normal": {"low": 199.99}}}},
        )
        assert product is not None

        self.assertEqual(product["kind"], "Collection Box")
        self.assertIsNotNone(_sealed_catalog_product_match_score("kingdra ex", product))
        self.assertIsNotNone(
            _sealed_catalog_product_match_score(
                "https://www.tcgplayer.com/product/131867/pokemon-sm-base-set-kingdra-ex-box",
                product,
            )
        )
        self.assertIsNone(_sealed_catalog_product_match_score("charizard ex 151 199", product))

        newer_bundle = {
            **product,
            "external_id": "656950",
            "name": "Greninja EX and Kingdra EX Special Collection Box",
            "set_name": "Miscellaneous Cards & Products",
        }
        match = _best_sealed_product_match("kingdra ex", [newer_bundle, product])
        assert match is not None
        self.assertEqual(match["external_id"], "131867")

    def test_pokemon_catalog_fallback_stays_english_unless_japanese_requested(self) -> None:
        self.assertEqual(
            _sealed_catalog_category_ids_for_query("kingdra ex", selected_game="Pokemon", category_ids=("3", "85")),
            ("3",),
        )
        self.assertEqual(
            _sealed_catalog_category_ids_for_query("japanese booster box", selected_game="Pokemon", category_ids=("3", "85")),
            ("3", "85"),
        )

    def test_search_queries_handle_specific_collection_box_names(self) -> None:
        queries = _pokemon_set_search_queries("Ascended Heroes Mega Meganium ex Box")

        self.assertIn("ascended heroes", queries)

    def test_pack_plural_is_booster_pack_hint(self) -> None:
        hints = _sealed_kind_hints_from_query("151 Packs English")

        self.assertIn("Booster Pack", hints)

    def test_best_match_prefers_regular_product_over_case(self) -> None:
        products = [
            {
                "name": "Evolving Skies Booster Box Case",
                "set_name": "SWSH07: Evolving Skies",
                "kind": "Booster Box Case",
            },
            {
                "name": "Evolving Skies Booster Box",
                "set_name": "SWSH07: Evolving Skies",
                "kind": "Booster Box",
            },
        ]
        match = _best_sealed_product_match("Evolving Skies Booster Box", products)

        assert match is not None
        self.assertEqual(match["name"], "Evolving Skies Booster Box")

    def test_spc_shorthand_maps_to_super_premium_collection(self) -> None:
        product = _tcgtracking_sealed_product(
            product={
                "id": 622770,
                "clean_name": "Prismatic Evolutions Super Premium Collection",
                "image_url": "https://example.test/spc_200w.jpg",
                "tcgplayer_url": "https://www.tcgplayer.com/product/622770/pokemon-sv-prismatic-evolutions-prismatic-evolutions-superpremium-collection",
            },
            set_info={"name": "SV: Prismatic Evolutions"},
            category_id="3",
            query_kind_hints=_sealed_kind_hints_from_query("prismatic spc"),
            pricing=None,
        )
        case_product = _tcgtracking_sealed_product(
            product={
                "id": 638058,
                "clean_name": "Prismatic Evolutions Super Premium Collection Case",
                "image_url": "https://example.test/spc-case_200w.jpg",
            },
            set_info={"name": "SV: Prismatic Evolutions"},
            category_id="3",
            query_kind_hints=_sealed_kind_hints_from_query("prismatic spc"),
            pricing=None,
        )

        self.assertIsNotNone(product)
        self.assertIsNotNone(case_product)
        assert product is not None
        assert case_product is not None
        self.assertEqual(product["kind"], "Super Premium Collection")
        self.assertIn("prismatic", _pokemon_set_search_queries("prismatic spc"))
        match = _best_sealed_product_match("prismatic spc", [case_product, product])
        assert match is not None
        self.assertEqual(match["external_id"], "622770")

    def test_riftbound_origins_booster_display_is_searchable(self) -> None:
        display = _tcgtracking_sealed_product(
            product={
                "id": 635368,
                "clean_name": "Origins Booster Display",
                "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/635368_200w.jpg",
                "tcgplayer_url": "https://www.tcgplayer.com/product/635368/riftbound-league-of-legends-trading-card-game-origins-origins-booster-display",
            },
            set_info={"name": "Origins"},
            category_id="89",
            query_kind_hints=_sealed_kind_hints_from_query("origins booster display"),
            pricing={"635368": {"tcg": {"Normal": {"market": 185.42}}}},
            game="Riftbound",
        )
        display_from_box_query = _tcgtracking_sealed_product(
            product={
                "id": 635368,
                "clean_name": "Origins Booster Display",
                "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/635368_200w.jpg",
            },
            set_info={"name": "Origins"},
            category_id="89",
            query_kind_hints=_sealed_kind_hints_from_query("origins booster box"),
            pricing=None,
            game="Riftbound",
        )
        case_display = _tcgtracking_sealed_product(
            product={
                "id": 635369,
                "clean_name": "Origins Booster Display Case",
                "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/635369_200w.jpg",
            },
            set_info={"name": "Origins"},
            category_id="89",
            query_kind_hints=_sealed_kind_hints_from_query("origins booster display"),
            pricing=None,
            game="Riftbound",
        )

        self.assertIsNotNone(display)
        self.assertIsNotNone(display_from_box_query)
        self.assertIsNotNone(case_display)
        assert display is not None
        assert case_display is not None
        self.assertEqual(display["kind"], "Booster Display")
        self.assertEqual(display["game"], "Riftbound")
        self.assertEqual(display["market_price"], 185.42)
        self.assertIn("origins", _sealed_set_search_queries("origins booster display", game="Riftbound"))
        self.assertIn("origins", _sealed_set_search_queries("origins booster box", game="Riftbound"))
        self.assertIn(
            "origins",
            _sealed_set_search_queries(
                "https://www.tcgplayer.com/product/635368/riftbound-league-of-legends-trading-card-game-origins-origins-booster-display?Language=English",
                game="Riftbound",
            ),
        )
        match = _best_sealed_product_match("origins booster display", [case_display, display])
        assert match is not None
        self.assertEqual(match["external_id"], "635368")

    def test_add_stock_game_aliases_route_to_tcgtracking_categories(self) -> None:
        self.assertEqual(_normalize_add_stock_game("mtg"), "Magic")
        self.assertEqual(_normalize_add_stock_game("yugioh"), "Yu-Gi-Oh")
        self.assertEqual(_normalize_add_stock_game("opcg"), "One Piece")
        self.assertEqual(_normalize_add_stock_game("pokemon jp"), "Pokemon Japan")
        self.assertEqual(_add_stock_single_category_id("Magic"), "1")
        self.assertEqual(_add_stock_single_category_id("Pokemon Japan"), "85")
        self.assertEqual(_add_stock_category_ids_for_game("One Piece"), ("68",))
        self.assertEqual(_add_stock_single_category_id("Digimon"), "63")
        self.assertEqual(_add_stock_single_category_id("Weiss Schwarz"), "20")
        self.assertEqual(_add_stock_single_category_id("Union Arena"), "81")

    def test_add_stock_search_type_aliases(self) -> None:
        self.assertEqual(_normalize_add_stock_search_type("card"), "cards")
        self.assertEqual(_normalize_add_stock_search_type("single"), "cards")
        self.assertEqual(_normalize_add_stock_search_type("products"), "sealed")
        self.assertEqual(_normalize_add_stock_search_type("auto"), "both")
        self.assertEqual(_normalize_add_stock_search_type("something weird"), "both")

    def test_add_stock_cards_filter_skips_sealed_lookup(self) -> None:
        sealed_search = AsyncMock(return_value=([{"name": "Should Not Run"}], ""))
        single_search = AsyncMock(
            return_value=(
                [
                    {
                        "name": "Charizard ex",
                        "set_name": "151",
                        "card_number": "199/165",
                        "variants": [],
                        "default_variant": "",
                        "default_condition": "NM",
                        "default_price": 437.05,
                    }
                ],
                "",
            )
        )
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/inventory/add-stock",
                "headers": [],
                "query_string": b"",
                "session": {},
            }
        )

        with Session(self.engine) as session, patch(
            "app.inventory.routes._cached_add_stock_sealed_search",
            sealed_search,
        ), patch(
            "app.inventory.routes._cached_add_stock_single_search",
            single_search,
        ), patch("app.inventory.routes.issue_token", return_value="csrf"):
            context = asyncio.run(
                _inventory_sealed_template_context(
                    request,
                    session,
                    game="Pokemon",
                    q="charizard ex 151 199",
                    search_type="cards",
                )
            )

        sealed_search.assert_not_called()
        single_search.assert_awaited_once()
        self.assertEqual(context["search_type"], "cards")
        self.assertEqual(context["suggestions"], [])
        self.assertEqual(context["single_results"][0]["name"], "Charizard ex")

    def test_add_stock_sealed_filter_skips_single_lookup(self) -> None:
        sealed_search = AsyncMock(
            return_value=(
                [
                    {
                        "name": "Kingdra EX Box",
                        "set_name": "SM Base Set",
                        "kind": "Collection Box",
                        "upc": "",
                    }
                ],
                "",
            )
        )
        single_search = AsyncMock(return_value=([], "Should Not Run"))
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/inventory/add-stock",
                "headers": [],
                "query_string": b"",
                "session": {},
            }
        )

        with Session(self.engine) as session, patch(
            "app.inventory.routes._cached_add_stock_sealed_search",
            sealed_search,
        ), patch(
            "app.inventory.routes._cached_add_stock_single_search",
            single_search,
        ), patch("app.inventory.routes.issue_token", return_value="csrf"):
            context = asyncio.run(
                _inventory_sealed_template_context(
                    request,
                    session,
                    game="Pokemon",
                    q="kingdra ex",
                    search_type="sealed",
                )
            )

        sealed_search.assert_awaited_once()
        single_search.assert_not_called()
        self.assertEqual(context["search_type"], "sealed")
        self.assertEqual(context["single_results"], [])
        self.assertEqual(context["suggestions"][0]["name"], "Kingdra EX Box")

    def test_tcgtracking_magic_bundle_maps_to_magic_sealed_product(self) -> None:
        product = _tcgtracking_sealed_product(
            product={
                "id": 612345,
                "clean_name": "Final Fantasy Bundle",
                "image_url": "https://example.test/final-fantasy-bundle_200w.jpg",
            },
            set_info={"name": "FIN: Final Fantasy"},
            category_id="1",
            query_kind_hints=_sealed_kind_hints_from_query("final fantasy bundle"),
            game="Magic",
            pricing=None,
        )

        self.assertIsNotNone(product)
        assert product is not None
        self.assertEqual(product["kind"], "Bundle")
        self.assertEqual(product["game"], "Magic")

    def test_starter_kit_and_lorcana_trove_are_sealed_kinds(self) -> None:
        starter = _tcgtracking_sealed_product(
            product={"id": 1, "clean_name": "FINAL FANTASY Starter Kit"},
            set_info={"name": "FINAL FANTASY"},
            category_id="1",
            query_kind_hints=_sealed_kind_hints_from_query("final fantasy starter kit"),
            game="Magic",
        )
        trove = _tcgtracking_sealed_product(
            product={"id": 2, "clean_name": "Disney Lorcana: Rise of the Floodborn Illumineer's Trove"},
            set_info={"name": "Rise of the Floodborn"},
            category_id="71",
            query_kind_hints=_sealed_kind_hints_from_query("rise of the floodborn illumineer's trove"),
            game="Lorcana",
        )

        assert starter is not None
        assert trove is not None
        self.assertEqual(starter["kind"], "Starter Kit")
        self.assertEqual(trove["kind"], "Illumineer's Trove")

    def test_lorcana_search_queries_drop_game_words_and_possessive_noise(self) -> None:
        queries = _sealed_set_search_queries(
            "Disney Lorcana: Archazia's Island Booster Box",
            game="Lorcana",
        )

        self.assertIn("archazia island", queries)
        self.assertIn("archazia", queries)
        self.assertNotIn("disney lorcana archazia s island", queries)

    def test_bulk_preview_parses_list_and_selects_matches(self) -> None:
        async def fake_search(query: str, *, game: str = "Pokemon", limit: int = 16):
            self.assertEqual(game, "Pokemon")
            mapping = {
                "ascended heroes etbs": [
                    {
                        "name": "Ascended Heroes Elite Trainer Box",
                        "set_name": "ME: Ascended Heroes",
                        "kind": "Elite Trainer Box",
                        "upc": "",
                        "image_url": "https://example.test/etb.jpg",
                        "market_price": 79.99,
                    }
                ],
                "151 Packs English": [
                    {
                        "name": "151 Booster Pack",
                        "set_name": "SV: Scarlet & Violet 151",
                        "kind": "Booster Pack",
                        "upc": "",
                        "image_url": "https://example.test/151-pack.jpg",
                        "market_price": 28.15,
                    }
                ],
            }
            return mapping.get(query, []), ""

        with patch("app.inventory.routes._search_sealed_products", side_effect=fake_search):
            rows = asyncio.run(
                _build_bulk_sealed_preview("5 ascended heroes etbs\n10 151 Packs English")
            )

        self.assertEqual(rows[0]["quantity"], 5)
        self.assertEqual(rows[0]["product"]["name"], "Ascended Heroes Elite Trainer Box")
        self.assertEqual(rows[1]["quantity"], 10)
        self.assertEqual(rows[1]["product"]["kind"], "Booster Pack")
        self.assertEqual(rows[1]["product"]["market_price"], 28.15)

    def test_add_stock_detects_sealed_product_queries(self) -> None:
        self.assertTrue(_add_stock_query_looks_sealed("ascended heroes etb"))
        self.assertTrue(_add_stock_query_looks_sealed("151 Packs English"))
        self.assertTrue(_add_stock_query_looks_sealed("Mega Meganium ex Box"))
        self.assertTrue(_add_stock_query_looks_sealed("prismatic spc"))
        self.assertFalse(_add_stock_query_looks_sealed("charizard ex 151 199"))

    def test_single_lookup_cache_uses_fast_parse(self) -> None:
        _ADD_STOCK_SINGLE_CACHE.clear()

        async def fake_search(
            query: str,
            *,
            category_id: str = "3",
            use_ai_parse: bool = True,
            max_results: int = 8,
            include_pokemontcg_supplement: bool = True,
        ):
            self.assertEqual(query, "charizard ex 151 199")
            self.assertEqual(category_id, "3")
            self.assertFalse(use_ai_parse)
            self.assertEqual(max_results, 6)
            self.assertFalse(include_pokemontcg_supplement)
            return {
                "game": "Pokemon",
                "status": "MATCHED",
                "best_match": {
                    "name": "Charizard ex",
                    "set_name": "151",
                    "number": "199/165",
                    "image_url": "https://example.test/charizard.jpg",
                    "available_variants": [{"name": "Holofoil", "price": 443.76}],
                },
                "candidates": [],
            }

        with patch("app.inventory.routes.text_search_cards", side_effect=fake_search) as mocked:
            first = asyncio.run(_cached_add_stock_single_search("charizard ex 151 199", game="Pokemon"))
            second = asyncio.run(_cached_add_stock_single_search("charizard ex 151 199", game="Pokemon"))

        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(first[0][0]["name"], "Charizard ex")
        self.assertEqual(second[0][0]["name"], "Charizard ex")
        self.assertEqual(first[1], "")

    def test_single_lookup_suggestions_include_condition_market_prices(self) -> None:
        result = {
            "game": "Pokemon",
            "status": "MATCHED",
            "best_match": {
                "name": "Charizard ex",
                "set_name": "151",
                "set_id": "sv03.5",
                "number": "199/165",
                "image_url": "https://example.test/charizard.jpg",
                "available_variants": [
                    {
                        "name": "Holofoil",
                        "price": 443.76,
                        "low_price": 384.75,
                        "conditions": {
                            "NM": {"mkt": 437.05, "low": 396.69},
                            "LP": {"mkt": 386.73, "low": 332.49},
                            "MP": {"mkt": 276.12, "low": 181.25},
                            "HP": {"mkt": 228.12, "low": 168.0},
                            "DMG": {"mkt": 216.71, "low": 124.97},
                        },
                    }
                ],
            },
            "candidates": [],
        }

        suggestions = _single_lookup_suggestions(result)

        self.assertEqual(suggestions[0]["default_price"], 437.05)
        prices = suggestions[0]["variants"][0]["condition_prices"]
        self.assertEqual(prices["LP"]["market"], 386.73)
        self.assertEqual(prices["DMG"]["low"], 124.97)

    def test_receive_route_creates_stock_and_redirects(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/inventory/sealed/receive",
                "headers": [],
            }
        )
        with Session(self.engine) as session:
            with patch("app.inventory.routes._require_employee_permission", return_value=None), patch(
                "app.inventory.routes._current_user_label", return_value="tester"
            ):
                response = asyncio.run(
                    inventory_sealed_receive(
                        request=request,
                        session=session,
                        item_id="",
                        game="Pokemon",
                        product_name="Surging Sparks Booster Box",
                        set_name="SV: Surging Sparks",
                        sealed_product_kind="Booster Box",
                        upc="",
                        image_url="https://example.test/surging-sparks-booster-box.jpg",
                        quantity="2",
                        unit_cost="95.50",
                        list_price="159.99",
                        auto_price="164.50",
                        location="Wall",
                        source="Distributor",
                        notes="",
                    )
                )

            self.assertEqual(response.status_code, 303)
            item = session.exec(
                select(InventoryItem).where(
                    InventoryItem.item_type == ITEM_TYPE_SEALED,
                    InventoryItem.card_name == "Surging Sparks Booster Box",
                )
            ).one()
            self.assertEqual(item.quantity, 2)
            self.assertEqual(item.cost_basis, 95.5)
            self.assertEqual(item.auto_price, 164.5)
            self.assertEqual(item.image_url, "https://example.test/surging-sparks-booster-box.jpg")
            movement = session.exec(
                select(InventoryStockMovement).where(InventoryStockMovement.item_id == item.id)
            ).one()
            self.assertEqual(movement.quantity_after, 2)
            self.assertEqual(movement.created_by, "tester")
            history = session.exec(select(PriceHistory).where(PriceHistory.item_id == item.id)).one()
            self.assertEqual(history.source, "tcgtracking")
            self.assertEqual(history.market_price, 164.5)

    def test_single_receive_route_uses_selected_condition_price(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/inventory/singles/receive",
                "headers": [],
            }
        )
        variants_json = """
        [
          {
            "name": "Holofoil",
            "market_price": 443.76,
            "low_price": 384.75,
            "condition_prices": {
              "NM": {"market": 437.05, "low": 396.69},
              "LP": {"market": 386.73, "low": 332.49}
            }
          }
        ]
        """
        with Session(self.engine) as session:
            with patch("app.inventory.routes._require_employee_permission", return_value=None), patch(
                "app.inventory.routes._current_user_label", return_value="tester"
            ):
                response = asyncio.run(
                    inventory_singles_receive(
                        request=request,
                        session=session,
                        game="Pokemon",
                        card_name="Charizard ex",
                        set_name="151",
                        set_code="sv03.5",
                        card_number="199/165",
                        variant="Holofoil",
                        variants_json=variants_json,
                        condition="LP",
                        image_url="https://example.test/charizard.jpg",
                        quantity="1",
                        unit_cost="250",
                        list_price="386.73",
                        location="Case A",
                        source="Manual Lookup",
                        notes="",
                    )
                )

            self.assertEqual(response.status_code, 303)
            item = session.exec(
                select(InventoryItem).where(
                    InventoryItem.item_type == ITEM_TYPE_SINGLE,
                    InventoryItem.card_name == "Charizard ex",
                )
            ).one()
            self.assertEqual(item.variant, "Holofoil")
            self.assertEqual(item.condition, "LP")
            self.assertEqual(item.auto_price, 386.73)

    def test_shopify_sale_decrements_sealed_quantity_and_logs_movement(self) -> None:
        from app.inventory.shopify_ingest import mark_inventory_sold_from_shopify_order
        from app.models import ShopifySyncJob

        with Session(self.engine) as session:
            item = InventoryItem(
                barcode="DGN-SHOP1",
                item_type=ITEM_TYPE_SEALED,
                game="Pokemon",
                card_name="Prismatic Evolutions Booster Bundle",
                quantity=5,
            )
            session.add(item)
            session.commit()
            session.refresh(item)

            marked = mark_inventory_sold_from_shopify_order(
                session,
                {
                    "id": "shop-order-1",
                    "line_items": [
                        {"sku": "DGN-SHOP1", "quantity": 2, "price": "39.99"},
                    ],
                },
                runtime_name="unit-test",
            )

            self.assertEqual(marked, 1)
            session.refresh(item)
            self.assertEqual(item.quantity, 3)
            self.assertNotEqual(item.status, INVENTORY_SOLD)
            movement = session.exec(
                select(InventoryStockMovement).where(InventoryStockMovement.item_id == item.id)
            ).one()
            self.assertEqual(movement.reason, "sale")
            self.assertEqual(movement.quantity_delta, -2)
            self.assertEqual(movement.quantity_before, 5)
            self.assertEqual(movement.quantity_after, 3)
            sync_job = session.exec(
                select(ShopifySyncJob).where(ShopifySyncJob.item_id == item.id)
            ).one()
            self.assertEqual(sync_job.action, "quantity")
            self.assertEqual(sync_job.status, "pending")
            self.assertEqual(sync_job.source, "Shopify order webhook")

            marked_again = mark_inventory_sold_from_shopify_order(
                session,
                {
                    "id": "shop-order-2",
                    "line_items": [
                        {"sku": "DGN-SHOP1", "quantity": 3, "price": "39.99"},
                    ],
                },
                runtime_name="unit-test",
            )

            self.assertEqual(marked_again, 1)
            session.refresh(item)
            self.assertEqual(item.quantity, 0)
            self.assertEqual(item.status, INVENTORY_SOLD)

    def test_shopify_sale_webhook_retry_does_not_double_decrement(self) -> None:
        from app.inventory.shopify_ingest import mark_inventory_sold_from_shopify_order
        from app.models import ShopifySyncJob

        with Session(self.engine) as session:
            item = InventoryItem(
                barcode="DGN-RETRY1",
                item_type=ITEM_TYPE_SEALED,
                game="Pokemon",
                card_name="Retry Booster Box",
                quantity=5,
            )
            session.add(item)
            session.commit()
            session.refresh(item)

            payload = {
                "id": "shop-order-retry",
                "line_items": [
                    {"sku": "DGN-RETRY1", "quantity": 2, "price": "99.99"},
                ],
            }
            first = mark_inventory_sold_from_shopify_order(session, payload, runtime_name="unit-test")
            second = mark_inventory_sold_from_shopify_order(session, payload, runtime_name="unit-test")

            self.assertEqual(first, 1)
            self.assertEqual(second, 0)
            session.refresh(item)
            self.assertEqual(item.quantity, 3)
            movements = session.exec(
                select(InventoryStockMovement).where(InventoryStockMovement.item_id == item.id)
            ).all()
            self.assertEqual(len(movements), 1)
            sync_jobs = session.exec(
                select(ShopifySyncJob).where(ShopifySyncJob.item_id == item.id)
            ).all()
            self.assertEqual(len(sync_jobs), 1)

    def test_shopify_unknown_sku_creates_visible_sync_issue(self) -> None:
        from app.inventory.shopify_ingest import mark_inventory_sold_from_shopify_order
        from app.models import ShopifySyncIssue

        with Session(self.engine) as session:
            marked = mark_inventory_sold_from_shopify_order(
                session,
                {
                    "id": "shop-order-missing",
                    "name": "#MISSING",
                    "line_items": [
                        {"sku": "DGN-NOTFOUND", "quantity": 1, "title": "Unknown Shopify Product"},
                    ],
                },
                runtime_name="unit-test",
            )

            self.assertEqual(marked, 0)
            issue = session.exec(select(ShopifySyncIssue)).one()
            self.assertEqual(issue.issue_type, "unknown_sku")
            self.assertEqual(issue.shopify_sku, "DGN-NOTFOUND")
            self.assertEqual(issue.status, "open")
            self.assertIn("shop-order-missing", issue.message)


if __name__ == "__main__":
    unittest.main()
