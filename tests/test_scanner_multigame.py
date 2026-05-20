"""Smoke-level regression test for multi-game scanner routing.

Doesn't hit live APIs — monkeypatches the underlying search functions and
verifies _lookup_candidates_by_category sends each category_id to the
right backend with the right keyword arguments.

For live-API smoke coverage, see scripts/smoke_test_degen_eye.py.
"""
import asyncio
import os
import sys
from dataclasses import asdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.inventory import pokemon_scanner as ps


def _sync(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


class _Recorder:
    """Monkeypatch target — captures positional+keyword call args."""
    def __init__(self, fake_result=None):
        self.calls = []
        self.fake_result = fake_result if fake_result is not None else []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.fake_result


def _patch_all(monkey):
    """Replace every backend searcher with a recorder."""
    stubs = {}
    for attr in (
        "_scryfall_search",
        "_ygoprodeck_search",
        "_optcg_search",
        "_lorcast_search",
        "_riftbound_search",
        "_tcgtracking_pokemon_jp_search",
        "_tcgtracking_product_search",
        "lookup_candidates",
    ):
        stubs[attr] = _Recorder()
        monkey(attr, stubs[attr])
    return stubs


def test_routing_magic(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Bolt", set_name="4th", collector_number="208")
    _sync(ps._lookup_candidates_by_category(fields, "1"))
    assert len(stubs["_scryfall_search"].calls) == 1
    _args, kwargs = stubs["_scryfall_search"].calls[0]
    assert kwargs["name"] == "Bolt"
    assert kwargs["set_name"] == "4th"
    assert kwargs["number"] == "208"


def test_routing_one_piece_passes_number(monkeypatch):
    """OPTCG search must get the collector_number so downstream can post-filter."""
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Luffy", set_name="Romance Dawn", collector_number="OP01-003")
    _sync(ps._lookup_candidates_by_category(fields, "68"))
    _args, kwargs = stubs["_optcg_search"].calls[0]
    assert kwargs["number"] == "OP01-003"


def test_routing_riftbound(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Annie", set_name="Origins", collector_number="001/024")
    _sync(ps._lookup_candidates_by_category(fields, "89"))
    assert len(stubs["_riftbound_search"].calls) == 1
    # Generic TCGTracking fallback should NOT be hit for Riftbound.
    assert len(stubs["_tcgtracking_product_search"].calls) == 0


def test_routing_pokemon_uses_waterfall(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Pikachu", collector_number="25")
    _sync(ps._lookup_candidates_by_category(fields, "3"))
    assert len(stubs["lookup_candidates"].calls) == 1
    # And not any of the per-game branches.
    for attr in ("_scryfall_search", "_optcg_search", "_ygoprodeck_search",
                 "_lorcast_search", "_riftbound_search", "_tcgtracking_pokemon_jp_search",
                 "_tcgtracking_product_search"):
        assert len(stubs[attr].calls) == 0, f"{attr} should not be called for Pokemon"


def test_pokemon_set_search_uses_targeted_supplement_even_in_fast_mode(monkeypatch):
    monkeypatch.setattr(ps, "_fetch_tcgdex_sets", _Recorder([]))
    monkeypatch.setattr(ps, "_tcgdex_search_by_name", _Recorder([]))
    supplement = _Recorder([
        ps.CandidateCard(
            id="sv03.5-025",
            name="Pikachu",
            number="25",
            set_name="151",
            source="pokemontcg",
        )
    ])
    monkeypatch.setattr(ps, "_pokemontcg_search", supplement)

    fields = ps.ExtractedFields(card_name="Pikachu", set_name="151")
    results = _sync(ps.lookup_candidates(fields, include_pokemontcg_supplement=False))

    assert results[0].id == "sv03.5-025"
    _args, kwargs = supplement.calls[0]
    assert kwargs["name"] == "Pikachu"
    assert kwargs["set_name"] == "151"


def test_pokemon_set_search_skips_targeted_supplement_when_tcgdex_found_set(monkeypatch):
    monkeypatch.setattr(ps, "_fetch_tcgdex_sets", _Recorder([]))
    monkeypatch.setattr(
        ps,
        "_tcgdex_search_by_name",
        _Recorder([
            {
                "id": "sv03.5-025",
                "name": "Pikachu",
                "localId": "025",
                "set": {"id": "sv03.5", "name": "151", "cardCount": {"official": 165}},
            }
        ]),
    )
    supplement = _Recorder([])
    monkeypatch.setattr(ps, "_pokemontcg_search", supplement)

    fields = ps.ExtractedFields(card_name="Pikachu", set_name="151")
    results = _sync(ps.lookup_candidates(fields, include_pokemontcg_supplement=False))

    assert results[0].id == "sv03.5-025"
    assert supplement.calls == []


def test_tcgdex_name_search_prioritizes_inferred_set_id(monkeypatch):
    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def json(self):
            return self._payload

    class FakeClient:
        async def get(self, url, params=None):
            if url.endswith("/cards") and params == {"name": "Pikachu"}:
                return FakeResponse([
                    {"id": "basep-1", "localId": "1"},
                    {"id": "sv03.5-025", "localId": "025"},
                ])
            if url.endswith("/cards/sv03.5-025"):
                return FakeResponse({"id": "sv03.5-025", "localId": "025"})
            if url.endswith("/cards/basep-1"):
                return FakeResponse({"id": "basep-1", "localId": "1"})
            raise AssertionError(url)

    async def fake_sets():
        return [{"id": "sv03.5", "name": "151"}]

    monkeypatch.setattr(ps, "_fetch_tcgdex_sets", fake_sets)

    results = _sync(ps._tcgdex_search_by_name(FakeClient(), "Pikachu", limit=1, prefer_set="151"))

    assert results[0]["id"] == "sv03.5-025"


def test_routing_pokemon_jp_uses_tcgtracking_not_english_waterfall(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Charizard ex", set_name="Scarlet & Violet: 151")

    _sync(ps._lookup_candidates_by_category(fields, "85"))

    assert len(stubs["_tcgtracking_pokemon_jp_search"].calls) == 1
    assert len(stubs["lookup_candidates"].calls) == 0


def test_pokemon_jp_set_queries_fall_back_to_card_name_without_set():
    fields = ps.ExtractedFields(card_name="Gengar VMAX")

    queries = ps._pokemon_jp_set_queries(fields)

    assert "Gengar VMAX" in queries


def test_tcgtracking_number_match_keeps_serial_suffix_distinct():
    assert ps._nums_match("748z", "748") is False
    assert ps._nums_match("748z", "748z") is True
    assert ps._nums_match("42", "142") is False


def test_score_candidates_prefers_full_token_match_over_partial_name():
    fields = ps.ExtractedFields(card_name="gengar vmax")
    candidates = [
        ps.CandidateCard(id="v", name="Gengar V", number="001/019"),
        ps.CandidateCard(id="vmax", name="Gengar VMAX - 002/019", number="002/019"),
    ]

    scored = ps.score_candidates(candidates, fields)

    assert scored[0].id == "vmax"


def test_routing_unknown_falls_through_to_tcgtracking(monkeypatch):
    stubs = _patch_all(lambda a, v: monkeypatch.setattr(ps, a, v))
    fields = ps.ExtractedFields(card_name="Whatever", set_name="Some Set")
    _sync(ps._lookup_candidates_by_category(fields, "63"))  # Digimon
    assert len(stubs["_tcgtracking_product_search"].calls) == 1
    _args, kwargs = stubs["_tcgtracking_product_search"].calls[0]
    assert kwargs["category_id"] == "63"


def test_category_game_map_includes_riftbound():
    assert ps._CATEGORY_TO_GAME.get("89") == "Riftbound"
    assert ps._VISION_GAME_TO_CATEGORY.get("riftbound") == "89"
    assert ps._XIMILAR_TAG_TO_CATEGORY.get("riftbound") == "89"
    assert ps._CATEGORY_TO_GAME.get("63") == "Digimon"
    assert ps._CATEGORY_TO_GAME.get("20") == "Weiss Schwarz"
    assert ps._CATEGORY_TO_GAME.get("81") == "Union Arena"


def test_vision_prompt_mentions_riftbound():
    assert "riftbound" in ps._VISION_IDENTIFY_PROMPT.lower()


def test_manual_fallback_covers_supported_games():
    ids = {c["id"] for c in ps._MANUAL_CATEGORY_FALLBACK}
    for must_have in ("3", "89", "1", "2", "68", "71"):
        assert must_have in ids, f"Manual fallback missing category {must_have}"


def test_tcgtracking_enrichment_maps_non_pokemon_variant_codes():
    ps._tcgtracking_cache.clear()
    ps._tcgtracking_cache["1:final fantasy"] = {
        "set_id": 24219,
        "cat_id": "1",
        "products": [
            {
                "id": 630917,
                "clean_name": "Summon Bahamut",
                "number": "1",
                "tcgplayer_url": "https://www.tcgplayer.com/product/630917/magic-test",
            }
        ],
        "pricing": {
            "630917": {
                "tcg": {
                    "Foil": {"market": 22.89, "low": 18.0},
                    "Normal": {"market": 19.62, "low": 14.92},
                }
            }
        },
        "skus": {
            "630917": {
                "8694295": {"cnd": "NM", "var": "N", "lng": "EN", "mkt": 19.12, "low": 13.99, "cnt": 25},
                "8694300": {"cnd": "NM", "var": "F", "lng": "EN", "mkt": 22.81, "low": 19.67, "cnt": 25},
                "8694296": {"cnd": "LP", "var": "N", "lng": "EN", "mkt": 17.96, "low": 16.54, "cnt": 25},
                "8694345": {"cnd": "NM", "var": "N", "lng": "ES"},
            }
        },
    }
    candidate = ps.ScoredCandidate(
        id="scryfall-1",
        name="Summon Bahamut",
        number="1",
        set_name="FINAL FANTASY",
        source="scryfall",
    )

    try:
        _sync(ps._enrich_price_fast(candidate, category_id="1"))
    finally:
        ps._tcgtracking_cache.clear()

    assert candidate.market_price == 19.62
    variants = {row["name"]: row for row in candidate.available_variants}
    assert variants["Normal"]["conditions"]["NM"]["mkt"] == 19.12
    assert variants["Normal"]["conditions"]["LP"]["mkt"] == 17.96
    assert variants["Foil"]["conditions"]["NM"]["mkt"] == 22.81


def test_tcgtracking_jp_variants_use_japanese_skus_only():
    cached = {
        "cat_id": "85",
        "pricing": {"566351": {"tcg": {"Normal": {"market": 6.25, "low": 4.0}}}},
        "skus": {
            "566351": {
                "jp-nm": {"cnd": "NM", "var": "N", "lng": "JP", "mkt": 6.25, "low": 4.0},
                "en-nm": {"cnd": "NM", "var": "N", "lng": "EN", "mkt": 999.0, "low": 999.0},
            }
        },
    }

    variants, market = ps._tcgtracking_variants_for_product("566351", cached, "85")

    assert market == 6.25
    assert variants[0]["conditions"]["NM"]["mkt"] == 6.25
    assert variants[0]["conditions"]["NM"]["sku_id"] == "jp-nm"


def test_tcgtracking_set_selection_prefers_exact_set_name():
    selected = ps._select_tcgtracking_set(
        [
            {"id": 24341, "name": "Art Series: FINAL FANTASY"},
            {"id": 24220, "name": "Commander: FINAL FANTASY"},
            {"id": 24219, "name": "FINAL FANTASY"},
        ],
        "FINAL FANTASY",
    )

    assert selected["id"] == 24219


def test_tcgtracking_set_selection_prefers_set_code_over_art_series_prefix():
    selected = ps._select_tcgtracking_set(
        [
            {"id": 23223, "name": "Art Series: Universes Beyond: The Lord of the Rings: Tales of Middle-earth", "abbreviation": "ASLTR"},
            {"id": 23071, "name": "Commander: The Lord of the Rings: Tales of Middle-earth", "abbreviation": "LTC"},
            {"id": 23019, "name": "Universes Beyond: The Lord of the Rings: Tales of Middle-earth", "abbreviation": "LTR"},
        ],
        "The Lord of the Rings: Tales of Middle-earth",
        "ltr",
    )

    assert selected["id"] == 23019


def test_scryfall_name_only_search_prefers_edhrec_and_foil_only_price():
    class Response:
        status_code = 200
        text = ""

        def json(self):
            return {
                "data": [
                    {
                        "id": "scryfall-791",
                        "name": "The One Ring",
                        "collector_number": "791",
                        "set": "ltr",
                        "set_name": "The Lord of the Rings: Tales of Middle-earth",
                        "prices": {"usd": None, "usd_foil": "201.44"},
                        "purchase_uris": {"tcgplayer": "https://www.tcgplayer.com/product/517966"},
                    }
                ]
            }

    class Client:
        def __init__(self):
            self.calls = []

        async def get(self, url, params=None):
            self.calls.append((url, params))
            return Response()

    client = Client()
    results = _sync(ps._scryfall_search(client, name="The One Ring"))

    assert client.calls[0][1]["order"] == "edhrec"
    assert results[0].market_price == 201.44
    assert results[0].available_variants == [{"name": "Foil", "price": 201.44}]
