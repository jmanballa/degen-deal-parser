"""Test: Zweilous should never match Staraptor despite collector number match."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.inventory.pokemon_scanner import (
    ExtractedFields, CandidateCard, score_candidates,
)


def test_name_mismatch_penalty():
    """When OCR says 'Zweilous' but candidate is 'Staraptor', score must be LOW."""
    fields = ExtractedFields()
    fields.card_name = "Zweilous"
    fields.collector_number = "147/086"
    fields.set_name = "DWHTEN"

    staraptor = CandidateCard(
        id="daa-147",
        name="Staraptor",
        number="147/189",
        set_id="swsh3",
        set_name="Darkness Ablaze",
        image_url="",
        rarity="Common",
        source="pokemontcg",
    )

    scored = score_candidates([staraptor], fields)
    assert len(scored) == 1
    result = scored[0]
    print(f"Score: {result.score}  Confidence: {result.confidence}")
    print(f"Breakdown: {result.score_breakdown}")

    assert result.confidence == "LOW", (
        f"Expected LOW, got {result.confidence} (score={result.score})"
    )
    assert result.score < 50, (
        f"Score {result.score} should be below MEDIUM threshold"
    )
    assert "name_mismatch_penalty" in result.score_breakdown, (
        "Missing name_mismatch_penalty in breakdown"
    )
    print("ALL ASSERTIONS PASSED")


def test_correct_name_not_penalized():
    """Zweilous matching Zweilous should NOT get the penalty."""
    fields = ExtractedFields()
    fields.card_name = "Zweilous"
    fields.collector_number = "147/086"

    zweilous = CandidateCard(
        id="twm-147",
        name="Zweilous",
        number="147/086",
        set_id="sv6a",
        set_name="Twilight Masquerade",
        image_url="",
        rarity="Common",
        source="tcgdex",
    )

    scored = score_candidates([zweilous], fields)
    assert len(scored) == 1
    result = scored[0]
    print(f"Score: {result.score}  Confidence: {result.confidence}")
    print(f"Breakdown: {result.score_breakdown}")

    assert "name_mismatch_penalty" not in result.score_breakdown, (
        "Correct match should NOT have penalty"
    )
    assert result.score >= 80, (
        f"Correct match should be HIGH confidence, got {result.score}"
    )
    print("ALL ASSERTIONS PASSED")


if __name__ == "__main__":
    test_name_mismatch_penalty()
    print()
    test_correct_name_not_penalized()
