from pathlib import Path


FINANCE_TEMPLATE = Path(__file__).resolve().parents[1] / "app" / "templates" / "finance.html"


def _template_text() -> str:
    return FINANCE_TEMPLATE.read_text(encoding="utf-8")


def test_finance_hero_uses_cash_specific_owner_language():
    template = _template_text()

    assert "<h1>Finance Dashboard</h1>" in template
    assert '<div class="hero-label">Operating Cash Profit</div>' in template
    assert '<div class="hero-label">Operating Profit</div>' not in template


def test_finance_surfaces_readiness_before_kpis():
    template = _template_text()

    readiness_index = template.index('class="finance-readiness"')
    kpi_index = template.index('class="kpi-grid"')

    assert readiness_index < kpi_index
    assert "Data confidence" in template
    assert "Open ledger cleanup" in template

    data_quality_anchor = template.index('id="data-quality"')
    data_quality_block = template[data_quality_anchor : data_quality_anchor + 500]
    assert '<h2 class="section-title">Data Quality</h2>' in data_quality_block


def test_finance_kpi_grid_does_not_force_clipped_desktop_cards():
    template = _template_text()

    assert "repeat(auto-fit, minmax(170px, 1fr))" in template
    assert ".kpi-grid { grid-template-columns: repeat(3" not in template
