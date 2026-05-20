"""DATA_ROOT / MEDIA_ROOT resolution.

Covers the Green-deployment promise that durable runtime data lives under
DATA_ROOT (default repo-relative `data/` for dev), and that legacy `data/...`
relative values in env files keep resolving correctly when DATA_ROOT is set
to an absolute path like /opt/degen/data.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app import config as cfg


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    cfg.get_settings.cache_clear()
    yield
    cfg.get_settings.cache_clear()


def test_data_root_defaults_to_repo_relative():
    settings = cfg.Settings(DATA_ROOT="data")
    assert settings.data_root_path == cfg.BASE_DIR / "data"
    assert settings.data_path("attachments") == cfg.BASE_DIR / "data" / "attachments"


def test_data_root_absolute_path_is_used_verbatim(tmp_path: Path):
    settings = cfg.Settings(DATA_ROOT=str(tmp_path))
    assert settings.data_root_path == tmp_path
    assert settings.media_root_path == tmp_path  # MEDIA_ROOT inherits


def test_media_root_overrides_data_root(tmp_path: Path):
    media = tmp_path / "media"
    settings = cfg.Settings(DATA_ROOT=str(tmp_path), MEDIA_ROOT=str(media))
    assert settings.data_root_path == tmp_path
    assert settings.media_root_path == media
    assert settings.media_path("attachments") == media / "attachments"


def test_legacy_data_prefix_strips_under_data_root(monkeypatch, tmp_path: Path):
    # Simulate a Green host with DATA_ROOT=/opt/degen/data and the .env still
    # carrying DEGEN_EYE_V2_INDEX_PATH=data/phash_index.sqlite.
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("DEGEN_EYE_V2_INDEX_PATH", "data/phash_index.sqlite")
    cfg.get_settings.cache_clear()

    from app.inventory.phash_scanner import _configured_index_path

    assert _configured_index_path() == tmp_path / "phash_index.sqlite"


def test_attachment_dirs_use_media_root(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("MEDIA_ROOT", raising=False)
    cfg.get_settings.cache_clear()

    from app import attachment_storage

    cache = attachment_storage.ensure_attachment_cache_dir()
    thumbs = attachment_storage.ensure_thumbnail_cache_dir()
    assert cache == tmp_path / "attachments"
    assert thumbs == tmp_path / "attachments" / "thumbs"
    assert cache.is_dir() and thumbs.is_dir()
