"""Unit tests for ConfigValidator.validate_all (contract: list of error strings)."""
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.orchestrator.config_validator import ConfigValidator
from tests.conftest import make_sync_config


def test_valid_config_returns_no_errors(tmp_path):
    cfg = make_sync_config(tmp_path / "p")
    assert ConfigValidator.validate_all(cfg) == []


def test_missing_token_rejected(tmp_path):
    cfg = make_sync_config(tmp_path / "p", token="")
    errs = ConfigValidator.validate_all(cfg)
    assert any("SHOPIFY_TOKEN" in e for e in errs)


def test_short_token_rejected(tmp_path):
    cfg = make_sync_config(tmp_path / "p", token="short")
    errs = ConfigValidator.validate_all(cfg)
    assert any("SHOPIFY_TOKEN" in e and "short" in e.lower() for e in errs)


def test_missing_store_id_rejected(tmp_path):
    cfg = make_sync_config(tmp_path / "p", store_id="")
    errs = ConfigValidator.validate_all(cfg)
    assert any("STORE_ID" in e for e in errs)


def test_store_id_invalid_characters(tmp_path):
    cfg = make_sync_config(tmp_path / "p", store_id="bad id!")
    errs = ConfigValidator.validate_all(cfg)
    assert any("STORE_ID" in e and "invalid" in e.lower() for e in errs)


def test_target_countries_required_when_restricted(tmp_path):
    cfg = make_sync_config(tmp_path / "p", target_countries=set(), restrict=True)
    errs = ConfigValidator.validate_all(cfg)
    assert any("TARGET_COUNTRIES" in e for e in errs)


def test_meta_unrestricted_empty_target_countries_ok(tmp_path):
    cfg = make_sync_config(tmp_path / "p", target_countries=set(), restrict=False)
    errs = ConfigValidator.validate_all(cfg)
    assert not any("TARGET_COUNTRIES must be set" in e for e in errs)


def test_target_countries_count_limit_when_restricted(tmp_path):
    codes = {f"{i:02d}" for i in range(51)}  # invalid as ISO but exercises length rule
    cfg = make_sync_config(tmp_path / "p", target_countries=codes, restrict=True)
    errs = ConfigValidator.validate_all(cfg)
    assert any("TARGET_COUNTRIES" in e and "limited" in e.lower() for e in errs)


def test_drive_partial_config_rejected(tmp_path):
    cfg = make_sync_config(tmp_path / "p", drive_folder="folderid12345", drive_file="")
    errs = ConfigValidator.validate_all(cfg)
    assert any("GOOGLE_SERVICE_ACCOUNT_FILE" in e for e in errs)

    cfg2 = make_sync_config(tmp_path / "p2", drive_folder="", drive_file="/path/sa.json")
    errs2 = ConfigValidator.validate_all(cfg2)
    assert any("GOOGLE_DRIVE_FOLDER_ID" in e for e in errs2)


def test_drive_folder_id_too_short(tmp_path):
    cfg = make_sync_config(
        tmp_path / "p",
        drive_folder="short",
        drive_file=str(tmp_path / "sa.json"),
    )
    (tmp_path / "sa.json").write_text("{}")
    errs = ConfigValidator.validate_all(cfg)
    assert any("GOOGLE_DRIVE_FOLDER_ID" in e for e in errs)


def test_bulk_chunk_and_retry_bounds(tmp_path):
    cfg = make_sync_config(tmp_path / "p", bulk_chunk_size=50)
    assert any("BULK_CHUNK_SIZE" in e for e in ConfigValidator.validate_all(cfg))

    cfg2 = make_sync_config(tmp_path / "p2", max_retries=20)
    assert any("MAX_RETRIES" in e for e in ConfigValidator.validate_all(cfg2))

    cfg3 = make_sync_config(tmp_path / "p3", base_retry_delay=0)
    assert any("BASE_RETRY_DELAY" in e for e in ConfigValidator.validate_all(cfg3))


def test_non_directory_output_rejected(tmp_path):
    cfg = make_sync_config(tmp_path / "p")
    cfg.output_dir.rmdir()
    cfg.output_dir.write_text("notadir")
    errs = ConfigValidator.validate_all(cfg)
    assert any("OUTPUT_DIR" in e and "directory" in e.lower() for e in errs)


def test_validate_all_uses_get_config_when_omitted(google_env):
    """Contract: validate_all() with no args uses loaded runtime config."""
    assert ConfigValidator.validate_all() == []


def test_validate_all_cache_mkdir_failure(tmp_path, monkeypatch):
    cfg = make_sync_config(tmp_path / "p")
    cfg.cache_dir.rmdir()
    orig_mkdir = Path.mkdir

    def mkdir_wrapper(self, *args, **kwargs):
        if hasattr(self, "resolve") and self.resolve() == cfg.cache_dir.resolve():
            raise PermissionError("denied")
        return orig_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir_wrapper)
    errs = ConfigValidator.validate_all(cfg)
    assert any("CACHE_DIR" in e and "denied" in e for e in errs)


def _patch_shopify_session_ok(monkeypatch):
    import core.orchestrator.config_validator as cv

    fake_session = MagicMock()
    SessionCls = MagicMock(return_value=fake_session)
    SessionCls.setup = MagicMock()
    monkeypatch.setattr(cv.shopify, "Session", SessionCls)
    monkeypatch.setattr(cv.shopify.ShopifyResource, "activate_session", MagicMock())


def test_connectivity_shopify_verified_when_shop_present(google_env, monkeypatch):
    import core.orchestrator.config_validator as cv

    _patch_shopify_session_ok(monkeypatch)
    monkeypatch.setattr(cv.shopify.Shop, "current", MagicMock(return_value=MagicMock()))
    assert ConfigValidator.test_connectivity() == []


def test_connectivity_shopify_fails_when_shop_missing(google_env, monkeypatch):
    import core.orchestrator.config_validator as cv

    _patch_shopify_session_ok(monkeypatch)
    monkeypatch.setattr(cv.shopify.Shop, "current", MagicMock(return_value=None))
    errs = ConfigValidator.test_connectivity()
    assert any("authentication failed" in e.lower() for e in errs)


def test_connectivity_shopify_surfaces_exceptions(google_env, monkeypatch):
    import core.orchestrator.config_validator as cv

    _patch_shopify_session_ok(monkeypatch)
    monkeypatch.setattr(
        cv.shopify.Shop,
        "current",
        MagicMock(side_effect=RuntimeError("boom")),
    )
    errs = ConfigValidator.test_connectivity()
    assert len(errs) == 1
    assert "Shopify connectivity failed" in errs[0]
    assert "boom" in errs[0]


def test_connectivity_drive_fails_when_service_missing(google_env, monkeypatch, tmp_path):
    import core.orchestrator.config_validator as cv

    _patch_shopify_session_ok(monkeypatch)
    monkeypatch.setattr(cv.shopify.Shop, "current", MagicMock(return_value=MagicMock()))
    sa = tmp_path / "sa.json"
    sa.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "folderid12345")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", str(sa))
    from core.config import load_config

    load_config(base_dir=google_env, target="google")
    monkeypatch.setattr(
        "core.output.drive_sync.SyncManager",
        MagicMock(return_value=MagicMock(service=None)),
    )
    errs = ConfigValidator.test_connectivity()
    assert any("Google Drive authentication failed" in e for e in errs)
