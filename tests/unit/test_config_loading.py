"""Unit tests for core.config.load_config (runtime SyncConfig construction)."""
import pytest

from core.config import clear_loaded_config, get_config, load_config


def test_importing_config_module_does_not_load_config():
    clear_loaded_config()
    import core.config.settings as settings  # noqa: F401

    assert settings._loaded_config is None  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="Configuration not loaded"):
        get_config()


def test_google_target_paths_and_defaults(tmp_project, monkeypatch):
    monkeypatch.delenv("FEED_ID_PREFIX", raising=False)
    monkeypatch.delenv("META_FILTER_TARGET_COUNTRIES", raising=False)
    cfg = load_config(base_dir=tmp_project, target="google")

    assert cfg.feed_extension == ".tsv"
    assert cfg.feed_id_prefix == "shopify_US_"
    assert "Google Merchant" in str(cfg.output_dir)
    assert cfg.target == "google"
    assert cfg.cache_dir == tmp_project / "cache" / "google"
    assert cfg.temp_dir == tmp_project / "temp" / "google"
    assert cfg.state_json_path == tmp_project / "cache" / "google" / "sync_state.json"
    assert cfg.variant_state_json_path == tmp_project / "cache" / "google" / "variant_states.json"
    assert cfg.mapping_comparison_file == tmp_project / "cache" / "google" / "mapping_comparison.json"
    assert cfg.restrict_markets_to_target_countries is True
    assert cfg.target_countries == {"US", "CA"}
    assert cfg.shopify_api_version == "2024-10"


def test_meta_target_paths_and_filter_default(tmp_project, monkeypatch):
    monkeypatch.delenv("META_FILTER_TARGET_COUNTRIES", raising=False)
    cfg = load_config(base_dir=tmp_project, target="meta")

    assert cfg.feed_extension == ".csv"
    # load_config normalizes empty prefix to the same fallback as Google for downstream safety
    assert cfg.feed_id_prefix == "shopify_US_"
    assert "Meta catalog" in str(cfg.output_dir)
    assert cfg.target == "meta"
    assert cfg.cache_dir == tmp_project / "cache" / "meta"
    assert cfg.temp_dir == tmp_project / "temp" / "meta"
    assert cfg.state_json_path == tmp_project / "cache" / "meta" / "sync_state.json"
    assert cfg.variant_state_json_path == tmp_project / "cache" / "meta" / "variant_states.json"
    assert cfg.mapping_comparison_file == tmp_project / "cache" / "meta" / "mapping_comparison.json"
    assert cfg.restrict_markets_to_target_countries is False


def test_meta_filter_target_countries_enables_restrict(tmp_project, monkeypatch):
    monkeypatch.setenv("META_FILTER_TARGET_COUNTRIES", "true")
    cfg = load_config(base_dir=tmp_project, target="meta")
    assert cfg.restrict_markets_to_target_countries is True


def test_shopify_api_version_from_env(tmp_project, monkeypatch):
    monkeypatch.setenv("SHOPIFY_API_VERSION", "2025-01")
    cfg = load_config(base_dir=tmp_project, target="google")
    assert cfg.shopify_api_version == "2025-01"
    assert cfg.shopify_session_config["api_version"] == "2025-01"


def test_optional_drive_empty_does_not_break_load(tmp_project, monkeypatch):
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID_GOOGLE", "")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID_META", "")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    cfg = load_config(base_dir=tmp_project, target="google")
    assert cfg.google_drive_folder_id == ""
    assert cfg.google_service_account_file == ""


def test_feed_id_prefix_from_env_google(tmp_project, monkeypatch):
    monkeypatch.setenv("FEED_ID_PREFIX", "custom_")
    cfg = load_config(base_dir=tmp_project, target="google")
    assert cfg.feed_id_prefix == "custom_"


def test_numeric_env_defaults(tmp_project, monkeypatch):
    monkeypatch.setenv("BULK_CHUNK_SIZE", "500")
    monkeypatch.setenv("MAX_RETRIES", "5")
    monkeypatch.setenv("BASE_RETRY_DELAY", "3.5")
    cfg = load_config(base_dir=tmp_project, target="google")
    assert cfg.bulk_chunk_size == 500
    assert cfg.max_retries == 5
    assert cfg.base_retry_delay == 3.5


def test_get_config_returns_last_load(tmp_project):
    load_config(base_dir=tmp_project, target="google")
    assert get_config().feed_extension == ".tsv"
    load_config(base_dir=tmp_project, target="meta")
    assert get_config().feed_extension == ".csv"


def test_target_specific_drive_folder_id_selected(tmp_project, monkeypatch):
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID_GOOGLE", "google-folder-id")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID_META", "meta-folder-id")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "legacy-shared-id")

    gcfg = load_config(base_dir=tmp_project, target="google")
    assert gcfg.google_drive_folder_id == "google-folder-id"

    mcfg = load_config(base_dir=tmp_project, target="meta")
    assert mcfg.google_drive_folder_id == "meta-folder-id"


def test_legacy_drive_folder_id_fallback_when_target_specific_missing(tmp_project, monkeypatch):
    monkeypatch.delenv("GOOGLE_DRIVE_FOLDER_ID_GOOGLE", raising=False)
    monkeypatch.delenv("GOOGLE_DRIVE_FOLDER_ID_META", raising=False)
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "legacy-shared-id")

    gcfg = load_config(base_dir=tmp_project, target="google")
    mcfg = load_config(base_dir=tmp_project, target="meta")
    assert gcfg.google_drive_folder_id == "legacy-shared-id"
    assert mcfg.google_drive_folder_id == "legacy-shared-id"
