"""SyncOrchestrator component tests for workflow decisions and collaboration order."""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.orchestrator.sync_orchestrator import SyncOrchestrator


def _mapping_result(changed: bool, reason: str):
    return (
        {"active_countries": {"US": {}}, "location_country_map": {"1": {"countries": ["US"]}}},
        changed,
        reason,
    )


def test_smart_full_when_mapping_changed(google_env, no_config_connectivity, tmp_path, sample_variant_us_in_stock):
    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = _mapping_result(True, "hash mismatch")
    sm = MagicMock()
    sm.load_sync_state.return_value = datetime.now(timezone.utc)
    shopify_sync = MagicMock()
    jl = tmp_path / "b.jsonl"
    jl.write_text("", encoding="utf-8")
    shopify_sync.bulk_query.get_products_variants_inventory_bulk.return_value = str(jl)
    pl = MagicMock()
    pl.load_products_from_bulk_jsonl.return_value = [sample_variant_us_in_stock]
    exporter = MagicMock()
    out = tmp_path / "country_feed_US.tsv"
    exporter.create_country_feeds_full.return_value = [out]
    sync_mgr = MagicMock()

    orch = SyncOrchestrator(
        state_manager=sm,
        shopify_sync=shopify_sync,
        product_loader=pl,
        country_mapper=mapper,
        sync_manager=sync_mgr,
        exporter=exporter,
    )
    orch.run_smart()

    exporter.create_country_feeds_full.assert_called_once()
    exporter.update_country_feeds_incremental.assert_not_called()
    sm.reset_variant_states.assert_called_once()
    sync_mgr.upload_files_with_cleanup.assert_called_once()


def test_smart_full_when_no_previous_sync(google_env, no_config_connectivity, tmp_path, sample_variant_us_in_stock):
    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = _mapping_result(False, "unchanged")
    sm = MagicMock()
    sm.load_sync_state.return_value = None
    shopify_sync = MagicMock()
    jl = tmp_path / "b.jsonl"
    jl.write_text("", encoding="utf-8")
    shopify_sync.bulk_query.get_products_variants_inventory_bulk.return_value = str(jl)
    pl = MagicMock()
    pl.load_products_from_bulk_jsonl.return_value = [sample_variant_us_in_stock]
    exporter = MagicMock()
    exporter.create_country_feeds_full.return_value = [tmp_path / "x.tsv"]
    sync_mgr = MagicMock()

    orch = SyncOrchestrator(
        state_manager=sm,
        shopify_sync=shopify_sync,
        product_loader=pl,
        country_mapper=mapper,
        sync_manager=sync_mgr,
        exporter=exporter,
    )
    orch.run_smart()
    exporter.create_country_feeds_full.assert_called_once()


def test_smart_incremental_when_mapping_stable_and_state_exists(
    google_env, no_config_connectivity, tmp_path, sample_variant_us_in_stock
):
    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = _mapping_result(False, "unchanged")
    sm = MagicMock()
    sm.load_sync_state.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sm.detect_stock_changes.return_value = ([], [sample_variant_us_in_stock])
    shopify_sync = MagicMock()
    jl = tmp_path / "b.jsonl"
    jl.write_text("", encoding="utf-8")
    shopify_sync.bulk_query.get_products_variants_inventory_bulk.return_value = str(jl)
    pl = MagicMock()
    pl.load_products_from_bulk_jsonl.return_value = [sample_variant_us_in_stock]
    exporter = MagicMock()
    exporter.update_country_feeds_incremental.return_value = [tmp_path / "x.tsv"]
    sync_mgr = MagicMock()

    orch = SyncOrchestrator(
        state_manager=sm,
        shopify_sync=shopify_sync,
        product_loader=pl,
        country_mapper=mapper,
        sync_manager=sync_mgr,
        exporter=exporter,
    )
    orch.run_smart()

    shopify_sync.bulk_query.get_products_variants_inventory_bulk.assert_called_once()
    call_kw = shopify_sync.bulk_query.get_products_variants_inventory_bulk.call_args[0][0]
    assert call_kw is not None
    exporter.update_country_feeds_incremental.assert_called_once()
    exporter.create_country_feeds_full.assert_not_called()


def test_incremental_command_delegates_when_no_state(google_env, no_config_connectivity, monkeypatch):
    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = _mapping_result(False, "unchanged")
    sm = MagicMock()
    sm.load_sync_state.return_value = None
    called = []

    def fake_smart():
        called.append("smart")

    orch = SyncOrchestrator(
        state_manager=sm,
        shopify_sync=MagicMock(),
        product_loader=MagicMock(),
        country_mapper=mapper,
        sync_manager=MagicMock(),
        exporter=MagicMock(),
    )
    monkeypatch.setattr(orch, "run_smart", fake_smart)
    orch.run_incremental()
    assert called == ["smart"]


def test_refresh_mapping_cache(google_env, no_config_connectivity):
    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = _mapping_result(False, "unchanged")
    orch = SyncOrchestrator(
        country_mapper=mapper,
        shopify_sync=MagicMock(),
        state_manager=MagicMock(),
        product_loader=MagicMock(),
        sync_manager=MagicMock(),
        exporter=MagicMock(),
    )
    orch.refresh_mapping_cache()
    mapper.clear_mapping_hash.assert_called_once()
    mapper.get_mapping_with_change_detection.assert_called_once()


def test_run_full_uses_full_pipeline(google_env, no_config_connectivity, tmp_path, sample_variant_us_in_stock):
    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = _mapping_result(False, "x")
    shopify_sync = MagicMock()
    jl = tmp_path / "b.jsonl"
    jl.write_text("", encoding="utf-8")
    shopify_sync.bulk_query.get_products_variants_inventory_bulk.return_value = str(jl)
    pl = MagicMock()
    pl.load_products_from_bulk_jsonl.return_value = [sample_variant_us_in_stock]
    exporter = MagicMock()
    exporter.create_country_feeds_full.return_value = [tmp_path / "o.tsv"]
    sm = MagicMock()
    sync_mgr = MagicMock()

    orch = SyncOrchestrator(
        state_manager=sm,
        shopify_sync=shopify_sync,
        product_loader=pl,
        country_mapper=mapper,
        sync_manager=sync_mgr,
        exporter=exporter,
    )
    orch.run_full()
    sync_mgr.upload_files_with_cleanup.assert_called_once()
    args, kwargs = sync_mgr.upload_files_with_cleanup.call_args
    assert args[2] == "FULL"


def test_no_exporter_raises_on_smart(google_env, no_config_connectivity):
    orch = SyncOrchestrator(exporter=None)
    with pytest.raises(RuntimeError, match="No exporter"):
        orch.run_smart()


def test_debug_state_logs_stats(google_env, no_config_connectivity, caplog):
    import logging

    caplog.set_level(logging.INFO)
    sm = MagicMock()
    sm.get_stats.return_value = {
        "variant_count": 1,
        "in_stock_count": 1,
        "out_of_stock_count": 0,
        "last_sync": None,
        "db_size_kb": 0,
    }
    mapper = MagicMock()
    mapper.get_mapping_stats.return_value = {"has_previous_hash": False, "last_hash": "None"}
    orch = SyncOrchestrator(
        state_manager=sm,
        country_mapper=mapper,
        shopify_sync=MagicMock(),
        product_loader=MagicMock(),
        sync_manager=MagicMock(),
        exporter=MagicMock(),
    )
    orch.debug_state()
    assert "Debug State" in caplog.text
