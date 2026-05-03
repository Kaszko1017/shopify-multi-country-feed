"""Tests for StateManager JSON persistence and stock change detection."""
import json
from datetime import datetime, timezone

import pytest

from core.config import load_config
from core.state.state_manager import StateManager


def test_load_sync_state_missing_returns_none(google_env):
    sm = StateManager()
    assert sm.load_sync_state() is None


def test_save_and_load_sync_state_roundtrip(google_env):
    sm = StateManager()
    ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    sm.save_sync_state(ts)
    loaded = sm.load_sync_state()
    assert loaded is not None
    assert loaded.replace(microsecond=0) == ts.replace(microsecond=0)


def test_corrupted_sync_state_returns_none(google_env):
    sm = StateManager()
    sm.sync_state_file.write_text("not json {{{", encoding="utf-8")
    assert sm.load_sync_state() is None


def test_variant_states_roundtrip(google_env, sample_variant_us_in_stock):
    sm = StateManager()
    sm.update_variant_states([sample_variant_us_in_stock])
    assert sm.variant_states_file.exists()
    data = json.loads(sm.variant_states_file.read_text(encoding="utf-8"))
    assert data["111-US"]["stock_status"] == "in stock"


def test_corrupted_variant_states_treated_as_empty(google_env):
    sm = StateManager()
    sm.variant_states_file.write_text("{broken", encoding="utf-8")
    assert sm._load_variant_states() == {}


def test_detect_new_variant(google_env, sample_variant_us_in_stock):
    sm = StateManager()
    new, changed = sm.detect_stock_changes([sample_variant_us_in_stock])
    assert len(new) == 1 and changed == []


def test_detect_unchanged_stock_not_flagged(google_env, sample_variant_us_in_stock):
    sm = StateManager()
    sm.update_variant_states([sample_variant_us_in_stock])
    new, changed = sm.detect_stock_changes([sample_variant_us_in_stock])
    assert new == [] and changed == []


def test_detect_quantity_change_flags_changed(google_env, sample_variant_us_in_stock):
    sm = StateManager()
    sm.update_variant_states([sample_variant_us_in_stock])
    v2 = dict(sample_variant_us_in_stock)
    v2["inventory_quantity"] = 0
    new, changed = sm.detect_stock_changes([v2])
    assert new == [] and len(changed) == 1


def test_reset_variant_states_removes_file(google_env, sample_variant_us_in_stock):
    sm = StateManager()
    sm.update_variant_states([sample_variant_us_in_stock])
    sm.reset_variant_states()
    assert not sm.variant_states_file.exists()


def test_update_variant_states_empty_noop(google_env):
    sm = StateManager()
    sm.update_variant_states([])
    assert not sm.variant_states_file.exists()


def test_state_files_are_isolated_per_target(tmp_project, sample_variant_us_in_stock):
    load_config(base_dir=tmp_project, target="google")
    google_sm = StateManager()
    ts_google = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    google_sm.save_sync_state(ts_google)
    google_sm.update_variant_states([sample_variant_us_in_stock])

    load_config(base_dir=tmp_project, target="meta")
    meta_sm = StateManager()

    assert meta_sm.load_sync_state() is None
    assert meta_sm._load_variant_states() == {}
    assert google_sm.sync_state_file != meta_sm.sync_state_file
    assert google_sm.variant_states_file != meta_sm.variant_states_file
