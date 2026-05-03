"""
Shared pytest fixtures for the Shopify feed sync project.

Domain-oriented fixtures keep tests readable and aligned with core vocabulary:
mapping, variants, bulk JSONL, and isolated project directories.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest

from core.config import clear_loaded_config, load_config
from core.models import SyncConfig


@pytest.fixture(autouse=True)
def _reset_config_after_test():
    yield
    clear_loaded_config()


@pytest.fixture
def no_config_connectivity(monkeypatch: pytest.MonkeyPatch):
    """Use controlled validation responses for orchestrator tests."""
    monkeypatch.setattr(
        "core.orchestrator.sync_orchestrator.ConfigValidator.validate_all",
        lambda *_a, **_k: [],
    )
    monkeypatch.setattr(
        "core.orchestrator.sync_orchestrator.ConfigValidator.test_connectivity",
        lambda *_a, **_k: [],
    )


@pytest.fixture
def tmp_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """
    Isolated repository root with required env vars for a valid ConfigValidator pass.

    Creates base_dir layout; load_config(base_dir=...) also mkdirs cache/output/temp.
    """
    root = tmp_path / "repo"
    root.mkdir()
    monkeypatch.setenv("SHOPIFY_TOKEN", "x" * 32)
    monkeypatch.setenv("STORE_ID", "test-store")
    monkeypatch.setenv("TARGET_COUNTRIES", "US,CA")
    monkeypatch.setenv("SHOPIFY_API_VERSION", "2024-10")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID_GOOGLE", "")
    monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID_META", "")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    monkeypatch.delenv("META_FILTER_TARGET_COUNTRIES", raising=False)
    return root


@pytest.fixture
def google_env(tmp_project: Path):
    load_config(base_dir=tmp_project, target="google")
    return tmp_project


@pytest.fixture
def meta_env(tmp_project: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("META_FILTER_TARGET_COUNTRIES", raising=False)
    load_config(base_dir=tmp_project, target="meta")
    return tmp_project


@pytest.fixture
def sample_active_countries() -> Dict[str, Dict[str, Any]]:
    return {
        "US": {"name": "United States", "market_id": "gid://shopify/Market/1"},
        "CA": {"name": "Canada", "market_id": "gid://shopify/Market/1"},
    }


@pytest.fixture
def sample_location_country_map() -> Dict[str, Dict[str, Any]]:
    return {
        "55": {"name": "Warehouse", "countries": ["US"]},
        "56": {"name": "Canada DC", "countries": ["CA"]},
    }


@pytest.fixture
def sample_variant_us_in_stock() -> Dict[str, Any]:
    return {
        "id": "111-US",
        "shopify_variant_id": "111",
        "country_code": "US",
        "country_name": "United States",
        "inventory_quantity": 5,
        "sku": "PROD-M",
        "price": "19.99",
        "size": "M",
        "updated_at": "2024-01-01T00:00:00Z",
        "product_id": "222",
        "product_title": "Tee",
        "product_handle": "tee",
        "product_description": "A shirt",
        "featured_image": "https://example.com/x.jpg",
    }


@pytest.fixture
def sample_variant_us_out_of_stock(sample_variant_us_in_stock) -> Dict[str, Any]:
    v = dict(sample_variant_us_in_stock)
    v["inventory_quantity"] = 0
    return v


def write_bulk_jsonl(path: Path, lines: List[Dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for obj in lines:
            f.write(json.dumps(obj) + "\n")
    return str(path)


@pytest.fixture
def minimal_bulk_jsonl_path(tmp_path: Path, sample_active_countries, sample_location_country_map) -> str:
    """JSONL matching ProductLoader._process_bulk_record expectations."""
    variant_gid = "gid://shopify/ProductVariant/111"
    product_gid = "gid://shopify/Product/222"
    lines = [
        {
            "id": variant_gid,
            "product": {
                "id": product_gid,
                "title": "Tee",
                "handle": "tee",
                "description": "<p>Hello</p>",
                "featuredImage": {"url": "https://example.com/i.jpg"},
            },
            "inventoryItem": {"sku": "PROD-M"},
            "price": "19.99",
            "updatedAt": "2024-01-01T00:00:00Z",
        },
        {
            "__parentId": variant_gid,
            "location": {"id": "gid://shopify/Location/55"},
            "quantities": [{"name": "available", "quantity": 3}],
        },
    ]
    return write_bulk_jsonl(tmp_path / "bulk.jsonl", lines)


def make_sync_config(
    base: Path,
    *,
    target: str = "google",
    token: str = "x" * 32,
    store_id: str = "validstore",
    target_countries: Set[str] | None = None,
    restrict: bool = True,
    drive_folder: str = "",
    drive_file: str = "",
    bulk_chunk_size: int = 1000,
    max_retries: int = 3,
    base_retry_delay: float = 2.0,
) -> SyncConfig:
    """Build a SyncConfig for direct ConfigValidator.validate_all(config=...) calls."""
    base.mkdir(parents=True, exist_ok=True)
    cache = base / "cache" / target
    out = base / "out"
    temp = base / "temp" / target
    for d in (cache, out, temp):
        d.mkdir(parents=True, exist_ok=True)
    tc = target_countries if target_countries is not None else {"US"}
    return SyncConfig(
        target=target,
        base_dir=base,
        cache_dir=cache,
        output_dir=out,
        temp_dir=temp,
        state_json_path=cache / "sync_state.json",
        variant_state_json_path=cache / "variant_states.json",
        mapping_comparison_file=cache / "mapping_comparison.json",
        shopify_token=token,
        store_id=store_id,
        shopify_api_version="2024-07",
        shopify_session_config={
            "shop_url": f"https://{store_id}.myshopify.com",
            "api_version": "2024-07",
            "access_token": token,
        },
        google_drive_folder_id=drive_folder,
        google_service_account_file=drive_file,
        target_countries=tc,
        bulk_chunk_size=bulk_chunk_size,
        max_retries=max_retries,
        base_retry_delay=base_retry_delay,
        csv_buffer_size=65536,
        smart_mapping_enabled=True,
        enable_data_validation=True,
        restrict_markets_to_target_countries=restrict,
        feed_prefix="country_feed_",
        feed_extension=".tsv",
        feed_id_prefix="shopify_US_",
    )
