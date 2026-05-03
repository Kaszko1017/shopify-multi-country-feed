"""CountryLocationMapper parsing and change-detection behavior."""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from core.config import get_config, load_config
from core.mapping.country_location_mapper import CountryLocationMapper


def _markets_graph_with_codes(codes):
    edges = [
        {"node": {"code": c, "name": c}} for c in codes
    ]
    return {
        "markets": {
            "edges": [
                {
                    "node": {
                        "id": "gid://shopify/Market/1",
                        "regions": {"edges": edges},
                    }
                }
            ]
        }
    }


def test_parse_active_countries_filters_to_target_when_restricted(google_env):
    """Google mapping uses the configured target-country allow-list."""
    mapper = CountryLocationMapper(shopify_sync=None)
    data = _markets_graph_with_codes(["US", "DE"])
    out = mapper._parse_active_countries(data)
    assert set(out.keys()) == {"US"}


def test_parse_active_countries_includes_all_when_unrestricted(meta_env):
    """Meta mapping includes every active market region unless filtering is enabled."""
    mapper = CountryLocationMapper(shopify_sync=None)
    data = _markets_graph_with_codes(["US", "DE"])
    out = mapper._parse_active_countries(data)
    assert set(out.keys()) == {"US", "DE"}


def test_parse_active_countries_skips_region_without_code_or_name(google_env):
    mapper = CountryLocationMapper(shopify_sync=None)
    data = {
        "markets": {
            "edges": [
                {
                    "node": {
                        "id": "gid://shopify/Market/1",
                        "regions": {
                            "edges": [
                                {"node": {"code": "US", "name": "United States"}},
                                {"node": {"code": "", "name": "Bad"}},
                                {"node": {"code": "CA", "name": ""}},
                            ]
                        },
                    }
                }
            ]
        }
    }
    out = mapper._parse_active_countries(data)
    assert set(out.keys()) == {"US"}


def test_get_mapping_without_shopify_raises():
    with pytest.raises(ValueError, match="ShopifySync instance required"):
        CountryLocationMapper(shopify_sync=None).get_mapping_with_change_detection()


def test_smart_mapping_disabled_returns_cached_payload(google_env, monkeypatch):
    monkeypatch.setenv("SMART_MAPPING_ENABLED", "false")
    load_config(base_dir=google_env, target="google")
    cfg = get_config()
    cached = {
        "active_countries": {"US": {"name": "U", "market_id": "m"}},
        "location_country_map": {"1": {"name": "L", "countries": ["US"]}},
        "mapping_hash": "abc",
        "method": "cached",
    }
    mapping_file = cfg.cache_dir / "country-mapping.json"
    mapping_file.write_text(json.dumps(cached), encoding="utf-8")

    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    data, changed, reason = mapper.get_mapping_with_change_detection()
    assert changed is False
    assert reason == "Smart mapping disabled"
    assert data["mapping_hash"] == "abc"


def test_parse_locations_jsonl_skips_non_location_records(tmp_path, google_env):
    p = tmp_path / "loc.jsonl"
    lines = [
        {"id": "gid://shopify/Product/1", "name": "ignore"},
        {
            "id": "gid://shopify/Location/99",
            "name": "DC",
            "address": {"countryCode": "US", "city": "Austin", "province": "TX"},
            "isActive": True,
        },
    ]
    p.write_text("\n".join(json.dumps(x) for x in lines), encoding="utf-8")
    mapper = CountryLocationMapper(shopify_sync=None)
    out = mapper._parse_locations_from_jsonl(str(p))
    assert list(out.keys()) == ["99"]
    assert out["99"]["country_code"] == "US"


def test_create_country_mapping_skips_inactive_location(google_env):
    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    mapper.shopify_sync.bulk_query.get_location_country_relationships.return_value = {}
    locations = {
        "1": {"name": "Off", "country_code": "US", "is_active": False},
    }
    active = {"US": {"name": "United States", "market_id": "m"}}
    assert mapper._create_country_mapping(locations, active) == {}


def test_create_country_mapping_uses_delivery_when_present(google_env):
    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    mapper.shopify_sync.bulk_query.get_location_country_relationships.return_value = {
        "10": ["US", "XX"],
    }
    locations = {
        "10": {"name": "Hub", "country_code": "CA", "is_active": True},
    }
    active = {"US": {"name": "United States", "market_id": "m"}}
    out = mapper._create_country_mapping(locations, active)
    assert out["10"]["countries"] == ["US"]


def test_create_country_mapping_falls_back_when_delivery_raises(google_env):
    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    mapper.shopify_sync.bulk_query.get_location_country_relationships.side_effect = RuntimeError("api down")
    locations = {
        "20": {"name": "Local", "country_code": "US", "is_active": True},
    }
    active = {"US": {"name": "United States", "market_id": "m"}}
    out = mapper._create_country_mapping(locations, active)
    assert out["20"]["countries"] == ["US"]


def test_create_country_mapping_falls_back_when_delivery_empty_for_location(google_env):
    """Delivery lists only countries outside active set → use location address country."""
    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    mapper.shopify_sync.bulk_query.get_location_country_relationships.return_value = {"30": ["DE"]}
    locations = {
        "30": {"name": "US wh", "country_code": "US", "is_active": True},
    }
    active = {"US": {"name": "United States", "market_id": "m"}}
    out = mapper._create_country_mapping(locations, active)
    assert out["30"]["countries"] == ["US"]


def test_get_mapping_stats_and_clear_mapping_hash(google_env):
    cfg = get_config()
    cfg.mapping_comparison_file.write_text(json.dumps({"hash": "deadbeefcafe"}), encoding="utf-8")
    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    stats = mapper.get_mapping_stats()
    assert stats["has_previous_hash"] is True
    assert stats["last_hash"].startswith("deadbeefcafe"[:12])

    mapper.clear_mapping_hash()
    assert not cfg.mapping_comparison_file.exists()
    assert mapper.get_mapping_stats()["has_previous_hash"] is False


def test_load_previous_mapping_hash_corrupt_returns_none(google_env):
    cfg = get_config()
    cfg.mapping_comparison_file.write_text("{not json", encoding="utf-8")
    mapper = CountryLocationMapper(shopify_sync=MagicMock())
    assert mapper._load_previous_mapping_hash() is None


@pytest.fixture
def mapping_shopify_mock(tmp_path, google_env):
    """ShopifySync substitute that supports fresh mapping construction."""
    markets = {
        "data": {
            "markets": {
                "edges": [
                    {
                        "node": {
                            "id": "gid://shopify/Market/1",
                            "regions": {
                                "edges": [{"node": {"code": "US", "name": "United States"}}]
                            },
                        }
                    }
                ]
            }
        }
    }
    jl = tmp_path / "locs.jsonl"
    jl.write_text(
        json.dumps(
            {
                "id": "gid://shopify/Location/55",
                "name": "WH",
                "address": {"countryCode": "US"},
                "isActive": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    bulk = MagicMock()
    bulk.get_markets_and_countries.return_value = markets
    bulk.get_locations.return_value = str(jl)
    bulk.get_location_country_relationships.return_value = {}
    sync = MagicMock()
    sync.bulk_query = bulk
    return sync


def test_get_mapping_change_detection_first_run_reason(mapping_shopify_mock, google_env, monkeypatch):
    monkeypatch.setattr(
        "core.mapping.country_location_mapper.unlink_bulk_jsonl",
        lambda _p: None,
    )
    mapper = CountryLocationMapper(shopify_sync=mapping_shopify_mock)
    _data, changed, reason = mapper.get_mapping_with_change_detection()
    assert changed is True
    assert "first run" in reason.lower()


def test_get_mapping_change_detection_unchanged_when_hash_matches(
    mapping_shopify_mock, google_env, monkeypatch
):
    monkeypatch.setattr(
        "core.mapping.country_location_mapper.unlink_bulk_jsonl",
        lambda _p: None,
    )
    mapper = CountryLocationMapper(shopify_sync=mapping_shopify_mock)
    data1, changed1, _ = mapper.get_mapping_with_change_detection()
    assert changed1 is True
    current_hash = data1["mapping_hash"]

    mapper2 = CountryLocationMapper(shopify_sync=mapping_shopify_mock)
    _data2, changed2, reason2 = mapper2.get_mapping_with_change_detection()
    assert changed2 is False
    assert "unchanged" in reason2.lower()
    assert _data2["mapping_hash"] == current_hash


def test_mapping_hash_is_isolated_per_target(tmp_project):
    load_config(base_dir=tmp_project, target="google")
    google_mapper = CountryLocationMapper(shopify_sync=MagicMock())
    google_mapper._save_mapping_hash("googlehash")
    assert google_mapper._load_previous_mapping_hash() == "googlehash"

    load_config(base_dir=tmp_project, target="meta")
    meta_mapper = CountryLocationMapper(shopify_sync=MagicMock())
    assert meta_mapper._load_previous_mapping_hash() is None
