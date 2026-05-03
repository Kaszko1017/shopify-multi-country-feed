"""ShopifyBulkQuery wrapper tests with external API calls controlled by test doubles."""
from unittest.mock import MagicMock

import pytest

from core.shopify.shopify_bulk_query import ShopifyBulkQuery


@pytest.fixture
def bulk_query_with_sync(monkeypatch, google_env):
    """Provide a configured ShopifySync substitute for query wrapper tests."""
    sync = MagicMock()
    bq = ShopifyBulkQuery(shopify_sync=sync)
    bq.bulk_handler = MagicMock()
    return bq


def test_execute_query_returns_parsed_dict(bulk_query_with_sync, monkeypatch):
    raw = '{"data": {"markets": {"edges": []}}}'
    monkeypatch.setattr(
        "core.shopify.shopify_bulk_query.shopify.GraphQL",
        lambda: MagicMock(execute=lambda q: raw),
    )
    out = bulk_query_with_sync.execute_query("{ x }")
    assert out == {"data": {"markets": {"edges": []}}}


def test_execute_query_propagates_shopify_errors(bulk_query_with_sync, monkeypatch):
    monkeypatch.setattr(
        "core.shopify.shopify_bulk_query.shopify.GraphQL",
        lambda: MagicMock(execute=MagicMock(side_effect=RuntimeError("network"))),
    )
    with pytest.raises(RuntimeError, match="network"):
        bulk_query_with_sync.execute_query("{ x }")


def test_get_products_bulk_includes_incremental_filter_when_since_set(bulk_query_with_sync, monkeypatch):
    captured = {}

    def capture(q: str):
        captured["q"] = q
        return "/tmp/fake.jsonl"

    bulk_query_with_sync.bulk_handler.execute_bulk_query.side_effect = capture
    path = bulk_query_with_sync.get_products_variants_inventory_bulk("2024-01-01T00:00:00Z")
    assert path == "/tmp/fake.jsonl"
    assert "updated_at:>2024-01-01T00:00:00Z" in captured["q"]


def test_get_products_bulk_all_active_when_no_since(bulk_query_with_sync):
    captured = {}

    def capture(q: str):
        captured["q"] = q
        return None

    bulk_query_with_sync.bulk_handler.execute_bulk_query.side_effect = capture
    bulk_query_with_sync.get_products_variants_inventory_bulk(None)
    assert "product_status:active" in captured["q"]
    assert "updated_at:>" not in captured["q"]


def test_get_location_country_relationships_graphql_errors_return_empty(bulk_query_with_sync, monkeypatch):
    monkeypatch.setattr(
        bulk_query_with_sync,
        "execute_query",
        lambda _q: {"errors": [{"message": "bad"}]},
    )
    assert bulk_query_with_sync.get_location_country_relationships() == {}


def test_get_location_country_relationships_malformed_response_return_empty(bulk_query_with_sync, monkeypatch):
    monkeypatch.setattr(
        bulk_query_with_sync,
        "execute_query",
        lambda _q: {"data": {}},
    )
    assert bulk_query_with_sync.get_location_country_relationships() == {}


def test_get_location_country_relationships_builds_location_to_countries(bulk_query_with_sync, monkeypatch):
    payload = {
        "data": {
            "deliveryProfiles": {
                "edges": [
                    {
                        "node": {
                            "profileLocationGroups": [
                                {
                                    "locationGroup": {
                                        "locations": {
                                            "edges": [
                                                {"node": {"id": "gid://shopify/Location/55"}},
                                            ]
                                        }
                                    },
                                    "locationGroupZones": {
                                        "edges": [
                                            {
                                                "node": {
                                                    "zone": {
                                                        "countries": [
                                                            {"code": {"countryCode": "US"}},
                                                            {"code": {"countryCode": "CA"}},
                                                        ]
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
    monkeypatch.setattr(bulk_query_with_sync, "execute_query", lambda _q: payload)
    rel = bulk_query_with_sync.get_location_country_relationships()
    assert set(rel["55"]) == {"US", "CA"}
