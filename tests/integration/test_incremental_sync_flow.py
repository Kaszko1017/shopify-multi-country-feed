"""Incremental sync integration test for adding a newly available variant."""
import json
from unittest.mock import MagicMock

from core.orchestrator.sync_orchestrator import SyncOrchestrator
from core.shopify.product_loader import ProductLoader
from core.state.state_manager import StateManager
from targets.google import TSVExporter


def _write_bulk_one_variant(path, vid: str, pid: str, sku: str, qty: int):
    variant_gid = f"gid://shopify/ProductVariant/{vid}"
    product_gid = f"gid://shopify/Product/{pid}"
    lines = [
        {
            "id": variant_gid,
            "product": {
                "id": product_gid,
                "title": "Tee",
                "handle": "tee",
                "description": "",
                "featuredImage": None,
            },
            "inventoryItem": {"sku": sku},
            "price": "19.99",
            "updatedAt": "",
        },
        {
            "__parentId": variant_gid,
            "location": {"id": "gid://shopify/Location/55"},
            "quantities": [{"name": "available", "quantity": qty}],
        },
    ]
    with open(path, "w", encoding="utf-8") as f:
        for o in lines:
            f.write(json.dumps(o) + "\n")


def _write_bulk_two_variants(path):
    """Two variants sharing the same mapped location (US)."""
    lines = []
    for vid, pid, sku, qty in (
        ("111", "222", "PROD-M", 2),
        ("333", "444", "PROD-L", 1),
    ):
        variant_gid = f"gid://shopify/ProductVariant/{vid}"
        product_gid = f"gid://shopify/Product/{pid}"
        lines.extend(
            [
                {
                    "id": variant_gid,
                    "product": {
                        "id": product_gid,
                        "title": "Tee",
                        "handle": "tee",
                        "description": "",
                        "featuredImage": None,
                    },
                    "inventoryItem": {"sku": sku},
                    "price": "19.99",
                    "updatedAt": "",
                },
                {
                    "__parentId": variant_gid,
                    "location": {"id": "gid://shopify/Location/55"},
                    "quantities": [{"name": "available", "quantity": qty}],
                },
            ]
        )
    with open(path, "w", encoding="utf-8") as f:
        for o in lines:
            f.write(json.dumps(o) + "\n")


def test_incremental_adds_new_variant_after_full(
    google_env,
    no_config_connectivity,
    tmp_path,
    sample_active_countries,
    sample_location_country_map,
):
    """
    First bulk export contains one variant; the second contains the original variant
    plus a new in-stock variant. The feed should be updated with the new composite ID.
    """
    jl1 = tmp_path / "b1.jsonl"
    jl2 = tmp_path / "b2.jsonl"
    _write_bulk_one_variant(jl1, "111", "222", "PROD-M", 5)
    _write_bulk_two_variants(jl2)

    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = (
        {
            "active_countries": sample_active_countries,
            "location_country_map": sample_location_country_map,
        },
        False,
        "unchanged",
    )
    shopify_sync = MagicMock()
    paths_iter = iter([str(jl1), str(jl2)])

    def next_bulk(_since=None):
        return next(paths_iter)

    shopify_sync.bulk_query.get_products_variants_inventory_bulk.side_effect = next_bulk

    sm = StateManager()
    orch = SyncOrchestrator(
        state_manager=sm,
        shopify_sync=shopify_sync,
        product_loader=ProductLoader(),
        country_mapper=mapper,
        sync_manager=MagicMock(),
        exporter=TSVExporter(),
    )

    orch.run_full()
    feed = google_env / "Google Merchant - country feed updates" / "country_feed_US.tsv"
    first = feed.read_text(encoding="utf-8")
    assert first.count("shopify_US_") == 1

    orch.run_smart()
    second = feed.read_text(encoding="utf-8")
    assert second.count("shopify_US_") == 2
