"""Google full-sync integration test for loader, exporter, and state collaboration."""
import json
from pathlib import Path
from unittest.mock import MagicMock

from core.orchestrator.sync_orchestrator import SyncOrchestrator
from core.shopify.product_loader import ProductLoader
from core.state.state_manager import StateManager
from targets.google import TSVExporter


def _bulk_lines(qty: int):
    variant_gid = "gid://shopify/ProductVariant/111"
    product_gid = "gid://shopify/Product/222"
    return [
        {
            "id": variant_gid,
            "product": {
                "id": product_gid,
                "title": "Tee",
                "handle": "tee",
                "description": "",
                "featuredImage": None,
            },
            "inventoryItem": {"sku": "PROD-M"},
            "price": "19.99",
            "updatedAt": "",
        },
        {
            "__parentId": variant_gid,
            "location": {"id": "gid://shopify/Location/55"},
            "quantities": [{"name": "available", "quantity": qty}],
        },
    ]


def test_full_google_sync_writes_feed_and_state(
    google_env,
    no_config_connectivity,
    tmp_path,
    sample_active_countries,
    sample_location_country_map,
):
    jl = tmp_path / "bulk.jsonl"
    with open(jl, "w", encoding="utf-8") as f:
        for o in _bulk_lines(2):
            f.write(json.dumps(o) + "\n")

    mapper = MagicMock()
    mapper.get_mapping_with_change_detection.return_value = (
        {
            "active_countries": sample_active_countries,
            "location_country_map": sample_location_country_map,
        },
        True,
        "integration",
    )
    shopify_sync = MagicMock()
    shopify_sync.bulk_query.get_products_variants_inventory_bulk.return_value = str(jl)

    orch = SyncOrchestrator(
        state_manager=StateManager(),
        shopify_sync=shopify_sync,
        product_loader=ProductLoader(),
        country_mapper=mapper,
        sync_manager=MagicMock(),
        exporter=TSVExporter(),
    )
    orch.run_full()

    feed = google_env / "Google Merchant - country feed updates" / "country_feed_US.tsv"
    assert feed.exists()
    cache_google = google_env / "cache" / "google"
    assert (cache_google / "sync_state.json").exists()
    assert (cache_google / "variant_states.json").exists()
    text = feed.read_text(encoding="utf-8")
    assert "id\tavailability" in text.splitlines()[0]
