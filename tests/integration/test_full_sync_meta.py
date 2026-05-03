"""Meta full-sync integration test for CSV output and target-scoped state."""
import json
from unittest.mock import MagicMock

from core.orchestrator.sync_orchestrator import SyncOrchestrator
from core.shopify.product_loader import ProductLoader
from core.state.state_manager import StateManager
from targets.meta import CSVExporter


def test_full_meta_sync_writes_csv_and_state(
    meta_env,
    no_config_connectivity,
    tmp_path,
    sample_active_countries,
    sample_location_country_map,
):
    variant_gid = "gid://shopify/ProductVariant/111"
    product_gid = "gid://shopify/Product/222"
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
            "inventoryItem": {"sku": "PROD-M"},
            "price": "10",
            "updatedAt": "",
        },
        {
            "__parentId": variant_gid,
            "location": {"id": "gid://shopify/Location/55"},
            "quantities": [{"name": "available", "quantity": 4}],
        },
    ]
    jl = tmp_path / "bulk.jsonl"
    with open(jl, "w", encoding="utf-8") as f:
        for o in lines:
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
        exporter=CSVExporter(),
    )
    orch.run_full()

    feed = meta_env / "Meta catalog - country feed updates" / "country_feed_US.csv"
    assert feed.exists()
    cache_meta = meta_env / "cache" / "meta"
    assert (cache_meta / "sync_state.json").exists()
    assert (cache_meta / "variant_states.json").exists()
    header = feed.read_text(encoding="utf-8").splitlines()[0]
    assert header == "id,override,availability"
