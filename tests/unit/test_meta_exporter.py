"""Tests for targets.meta.CSVExporter."""
import csv
from pathlib import Path

import pytest

from targets.meta import CSVExporter


def test_full_sync_empty(meta_env):
    exp = CSVExporter()
    assert exp.create_country_feeds_full([]) == []


def test_full_sync_csv_headers_and_rows(meta_env, sample_variant_us_in_stock):
    exp = CSVExporter()
    files = exp.create_country_feeds_full([sample_variant_us_in_stock])
    assert len(files) == 1
    assert files[0].suffix == ".csv"
    assert files[0].name == "country_feed_US.csv"
    with open(files[0], newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["id", "override", "availability"]
    assert rows[1][0] == "111"
    assert rows[1][1] == "US"
    assert rows[1][2] == "in stock"


def test_special_characters_in_row(meta_env):
    exp = CSVExporter()
    v = {
        "id": "999-US",
        "shopify_variant_id": "999",
        "country_code": "US",
        "inventory_quantity": 1,
        "sku": 'SKU"X',
        "product_id": "1",
        "product_title": 'Title, with comma',
        "product_handle": "h",
        "product_description": "d",
        "featured_image": "",
        "country_name": "U",
        "price": "1",
        "size": "M",
        "updated_at": "",
    }
    files = exp.create_country_feeds_full([v])
    raw = files[0].read_text(encoding="utf-8")
    assert "999" in raw


def test_incremental_appends_new_row(meta_env, sample_variant_us_in_stock):
    exp = CSVExporter()
    exp.create_country_feeds_full([sample_variant_us_in_stock])
    ca = dict(sample_variant_us_in_stock)
    ca["id"] = "222-CA"
    ca["shopify_variant_id"] = "222"
    ca["country_code"] = "CA"
    ca["country_name"] = "Canada"
    ca["inventory_quantity"] = 2
    exp.update_country_feeds_incremental([ca], [])
    us_path = meta_env / "Meta catalog - country feed updates" / "country_feed_US.csv"
    ca_path = meta_env / "Meta catalog - country feed updates" / "country_feed_CA.csv"
    assert us_path.exists() and ca_path.exists()
