"""Tests for ProductLoader JSONL parsing and country variant construction."""
import json
from pathlib import Path

import pytest

from core.shopify.product_loader import ProductLoader, clean_description, extract_size_from_sku


def test_clean_description_empty():
    assert clean_description("") == "Product description not available"
    assert clean_description(None) == "Product description not available"


def test_clean_description_strips_html():
    assert "Hello" in clean_description("<p>Hello</p>")


def test_extract_size_from_sku():
    assert extract_size_from_sku("X", "AB-M-L") == "M"
    assert extract_size_from_sku("nope") == "Unknown"


def test_load_missing_file_returns_empty(tmp_path, google_env, sample_active_countries, sample_location_country_map):
    pl = ProductLoader()
    assert (
        pl.load_products_from_bulk_jsonl(
            str(tmp_path / "missing.jsonl"),
            sample_active_countries,
            sample_location_country_map,
        )
        == []
    )


def test_malformed_json_line_skipped(tmp_path, google_env, sample_active_countries, sample_location_country_map):
    p = tmp_path / "b.jsonl"
    p.write_text('{"id":"x"}\nnot-json\n', encoding="utf-8")
    pl = ProductLoader()
    out = pl.load_products_from_bulk_jsonl(
        str(p),
        sample_active_countries,
        sample_location_country_map,
    )
    assert out == []


def test_empty_file(tmp_path, google_env, sample_active_countries, sample_location_country_map):
    p = tmp_path / "e.jsonl"
    p.write_text("", encoding="utf-8")
    pl = ProductLoader()
    assert (
        pl.load_products_from_bulk_jsonl(
            str(p),
            sample_active_countries,
            sample_location_country_map,
        )
        == []
    )


def test_valid_bulk_builds_country_variant(
    google_env,
    minimal_bulk_jsonl_path,
    sample_active_countries,
    sample_location_country_map,
):
    pl = ProductLoader()
    rows = pl.load_products_from_bulk_jsonl(
        minimal_bulk_jsonl_path,
        sample_active_countries,
        sample_location_country_map,
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["country_code"] == "US"
    assert r["inventory_quantity"] == 3
    assert r["sku"] == "PROD-M"
    assert r["product_id"] == "222"


def test_variant_without_sku_skipped(
    tmp_path,
    google_env,
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
            "inventoryItem": {},
            "price": "10",
            "updatedAt": "",
        },
        {
            "__parentId": variant_gid,
            "location": {"id": "gid://shopify/Location/55"},
            "quantities": [{"name": "available", "quantity": 1}],
        },
    ]
    p = tmp_path / "x.jsonl"
    with open(p, "w", encoding="utf-8") as f:
        for o in lines:
            f.write(json.dumps(o) + "\n")
    pl = ProductLoader()
    assert (
        pl.load_products_from_bulk_jsonl(
            str(p),
            sample_active_countries,
            sample_location_country_map,
        )
        == []
    )
