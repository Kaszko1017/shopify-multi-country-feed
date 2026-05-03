"""Tests for targets.google.TSVExporter."""
import csv

from targets.google import TSVExporter


def test_full_sync_empty_variants_returns_no_files(google_env):
    exp = TSVExporter()
    assert exp.create_country_feeds_full([]) == []


def test_full_sync_creates_tsv_with_headers_and_rows(google_env, sample_variant_us_in_stock):
    exp = TSVExporter()
    files = exp.create_country_feeds_full([sample_variant_us_in_stock])
    assert len(files) == 1
    fp = files[0]
    assert fp.suffix == ".tsv"
    assert "country_feed_US.tsv" == fp.name
    text = fp.read_text(encoding="utf-8")
    lines = text.strip().splitlines()
    assert lines[0].split("\t") == ["id", "availability"]
    assert len(lines) == 2
    id_val, avail = lines[1].split("\t")
    assert avail == "in stock"
    assert id_val.startswith("shopify_US_")
    assert "222" in id_val and "111" in id_val


def test_availability_out_of_stock(google_env, sample_variant_us_out_of_stock):
    exp = TSVExporter()
    files = exp.create_country_feeds_full([sample_variant_us_out_of_stock])
    with open(files[0], newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert rows[0]["availability"] == "out of stock"


def test_incremental_updates_existing_file(google_env, sample_variant_us_in_stock, sample_variant_us_out_of_stock):
    exp = TSVExporter()
    exp.create_country_feeds_full([sample_variant_us_in_stock])
    updated = exp.update_country_feeds_incremental([], [sample_variant_us_out_of_stock])
    assert len(updated) == 1
    with open(updated[0], newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    assert rows[0]["availability"] == "out of stock"


def test_invalid_country_code_row_skipped(google_env):
    """Invalid ISO codes are skipped in rows, but a per-country file may still be opened from grouping."""
    exp = TSVExporter()
    bad = {
        "id": "1-US",
        "country_code": "USA",
        "product_id": "9",
        "inventory_quantity": 1,
        "sku": "s",
    }
    files = exp.create_country_feeds_full([bad])
    assert len(files) == 1
    body = files[0].read_text(encoding="utf-8").strip().splitlines()
    assert body == ["id\tavailability"]
