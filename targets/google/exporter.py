"""TSV feed writer for Google Merchant Center (id, availability)."""
import csv
import glob
import re
from pathlib import Path
from typing import Dict, List

import logging

from core.config import get_config

logger = logging.getLogger(__name__)


class TSVExporter:
    """Writes per-country TSV feeds and supports full and incremental updates."""

    def __init__(self):
        self.buffer_size = get_config().csv_buffer_size
        self._cached_feed_rows = {}

    def create_country_feeds_full(self, variants: List[Dict]) -> List[Path]:
        if not variants:
            logger.warning("No variants provided for full sync")
            return []
        cfg = get_config()
        logger.debug("Creating Google Merchant Center country TSV feeds")

        current_countries = self._get_current_countries(variants)
        logger.info(f"Creating feeds for {len(current_countries)} countries")
        self._cleanup_orphaned_country_files(current_countries)

        created_files = []
        feed_data_by_country = self._organize_variants_by_country(variants)

        for country_code, country_variants in feed_data_by_country.items():
            filename = f"{cfg.feed_prefix}{country_code}{cfg.feed_extension}"
            filepath = cfg.output_dir / filename
            try:
                rows = []
                for variant in country_variants:
                    if self._validate_variant_data(variant):
                        composite_id = self._composite_id(variant)
                        inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
                        availability = "in stock" if inventory_qty > 0 else "out of stock"
                        rows.append((composite_id, availability))
                self._cached_feed_rows[country_code] = {row[0]: row[1] for row in rows}
                self._write_tsv_file(filepath, rows)
                created_files.append(filepath)
                logger.debug(f"Created feed: {filename}")
            except Exception as e:
                logger.error(f"Failed to create TSV feed for {country_code}: {e}")
                if filepath.exists():
                    filepath.unlink(missing_ok=True)

        logger.info(f"Created {len(created_files)} country TSV feed files")
        return created_files

    def update_country_feeds_incremental(
        self, new_variants: List[Dict], changed_variants: List[Dict]
    ) -> List[Path]:
        if not new_variants and not changed_variants:
            return []
        cfg = get_config()
        all_changes = new_variants + changed_variants
        countries_to_update = self._get_countries_to_update(all_changes)
        updated_files = []

        for country_code in countries_to_update:
            filename = f"{cfg.feed_prefix}{country_code}{cfg.feed_extension}"
            filepath = cfg.output_dir / filename
            try:
                existing_rows = self._cached_feed_rows.get(country_code, {})
                if not existing_rows and filepath.exists():
                    existing_rows = self._load_existing_rows(filepath)
                    self._cached_feed_rows[country_code] = existing_rows
                self._apply_variant_changes(
                    existing_rows, changed_variants + new_variants, country_code
                )
                rows = [(id_val, avail) for id_val, avail in existing_rows.items()]
                self._write_tsv_file(filepath, rows)
                updated_files.append(filepath)
                logger.debug(f"Updated feed: {filename}")
            except Exception as e:
                logger.error(f"Failed to update TSV feed for {country_code}: {e}")

        logger.info(f"Updated {len(updated_files)} country TSV feeds")
        return updated_files

    def _composite_id(self, variant: Dict) -> str:
        cfg = get_config()
        base_id = self._extract_variant_id(variant)
        product_id = variant.get("product_id", "")
        return f"{cfg.feed_id_prefix}{product_id}_{base_id}"

    def _organize_variants_by_country(self, variants: List[Dict]) -> Dict[str, List[Dict]]:
        feed_data = {}
        for variant in variants:
            country_code = variant.get("country_code")
            if country_code and variant.get("product_id"):
                if country_code not in feed_data:
                    feed_data[country_code] = []
                feed_data[country_code].append(variant)
        return feed_data

    def _apply_variant_changes(
        self, existing_rows: Dict, variants: List[Dict], country_code: str
    ):
        for variant in variants:
            if variant.get("country_code") == country_code and self._validate_variant_data(variant):
                composite_id = self._composite_id(variant)
                inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
                existing_rows[composite_id] = (
                    "in stock" if inventory_qty > 0 else "out of stock"
                )

    def _load_existing_rows(self, filepath: Path) -> Dict[str, str]:
        existing_rows = {}
        try:
            with open(filepath, "r", newline="", encoding="utf-8") as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter="\t")
                for row in reader:
                    existing_rows[row["id"]] = row["availability"]
        except Exception as e:
            logger.warning(f"Could not load existing rows from {filepath}: {e}")
        return existing_rows

    def _write_tsv_file(self, filepath: Path, rows: List[tuple]):
        with open(
            filepath, "w", newline="", encoding="utf-8", buffering=self.buffer_size
        ) as tsvfile:
            writer = csv.writer(tsvfile, delimiter="\t")
            writer.writerow(["id", "availability"])
            writer.writerows(rows)

    def _validate_variant_data(self, variant: Dict) -> bool:
        cfg = get_config()
        if not cfg.enable_data_validation:
            return True
        try:
            if not variant.get("id") or not variant.get("country_code"):
                return False
            if self._safe_int(variant.get("inventory_quantity", 0)) < 0:
                return False
            cc = variant.get("country_code")
            if not re.match(r"^[A-Z]{2}$", cc):
                return False
            return True
        except Exception as e:
            logger.debug(f"Validation error for variant {variant.get('id', 'unknown')}: {e}")
            return False

    def _get_current_countries(self, variants: List[Dict]) -> set:
        current = set()
        for v in variants:
            cc = v.get("country_code")
            if cc and v.get("product_id") and self._validate_variant_data(v):
                current.add(cc)
        return current

    def _get_countries_to_update(self, variants: List[Dict]) -> set:
        return self._get_current_countries(variants)

    def _cleanup_orphaned_country_files(self, current_countries: set):
        cfg = get_config()
        pattern = str(cfg.output_dir / f"{cfg.feed_prefix}*{cfg.feed_extension}")
        existing = glob.glob(pattern)
        if not existing:
            return
        existing_countries = set()
        files_by_country = {}
        for file_path in existing:
            cc = self._extract_country_from_filename(file_path)
            if cc:
                existing_countries.add(cc)
                files_by_country.setdefault(cc, []).append(file_path)
        orphaned = existing_countries - current_countries
        for country_code in orphaned:
            for file_path in files_by_country.get(country_code, []):
                try:
                    Path(file_path).unlink(missing_ok=True)
                    self._cached_feed_rows.pop(country_code, None)
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")
        if orphaned:
            logger.info(f"Cleaned up {len(orphaned)} orphaned country files")

    def _extract_country_from_filename(self, file_path: str) -> str:
        cfg = get_config()
        name = Path(file_path).name
        m = re.search(
            rf"{re.escape(cfg.feed_prefix)}([A-Z]{{2}}){re.escape(cfg.feed_extension)}$",
            name,
        )
        return m.group(1) if m else ""

    def _extract_variant_id(self, variant: Dict) -> str:
        composite_id = variant.get("id", "")
        if "-" in composite_id:
            parts = composite_id.rsplit("-", 1)
            if len(parts) == 2:
                return parts[0]
        return composite_id

    def _safe_int(self, value) -> int:
        try:
            return 0 if value is None else int(float(str(value)))
        except (ValueError, TypeError):
            return 0

    def get_export_stats(self, variants: List[Dict]) -> Dict:
        if not variants:
            return {}
        countries = {}
        in_stock_count = 0
        for v in variants:
            cc = v.get("country_code")
            inv = self._safe_int(v.get("inventory_quantity", 0))
            if not self._validate_variant_data(v):
                continue
            if cc:
                countries.setdefault(cc, {"count": 0, "inventory": 0})
                countries[cc]["count"] += 1
                countries[cc]["inventory"] += inv
            if inv > 0:
                in_stock_count += 1
        return {
            "total_variants": len(variants),
            "countries": len(countries),
            "country_breakdown": countries,
            "in_stock_variants": in_stock_count,
            "out_of_stock_variants": len(variants) - in_stock_count,
        }
