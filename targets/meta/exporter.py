"""CSV feed writer for Meta catalog (id, override, availability)."""
import csv
import glob
import re
import tempfile
from pathlib import Path
from typing import Dict, Generator, List

import logging

from core.config import get_config

logger = logging.getLogger(__name__)


class CSVExporter:
    """Writes per-country CSV feeds with streaming full and incremental updates."""

    def __init__(self):
        self.buffer_size = get_config().csv_buffer_size

    def create_country_feeds_full(self, variants: List[Dict]) -> List[Path]:
        if not variants:
            logger.warning("No variants provided for full sync")
            return []
        cfg = get_config()
        current_countries = self._get_current_countries(variants)
        logger.info(f"Current active countries: {sorted(current_countries)}")
        self._cleanup_orphaned_country_files(current_countries)

        created_files = []
        for country_code in current_countries:
            gen = self._create_country_variant_generator(variants, country_code)
            filename = f"{cfg.feed_prefix}{country_code}{cfg.feed_extension}"
            filepath = cfg.output_dir / filename
            if self._create_country_feed_streaming(gen, country_code, filepath):
                created_files.append(filepath)
                logger.info(f"Created CSV feed: {filename}")

        logger.info(f"Full sync complete: {len(created_files)} CSV files created")
        return created_files

    def update_country_feeds_incremental(
        self, new_variants: List[Dict], changed_variants: List[Dict]
    ) -> List[Path]:
        if not new_variants and not changed_variants:
            return []
        cfg = get_config()
        all_changed = new_variants + changed_variants
        countries_to_update = self._get_countries_to_update(all_changed)
        updated_files = []

        for country_code in countries_to_update:
            filename = f"{cfg.feed_prefix}{country_code}{cfg.feed_extension}"
            filepath = cfg.output_dir / filename
            result = self._update_country_feed_streaming(
                new_variants, changed_variants, country_code, filepath
            )
            if result:
                updated_files.append(result)
                logger.info(f"Updated CSV feed: {filename}")

        logger.info(f"Incremental update complete: {len(updated_files)} CSV files updated")
        return updated_files

    def _create_country_variant_generator(
        self, variants: List[Dict], country_code: str
    ) -> Generator[Dict, None, None]:
        for v in variants:
            if v.get("country_code") == country_code and v.get("product_id"):
                yield v

    def _create_country_feed_streaming(
        self,
        variant_generator: Generator[Dict, None, None],
        country_code: str,
        output_file: Path,
    ) -> bool:
        variant_count = 0
        try:
            with open(
                output_file,
                "w",
                newline="",
                encoding="utf-8",
                buffering=self.buffer_size,
            ) as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["id", "override", "availability"])
                for variant in variant_generator:
                    if self._validate_variant_data(variant):
                        row = self._format_csv_row(variant)
                        if row:
                            writer.writerow(row)
                            variant_count += 1
            logger.info(f"Streaming CSV created: {variant_count} variants for {country_code}")
            return True
        except Exception as e:
            logger.error(f"Failed to create CSV for {country_code}: {e}")
            if output_file.exists():
                output_file.unlink(missing_ok=True)
            return False

    def _update_country_feed_streaming(
        self,
        new_variants: List[Dict],
        changed_variants: List[Dict],
        country_code: str,
        output_file: Path,
    ) -> Path:
        country_new = [v for v in new_variants if v.get("country_code") == country_code]
        country_changed = [v for v in changed_variants if v.get("country_code") == country_code]
        if not country_new and not country_changed:
            return output_file

        change_lookup = {}
        for v in country_changed:
            vid = self._extract_shopify_variant_id(v)
            if vid:
                qty = self._safe_int(v.get("inventory_quantity", 0))
                change_lookup[vid] = "in stock" if qty > 0 else "out of stock"

        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                newline="",
                encoding="utf-8",
                delete=False,
                suffix=".csv",
                dir=output_file.parent,
            ) as tmp:
                temp_file = tmp.name
                writer = csv.writer(tmp)
                writer.writerow(["id", "override", "availability"])

                if output_file.exists():
                    with open(output_file, "r", newline="", encoding="utf-8") as existing:
                        reader = csv.DictReader(existing)
                        for row in reader:
                            if row.get("override") == country_code:
                                vid = row["id"]
                                writer.writerow(
                                    [
                                        vid,
                                        country_code,
                                        change_lookup.get(vid, row["availability"]),
                                    ]
                                )

                for v in country_new:
                    if self._validate_variant_data(v):
                        row = self._format_csv_row(v)
                        if row:
                            writer.writerow(row)

            Path(temp_file).replace(output_file)
            return output_file
        except Exception as e:
            logger.error(f"Failed incremental update for {country_code}: {e}")
            if temp_file and Path(temp_file).exists():
                Path(temp_file).unlink(missing_ok=True)
            return output_file

    def _format_csv_row(self, variant: Dict) -> List:
        cfg = get_config()
        vid = self._extract_shopify_variant_id(variant)
        if not vid:
            return None
        qty = self._safe_int(variant.get("inventory_quantity", 0))
        avail = "in stock" if qty > 0 else "out of stock"
        return [vid, variant.get("country_code", ""), avail]

    def _validate_variant_data(self, variant: Dict) -> bool:
        cfg = get_config()
        if not cfg.enable_data_validation:
            return True
        try:
            if not variant.get("id") or not variant.get("country_code"):
                return False
            qty = self._safe_int(variant.get("inventory_quantity", 0))
            if qty < 0:
                return False
            if qty > cfg.max_inventory_threshold:
                logger.warning(f"Unusually high inventory for variant {variant.get('id')}: {qty}")
            cc = variant.get("country_code")
            if not re.match(r"^[A-Z]{2}$", cc):
                return False
            return True
        except Exception as e:
            logger.warning(f"Validation error for variant {variant.get('id', 'unknown')}: {e}")
            return False

    def _get_current_countries(self, variants: List[Dict]) -> set:
        return self._get_countries_to_update(variants)

    def _get_countries_to_update(self, variants: List[Dict]) -> set:
        s = set()
        for v in variants:
            if v.get("country_code") and v.get("product_id") and self._validate_variant_data(v):
                s.add(v["country_code"])
        return s

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
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")
        if orphaned:
            logger.info(f"Cleaned up orphaned CSV files for: {sorted(orphaned)}")

    def _extract_country_from_filename(self, file_path: str) -> str:
        cfg = get_config()
        name = Path(file_path).name
        m = re.search(
            rf"{re.escape(cfg.feed_prefix)}([A-Z]{{2}}){re.escape(cfg.feed_extension)}$",
            name,
        )
        return m.group(1) if m else ""

    def _extract_shopify_variant_id(self, variant: Dict) -> str:
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
        validation_failures = 0
        for v in variants:
            if not self._validate_variant_data(v):
                validation_failures += 1
                continue
            cc = v.get("country_code")
            inv = self._safe_int(v.get("inventory_quantity", 0))
            if cc:
                countries.setdefault(cc, {"count": 0, "inventory": 0})
                countries[cc]["count"] += 1
                countries[cc]["inventory"] += inv
            if inv > 0:
                in_stock_count += 1
        return {
            "total_variants": len(variants),
            "valid_variants": len(variants) - validation_failures,
            "validation_failures": validation_failures,
            "countries": len(countries),
            "country_breakdown": countries,
            "in_stock_variants": in_stock_count,
            "out_of_stock_variants": len(variants) - in_stock_count - validation_failures,
        }
