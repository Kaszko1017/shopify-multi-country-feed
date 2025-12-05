import glob
import re
import csv
from pathlib import Path
import logging
from typing import Dict, List
import tempfile
from config import settings

logger = logging.getLogger(__name__)

class TSVExporter:

    def __init__(self):
        self.buffer_size = settings.CSV_BUFFER_SIZE
        self._cached_feed_rows = {}

    def create_country_feeds_full(self, variants: List[Dict]) -> List[Path]:
        if not variants:
            logger.warning("No variants provided for full sync")
            return []

        logger.debug("Creating Google Merchant Center country TSV feeds")

        current_countries = self._get_current_countries(variants)
        logger.info(f"Creating feeds for {len(current_countries)} countries")

        self._cleanup_orphaned_country_files(current_countries)

        created_files = []
        feed_data_by_country = self._organize_variants_by_country(variants)

        for country_code, country_variants in feed_data_by_country.items():
            filename = f"{settings.FEED_PREFIX}{country_code}{settings.FEED_EXTENSION}"
            filepath = settings.OUTPUT_DIR / filename

            try:
                rows = []
                for variant in country_variants:
                    if self._validate_variant_data(variant):
                        base_id = self._extract_variant_id(variant)
                        product_id = variant.get("product_id")
                        # TODO: prefix needs to be dynamic based on the store settings
                        composite_id = f"shopify_US_{product_id}_{base_id}"
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

    def update_country_feeds_incremental(self, new_variants: List[Dict], changed_variants: List[Dict]) -> List[Path]:
        """Incremental update of country TSV feeds."""
        if not new_variants and not changed_variants:
            logger.debug("No changes - feeds are up to date")
            return []

        all_changes = new_variants + changed_variants
        countries_to_update = self._get_countries_to_update(all_changes)

        updated_files = []
        for country_code in countries_to_update:
            filename = f"{settings.FEED_PREFIX}{country_code}{settings.FEED_EXTENSION}"
            filepath = settings.OUTPUT_DIR / filename

            try:
                # Use cached rows if available, otherwise load from file
                existing_rows = self._cached_feed_rows.get(country_code, {})
                if not existing_rows and filepath.exists():
                    existing_rows = self._load_existing_rows(filepath)
                    self._cached_feed_rows[country_code] = existing_rows

                # Apply changes efficiently
                self._apply_variant_changes(existing_rows, changed_variants + new_variants, country_code)

                # Write updated file
                rows = [(id_val, avail) for id_val, avail in existing_rows.items()]
                self._write_tsv_file(filepath, rows)

                updated_files.append(filepath)
                logger.debug(f"Updated feed: {filename}")
            except Exception as e:
                logger.error(f"Failed to update TSV feed for {country_code}: {e}")

        logger.info(f"Updated {len(updated_files)} country TSV feeds")
        return updated_files

    def _organize_variants_by_country(self, variants: List[Dict]) -> Dict[str, List[Dict]]:
        """Organize variants by country code."""
        feed_data = {}
        for variant in variants:
            country_code = variant.get("country_code")
            if country_code and variant.get("product_id"):
                if country_code not in feed_data:
                    feed_data[country_code] = []
                feed_data[country_code].append(variant)
        return feed_data

    def _apply_variant_changes(self, existing_rows: Dict, variants: List[Dict], country_code: str):
        """Apply variant changes to existing rows."""
        for variant in variants:
            if variant.get("country_code") == country_code and self._validate_variant_data(variant):
                base_id = self._extract_variant_id(variant)
                product_id = variant.get("product_id")
                # TODO: prefix needs to be dynamic based on the store settings
                composite_id = f"shopify_US_{product_id}_{base_id}"
                inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
                existing_rows[composite_id] = "in stock" if inventory_qty > 0 else "out of stock"

    def _load_existing_rows(self, filepath: Path) -> Dict[str, str]:
        """Load existing rows from TSV file."""
        existing_rows = {}
        try:
            with open(filepath, 'r', newline='', encoding='utf-8') as tsvfile:
                reader = csv.DictReader(tsvfile, delimiter='\t')
                for row in reader:
                    existing_rows[row['id']] = row['availability']
        except Exception as e:
            logger.warning(f"Could not load existing rows from {filepath}: {e}")
        return existing_rows

    def _write_tsv_file(self, filepath: Path, rows: List[tuple]):
        """Write TSV file efficiently."""
        with open(filepath, 'w', newline='', encoding='utf-8', buffering=self.buffer_size) as tsvfile:
            writer = csv.writer(tsvfile, delimiter='\t')
            writer.writerow(['id', 'availability'])
            writer.writerows(rows)

    def _validate_variant_data(self, variant: Dict) -> bool:
        """Validate variant data."""
        if not settings.ENABLE_DATA_VALIDATION:
            return True

        try:
            if not variant.get("id") or not variant.get("country_code"):
                return False

            inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
            if inventory_qty < 0:
                return False

            country_code = variant.get("country_code")
            if not re.match(r'^[A-Z]{2}$', country_code):
                return False

            return True

        except Exception as e:
            logger.debug(f"Validation error for variant {variant.get('id', 'unknown')}: {e}")
            return False

    def _get_current_countries(self, variants: List[Dict]) -> set:
        """Get current active countries from variants."""
        current_countries = set()
        for variant in variants:
            country_code = variant.get("country_code")
            if (country_code and variant.get("product_id") and 
                self._validate_variant_data(variant)):
                current_countries.add(country_code)
        return current_countries

    def _get_countries_to_update(self, variants: List[Dict]) -> set:
        """Get countries that need updates."""
        countries_to_update = set()
        for variant in variants:
            country_code = variant.get("country_code")
            if (country_code and variant.get("product_id") and 
                self._validate_variant_data(variant)):
                countries_to_update.add(country_code)
        return countries_to_update

    def _cleanup_orphaned_country_files(self, current_countries: set):
        """Clean up TSV files for inactive countries."""
        existing_files = self._find_existing_country_files()

        if not existing_files:
            return

        existing_countries = set()
        files_by_country = {}

        for file_path in existing_files:
            country_code = self._extract_country_from_filename(file_path)
            if country_code:
                existing_countries.add(country_code)
                if country_code not in files_by_country:
                    files_by_country[country_code] = []
                files_by_country[country_code].append(file_path)

        orphaned_countries = existing_countries - current_countries

        if not orphaned_countries:
            logger.debug("No orphaned country files found")
            return

        deleted_count = 0
        for country_code in orphaned_countries:
            for file_path in files_by_country[country_code]:
                try:
                    if Path(file_path).exists():
                        Path(file_path).unlink()
                        deleted_count += 1
                        # Remove from cache
                        self._cached_feed_rows.pop(country_code, None)
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} orphaned files")

    def _find_existing_country_files(self) -> List[str]:
        """Find existing country feed files."""
        pattern = str(settings.OUTPUT_DIR / f"{settings.FEED_PREFIX}*{settings.FEED_EXTENSION}")
        return glob.glob(pattern)

    def _extract_country_from_filename(self, file_path: str) -> str:
        """Extract country code from filename."""
        filename = Path(file_path).name
        pattern = rf'{settings.FEED_PREFIX}([A-Z]{{2}}){re.escape(settings.FEED_EXTENSION)}$'
        match = re.search(pattern, filename)
        return match.group(1) if match else None

    def _extract_variant_id(self, variant: Dict) -> str:
        """Extract variant ID."""
        composite_id = variant.get("id", "")
        if "-" in composite_id:
            parts = composite_id.rsplit("-", 1)
            if len(parts) == 2:
                return parts[0]
        return composite_id

    def _safe_int(self, value) -> int:
        """Safely convert to integer."""
        try:
            if value is None:
                return 0
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0

    def get_export_stats(self, variants: List[Dict]) -> Dict:
        """Get export statistics."""
        if not variants:
            return {}

        countries = {}
        total_inventory = 0
        in_stock_count = 0

        for variant in variants:
            country_code = variant.get("country_code")
            inventory = self._safe_int(variant.get("inventory_quantity", 0))

            if not self._validate_variant_data(variant):
                continue

            if country_code:
                if country_code not in countries:
                    countries[country_code] = {"count": 0, "inventory": 0}
                countries[country_code]["count"] += 1
                countries[country_code]["inventory"] += inventory

            total_inventory += inventory
            if inventory > 0:
                in_stock_count += 1

        return {
            "total_variants": len(variants),
            "countries": len(countries),
            "country_breakdown": countries,
            "total_inventory": total_inventory,
            "in_stock_variants": in_stock_count,
            "out_of_stock_variants": len(variants) - in_stock_count
        }
