import os
import csv
import glob
import re
from pathlib import Path
import logging
from typing import Generator, Dict, List, Tuple
import tempfile
from config import settings

logger = logging.getLogger(__name__)

class CSVExporter:

    def __init__(self):
        self.buffer_size = settings.CSV_BUFFER_SIZE

    def create_country_feeds_full(self, variants: List[Dict]) -> List[Path]:
        if not variants:
            logger.warning("No variants provided for full sync")
            return []

        logger.info("Full sync: Creating CSV country feeds with streaming approach")

        current_countries = self._get_current_countries(variants)
        logger.info(f"Current active countries: {sorted(list(current_countries))}")

        self._cleanup_orphaned_country_files(current_countries)

        # Use generator-based approach for memory efficiency
        created_files = []
        for country_code in current_countries:
            country_generator = self._create_country_variant_generator(variants, country_code)
            filename = f"{settings.FEED_PREFIX}{country_code}{settings.FEED_EXTENSION}"
            filepath = settings.OUTPUT_DIR / filename

            if self._create_country_feed_streaming(country_generator, country_code, filepath):
                created_files.append(filepath)
                logger.info(f"Created CSV feed: {filename}")

        logger.info(f"Full sync complete: {len(created_files)} CSV files created")
        return created_files

    def update_country_feeds_incremental(self, new_variants: List[Dict],
                                         changed_variants: List[Dict]) -> List[Path]:
        if not new_variants and not changed_variants:
            logger.info("No variant changes detected - no CSV updates needed")
            return []

        all_changed = new_variants + changed_variants
        countries_to_update = self._get_countries_to_update(all_changed)

        updated_files = []
        for country_code in countries_to_update:
            filename = f"{settings.FEED_PREFIX}{country_code}{settings.FEED_EXTENSION}"
            filepath = settings.OUTPUT_DIR / filename

            updated_file = self._update_country_feed_streaming(
                new_variants, changed_variants, country_code, filepath
            )

            if updated_file:
                updated_files.append(updated_file)
                logger.info(f"Updated CSV feed: {filename}")

        logger.info(f"Incremental update complete: {len(updated_files)} CSV files updated")
        return updated_files

    def _create_country_variant_generator(self, variants: List[Dict],
                                          country_code: str) -> Generator[Dict, None, None]:
        """Generator that yields variants for a specific country."""
        for variant in variants:
            if variant.get("country_code") == country_code and variant.get("product_id"):
                yield variant

    def _create_country_feed_streaming(self, variant_generator: Generator[Dict, None, None],
                                       country_code: str, output_file: Path) -> bool:
        """Create CSV using streaming generator approach."""
        logger.info(f"Creating streaming CSV feed for {country_code}")

        variant_count = 0
        try:
            with open(output_file, 'w', newline='', encoding='utf-8',
                      buffering=self.buffer_size) as csvfile:
                writer = csv.writer(csvfile)

                # Write header
                writer.writerow(['id', 'override', 'availability'])

                # Stream variants one by one
                for variant in variant_generator:
                    if self._validate_variant_data(variant):
                        row = self._format_csv_row(variant, country_code)
                        if row:
                            writer.writerow(row)
                            variant_count += 1

            logger.info(f"Streaming CSV created: {variant_count} variants written to {output_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to create streaming CSV for {country_code}: {e}")
            if output_file.exists():
                output_file.unlink(missing_ok=True)
            return False

    def _update_country_feed_streaming(self, new_variants: List[Dict],
                                       changed_variants: List[Dict],
                                       country_code: str, output_file: Path) -> Path:
        """Incremental update using streaming with temporary file approach."""
        logger.info(f"Streaming incremental update for {country_code}")

        country_new_variants = [v for v in new_variants if v.get("country_code") == country_code]
        country_changed_variants = [v for v in changed_variants if v.get("country_code") == country_code]

        if not country_new_variants and not country_changed_variants:
            return output_file

        # Build change lookup for efficiency
        change_lookup = {}
        for variant in country_changed_variants:
            variant_id = self._extract_shopify_variant_id(variant)
            if variant_id:
                inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
                availability = "in stock" if inventory_qty > 0 else "out of stock"
                change_lookup[variant_id] = availability

        # Process with temporary file for atomic updates
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', newline='', encoding='utf-8',
                                             delete=False, suffix='.csv', dir=output_file.parent) as temp_file:
                writer = csv.writer(temp_file)
                writer.writerow(['id', 'override', 'availability'])

                updated_count = 0
                existing_count = 0

                # Copy existing data with updates
                if output_file.exists():
                    with open(output_file, 'r', newline='', encoding='utf-8') as existing_file:
                        reader = csv.DictReader(existing_file)
                        for row in reader:
                            if row.get('override') == country_code:
                                variant_id = row['id']
                                if variant_id in change_lookup:
                                    # Update existing variant
                                    writer.writerow([variant_id, country_code, change_lookup[variant_id]])
                                    updated_count += 1
                                else:
                                    # Keep existing variant unchanged
                                    writer.writerow([variant_id, country_code, row['availability']])
                                    existing_count += 1

                # Append new variants
                appended_count = 0
                for variant in country_new_variants:
                    if self._validate_variant_data(variant):
                        row = self._format_csv_row(variant, country_code)
                        if row:
                            writer.writerow(row)
                            appended_count += 1

            # Atomic replace
            Path(temp_file.name).replace(output_file)

            logger.info(f"Streaming incremental update complete: {existing_count} existing, "
                        f"{updated_count} updated, {appended_count} appended")
            return output_file

        except Exception as e:
            logger.error(f"Failed streaming incremental update for {country_code}: {e}")
            if temp_file and Path(temp_file.name).exists():
                Path(temp_file.name).unlink(missing_ok=True)
            return output_file

    def _format_csv_row(self, variant: Dict, country_code: str) -> List[str]:
        """Format a single variant into CSV row."""
        try:
            variant_id = self._extract_shopify_variant_id(variant)
            if not variant_id:
                return None

            inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
            availability = "in stock" if inventory_qty > 0 else "out of stock"

            return [variant_id, country_code, availability]

        except Exception as e:
            logger.warning(f"Error formatting CSV row for variant {variant.get('id', 'unknown')}: {e}")
            return None

    def _validate_variant_data(self, variant: Dict) -> bool:
        """Validate variant data structure and business rules."""
        if not settings.ENABLE_DATA_VALIDATION:
            return True

        try:
            # Required fields validation
            if not variant.get("id"):
                return False

            if not variant.get("country_code"):
                return False

            # Business logic validation
            inventory_qty = self._safe_int(variant.get("inventory_quantity", 0))
            if inventory_qty < 0:
                logger.warning(f"Negative inventory detected for variant {variant.get('id')}: {inventory_qty}")
                return False

            # Sanity check for extremely high inventory
            if inventory_qty > settings.MAX_INVENTORY_THRESHOLD:
                logger.warning(f"Unusually high inventory for variant {variant.get('id')}: {inventory_qty}")

            # Country code validation
            country_code = variant.get("country_code")
            if not re.match(r'^[A-Z]{2}$', country_code):
                logger.warning(f"Invalid country code format: {country_code}")
                return False

            return True

        except Exception as e:
            logger.warning(f"Validation error for variant {variant.get('id', 'unknown')}: {e}")
            return False

    def manual_cleanup_orphaned_files(self):
        """Manual cleanup for CLI usage."""
        logger.info("=== Manual CSV Cleanup ===")
        try:
            mapping_files = glob.glob(str(settings.CACHE_DIR / "country-mapping-*.json"))
            if mapping_files:
                latest_mapping = sorted(mapping_files)[-1]
                with open(latest_mapping, 'r') as f:
                    import json
                    mapping_data = json.load(f)

                active_countries = set(mapping_data.get("active_countries", {}).keys())
                logger.info(f"Active countries from mapping: {sorted(list(active_countries))}")
                self._cleanup_orphaned_country_files(active_countries)
            else:
                logger.warning("No country mapping found - cannot determine active countries")

        except Exception as e:
            logger.error(f"Error during manual cleanup: {e}")

    def get_export_stats(self, variants: List[Dict]) -> Dict:
        if not variants:
            return {}

        countries = {}
        total_inventory = 0
        in_stock_count = 0
        validation_failures = 0

        for variant in variants:
            country_code = variant.get("country_code")
            inventory = self._safe_int(variant.get("inventory_quantity", 0))

            if not self._validate_variant_data(variant):
                validation_failures += 1
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
            "valid_variants": len(variants) - validation_failures,
            "validation_failures": validation_failures,
            "countries": len(countries),
            "country_breakdown": countries,
            "total_inventory": total_inventory,
            "in_stock_variants": in_stock_count,
            "out_of_stock_variants": len(variants) - in_stock_count - validation_failures
        }

    def _get_current_countries(self, variants: List[Dict]) -> set:
        """Get set of current active countries from variants."""
        current_countries = set()
        for variant in variants:
            country_code = variant.get("country_code")
            if country_code and variant.get("product_id") and self._validate_variant_data(variant):
                current_countries.add(country_code)
        return current_countries

    def _get_countries_to_update(self, variants: List[Dict]) -> set:
        """Get set of countries that need updates."""
        countries_to_update = set()
        for variant in variants:
            country_code = variant.get("country_code")
            if country_code and variant.get("product_id") and self._validate_variant_data(variant):
                countries_to_update.add(country_code)
        return countries_to_update

    def _cleanup_orphaned_country_files(self, current_countries: set):
        """Clean up CSV files for countries that are no longer active."""
        existing_files = self._find_existing_country_files()

        if not existing_files:
            logger.info("No existing country CSV files found")
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
        logger.info(f"Orphaned countries: {sorted(list(orphaned_countries))}")

        if not orphaned_countries:
            logger.info("No orphaned country CSV files found")
            return

        for country_code in orphaned_countries:
            for file_path in files_by_country[country_code]:
                try:
                    if Path(file_path).exists():
                        Path(file_path).unlink()
                        logger.info(f"Deleted orphaned CSV: {Path(file_path).name}")
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")

    def _find_existing_country_files(self) -> List[str]:
        pattern = str(settings.OUTPUT_DIR / f"{settings.FEED_PREFIX}*{settings.FEED_EXTENSION}")
        return glob.glob(pattern)

    def _extract_country_from_filename(self, file_path: str) -> str:
        """Extract country code from CSV filename."""
        filename = Path(file_path).name
        pattern = rf'{settings.FEED_PREFIX}([A-Z]{{2}}){re.escape(settings.FEED_EXTENSION)}$'
        match = re.search(pattern, filename)

        if match:
            return match.group(1)

        logger.warning(f"Could not extract country code from filename: {filename}")
        return None

    def _extract_shopify_variant_id(self, variant: Dict) -> str:
        """Extract Shopify variant ID from composite ID."""
        composite_id = variant.get("id", "")
        if "-" in composite_id:
            parts = composite_id.rsplit("-", 1)
            if len(parts) == 2:
                return parts[0]
        return composite_id

    def _safe_int(self, value) -> int:
        """Safely convert value to integer."""
        try:
            if value is None:
                return 0
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0
