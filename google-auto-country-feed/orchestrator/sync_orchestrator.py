import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import glob

from config import settings
from state_management.state_manager import StateManager
from shopify_client.shopify_sync import ShopifySync
from shopify_client.product_loader import ProductLoader
from output.tsv_exporter import TSVExporter
from mapping.country_location_mapper import CountryLocationMapper
from orchestrator.config_validator import ConfigValidator
from output.drive_sync import SyncManager

logger = logging.getLogger(__name__)

class SyncOrchestrator:

    def __init__(self, state_manager=None, shopify_sync=None, product_loader=None, 
                 tsv_exporter=None, country_mapper=None, sync_manager=None):
        self.state_manager = state_manager or StateManager()
        self.shopify_sync = shopify_sync or ShopifySync()
        self.product_loader = product_loader or ProductLoader()
        self.tsv_exporter = tsv_exporter or TSVExporter()
        self.country_mapper = country_mapper or CountryLocationMapper(self.shopify_sync)
        self.sync_manager = sync_manager or SyncManager()

    def run_smart(self):
        """ sync with automatic change detection."""
        logger.info("--- Starting Sync ---")
        return self._run_smart_sync_with_detection()

    def run_full(self):
        """Force full sync."""
        logger.info("--- Starting Full Sync ---")
        logger.debug("Forcing full sync with fresh mapping")
        return self._run_full_sync(force_full=True)

    def run_incremental(self):
        """Incremental sync with smart detection."""
        logger.info("--- Starting Incremental Sync ---")

        last_sync = self.state_manager.load_sync_state()
        if not last_sync:
            logger.info("No previous sync - using full sync")
            return self.run_smart()

        return self._run_smart_sync_with_detection()

    def _run_smart_sync_with_detection(self):
        """Core smart sync logic."""
        start_time = time.time()
        sync_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)

        try:
            # Validate configuration
            self._validate_configuration()

            # Smart mapping detection
            mapping_data, mapping_changed, change_reason = self._detect_mapping_changes()

            # Determine sync strategy
            sync_type, since_timestamp = self._determine_smart_sync_strategy(mapping_changed, change_reason)

            # Execute sync
            variants = self._get_shopify_data(since_timestamp, mapping_data)

            if not variants:
                logger.info("No data to process - sync is up to date")
                self.state_manager.save_sync_state(sync_timestamp)
                return

            # Generate TSV files
            created_files = self._process_variants(sync_type, variants)

            # Upload to Google Drive
            if created_files:
                logger.info(f"Uploading {len(created_files)} feed files to Google Drive")
                self.sync_manager.upload_files_with_cleanup(
                    created_files,
                    set(settings.TARGET_COUNTRIES),
                    sync_type
                )

            # Save state
            self.state_manager.save_sync_state(sync_timestamp)

            self._log_summary(sync_type, variants, created_files, start_time, change_reason)
            logger.info("--- Sync Completed ---")

        except Exception as e:
            logger.error(f"Smart sync failed: {e}")
            raise

    def _detect_mapping_changes(self) -> Tuple[Dict, bool, str]:
        """Detect mapping changes."""
        logger.debug("Detecting mapping changes...")
        mapping_data, mapping_changed, change_reason = self.country_mapper.get_mapping_with_change_detection()
        return mapping_data, mapping_changed, change_reason

    def _determine_smart_sync_strategy(self, mapping_changed: bool, change_reason: str) -> Tuple[str, Optional[datetime]]:
        """Determine sync strategy."""
        last_sync = self.state_manager.load_sync_state()

        if mapping_changed:
            logger.info(f"FULL SYNC: {change_reason}")
            return "FULL", None
        elif not last_sync:
            logger.info("FULL SYNC: No previous sync state")
            return "FULL", None
        else:
            logger.debug(f"INCREMENTAL SYNC: Using state since {last_sync.isoformat()}")
            return "INCREMENTAL", last_sync

    def _run_full_sync(self, force_full=False):
        """Full sync method."""
        start_time = time.time()
        sync_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)

        try:
            self._validate_configuration()

            # Fresh mapping
            mapping_data, _, _ = self.country_mapper.get_mapping_with_change_detection()

            variants = self._get_shopify_data(None, mapping_data)

            if not variants:
                logger.info("No variants to process")
                return

            created_files = self._process_variants("FULL", variants)

            # Upload to Google Drive
            if created_files:
                logger.info(f"Uploading {len(created_files)} feed files to Google Drive")
                self.sync_manager.upload_files_with_cleanup(
                    created_files,
                    set(settings.TARGET_COUNTRIES),
                    "FULL"
                )

            self.state_manager.save_sync_state(sync_timestamp)

            self._log_summary("FULL", variants, created_files, start_time, "Explicit full sync")
            logger.info("--- Full Sync Completed ---")

        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            raise

    def _get_shopify_data(self, since_timestamp: Optional[datetime], mapping_data: Dict) -> List[Dict]:
        """Fetch data using bulk operations."""
        logger.debug("Fetching data via bulk operations...")
        
        since_iso = None
        if since_timestamp:
            since_iso = since_timestamp.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.debug(f"Incremental fetch since: {since_iso}")
        else:
            logger.debug("Full fetch of all products")
        
        # Bulk operations
        jsonl_file_path = self.shopify_sync.bulk_query.get_products_variants_inventory_bulk(since_iso)
        
        if not jsonl_file_path:
            logger.debug("No data from bulk operation")
            return []
        
        # Process JSONL
        variants = self.product_loader.load_products_from_bulk_jsonl(
            jsonl_file_path,
            mapping_data["active_countries"], 
            mapping_data["location_country_map"]
        )
        
        # Cleanup temp files
        self._cleanup_temp_bulk_files(jsonl_file_path)
        
        return variants

    def _cleanup_temp_bulk_files(self, current_file: str = None):
        """Centralized cleanup of temporary bulk operation files."""
        try:
            temp_files = glob.glob(str(settings.TEMP_DIR / "bulk_result_*.jsonl"))
            deleted_count = 0
            
            for temp_file in temp_files:
                try:
                    Path(temp_file).unlink()
                    deleted_count += 1
                except Exception as e:
                    logger.debug(f"Could not delete {temp_file}: {e}")
            
            if deleted_count > 0:
                logger.debug(f"Cleaned up {deleted_count} temporary bulk operation files")
                
        except Exception as e:
            logger.debug(f"Failed to cleanup temp files: {e}")

    def _process_variants(self, sync_type: str, variants: List[Dict]) -> List[Path]:
        """Process variants and generate TSV files."""
        logger.debug("Generating Google Merchant Center TSV feeds...")

        if sync_type == "FULL":
            logger.debug("Creating complete country feeds")
            created_files = self.tsv_exporter.create_country_feeds_full(variants)
            self.state_manager.reset_variant_states()
            self.state_manager.update_variant_states(variants)
        else:
            logger.debug("Detecting changes for incremental update")
            new_variants, changed_variants = self.state_manager.detect_stock_changes(variants)

            if not new_variants and not changed_variants:
                logger.info("No changes detected - feeds are up to date")
                return []

            created_files = self.tsv_exporter.update_country_feeds_incremental(new_variants, changed_variants)
            self.state_manager.update_variant_states(variants)

        return created_files

    def _validate_configuration(self):
        """Validate configuration."""
        logger.debug("Validating configuration...")

        config_errors = ConfigValidator.validate_all()
        if config_errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f" - {error}" for error in config_errors)
            raise ValueError(error_msg)

        connectivity_errors = ConfigValidator.test_connectivity()
        if connectivity_errors:
            logger.warning("Connectivity issues:")
            for error in connectivity_errors:
                logger.warning(f" - {error}")

        logger.debug("Configuration validation passed")

    def refresh_mapping_cache(self):
        """Refresh mapping cache."""
        logger.info("--- Refreshing Mapping Cache ---")
        try:
            self.country_mapper.clear_mapping_hash()
            mapping_data, mapping_changed, change_reason = self.country_mapper.get_mapping_with_change_detection()

            logger.info("Mapping cache refreshed")
            logger.info(f" - Countries: {len(mapping_data.get('active_countries', {}))}")
            logger.info(f" - Locations: {len(mapping_data.get('location_country_map', {}))}")
            logger.info(f" - Change detected: {mapping_changed}")
            logger.info(f" - Reason: {change_reason}")

        except Exception as e:
            logger.error(f"Failed to refresh mapping: {e}")
            raise

    def debug_state(self):
        """Debug state information."""
        logger.info("--- Debug State Information ---")

        # State stats
        stats = self.state_manager.get_stats()
        logger.info("State Statistics:")
        logger.info(f" - Total variants: {stats['variant_count']}")
        logger.info(f" - In stock: {stats['in_stock_count']}")
        logger.info(f" - Out of stock: {stats['out_of_stock_count']}")
        logger.info(f" - Last sync: {stats['last_sync']}")
        logger.info(f" - Database size: {stats['db_size_kb']} KB")

        # Mapping stats
        mapping_stats = self.country_mapper.get_mapping_stats()
        logger.info("Mapping Status:")
        logger.info(f" - Enabled: {settings.SMART_MAPPING_ENABLED}")
        logger.info(f" - Has previous hash: {mapping_stats['has_previous_hash']}")
        logger.info(f" - Last hash: {mapping_stats['last_hash']}")

        # Target countries
        logger.info(f"Target Countries (Plan A): {sorted(list(settings.TARGET_COUNTRIES))}")

    def _log_summary(self, sync_type: str, variants: List[Dict], created_files: List[Path],
                     start_time: float, change_reason: str):
        """Log performance summary."""
        total_time = time.time() - start_time
        stats = self.tsv_exporter.get_export_stats(variants)

        logger.info("=== SYNC SUMMARY ===")
        logger.info(f"Sync Type: {sync_type}")
        logger.info(f"Reason: {change_reason}")
        logger.info(f"Target Countries: {sorted(list(settings.TARGET_COUNTRIES))}")
        logger.info(f"Total Variants: {stats['total_variants']}")
        logger.info(f"Countries: {stats['countries']}")
        logger.info(f"In Stock: {stats['in_stock_variants']}")
        logger.info(f"Out of Stock: {stats['out_of_stock_variants']}")
        logger.info(f"Files Created: {len(created_files)}")
        logger.info(f"Total Time: {total_time:.2f}s")
        logger.info("=" * 27)
