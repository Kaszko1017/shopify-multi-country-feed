import time
import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import settings
from state_management.state_manager import StateManager
from output.drive_sync import SyncManager
from shopify_client.shopify_sync import ShopifySync
from shopify_client.product_loader import ProductLoader
from output.csv_exporter import CSVExporter
from mapping.country_location_mapper import CountryLocationMapper
from orchestrator.config_validator import ConfigValidator

logger = logging.getLogger(__name__)

class SyncOrchestrator:

    def __init__(self):
        self.state_manager = StateManager()
        self.sync_manager = SyncManager()
        self.shopify_sync = ShopifySync()
        self.product_loader = ProductLoader()
        self.csv_exporter = CSVExporter()
        self.country_mapper = CountryLocationMapper(self.shopify_sync)

    def run_smart(self):
        logger.info("--- Starting Sync ---")
        return self._run_smart_sync_with_detection()

    def run_full(self):
        logger.info("--- Starting Full Sync ---")
        logger.info("Force full sync requested - will use fresh mapping regardless of changes")
        return self._run_full_sync(force_full=True)

    def run_incremental(self):
        logger.info("--- Starting Incremental Sync ---")

        # Check if we have previous state
        last_sync = self.state_manager.load_sync_state()
        if not last_sync:
            logger.info("No previous sync state found - using full sync")
            return self.run_smart()

        # Run smart detection to see if incremental is appropriate
        return self._run_smart_sync_with_detection()

    def _run_smart_sync_with_detection(self):
        """Core smart sync logic with mapping change detection."""
        start_time = time.time()

        # Begin timing individual phases
        phase_start = time.time()
        phase_durations: Dict[str, float] = {}

        sync_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)

        try:
            phase_name = "Config"
            self._validate_configuration()
            phase_durations[phase_name] = time.time() - phase_start
            phase_start = time.time()

            phase_name = "Mapping"
            mapping_data, mapping_changed, change_reason = self._detect_mapping_changes()
            phase_durations[phase_name] = time.time() - phase_start
            phase_start = time.time()

            sync_type, since_timestamp = self._determine_smart_sync_strategy(mapping_changed, change_reason)

            phase_name = "Shopify"
            variants = self._get_shopify_data(since_timestamp, mapping_data)
            phase_durations[phase_name] = time.time() - phase_start
            phase_start = time.time()

            if not variants:
                logger.info("No data to process - sync is up to date")
                self.state_manager.save_sync_state(sync_timestamp)
                return

            phase_name = "CSV"
            created_files = self._process_variants(sync_type, variants)
            phase_durations[phase_name] = time.time() - phase_start
            phase_start = time.time()

            phase_name = "Upload"
            logger.info("Uploading to Google Drive...")
            
            # Extract current countries from created files
            current_countries = set()
            for file_path in created_files:
                filename = file_path.name if hasattr(file_path, 'name') else str(file_path).split('/')[-1]
                match = re.search(r'country_feed_([A-Z]{2})\.csv$', filename)
                if match:
                    current_countries.add(match.group(1))

            uploaded_count = asyncio.run(
                self.sync_manager.upload_files_with_cleanup(created_files, current_countries, sync_type)
            )

            logger.info(f"Uploaded {uploaded_count} files to Google Drive")
            phase_durations[phase_name] = time.time() - phase_start

            self.state_manager.save_sync_state(sync_timestamp)

            self._log_summary(sync_type, variants, created_files, start_time, change_reason, phase_durations)
            logger.info("--- Sync Completed ---")

        except Exception as e:
            logger.error(f"Smart sync failed: {e}")
            raise
        finally:
            self.country_mapper.cleanup_temp_files()

    def _detect_mapping_changes(self) -> Tuple[Dict, bool, str]:
        """Detect mapping changes and return mapping data."""
        logger.info("Phase 1: Mapping change detection...")
        mapping_data, mapping_changed, change_reason = self.country_mapper.get_mapping_with_change_detection()
        return mapping_data, mapping_changed, change_reason

    def _determine_smart_sync_strategy(self, mapping_changed: bool, change_reason: str) -> Tuple[str, Optional[datetime]]:
        """Determine sync strategy based on mapping changes and previous state."""
        last_sync = self.state_manager.load_sync_state()

        if mapping_changed:
            logger.info(f"FULL SYNC: {change_reason}")
            return "FULL", None
        elif not last_sync:
            logger.info("FULL SYNC: No previous sync state found")
            return "FULL", None
        else:
            logger.info(f"INCREMENTAL SYNC: Mapping unchanged, using state since {last_sync.isoformat()}")
            return "INCREMENTAL", last_sync

    def _run_full_sync(self, force_full=False):
        start_time = time.time()
        sync_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)

        try:
            self._validate_configuration()

            # Force fresh mapping for explicit full sync
            mapping_data, _, _ = self.country_mapper.get_mapping_with_change_detection()

            variants = self._get_shopify_data(None, mapping_data)

            if not variants:
                logger.info("No variants to process")
                return

            created_files = self._process_variants("FULL", variants)
            self._upload_files(created_files)

            self.state_manager.save_sync_state(sync_timestamp)

            self._log_summary("FULL", variants, created_files, start_time, "Explicit full sync requested", {})
            logger.info("--- Full Sync Completed ---")

        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            raise
        finally:
            self.country_mapper.cleanup_temp_files()

    def _get_shopify_data(self, since_timestamp: Optional[datetime], mapping_data: Dict) -> List[Dict]:
        """Fetch data using true bulk operations."""
        logger.info("Fetching data via bulk operations...")
        
        since_iso = None
        if since_timestamp:
            since_iso = since_timestamp.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(f"Incremental bulk fetch since: {since_iso}")
        else:
            logger.info("Full bulk fetch of all products")
        
        # Use new bulk operations method
        jsonl_file_path = self.shopify_sync.bulk_query.get_products_variants_inventory_bulk(since_iso)
        
        if not jsonl_file_path:
            logger.info("No data returned from bulk operation")
            return []
        
        # Process JSONL file
        variants = self.product_loader.load_products_from_bulk_jsonl(
            jsonl_file_path,
            mapping_data["active_countries"], 
            mapping_data["location_country_map"]
        )
        
        # Clean up temporary file
        try:
            Path(jsonl_file_path).unlink()
        except Exception as e:
            logger.warning(f"Failed to clean up temporary file {jsonl_file_path}: {e}")
        
        return variants

    def _process_variants(self, sync_type: str, variants: List[Dict]) -> List[Path]:
        """Process variants."""
        logger.info("Generating CSV files...")

        if sync_type == "FULL":
            logger.info("Creating complete CSV country feeds")
            created_files = self.csv_exporter.create_country_feeds_full(variants)
            self.state_manager.reset_variant_states()
            self.state_manager.update_variant_states(variants)
        else:
            logger.info("Detecting variant changes for incremental update")
            new_variants, changed_variants = self.state_manager.detect_stock_changes(variants)

            if not new_variants and not changed_variants:
                logger.info("No changes detected - CSV files are up to date")
                return []

            created_files = self.csv_exporter.update_country_feeds_incremental(new_variants, changed_variants)
            self.state_manager.update_variant_states(variants)

        return created_files

    def _upload_files(self, created_files: List[Path]):
        """Upload files."""
        if not created_files:
            logger.info("No files to upload")
            return

        if not settings.GOOGLE_DRIVE_FOLDER_ID:
            logger.info("Google Drive not configured - skipping upload")
            return

        logger.info("Phase 4: Uploading to Google Drive...")

        try:
            current_countries = set()
            for file_path in created_files:
                filename = file_path.name if hasattr(file_path, 'name') else str(file_path).split('/')[-1]
                match = re.search(r'country_feed_([A-Z]{2})\.csv$', filename)
                if match:
                    current_countries.add(match.group(1))

            # Use asyncio for parallel uploads
            uploaded_count = asyncio.run(
                self.sync_manager.upload_files_with_cleanup(created_files, current_countries)
            )

            logger.info(f"Uploaded {uploaded_count} files to Google Drive")

        except Exception as e:
            logger.warning(f"Google Drive upload failed: {e}")

    def _validate_configuration(self):
        """Validate configuration with detailed error reporting."""
        logger.info("Validating configuration...")

        config_errors = ConfigValidator.validate_all()
        if config_errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f" - {error}" for error in config_errors)
            raise ValueError(error_msg)

        # Test connectivity if validation passes
        connectivity_errors = ConfigValidator.test_connectivity()
        if connectivity_errors:
            logger.warning("Connectivity issues detected:")
            for error in connectivity_errors:
                logger.warning(f" - {error}")
            # Don't fail on connectivity issues, just warn

        logger.info("Configuration validation passed")

    def cleanup_orphaned_files(self):
        """Manual cleanup of orphaned files."""
        logger.info("--- Manual Orphaned File Cleanup ---")
        self.csv_exporter.manual_cleanup_orphaned_files()

    def refresh_mapping_cache(self):
        """Force refresh mapping cache and clear hash."""
        logger.info("--- Refreshing Mapping Cache ---")
        try:
            # Clear the mapping hash to force change detection
            self.country_mapper.clear_mapping_hash()

            # Get fresh mapping
            mapping_data, mapping_changed, change_reason = self.country_mapper.get_mapping_with_change_detection()

            logger.info("Mapping cache refreshed")
            logger.info(f" - Countries: {len(mapping_data.get('active_countries', {}))}")
            logger.info(f" - Locations: {len(mapping_data.get('location_country_map', {}))}")
            logger.info(f" - Created: {mapping_data.get('created_at', 'Unknown')}")
            logger.info(f" - Change detected: {mapping_changed}")
            logger.info(f" - Reason: {change_reason}")

        except Exception as e:
            logger.error(f"Failed to refresh mapping cache: {e}")
            raise

    def clear_mapping_cache(self):
        """Clear mapping hash to force fresh comparison."""
        logger.info("--- Clearing Mapping Hash ---")
        try:
            self.country_mapper.clear_mapping_hash()
            logger.info("Mapping hash cleared - next sync will detect changes")
        except Exception as e:
            logger.error(f"Failed to clear mapping hash: {e}")
            raise

    def debug_state(self):
        logger.info("--- State Debug Information ---")

        # Database stats
        stats = self.state_manager.get_stats()
        logger.info("Database Statistics:")
        logger.info(f" - Total variants: {stats['variant_count']}")
        logger.info(f" - In stock: {stats['in_stock_count']}")
        logger.info(f" - Out of stock: {stats['out_of_stock_count']}")
        logger.info(f" - Last sync: {stats['last_sync']}")
        logger.info(f" - Database size: {stats['db_size_kb']} KB")

        # Smart mapping info
        mapping_stats = self.country_mapper.get_mapping_stats()
        logger.info("Smart Mapping Status:")
        logger.info(f" - Enabled: {settings.SMART_MAPPING_ENABLED}")
        logger.info(f" - Has previous hash: {mapping_stats['has_previous_hash']}")
        logger.info(f" - Last hash: {mapping_stats['last_hash']}")
        logger.info(f" - Last updated: {mapping_stats['last_updated']}")

        # Configuration info
        logger.info("Configuration:")
        logger.info(f" - Bulk chunk size: {settings.BULK_CHUNK_SIZE}")
        logger.info(f" - Max retries: {settings.MAX_RETRIES}")
        logger.info(f" - Drive max uploads: {settings.DRIVE_MAX_CONCURRENT_UPLOADS}")
        logger.info(f" - Data validation: {settings.ENABLE_DATA_VALIDATION}")

        # Google Drive debug
        if settings.GOOGLE_DRIVE_FOLDER_ID:
            logger.info("Google Drive:")
            self.sync_manager.debug_drive_files()
        else:
            logger.info("Google Drive: Not configured")

    def _log_summary(self, sync_type: str, variants: List[Dict], created_files: List[Path],
                     start_time: float, change_reason: str, phase_durations: Dict[str, float]):
        total_time = time.time() - start_time
        stats = self.csv_exporter.get_export_stats(variants)

        logger.info("=== SYNC SUMMARY ===")
        logger.info(f"Sync Type: {sync_type}")
        logger.info(f"Reason: {change_reason}")
        logger.info(f"Mapping: {'Enabled' if settings.SMART_MAPPING_ENABLED else 'Disabled'}")
        logger.info(f"Total Variants: {stats['total_variants']}")
        logger.info(f"Valid Variants: {stats['valid_variants']}")
        logger.info(f"Validation Failures: {stats['validation_failures']}")
        logger.info(f"Countries: {stats['countries']}")
        logger.info(f"In Stock: {stats['in_stock_variants']}")
        logger.info(f"Out of Stock: {stats['out_of_stock_variants']}")
        logger.info(f"Files Created: {len(created_files)}")
        logger.info(f"Total Time: {total_time:.2f}s")

        if total_time > 0 and phase_durations:
            logger.info("Phase Breakdown:")
            for phase, duration in phase_durations.items():
                percent = (duration / total_time) * 100
                logger.info(f" - {phase}: {duration:.2f}s ({percent:.1f}%)")

        logger.info("=" * 39)
