"""End-to-end sync workflow: validate, map, fetch, export, upload, persist state."""
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from core.config import get_config
from core.utils.io import unlink_bulk_jsonl
from core.state.state_manager import StateManager
from core.shopify.shopify_sync import ShopifySync
from core.shopify.product_loader import ProductLoader
from core.mapping.country_location_mapper import CountryLocationMapper
from core.orchestrator.config_validator import ConfigValidator
from core.output.drive_sync import SyncManager

import logging

logger = logging.getLogger(__name__)


class SyncOrchestrator:
    """Coordinates mapping, bulk fetch, feed export, state, and optional Drive upload."""

    def __init__(
        self,
        state_manager: Optional[StateManager] = None,
        shopify_sync: Optional[ShopifySync] = None,
        product_loader: Optional[ProductLoader] = None,
        country_mapper: Optional[CountryLocationMapper] = None,
        sync_manager: Optional[SyncManager] = None,
        exporter=None,
    ):
        self.shopify_sync = shopify_sync or ShopifySync()
        self.state_manager = state_manager or StateManager()
        self.product_loader = product_loader or ProductLoader()
        self.country_mapper = country_mapper or CountryLocationMapper(self.shopify_sync)
        self.sync_manager = sync_manager or SyncManager()
        self.exporter = exporter

    def run_smart(self):
        logger.info("--- Starting Sync ---")
        return self._run_smart_sync_with_detection()

    def run_full(self):
        logger.info("--- Starting Full Sync ---")
        return self._run_full_sync(force_full=True)

    def run_incremental(self):
        logger.info("--- Starting Incremental Sync ---")
        last_sync = self.state_manager.load_sync_state()
        if not last_sync:
            logger.info("No previous sync - using full sync")
            return self.run_smart()
        return self._run_smart_sync_with_detection()

    def _run_smart_sync_with_detection(self):
        start_time = time.time()
        sync_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)
        cfg = get_config()

        if not self.exporter:
            raise RuntimeError("No exporter configured; pass --target google or --target meta.")

        try:
            self._validate_configuration()
            mapping_data, mapping_changed, change_reason = self._detect_mapping_changes()
            sync_type, since_timestamp = self._determine_smart_sync_strategy(mapping_changed, change_reason)
            variants = self._get_shopify_data(since_timestamp, mapping_data)

            if not variants:
                logger.info("No data to process - sync is up to date")
                self.state_manager.save_sync_state(sync_timestamp)
                return

            created_files = self._process_variants(sync_type, variants)

            if created_files:
                logger.info(f"Uploading {len(created_files)} feed files to Google Drive")
                self.sync_manager.upload_files_with_cleanup(
                    created_files, self._countries_for_drive_cleanup(created_files), sync_type
                )

            self.state_manager.save_sync_state(sync_timestamp)
            self._log_summary(sync_type, variants, created_files, start_time, change_reason)
            logger.info("--- Sync Completed ---")

        except Exception as e:
            logger.error(f"Smart sync failed: {e}")
            raise

    def _countries_for_drive_cleanup(self, created_files: List[Path]) -> Set[str]:
        """Country codes to retain in Drive: from config when filtering markets, else from written filenames."""
        cfg = get_config()
        if cfg.restrict_markets_to_target_countries:
            return set(cfg.target_countries)
        codes = set()
        pattern = re.compile(r"country_feed_([A-Z]{2})\.(tsv|csv)$")
        for fp in created_files:
            m = pattern.search(fp.name)
            if m:
                codes.add(m.group(1))
        return codes if codes else set(cfg.target_countries)

    def _detect_mapping_changes(self) -> Tuple[Dict, bool, str]:
        return self.country_mapper.get_mapping_with_change_detection()

    def _determine_smart_sync_strategy(
        self, mapping_changed: bool, change_reason: str
    ) -> Tuple[str, Optional[datetime]]:
        last_sync = self.state_manager.load_sync_state()
        if mapping_changed:
            logger.info(f"FULL SYNC: {change_reason}")
            return "FULL", None
        if not last_sync:
            logger.info("FULL SYNC: No previous sync state")
            return "FULL", None
        logger.debug(f"INCREMENTAL SYNC: since {last_sync.isoformat()}")
        return "INCREMENTAL", last_sync

    def _run_full_sync(self, force_full: bool = False):
        start_time = time.time()
        sync_timestamp = datetime.now(timezone.utc) - timedelta(seconds=5)
        cfg = get_config()

        try:
            self._validate_configuration()
            mapping_data, _, _ = self.country_mapper.get_mapping_with_change_detection()
            variants = self._get_shopify_data(None, mapping_data)

            if not variants:
                logger.info("No variants to process")
                return

            created_files = self._process_variants("FULL", variants)

            if created_files:
                self.sync_manager.upload_files_with_cleanup(
                    created_files, self._countries_for_drive_cleanup(created_files), "FULL"
                )

            self.state_manager.save_sync_state(sync_timestamp)
            self._log_summary("FULL", variants, created_files, start_time, "Explicit full sync")
            logger.info("--- Full Sync Completed ---")

        except Exception as e:
            logger.error(f"Full sync failed: {e}")
            raise

    def _get_shopify_data(
        self, since_timestamp: Optional[datetime], mapping_data: Dict
    ) -> List[Dict]:
        since_iso = None
        if since_timestamp:
            since_iso = since_timestamp.astimezone(timezone.utc).replace(microsecond=0).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            logger.debug(f"Incremental fetch since: {since_iso}")
        else:
            logger.debug("Full fetch of all products")

        jsonl_file_path = self.shopify_sync.bulk_query.get_products_variants_inventory_bulk(
            since_iso
        )
        if not jsonl_file_path:
            logger.debug("No data from bulk operation")
            return []

        try:
            return self.product_loader.load_products_from_bulk_jsonl(
                jsonl_file_path,
                mapping_data["active_countries"],
                mapping_data["location_country_map"],
            )
        finally:
            unlink_bulk_jsonl(jsonl_file_path)

    def _process_variants(self, sync_type: str, variants: List[Dict]) -> List[Path]:
        if sync_type == "FULL":
            created_files = self.exporter.create_country_feeds_full(variants)
            self.state_manager.reset_variant_states()
            self.state_manager.update_variant_states(variants)
        else:
            new_variants, changed_variants = self.state_manager.detect_stock_changes(variants)
            if not new_variants and not changed_variants:
                logger.info("No changes detected - feeds are up to date")
                return []
            created_files = self.exporter.update_country_feeds_incremental(
                new_variants, changed_variants
            )
            self.state_manager.update_variant_states(variants)
        return created_files

    def _validate_configuration(self):
        config_errors = ConfigValidator.validate_all()
        if config_errors:
            raise ValueError(
                "Configuration validation failed:\n"
                + "\n".join(f" - {e}" for e in config_errors)
            )
        connectivity_errors = ConfigValidator.test_connectivity()
        if connectivity_errors:
            for error in connectivity_errors:
                logger.warning(f" - {error}")
        logger.debug("Configuration validation passed")

    def refresh_mapping_cache(self):
        logger.info("--- Refreshing Mapping Cache ---")
        try:
            self.country_mapper.clear_mapping_hash()
            mapping_data, mapping_changed, change_reason = (
                self.country_mapper.get_mapping_with_change_detection()
            )
            logger.info("Mapping cache refreshed")
            logger.info(f" - Countries: {len(mapping_data.get('active_countries', {}))}")
            logger.info(f" - Locations: {len(mapping_data.get('location_country_map', {}))}")
            logger.info(f" - Change detected: {mapping_changed}")
            logger.info(f" - Reason: {change_reason}")
        except Exception as e:
            logger.error(f"Failed to refresh mapping: {e}")
            raise

    def debug_state(self):
        cfg = get_config()
        logger.info("--- Debug State Information ---")
        stats = self.state_manager.get_stats()
        logger.info("State Statistics:")
        logger.info(f" - Total variants: {stats['variant_count']}")
        logger.info(f" - In stock: {stats['in_stock_count']}")
        logger.info(f" - Out of stock: {stats['out_of_stock_count']}")
        logger.info(f" - Last sync: {stats['last_sync']}")
        logger.info(f" - Database size: {stats['db_size_kb']} KB")
        mapping_stats = self.country_mapper.get_mapping_stats()
        logger.info("Mapping Status:")
        logger.info(f" - Enabled: {cfg.smart_mapping_enabled}")
        logger.info(f" - Has previous hash: {mapping_stats['has_previous_hash']}")
        logger.info(f" - Last hash: {mapping_stats['last_hash']}")
        if cfg.restrict_markets_to_target_countries:
            logger.info(f"Market filter (TARGET_COUNTRIES): {sorted(cfg.target_countries)}")
        else:
            logger.info(
                "Markets: all Shopify regions (TARGET_COUNTRIES not applied to mapping). "
                f"TARGET_COUNTRIES in env: {sorted(cfg.target_countries) or '—'}"
            )

    def _log_summary(
        self,
        sync_type: str,
        variants: List[Dict],
        created_files: List[Path],
        start_time: float,
        change_reason: str,
    ):
        cfg = get_config()
        total_time = time.time() - start_time
        stats = self.exporter.get_export_stats(variants)
        logger.info("=== SYNC SUMMARY ===")
        logger.info(f"Sync Type: {sync_type}")
        logger.info(f"Reason: {change_reason}")
        if cfg.restrict_markets_to_target_countries:
            logger.info(f"Market filter (TARGET_COUNTRIES): {sorted(cfg.target_countries)}")
        else:
            logger.info(
                "Markets: all Shopify regions. TARGET_COUNTRIES (env, informational): "
                f"{sorted(cfg.target_countries) or '—'}"
            )
        logger.info(f"Total Variants: {stats.get('total_variants', len(variants))}")
        logger.info(f"Countries: {stats.get('countries', 'N/A')}")
        logger.info(f"In Stock: {stats.get('in_stock_variants', 'N/A')}")
        logger.info(f"Out of Stock: {stats.get('out_of_stock_variants', 'N/A')}")
        logger.info(f"Files Created: {len(created_files)}")
        logger.info(f"Total Time: {total_time:.2f}s")
        logger.info("=" * 27)
