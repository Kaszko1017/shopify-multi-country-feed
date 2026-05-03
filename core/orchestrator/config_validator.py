"""Validates environment and connectivity before a sync run."""
import logging
from pathlib import Path
from typing import List

import shopify

from core.config import get_config
from core.models import SyncConfig

logger = logging.getLogger(__name__)


class ConfigValidator:
    """Pre-flight checks for environment, paths, and optional services."""

    @staticmethod
    def validate_all(config: SyncConfig = None) -> List[str]:
        if config is None:
            config = get_config()
        errors = []

        if not config.shopify_token:
            errors.append("SHOPIFY_TOKEN is required")
        elif len(config.shopify_token) < 20:
            errors.append("SHOPIFY_TOKEN appears invalid (too short)")

        if not config.store_id:
            errors.append("STORE_ID is required")
        elif not config.store_id.replace("-", "").replace("_", "").isalnum():
            errors.append("STORE_ID contains invalid characters")

        if config.restrict_markets_to_target_countries:
            if not config.target_countries:
                errors.append("TARGET_COUNTRIES must be set (Google / Meta with META_FILTER_TARGET_COUNTRIES)")
            elif len(config.target_countries) > 50:
                errors.append("TARGET_COUNTRIES should be limited")
        elif config.target_countries and len(config.target_countries) > 50:
            errors.append("TARGET_COUNTRIES should be limited (when set)")

        if config.google_drive_folder_id or config.google_service_account_file:
            if config.google_drive_folder_id and not config.google_service_account_file:
                errors.append("GOOGLE_SERVICE_ACCOUNT_FILE required when GOOGLE_DRIVE_FOLDER_ID is set")
            elif config.google_service_account_file and not config.google_drive_folder_id:
                errors.append("GOOGLE_DRIVE_FOLDER_ID required when GOOGLE_SERVICE_ACCOUNT_FILE is set")
            elif config.google_drive_folder_id and len(config.google_drive_folder_id) < 10:
                errors.append("GOOGLE_DRIVE_FOLDER_ID appears invalid")
            elif config.google_service_account_file and not Path(config.google_service_account_file).exists():
                logger.warning(
                    "Service account file not found: %s – feed files will be generated locally only, no Drive upload",
                    config.google_service_account_file,
                )

        for dir_name, dir_path in [
            ("CACHE_DIR", config.cache_dir),
            ("OUTPUT_DIR", config.output_dir),
            ("TEMP_DIR", config.temp_dir),
        ]:
            if not dir_path.exists():
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    errors.append(f"Cannot create {dir_name}: {e}")
            elif not dir_path.is_dir():
                errors.append(f"{dir_name} is not a directory: {dir_path}")

        if config.max_retries < 0 or config.max_retries > 10:
            errors.append("MAX_RETRIES must be between 0 and 10")
        if config.base_retry_delay <= 0 or config.base_retry_delay > 60:
            errors.append("BASE_RETRY_DELAY must be between 0 and 60")
        if config.bulk_chunk_size < 100 or config.bulk_chunk_size > 10000:
            errors.append("BULK_CHUNK_SIZE must be between 100 and 10000")

        return errors

    @staticmethod
    def test_connectivity(config: SyncConfig = None) -> List[str]:
        if config is None:
            config = get_config()
        errors = []
        try:
            shopify.Session.setup(api_key="dummy", secret="dummy")
            session = shopify.Session(
                config.shopify_session_config["shop_url"],
                config.shopify_session_config["api_version"],
                config.shopify_session_config["access_token"],
            )
            shopify.ShopifyResource.activate_session(session)
            shop = shopify.Shop.current()
            if not shop:
                errors.append("Shopify authentication failed")
            else:
                logger.debug("Shopify connectivity verified")
        except Exception as e:
            errors.append(f"Shopify connectivity failed: {e}")

        if (
            config.google_drive_folder_id
            and config.google_service_account_file
            and Path(config.google_service_account_file).exists()
        ):
            try:
                from core.output.drive_sync import SyncManager
                sync_manager = SyncManager()
                if not sync_manager.service:
                    errors.append("Google Drive authentication failed")
                else:
                    logger.debug("Google Drive authentication verified")
            except Exception as e:
                errors.append(f"Google Drive connectivity test failed: {e}")

        return errors
