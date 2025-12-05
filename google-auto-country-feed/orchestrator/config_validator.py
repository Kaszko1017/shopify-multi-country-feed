import logging
import shopify
from pathlib import Path
from typing import List
from config import settings
from output.drive_sync import SyncManager

logger = logging.getLogger(__name__)

class ConfigValidator:

    @staticmethod
    def validate_all() -> List[str]:
        errors = []

        # Required Shopify config
        if not settings.SHOPIFY_TOKEN:
            errors.append("SHOPIFY_TOKEN is required")
        elif len(settings.SHOPIFY_TOKEN) < 20:
            errors.append("SHOPIFY_TOKEN appears invalid (too short)")

        if not settings.STORE_ID:
            errors.append("STORE_ID is required")
        elif not settings.STORE_ID.replace('-', '').replace('_', '').isalnum():
            errors.append("STORE_ID contains invalid characters")

        # Target countries validation
        if not settings.TARGET_COUNTRIES:
            errors.append("TARGET_COUNTRIES must be set")
        elif len(settings.TARGET_COUNTRIES) > 10:
            errors.append("TARGET_COUNTRIES should be limited")

        # Google Drive configuration (optional but if provided, must be complete)
        if settings.GOOGLE_DRIVE_FOLDER_ID or settings.GOOGLE_SERVICE_ACCOUNT_FILE:
            if not settings.GOOGLE_DRIVE_FOLDER_ID:
                errors.append("GOOGLE_DRIVE_FOLDER_ID required when service account is configured")
            elif len(settings.GOOGLE_DRIVE_FOLDER_ID) < 10:
                errors.append("GOOGLE_DRIVE_FOLDER_ID appears to be invalid")

            if not settings.GOOGLE_SERVICE_ACCOUNT_FILE:
                errors.append("GOOGLE_SERVICE_ACCOUNT_FILE required when folder ID is configured")
            elif not Path(settings.GOOGLE_SERVICE_ACCOUNT_FILE).exists():
                errors.append(f"Service account file not found: {settings.GOOGLE_SERVICE_ACCOUNT_FILE}")

        # Directory validation
        for dir_name, dir_path in [
            ('CACHE_DIR', settings.CACHE_DIR),
            ('OUTPUT_DIR', settings.OUTPUT_DIR),
            ('TEMP_DIR', settings.TEMP_DIR)
        ]:
            if not dir_path.exists():
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    errors.append(f"Cannot create {dir_name}: {e}")
            elif not dir_path.is_dir():
                errors.append(f"{dir_name} is not a directory: {dir_path}")

        # Performance validation
        if settings.MAX_RETRIES < 0 or settings.MAX_RETRIES > 10:
            errors.append("MAX_RETRIES must be between 0 and 10")

        if settings.BASE_RETRY_DELAY <= 0 or settings.BASE_RETRY_DELAY > 60:
            errors.append("BASE_RETRY_DELAY must be between 0 and 60")

        if settings.BULK_CHUNK_SIZE < 100 or settings.BULK_CHUNK_SIZE > 10000:
            errors.append("BULK_CHUNK_SIZE must be between 100 and 10000")

        return errors

    @staticmethod
    def test_connectivity() -> List[str]:
        """Test connectivity."""
        errors = []

        # Test Shopify
        try:
            shopify.Session.setup(api_key="dummy", secret="dummy")
            session = shopify.Session(
                settings.SHOPIFY_SESSION_CONFIG['shop_url'],
                settings.SHOPIFY_SESSION_CONFIG['api_version'],
                settings.SHOPIFY_SESSION_CONFIG['access_token']
            )
            shopify.ShopifyResource.activate_session(session)
            
            shop = shopify.Shop.current()
            if shop:
                logger.debug("Shopify connectivity verified")
            else:
                errors.append("Shopify authentication failed")
        except Exception as e:
            errors.append(f"Shopify connectivity failed: {e}")

        # Google Drive test
        if settings.GOOGLE_DRIVE_FOLDER_ID and settings.GOOGLE_SERVICE_ACCOUNT_FILE:
            try:
                sync_manager = SyncManager()
                if sync_manager.service:
                    logger.debug("Google Drive authentication verified")
                else:
                    errors.append("Google Drive authentication failed")
            except Exception as e:
                errors.append(f"Google Drive connectivity test failed: {e}")

        return errors
