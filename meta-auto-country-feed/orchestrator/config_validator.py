import logging
import shopify
from pathlib import Path
from typing import List
from config import settings
from output.drive_sync import SyncManager

logger = logging.getLogger(__name__)

class ConfigValidator:
    """Configuration validation with detailed error reporting."""

    @staticmethod
    def validate_all() -> List[str]:
        """Validate all configuration and return list of errors."""
        errors = []

        # Required configuration
        if not settings.SHOPIFY_TOKEN:
            errors.append("SHOPIFY_TOKEN is required")
        elif len(settings.SHOPIFY_TOKEN) < 20:
            errors.append("SHOPIFY_TOKEN appears to be invalid (too short)")

        if not settings.STORE_ID:
            errors.append("STORE_ID is required")
        elif not settings.STORE_ID.replace('-', '').replace('_', '').isalnum():
            errors.append("STORE_ID contains invalid characters")

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
                    errors.append(f"Cannot create {dir_name} directory: {e}")
            elif not dir_path.is_dir():
                errors.append(f"{dir_name} exists but is not a directory: {dir_path}")

        # Numeric configuration validation
        if settings.MAX_RETRIES < 0 or settings.MAX_RETRIES > 10:
            errors.append("MAX_RETRIES must be between 0 and 10")

        if settings.BASE_RETRY_DELAY <= 0 or settings.BASE_RETRY_DELAY > 60:
            errors.append("BASE_RETRY_DELAY must be between 0 and 60 seconds")

        if settings.BULK_CHUNK_SIZE < 100 or settings.BULK_CHUNK_SIZE > 10000:
            errors.append("BULK_CHUNK_SIZE must be between 100 and 10000")

        if settings.DRIVE_MAX_CONCURRENT_UPLOADS < 1 or settings.DRIVE_MAX_CONCURRENT_UPLOADS > 10:
            errors.append("DRIVE_MAX_CONCURRENT_UPLOADS must be between 1 and 10")

        return errors

    @staticmethod
    def test_connectivity() -> List[str]:
        """Test external service connectivity."""
        errors = []

        # Test Shopify connectivity using SDK
        try:
            shopify.Session.setup(api_key="dummy", secret="dummy")
            session = shopify.Session(
                settings.SHOPIFY_SESSION_CONFIG['shop_url'],
                settings.SHOPIFY_SESSION_CONFIG['api_version'],
                settings.SHOPIFY_SESSION_CONFIG['access_token']
            )
            shopify.ShopifyResource.activate_session(session)
            
            # Test with a simple shop query
            shop = shopify.Shop.current()
            if shop:
                logger.info("Shopify connectivity verified via SDK")
            else:
                errors.append("Shopify SDK authentication failed")
        except Exception as e:
            errors.append(f"Shopify SDK connectivity test failed: {e}")

        # Google Drive test
        if settings.GOOGLE_DRIVE_FOLDER_ID and settings.GOOGLE_SERVICE_ACCOUNT_FILE:
            try:
                sync_manager = SyncManager()
                if sync_manager.service:
                    logger.info("Google Drive authentication verified")
                else:
                    errors.append("Google Drive authentication failed")
            except Exception as e:
                errors.append(f"Google Drive connectivity test failed: {e}")

        return errors
