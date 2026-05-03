"""Environment-backed configuration loader."""
import os
from pathlib import Path
from typing import Optional

from core.models import SyncConfig

_loaded_config: Optional[SyncConfig] = None


def _resolve_drive_folder_id(target: str) -> str:
    """Prefer target-specific Drive folder settings, then the shared folder setting."""
    target_key = f"GOOGLE_DRIVE_FOLDER_ID_{target.upper()}"
    return os.getenv(target_key, "") or os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")


def load_config(
    base_dir: Optional[Path] = None,
    target: str = "google",
) -> SyncConfig:
    """
    Build SyncConfig from os.environ.

    Missing variables do not raise here; ConfigValidator.validate_all() enforces requirements.

    Args:
        base_dir: Repository root. Defaults to parent of the core package.
        target: ``google`` or ``meta``. Selects output directory, file extension, and market rules.
    """
    global _loaded_config
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent.parent

    if target == "meta":
        feed_extension = ".csv"
        feed_id_prefix = ""
        output_dir = base_dir / "Meta catalog - country feed updates"
    else:
        feed_extension = ".tsv"
        feed_id_prefix = os.getenv("FEED_ID_PREFIX", "shopify_US_")
        output_dir = base_dir / "Google Merchant - country feed updates"

    cache_root_dir = base_dir / "cache"
    temp_root_dir = base_dir / "temp"
    cache_dir = cache_root_dir / target
    temp_dir = temp_root_dir / target
    cache_root_dir.mkdir(parents=True, exist_ok=True)
    temp_root_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    token = os.getenv("SHOPIFY_TOKEN", "")
    store_id = os.getenv("STORE_ID", "")
    api_version = os.getenv("SHOPIFY_API_VERSION", "2024-07")

    target_str = os.getenv("TARGET_COUNTRIES", "US,CA,AE")
    target_countries = set(c.strip() for c in target_str.split(",") if c.strip())

    if target == "meta":
        restrict = os.getenv("META_FILTER_TARGET_COUNTRIES", "false").lower() in (
            "1",
            "true",
            "yes",
        )
    else:
        restrict = True

    _loaded_config = SyncConfig(
        target=target,
        base_dir=base_dir,
        cache_dir=cache_dir,
        output_dir=output_dir,
        temp_dir=temp_dir,
        state_json_path=cache_dir / "sync_state.json",
        variant_state_json_path=cache_dir / "variant_states.json",
        mapping_comparison_file=cache_dir / "mapping_comparison.json",
        shopify_token=token,
        store_id=store_id,
        shopify_api_version=api_version,
        shopify_session_config={
            "shop_url": f"https://{store_id}.myshopify.com",
            "api_version": api_version,
            "access_token": token,
        },
        google_drive_folder_id=_resolve_drive_folder_id(target),
        google_service_account_file=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
        target_countries=target_countries,
        restrict_markets_to_target_countries=restrict,
        bulk_chunk_size=int(os.getenv("BULK_CHUNK_SIZE", "1000")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        base_retry_delay=float(os.getenv("BASE_RETRY_DELAY", "2.0")),
        csv_buffer_size=int(os.getenv("CSV_BUFFER_SIZE", "65536")),
        smart_mapping_enabled=os.getenv("SMART_MAPPING_ENABLED", "true").lower() == "true",
        enable_data_validation=os.getenv("ENABLE_DATA_VALIDATION", "true").lower() == "true",
        feed_prefix="country_feed_",
        feed_extension=feed_extension,
        feed_id_prefix=feed_id_prefix or "shopify_US_",
        max_retry_delay=float(os.getenv("MAX_RETRY_DELAY", "60.0")),
        max_inventory_threshold=int(os.getenv("MAX_INVENTORY_THRESHOLD", "100000")),
    )
    return _loaded_config


def get_config() -> SyncConfig:
    """Return the config from the last load_config() call."""
    if _loaded_config is None:
        raise RuntimeError("Configuration not loaded. Call load_config() before using the application.")
    return _loaded_config


def clear_loaded_config() -> None:
    """Clear cached config so separate runs can load independent environments."""
    global _loaded_config
    _loaded_config = None
