import os
from pathlib import Path
from typing import Dict

# Base paths
BASE_DIR = Path(__file__).parent.parent
CACHE_DIR = BASE_DIR / "cache"
OUTPUT_DIR = BASE_DIR / "Meta catalog - country feed updates"
TEMP_DIR = BASE_DIR / "temp"

# Ensure directories exist
for directory in [CACHE_DIR, OUTPUT_DIR, TEMP_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Shopify Configuration
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "")
STORE_ID = os.getenv("STORE_ID", "")
SHOPIFY_API_VERSION=os.getenv("SHOPIFY_API_VERSION", "2024-07")

# Validate required Shopify settings
if not SHOPIFY_TOKEN or not STORE_ID:
    raise ValueError("SHOPIFY_TOKEN and STORE_ID environment variables are required")

# Build Shopify session configuration
SHOPIFY_SESSION_CONFIG = {
    'shop_url': f"https://{STORE_ID}.myshopify.com",
    'api_version': '2024-07',
    'access_token': SHOPIFY_TOKEN
}

# Google Drive Configuration (optional)
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")

# Performance Configuration
BULK_CHUNK_SIZE = int(os.getenv("BULK_CHUNK_SIZE", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_RETRY_DELAY = float(os.getenv("BASE_RETRY_DELAY", "2.0"))
MAX_RETRY_DELAY = float(os.getenv("MAX_RETRY_DELAY", "60.0"))
CSV_BUFFER_SIZE = int(os.getenv("CSV_BUFFER_SIZE", "65536"))
DRIVE_MAX_CONCURRENT_UPLOADS = int(os.getenv("DRIVE_MAX_CONCURRENT_UPLOADS", "3"))

# Bulk Query Configuration
BULK_QUERY_MAX_BATCH_SIZE = int(os.getenv("BULK_QUERY_MAX_BATCH_SIZE", "250"))

# Data Validation
ENABLE_DATA_VALIDATION = os.getenv("ENABLE_DATA_VALIDATION", "true").lower() == "true"
MAX_INVENTORY_THRESHOLD = int(os.getenv("MAX_INVENTORY_THRESHOLD", "100000"))

# Smart Mapping Configuration
SMART_MAPPING_ENABLED = os.getenv("SMART_MAPPING_ENABLED", "true").lower() == "true"

# File paths
MAPPING_COMPARISON_FILE = CACHE_DIR / "mapping_comparison.json"
STATE_JSON_PATH = CACHE_DIR / "sync_state.json"
VARIANT_STATE_JSON_PATH = CACHE_DIR / "variant_states.json"

# CSV Export Configuration
FEED_PREFIX = "country_feed_"
FEED_EXTENSION = ".csv"
