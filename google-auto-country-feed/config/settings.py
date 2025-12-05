import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
CACHE_DIR = BASE_DIR / "cache"
OUTPUT_DIR = BASE_DIR / "Google Merchant - country feed updates"
TEMP_DIR = BASE_DIR / "temp"

# Ensure directories exist
for directory in [CACHE_DIR, OUTPUT_DIR, TEMP_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Shopify Configuration
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "")
STORE_ID = os.getenv("STORE_ID", "")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-07")

if not SHOPIFY_TOKEN or not STORE_ID:
    raise ValueError("SHOPIFY_TOKEN and STORE_ID environment variables are required")

SHOPIFY_SESSION_CONFIG = {
    'shop_url': f"https://{STORE_ID}.myshopify.com",
    'api_version': SHOPIFY_API_VERSION,
    'access_token': SHOPIFY_TOKEN
}

# Google Drive Configuration (optional)
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")

# Target Countries
TARGET_COUNTRIES_STR = os.getenv("TARGET_COUNTRIES", "US,CA,AE")
TARGET_COUNTRIES = set(country.strip() for country in TARGET_COUNTRIES_STR.split(",") if country.strip())

# Performance Configuration
BULK_CHUNK_SIZE = int(os.getenv("BULK_CHUNK_SIZE", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_RETRY_DELAY = float(os.getenv("BASE_RETRY_DELAY", "2.0"))
CSV_BUFFER_SIZE = int(os.getenv("CSV_BUFFER_SIZE", "65536"))

# Features
SMART_MAPPING_ENABLED = os.getenv("SMART_MAPPING_ENABLED", "true").lower() == "true"
ENABLE_DATA_VALIDATION = os.getenv("ENABLE_DATA_VALIDATION", "true").lower() == "true"

# File paths
MAPPING_COMPARISON_FILE = CACHE_DIR / "mapping_comparison.json"
STATE_JSON_PATH = CACHE_DIR / "sync_state.json"
VARIANT_STATE_JSON_PATH = CACHE_DIR / "variant_states.json"

# Feed Export Configuration
FEED_PREFIX = "country_feed_"
FEED_EXTENSION = ".tsv"
