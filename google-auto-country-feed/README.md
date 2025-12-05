# Google Merchant Country Feeds Automation

Synchronize Shopify product feeds for Google Merchant Center with country-specific availability.

## Features

- Smart Sync: Detects and updates only what's changed
- Full & Incremental Sync: Complete refresh or incremental updates
- Country Mapping: Maps Shopify locations to target countries
- Google Drive Integration: Optional uploads to Google Drive
- State Management: Tracks syncs and product changes
- Configurable: Settings via environment variables

---

## Project Structure

```
main.py                       # Entry point
orchestrator/                 # Core orchestration & validation
shopify_client/               # Shopify API & product loading
mapping/                      # Country-location mapping logic
state_management/             # Sync & variant state tracking
config/                       # Centralized settings
output/                       # Feed files & Drive sync logic
cache/, temp/                 # Working directories
```

---

## Quickstart

### 1. **Install Requirements**

```bash
pip install -r requirements.txt
```

### 2. **Configure Environment**

Copy and edit the example environment file:

```bash
cp project.env.example project.env
```

Edit `project.env` with your credentials and preferences:

```env
# Shopify
SHOPIFY_TOKEN=your_shopify_admin_api_token
STORE_ID=your-store-name
SHOPIFY_API_VERSION=2024-07

# Google Drive (Optional)
GOOGLE_DRIVE_FOLDER_ID=your_google_drive_folder_id
GOOGLE_SERVICE_ACCOUNT_FILE=path/to/service-account.json

# Performance
BULK_CHUNK_SIZE=1000
MAX_RETRIES=3
BASE_RETRY_DELAY=2.0
CSV_BUFFER_SIZE=65536

# Target Countries
TARGET_COUNTRIES=US,CA,AE

# Features
SMART_MAPPING_ENABLED=true
ENABLE_DATA_VALIDATION=true
```

---

### 3. **Run a Sync**

```bash
python main.py [smart|full|incremental|refresh-mapping|debug]
```

| Mode              | Description                                 |
|-------------------|---------------------------------------------|
| `smart`           | Smart sync with change detection (default)  |
| `full`            | Force a full sync of all data               |
| `incremental`     | Only sync new/changed products              |
| `refresh-mapping` | Refresh the country-location mapping cache  |
| `debug`           | Print debug information about the current state |

---

## How It Works

1. **Validate Configuration**: Ensures all required settings and directories exist.
2. **Country Mapping**: Dynamically maps Shopify locations to your target countries.
3. **Product Loading**: Efficiently loads and processes Shopify product data.
4. **Feed Generation**: Exports country-specific feeds in TSV format.
5. **Google Drive Upload**: (Optional) Uploads feeds to Google Drive.
6. **State Management**: Tracks syncs and product changes for efficient updates.

---

## Tips & Troubleshooting

- **Logs**: Check logs for configuration or API errors.
- **Shopify**: Ensure your token and store ID are correct and have API access.
- **Google Drive**: If using, verify your service account and folder ID.
- **Performance**: Adjust chunk sizes and retry settings in your `.env` file for large catalogs.