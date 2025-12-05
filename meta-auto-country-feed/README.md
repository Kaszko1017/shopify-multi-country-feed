# Shopify Country Feed Sync

A Docker-containerized Python application that synchronizes Shopify product data into country-specific CSV feeds and uploads them to Google Drive.

## Key Features

- Sync Detection: Detects mapping changes and adjusts sync strategy
- Recovery System: Resume from checkpoints on failure
- Country-Based Feeds: Generates separate CSV feeds for each active country/market
- Google Drive Integration: Upload, update, and cleanup of orphaned files
- Multiple Sync Modes: Smart, full, and incremental synchronization
- Docker Support: Containerized with docker-compose
- Streaming Processing: Configurable chunk sizes for large datasets
- Environment Configuration: Settings via environment variables

## Prerequisites

- Docker and Docker Compose
- Shopify Store with API access
- Google Drive API credentials (optional, for automatic uploads)
- Python 3.10+ (if running without Docker)

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/Kaszko1017/shopify-multi-country-feed.git
cd shopify-multi-country-feed/meta-auto-country-feed
```

### 2. Configure Environment

Create your environment file from the template:
```bash
cp project.env.example project.env
```

Edit `project.env` with your credentials:

```env
# Shopify Configuration
STORE_ID=your-store-name
SHOPIFY_TOKEN=your-shopify-access-token
SHOPIFY_API_VERSION=2025-04

# Google Drive Configuration (Optional)
GOOGLE_DRIVE_FOLDER_ID=your-google-drive-folder-id
GOOGLE_SERVICE_ACCOUNT_FILE=path/to/service-account.json
```

### 3. Run with Docker

```bash
# Smart sync (recommended for most use cases)
docker-compose run --rm sync smart

# Or run in background
docker-compose up -d
```

### 4. Monitor Logs

```bash
docker-compose logs -f sync
```

## Configuration

### Environment Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `STORE_ID` | Yes | Your Shopify store identifier | - |
| `SHOPIFY_TOKEN` | Yes | Shopify API access token | - |
| `SHOPIFY_API_VERSION` | No | Shopify API version | `2025-04` |
| `GOOGLE_DRIVE_FOLDER_ID` | No | Target Google Drive folder ID | - |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | No | Path to Google service account JSON | - |
| `SMART_MAPPING_ENABLED` | No | Enable intelligent mapping detection | `true` |
| `MAX_RETRIES` | No | Maximum retry attempts | `3` |
| `BULK_CHUNK_SIZE` | No | Processing chunk size | `1500` |

### Google Drive Setup

1. Create a Google Cloud Project
2. Enable the Google Drive API
3. Create a Service Account and download the JSON key
4. Share your target Google Drive folder with the service account email
5. Set `GOOGLE_SERVICE_ACCOUNT_FILE` to the path of your JSON key

## Usage

### Command Line Interface

```bash
# Smart sync - automatically detects changes and chooses strategy
python main.py smart

# Force full sync - regenerates all feeds regardless of changes  
python main.py full

# Incremental sync - only processes changes since last run
python main.py incremental

# Utility commands
python main.py debug           # Show system state and statistics
python main.py cleanup         # Manual cleanup of orphaned files
python main.py refresh-mapping # Force refresh mapping cache
python main.py clear-cache     # Clear mapping cache to force detection
python main.py clear-checkpoint # Clear recovery checkpoint
```

### Docker Commands

```bash
# Smart sync
docker-compose run --rm sync smart

# Full sync  
docker-compose run --rm sync full

# Debug system state
docker-compose run --rm sync debug

# View logs
docker-compose logs sync
```

## Project Structure

```
meta-auto-country-feed/
├── main.py                        # Main orchestrator and CLI entry point
├── config/
│   └── settings.py                # Configuration management
├── orchestrator/
│   ├── sync_orchestrator.py       # Sync orchestration logic
│   ├── config_validator.py        # Configuration validation and connectivity
│   └── usage.py                   # Usage/help logic
├── shopify_client/
│   ├── shopify_sync.py            # Shopify API integration using SDK
│   ├── product_loader.py          # Product data processing and streaming
│   ├── shopify_bulk_query.py      # Bulk GraphQL queries
│   └── bulk_operations_handler.py # Bulk operations handler
├── mapping/
│   └── country_location_mapper.py # Smart country-location mapping
├── output/
│   ├── csv_exporter.py            # CSV generation and export logic
│   └── drive_sync.py              # Google Drive operations
├── state_management/
│   └── state_manager.py           # JSON-based state management
├── requirements.txt               # Python dependencies
├── dockerfile                     # Container definition
├── docker-compose.yml             # Container orchestration
├── project.env                    # Environment variables
├── cache/                         # State and mapping cache files
├── temp/                          # Temporary processing files
└── Meta catalog - country feed updates/  # Generated CSV output
```

## Sync Modes

### Smart Sync (Recommended)
```bash
docker-compose run --rm sync smart
```
- Detects mapping changes and chooses sync strategy
- Performs full sync when necessary
- Uses incremental sync when mapping is unchanged

### Full Sync
```bash
docker-compose run --rm sync full
```
- Processes all products regardless of changes
- Clears existing state and rebuilds from scratch
- Use cases: Initial setup, major configuration changes, data corruption recovery

### Incremental Sync
```bash
docker-compose run --rm sync incremental
```
- Only processes new and modified variants
- Minimal API calls and processing time
- Requires previous sync state to function

## Recovery System

The application includes a recovery system that saves checkpoints during sync operations:

### Automatic Recovery
```bash
# If a sync fails, simply re-run the same command
docker-compose run --rm sync smart
# Will automatically resume from the last successful checkpoint
```

### Manual Recovery Management
```bash
# Clear recovery checkpoint to start fresh
docker-compose run --rm sync clear-checkpoint

# Check current system state
docker-compose run --rm sync debug
```

### Recovery Stages
1. Mapping Complete: Country-location mapping built
2. Shopify Complete: Product data fetched from Shopify
3. CSV Complete: Country CSV files generated
4. Upload Complete: Files uploaded to Google Drive

## Environment Variables Reference

### Core Configuration
```env
# Shopify API Configuration
STORE_ID=your-store-name                    # Required: Shopify store identifier
SHOPIFY_TOKEN=shppa_xxxxx                   # Required: Private app access token
SHOPIFY_API_VERSION=2025-04                 # Optional: API version

# Google Drive Configuration  
GOOGLE_DRIVE_FOLDER_ID=1ABC123xyz           # Optional: Target folder ID
GOOGLE_SERVICE_ACCOUNT_FILE=credentials.json # Optional: Service account file path
```

### Advanced Configuration
```env
# Smart Mapping
SMART_MAPPING_ENABLED=true                  # Enable intelligent change detection

# Performance Tuning
BULK_CHUNK_SIZE=1500                       # Processing chunk size
MAX_RETRIES=3                              # Maximum retry attempts
BASE_RETRY_DELAY=1.0                       # Initial retry delay (seconds)
DRIVE_MAX_CONCURRENT_UPLOADS=3             # Parallel upload limit

# Data Validation
ENABLE_DATA_VALIDATION=true                # Enable comprehensive data validation
MAX_INVENTORY_THRESHOLD=100000             # Sanity check for inventory levels

# Directory Paths
CACHE_DIR=cache                            # Cache directory path
OUTPUT_DIR=Meta catalog - country feed updates # CSV output directory
TEMP_DIR=temp                              # Temporary files directory
```

## Monitoring and Debugging

### Performance Monitoring
```bash
# View detailed performance breakdown
docker-compose run --rm sync debug

# Monitor logs in real-time
docker-compose logs -f sync
```

### Common Debug Commands
```bash
# Check mapping status
docker-compose run --rm sync refresh-mapping

# Verify configuration
docker-compose run --rm sync debug

# Manual cleanup
docker-compose run --rm sync cleanup
```

## Troubleshooting

### Common Issues

#### Authentication Errors
```bash
# Verify Shopify credentials
curl -H "X-Shopify-Access-Token: YOUR_TOKEN" \
     https://YOUR_STORE.myshopify.com/admin/api/2025-04/shop.json
```

#### Google Drive Permission Issues
- Ensure service account email has access to the target folder
- Verify `GOOGLE_SERVICE_ACCOUNT_FILE` path is correct
- Check that the Google Drive API is enabled in your Google Cloud Project

#### Sync Failures
```bash
# Clear checkpoint and start fresh
docker-compose run --rm sync clear-checkpoint

# Check system state
docker-compose run --rm sync debug

# Review logs for specific errors
docker-compose logs sync
```

#### Performance Issues
- Increase `BULK_CHUNK_SIZE` for faster processing (max: 10000)
- Adjust `DRIVE_MAX_CONCURRENT_UPLOADS` based on bandwidth
- Monitor memory usage with large product catalogs

### Log Levels
The application provides detailed logging with timestamps and performance metrics:

```
2025-01-07 10:30:15 - INFO - Phase 1: Smart mapping change detection...
2025-01-07 10:30:16 - INFO - INCREMENTAL SYNC: Mapping unchanged, using state since 2025-01-07T09:15:23
2025-01-07 10:30:17 - INFO - Phase 2: Streaming Shopify data...
2025-01-07 10:30:25 - INFO - Processed 1,250 products via Shopify SDK
2025-01-07 10:30:30 - INFO - Created 15 country-specific CSV feeds
2025-01-07 10:30:35 - INFO - Uploaded 15 files to Google Drive
```

## Development

### Local Development Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Run directly
python main.py smart
```

### Testing Configuration
```bash
# Validate configuration without running sync
python -c "from orchestrator.config_validator import ConfigValidator; print('Config valid' if not ConfigValidator.validate_all() else 'Config errors')"
```

### Code Structure
- **main.py**: Central orchestrator with CLI interface and recovery logic
- **orchestrator/sync_orchestrator.py**: Sync orchestration logic
- **orchestrator/config_validator.py**: Configuration validation and connectivity
- **mapping/country_location_mapper.py**: Intelligent mapping with change detection
- **shopify_client/product_loader.py**: Streaming product data processing
- **output/csv_exporter.py**: High-performance CSV generation
- **output/drive_sync.py**: Google Drive API operations with concurrent uploads
- **state_management/state_manager.py**: JSON-based state management
- **config/settings.py**: Configuration management
