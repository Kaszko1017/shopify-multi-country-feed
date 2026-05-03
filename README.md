# Shopify Multi-Country Feed System

Thesis project: unified architecture for generating country-specific product feeds from Shopify for **Google Merchant Center** (TSV) and **Meta/Facebook** (CSV) catalogs.

## What it does

- Connects to a Shopify store via Admin API and GraphQL bulk operations.
- Builds a country/location mapping from markets, locations, and delivery profiles.
- Fetches product variants and inventory; maps inventory by location to target countries.
- Generates per-country feed files (TSV for Google, CSV for Meta).
- Optionally uploads feed files to a Google Drive folder.
- Tracks sync state and variant state for incremental updates.

## Supported targets

| Target  | Output format | Output folder | Which countries get feeds |
|---------|----------------|---------------|---------------------------|
| `google` | TSV (id, availability) | `Google Merchant - country feed updates/` | Only countries listed in **`TARGET_COUNTRIES`** (intersected with Shopify markets). |
| `meta`   | CSV (id, override, availability) | `Meta catalog - country feed updates/` | **All countries** returned from Shopify **Markets**. `TARGET_COUNTRIES` is only applied when `META_FILTER_TARGET_COUNTRIES=true`. |

## Architecture

```
.
├── README.md
├── main.py                 # Single CLI entrypoint
├── requirements.txt
├── project.env.example     # Copy to project.env and fill in
├── .devcontainer.json      # Dev container (env from project.env)
├── Dockerfile              # Container image for sync
├── docker-compose.yml     # Run sync with volumes for cache/feeds
├── .dockerignore
├── core/
│   ├── config/             # load_config(), no import-time failure
│   ├── orchestrator/       # ConfigValidator, SyncOrchestrator
│   ├── shopify/            # ShopifySync, bulk query, product loader
│   ├── mapping/            # CountryLocationMapper (hash includes mapping structure)
│   ├── state/              # StateManager (atomic JSON writes)
│   ├── output/             # Drive sync (TSV/CSV)
│   ├── models/             # SyncConfig, VariantSnapshot, etc.
│   └── utils/              # atomic_write_json
└── targets/
    ├── google/
    │   └── exporter.py     # TSVExporter
    └── meta/
        └── exporter.py     # CSVExporter
```

## Commands

From the project root (with `project.env` or environment variables set):

```bash
# Smart sync (change detection; full or incremental)
python main.py smart --target google
python main.py smart --target meta

# Force full sync
python main.py full --target google
python main.py full --target meta

# Incremental (uses last sync state; falls back to smart if no state)
python main.py incremental --target google

# Refresh mapping cache (clear hash and refetch mapping)
python main.py refresh-mapping --target google

# Debug (state stats, mapping stats, target countries; no config validation)
python main.py debug --target google
```

- **smart**: Detects mapping changes; if mapping changed or no previous sync, runs full sync; otherwise incremental.
- **full**: Always full sync with fresh mapping.
- **incremental**: Same as smart (uses last sync timestamp for bulk query).
- **refresh-mapping**: Clears stored mapping hash and refetches mapping (useful after store config changes).
- **debug**: Prints state and mapping info; does not validate Shopify/Drive config.

## Configuration

1. Copy `project.env.example` to `project.env` (or set environment variables).
2. Set required values:
   - `SHOPIFY_TOKEN` – Shopify Admin API access token
   - `STORE_ID` – store subdomain (e.g. `my-store` for my-store.myshopify.com)
   - **`TARGET_COUNTRIES`** – **Required for `--target google`**: comma-separated ISO codes (e.g. `US,CA,AE`). Feeds are only built for those countries that also appear in your Shopify markets.
   - For **`--target meta`**, `TARGET_COUNTRIES` is optional by default (countries come from Shopify markets only). Set `META_FILTER_TARGET_COUNTRIES=true` if you want Meta to use the same allow-list as Google.

Optional:

- `META_FILTER_TARGET_COUNTRIES` – set to `true` for Meta to restrict mapping to `TARGET_COUNTRIES` (default: `false`).
- `SHOPIFY_API_VERSION` – default `2024-07`; used consistently for session and GraphQL.
- `FEED_ID_PREFIX` – for Google TSV only (default `shopify_US_`); used as prefix for feed item IDs.
- `GOOGLE_DRIVE_FOLDER_ID_GOOGLE`, `GOOGLE_DRIVE_FOLDER_ID_META`, and `GOOGLE_SERVICE_ACCOUNT_FILE` – if set, feed files are uploaded to the target-specific folder after sync; on full sync, orphaned feed files in that same target folder are trashed.
- Backward-compatible fallback: `GOOGLE_DRIVE_FOLDER_ID` is used when the target-specific variable is not set.
- Performance: `BULK_CHUNK_SIZE`, `MAX_RETRIES`, `BASE_RETRY_DELAY`, `CSV_BUFFER_SIZE`, etc. (see `project.env.example`).

Config is loaded when you run `main.py`; validation runs before sync commands (not for `debug`). Missing required env vars cause validation to fail; they do not cause import-time errors.

## Output files

- **Google**: `Google Merchant - country feed updates/country_feed_XX.tsv` – TSV with columns `id`, `availability`.
- **Meta**: `Meta catalog - country feed updates/country_feed_XX.csv` – CSV with columns `id`, `override`, `availability`.

Runtime state/cache/temp are target-scoped, while output directories remain target-specific:

- Google state/temp: `cache/google/`, `temp/google/`
- Meta state/temp: `cache/meta/`, `temp/meta/`

## Optional Google Drive upload

If a target Drive folder ID (`GOOGLE_DRIVE_FOLDER_ID_GOOGLE` / `GOOGLE_DRIVE_FOLDER_ID_META`, or fallback `GOOGLE_DRIVE_FOLDER_ID`) and `GOOGLE_SERVICE_ACCOUNT_FILE` are set:

- After each sync, generated feed files are uploaded to the given folder (create or update by name).
- On full sync, feed files in that folder for countries not in the current run are trashed (orphan cleanup).
- Incremental sync does not perform orphan cleanup.

## Dev container and Docker

### Dev container (VS Code / Cursor)

- **`.devcontainer.json`** at the repo root uses the Python 3.12 dev container image, installs deps from `requirements.txt`, and loads `project.env` via `runArgs` and `DOTENV_CONFIG_PATH`.
- Open the repo in VS Code/Cursor and run **“Reopen in Container”** so the environment matches.

### Docker Compose

- **`Dockerfile`**: Python 3.12 slim image, installs `requirements.txt`, copies `main.py`, `core/`, `targets/`, and `project.env.example`. Default command: `smart --target google`.
- **`docker-compose.yml`**: one service `sync` with `env_file: project.env`, volumes for `cache`, `temp`, and both feed output folders.

Create `project.env` from `project.env.example` in the repo root (same directory as `docker-compose.yml`). Then:

```bash
# Build and run default (smart sync, Google target)
docker-compose run --rm sync

# Override command and target
docker-compose run --rm sync full --target google
docker-compose run --rm sync smart --target meta
docker-compose run --rm sync refresh-mapping --target google
docker-compose run --rm sync debug --target meta
```

Feed files end up in Docker volumes `google-feeds` and `meta-feeds`; list or inspect with `docker volume ls` and `docker volume inspect szakdoga_google-feeds` (prefix may vary by project name).

## Development notes

- **Secrets**: Do not commit `project.env` or service account JSON files. Use `project.env.example` as a template only.
- **Python**: Use a virtualenv; install with `pip install -r requirements.txt`.
- **Mapping hash**: The mapping change detection hashes the full location→countries structure (sorted), not only country/location key lists.
- **State and mapping files**: Writes are atomic (temp file + replace) for sync state, variant state, and mapping hash.
- **Temp cleanup**: Only the bulk result file produced by the current run is deleted; no global purge of all `bulk_result_*.jsonl` files.

## Testing the system

### 1. Install dependencies

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure the environment

```bash
cp project.env.example project.env
# Edit project.env: set SHOPIFY_TOKEN, STORE_ID, TARGET_COUNTRIES
```

### 3. Quick checks (no Shopify/Drive needed)

- **Help and CLI parsing**
  ```bash
  python main.py
  python main.py --help
  python main.py debug --target google
  python main.py debug --target meta
  ```
  `debug` does not validate Shopify or Drive; it only prints state/mapping stats (and will show zeros if you’ve never run a sync).

- **Config validation (will fail until env is set)**  
  Run any sync command without a valid `project.env` to see validation errors:
  ```bash
  python main.py smart --target google
  # Expect: "Configuration validation failed" and list of missing/invalid vars
  ```

### 4. Full test (real Shopify store)

With valid `SHOPIFY_TOKEN`, `STORE_ID`, and `TARGET_COUNTRIES` in `project.env`:

1. **Refresh mapping only** (no feed generation):
   ```bash
   python main.py refresh-mapping --target google
   ```
   Checks markets, locations, delivery profiles; writes mapping hash to `cache/<target>/`.

2. **Full sync** (generates feed files):
   ```bash
   python main.py full --target google
   # Output: Google Merchant - country feed updates/country_feed_XX.tsv
   python main.py full --target meta
   # Output: Meta catalog - country feed updates/country_feed_XX.csv
   ```

3. **Smart sync** (incremental if mapping unchanged):
   ```bash
   python main.py smart --target google
   ```

4. **Optional: Google Drive**  
   Set `GOOGLE_DRIVE_FOLDER_ID_GOOGLE`, `GOOGLE_DRIVE_FOLDER_ID_META`, and `GOOGLE_SERVICE_ACCOUNT_FILE` in `project.env`; after a sync, feed files are uploaded to the target's folder.

### 5. What to inspect

- `cache/<target>/sync_state.json` – last sync timestamp.
- `cache/<target>/variant_states.json` – per-variant stock state (after at least one full/incremental run).
- `cache/<target>/mapping_comparison.json` – mapping hash for change detection.
- `Google Merchant - country feed updates/*.tsv` or `Meta catalog - country feed updates/*.csv` – generated feeds.

## Current implementation scope

- Recovery checkpoints and cache maintenance are handled through the existing state files and sync commands.
- Google Drive uploads run sequentially, which keeps folder updates deterministic and easy to audit.
- Feed generation targets the formats required by the project: TSV for Google Merchant Center and CSV for Meta catalogs.
