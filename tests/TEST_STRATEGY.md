# Automated test strategy

## Layers

| Layer | Location | Role |
|-------|----------|------|
| Unit | `tests/unit/` | Pure or isolated behavior: config, validation, mapping hash, market filtering, state, loaders, exporters |
| Component | `tests/component/` | Orchestrator decisions, Drive adapter, bulk handler with mocks |
| CLI | `tests/cli/` | `main.main(argv=...)` dispatch, exit codes, target selection |
| Integration | `tests/integration/` | End-to-end collaboration between loader, exporter, and state management using representative bulk JSONL data and controlled Shopify/Drive boundaries |

## Targeted coverage (priority modules)

The test suite focuses on project-owned behavior and keeps external services behind controlled boundaries:

- **`country_location_mapper`**: `_parse_active_countries` edge cases, `_parse_locations_from_jsonl`, `_create_country_mapping` (delivery vs address fallback, inactive locations, delivery API errors), `get_mapping_stats` / `clear_mapping_hash`, corrupt comparison file, smart-mapping-disabled cache path, full `get_mapping_with_change_detection` first-run vs stable-hash reasons (with `unlink_bulk_jsonl` stubbed).
- **`drive_sync`**: MIME types, upload success counting when one file fails, create vs update branch, orphan cleanup list/trash, empty `current_countries` early exit, import failure → no service, `_verify_folder_access` shared-drive fallback.
- **`shopify_bulk_query`**: `execute_query` success/failure, product bulk query string with/without `since_timestamp`, `get_location_country_relationships` GraphQL errors / malformed `data` / happy-path aggregation.
- **`config_validator`**: `validate_all()` defaulting to `get_config()`, cache `mkdir` failure, `test_connectivity` Shopify success / no shop / exception, Drive branch when `SyncManager.service` is missing.

## Risks covered

- Wrong or partial configuration reaching Shopify/export (validator + load_config defaults)
- Mapping change detection false positives/negatives (canonical JSON + sorted structure hashing)
- State corruption or missing files breaking sync (graceful reads, atomic writes via exercised code paths)
- Target mix-ups (Google TSV vs Meta CSV naming and headers)
- CLI regressions (commands, `--target`, debug skipping validation, exit codes)

## Architectural claims validated

- Single entrypoint (`main.py`) and injectable `argv` / `_project_root()` for tests
- Shared `core/` vs `targets/google` and `targets/meta` split (exporter tests + CLI target tests)
- `SyncOrchestrator` dependency seams (`StateManager`, `ShopifySync`, `ProductLoader`, `CountryLocationMapper`, `SyncManager`, exporter)
- Config load is lazy and separate from import; `clear_loaded_config()` enables isolation

## ProductLoader / incremental validation

The incremental integration test verifies that a second bulk export containing an additional in-stock variant is recognized as a state change and written to the country feed. This keeps the scenario focused on incremental export behavior while `ProductLoader` unit tests cover stock availability mapping separately.

## Manual validation scope

- Live Shopify Admin API access, rate limits, and store-specific permissions
- Live Google Drive permissions and shared-drive administration
- Production secret management and deployment environment setup
- Merchant Center and Meta catalog policy review beyond generated schema checks

## Running

```bash
pytest
```

Coverage (requires `pytest-cov`, listed in `requirements.txt`):

```bash
pip install -r requirements.txt
pytest --cov=core --cov=targets --cov=main --cov-report=term-missing
```

If you see `unrecognized arguments: --cov=...`, install the plugin: `pip install pytest-cov`.
