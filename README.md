# hass_databricks

Tools to sync Home Assistant state history to Databricks as a Home Assistant custom integration.

## Home Assistant Custom Component

This repository now includes a Home Assistant custom integration in:

- custom_components/hass_databricks/

Key files:

- custom_components/hass_databricks/manifest.json
- custom_components/hass_databricks/__init__.py
- custom_components/hass_databricks/services.yaml
- custom_components/hass_databricks/pipeline.py

HACS metadata is provided in:

- hacs.json

## What The Sync Service Does

The service hass_databricks.sync:

1. Copies the live Home Assistant SQLite DB to a temporary file using shutil.copyfile.
2. Reads data from the copy with sqlite3 to avoid locking the live DB.
3. Extracts rows in chunks from SQLite to keep memory usage bounded.
4. Writes a parquet file locally.
5. Uploads the parquet to a Databricks Volume.
6. Runs MERGE into the target table.

### Incremental Sync Behavior

After the first successful run, the integration stores the last successful source timestamp.
Subsequent runs only extract rows where `last_updated_ts >= (last_success - 10 minutes)`.

This 10-minute overlap is intentional to avoid missing late/edge updates. Duplicates are handled by the MERGE condition in Databricks.

## Service Usage

Example service call data:

```yaml
service: hass_databricks.sync
data:
	catalog: main
	schema: ha
	table: sensor_states
	dbx_volumes_path: /Volumes/main/ha/ingest
	local_path: /tmp/
	db_path: /config/home-assistant_v2.db
	entity_like: sensor.%
	chunk_size: 50000
	keep_local_file: false
	hot_copy_db: true
```

`hot_copy_db` is optional. If omitted, the integration uses:

- `true` for initial syncs (no prior successful watermark)
- `false` for incremental syncs

Databricks credentials are read from environment variables:

- DATABRICKS_SERVER_HOSTNAME
- DATABRICKS_HTTP_PATH
- DATABRICKS_TOKEN

For the Home Assistant integration path, the same values are configured in the integration UI (Config Entry).

## Observability In Home Assistant

Each sync run records:

- Success/failure
- Rows uploaded
- Target table
- Filename
- Error (if failed)

### Activity / Logbook

The integration writes run results to the Home Assistant Logbook (Activity view) using the name `HASS Databricks`.

### Event Bus

The integration fires event `hass_databricks_sync_result` on each run with run metadata.

### Sensors

The integration exposes the following entities:

- `sensor.hass_databricks_last_run`
- `sensor.hass_databricks_last_rows`
- `sensor.hass_databricks_last_success`

`sensor.hass_databricks_last_run` includes extra attributes such as `last_target`, `last_trigger`, `last_error`, and incremental `last_since` watermark.

## CI / Validation

This repository includes GitHub Actions workflows for Home Assistant ecosystem checks:

- HACS Action: `.github/workflows/hacs.yml`
- Hassfest: `.github/workflows/hassfest.yml`

## Development

This project uses uv:

```bash
uv sync --extra test
uv run pytest -q
```
