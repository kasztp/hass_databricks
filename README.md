# hass_databricks

![HASS Databricks](custom_components/hass_databricks/brand/logo.png)

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

## Installation

### Via HACS (Home Assistant Community Store)

1. Navigate to **Settings** → **Devices & Services** → **Custom Repositories**
2. Add repository URL: `https://github.com/kasztp/hass_databricks`
3. Select category: **Integration**
4. Click **Create Repository**
5. Go to **HACS** → **Integrations** → Search for "HASS Databricks"
6. Click **Install**
7. Restart Home Assistant

### Manual Installation

1. Download the repository: `git clone https://github.com/kasztp/hass_databricks`
2. Copy `custom_components/hass_databricks/` to `<config>/custom_components/`
3. Restart Home Assistant

## Configuration

### Setup via Home Assistant UI

1. Go to **Settings** → **Devices & Services**
2. Click **Add Integration**
3. Search for **HASS Databricks**
4. Fill in the required connection parameters

### Configuration Parameters

**Required (Initial Setup):**
- **Server Hostname**: Databricks workspace hostname (e.g., `adb-xxxx.azuredatabricks.net`)
- **HTTP Path**: SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/xxxx`)
- **Access Token**: Databricks personal access token (create in Databricks workspace)
- **Catalog**: Target Databricks catalog name (e.g., `main`)
- **Schema**: Target schema in the catalog (e.g., `ha`)
- **Table**: Target table for synced data (e.g., `sensor_states`)
- **Databricks Volumes Path**: Staging path for csv.gz files (e.g., `/Volumes/main/ha/ingest`)

**Optional (Adjustable in Integration Options):**
- **Entity Filter** (default: `sensor.%`): SQL LIKE pattern to filter entities (e.g., `sensor.%`, `climate.%`, `%temperature%`)
- **Chunk Size** (default: `50000`): Number of rows extracted per batch (1,000–500,000)
- **Keep Local File** (default: `false`): Retain local csv.gz file after upload for debugging
- **Local Staging Path** (default: `/tmp/`): Directory for temporary staging
- **Enable Automatic Sync** (default: `true`): Schedule periodic syncs
- **Sync Interval** (default: `60` minutes): How often automatic syncs run (1–1440 minutes)

### Environment Variables

Databricks credentials can also be read from environment variables (if not configured via UI):
- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`

## What The Sync Service Does

The service hass_databricks.sync:

1. Safely creates a "hot copy" of the live Home Assistant SQLite DB using `sqlite3.backup` to properly handle WAL and avoid locking issues.
2. Reads data from the copy with sqlite3 to avoid locking the live DB.
3. Extracts rows in chunks from SQLite to keep memory usage bounded.
4. Writes a compressed csv.gz file locally.
5. Uploads the file to a Databricks Volume.
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

Availability is also logged:
- When a sync fails, a warning is logged once (repeated failures do not spam logs)
- When the service reconnects after a failure, an info message is logged

### Event Bus

The integration fires event `hass_databricks_sync_result` on each run with run metadata.

### Sensors

The integration exposes the following entities:

- `sensor.hass_databricks_last_run`: Last sync status with attributes (target, rows, error, since)
- `sensor.hass_databricks_last_rows`: Number of rows in the last successful sync
- `sensor.hass_databricks_last_success`: Timestamp of the last successful sync

Sensors are marked unavailable if the most recent sync failed (reconnect when sync succeeds).

## Removal

To remove the integration:

1. Go to **Settings** → **Devices & Services**
2. Find **HASS Databricks** in the list
3. Click the three dots menu → **Delete**
4. Confirm deletion

If installed via HACS, also uninstall from **HACS** → **Integrations** → **HASS Databricks** → **Uninstall**.

If manually installed, remove the directory `<config>/custom_components/hass_databricks/` and restart Home Assistant.

## Troubleshooting

**Symptoms: Sensors show unavailable**
- **Cause**: Most recent sync failed
- **Solution**: Check Home Assistant logs for error details. Common causes: invalid credentials, unreachable Databricks server, missing database file
- **Recovery**: Fix the underlying issue and trigger a manual sync via the service or wait for the next automatic sync. Sensors will become available upon success.

**Symptoms: "Sync dependencies unavailable"**
- **Cause**: Integration dependencies not installed
- **Solution**: Ensure your network is active and HA meets baseline built-in package needs. (Previously, this plugin required heavy dependencies, but it now uses pure Python `csv`, `gzip`, and `aiohttp` to run natively!)

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
