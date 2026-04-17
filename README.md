# HASS Databricks

[![Tests](https://github.com/kasztp/hass_databricks/actions/workflows/run-tests.yml/badge.svg)](https://github.com/kasztp/hass_databricks/actions/workflows/run-tests.yml)
[![Pylint](https://github.com/kasztp/hass_databricks/actions/workflows/pylint.yml/badge.svg)](https://github.com/kasztp/hass_databricks/actions/workflows/pylint.yml)
[![Hassfest](https://github.com/kasztp/hass_databricks/actions/workflows/hassfest.yml/badge.svg)](https://github.com/kasztp/hass_databricks/actions/workflows/hassfest.yml)
[![HACS](https://github.com/kasztp/hass_databricks/actions/workflows/hacs.yml/badge.svg)](https://github.com/kasztp/hass_databricks/actions/workflows/hacs.yml)
[![Codecov](https://codecov.io/gh/kasztp/hass_databricks/branch/main/graph/badge.svg)](https://codecov.io/gh/kasztp/hass_databricks)

![HASS Databricks](custom_components/hass_databricks/brand/logo.png)

Tools to sync Home Assistant state history to Databricks as a Home Assistant custom integration.

## Project Architecture

This repository contains two separate but related codebases that share a common goal: syncing Home Assistant state data to Databricks.

### Home Assistant Custom Integration (`custom_components/hass_databricks/`)

A self-contained Home Assistant integration designed to run safely on resource-constrained HAOS devices (e.g., Raspberry Pi, Home Assistant Green). It uses only lightweight, built-in dependencies (`sqlite3`, `csv`, `gzip`, `aiohttp`) and implements a non-blocking, streaming micro-batch pipeline optimized for the Home Assistant event loop.

Key files:

- `custom_components/hass_databricks/manifest.json`
- `custom_components/hass_databricks/__init__.py`
- `custom_components/hass_databricks/services.yaml`
- `custom_components/hass_databricks/pipeline.py`

HACS metadata is provided in:

- `hacs.json`

### Standalone Python Package (`hass_databricks/`) — ⚠️ Deprecated

> **Warning:** This package is deprecated. All active development has moved to the Home Assistant custom integration above. The standalone package is retained for reference only and will be removed in a future release.

The original standalone Python package was intended for use in Notebooks or as a CLI tool. It has been superseded by the integration's streaming micro-batch pipeline, which offers superior performance, non-blocking I/O, and immediate resource cleanup — all without requiring additional dependencies such as `pandas` or `pydantic`.

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

### Databricks Workspace Configuration

Before connecting the integration to your Databricks environment, ensure the following setup is complete in your workspace:


1. **Setup Target Volume**: Create a volume within the desired Catalog and Schema. This will act as a transient staging directory during micro-batch synchronization. Ensure you record the exact Volume path (e.g., `/Volumes/main/home_assistant/ingest`) to use during setup.
2. **Generate Access Token**: Create a Personal Access Token (PAT) via *User Settings → Developer → Access Tokens* or configure a Service Principal. This token requires `CREATE TABLE`, `CREATE SCHEMA`, and `WRITE VOLUME` privileges within your target catalog.
3. **Acquire Compute Details**: The integration utilizes Databricks REST API endpoints for performing cluster-side MERGE aggregations. Retrieve both the **Server Hostname** and the **HTTP Path** from your target SQL Warehouse or designated compute cluster via its *Connection Details* tab.

### Setup via Home Assistant UI

1. Go to **Settings** → **Devices & Services**
2. Click **Add Integration**
3. Search for **HASS Databricks**
4. Fill in the required connection parameters

The integration will attempt to connect to your Databricks SQL Warehouse. If the warehouse is in an idle state (sleeping), the setup will automatically **retry for up to 30 seconds** to allow it to wake up.

### Reconfiguration

If you need to update your Access Token or Move to a different SQL Warehouse:

1. Go to **Settings** → **Devices & Services**
2. Find the **HASS Databricks** card
3. Click the three dots menu → **Reconfigure**
4. Update the necessary fields and click **Submit**

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

1. Connects strictly as a read-only client to the active Home Assistant SQLite DB, extracting rows in micro-batch chunks sequentially to avoid long-running WAL lock contention.
2. Iteratively writes these chunks to transient local `csv.gz` buffers.
3. Instantly uploads each buffered chunk to a tracking folder inside Databricks Volumes.
4. Deletes the local chunk buffer immediately (prohibiting memory creep and massive local storage exhaustion).
5. Once all chunks are ingested, triggers a MERGE operation on the cluster side for high-performance ingestion into the target table.
6. Maps Databricks APIs to safely delete the transient volume tracking folder on completion.

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
```

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

> [!NOTE]
> These sensors are assigned to the **Diagnostic** category and appear in the Diagnostic section of the device page in the Home Assistant UI.

Sensors are marked unavailable if the most recent sync failed (reconnect when sync succeeds).

### Manual Trigger

- `button.databricks_sync`: A manual trigger entity that allows you to start a synchronization run immediately from the Home Assistant UI.

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

## Known Limitations

- **Schema Evolution**: The integration handles basic schema creation but does not automatically perform `ALTER TABLE` operations if the Home Assistant database schema changes significantly.
- **Micro-Batching**: While performance is high, sync is not real-time. Data is pushed in batches according to your configured interval.
- **SQL Warehouse Only**: While it may work with standard All-Purpose clusters, it is optimized for and tested against **Databricks SQL Warehouses** using the SQL Statements API.

## CI / Validation

This repository includes GitHub Actions workflows for Home Assistant ecosystem checks:

- HACS Action: `.github/workflows/hacs.yml`
- Hassfest: `.github/workflows/hassfest.yml`

## Development

This project uses `uv` for dependency management alongside `ruff` for rapid code linting and formatting.

### Developer Setup

To ensure strict code quality, configure your GitHub pre-commit hooks, which contain automated `ruff` bindings, immediately after downloading the dependencies:

```bash
uv sync --extra test
uv run pre-commit install
```

This ensures that `ruff --fix` and `ruff-format` protect your commits natively.

### Running Tests

Execute the standardized Pytest suite leveraging your isolated `uv` environment:

```bash
uv run pytest -q
```
