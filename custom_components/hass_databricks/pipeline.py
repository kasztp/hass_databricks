"""Async sync pipeline for extracting HA data and uploading to Databricks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import shutil
import sqlite3
import tempfile
import asyncio
from typing import Optional
import csv
import gzip
import aiohttp
from typing import Any


@dataclass
class SyncRequest:
    """Service request parameters for sync."""

    db_path: str
    server_hostname: str
    http_path: str
    access_token: str
    catalog: str
    schema: str
    table: str
    local_path: str
    dbx_volumes_path: str
    entity_like: str
    chunk_size: int
    keep_local_file: bool
    hot_copy_db: bool | None = None
    min_last_updated_ts: float | None = None
    session: aiohttp.ClientSession | None = None
    hass: Any | None = None


def _read_file_content(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _remove_path(path: str) -> None:
    if os.path.exists(path):
        try:
            if os.path.isdir(path):
                os.rmdir(path)
            else:
                os.remove(path)
        except OSError:
            pass


def _makedirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)


async def _async_run_job(hass: Any | None, func, *args):
    """Run a blocking job in the HA executor or default event loop."""
    if hass:
        return await hass.async_add_executor_job(func, *args)
    return await asyncio.get_running_loop().run_in_executor(None, func, *args)


@dataclass
class RuntimeSyncConfig:
    """Runtime config used by the async sync pipeline."""

    catalog: str
    schema: str
    table: str
    local_path: str
    dbx_path: str


class DatabricksTarget:
    """Databricks upload and merge helper for Home Assistant runtime."""

    def __init__(
        self,
        config: RuntimeSyncConfig,
        *,
        server_hostname: str,
        http_path: str,
        access_token: str,
        session: aiohttp.ClientSession | None = None,
        hass: Any | None = None,
    ):
        self._config = config
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token
        self._session = session
        self._hass = hass

    async def _execute_sql(self, statement: str) -> dict:
        """Execute a SQL statement using the Databricks REST API asynchronously."""
        close_session = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            url = f"https://{self._server_hostname}/api/2.0/sql/statements"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

            warehouse_id = self._http_path.strip("/").split("/")[-1]

            payload = {
                "warehouse_id": warehouse_id,
                "statement": statement,
                "wait_timeout": "30s",
            }
            async with session.post(url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                result = await resp.json()
                if result.get("status", {}).get("state") in ("FAILED", "CANCELED"):
                    raise Exception(f"SQL execution failed: {result}")
                return result
        finally:
            if close_session:
                await session.close()

    async def _upload_file(self, file_path: str, databricks_path: str) -> dict:
        """Upload a file to Databricks Volumes using the Files API asynchronously."""
        close_session = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            url = f"https://{self._server_hostname}/api/2.0/fs/files{databricks_path}?overwrite=true"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/octet-stream",
            }
            file_bytes = await _async_run_job(self._hass, _read_file_content, file_path)
            async with session.put(url, headers=headers, data=file_bytes) as resp:
                resp.raise_for_status()
                text = await resp.text()
                return {"status": resp.status, "response": text}
        finally:
            if close_session:
                await session.close()

    async def delete_volume_folder(self, databricks_path: str) -> dict:
        """Delete a folder from Databricks Volumes asynchronously."""
        close_session = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            url = f"https://{self._server_hostname}/api/2.0/fs/directories{databricks_path}"
            headers = {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }
            async with session.delete(url, headers=headers) as resp:
                resp.raise_for_status()
                text = await resp.text()
                return {"status": resp.status, "response": text}
        finally:
            if close_session:
                await session.close()

    async def create_schema(self):
        """Create target schema when it does not exist."""
        statement = f"CREATE SCHEMA IF NOT EXISTS `{self._config.catalog}`.`{self._config.schema}`"
        return await self._execute_sql(statement)

    async def create_table(self):
        """Create target table when it does not exist."""
        statement = f"""
        CREATE TABLE IF NOT EXISTS `{self._config.catalog}`.`{self._config.schema}`.`{self._config.table}` (
            state FLOAT,
            last_updated_ts TIMESTAMP,
            entity_id STRING
        )
        USING DELTA
        PARTITIONED BY (entity_id)
        """
        return await self._execute_sql(statement)

    async def upload_to_databricks(self, input_full_path: str, filename: str):
        """Upload local csv.gz file to Databricks Volume."""
        databricks_path = f"{self._config.dbx_path}/{filename}"
        return await self._upload_file(input_full_path, databricks_path)

    async def upsert_new_data(self, source_path: str):
        """Upsert uploaded csv into target Delta table using wildcards or explicit paths."""
        statement = f"""
        MERGE INTO `{self._config.catalog}`.`{self._config.schema}`.`{self._config.table}` AS target
        USING (
            SELECT
                TRY_CAST(state AS FLOAT) AS state,
                TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts,
                entity_id
            FROM read_files('{source_path}', format => 'csv', header => 'true')
        ) AS source
        ON
            target.entity_id = source.entity_id
        AND
            target.last_updated_ts = source.last_updated_ts
        WHEN MATCHED THEN
            UPDATE SET
                target.state = source.state,
                target.last_updated_ts = source.last_updated_ts
        WHEN NOT MATCHED THEN
            INSERT (state, last_updated_ts, entity_id)
            VALUES (source.state, source.last_updated_ts, source.entity_id)
        """
        return await self._execute_sql(statement)


def _extract_chunk_to_csv(
    source_db_path: str,
    output_csv_path: str,
    entity_like: str,
    chunk_size: int,
    min_last_updated_ts: float | None = None,
    last_state_id: int = 0,
) -> tuple[int, float | None, int]:
    """Extract matching states into csv.gz for a single chunk window.

    Native RO transaction closes completely after returning to unblock WAL checkpoints.
    """

    try:
        connection = sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True)
        try:
            cursor = connection.cursor()

            # Detect the correct metadata table name (state_metadata in new HA, states_meta in older)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('state_metadata', 'states_meta')"
            )
            meta_table_row = cursor.fetchone()
            if not meta_table_row:
                # If neither exists, check if 'states' exists to provide a better error
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='states'"
                )
                if not cursor.fetchone():
                    raise sqlite3.OperationalError(
                        "Home Assistant database schema not found (missing 'states' table). Ensure recorder has initialized."
                    )
                raise sqlite3.OperationalError(
                    "Home Assistant metadata table not found (missing 'state_metadata' or 'states_meta')."
                )

            meta_table = meta_table_row[0]

            query = f"""
            SELECT
                s.state_id,
                s.state,
                s.last_updated_ts,
                sm.entity_id
            FROM states AS s
            JOIN {meta_table} AS sm
                ON s.metadata_id = sm.metadata_id
            WHERE
                s.state_id > ?
                AND s.last_updated_ts >= ?
                AND sm.entity_id LIKE ?
                AND s.state NOT IN ('unknown', 'unavailable')
            ORDER BY s.state_id ASC
            LIMIT ?
            """

            effective_min_ts = (
                float(min_last_updated_ts) if min_last_updated_ts is not None else 0.0
            )

            with gzip.open(output_csv_path, "wt", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["state", "last_updated_ts", "entity_id"])

                cursor.execute(
                    query, (last_state_id, effective_min_ts, entity_like, chunk_size)
                )
                rows = cursor.fetchall()
                if not rows:
                    return 0, None, last_state_id

                max_last_updated_ts: float | None = None
                next_state_id = int(rows[-1][0])
                csv_rows = []
                for row in rows:
                    row_ts = row[2]
                    if max_last_updated_ts is None or row_ts > max_last_updated_ts:
                        max_last_updated_ts = float(row_ts)
                    csv_rows.append([row[1], row[2], row[3]])

                writer.writerows(csv_rows)
                total_rows = len(rows)

        finally:
            connection.close()
    except Exception as err:
        raise Exception(f"Failed to query database state chunk: {err}") from err

    return total_rows, max_last_updated_ts, next_state_id


async def run_sync_pipeline(request: SyncRequest) -> dict:
    """Run async extraction, upload, and merge in isolated micro-batches."""

    await _async_run_job(request.hass, _makedirs, request.local_path)
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    job_id = f"upload_{export_time}"
    job_local_path = os.path.join(request.local_path, job_id)
    await _async_run_job(request.hass, _makedirs, job_local_path)

    dbx_job_path = f"{request.dbx_volumes_path}/{job_id}"

    runtime_config = RuntimeSyncConfig(
        catalog=request.catalog,
        schema=request.schema,
        table=request.table,
        local_path=request.local_path,
        dbx_path=request.dbx_volumes_path,
    )
    sqlwh = DatabricksTarget(
        runtime_config,
        server_hostname=request.server_hostname,
        http_path=request.http_path,
        access_token=request.access_token,
        session=request.session,
        hass=request.hass,
    )

    await sqlwh.create_schema()
    await sqlwh.create_table()

    loop = asyncio.get_running_loop()
    total_rows = 0
    global_max_ts: float | None = None
    last_state_id = 0
    chunk_index = 0

    while True:
        chunk_index += 1
        filename = f"part_{chunk_index:05d}.csv.gz"
        chunk_output_path = os.path.join(job_local_path, filename)

        rows_extracted, max_ts, next_state_id = await loop.run_in_executor(
            None,
            _extract_chunk_to_csv,
            request.db_path,
            chunk_output_path,
            request.entity_like,
            request.chunk_size,
            request.min_last_updated_ts,
            last_state_id,
        )

        if rows_extracted == 0:
            await _async_run_job(request.hass, _remove_path, chunk_output_path)
            break

        total_rows += rows_extracted
        if max_ts is not None:
            if global_max_ts is None or max_ts > global_max_ts:
                global_max_ts = max_ts
        last_state_id = next_state_id

        # Upload
        await sqlwh._upload_file(chunk_output_path, f"{dbx_job_path}/{filename}")

        # Cleanup local chunk immediately
        if not request.keep_local_file:
            await _async_run_job(request.hass, _remove_path, chunk_output_path)

    if total_rows == 0:
        await _async_run_job(request.hass, _remove_path, job_local_path)
        raise ValueError("No rows were extracted from Home Assistant database.")

    # Finish: Merge all chunks dynamically mapped explicitly
    upsert_state = await sqlwh.upsert_new_data(f"{dbx_job_path}/*.csv.gz")

    # Cleanup Databricks Volume explicit ingest folder
    try:
        await sqlwh.delete_volume_folder(dbx_job_path)
    except Exception:
        pass

    # Cleanup local Job Dir
    await _async_run_job(request.hass, _remove_path, job_local_path)

    return {
        "filename": job_id,
        "rows": total_rows,
        "used_hot_copy": False,
        "max_last_updated_ts": global_max_ts,
        "upload_state": {"status": "SUCCESS"},
        "upsert_state": upsert_state,
    }
