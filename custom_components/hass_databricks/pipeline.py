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
    ):
        self._config = config
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token
        self._session = session

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
            with open(file_path, "rb") as f:
                async with session.put(url, headers=headers, data=f) as resp:
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

    async def upsert_new_data(self, filename: str):
        """Upsert uploaded csv into target Delta table."""
        statement = f"""
        MERGE INTO `{self._config.catalog}`.`{self._config.schema}`.`{self._config.table}` AS target
        USING (
            SELECT
                TRY_CAST(state AS FLOAT) AS state,
                TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts,
                entity_id
            FROM read_files('{self._config.dbx_path}/{filename}', format => 'csv', header => 'true')
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


def _extract_states_to_csv(
    source_db_path: str,
    output_csv_path: str,
    entity_like: str,
    chunk_size: int,
    min_last_updated_ts: float | None = None,
    use_hot_copy: bool = True,
) -> tuple[int, float | None]:
    """Extract matching states into csv.gz in chunks.

    When use_hot_copy is True, use sqlite3.backup to copy the active DB safely,
    handling WAL mode correctly unlike shutil.copyfile.
    """

    read_db_path = source_db_path
    temp_db_path: str | None = None
    if use_hot_copy:
        # fd, temp_db_path = tempfile.mkstemp(prefix="ha_temp_", suffix=".db")
        # os.close(fd)
        # Using a proper sqlite backup is safer for WAL databases
        _, temp_db_path = tempfile.mkstemp(prefix="ha_temp_", suffix=".db")

        try:
            # Open source in read-only mode for backup
            with sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True) as src:
                with sqlite3.connect(temp_db_path) as dst:
                    src.backup(dst)
            read_db_path = temp_db_path
        except Exception as err:
            if os.path.exists(temp_db_path):
                os.remove(temp_db_path)
            raise Exception(f"Failed to create hot copy of database: {err}") from err

    total_rows = 0
    last_state_id = 0
    max_last_updated_ts: float | None = None

    try:
        connection = sqlite3.connect(f"file:{read_db_path}?mode=ro", uri=True)
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
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    last_state_id = int(rows[-1][0])
                    csv_rows = []
                    for row in rows:
                        row_ts = row[2]
                        if max_last_updated_ts is None or row_ts > max_last_updated_ts:
                            max_last_updated_ts = float(row_ts)
                        csv_rows.append([row[1], row[2], row[3]])

                    writer.writerows(csv_rows)
                    total_rows += len(rows)
                    cursor.execute(
                        query,
                        (last_state_id, effective_min_ts, entity_like, chunk_size),
                    )
        finally:
            connection.close()
    finally:
        if temp_db_path is not None and os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    return total_rows, max_last_updated_ts


async def run_sync_pipeline(request: SyncRequest) -> dict:
    """Run async extraction, upload, and merge in Home Assistant runtime."""

    os.makedirs(request.local_path, exist_ok=True)
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"upload_{export_time}.csv.gz"
    full_path = os.path.join(request.local_path, filename)

    use_hot_copy = (
        request.hot_copy_db
        if request.hot_copy_db is not None
        else request.min_last_updated_ts is None
    )

    # Run the sqlite extraction in an executor since it is blocking disk I/O
    loop = asyncio.get_running_loop()
    rows_written, max_last_updated_ts = await loop.run_in_executor(
        None,
        _extract_states_to_csv,
        request.db_path,
        full_path,
        request.entity_like,
        request.chunk_size,
        request.min_last_updated_ts,
        use_hot_copy,
    )

    if rows_written == 0:
        raise ValueError("No rows were extracted from Home Assistant database.")

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
    )

    await sqlwh.create_schema()
    await sqlwh.create_table()
    upload_state = await sqlwh.upload_to_databricks(full_path, filename)
    upsert_state = await sqlwh.upsert_new_data(filename)

    if not request.keep_local_file and os.path.exists(full_path):
        os.remove(full_path)

    return {
        "filename": filename,
        "rows": rows_written,
        "used_hot_copy": use_hot_copy,
        "max_last_updated_ts": max_last_updated_ts,
        "upload_state": upload_state,
        "upsert_state": upsert_state,
    }
