"""Blocking sync pipeline for extracting HA data and uploading to Databricks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import shutil
import sqlite3
import tempfile
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from databricks import sql


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


PARQUET_SCHEMA = pa.schema(
    [
        ("state", pa.string()),
        ("last_updated_ts", pa.float64()),
        ("entity_id", pa.string()),
    ]
)


@dataclass
class RuntimeSyncConfig:
    """Runtime config used by the blocking sync pipeline."""

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
    ):
        self._config = config
        self._server_hostname = server_hostname
        self._http_path = http_path
        self._access_token = access_token

    def _connect(self):
        return sql.connect(
            server_hostname=self._server_hostname,
            http_path=self._http_path,
            access_token=self._access_token,
            staging_allowed_local_path=self._config.local_path,
        )

    def create_schema(self):
        """Create target schema when it does not exist."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE SCHEMA IF NOT EXISTS `{self._config.catalog}`.`{self._config.schema}`
                    """
                )
                return cursor.fetchall()

    def create_table(self):
        """Create target table when it does not exist."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self._config.catalog}`.`{self._config.schema}`.`{self._config.table}` (
                        state FLOAT,
                        last_updated_ts TIMESTAMP,
                        entity_id STRING
                    )
                    USING DELTA
                    PARTITIONED BY (entity_id)
                    """
                )
                return cursor.fetchall()

    def upload_to_databricks(self, input_full_path: str, filename: str):
        """Upload local parquet file to Databricks Volume."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"PUT '{input_full_path}' INTO '{self._config.dbx_path}/{filename}' OVERWRITE"
                )
                return cursor.fetchall()

    def upsert_new_data(self, parquet_filename: str):
        """Upsert uploaded parquet into target Delta table."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    MERGE INTO `{self._config.catalog}`.`{self._config.schema}`.`{self._config.table}` AS target
                    USING (
                        SELECT
                            TRY_CAST(state AS FLOAT) AS state,
                            TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts,
                            entity_id
                        FROM read_files('{self._config.dbx_path}/{parquet_filename}')
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
                )
                return cursor.fetchall()


def _extract_states_to_parquet(
    source_db_path: str,
    output_parquet_path: str,
    entity_like: str,
    chunk_size: int,
    min_last_updated_ts: float | None = None,
    use_hot_copy: bool = True,
) -> tuple[int, float | None]:
    """Extract matching states into parquet in chunks.

    When use_hot_copy is True, copy the active DB first to reduce lock contention.
    """

    read_db_path = source_db_path
    temp_db_path: str | None = None
    if use_hot_copy:
        fd, temp_db_path = tempfile.mkstemp(prefix="ha_temp_", suffix=".db")
        os.close(fd)
        shutil.copyfile(source_db_path, temp_db_path)
        read_db_path = temp_db_path

    writer: Optional[pq.ParquetWriter] = None
    total_rows = 0
    last_state_id = 0
    max_last_updated_ts: float | None = None

    query = """
    SELECT
        s.state_id,
        s.state,
        s.last_updated_ts,
        sm.entity_id
    FROM states AS s
    JOIN states_meta AS sm
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

    try:
        connection = sqlite3.connect(f"file:{read_db_path}?mode=ro", uri=True)
        try:
            cursor = connection.cursor()
            cursor.execute(query, (last_state_id, effective_min_ts, entity_like, chunk_size))
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                last_state_id = int(rows[-1][0])
                state_values = [row[1] for row in rows]
                ts_values = [row[2] for row in rows]
                entity_values = [row[3] for row in rows]
                chunk_max_ts = max(ts_values)
                if max_last_updated_ts is None or chunk_max_ts > max_last_updated_ts:
                    max_last_updated_ts = float(chunk_max_ts)

                table = pa.Table.from_arrays(
                    [
                        pa.array(state_values, type=pa.string()),
                        pa.array(ts_values, type=pa.float64()),
                        pa.array(entity_values, type=pa.string()),
                    ],
                    schema=PARQUET_SCHEMA,
                )
                if writer is None:
                    writer = pq.ParquetWriter(output_parquet_path, PARQUET_SCHEMA)
                writer.write_table(table)
                total_rows += len(rows)
                cursor.execute(
                    query, (last_state_id, effective_min_ts, entity_like, chunk_size)
                )
        finally:
            connection.close()
    finally:
        if writer is not None:
            writer.close()
        if temp_db_path is not None and os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    return total_rows, max_last_updated_ts


def run_sync_pipeline(request: SyncRequest) -> dict:
    """Run blocking extraction, upload, and merge in Home Assistant runtime."""

    os.makedirs(request.local_path, exist_ok=True)
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"upload_{export_time}.parquet"
    full_path = os.path.join(request.local_path, filename)

    use_hot_copy = (
        request.hot_copy_db
        if request.hot_copy_db is not None
        else request.min_last_updated_ts is None
    )

    rows_written, max_last_updated_ts = _extract_states_to_parquet(
        source_db_path=request.db_path,
        output_parquet_path=full_path,
        entity_like=request.entity_like,
        chunk_size=request.chunk_size,
        min_last_updated_ts=request.min_last_updated_ts,
        use_hot_copy=use_hot_copy,
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
    )
    sqlwh.create_schema()
    sqlwh.create_table()
    upload_state = sqlwh.upload_to_databricks(full_path, filename)
    upsert_state = sqlwh.upsert_new_data(filename)

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
