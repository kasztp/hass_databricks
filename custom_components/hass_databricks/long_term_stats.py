"""Incrementally export Home Assistant long-term statistics to Databricks.

Reads the recorder's hourly ``statistics`` table (kept indefinitely by Home
Assistant, unlike raw states which are purged) and MERGEs it into
``<catalog>.<schema>.ha_statistics`` keyed on ``(statistic_id, start_ts)``.
"""

from __future__ import annotations

import asyncio
import csv
from datetime import datetime, timezone
import gzip
import os
import sqlite3

from .metadata import _quote_ident, _quote_path  # shared SQL quoting helpers
from .pipeline import (
    DatabricksTarget,
    RuntimeSyncConfig,
    SyncRequest,
    _async_run_job,
    _makedirs,
    _remove_path,
)

STATISTICS_TABLE = "ha_statistics"
CSV_COLUMNS = [
    "statistic_id",
    "start_ts",
    "mean",
    "min",
    "max",
    "sum",
    "state",
    "last_reset_ts",
    "unit",
    "source",
]


def _iso(ts: float | None) -> str | None:
    """Epoch seconds -> unambiguous UTC ISO string (None stays None)."""
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def _extract_statistics_chunk(
    source_db_path: str,
    output_csv_path: str,
    chunk_size: int,
    min_start_ts: float | None = None,
    last_id: int = 0,
) -> tuple[int, float | None, int]:
    """Write one chunk of long-term statistics to csv.gz.

    Returns ``(rows, max_start_ts, next_id)``. Rows are keyset-paged on the
    statistics primary key so the read stays bounded.
    """
    query = """
    SELECT s.id, sm.statistic_id, s.start_ts, s.mean, s.min, s.max, s.sum,
           s.state, s.last_reset_ts, sm.unit_of_measurement, sm.source
    FROM statistics AS s
    JOIN statistics_meta AS sm ON s.metadata_id = sm.id
    WHERE s.id > ? AND s.start_ts >= ?
    ORDER BY s.id ASC
    LIMIT ?
    """
    connection = sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            query,
            (
                last_id,
                float(min_start_ts) if min_start_ts is not None else 0.0,
                chunk_size,
            ),
        ).fetchall()
    finally:
        connection.close()

    with gzip.open(output_csv_path, "wt", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        for r in rows:
            writer.writerow(
                [
                    r[1],
                    _iso(r[2]),
                    r[3],
                    r[4],
                    r[5],
                    r[6],
                    r[7],
                    _iso(r[8]),
                    r[9],
                    r[10],
                ]
            )

    if not rows:
        return 0, None, last_id
    return len(rows), max(float(r[2]) for r in rows), int(rows[-1][0])


def build_create_table_sql(catalog: str, schema: str) -> str:
    """Create the statistics table if it does not exist."""
    return (
        f"CREATE TABLE IF NOT EXISTS "
        f"{_quote_ident(catalog)}.{_quote_ident(schema)}.{STATISTICS_TABLE} ("
        "statistic_id STRING, start_ts TIMESTAMP, mean DOUBLE, min DOUBLE, "
        "max DOUBLE, sum DOUBLE, state DOUBLE, last_reset_ts TIMESTAMP, "
        "unit STRING, source STRING) USING DELTA"
    )


def build_merge_sql(catalog: str, schema: str, source_path: str) -> str:
    """Idempotent upsert keyed on (statistic_id, start_ts)."""
    return f"""
    MERGE INTO {_quote_ident(catalog)}.{_quote_ident(schema)}.{STATISTICS_TABLE} AS target
    USING (
        SELECT
            statistic_id,
            TRY_CAST(start_ts AS TIMESTAMP) AS start_ts,
            TRY_CAST(mean AS DOUBLE) AS mean,
            TRY_CAST(min AS DOUBLE) AS min,
            TRY_CAST(max AS DOUBLE) AS max,
            TRY_CAST(sum AS DOUBLE) AS sum,
            TRY_CAST(state AS DOUBLE) AS state,
            TRY_CAST(last_reset_ts AS TIMESTAMP) AS last_reset_ts,
            unit,
            source
        FROM read_files({_quote_path(source_path)}, format => 'csv', header => 'true')
    ) AS source
    ON target.statistic_id = source.statistic_id
       AND target.start_ts = source.start_ts
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """


async def run_statistics_sync(request: SyncRequest, min_start_ts: float | None) -> dict:
    """Upload statistics newer than ``min_start_ts`` (all, when None) and merge.

    Returns ``{"rows": int, "max_start_ts": float | None}``. Raises on failure;
    the caller decides how to isolate it from the other syncs.
    """
    target = DatabricksTarget(
        RuntimeSyncConfig(
            catalog=request.catalog,
            schema=request.schema,
            table=request.table,
            local_path=request.local_path,
            dbx_path=request.dbx_volumes_path,
        ),
        server_hostname=request.server_hostname,
        http_path=request.http_path,
        access_token=request.access_token,
        session=request.session,
        hass=request.hass,
    )
    await target.create_schema()
    await target._execute_sql(  # pylint: disable=protected-access
        build_create_table_sql(request.catalog, request.schema)
    )

    job_id = "statistics_" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    local_dir = os.path.join(request.local_path, job_id)
    dbx_dir = f"{request.dbx_volumes_path}/{job_id}"
    await _async_run_job(request.hass, _makedirs, local_dir)

    loop = asyncio.get_running_loop()
    total, max_ts, last_id, index = 0, None, 0, 0
    try:
        while True:
            index += 1
            filename = f"part_{index:05d}.csv.gz"
            local_file = os.path.join(local_dir, filename)
            rows, chunk_max, last_id = await loop.run_in_executor(
                None,
                _extract_statistics_chunk,
                request.db_path,
                local_file,
                request.chunk_size,
                min_start_ts,
                last_id,
            )
            if rows == 0:
                break
            total += rows
            if max_ts is None or chunk_max > max_ts:
                max_ts = chunk_max
            await target._upload_file(  # pylint: disable=protected-access
                local_file, f"{dbx_dir}/{filename}"
            )
            await _async_run_job(request.hass, _remove_path, local_file)
        await _async_run_job(request.hass, _remove_path, local_file)  # empty last chunk

        if total:
            await target._execute_sql(  # pylint: disable=protected-access
                build_merge_sql(request.catalog, request.schema, f"{dbx_dir}/*.csv.gz")
            )
            try:
                await target.delete_volume_folder(dbx_dir)
            except Exception:  # pylint: disable=broad-except
                pass
    finally:
        await _async_run_job(request.hass, _remove_path, local_dir)

    return {"rows": total, "max_start_ts": max_ts}
