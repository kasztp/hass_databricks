import asyncio
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
import os
import shutil
import sqlite3
import tempfile
from typing import Optional, Tuple
import csv
import gzip

from hass_databricks.core.databricks import DatabricksTarget
from hass_databricks.utils.config import Config


CHUNK_SIZE = 50_000


@dataclass
class RuntimeSyncConfig:
    """Runtime config used by the shared blocking sync pipeline."""

    catalog: str
    schema: str
    table: str
    local_path: str
    dbx_path: str


def _extract_states_to_csv(
    source_db_path: str,
    output_csv_path: str,
    entity_like: str = "sensor.%",
    start_state_id: int = 0,
    chunk_size: int = CHUNK_SIZE,
) -> int:
    """Copy the active HA DB and extract sensor states in bounded chunks.

    This avoids locking the live Home Assistant SQLite database and keeps
    memory usage bounded during export.
    """
    fd, temp_db_path = tempfile.mkstemp(prefix="ha_temp_", suffix=".db")
    os.close(fd)
    shutil.copyfile(source_db_path, temp_db_path)

    total_rows = 0
    last_state_id = int(start_state_id)

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
        AND sm.entity_id LIKE ?
        AND s.state NOT IN ('unknown', 'unavailable')
    ORDER BY s.state_id ASC
    LIMIT ?
    """

    try:
        connection = sqlite3.connect(f"file:{temp_db_path}?mode=ro", uri=True)
        try:
            cursor = connection.cursor()

            with gzip.open(output_csv_path, "wt", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["state", "last_updated_ts", "entity_id"])

                cursor.execute(query, (last_state_id, entity_like, chunk_size))
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    last_state_id = int(rows[-1][0])

                    csv_rows = [[row[1], row[2], row[3]] for row in rows]
                    writer.writerows(csv_rows)
                    total_rows += len(rows)

                    cursor.execute(query, (last_state_id, entity_like, chunk_size))
        finally:
            connection.close()
    finally:
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)

    return total_rows


async def run_sync_pipeline(
    source_db_path: str,
    catalog: str,
    schema: str,
    table: str,
    local_path: str,
    dbx_volumes_path: str,
    entity_like: str = "sensor.%",
    chunk_size: int = CHUNK_SIZE,
    keep_local_file: bool = False,
    server_hostname: Optional[str] = None,
    http_path: Optional[str] = None,
    access_token: Optional[str] = None,
) -> dict:
    """Run async extraction, upload, and merge using core modules only."""

    os.makedirs(local_path, exist_ok=True)
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"upload_{export_time}.csv.gz"
    full_path = os.path.join(local_path, filename)

    rows_written = _extract_states_to_csv(
        source_db_path=source_db_path,
        output_csv_path=full_path,
        entity_like=entity_like,
        chunk_size=chunk_size,
    )
    if rows_written == 0:
        raise ValueError("No rows were extracted from Home Assistant database.")

    runtime_config = RuntimeSyncConfig(
        catalog=catalog,
        schema=schema,
        table=table,
        local_path=local_path,
        dbx_path=dbx_volumes_path,
    )
    sqlwh = DatabricksTarget(
        runtime_config,
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )
    await sqlwh.create_schema()
    await sqlwh.create_table()
    upload_state = await sqlwh.upload_to_databricks(full_path, filename)
    upsert_state = await sqlwh.upsert_new_data(filename)

    if not keep_local_file and os.path.exists(full_path):
        os.remove(full_path)

    return {
        "filename": filename,
        "rows": rows_written,
        "upload_state": upload_state,
        "upsert_state": upsert_state,
    }


def create_data_pack(configuration: Config) -> Tuple[str, str]:
    """Create a data pack for upload to Databricks.

    Args:
        config (Config): The project config.

    Returns:
        full_path (str): The full path of the data
        filename_csv_gz (str): The filename of the data in CSV Gzip format
    """
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    staging_path = configuration.local_path
    filename_csv_gz = f"upload_{export_time}.csv.gz"
    full_path = os.path.join(staging_path, filename_csv_gz)

    db_path = os.getenv("HA_SQLITE_DB_PATH", "home-assistant_v2.db")
    rows_written = _extract_states_to_csv(db_path, full_path)
    if rows_written == 0:
        raise ValueError("No rows were extracted from Home Assistant database.")
    print(f"File saved for upload: {filename_csv_gz}")

    return full_path, filename_csv_gz


def create_incremental_data_pack(
    configuration: Config, last_update_time: Optional[str]
) -> Tuple[str, str]:
    """Create an incremental data pack for upload to Databricks."""

    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    start_state_id = 0
    if last_update_time is not None:
        start_state_id = int(last_update_time)
    else:
        existing_data_packs = [
            f for f in os.listdir(configuration.local_path) if f.endswith(".csv.gz")
        ]
        if existing_data_packs:
            latest_data_pack = max(existing_data_packs, key=os.path.getctime)
            suffix = latest_data_pack.split(".")[0].split("_")[-1]
            if suffix.isdigit():
                start_state_id = int(suffix)

    staging_path = configuration.local_path
    filename_csv_gz = f"upload_{export_time}.csv.gz"
    full_path = os.path.join(staging_path, filename_csv_gz)

    db_path = os.getenv("HA_SQLITE_DB_PATH", "home-assistant_v2.db")
    rows_written = _extract_states_to_csv(
        source_db_path=db_path,
        output_csv_path=full_path,
        start_state_id=start_state_id,
    )
    if rows_written == 0:
        raise ValueError("No rows were extracted from Home Assistant database.")
    print(f"File saved for upload: {filename_csv_gz}")

    return full_path, filename_csv_gz


async def main():
    # Parse the command line arguments
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="config.json")
    parser.add_argument("-i", "--incremental", action="store_true")
    parser.add_argument("-k", "--keep_last", action="store_true")
    parser.add_argument("-l", "--last_update_time", type=str)
    args = parser.parse_args()

    config = Config(args.config)

    if args.incremental:
        file_path, filename = create_incremental_data_pack(
            config, args.last_update_time
        )
    else:
        file_path, filename = create_data_pack(config)

    sqlwh = DatabricksTarget(config)

    await sqlwh.create_schema()
    await sqlwh.create_table()

    load_state = await sqlwh.upload_to_databricks(file_path, filename)
    print(f"Data pack {filename} uploaded to Databricks.")
    print(f"Upload details: {load_state}")

    upsert_state = await sqlwh.upsert_new_data(filename)
    print(f"Data pack {filename} upserted into the table.")
    print(f"Operation details: {upsert_state}")

    if not args.keep_last:
        os.remove(file_path)
        print(f"File {file_path} removed.")

    print("Data upload complete.")


if __name__ == "__main__":
    asyncio.run(main())
