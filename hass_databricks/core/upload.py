from argparse import ArgumentParser
from datetime import datetime
import os
from typing import Optional

import detective.core as detective
import detective.functions as functions
import pandas as pd

from hass_databricks.core.databricks import DatabricksTarget
from hass_databricks.utils.config import Config


def create_data_pack(config: Config) -> str:
    """Create a data pack for upload to Databricks.

    Args:
        config (Config): The project config.

    Returns:
        full_path (str): The full path of the data
        filename_parquet (str): The filename of the data in Parquet
    """
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    db = detective.db_from_hass_config()
    hadf = db.fetch_all_sensor_data(limit=1_000_000_000)

    staging_path = "/tmp/"
    filename_parquet = 'upload_{export_time}.parquet'
    full_path = staging_path + filename_parquet

    hadf.to_parquet(full_path, index=False)
    print(f"File saved for upload: {filename_parquet}")

    return full_path, filename_parquet


def create_incremental_data_pack(config: Config, last_update_time: Optional[str]) -> str:
    """Create an incremental data pack for upload to Databricks.

    Args:
        config (Config): The project config.
        last_update_time (str): The last update time in "%Y-%m-%d-%H-%M-%S" format.
        If None, the function will check for the latest existing data pack in the staging_path,
        then derive last_update_time from the filename.

    Returns:
        full_path (str): The full path of the data
        filename_parquet (str): The filename of the data in Parquet
    """
    export_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    db = detective.db_from_hass_config()

    if last_update_time is not None:
        last_update_time = datetime.timestamp(datetime.strptime(last_update_time, "%Y-%m-%d-%H-%M-%S"))
    else:
        # Get the latest existing data pack
        existing_data_packs = [f for f in os.listdir(config.staging_allowed_local_path) if f.endswith(".parquet")]
        if existing_data_packs:
            latest_data_pack = max(existing_data_packs, key=os.path.getctime)
            last_update_time = latest_data_pack.split(".")[0].split("_")[-1]
        else:
            last_update_time = None

    query = f"""
        SELECT states.state, states.last_updated_ts, states_meta.entity_id
            FROM states
        WHERE
            states.last_updated_ts >= '{last_update_time}'
        JOIN states_meta
        ON states.metadata_id = states_meta.metadata_id
        WHERE
            states_meta.entity_id  LIKE '%sensor%'
        AND
            states.state NOT IN ('unknown', 'unavailable')
        ORDER BY last_updated_ts DESC
    """
    
    hadf = db.perform_query(query, limit=1_000_000_000)

    staging_path = "/tmp/"
    filename_parquet = 'upload_{export_time}.parquet'
    full_path = staging_path + filename_parquet

    hadf.to_parquet(full_path, index=False)
    print(f"File saved for upload: {filename_parquet}")

    return full_path, filename_parquet


if __name__ == "__main__":
    # Parse the command line arguments
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", type=str, default="config.json", help="The path to the config file (default: config.json).")
    parser.add_argument("-i", "--incremental", action="store_true", help="Create an incremental data pack.")
    parser.add_argument("-l", "--last_update_time", type=str, help="The last update time in '%Y-%m-%d-%H-%M-%S' format.")
    args = parser.parse_args()

    # Load the config
    config = Config(args.config)

    # Create a data pack
    if args.incremental:
        full_path, filename_parquet = create_incremental_data_pack(config, args.last_update_time)
    else:
        full_path, filename_parquet = create_data_pack(config)

    # Initialize the Databricks target
    sqlwh = DatabricksTarget(
        server_hostname=config.server_hostname,
        http_path=config.http_path,
        access_token=config.access_token,
        staging_allowed_local_path=config.staging_allowed_local_path,
        catalog=config.catalog,
        schema=config.schema,
        table=config.table
    )

    # Create a schema and table in Databricks if necessary
    sqlwh.create_schema()
    sqlwh.create_table()

    # Upload the data to Databricks
    load_state = sqlwh.upload_to_databricks(full_path, filename_parquet)
    print(f"Data pack {filename_parquet} uploaded to Databricks.")

    # Upsert the new data into the table
    upsert_state = sqlwh.upsert_new_data(filename_parquet)
    print(f"Data pack {filename_parquet} upserted into the table.")
    print(f"Operation details: {upsert_state}")

    # Clean up
    os.remove(full_path)
    print(f"File {full_path} removed.")

    print("Data upload complete.")
