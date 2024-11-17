from argparse import ArgumentParser
from datetime import datetime
import os
from typing import Optional, Tuple

import detective.core as detective

from hass_databricks.core.databricks import DatabricksTarget
from hass_databricks.utils.config import Config


def create_data_pack(configuration: Config) -> str:
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

    staging_path = configuration.staging_allowed_local_path
    filename_parquet = f"upload_{export_time}.parquet"
    full_path = staging_path + filename_parquet

    hadf.to_parquet(full_path, index=False)
    print(f"File saved for upload: {filename_parquet}")

    return full_path, filename_parquet


def create_incremental_data_pack(
        configuration: Config,
        last_update_time: Optional[str]
    ) -> Tuple[str, str]:
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
        last_update_time = datetime.timestamp(
                                datetime.strptime(last_update_time, "%Y-%m-%d-%H-%M-%S")
                            )
    else:
        # Get the latest existing data pack
        existing_data_packs = [
            f for f in os.listdir(configuration.staging_allowed_local_path)
            if f.endswith(".parquet")
        ]
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
    filename_parquet = f"upload_{export_time}.parquet"
    full_path = staging_path + filename_parquet

    hadf.to_parquet(full_path, index=False)
    print(f"File saved for upload: {filename_parquet}")

    return full_path, filename_parquet


if __name__ == "__main__":
    # Parse the command line arguments
    parser = ArgumentParser()
    parser.add_argument(
        "-c", "--config",
        type=str, default="config.json",
        help="The path to the config file (default: config.json)."
    )
    parser.add_argument(
        "-i", "--incremental",
        action="store_true",
        help="Create an incremental data pack."
    )
    parser.add_argument(
        "-k", "--keep_last",
        action="store_true",
        help="Keep the last data pack."
    )
    parser.add_argument(
        "-l", "--last_update_time",
        type=str,
        help="The last update time in '%Y-%m-%d-%H-%M-%S' format."
    )
    args = parser.parse_args()

    # Load the config
    config = Config(args.config)

    # Create a data pack
    if args.incremental:
        file_path, filename = create_incremental_data_pack(
                                        config,
                                        args.last_update_time
                                    )
    else:
        file_path, filename = create_data_pack(config)

    # Initialize the Databricks target
    sqlwh = DatabricksTarget(config)

    # Create a schema and table in Databricks if necessary
    sqlwh.create_schema()
    sqlwh.create_table()

    # Upload the data to Databricks
    load_state = sqlwh.upload_to_databricks(file_path, filename)
    print(f"Data pack {filename} uploaded to Databricks.")

    # Upsert the new data into the table
    upsert_state = sqlwh.upsert_new_data(filename)
    print(f"Data pack {filename} upserted into the table.")
    print(f"Operation details: {upsert_state}")

    # Clean up
    if not args.keep_last:
        os.remove(file_path)
        print(f"File {file_path} removed.")

    print("Data upload complete.")
