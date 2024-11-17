import os

from databricks import sql
from hass_databricks.utils.config import Config


class DatabricksTarget:
    """Class to interact with Databricks."""
    def __init__(self, config: Config):
        self._config                     = config
        self._server_hostname            = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self._http_path                  = os.getenv("DATABRICKS_HTTP_PATH")
        self._access_token               = os.getenv("DATABRICKS_TOKEN")
        self._staging_allowed_local_path = config.local_path
        self._dbx_volumes_path           = config.dbx_path

    @property
    def server_hostname(self):
        """Return the server hostname."""
        return self._server_hostname

    @property
    def http_path(self):
        """Return the HTTP path."""
        return self._http_path

    @property
    def access_token(self):
        """Return the access token."""
        return self._access_token

    @property
    def staging_allowed_local_path(self):
        """Return the staging allowed local path."""
        return self._staging_allowed_local_path

    @staging_allowed_local_path.setter
    def staging_allowed_local_path(self, value):
        self._staging_allowed_local_path = value

    @property
    def dbx_volumes_path(self):
        """Return the Databricks volumes path."""
        return self._dbx_volumes_path

    @dbx_volumes_path.setter
    def dbx_volumes_path(self, value):
        self._dbx_volumes_path = value

    @property
    def catalog(self):
        """Return the catalog."""
        return self._config.catalog

    @catalog.setter
    def catalog(self, value):
        self._config.catalog = value

    @property
    def schema(self):
        """Return the schema."""
        return self._config.schema

    @schema.setter
    def schema(self, value):
        self._config.schema = value

    @property
    def table(self):
        """Return the table."""
        return self._config.table

    @table.setter
    def table(self, value):
        self._config.table = value

    def create_table(self):
        """Create a table for sensor data in Databricks."""
        with sql.connect(
            server_hostname            = self.server_hostname,
            http_path                  = self.http_path,
            access_token               = self.access_token,
            staging_allowed_local_path = self.staging_allowed_local_path
        ) as connection:

            with connection.cursor() as cursor:

                # Create a table in the specified catalog and schema.
                cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self.catalog}`.`{self.schema}`.`{self.table}` (
                    state FLOAT,
                    last_updated_ts TIMESTAMP,
                    entity_id STRING
                )
                USING DELTA
                PARTITIONED BY (entity_id)
                """
                )

                return cursor.fetchall()

    def create_schema(self):
        """Create a schema for sensor data in Databricks."""
        with sql.connect(
            server_hostname            = self.server_hostname,
            http_path                  = self.http_path,
            access_token               = self.access_token,
            staging_allowed_local_path = self.staging_allowed_local_path
        ) as connection:

            with connection.cursor() as cursor:

                # Create a schema in the specified catalog.
                cursor.execute(
                f"""
                CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`
                """
                )

                return cursor.fetchall()

    def upload_to_databricks(self, input_full_path: str, filename: str):
        """Upload a file to Databricks."""

        with sql.connect(
            server_hostname            = self.server_hostname,
            http_path                  = self.http_path,
            access_token               = self.access_token,
            staging_allowed_local_path = self.staging_allowed_local_path
        ) as connection:

            with connection.cursor() as cursor:

                # Write a local file to the specified path in a volume.
                # Specify OVERWRITE to overwrite any existing file in that path.
                cursor.execute(
                f"PUT '{input_full_path}' INTO '{self.dbx_volumes_path}/{filename}' OVERWRITE"
                )

                return cursor.fetchall()

    def upsert_new_data(self, parquet_filename: str):
        """Upsert new data from a parquet file into the table."""
        with sql.connect(
            server_hostname            = self.server_hostname,
            http_path                  = self.http_path,
            access_token               = self.access_token,
            staging_allowed_local_path = self.staging_allowed_local_path
        ) as connection:

            with connection.cursor() as cursor:

                # Upsert data from the uploaded file into the table.
                cursor.execute(
                f"""
                MERGE INTO `{self.catalog}`.`{self.schema}`.`{self.table}` AS target
                    USING (
                    SELECT 
                        TRY_CAST(state AS FLOAT) AS state, 
                        TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts, 
                        entity_id
                    FROM read_files('{self.dbx_volumes_path}/{parquet_filename}')
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
                    VALUES (source.state, source.last_updated_ts, source.entity_id);
                """
                )

                return cursor.fetchall()

    def initial_load(self, parquet_filename: str):
        """Initial load from a parquet file into the table."""
        with sql.connect(
            server_hostname            = self.server_hostname,
            http_path                  = self.http_path,
            access_token               = self.access_token,
            staging_allowed_local_path = self.staging_allowed_local_path
        ) as connection:

            with connection.cursor() as cursor:

                # Initial load from the uploaded file into the table.
                cursor.execute(
                f"""
                COPY INTO `{self.catalog}`.`{self.schema}`.`{self.table}`
                FROM (
                    SELECT 
                        TRY_CAST(state AS FLOAT) AS state, 
                        TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts, 
                        entity_id 
                    FROM '{self.dbx_volumes_path}/{parquet_filename}'
                )
                FILEFORMAT = parquet
                COPY_OPTIONS ('mergeSchema' = 'true');
                """
                )

                return cursor.fetchall()
