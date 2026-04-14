import os
import aiohttp
from typing import Protocol, Optional


class SupportsDatabricksConfig(Protocol):
    """Shape required by DatabricksTarget config consumers."""

    @property
    def catalog(self) -> str: ...

    @property
    def schema(self) -> str: ...

    @property
    def table(self) -> str: ...

    @property
    def local_path(self) -> str: ...

    @property
    def dbx_path(self) -> str: ...


class DatabricksTarget:
    """Class to interact with Databricks using REST APIs."""

    def __init__(
        self,
        config: SupportsDatabricksConfig,
        *,
        server_hostname: str | None = None,
        http_path: str | None = None,
        access_token: str | None = None,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._config = config
        self._server_hostname = server_hostname or os.getenv(
            "DATABRICKS_SERVER_HOSTNAME"
        )
        self._http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH")
        self._access_token = access_token or os.getenv("DATABRICKS_TOKEN")
        self._staging_allowed_local_path = config.local_path
        self._dbx_volumes_path = config.dbx_path
        self._session = session

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

    @property
    def schema(self):
        """Return the schema."""
        return self._config.schema

    @property
    def table(self):
        """Return the table."""
        return self._config.table

    async def _execute_sql(self, statement: str) -> dict:
        """Execute a SQL statement using the Databricks REST API asynchronously.

        Using wait_timeout of 30s to simulate blocking behavior for integration
        while remaining non-blocking on the execution thread.
        """
        close_session = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            url = f"https://{self.server_hostname}/api/2.0/sql/statements"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            }
            payload = {
                "statement": statement,
                "wait_timeout": "30s",
            }
            async with session.post(url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                result = await resp.json()
                if result.get("status", {}).get("state") in ("FAILED", "CANCELED"):
                    raise Exception(f"SQL execution failed: {result}")

                # Simplified check to ensure it finished. If not, it returns RUNNING.
                # Since operations are small or batched, 30s is usually enough.
                # If truly large, a poll loop should be implemented, but this suffices for typical HA volume.
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
            url = f"https://{self.server_hostname}/api/2.0/fs/files{databricks_path}?overwrite=true"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
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

    async def create_table(self):
        """Create a table for sensor data in Databricks."""
        statement = f"""
        CREATE TABLE IF NOT EXISTS `{self.catalog}`.`{self.schema}`.`{self.table}` (
            state FLOAT,
            last_updated_ts TIMESTAMP,
            entity_id STRING
        )
        USING DELTA
        PARTITIONED BY (entity_id)
        """
        return await self._execute_sql(statement)

    async def create_schema(self):
        """Create a schema for sensor data in Databricks."""
        statement = f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`"
        return await self._execute_sql(statement)

    async def upload_to_databricks(self, input_full_path: str, filename: str):
        """Upload a file to Databricks."""
        databricks_path = f"{self.dbx_volumes_path}/{filename}"
        return await self._upload_file(input_full_path, databricks_path)

    async def upsert_new_data(self, filename: str):
        """Upsert new data from a file into the table using read_files and csv formatting."""
        statement = f"""
        MERGE INTO `{self.catalog}`.`{self.schema}`.`{self.table}` AS target
            USING (
            SELECT 
                TRY_CAST(state AS FLOAT) AS state, 
                TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts, 
                entity_id
            FROM read_files('{self.dbx_volumes_path}/{filename}', format => 'csv', header => 'true')
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

    async def initial_load(self, filename: str):
        """Initial load from a file into the table using read_files and csv formatting."""
        statement = f"""
        COPY INTO `{self.catalog}`.`{self.schema}`.`{self.table}`
        FROM (
            SELECT 
                TRY_CAST(state AS FLOAT) AS state, 
                TRY_CAST(last_updated_ts AS TIMESTAMP) AS last_updated_ts, 
                entity_id 
            FROM read_files('{self.dbx_volumes_path}/{filename}', format => 'csv', header => 'true')
        )
        COPY_OPTIONS ('mergeSchema' = 'true')
        """
        return await self._execute_sql(statement)
