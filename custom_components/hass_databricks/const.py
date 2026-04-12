"""Constants for the hass_databricks integration."""

DOMAIN = "hass_databricks"
SERVICE_SYNC = "sync"
EVENT_SYNC_RESULT = "hass_databricks_sync_result"
STORAGE_VERSION = 1
STORAGE_KEY_PREFIX = "hass_databricks.sync_meta"
SYNC_META_LAST_SUCCESS_TS = "last_success_ts"
SYNC_META_LAST_RUN_TS = "last_run_ts"
SYNC_META_LAST_STATUS = "last_status"
SYNC_META_LAST_ROWS = "last_rows"
SYNC_META_LAST_ERROR = "last_error"
SYNC_META_LAST_FILENAME = "last_filename"
SYNC_META_LAST_TARGET = "last_target"
SYNC_META_LAST_TRIGGER = "last_trigger"
SYNC_META_LAST_SINCE_TS = "last_since_ts"
DEFAULT_INCREMENTAL_LOOKBACK_MINUTES = 10

# Databricks connection credentials (stored in config entry data)
CONF_SERVER_HOSTNAME = "server_hostname"
CONF_HTTP_PATH = "http_path"
CONF_ACCESS_TOKEN = "access_token"

# Sync target fields (stored in config entry data)
CONF_DB_PATH = "db_path"
CONF_CATALOG = "catalog"
CONF_SCHEMA = "schema"
CONF_TABLE = "table"
CONF_DBX_VOLUMES_PATH = "dbx_volumes_path"

# Sync tunable options (stored in config entry options)
CONF_LOCAL_PATH = "local_path"
CONF_ENTITY_LIKE = "entity_like"
CONF_CHUNK_SIZE = "chunk_size"
CONF_KEEP_LOCAL_FILE = "keep_local_file"
CONF_HOT_COPY_DB = "hot_copy_db"
CONF_AUTO_SYNC_ENABLED = "auto_sync_enabled"
CONF_AUTO_SYNC_INTERVAL_MINUTES = "auto_sync_interval_minutes"

DEFAULT_DB_FILENAME = "home-assistant_v2.db"
DEFAULT_ENTITY_LIKE = "sensor.%"
DEFAULT_CHUNK_SIZE = 50_000
DEFAULT_LOCAL_PATH = "/tmp/"
DEFAULT_AUTO_SYNC_ENABLED = True
DEFAULT_AUTO_SYNC_INTERVAL_MINUTES = 60
