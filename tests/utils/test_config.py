"""Tests for the Config class."""
import json
import os
from unittest.mock import patch, mock_open

from hass_databricks.utils.config import Config
from tests.utils.conftest import clean_up_env, setup_env


def test_config_from_json(good_config):
    """Test loading a config from a JSON file."""
    c = Config("config.json")
    assert c.catalog == "catalog_name"
    assert c.schema == "schema_name"
    assert c.table == "table_name"
    assert c.local_path == "valid_local_path"
    assert c.dbx_path == "valid_remote_path_in a Databricks volume"


def test_config_from_env():
    """Test loading a config from environment variables."""
    # Set environment variables
    setup_env()

    # Load config from environment variables
    c = Config()
    assert c.catalog == "catalog_name"
    assert c.schema == "schema_name"
    assert c.table == "table_name"
    assert c.local_path == "valid_local_path"
    assert c.dbx_path == "valid_remote_path_in a Databricks volume"

    # Clean up
    clean_up_env()


def test_reload_config(good_config):
    """Test re-loading a config from a JSON file."""
    c = Config("config.json")
    assert c.catalog == "catalog_name"
    assert c.schema == "schema_name"
    assert c.table == "table_name"
    assert c.local_path == "valid_local_path"
    assert c.dbx_path == "valid_remote_path_in a Databricks volume"

    # Change the config file
    changed_config = {
        "CATALOG": "new_catalog_name",
        "SCHEMA": "new_schema_name",
        "TABLE": "new_table_name",
        "LOCAL_PATH": "new_valid_local_path",
        "DBX_VOLUMES_PATH": "new_valid_remote_path_in a Databricks volume"
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(changed_config))):
        c.reload_config()
    assert c.catalog == "new_catalog_name"
    assert c.schema == "new_schema_name"
    assert c.table == "new_table_name"
    assert c.local_path == "new_valid_local_path"
    assert c.dbx_path == "new_valid_remote_path_in a Databricks volume"


def test_get_full_config(good_config):
    """Test getting the full config dict."""
    config = {
        "CATALOG": "catalog_name",
        "SCHEMA": "schema_name",
        "TABLE": "table_name",
        "LOCAL_PATH": "valid_local_path",
        "DBX_VOLUMES_PATH": "valid_remote_path_in a Databricks volume"
    }
    c = Config("config.json")
    assert c.get_full_config() == config
