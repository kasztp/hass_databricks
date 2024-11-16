"""Tests for the validators module."""
import json
import os
from unittest.mock import patch, mock_open

import pytest

from hass_databricks.utils.validators import ConfigValidator

from tests.utils.conftest import clean_up_env, setup_env


def test_config_validator_from_json_good_config(good_config):
    """Test loading a config from a JSON file."""
    c = ConfigValidator.from_json("config.json")
    assert c["CATALOG"] == "catalog_name"
    assert c["SCHEMA"] == "schema_name"
    assert c["TABLE"] == "table_name"
    assert c["LOCAL_PATH"] == "valid_local_path"
    assert c["DBX_VOLUMES_PATH"] == "valid_remote_path_in a Databricks volume"


def test_config_validator_from_json_missing_key_in_config():
    """Test loading a config from a JSON file with missing keys."""
    config = {
        "CATALOG": "catalog_name",
        "SCHEMA": "schema_name",
        "TABLE": "table_name",
        "LOCAL_PATH": "valid_local_path"
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(config))):
        with pytest.raises(ValueError):
            ConfigValidator.from_json("config.json")


def test_config_validator_from_json_bad_config():
    """Test loading a config from a JSON file with invalid keys."""
    config = {
        "CATALOG": "catalog_name",
        "SCHEMA": "schema_name",
        "TABLE": "table_name",
        "LOCAL_PATH": "valid_local_path",
        "INVALID_KEY": "invalid_value"
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(config))):
        with pytest.raises(ValueError):
            ConfigValidator.from_json("config.json")


def test_config_validator_from_json_empty_config():
    """Test loading a config from an empty JSON file."""
    config = {}
    with patch("builtins.open", mock_open(read_data=json.dumps(config))):
        with pytest.raises(ValueError):
            ConfigValidator.from_json("config.json")


def test_config_validator_from_env_good_config():
    """Test loading a config from environment variables."""
    # Set environment variables
    setup_env()

    # Load config from environment variables
    c = ConfigValidator.from_env()
    assert c["CATALOG"] == "catalog_name"
    assert c["SCHEMA"] == "schema_name"
    assert c["TABLE"] == "table_name"
    assert c["LOCAL_PATH"] == "valid_local_path"
    assert c["DBX_VOLUMES_PATH"] == "valid_remote_path_in a Databricks volume"

    # Clean up
    clean_up_env()


def test_config_validator_from_env_bad_config():
    """Test loading a config from environment variables with missing keys."""
    # Set environment variables
    setup_env()
    # Remove a key
    if "DBX_VOLUMES_PATH" in os.environ:
        del os.environ["DBX_VOLUMES_PATH"]

    # Load config from environment variables
    with pytest.raises(KeyError):
        ConfigValidator.from_env()

    # Clean up
    clean_up_env()


def test_config_validator_get_config_as_dict():
    """Test getting the config as a dictionary."""
    config = {
        "CATALOG": "catalog_name",
        "SCHEMA": "schema_name",
        "TABLE": "table_name",
        "LOCAL_PATH": "valid_local_path",
        "DBX_VOLUMES_PATH": "valid_remote_path_in a Databricks volume"
    }
    c = ConfigValidator(**config)
    assert c.get_config_as_dict() == config
