
import json
import os
from unittest.mock import patch, mock_open

import pytest


@pytest.fixture(autouse=True, scope="module")
def good_config():
    """Set up a good config."""
    config = {
        "CATALOG": "catalog_name",
        "SCHEMA": "schema_name",
        "TABLE": "table_name",
        "LOCAL_PATH": "valid_local_path",
        "DBX_VOLUMES_PATH": "valid_remote_path_in a Databricks volume"
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(config))):
        yield


def setup_env():
    """Set environment variables"""
    os.environ["CATALOG"] = "catalog_name"
    os.environ["SCHEMA"] = "schema_name"
    os.environ["TABLE"] = "table_name"
    os.environ["LOCAL_PATH"] = "valid_local_path"
    os.environ["DBX_VOLUMES_PATH"] = "valid_remote_path_in a Databricks volume"


def clean_up_env():
    """Clean up environment variables."""
    yield
    try:
        del os.environ["CATALOG"]
        del os.environ["SCHEMA"]
        del os.environ["TABLE"]
        del os.environ["LOCAL_PATH"]
        del os.environ["DBX_VOLUMES_PATH"]
    except KeyError:
        pass
    assert "CATALOG" not in os.environ
    assert "SCHEMA" not in os.environ
    assert "TABLE" not in os.environ
    assert "LOCAL_PATH" not in os.environ
    assert "DBX_VOLUMES_PATH" not in os.environ
