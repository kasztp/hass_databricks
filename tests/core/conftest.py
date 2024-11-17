
import json
import os
from unittest.mock import patch, mock_open

import pandas as pd
import pytest

from hass_databricks.utils.config import Config

@pytest.fixture(autouse=True, scope="module")
def configuration():
    """Set up a good Config from /tests/data/config.json."""
    return Config("tests/data/config.json")


