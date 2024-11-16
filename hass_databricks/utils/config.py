"""Config class for the project"""
from typing import Dict, Any
import os
import json

from hass_databricks.utils.validators import ConfigValidator


class Config:
    """Config class for the project"""
    def __init__(self, config_file: str = None):
        self._config_file = config_file
        self._config = self.load_config(config_file)

    @property
    def catalog(self) -> str:
        """Return the catalog name"""
        return self._config.get("CATALOG")
    
    @property
    def schema(self) -> str:
        """Return the schema name"""
        return self._config.get("SCHEMA")
    
    @property
    def table(self) -> str:
        """Return the table name"""
        return self._config.get("TABLE")
    
    @property
    def local_path(self) -> str:
        """Return the local path"""
        return self._config.get("LOCAL_PATH")
    
    @property
    def dbx_path(self) -> str:
        """Return the Databricks volumes path"""
        return self._config.get("DBX_VOLUMES_PATH")
    

    def load_config(self, config_file: str = None) -> Dict[str, Any]:
        """Load a config file and return a validated config dict"""
        if config_file:
            return ConfigValidator.from_json(config_file)
        return ConfigValidator.from_env()
    
    def reload_config(self) -> None:
        """Reload the config"""
        self._config = self.load_config(self._config_file)

    def get_full_config(self) -> Dict[str, Any]:
        """Return the config dict"""
        return self._config
