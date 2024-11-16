"""Validate config files and environment variables"""
import json
import os
from typing import Any, Dict
from pydantic import BaseModel, ValidationError


class ConfigValidator(BaseModel):
    """Config validation class.
    
    A config should be a valid JSON file with the following structure:
    
    {
        "CATALOG": "catalog_name",
        "SCHEMA": "schema_name",
        "TABLE": "table_name",
        "LOCAL_PATH": "valid_local_path",
        "DBX_VOLUMES_PATH": "valid_remote_path_in a Databricks volume"
    }
    """
    CATALOG: str
    SCHEMA: str
    TABLE: str
    LOCAL_PATH: str
    DBX_VOLUMES_PATH: str

    @classmethod
    def from_json(cls, config_file: str) -> Dict[str, Any]:
        """Load a JSON config file and return a validated config dict"""
        with open(config_file, "r") as f:
            config = json.load(f)
        try:
            return cls(**config).model_dump()
        except ValidationError as e:
            raise ValueError(f"Invalid config file: {e}")

    @classmethod
    def from_env(cls) -> Dict[str, Any]:
        """Load config from environment variables"""
        try:
            return cls(
                CATALOG=os.environ["CATALOG"],
                SCHEMA=os.environ["SCHEMA"],
                TABLE=os.environ["TABLE"],
                LOCAL_PATH=os.environ["LOCAL_PATH"],
                DBX_VOLUMES_PATH=os.environ["DBX_VOLUMES_PATH"]
            ).model_dump()
        except ValidationError as e:
            raise ValueError(f"Invalid environment variables: {e}")

    def get_config_as_dict(self) -> Dict[str, Any]:
        """Return the model as a dictionary"""
        return self.model_dump()
