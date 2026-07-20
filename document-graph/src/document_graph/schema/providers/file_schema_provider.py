# Copyright (c) Evan Erwee. All rights reserved.

"""File Schema Provider Module for Document Graph Operations.

This module provides a schema provider for loading and saving ETL schemas from/to local JSON files.
Unlike discovery-based providers, this provider works with pre-defined schema files rather than
inferring schemas from data sources.

The module includes the following components:
- FileSchemaProvider: Schema provider for JSON schema files

The file schema provider loads ETL schemas directly from JSON files and can also save
schemas back to files. It's commonly used to store and retrieve pre-defined or previously
discovered schemas.

Usage:
    # Get a file schema provider for a specific schema file
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "file",
        "schema_id": "my_schema",
        "connection_config": {
            "path": "/path/to/schema.json"
        }
    }
    
    provider = get_schema_provider(config)
    
    # Load the schema
    schema = provider.pipeline_schema()
    
    # Use the schema for ETL operations
    print(f"Loaded schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
    
    # Save a modified schema
    provider.save_schema(schema)
"""

# Import custom logging to ensure configuration is available

import json
import logging
from pathlib import Path
from typing import Any, Optional

from document_graph.schema.providers.schema_provider_base import SchemaProviderBase
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.etl_schema_model import ETLSchema

logger = logging.getLogger(__name__)


class FileSchemaProvider(SchemaProviderBase):
    """
    Schema provider for loading and saving ETL schemas from/to local JSON files.
    
    This class provides functionality to load pre-defined ETL schemas from JSON files
    and save schemas back to files. Unlike discovery-based providers, this provider
    works with existing schema files rather than inferring schemas from data sources.
    
    The provider is commonly used to store and retrieve pre-defined or previously
    discovered schemas for reuse in ETL pipelines.
    
    Attributes:
        config (SchemaProviderConfig): Configuration for the provider.
        path (Path): The path to the JSON schema file.
        _schema_id (str): The unique identifier for the schema.
    """

    def __init__(self, config: SchemaProviderConfig):
        """
        Initialize a file schema provider with the given configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider, including
                the path to the JSON schema file in the connection_config.
                
        Raises:
            ValueError: If the path is not provided in the connection_config.
            FileNotFoundError: If the specified JSON file does not exist.
        """
        self.config = config
        path_str = config.connection_config.get("path")

        if not path_str:
            raise ErrorHandler.validation_error(
                "schema_config_path",
                "None",
                "valid file path in config.connection_config['path']"
            )

        self.path: Path = Path(path_str)
        if not self.path.exists():
            raise FileNotFoundError(f"File not found: {self.path}")

        self._schema_id: str = config.schema_id or self.path.stem

    @classmethod
    def from_config(cls, config: SchemaProviderConfig) -> "FileSchemaProvider":
        """
        Factory method to construct a file provider from a configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider.
            
        Returns:
            FileSchemaProvider: A new instance of the file schema provider.
        """
        return cls(config)

    def load_schema(self, source=None, **kwargs) -> ETLSchema:
        """
        Load the ETL schema from the JSON file and parse it into an ETLSchema object.
        
        This method reads the JSON schema file, parses it, and returns an ETLSchema object.
        Unlike discovery-based providers, this method loads a pre-defined schema rather
        than inferring one from a data source.
        
        Args:
            source: Optional source override (not used in this provider).
            **kwargs: Additional provider-specific parameters (not used in this provider).
            
        Returns:
            ETLSchema: The ETL schema loaded from the JSON file.
            
        Raises:
            FileNotFoundError: If the JSON file does not exist.
            ValueError: If the JSON file cannot be parsed or is not a valid ETL schema.
            IOError: If there's an error reading the file.
        """
        try:
            with self.path.open("r", encoding="utf-8") as f:
                schema_json = json.load(f)
                logger.info(f"Loaded schema from file: {self.path}")
                return ETLSchema(**schema_json)
        except Exception as e:
            logger.error(f"Failed to load ETL schema from file {self.path}: {e}")
            raise ErrorHandler.database_error("file schema load", e)

    def save_schema(self, schema: ETLSchema) -> None:
        """
        Save the ETL schema to the file path specified in the configuration.
        
        This method serializes the ETL schema to JSON and writes it to the file
        specified in the provider's configuration. The schema can then be loaded
        later using this or another FileSchemaProvider.
        
        Args:
            schema (ETLSchema): The ETL schema to save.
            
        Raises:
            IOError: If there's an error writing to the file.
        """
        try:
            with self.path.open("w", encoding="utf-8") as f:
                json.dump(schema.model_dump(mode="json", exclude_unset=True), f, indent=2)
                logger.info(f"Saved schema to file: {self.path}")
        except Exception as e:
            logger.error(f"Failed to save ETL schema to file {self.path}: {e}")
            raise IOError(f"IO error: {self.path, e}")

    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema, either from the
        configuration or generated from the JSON file name.
        
        Returns:
            str: A unique identifier for the schema.
        """
        return self._schema_id
