# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Schema Provider Module for Document Graph Operations.

This module provides a schema provider for JSON (JavaScript Object Notation) files.
It discovers and generates ETL schemas from JSON files for processing and loading
the data into a document graph.

The module includes the following components:
- JSONSchemaProvider: Schema provider for JSON files

The JSON schema provider uses the JSONSchemaDiscoveryProvider to analyze JSON files
and generate appropriate ETL schemas. It supports loading schemas from JSON files
and optionally saving the discovered schemas to JSON files.

Usage:
    # Get a JSON schema provider for a specific file
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "json",
        "schema_id": "my_json_schema",
        "connection_config": {
            "path": "/path/to/data.json"
        }
    }
    
    provider = get_schema_provider(config)
    
    # Load the schema
    schema = provider.pipeline_schema()
    
    # Use the schema for ETL operations
    print(f"Loaded schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""

import logging
import json
from pathlib import Path
from typing import Optional, Any, Dict

# Import custom logging to ensure configuration is available

from document_graph.schema.etl_schema_model import ETLSchema
from document_graph.schema.providers.schema_provider_base import SchemaProviderBase
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.discovery.json_discovery_provider import JSONSchemaDiscoveryProvider

logger = logging.getLogger(__name__)


class JSONSchemaProvider(SchemaProviderBase):
    """
    Schema provider for JSON (JavaScript Object Notation) files.
    
    This class provides functionality to discover and generate ETL schemas from JSON files.
    It uses the JSONSchemaDiscoveryProvider to analyze JSON files and extract field
    information to create appropriate ETL schemas for processing and loading the data into
    a document graph.
    
    The provider supports loading schemas directly from JSON files and optionally saving
    the discovered schemas to JSON files for later use. It handles both array-based JSON
    files (containing multiple records) and single-object JSON files.
    
    Attributes:
        config (SchemaProviderConfig): Configuration for the provider.
        json_path (Path): The path to the JSON file to analyze.
    """

    def __init__(self, config: SchemaProviderConfig):
        """
        Initialize a JSON schema provider with the given configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider, including
                the path to the JSON file in the connection_config.
                
        Raises:
            ValueError: If the path is not provided in the connection_config.
            FileNotFoundError: If the specified JSON file does not exist.
        """
        self.config = config
        path_str = config.connection_config.get("path")
        if not path_str:
            raise ValueError("Validation error")
        self.json_path = Path(path_str)

        if not self.json_path.exists():
            raise FileNotFoundError(f"File not found")

    @classmethod
    def from_config(cls, config: SchemaProviderConfig) -> "JSONSchemaProvider":
        """
        Factory method to construct a JSON provider from a configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider.
            
        Returns:
            JSONSchemaProvider: A new instance of the JSON schema provider.
        """
        return cls(config)

    def load_schema(self, source=None, **kwargs) -> ETLSchema:
        """
        Load (or generate) an ETL schema from a JSON file using schema discovery.
        
        This method uses the JSONSchemaDiscoveryProvider to analyze the JSON file and
        generate an appropriate ETL schema based on the structure and content of the file.
        It handles both array-based JSON files and single-object JSON files.
        
        Args:
            source: Optional source override (not used in this provider).
            **kwargs: Additional provider-specific parameters (not used in this provider).
            
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the JSON file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the JSON file does not exist.
            ValueError: If the JSON file cannot be parsed or has no discoverable fields.
            Exception: Any exception raised during JSON parsing.
        """
        discovery = JSONSchemaDiscoveryProvider(source=self.json_path)
        return discovery.discover_schema()

    def save_schema(self, output_path: Path) -> None:
        """
        Save the inferred ETL schema to a JSON file at the given output path.
        
        This method discovers the schema from the JSON file and saves it to a JSON file
        at the specified path. The schema can then be loaded directly using a FileSchemaProvider.
        
        Args:
            output_path (Path): The path where the schema JSON file should be saved.
            
        Raises:
            IOError: If the schema cannot be saved to the output path.
        """
        schema = self.load_schema()
        try:
            with output_path.open("w", encoding="utf-8") as f:
                json.dump(schema.model_dump(mode="json", exclude_unset=True), f, indent=2)
            logger.info(f"Saved JSON schema to file: {output_path}")
        except Exception as e:
            raise IOError(f"IO error: {output_path, e}")

    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema, either from the
        configuration or generated from the JSON file name.
        
        Returns:
            str: A unique identifier for the schema.
        """
        return self.config.schema_id or f"json-schema-{self.json_path.stem}"
