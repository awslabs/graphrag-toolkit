# Copyright (c) Evan Erwee. All rights reserved.

"""Excel Schema Provider Module for Document Graph Operations.

This module provides a schema provider for Excel files.
It discovers and generates ETL schemas from Excel files for processing and loading
the data into a document graph.

The module includes the following components:
- ExcelSchemaProvider: Schema provider for Excel files

The Excel schema provider uses the ExcelSchemaDiscoveryProvider to analyze Excel files
and generate appropriate ETL schemas. It supports loading schemas from Excel files
and optionally saving the discovered schemas to JSON files.

Usage:
    # Get an Excel schema provider for a specific file
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "excel",
        "schema_id": "my_excel_schema",
        "connection_config": {
            "path": "/path/to/data.xlsx"
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

# Import custom logging to ensure configuration is available

import json
from pathlib import Path
from typing import Optional, Any

from document_graph.schema.etl_schema_model import ETLSchema
from document_graph.schema.providers.schema_provider_base import SchemaProviderBase
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig

from document_graph.schema.discovery.excel_discovery_provider import (
    ExcelSchemaDiscoveryProvider
)

logger = logging.getLogger(__name__)


class ExcelSchemaProvider(SchemaProviderBase):
    """
    Schema provider for Excel files.
    
    This class provides functionality to discover and generate ETL schemas from Excel files.
    It uses the ExcelSchemaDiscoveryProvider to analyze Excel files and extract field information
    to create appropriate ETL schemas for processing and loading the data into a document graph.
    
    The provider supports loading schemas directly from Excel files and optionally saving
    the discovered schemas to JSON files for later use.
    
    Attributes:
        config (SchemaProviderConfig): Configuration for the provider.
        excel_path (Path): The path to the Excel file to analyze.
    """

    def __init__(self, config: SchemaProviderConfig):
        """
        Initialize an Excel schema provider with the given configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider, including
                the path to the Excel file in the connection_config.
                
        Raises:
            ValueError: If the path is not provided in the connection_config.
            FileNotFoundError: If the specified Excel file does not exist.
        """
        self.config = config
        path = config.connection_config.get("path")
        if not path:
            raise ValueError("Validation error")
        self.excel_path = Path(path)

        if not self.excel_path.exists():
            raise FileNotFoundError(f"File not found")

    @classmethod
    def from_config(cls, config: SchemaProviderConfig) -> "ExcelSchemaProvider":
        """
        Factory method to construct an Excel provider from a configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider.
            
        Returns:
            ExcelSchemaProvider: A new instance of the Excel schema provider.
        """
        return cls(config)

    def load_schema(self, source=None, **kwargs) -> ETLSchema:
        """
        Load (or generate) an ETL schema from an Excel file using schema discovery.
        
        This method uses the ExcelSchemaDiscoveryProvider to analyze the Excel file and
        generate an appropriate ETL schema based on the structure and content of the file.
        
        Args:
            source: Optional source override (not used in this provider).
            **kwargs: Additional provider-specific parameters (not used in this provider).
            
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the Excel file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the Excel file does not exist.
            ValueError: If the Excel file cannot be parsed or has no discoverable fields.
            ImportError: If the required Excel libraries are not installed.
        """
        discovery = ExcelSchemaDiscoveryProvider(source=self.excel_path)
        return discovery.discover_schema()

    def save_schema(self, output_path: Path) -> None:
        """
        Save the discovered ETL schema to a JSON file at the given path.
        
        This method discovers the schema from the Excel file and saves it to a JSON file
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
            logger.info(f"Saved Excel schema to file: {output_path}")
        except Exception as e:
            raise IOError(f"IO error: {output_path, e}")

    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema, either from the
        configuration or generated from the Excel file name.
        
        Returns:
            str: A unique identifier for the schema.
        """
        return self.config.schema_id or f"excel-schema-{self.excel_path.stem}"
