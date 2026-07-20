# Copyright (c) Evan Erwee. All rights reserved.

"""Parquet Schema Provider Module for Document Graph Operations.

This module provides a schema provider for Parquet files, a columnar storage format
commonly used in big data processing. It discovers and generates ETL schemas from
Parquet files for processing and loading the data into a document graph.

The module includes the following components:
- ParquetSchemaProvider: Schema provider for Parquet files

The Parquet schema provider uses the ParquetSchemaDiscoveryProvider to analyze Parquet
files and generate appropriate ETL schemas. It supports loading schemas from Parquet
files and optionally saving the discovered schemas to JSON files.

Usage:
    # Get a Parquet schema provider for a specific file
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "parquet",
        "schema_id": "my_parquet_schema",
        "connection_config": {
            "path": "/path/to/data.parquet"
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
from document_graph.schema.discovery.parquet_discovery_provider import ParquetSchemaDiscoveryProvider

logger = logging.getLogger(__name__)


class ParquetSchemaProvider(SchemaProviderBase):
    """
    Schema provider for Parquet files.
    
    This class provides functionality to discover and generate ETL schemas from Parquet files.
    It uses the ParquetSchemaDiscoveryProvider to analyze Parquet files and extract field
    information to create appropriate ETL schemas for processing and loading the data into
    a document graph.
    
    The provider supports loading schemas directly from Parquet files and optionally saving
    the discovered schemas to JSON files for later use.
    
    Attributes:
        config (SchemaProviderConfig): Configuration for the provider.
        parquet_path (Path): The path to the Parquet file to analyze.
    """

    def __init__(self, config: SchemaProviderConfig):
        """
        Initialize a Parquet schema provider with the given configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider, including
                the path to the Parquet file in the connection_config.
                
        Raises:
            ValueError: If the path is not provided in the connection_config.
            FileNotFoundError: If the specified Parquet file does not exist.
        """
        self.config = config
        path = config.connection_config.get("path")
        if not path:
            raise ValueError("Validation error")
        self.parquet_path = Path(path)

        if not self.parquet_path.exists():
            raise FileNotFoundError(f"File not found")

    @classmethod
    def from_config(cls, config: SchemaProviderConfig) -> "ParquetSchemaProvider":
        """
        Factory method to construct a Parquet provider from a configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider.
            
        Returns:
            ParquetSchemaProvider: A new instance of the Parquet schema provider.
        """
        return cls(config)

    def load_schema(self, source=None, **kwargs) -> ETLSchema:
        """
        Load (or generate) an ETL schema from a Parquet file using schema discovery.
        
        This method uses the ParquetSchemaDiscoveryProvider to analyze the Parquet file and
        generate an appropriate ETL schema based on the structure and content of the file.
        
        Args:
            source: Optional source override (not used in this provider).
            **kwargs: Additional provider-specific parameters that are passed to the
                discovery provider, such as specific columns to include.
            
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the Parquet file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the Parquet file does not exist.
            ValueError: If the Parquet file cannot be parsed or has no discoverable fields.
            Exception: Any exception raised during Parquet parsing.
        """
        discovery = ParquetSchemaDiscoveryProvider(source=self.parquet_path, args=kwargs)
        return discovery.discover_schema()

    def save_schema(self, output_path: Path) -> None:
        """
        Save the inferred ETL schema to a JSON file at the given output path.
        
        This method discovers the schema from the Parquet file and saves it to a JSON file
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
            logger.info(f"Saved Parquet schema to file: {output_path}")
        except Exception as e:
            raise IOError(f"IO error: {output_path, e}")

    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema, either from the
        configuration or generated from the Parquet file name.
        
        Returns:
            str: A unique identifier for the schema.
        """
        return self.config.schema_id or f"parquet-schema-{self.parquet_path.stem}"
