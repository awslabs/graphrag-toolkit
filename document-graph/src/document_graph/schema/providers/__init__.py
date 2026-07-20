# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Providers Package for Document Graph Operations.

This package provides a collection of schema providers for loading and managing ETL schemas
used in document graph operations. Schema providers are responsible for loading schemas
from various sources such as files, databases, and cloud services.

The package includes the following components:

Base Classes:
- SchemaProviderBase: Abstract base class for all schema providers
- SchemaProviderConfig: Base configuration model for schema providers
- AWSSchemaProviderConfig: Base configuration model for AWS-backed schema providers

Factory and Registry:
- SchemaProviderFactory: Factory for creating schema providers based on configuration
- SchemaProviderRegistry: Registry for managing schema provider classes

Provider Implementations:
- FileSchemaProvider: Provider for loading schemas from local files
- S3SchemaProvider: Provider for loading schemas from Amazon S3
- CSVSchemaProvider: Provider for generating schemas from CSV files
- JSONSchemaProvider: Provider for generating schemas from JSON files
- ExcelSchemaProvider: Provider for generating schemas from Excel files
- GlueSchemaProvider: Provider for loading schemas from AWS Glue
- ParquetSchemaProvider: Provider for generating schemas from Parquet files
- YAMLSchemaProvider: Provider for generating schemas from YAML files
- XMLSchemaProvider: Provider for generating schemas from XML files

Usage:
    # Create a schema provider using the factory
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "type": "file",
        "schema_id": "my_schema",
        "connection_config": {
            "path": "/path/to/schema.yaml"
        }
    }
    
    provider = get_schema_provider(config)
    
    # Load the schema
    schema = provider.pipeline_schema()
    
    # Use the schema for ETL operations
    print(f"Loaded schema ID: {schema.schema_id}")
"""

# Base classes
from .schema_provider_base import SchemaProviderBase
from .schema_provider_config import SchemaProviderConfig
from .schema_provider_config_aws_base import AWSSchemaProviderConfig

# Factory and registry
from .schema_provider_factory import SchemaProviderFactory
from .schema_provider_registry import SchemaProviderRegistry

# Provider implementations
from .file_schema_provider import FileSchemaProvider
from .s3_schema_provider import S3SchemaProvider
from .csv_schema_provider import CSVSchemaProvider
from .json_schema_provider import JSONSchemaProvider
from .excel_schema_provider import ExcelSchemaProvider
from .glue_schema_provider import GlueSchemaProvider
from .parquet_schema_provider import ParquetSchemaProvider
from .yaml_schema_provider import YAMLSchemaProvider
from .xml_schema_provider import XMLSchemaProvider

__all__ = [
    # Base classes
    'SchemaProviderBase',
    'SchemaProviderConfig',
    'AWSSchemaProviderConfig',
    
    # Factory and registry
    'SchemaProviderFactory',
    'SchemaProviderRegistry',
    
    # Provider implementations
    'FileSchemaProvider',
    'S3SchemaProvider',
    'CSVSchemaProvider',
    'JSONSchemaProvider',
    'ExcelSchemaProvider',
    'GlueSchemaProvider',
    'ParquetSchemaProvider',
    'YAMLSchemaProvider',
    'XMLSchemaProvider',
    
    # Aliases for backward compatibility
    'CSVSchemaProvider',
    'JSONSchemaProvider'
]

# Aliases
CSVSchemaProvider = CSVSchemaProvider  # Already correct name
JSONSchemaProvider = JSONSchemaProvider  # Already correct name