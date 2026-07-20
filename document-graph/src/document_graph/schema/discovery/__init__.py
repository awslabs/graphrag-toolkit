# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Discovery Module for Document Graph Operations.

This module provides schema discovery capabilities for various file formats,
allowing automatic inference of ETL schemas from source data files.
"""

from .schema_discovery_base import SchemaDiscoveryProvider
from .tabular_discovery_base import TabularSchemaDiscoveryProvider
from .csv_discovery_provider import CSVSchemaDiscoveryProvider
from .excel_discovery_provider import ExcelSchemaDiscoveryProvider
from .json_discovery_provider import JSONSchemaDiscoveryProvider
from .parquet_discovery_provider import ParquetSchemaDiscoveryProvider
from .xml_discovery_provider import XMLSchemaDiscoveryProvider
from .yaml_discovery_provider import YAMLSchemaDiscoveryProvider
from .schema_discovery_registry import get_discovery_provider, DISCOVERY_REGISTRY
from .schema_discovery_registry_class import SchemaDiscoveryRegistry

# Aliases for notebook 24 compatibility
CSVDiscoveryProvider = CSVSchemaDiscoveryProvider
JSONDiscoveryProvider = JSONSchemaDiscoveryProvider
ExcelDiscoveryProvider = ExcelSchemaDiscoveryProvider

__all__ = [
    'SchemaDiscoveryProvider',
    'TabularSchemaDiscoveryProvider',
    'CSVSchemaDiscoveryProvider',
    'ExcelSchemaDiscoveryProvider', 
    'JSONSchemaDiscoveryProvider',
    'ParquetSchemaDiscoveryProvider',
    'XMLSchemaDiscoveryProvider',
    'YAMLSchemaDiscoveryProvider',
    'get_discovery_provider',
    'DISCOVERY_REGISTRY',
    # Notebook 24 compatibility
    'CSVDiscoveryProvider',
    'JSONDiscoveryProvider', 
    'ExcelDiscoveryProvider',
    'SchemaDiscoveryRegistry'
]