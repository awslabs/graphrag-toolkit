# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Discovery Registry Module for Document Graph Operations.

This module provides a registry system for schema discovery providers, allowing
the appropriate provider to be selected based on file extensions. It serves as
the entry point for schema discovery operations, abstracting away the details of
which provider to use for a given file type.

The module includes the following components:
- DISCOVERY_REGISTRY: A dictionary mapping file extensions to provider classes
- get_discovery_provider: Function to get the appropriate provider for a file

The registry system simplifies the process of discovering ETL schemas from various
file formats by automatically selecting the correct provider based on the file extension.
This allows client code to work with a consistent interface regardless of the
underlying file format.

Supported file formats:
- CSV (.csv): Comma-separated values files
- Parquet (.parquet): Columnar storage format files
- JSON (.json): JavaScript Object Notation files
- Excel (.xlsx, .xls): Microsoft Excel spreadsheet files
- XML (.xml): eXtensible Markup Language files
- YAML (.yaml, .yml): YAML Ain't Markup Language files

Usage:
    # Get a discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    # The appropriate provider is selected based on the file extension
    csv_provider = get_discovery_provider("data/sample.csv")
    excel_provider = get_discovery_provider("data/sample.xlsx")
    json_provider = get_discovery_provider("data/sample.json")
    parquet_provider = get_discovery_provider("data/sample.parquet")
    
    # Discover the schema using the provider
    schema = csv_provider.discover_schema()
    
    # Use the schema for ETL operations
    print(f"Discovered schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

from pathlib import Path
from typing import Type, Dict, Optional, Any

from document_graph.schema.discovery.schema_discovery_base import SchemaDiscoveryProvider
from document_graph.schema.discovery.csv_discovery_provider import CSVSchemaDiscoveryProvider
from document_graph.schema.discovery.parquet_discovery_provider import ParquetSchemaDiscoveryProvider
from document_graph.schema.discovery.json_discovery_provider import JSONSchemaDiscoveryProvider
from document_graph.schema.discovery.excel_discovery_provider import ExcelSchemaDiscoveryProvider
from document_graph.schema.discovery.xml_discovery_provider import XMLSchemaDiscoveryProvider
from document_graph.schema.discovery.yaml_discovery_provider import YAMLSchemaDiscoveryProvider

DISCOVERY_REGISTRY: Dict[str, Type[SchemaDiscoveryProvider]] = {
    "csv": CSVSchemaDiscoveryProvider,
    "parquet": ParquetSchemaDiscoveryProvider,
    "json": JSONSchemaDiscoveryProvider,
    "xlsx": ExcelSchemaDiscoveryProvider,
    "xls": ExcelSchemaDiscoveryProvider,
    "xml": XMLSchemaDiscoveryProvider,
    "yaml": YAMLSchemaDiscoveryProvider,
    "yml": YAMLSchemaDiscoveryProvider,
}


def get_discovery_provider(file_path: Path, args: Optional[Dict[str, Any]] = None) -> SchemaDiscoveryProvider:
    """
    Get the appropriate schema discovery provider for a given file.
    
    This function selects the appropriate schema discovery provider based on the
    file extension of the provided file path. It uses the DISCOVERY_REGISTRY
    dictionary to map file extensions to provider classes.
    
    The function handles the instantiation of the provider with the file path
    and any provided arguments, returning a ready-to-use provider instance.
    
    Args:
        file_path (Path): The path to the file for which to get a discovery provider.
            The file extension determines which provider is selected.
        args (Optional[Dict[str, Any]]): Optional arguments to pass to the provider.
            These arguments are format-specific and are passed to the underlying
            data reading functions. Defaults to None.
            
    Returns:
        SchemaDiscoveryProvider: An instance of the appropriate schema discovery
            provider for the given file type, initialized with the provided file
            path and arguments.
            
    Raises:
        ValueError: If no discovery provider is registered for the file extension.
        
    Examples:
        >>> from pathlib import Path
        >>> provider = get_discovery_provider(Path("data/sample.csv"))
        >>> schema = provider.discover_schema()
        
        >>> # With custom arguments
        >>> provider = get_discovery_provider(
        ...     Path("data/sample.csv"),
        ...     args={"delimiter": ",", "encoding": "utf-8"}
        ... )
        >>> schema = provider.discover_schema()
    """
    ext = file_path.suffix.lower().lstrip(".")
    provider_class = DISCOVERY_REGISTRY.get(ext)
    if not provider_class:
        raise ValueError(f"No discovery provider registered for extension: {ext}")
    return provider_class(source=file_path, args=args or {})
