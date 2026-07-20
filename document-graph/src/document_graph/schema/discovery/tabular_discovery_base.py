# Copyright (c) Evan Erwee. All rights reserved.

"""Tabular Discovery Base Module for Document Graph Operations.

This module provides a base class for schema discovery providers that work with
tabular data formats such as CSV, Excel, Parquet, and similar structured data.
It extends the core schema discovery functionality with tabular-specific features.

The module includes the following components:
- TabularSchemaDiscoveryProvider: Base class for tabular schema discovery providers

Tabular schema discovery providers are specialized for handling data formats that
have a row-column structure. They share common patterns for extracting column headers,
inferring data types, and creating appropriate ETL schemas for tabular data.

This module serves as an intermediate layer between the abstract SchemaDiscoveryProvider
and concrete implementations for specific tabular formats like CSV, Excel, and Parquet.

Usage:
    # This class is not typically used directly, but through its subclasses
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    # Get a provider for a specific tabular format
    csv_provider = get_discovery_provider("data/sample.csv")
    excel_provider = get_discovery_provider("data/sample.xlsx")
    parquet_provider = get_discovery_provider("data/sample.parquet")
    
    # All these providers inherit from TabularSchemaDiscoveryProvider
    # and share common behavior for tabular data
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from pathlib import Path
from typing import Optional, Dict, Any

from document_graph.schema.discovery.schema_discovery_base import SchemaDiscoveryProvider


class TabularSchemaDiscoveryProvider(SchemaDiscoveryProvider):
    """
    Base class for tabular schema discovery providers like CSV, Excel, and Parquet.
    
    This class extends the SchemaDiscoveryProvider to provide common functionality
    for tabular data formats. Tabular data is characterized by a row-column structure
    where each row represents a record and each column represents a field.
    
    Concrete subclasses implement format-specific logic for reading and analyzing
    tabular data sources like CSV, Excel, and Parquet files. These subclasses share
    common patterns for extracting column headers, inferring data types, and creating
    appropriate ETL schemas.
    
    The TabularSchemaDiscoveryProvider simplifies the implementation of concrete
    providers by handling common aspects of tabular data processing, allowing
    subclasses to focus on format-specific details.
    
    Attributes:
        source (Path): The path to the tabular data file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            These arguments are format-specific and are passed to the underlying
            data reading functions (e.g., pandas.read_csv, pandas.read_excel).
    """
    def __init__(self, source: Path, args: Optional[Dict[str, Any]] = None):
        """
        Initialize the tabular schema discovery provider.
        
        This constructor sets up the provider with the source file path and any
        format-specific arguments needed for the discovery process. Unlike the
        parent class, it does not validate that the source file exists, leaving
        that responsibility to the concrete subclasses.
        
        Args:
            source (Path): The path to the tabular data file to analyze.
            args (Optional[Dict[str, Any]]): Optional arguments that control the
                discovery process. These arguments are format-specific and are
                passed to the underlying data reading functions (e.g., pandas.read_csv,
                pandas.read_excel). Defaults to None.
        """
        self.source = Path(source)
        self.args = args or {}
