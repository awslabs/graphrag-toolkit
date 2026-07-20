# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Discovery Base Module for Document Graph Operations.

This module provides the foundation for schema discovery in the document graph system.
It defines the abstract base class that all schema discovery providers must implement.
Schema discovery is the process of inferring a structured ETL schema from raw source data,
which is essential for properly processing and loading data into the document graph.

The module includes the following components:
- SchemaDiscoveryProvider: Abstract base class for all schema discovery providers

Schema discovery providers are responsible for examining source data files and
determining their structure, field types, and other metadata needed to create
a valid ETL schema. This schema is then used to guide the extraction, transformation,
and loading of data into the document graph.

Usage:
    # Get a discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    file_path = Path("data/sample.csv")
    provider = get_discovery_provider(file_path)
    
    # Discover the schema
    schema = provider.discover_schema()
    
    # Use the schema for ETL operations
    print(f"Discovered schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Dict, Any, Optional

from document_graph.schema.etl_schema_model import ETLSchema


class SchemaDiscoveryProvider(ABC):
    """
    Base class for all schema discovery providers.
    
    This abstract base class defines the interface that all schema discovery providers
    must implement. Schema discovery providers are responsible for examining source
    data files and inferring a structured ETL schema that can be used for document
    graph operations.
    
    The provider analyzes the structure, field types, and other metadata of the source
    data to create an appropriate ETL schema. This schema is then used to guide the
    extraction, transformation, and loading of data into the document graph.
    
    Subclasses should implement the discover_schema method to provide format-specific
    schema discovery logic.
    
    Attributes:
        source (Path): The path to the source data file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            These arguments are format-specific and are passed to the underlying
            data reading functions.
    """

    def __init__(self, source: Union[str, Path], args: Optional[Dict[str, Any]] = None):
        """
        Initialize the schema discovery provider.
        
        This constructor sets up the provider with the source file path and any
        format-specific arguments needed for the discovery process. It validates
        that the source file exists before proceeding.
        
        Args:
            source (Union[str, Path]): The path to the source data file to analyze.
                Can be provided as a string or a Path object.
            args (Optional[Dict[str, Any]]): Optional arguments that control the
                discovery process. These arguments are format-specific and are
                passed to the underlying data reading functions. Defaults to None.
                
        Raises:
            FileNotFoundError: If the specified source file does not exist.
        """
        self.source = Path(source)
        self.args = args or {}

        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

    @abstractmethod
    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from the source data.
        
        This abstract method must be implemented by subclasses to provide
        format-specific schema discovery logic. The implementation should
        analyze the source data file and infer an appropriate ETL schema
        that captures its structure and field types.
        
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the source data and how it should be processed.
                
        Raises:
            Various exceptions: Implementations may raise format-specific exceptions
                when the source data cannot be parsed or does not meet expected
                format requirements.
        """
        pass
