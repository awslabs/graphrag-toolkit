# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Provider Base Module for Document Graph Operations.

This module defines the abstract base class that all schema providers must implement.
Schema providers are responsible for loading ETL schemas from various sources such as
files, databases, and cloud services.

The module includes the following components:
- SchemaProviderBase: Abstract base class for all schema providers

Schema providers are used to load ETL schemas that define how data should be extracted,
transformed, and loaded into a document graph. Different providers handle different
source types (e.g., files, S3, databases) and formats (e.g., YAML, JSON, CSV).

Usage:
    # Get a schema provider for a specific configuration
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "file",
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

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Type


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
from document_graph.schema.etl_schema_model import ETLSchema


class SchemaProviderBase(ABC):
    """
    Abstract base class for all ETL schema providers.

    This class defines the interface that all schema providers must implement.
    Schema providers are responsible for loading ETL schemas from various sources
    including files, databases, and cloud services. Each provider implementation
    handles a specific source type or format.

    The SchemaProviderBase class enforces a consistent interface across all providers,
    ensuring that they can be used interchangeably in the document graph ETL pipeline.
    Concrete implementations must provide methods for loading schemas and retrieving
    schema identifiers.

    Attributes:
        None directly in the base class. Subclasses typically have:
            config: Configuration for the provider, usually a SchemaProviderConfig instance.
            schema_id: A unique identifier for the schema being provided.
    """

    @classmethod
    @abstractmethod
    def from_config(cls: Type["SchemaProviderBase"], config: Dict[str, Any]) -> "SchemaProviderBase":
        """
        Factory method to construct a provider from a raw config dictionary.

        This method is responsible for parsing the configuration dictionary and creating
        an instance of the appropriate schema provider. It validates the configuration
        parameters and raises appropriate exceptions if the configuration is invalid.

        The configuration dictionary typically includes:
        - provider_type: The type of provider (e.g., "file", "s3", "glue")
        - schema_id: A unique identifier for the schema
        - connection_config: Provider-specific connection parameters

        Args:
            config: A dictionary containing configuration parameters for the provider.
                   The exact structure depends on the provider implementation.

        Returns:
            An instance of the implementing SchemaProviderBase subclass, properly
            configured according to the provided configuration.

        Raises:
            ValueError: If the configuration is invalid or missing required parameters.
            TypeError: If the configuration contains parameters of the wrong type.
            Other exceptions may be raised by specific provider implementations.
        """
        pass

    @abstractmethod
    def load_schema(self, source = None, **kwargs) -> ETLSchema:
        """
        Load and return the ETL schema as a Pydantic model.
        
        This method is responsible for loading the ETL schema from the source specified
        in the provider's configuration. It reads the schema definition, validates it,
        and returns it as an ETLSchema object. The schema defines how data should be
        extracted, transformed, and loaded into a document graph.
        
        The method may perform additional processing such as:
        - Validating the schema against a schema definition
        - Enriching the schema with additional information
        - Converting between different schema formats
        
        Args:
            source: Optional DocumentGraphSource containing data source and registration info.
                   If provided, this may override the source specified in the provider's
                   configuration.
            **kwargs: Additional provider-specific parameters that control how the schema
                     is loaded and processed.
        
        Returns:
            ETLSchema: A complete ETL schema object that defines how data should be
                      extracted, transformed, and loaded into a document graph.
        
        Raises:
            FileNotFoundError: If the schema source file does not exist.
            ValueError: If the schema is invalid or cannot be parsed.
            Other exceptions may be raised by specific provider implementations.
        """
        pass

    @abstractmethod
    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema that this provider
        is responsible for. The schema ID is used to reference the schema in the
        document graph ETL pipeline and to identify the schema in storage systems.
        
        The schema ID should be unique within the context of the application and
        should be consistent across multiple invocations of the provider for the
        same schema source.
        
        Returns:
            str: A unique identifier for the schema. This is typically derived from
                the schema source (e.g., filename, database table) and may include
                additional information such as version numbers or timestamps.
        """
        pass
