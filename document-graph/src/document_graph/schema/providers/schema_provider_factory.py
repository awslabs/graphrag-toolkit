# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Provider Factory Module for Document Graph Operations.

This module provides a factory for creating schema providers based on configuration
parameters. It centralizes the instantiation of different schema provider types and
ensures that the appropriate provider is created based on the specified type.

The module includes the following components:
- SchemaProviderFactory: Factory class for creating schema providers

The SchemaProviderFactory maintains a registry of available schema provider types
and provides methods for registering new provider types and creating provider instances.
It supports various provider types including file-based, S3-based, and database-based
providers.

Usage:
    # Create a schema provider using a configuration dictionary
    from document_graph.schema.providers.schema_provider_factory import SchemaProviderFactory
    
    config = {
        "type": "file",
        "schema_id": "my_schema",
        "connection_config": {
            "path": "/path/to/schema.yaml"
        }
    }
    
    provider = SchemaProviderFactory.create(config)
    
    # Or use the convenience function
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    provider = get_schema_provider(config)
    
    # Load the schema
    schema = provider.pipeline_schema()
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Dict, Any, Type
from document_graph.schema.providers.schema_provider_base import SchemaProviderBase

from document_graph.schema.providers.file_schema_provider import FileSchemaProvider
from document_graph.schema.providers.s3_schema_provider import S3SchemaProvider
from document_graph.schema.providers.csv_schema_provider import CSVSchemaProvider
from document_graph.schema.providers.glue_schema_provider import GlueSchemaProvider
from document_graph.schema.providers.parquet_schema_provider import ParquetSchemaProvider
from document_graph.schema.providers.json_schema_provider import JSONSchemaProvider
from document_graph.schema.providers.excel_schema_provider import ExcelSchemaProvider
from document_graph.schema.providers.yaml_schema_provider import YAMLSchemaProvider
from document_graph.schema.providers.xml_schema_provider import XMLSchemaProvider


class SchemaProviderFactory:
    """
    Factory class to instantiate schema providers based on a configuration dictionary.
    
    This class provides a centralized way to create schema provider instances based on
    a configuration dictionary. It maintains a registry of available provider types and
    maps them to their corresponding provider classes. The factory ensures that the
    appropriate provider is created based on the specified type in the configuration.
    
    The factory supports various provider types including:
    - file: For loading schemas from local files
    - s3: For loading schemas from Amazon S3
    - static: For using predefined schemas
    - csv: For generating schemas from CSV files
    - json: For generating schemas from JSON files
    - excel: For generating schemas from Excel files
    - glue: For loading schemas from AWS Glue
    - parquet: For generating schemas from Parquet files
    - yaml: For generating schemas from YAML files
    - xml: For generating schemas from XML files
    
    New provider types can be registered using the register_provider method.
    
    Attributes:
        _registry (Dict[str, Type[SchemaProviderBase]]): A mapping of provider type names
            to their corresponding provider classes.
    """

    _registry: Dict[str, Type[SchemaProviderBase]] = {
        "file": FileSchemaProvider,
        "s3": S3SchemaProvider,
        "csv": CSVSchemaProvider,
        "json": JSONSchemaProvider,
        "excel": ExcelSchemaProvider,
        "glue": GlueSchemaProvider,
        "parquet": ParquetSchemaProvider,
        "yaml": YAMLSchemaProvider,
        "xml": XMLSchemaProvider,
    }
    
    @classmethod
    def _get_static_provider(cls):
        """Late import for StaticSchemaProvider to avoid circular imports."""
        from document_graph.schema.static_schema_provider import StaticSchemaProvider
        return StaticSchemaProvider

    @classmethod
    def register_provider(cls, type_name: str, provider_class: Type[SchemaProviderBase]) -> None:
        """
        Register a new schema provider class.
        
        This method allows for the registration of custom schema provider classes
        that are not included in the default registry. Once registered, the provider
        can be created using the factory's create method by specifying the registered
        type name in the configuration.
        
        Args:
            type_name: A unique string identifier for the provider type. This will be
                      used as the "type" value in configuration dictionaries to specify
                      that this provider should be used.
            provider_class: The schema provider class to register. This must be a subclass
                           of SchemaProviderBase and implement the required interface,
                           including the from_config class method.
                           
        Returns:
            None
            
        Example:
            # Register a custom provider
            from my_package import MyCustomProvider
            SchemaProviderFactory.register_provider("custom", MyCustomProvider)
            
            # Use the custom provider
            config = {"type": "custom", "schema_id": "my_schema", "connection_config": {...}}
            provider = SchemaProviderFactory.create(config)
        """
        cls._registry[type_name] = provider_class

    @classmethod
    def create(cls, config: Dict[str, Any]) -> SchemaProviderBase:
        """
        Instantiate a schema provider based on the `type` in the config.
        
        This method creates an instance of the appropriate schema provider based on
        the "type" specified in the configuration dictionary. It validates that the
        type is registered and that the provider class implements the required interface.
        
        The method performs the following steps:
        1. Extracts the "type" from the configuration
        2. Validates that the type is registered in the factory
        3. Gets the provider class from the registry
        4. Validates that the provider class implements the required interface
        5. Converts the configuration dictionary to a SchemaProviderConfig object
        6. Creates and returns an instance of the provider using the from_config method
        
        Args:
            config: A dictionary containing the configuration for the schema provider.
                   Must include a "type" key with a value that is registered in the factory.
                   Other required keys depend on the specific provider type.
        
        Returns:
            An instance of a SchemaProviderBase subclass, configured according to the
            provided configuration.
            
        Raises:
            ValueError: If the "type" is missing or not registered in the factory.
            TypeError: If the provider class does not implement the required interface.
            Other exceptions may be raised by the provider's from_config method.
        """
        type_name = config.get("type")
        
        # Handle static provider with late import to avoid circular dependency
        if type_name == "static":
            provider_class = cls._get_static_provider()
        elif type_name and type_name in cls._registry:
            provider_class = cls._registry[type_name]
        else:
            available_types = list(cls._registry.keys()) + ["static"]
            raise ErrorHandler.validation_error(
                "schema_provider_type",
                type_name or "None",
                f"one of {available_types}"
            )

        # provider_class is already set above

        if not hasattr(provider_class, "from_config"):
            raise TypeError(f"{provider_class.__name__} is missing required `from_config(config)` method.")

        # Convert dict to SchemaProviderConfig object
        from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
        config_obj = SchemaProviderConfig(**config)
        return provider_class.from_config(config_obj)
