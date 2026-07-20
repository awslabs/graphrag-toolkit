# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Provider Registry Module for Document Graph Operations.

This module provides a registry system for schema providers, allowing for runtime
registration and lookup of provider classes by name. It centralizes the management
of available schema provider types and ensures that providers can be dynamically
added and retrieved.

The module includes the following components:
- SchemaProviderRegistry: Registry class for managing schema provider classes

The SchemaProviderRegistry maintains a dictionary of provider names to provider classes
and provides methods for registering new providers, retrieving providers by name, and
listing all available providers. This registry is used by the schema provider factory
to look up provider classes based on configuration.

Usage:
    # Create a registry and register providers
    from document_graph.schema.providers.schema_provider_registry import SchemaProviderRegistry
    from document_graph.schema.providers.file_schema_provider import FileSchemaProvider
    from document_graph.schema.providers.s3_schema_provider import S3SchemaProvider
    
    registry = SchemaProviderRegistry()
    registry.register("file", FileSchemaProvider)
    registry.register("s3", S3SchemaProvider)
    
    # Look up a provider by name
    provider_class = registry.get("file")
    
    # List all registered providers
    available_providers = registry.list()
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Dict, Type
from document_graph.schema.providers.schema_provider_base import SchemaProviderBase


class SchemaProviderRegistry:
    """
    Central registry for all schema provider classes.
    
    This class provides a registry system for schema provider classes, allowing for
    runtime registration and lookup of providers by name. It centralizes the management
    of available schema provider types and ensures that providers can be dynamically
    added and retrieved.
    
    The registry maintains a dictionary mapping provider names to provider classes and
    provides methods for registering new providers, retrieving providers by name, and
    listing all available providers. This registry is used by the schema provider factory
    to look up provider classes based on configuration.
    
    Attributes:
        _registry (Dict[str, Type[SchemaProviderBase]]): A dictionary mapping provider
            names to provider classes.
    """

    def __init__(self):
        """
        [Brief one-line description of the function's purpose]
        
        [Detailed explanation of what the function does and how it works]
        
        Returns:
            [return_type]: [Description of return value]
        """
        self._registry: Dict[str, Type[SchemaProviderBase]] = {}

    def register(self, name: str, provider_cls: Type[SchemaProviderBase], overwrite: bool = False) -> None:
        """
        Register a new schema provider class.
        
        This method adds a new schema provider class to the registry, associating it
        with the specified name. The provider class must be a subclass of SchemaProviderBase.
        By default, attempting to register a provider with a name that already exists
        in the registry will raise an error, but this behavior can be overridden with
        the overwrite parameter.
        
        Args:
            name: Provider type name (e.g., 'csv', 's3'). This is the identifier that
                 will be used to look up the provider class.
            provider_cls: Provider implementation class. Must be a subclass of
                         SchemaProviderBase and implement the required interface.
            overwrite: Allow replacing an existing provider (default: False). If True,
                      an existing provider with the same name will be replaced.
                      
        Raises:
            ValueError: If a provider with the same name already exists and overwrite
                       is False.
                       
        Example:
            # Register a custom provider
            registry = SchemaProviderRegistry()
            registry.register("custom", MyCustomProvider)
        """
        if name in self._registry and not overwrite:
            raise ErrorHandler.validation_error(
                "schema_provider_registration",
                name,
                "a unique provider name not already registered"
            )
        self._registry[name] = provider_cls

    def get(self, name: str) -> Type[SchemaProviderBase]:
        """
        Retrieve a registered schema provider class by name.
        
        This method looks up a schema provider class in the registry by its registered
        name. If the name is not found in the registry, an error is raised with a list
        of available provider names.
        
        Args:
            name: The name of the provider to retrieve. This must be a name that has
                 been previously registered with the register method.
                 
        Returns:
            Type[SchemaProviderBase]: The schema provider class associated with the
                                     specified name.
                                     
        Raises:
            ValueError: If the specified name is not found in the registry.
            
        Example:
            # Get a provider class by name
            registry = SchemaProviderRegistry()
            provider_class = registry.get("file")
            
            # Create an instance of the provider
            provider = provider_class.from_config(config)
        """
        if name not in self._registry:
            available = list(self._registry.keys())
            raise ErrorHandler.validation_error(
                "schema_provider_lookup",
                name,
                f"one of: {available}"
            )
        return self._registry[name]

    def list(self) -> Dict[str, Type[SchemaProviderBase]]:
        """
        Return a copy of all registered providers.
        
        This method returns a dictionary containing all the schema provider classes
        currently registered in the registry. The dictionary maps provider names to
        their corresponding provider classes. A copy of the internal registry is
        returned to prevent modification of the registry through the returned dictionary.
        
        Returns:
            Dict[str, Type[SchemaProviderBase]]: A dictionary mapping provider names
                                               to their corresponding provider classes.
                                               
        Example:
            # List all registered providers
            registry = SchemaProviderRegistry()
            providers = registry.list()
            
            # Print the names of all registered providers
            for name in providers.keys():
                print(f"Provider: {name}")
        """
        return self._registry.copy()
