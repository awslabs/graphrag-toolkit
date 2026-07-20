# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Registry for extract provider implementations.

Maintains a mapping of provider type names to their concrete implementations.
New extract providers should be registered here to be available through
the ExtractProviderFactory.

This module provides a registry for extract provider classes, allowing them to be
dynamically registered and retrieved by name or type. The registry is a central
component of the extract framework, enabling the factory to instantiate providers
without direct dependencies on specific implementations.

The registry follows a singleton pattern, with a single instance (extract_provider_registry)
that is used throughout the application to register and retrieve provider classes.
"""

import logging
from typing import Dict, Type


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401

from .extract_provider_base import ExtractProvider



class ExtractProviderRegistry:
    """
    Provides a registry for managing extract provider classes.

    The class enables registering, retrieving, and listing extract provider classes by their type
    names. It is case-insensitive and supports runtime replacement of registered providers. This is
    commonly used to handle dynamic provider configurations in a flexible and modular way.
    """
    
    _registry: Dict[str, Type[ExtractProvider]] = {}
    
    @classmethod
    def register(cls, type_: str, provider_cls: Type[ExtractProvider]) -> None:
        """
        Registers an extract provider with a specific type.

        This method associates a provider class with a specific `type_` and
        stores it in the `_registry`. The type is stored in lowercase form
        to ensure case-insensitive lookups.

        Parameters:
        type_ (str): The case-insensitive type associated with the provider.
        provider_cls (Type[ExtractProvider]): The class of the provider to be registered.

        Returns:
        None
        """
        logger.debug(f"Registering extract provider: {type_} -> {provider_cls.__name__}")
        cls._registry[type_.lower()] = provider_cls
    
    @classmethod
    def get(cls, type_: str) -> Type[ExtractProvider]:
        """
        Retrieves an extract provider class registered for the specified type.

        This method is responsible for looking up an extract provider in the
        registry using the specified type. If no provider is found for the given
        type, a KeyError is raised.

        Args:
            type_: The type of extract provider to retrieve. The type should be
                provided as a string.

        Raises:
            KeyError: If no extract provider is registered for the specified type.

        Returns:
            A registered extract provider class corresponding to the specified type.
        """
        key = type_.lower()
        if key not in cls._registry:
            raise KeyError(f"No extract provider registered for type='{type_}'")
        return cls._registry[key]
    
    @classmethod
    def list_providers(cls) -> list[str]:
        """
        Provides a method to list all registered providers.

        Returns:
            list[str]: A list containing the names of all providers currently
            registered in the class-level registry.
        """
        return list(cls._registry.keys())


# Initialize registry instance
extract_provider_registry = ExtractProviderRegistry()
