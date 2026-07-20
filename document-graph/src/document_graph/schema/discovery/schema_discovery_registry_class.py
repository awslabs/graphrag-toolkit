# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Discovery Registry Class for notebook 24 compatibility."""

from typing import Dict, Type
from .schema_discovery_base import SchemaDiscoveryProvider


class SchemaDiscoveryRegistry:
    """
    Manages the registration and retrieval of schema discovery providers.

    This class provides functionality for registering schema discovery providers,
    retrieving providers by name, and listing all available providers. It serves as
    a central registry for accessing different schema discovery mechanisms.
    """
    
    def __init__(self):
        self._providers: Dict[str, Type[SchemaDiscoveryProvider]] = {}
    
    def register_provider(self, name: str, provider_class: Type[SchemaDiscoveryProvider]):
        """
        Registers a schema discovery provider with the specified name.

        This method associates a provider class with a given name, allowing the
        provider to be referenced and used later.

        Args:
            name: The name to register the provider under.
            provider_class: The class of the schema discovery provider to register.
        """
        self._providers[name] = provider_class
    
    def get_provider(self, name: str) -> Type[SchemaDiscoveryProvider]:
        """
        Retrieve a schema discovery provider by its name.

        This method looks up and returns a schema discovery provider associated with
        the provided name. If the name does not correspond to any registered provider,
        a KeyError is raised.

        Parameters:
        name (str): The name of the schema discovery provider to retrieve.

        Raises:
        KeyError: If the specified provider name is not found among registered providers.

        Returns:
        Type[SchemaDiscoveryProvider]: The schema discovery provider associated with
        the specified name.
        """
        if name not in self._providers:
            raise KeyError(f"Unknown discovery provider: {name}")
        return self._providers[name]
    
    def list_providers(self) -> list:
        """
        Returns a list of registered providers.

        This method retrieves all the registered providers from the internal
        providers dictionary and returns their keys as a list.

        Returns:
            list: List containing the names of all registered providers.
        """
        return list(self._providers.keys())