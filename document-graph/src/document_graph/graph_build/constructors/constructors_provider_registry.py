# Copyright (c) Evan Erwee. All rights reserved.
"""Constructors provider registry — centralized registry of constructor provider classes."""

from typing import Dict, Type
from document_graph.graph_build.constructors.constructors_provider_base import ConstructorProvider

class ConstructorProviderRegistry:
    """
    Manages the registration and access of constructor providers.

    This class serves as a centralized registry for constructor provider classes,
    allowing registration, retrieval, and listing of available providers. It is
    commonly used to dynamically manage and access different provider classes
    by their unique names.
    """
    
    _providers: Dict[str, Type[ConstructorProvider]] = {}
    
    @classmethod
    def register(cls, name: str, provider_class: Type[ConstructorProvider]):
        """
        Registers a provider with the given name.

        This class method allows registering a new provider by associating a name with
        a provider class.

        Parameters:
        name: str
            The unique name used to identify the provider.
        provider_class: Type[ConstructorProvider]
            A class representing the provider to be registered, typically inheriting
            from ConstructorProvider.

        Raises:
        KeyError
            If the provided name is already registered in the providers.
        """
        cls._providers[name] = provider_class
    
    @classmethod
    def get(cls, name: str) -> Type[ConstructorProvider]:
        """
        Retrieves a constructor provider based on the provided name.

        This method accesses a registry of constructor providers and retrieves the provider
        corresponding to the given name. If the name is not found within the registry, an
        exception is raised.

        Args:
            name (str): The name of the desired constructor provider.

        Returns:
            Type[ConstructorProvider]: The constructor provider associated with the given name.

        Raises:
            ValueError: If the specified name is not found in the registry of constructor providers.
        """
        if name not in cls._providers:
            raise ValueError(f"Unknown constructor provider: {name}")
        return cls._providers[name]
    
    @classmethod
    def list_providers(cls) -> list:
        """
        Provides functionality to retrieve the list of available providers.

        This method is a class-level method that retrieves and returns a list of
        all the available providers registered within the class.

        Returns:
            list: A list containing the keys of the registered providers within
            the class.
        """
        return list(cls._providers.keys())