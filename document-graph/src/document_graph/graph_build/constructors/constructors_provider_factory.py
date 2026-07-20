# Copyright (c) Evan Erwee. All rights reserved.
"""Constructors provider factory — creates constructor provider instances from config."""

from document_graph.graph_build.constructors.constructors_provider_registry import ConstructorProviderRegistry
from document_graph.graph_build.constructors.constructors_provider_config import ConstructorProviderConfig

class ConstructorProviderFactory:
    """
    Factory class for creating instances of constructor providers.

    This class provides a method to create a specific constructor provider
    instance based on the configuration provided. It utilizes a registry to
    look up the appropriate constructor provider based on the type specified
    in the configuration.
    """
    
    @staticmethod
    def create(config: ConstructorProviderConfig):
        """
        This static method is responsible for creating an instance of a provider class based on the provided
        configuration. It retrieves the appropriate class from the ConstructorProviderRegistry based on the
        type specified in the configuration and initializes it using the given configuration.

        Parameters:
            config (ConstructorProviderConfig): The configuration object used to determine the provider
            type and to initialize the provider instance.

        Returns:
            Any: An instance of the provider class corresponding to the type defined in the configuration.
        """
        provider_class = ConstructorProviderRegistry.get(config.type)
        return provider_class(config)