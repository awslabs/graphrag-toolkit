from typing import Type, Dict
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class ReaderProviderRegistry:
    """
    A registry for document reader providers.

    This class maintains a mapping of provider type names to their corresponding
    implementation classes, allowing for dynamic provider lookup and instantiation.
    """
    _registry: Dict[str, Type[ReaderProvider]] = {}

    @classmethod
    def register(cls, provider_type: str, provider_cls: Type[ReaderProvider]):
        """
        Register a reader provider class with the registry.

        Args:
            provider_type: A unique string identifier for the provider type
            provider_cls: The provider class to register

        Raises:
            ValueError: If a provider with the same type is already registered
        """
        if provider_type in cls._registry:
            raise ValueError(f"Reader provider '{provider_type}' is already registered.")
        cls._registry[provider_type] = provider_cls

    @classmethod
    def get(cls, provider_type: str) -> Type[ReaderProvider]:
        """
        Retrieve a registered provider class by its type identifier.

        Args:
            provider_type: The string identifier for the provider type

        Returns:
            Type[ReaderProvider]: The registered provider class

        Raises:
            ValueError: If no provider is registered with the given type
        """
        provider_cls = cls._registry.get(provider_type)
        if provider_cls is None:
            raise ValueError(f"Reader provider '{provider_type}' is not registered.")
        return provider_cls
