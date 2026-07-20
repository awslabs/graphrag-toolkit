"""Ingestors provider registry — centralized registry of ingestor provider classes."""
from typing import Dict, Type
from document_graph.ingest.ingestors_provider_base import IngestorProvider
from document_graph.ingest.field.numeric_id_cleanup_ingestor import NumericIdCleanupIngestor

class IngestorProviderRegistry:
    """
    Provides a registry for managing ingestor providers.

    The IngestorProviderRegistry class allows for the registration, retrieval, and
    listing of ingestor providers by name. This serves as a central management system for
    providers, enabling dynamic customization and extension of functionality.

    Methods:
        register: Registers a new ingestor provider with a specified name.
        get: Retrieves a registered ingestor provider by its name.
        list_providers: Lists all currently registered provider names.
    """
    
    _providers: Dict[str, Type[IngestorProvider]] = {
        'numeric_id_cleanup': NumericIdCleanupIngestor,
    }
    
    @classmethod
    def register(cls, name: str, provider_class: Type[IngestorProvider]):
        """
        Registers a new ingestor provider with a specified name.

        This class method allows adding a provider to the internal registry by associating it
        with a unique name. The registered ingestor provider can be used in the system for
        handling specific ingestion tasks. The uniqueness of the provider name must be ensured
        by the caller to avoid accidental overwrites.

        Parameters:
            name (str): The unique identifier for the ingestor provider.
            provider_class (Type[IngestorProvider]): The class implementing the ingestor
            provider functionality.

        Raises:
            KeyError: Raises an error if the provider name already exists in the registry.
        """
        cls._providers[name] = provider_class
    
    @classmethod
    def get(cls, name: str) -> Type[IngestorProvider]:
        """
        Retrieves an ingestor provider class by its name.

        Raises a ValueError if the provided name does not exist in the registered
        ingestor providers.

        Args:
            name: The name of the ingestor provider to retrieve.

        Returns:
            The ingestor provider class associated with the given name.

        Raises:
            ValueError: If the given name is not a valid provider.
        """
        if name not in cls._providers:
            raise ValueError(f"Unknown ingestor provider: {name}")
        return cls._providers[name]
    
    @classmethod
    def list_providers(cls) -> list:
        """
        Returns a list of available providers for the class.

        This method retrieves the keys from the internal `_providers` dictionary
        and returns them as a list. It is used to view all registered providers
        accessible for the class.

        Returns:
            list: A list containing the names of all available providers.
        """
        return list(cls._providers.keys())