from document_graph.ingest.ingestors_provider_registry import IngestorProviderRegistry
from document_graph.ingest.ingestors_provider_config import IngestorProviderConfig

class IngestorProviderFactory:
    """
    Provides a factory for creating instances of ingestor providers based on a
    given configuration.

    This class serves as a factory for instantiating ingestor providers that
    are registered in the `IngestorProviderRegistry`. It uses the type specified
    in the configuration to determine the appropriate provider class and returns
    an instance of it.
    """
    
    @staticmethod
    def create(config: IngestorProviderConfig):
        """
        Creates an instance of an ingestor provider based on the provided configuration.

        This method retrieves the appropriate ingestor provider class from the
        IngestorProviderRegistry based on the type specified in the given configuration.
        It then initializes an instance of this class using the configuration and
        returns the created instance.

        Args:
            config (IngestorProviderConfig): Configuration object containing
                details necessary for selecting and initializing the ingestor provider.

        Returns:
            IngestorProvider: An instance of the provider class initialized with
            the given configuration.
        """
        provider_class = IngestorProviderRegistry.get(config.type)
        return provider_class(config)
