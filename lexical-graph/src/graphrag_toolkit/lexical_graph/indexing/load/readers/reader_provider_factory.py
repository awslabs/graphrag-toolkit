"""
Factory module for creating document reader providers based on configuration.

This module provides a factory function to instantiate the appropriate reader provider
based on the provided configuration, handling both registry-based and special case providers.
"""

from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_base import ReaderProvider
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_registry import ReaderProviderRegistry
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import ReaderProviderConfig
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

def get_reader_provider(config: ReaderProviderConfig) -> ReaderProvider:
    """
    Create and return a reader provider instance based on the provided configuration.

    Args:
        config: The configuration object specifying the provider type and parameters

    Returns:
        ReaderProvider: An instantiated reader provider

    Raises:
        NotImplementedError: If a provider is referenced but not implemented
        ValueError: If the provider type is unknown or not registered
    """

    # Example placeholder for future special case:
    if config.type == "bedrock_s3_txt":
        raise NotImplementedError("BedrockS3TxtProvider is not implemented yet")

    provider_class = ReaderProviderRegistry.get(config.type)
    if provider_class:
        return provider_class(config)

    raise ValueError(f"Unknown reader provider type: {config.type}")
