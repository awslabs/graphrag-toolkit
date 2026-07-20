# Copyright (c) Evan Erwee. All rights reserved.

"""Transformer Provider Factory module for document graph operations.

This module provides a factory for creating transformer provider instances
based on configuration. It handles the resolution of transformer types and names,
instantiates the appropriate provider classes, and manages the configuration
process. The factory works with the transformer provider registry to locate
and instantiate the correct transformer implementation.
"""

import logging
from typing import Type
from document_graph.transform.transformer_provider_base import TransformerProvider
from document_graph.transform.transformer_provider_registry import transformer_provider_registry
from document_graph.transform.transformer_provider_config import TransformerProviderConfig

logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

class TransformerProviderFactory:
    """Factory to instantiate transformers providers based on config.
    
    Creates transformers instances for use in transformation pipelines
    and plugin systems. The factory resolves transformer types and names,
    retrieves the appropriate provider class from the registry, and
    instantiates it with the provided configuration.
    
    Examples:
        >>> config = TransformerProviderConfig(
        ...     name="text_normalizer",
        ...     type="normalizer",
        ...     args={"lowercase": True}
        ... )
        >>> provider = TransformerProviderFactory.get_provider(config)
        >>> isinstance(provider, TransformerProvider)
        True
    """

    @staticmethod
    def get_provider(config: TransformerProviderConfig) -> TransformerProvider:
        """Instantiate the appropriate transformers provider.
        
        Args:
            config: Transformer configuration with type and parameters
            
        Returns:
            TransformerProvider: Configured transformers instance
            
        Raises:
            KeyError: If transformers type is not registered
            
        Examples:
            >>> config = TransformerProviderConfig(
            ...     name="entity_enricher",
            ...     type="enricher",
            ...     args={"entities": ["person", "organization"]}
            ... )
            >>> provider = TransformerProviderFactory.get_provider(config)
            >>> provider.transform({"text": "John works at Amazon"})
            {'text': 'John works at Amazon', 'entities': [...]}
        """
        logger.debug(f"Creating transformers provider: {config.type or config.name}")
        
        # Use name for lookup when type is a directory-based category, otherwise use type
        directory_types = ["transformer", "normalizer", "enricher", "truncator", 
                          "field_transformer", "document_transformer", "filter_transformer", 
                          "graph_transformer"]
        if config.type in directory_types:
            lookup_key = config.name
            logger.debug(f"Using name '{lookup_key}' for category type '{config.type}'")
        else:
            lookup_key = config.type or config.name
            logger.debug(f"Using direct lookup key '{lookup_key}'")
        
        logger.debug(f"Registry lookup for key: {lookup_key}")
        logger.debug(f"Available providers: {transformer_provider_registry.list_providers()}")
        
        provider_cls: Type[TransformerProvider] = transformer_provider_registry.get(lookup_key)
        
        provider = provider_cls(config)
        logger.debug(f"Successfully created transformers: {provider.__class__.__name__}")
        return provider
    
    @staticmethod
    def create_config(
        name: str,
        transformer_type: str = None,
        **args
    ) -> TransformerProviderConfig:
        """Create a transformers configuration.
        
        Args:
            name: Transformer name
            transformer_type: Optional transformers type for factory lookup
            **args: Transformer-specific arguments
            
        Returns:
            TransformerProviderConfig: Transformer configuration
            
        Examples:
            >>> config = TransformerProviderFactory.create_config(
            ...     name="text_normalizer",
            ...     transformer_type="normalizer",
            ...     lowercase=True,
            ...     remove_punctuation=True
            ... )
            >>> config.name
            'text_normalizer'
            >>> config.type
            'normalizer'
            >>> config.args["lowercase"]
            True
        """
        return TransformerProviderConfig(
            name=name,
            type=transformer_type,
            args=args
        )
