# Copyright (c) Evan Erwee. All rights reserved.

"""Transformer Provider Factory module for document graph operations.

This module provides a factory for dynamically instantiating transformer provider 
implementations based on configuration. It supports:

- Dynamic class loading from fully qualified paths
- Instantiation of transformer providers with appropriate configuration
- Integration with the transformer provider registry system

The factory pattern used here allows for flexible creation of transformer providers
without tight coupling to specific implementations, enabling extensibility through plugins.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


import importlib
from typing import Type

from document_graph.transform.transformer_provider_base import TransformerProvider
from document_graph.transform.transformer_provider_config import TransformerProviderConfig


class TransformerProviderFactory:
    """
    Factory for instantiating TransformerProvider implementations
    based on the configuration provided.
    
    This factory class works in conjunction with the TransformerProviderRegistry
    to create instances of transformer providers. It handles:
    
    - Dynamic loading of provider classes from their fully qualified names
    - Instantiation of provider objects with appropriate configuration
    - Error handling for missing or invalid provider implementations
    
    The factory pattern implemented here allows for flexible creation of transformer
    providers at runtime without requiring direct dependencies on specific implementations.
    """

    @staticmethod
    def load_class(path: str) -> Type[TransformerProvider]:
        """
        Dynamically import and return the class from a fully qualified path.

        Args:
            path (str): e.g., "my_module.sub_module.MyClass"

        Returns:
            Type[TransformerProvider]: The class object.
        """
        module_path, class_name = path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        klass = getattr(module, class_name)
        return klass

    @classmethod
    def get_provider(cls, config: TransformerProviderConfig) -> TransformerProvider:
        """
        Instantiate a transformers provider from its config.

        Args:
            config (TransformerProviderConfig): Configuration for the provider.

        Returns:
            TransformerProvider: Instantiated provider.
        """
        klass = cls.load_class(config.implementation)
        return klass(config=config)
