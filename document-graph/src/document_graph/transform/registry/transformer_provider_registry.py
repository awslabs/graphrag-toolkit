# Copyright (c) Evan Erwee. All rights reserved.

"""Transformer Provider Registry module for document graph operations.

This module provides a registry system for managing transformer provider implementations
in the document graph processing system. It supports:

- Registration of transformer providers with unique identifiers
- Lookup of transformer providers by name
- Listing of all available transformer providers

The registry pattern implemented here enables a plugin architecture where transformer
providers can be registered dynamically and retrieved by name, facilitating extensibility
and decoupling between components that define transformers and those that use them.
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Dict, Type

from document_graph.transform.transformer_provider_base import TransformerProvider


class TransformerProviderRegistry:
    """
    A registry to hold and manage all available transformers providers.

    This allows for plugin-style registration and lookup of transformers classes
    by name, which is useful for managing normalizers, enrichers, truncators, etc.
    """

    _registry: Dict[str, Type[TransformerProvider]] = {}

    @classmethod
    def register(cls, name: str, provider_class: Type[TransformerProvider]) -> None:
        """
        Register a transformers provider with a given name.

        Args:
            name (str): The unique identifier for the provider.
            provider_class (Type[TransformerProvider]): The provider class.
        """
        if name in cls._registry:
            raise ValueError(f"Transformer provider '{name}' is already registered.")
        cls._registry[name] = provider_class

    @classmethod
    def get(cls, name: str) -> Type[TransformerProvider]:
        """
        Retrieve a transformers provider by name.

        Args:
            name (str): The name of the registered provider.

        Returns:
            Type[TransformerProvider]: The provider class.
        """
        if name not in cls._registry:
            raise ValueError(f"Transformer provider '{name}' not found in registry.")
        return cls._registry[name]

    @classmethod
    def list_registered(cls) -> Dict[str, Type[TransformerProvider]]:
        """
        List all registered transformers providers.

        Returns:
            Dict[str, Type[TransformerProvider]]: All registered providers.
        """
        return dict(cls._registry)
