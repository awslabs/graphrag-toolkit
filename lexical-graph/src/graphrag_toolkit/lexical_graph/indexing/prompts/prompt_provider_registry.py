# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional, Dict
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class PromptProviderRegistry:
    """
    Global registry for managing and retrieving named PromptProvider instances.
    Supports multiple sources (e.g., Bedrock, S3, File) and default fallback.
    """

    _registry: Dict[str, PromptProvider] = {}
    _default_provider_name: Optional[str] = None

    @classmethod
    def register(cls, name: str, provider: PromptProvider, default: bool = False) -> None:
        """
        Register a prompt provider under a unique name.
        Optionally, set it as the default provider.

        Parameters
        ----------
        name : str
            The unique name for the provider (e.g., "aws-prod", "local-dev").
        provider : PromptProvider
            The provider instance to register.
        default : bool
            Whether to make this the default provider.
        """
        cls._registry[name] = provider
        if default or cls._default_provider_name is None:
            cls._default_provider_name = name

    @classmethod
    def get(cls, name: Optional[str] = None) -> Optional[PromptProvider]:
        """
        Retrieve a prompt provider by name, or return the default if no name is specified.

        Parameters
        ----------
        name : Optional[str]
            The name of the provider to retrieve.

        Returns
        -------
        Optional[PromptProvider]
            The matching provider instance or None.
        """
        if name:
            return cls._registry.get(name)
        if cls._default_provider_name:
            return cls._registry.get(cls._default_provider_name)
        return None

    @classmethod
    def list_registered(cls) -> Dict[str, PromptProvider]:
        """
        List all registered prompt providers.

        Returns
        -------
        Dict[str, PromptProvider]
            A dictionary of provider names and their instances.
        """
        return cls._registry.copy()
