# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Factory module for document graph operations."""

import logging
from typing import Type

from .extract_provider_base import ExtractProvider
from .extract_provider_config import ExtractProviderConfig
from .extract_provider_registry import extract_provider_registry
from ...document_graph_config import DocumentGraphConfig


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401



class ExtractProviderFactory:
    """
    Factory class for creating and retrieving extract provider instances.

    This class provides a mechanism for creating extract providers based on their
    configuration and type. It utilizes a registry of available providers and
    performs validation to ensure the requested provider type is supported.
    The created provider is configured according to the supplied configurations.
    """

    @classmethod
    def get_provider(
        cls,
        config: ExtractProviderConfig,
        aws_config: DocumentGraphConfig
    ) -> ExtractProvider:
        """
        Creates and returns an instance of the appropriate `ExtractProvider` subclass based on the given
        configuration and type. The provider's class is determined dynamically by looking up the
        `extract_provider_registry` with the given type. If the type is not registered, an appropriate
        validation error is raised. Logs detailed debug and informational messages for the creation process.

        Args:
            config (ExtractProviderConfig): Configuration object specifying the type of extract provider
                to create and any additional provider-specific configuration details.
            aws_config (DocumentGraphConfig): Configuration object containing AWS-related settings for
                document processing.

        Raises:
            ErrorHandler.ValidationError: If the given `provider_type` is not recognized or not registered
                in the `extract_provider_registry`.

        Returns:
            ExtractProvider: An instance of the dynamically determined subclass of `ExtractProvider`.
        """
        provider_type = config.type
        logger.debug(f"Creating extract provider: type={provider_type}, config={config.dict()}")
        logger.debug(f"Looking up provider for type: {provider_type}")
        
        try:
            provider_cls: Type[ExtractProvider] = extract_provider_registry.get(provider_type)
            logger.debug(f"Found provider class: {provider_cls.__name__}")
        except KeyError:
            from ...shared.error_handler import ErrorHandler
            available_types = extract_provider_registry.list_providers()
            logger.error(f"Unknown extract provider type: {provider_type}. Available: {available_types}")
            raise ErrorHandler.validation_error(
                "provider_type", 
                provider_type, 
                f"one of {available_types}"
            )

        logger.debug(f"Instantiating provider: {provider_cls.__name__}")
        logger.info(f"Creating extract provider of type: {provider_type}")
        provider = provider_cls(config=config, aws_config=aws_config)
        logger.debug(f"Successfully created provider: {provider.__class__.__name__}")
        return provider
