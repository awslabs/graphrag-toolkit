# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Base module for document graph operations."""

import logging
from abc import ABC, abstractmethod
from document_graph.pipeline.extract.extract_provider_config import ExtractProviderConfig
from document_graph.config import DocumentGraphConfig
from document_graph.pipeline.extract.extraction_result import ExtractionResult
from document_graph.schema.etl_schema_model import ETLSchema


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401



class ExtractProvider(ABC):
    """
    Abstract base class for an extract provider.

    This class serves as a blueprint for creating extract providers that perform
    ETL (Extract, Transform, Load) operations on structured document nodes.
    The main purpose of this class is to define the interface and shared
    functionality for concrete implementations. Subclasses are required to
    implement the `extract` method.

    Attributes:
        config: Extract provider configuration.
        aws_config: AWS configuration for cloud operations.
    """
    
    def __init__(self, config: ExtractProviderConfig, aws_config: DocumentGraphConfig):
        """Initialize extract provider with configuration.
        
        Args:
            config: Extract provider configuration
            aws_config: AWS configuration for cloud operations
        """
        self.config = config
        self.aws_config = aws_config
        logger.debug(f"Initialized {self.__class__.__name__} with config: {config.type}")

    @abstractmethod
    def extract(self, source: str, **kwargs) -> ExtractionResult:
        """
        Represents an abstract base class for data extraction.

        This class defines an interface for extracting data from a given source string.
        Subclasses are required to implement the `extract` method.

        Methods:
            extract(source: str, **kwargs) -> ExtractionResult

            Abstract method intended to be implemented by subclasses. This method
            extracts specific data from a provided string source using potential
            additional arguments.

        Raises:
            NotImplementedError: If the method is not implemented in a deriving class.

        """
        pass
    
    def get_etl_schema(self) -> ETLSchema:
        """
        Returns the ETL schema for the current configuration.

        The method retrieves the ETL schema object by loading it from the
        config if it hasn't been loaded already.

        Returns:
            ETLSchema: The schema object representing the ETL schema.
        """
        return self.config.load_schema_if_needed()