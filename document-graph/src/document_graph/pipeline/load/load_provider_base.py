# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Load Provider Base module for document graph operations."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from document_graph.pipeline.extract.extraction_result import ExtractionResult
from document_graph.pipeline.load.load_result import LoadResult
from document_graph.pipeline.load.transformers.transformation_plan import TransformationPlan


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401



class LoadProviderConfig(BaseModel):
    """
    Represents the configuration needed to load a provider.

    This class is used to define the necessary information for loading a
    specific provider, including its name, type, optional parameters, and
    optional configurations such as transformation plans or AWS-specific
    settings. It extends the BaseModel class for structured data handling.
    """
    name: str
    type: str
    parameters: Dict[str, Any] = {}
    transformation_plan: Optional[TransformationPlan] = None
    aws_config: Optional[Any] = None


class LoadProvider(ABC):
    """
    Abstract base class for implementing load providers.

    The LoadProvider class defines the blueprint for any load provider that processes
    and optionally transforms extracted data before persisting it in a target system.
    It allows customization through a configuration and manages the lifecycle of loading
    and transformation operations.

    Attributes:
        config (LoadProviderConfig): Configuration for the load provider, including the transformation plan.
        transformation_plan (TransformationPlan): Transformation plan detailing the steps to be applied
            before persisting the data.
    """

    def __init__(self, config: LoadProviderConfig):
        """Initialize load provider with configuration."""
        self.config = config
        self.transformation_plan = config.transformation_plan
        logger.debug(f"Initialized {self.__class__.__name__}: name={config.name}, type={config.type}")
        logger.debug(f"Load provider parameters: {config.parameters}")
        if self.transformation_plan:
            logger.debug(f"Transformation plan configured with {len(self.transformation_plan.steps)} steps")

    def load(self, extraction_result: ExtractionResult) -> LoadResult:
        """
        Load data into the target destination after applying necessary transformations.

        This method orchestrates the load operation, which involves optional data
        transformations and persistence into the targeted destination. Logging is
        employed to track each stage of the operation for debugging and informational
        purposes. In case of failure, a failed load result is created and returned.

        Parameters:
            extraction_result (ExtractionResult): The input data to be loaded,
            containing the document ID and other necessary information.

        Returns:
            LoadResult: Indicates the success or failure of the load operation,
            including any relevant metadata or error details.
        """
        logger.debug(f"Starting load operation: document={extraction_result.document_id}, provider={self.__class__.__name__}")
        logger.info(f"Starting load operation for document: {extraction_result.document_id}")
        
        try:
            if self.transformation_plan:
                logger.debug(f"Applying {len(self.transformation_plan.steps)} transformation steps")
                extraction_result = self._apply_transformations(extraction_result)
                logger.debug(f"Transformations completed successfully")
            
            logger.debug(f"Persisting data using {self.__class__.__name__}")
            result = self._persist(extraction_result)
            logger.debug(f"Persistence completed: success={result.success}")
            logger.info(f"Load operation completed for document: {extraction_result.document_id}")
            return result
        except Exception as e:
            logger.error(f"Load operation failed for document {extraction_result.document_id}: {e}")
            return LoadResult.create_failed(
                document_id=extraction_result.document_id,
                error_message=str(e)
            )

    def _apply_transformations(self, extraction_result: ExtractionResult) -> ExtractionResult:
        """
        Applies a series of transformations to the given extraction result based on a predefined
        transformation plan. Each transformation step is executed sequentially, modifying the
        nodes or dataframe of the extraction result as specified.

        Parameters:
            extraction_result (ExtractionResult): The initial extraction result to be transformed.

        Returns:
            ExtractionResult: The transformed extraction result after applying all transformation
            steps.

        Raises:
            Exception: If any transformation step fails during the process.
        """
        from document_graph.transformers.transformer_provider_factory import TransformerProviderFactory
        from document_graph.transformers.transformer_provider_config import TransformerProviderConfig

        transformed_result = extraction_result

        for i, step in enumerate(self.transformation_plan.steps):
            logger.debug(f"Applying transformation step {i + 1}/{len(self.transformation_plan.steps)}: {step.name}")
            try:
                transformer_config = TransformerProviderConfig(
                    name=step.name,
                    type=step.type or "transformer",
                    args=step.args or {}
                )
                transformer = TransformerProviderFactory.get_provider(transformer_config)

                if transformed_result.nodes:
                    transformed_result.nodes = [
                        transformer.transform(node, **(step.args or {}))
                        for node in transformed_result.nodes
                    ]

                if transformed_result.dataframe is not None:
                    transformed_result.dataframe = transformer.transform(
                        transformed_result.dataframe, **(step.args or {})
                    )

                logger.debug(f"Successfully applied transformation: {step.name}")
            except Exception as e:
                logger.error(f"Failed to apply transformation {step.name}: {e}")
                raise

        return transformed_result

    @abstractmethod
    def _persist(self, extraction_result: ExtractionResult) -> LoadResult:
        """
        Represents an abstract method for persistence of extraction results. This method
        must be implemented by subclasses to define how to persist the given
        extraction result and return the corresponding load result.

        Parameters:
            extraction_result: ExtractionResult
                The result of some extraction process that needs to be persisted.

        Returns:
            LoadResult
                The result of the persistence operation.

        Raises:
            NotImplementedError
                If this method is not implemented in a subclass.
        """
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.config.name} type={self.config.type}>"
