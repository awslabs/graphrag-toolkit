# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Config module for document graph operations."""

import logging
from pydantic import BaseModel, Field
from typing import Literal, Dict, Any, Optional, TYPE_CHECKING

from document_graph.schema.etl_schema_model import ETLSchema, ExtractConfig
from document_graph.schema.providers.schema_provider_factory import SchemaProviderFactory

if TYPE_CHECKING:
    pass
    


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401



class ExtractProviderConfig(BaseModel):
    """
    Represents a configuration for an Extract Provider.

    This class provides a detailed configuration structure for extract providers,
    facilitating integration with document sources of various types. It serves
    as a foundational element for managing the extraction process, including
    the ETL schema, pipeline arguments, transformers, and factory integrations.

    Attributes:
        type (str): Type of extract provider. It can be one of the following values:
            "csv", "json", "parquet", or "excel".
        source (str): Path or URI to the document source.
        document_id (str): Document ID used in node construction.
        etl_schema (Optional[ETLSchema]): Complete ETL schema configuration.
        schema_provider_config (Optional[Dict[str, Any]]): Schema provider configuration.
        args (Dict[str, Any]): Arguments for Pandas file reader, such as separators
            or encoding specifications.
        pipeline_args (Dict[str, Any]): Configurations affecting pipeline behavior,
            such as logging settings or skipping conditions.
        transformers (Optional[list]): List of transformer configurations.
        factory (Optional[S3ArtifactFactory]): Factory used to load sources or pipelines.
        model_config (dict): Arbitrary types allowance configuration.

    Methods:
        parameters -> Dict[str, Any]:
            Retrieves backward-compatible alias for 'args'.

        schema_path -> Optional[str]:
            Provides backward-compatible alias for the schema path.

        get_extract_config -> ExtractConfig:
            Derives the extract configuration from ETL schema or defaults to the
            type configuration.

        load_schema_if_needed -> Optional[ETLSchema]:
            Loads ETL schema configuration dynamically if not already available.

        from_factory_source(cls, factory, source_name, pipeline_name=None, **kwargs) -> ExtractProviderConfig:
            Creates configuration using a factory source artifact with optional
            pipeline arguments.

        from_factory_pipeline(cls, factory, pipeline_name) -> ExtractProviderConfig:
            Creates configuration using a factory pipeline artifact.
    """
    type: Literal["csv", "json", "parquet", "excel"] = Field(..., description="Type of extract provider")
    source: str = Field(..., description="Path or URI to the document source")
    document_id: str = Field(..., description="Document ID used in node construction")

    # Optional schema integration
    etl_schema: Optional[ETLSchema] = Field(None, description="Complete ETL schema configuration")
    schema_provider_config: Optional[Dict[str, Any]] = Field(None, description="Schema provider configuration")

    # Clean separation of arguments
    args: Dict[str, Any] = Field(default_factory=dict,
                                 description="Arguments for Pandas file reader (e.g., sep, encoding, on_bad_lines)")
    pipeline_args: Dict[str, Any] = Field(default_factory=dict,
                                          description="Pipeline behavior (e.g., logging, skipping)")
    transformers: Optional[list] = Field(default_factory=list,
                                        description="List of transformer configurations")
    
    # Factory support
    factory: Optional[Any] = Field(None, description="Artifact factory for loading sources/pipelines")
    
    model_config = {"arbitrary_types_allowed": True}
    
    @property
    def parameters(self) -> Dict[str, Any]:
        """
        Provides access to the parameters of an object.

        This property allows retrieval of the object's parameters in the form of a dictionary.

        Returns:
            Dict[str, Any]: A dictionary containing the parameters of the object.
        """
        return self.args
    
    @property
    def schema_path(self) -> Optional[str]:
        """
        Gets the schema path of the object.

        The `schema_path` property is intended to provide the schema location
        for this specific instance, if available. If no schema path is available,
        it will return `None`.

        Returns:
            Optional[str]: The path to the schema or `None` if no schema path exists.
        """
        return None

    def get_extract_config(self) -> ExtractConfig:
        """
        Gets the extraction configuration.

        This method retrieves the extraction configuration based on the current ETL schema.
        If no specific ETL schema is defined, it returns a default minimal configuration.

        Returns:
            ExtractConfig: The extraction configuration object.

        """
        if self.etl_schema:
            return self.etl_schema.extract

        # Default minimal config
        return ExtractConfig(
            source_type="file",
            file_type=self.type
        )

    def load_schema_if_needed(self) -> Optional[ETLSchema]:
        """
        Loads the ETL schema if it is not already loaded.

        This method attempts to load and return the ETL schema. If the schema is already
        available in the `etl_schema` attribute, it will return that. If not, it will try to
        load the schema using the provided schema provider configuration. In the event of an
        error while loading the schema, it logs a warning and returns None.

        Raises:
            Logs a warning if the schema loading process fails due to an invalid configuration
            or other errors.

        Returns:
            Optional[ETLSchema]: The loaded ETL schema, or None if the schema could not be loaded.
        """
        if self.etl_schema:
            return self.etl_schema

        if self.schema_provider_config:
            try:
                schema_provider = SchemaProviderFactory.create(self.schema_provider_config)
                return schema_provider.load_schema()
            except Exception as e:
                # ErrorHandler removed
                raise ValueError(f"Schema load failed: {e}") from e




                logger.warning(f"Failed to load schema from provider: {error.message}")
                return None

        return None
    
    @classmethod
    def from_factory_source(cls, factory: Any, source_name: str, pipeline_name: str = None, **kwargs) -> "ExtractProviderConfig":
        """
        Creates an instance of ExtractProviderConfig from a factory source.

        Detailed Summary:
        This class method initializes an ExtractProviderConfig instance using a provided
        S3ArtifactFactory and a source name. It pulls metadata and arguments from the specified
        source artifact or associated pipeline if applicable. Additional parameters can be
        customized using keyword arguments, with a fallback to source-defined attributes.

        Parameters:
            factory (S3ArtifactFactory): The factory responsible for managing artifact storage
                and retrieval.
            source_name (str): The name of the source artifact within the factory to load data from.
            pipeline_name (str, optional): The name of the pipeline artifact to extract additional
                configuration arguments, if applicable.
            **kwargs: Additional keyword arguments for custom configuration, with document_id
                being extracted separately, defaulting to the source's name if not provided.

        Raises:
            ValueError: If the specified source artifact cannot be found within the provided factory.

        Returns:
            ExtractProviderConfig: An initialized configuration object for the extract provider.
        """
        source_artifact = factory.load_source(source_name)
        if not source_artifact:
            raise ValueError(f"Source artifact '{source_name}' not found in factory")
        
        # Load args from pipeline or source metadata
        args = {}
        if pipeline_name:
            pipeline_artifact = factory.load_pipeline(pipeline_name)
            if pipeline_artifact:
                args = pipeline_artifact.pipeline_config.get('source', {}).get('args', {})
        
        # Fallback to source metadata processing_args
        if not args and source_artifact.metadata:
            args = source_artifact.metadata.get('processing_args', {})
        
        # Extract document_id from kwargs to avoid duplicate
        document_id = kwargs.pop('document_id', source_artifact.name)
        
        return cls(
            factory=factory,
            type=source_artifact.source_type,
            source=f"factory://{source_name}",
            document_id=document_id,
            args=args,
            **kwargs
        )
    
    @classmethod 
    def from_factory_pipeline(cls, factory: Any, pipeline_name: str) -> "ExtractProviderConfig":
        """
        Creates an `ExtractProviderConfig` instance from a factory and pipeline name.

        This class method allows loading a pipeline artifact using the provided factory
        and pipeline name, extracting configuration data, and constructing an instance
        of `ExtractProviderConfig` with it.

        Parameters:
            factory (S3ArtifactFactory): The factory instance used to load the pipeline artifact.
            pipeline_name (str): The identifier of the pipeline whose artifact needs to
                be loaded.

        Returns:
            ExtractProviderConfig: A new instance of `ExtractProviderConfig` initialized with the
                extracted configuration data.

        Raises:
            ValueError: If the specified pipeline artifact is not found in the factory.
        """
        pipeline_artifact = factory.load_pipeline(pipeline_name)
        if not pipeline_artifact:
            raise ValueError(f"Pipeline artifact '{pipeline_name}' not found in factory")
        
        config_data = pipeline_artifact.pipeline_config
        return cls(
            factory=factory,
            type=config_data['source']['type'],
            source=config_data['source']['path'],
            document_id=config_data['extract']['document_id'],
            args=config_data['source'].get('args', {}),
            **config_data.get('extract', {})
        )
