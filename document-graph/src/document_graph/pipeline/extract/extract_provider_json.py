# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Json module for document graph operations."""

import logging
from pathlib import Path
from typing import List


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401
from document_graph.config import DocumentGraphConfig
from document_graph.pipeline.extract.extract_provider_base import ExtractProvider
from document_graph.pipeline.extract.extract_provider_config import ExtractProviderConfig
from document_graph.pipeline.extract.utils.dataframe_reader import DataFrameReader
# PathResolver removed
from document_graph.schema.document_node import DocumentNode
from document_graph.schema.schema_inference_utils import SchemaInferenceEngine
from document_graph.schema.schema_io import SchemaWriter
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.providers.json_schema_provider import JSONSchemaProvider



class JSONExtractProvider(ExtractProvider):
    """
    Handles JSON data extraction and schema inference for a given source.

    This class extends the ExtractProvider base class and provides functionality
    to read data from a JSON source, infer its schema, and generate document
    nodes for downstream processing. It also supports integration with external
    schema factories and provides mechanisms for ingesting additional data.

    Attributes:
        config (ExtractProviderConfig): Configuration for the extract provider, which includes
                                         document ID, extraction arguments, and schema handling details.
        aws_config (DocumentGraphConfig): AWS-specific configuration, including factory settings and
                                           schema management.

    Methods:
        __init__(config: ExtractProviderConfig, aws_config: DocumentGraphConfig)
            Initializes the JSONExtractProvider with the required configuration details.
        extract(source: str, ingestors=None, **kwargs)
            Extracts data from the JSON source, infers schema, and creates document nodes.
    """

    def __init__(self, config: ExtractProviderConfig, aws_config: DocumentGraphConfig):
        """
        [Brief one-line description of the function's purpose]
        
        [Detailed explanation of what the function does and how it works]
        
        Args:
            [param_name] ([param_type]): [Description of parameter]
            [param_name] ([param_type], optional): [Description of parameter]. Defaults to [default_value].
        
        Returns:
            [return_type]: [Description of return value]
        
        Raises:
            [exception_type]: [Conditions under which exception is raised]
        """
        super().__init__(config, aws_config)

    def extract(self, source: str, ingestors=None, **kwargs):
        """
        Extracts data from a JSON source, processes it, and returns an ExtractionResult object.

        This method performs the following actions:
        1. Resolves the path to the given source.
        2. Validates the existence of the file.
        3. Reads the JSON data into a DataFrame.
        4. Applies optional ingestors for data transformation.
        5. Infers and/or generates schemas for the data.
        6. Converts the DataFrame rows into document nodes.
        7. Returns an ExtractionResult containing the extracted information,
           nodes, and metadata.

        The method supports strict mode for handling empty data. It also integrates
        with schema factories for retrieving or storing schemas.

        Parameters:
            source: str
                Path to the JSON file to be extracted.
            ingestors
                Optional ingestors to be applied for data transformation.
            **kwargs
                Additional options for configuration. Supports "strict" flag
                to determine behavior for empty data.

        Returns:
            ExtractionResult
                Contains the document ID, extracted schema, document nodes,
                DataFrame, and metadata.

        Raises:
            Exception
                If an error occurs during reading, parsing, or schema handling.
            ErrorHandler.empty_data
                If the data is empty and strict mode is enabled.
        """
        logger.info(f"Starting JSON extraction from source: {source}")
        resolved_path = PathResolver(self.aws_config).resolve(source)
        resolved_path = Path(resolved_path)
        logger.debug(f"Resolved path: {resolved_path}")

        # ErrorHandler removed
        ErrorHandler.check_file_exists(str(resolved_path), "JSON extraction")

        try:
            logger.debug(f"Reading JSON with args: {self.config.args}")
            df = DataFrameReader(path=resolved_path, **self.config.args).read()
            logger.info(f"Successfully read JSON with shape: {df.shape}")
            
            # Apply ingestors if provided
            if ingestors:
                logger.info("Applying ingestors to JSON data")
                df = ingestors.execute(df)
        except Exception as e:
            raise ErrorHandler.file_parse_error(str(resolved_path), "json", e) from e

        if df.empty:
            strict_mode = kwargs.get("strict", True)
            error = ErrorHandler.empty_data(str(resolved_path), strict_mode)
            if error:
                raise error
            return [], {}

        logger.debug("Inferring schema from DataFrame")
        schema_engine = SchemaInferenceEngine(df)
        schema = schema_engine.infer_basic_schema()
        logger.debug(f"Inferred schema with {len(schema)} fields")

        # Check factory for existing schema first (before local generation)
        factory_schema_found = False
        if hasattr(self.aws_config, 'factory') and self.aws_config.factory:
            factory_schema = self.aws_config.factory.find_schema_for_source(
                self.config.document_id, "1.0.0"
            )
            if factory_schema:
                logger.info(f"Using existing factory schema: {factory_schema.name}@{factory_schema.version}")
                factory_schema_found = True
        
        # Only generate local schema if not found in factory
        if not factory_schema_found:
            logger.debug("Saving schema with mock data")
            schema_path = SchemaWriter(self.aws_config).save_schema_with_mock_data(
                schema_dict=schema,
                df=df,
                source=source,
                output_dir=Path("schemas"),
                document_id=self.config.document_id,
            )
            logger.info(f"Schema saved to: {schema_path}")
        else:
            logger.info("Skipping local schema generation - using factory schema")

        logger.debug(f"Converting {len(df)} rows to document nodes")
        nodes = []
        for i, row in df.iterrows():
            content = ", ".join(f"{k}: {v}" for k, v in row.items())
            nodes.append(DocumentNode(
                id=f"{self.config.document_id}-row-{i}",
                type="Row",
                content=content,
                metadata={"row_index": str(i)}
            ))

        # Load ETL schema if configured
        etl_schema = self.config.load_schema_if_needed()
        if not etl_schema:
            # Check factory for existing schema first
            factory_schema = None
            if hasattr(self.aws_config, 'factory') and self.aws_config.factory:
                factory_schema = self.aws_config.factory.find_schema_for_source(
                    self.config.document_id, "1.0.0"
                )
                if factory_schema:
                    logger.info(f"Using factory schema: {factory_schema.name}@{factory_schema.version}")
                    # Convert factory schema to ETL schema format if needed
                    etl_schema = factory_schema.schema_data
            
            # Generate new schema if not found in factory
            if not factory_schema:
                schema_config = SchemaProviderConfig(
                    type="json",
                    connection_config={"path": str(resolved_path)},
                )
                json_schema_provider = JSONSchemaProvider(schema_config)
                etl_schema = json_schema_provider.load_schema()
                logger.info(f"Generated ETL schema from JSON: {etl_schema.schema_id if hasattr(etl_schema, 'schema_id') else 'new-schema'}")
                
                # Store generated schema in factory
                if hasattr(self.aws_config, 'factory') and self.aws_config.factory:
                    from document_graph.factory.schema_artifact import SchemaArtifact
                    import json
                    
                    # Convert ETLSchema to simple dict for JSON serialization
                    try:
                        schema_dict = {
                            "schema_id": getattr(etl_schema, 'schema_id', 'generated'),
                            "fields": schema,  # Use the inferred basic schema
                            "document_id": self.config.document_id,
                            "source_type": "json"
                        }
                        schema_artifact = SchemaArtifact(
                            name=f"{self.config.document_id}-schema",
                            version="1.0.0",
                            description=f"Auto-generated schema for {self.config.document_id}",
                            schema_type="etl",
                            schema_data=schema_dict,
                            source_ref=f"{self.config.document_id}@1.0.0"
                        )
                        schema_id = self.aws_config.factory.store_schema(schema_artifact)
                        logger.info(f"Stored schema in factory: {schema_id}")
                    except Exception as e:
                        logger.warning(f"Failed to store schema in factory: {e}")


        from document_graph.pipeline.extract.extraction_result import ExtractionResult
        return ExtractionResult(
            document_id=self.config.document_id,
            extracted_schema=schema,
            nodes=nodes,
            dataframe=df,
            metadata={
                "source": source,
                "extraction_type": "json",
                "etl_schema_id": etl_schema.get('schema_id') if isinstance(etl_schema, dict) else (etl_schema.schema_id if etl_schema and hasattr(etl_schema, 'schema_id') else None),
                "rows_processed": str(len(df)),
                "schema_fields": str(len(schema))
            }
        )
