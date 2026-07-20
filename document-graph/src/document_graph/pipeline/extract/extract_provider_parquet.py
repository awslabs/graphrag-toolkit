# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Parquet module for document graph operations."""

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
from document_graph.schema.providers.parquet_schema_provider import ParquetSchemaProvider



class ParquetExtractProvider(ExtractProvider):
    """
    A provider for extracting data from Parquet files.

    This class is designed to handle the process of reading, processing, and
    extracting data from Parquet files. It leverages configurable components
    to provide flexibility in handling various data extraction and schema
    inference scenarios. The main functionalities include resolving file paths,
    reading Parquet files, schema inference, and metadata preparation.

    Attributes:
        config (ExtractProviderConfig): Configuration for the extraction process.
        aws_config (DocumentGraphConfig): Configuration for AWS-related operations.
    """

    def __init__(self, config: ExtractProviderConfig, aws_config: DocumentGraphConfig):
        """
        Initializes an instance of the class.

        This constructor initializes the class instance by calling the parent class
        constructor with the provided configuration objects. It ensures that the
        necessary configurations are passed for operation.

        Args:
            config (ExtractProviderConfig): The configuration object containing
                settings for the extract provider.
            aws_config (DocumentGraphConfig): The configuration object specific
                to the document graph service or AWS-related settings.
        """
        super().__init__(config, aws_config)

    def extract(self, source: str, ingestors=None, **kwargs):
        """
        Extracts data from a source in Parquet format, processes it, and infers schema and
        document nodes.

        This method handles different aspects of the Parquet extraction pipeline including
        path resolution, Parquet file reading, schema inference, and document node creation.
        Additionally, it supports applying custom ingestors to transform the data. The method
        saves the inferred schema along with mock data and returns all results in an
        ExtractionResult object.

        Raises:
            FileNotFoundError: If the specified Parquet source file does not exist.
            RuntimeError: If the schema inference or document node creation fails.
            ValueError: If the extracted data is empty and strict mode is enabled.

        Args:
            source (str): The source path of the Parquet file.
            ingestors (optional): Object containing ingestors to process the DataFrame. Defaults to None.
            **kwargs: Additional keyword arguments for configuring extraction behavior.

        Returns:
            ExtractionResult: Result of the extraction process, including document nodes,
            inferred schema, processed DataFrame, and metadata.
        """
        logger.info(f"Starting Parquet extraction from source: {source}")
        resolved_path = PathResolver(self.aws_config).resolve(source)
        resolved_path = Path(resolved_path)
        logger.debug(f"Resolved path: {resolved_path}")

        # ErrorHandler removed
        ErrorHandler.check_file_exists(str(resolved_path), "Parquet extraction")

        try:
            logger.debug(f"Reading Parquet with args: {self.config.args}")
            df = DataFrameReader(path=resolved_path, **self.config.args).read()
            logger.info(f"Successfully read Parquet with shape: {df.shape}")
            
            # Apply ingestors if provided
            if ingestors:
                logger.info("Applying ingestors to Parquet data")
                df = ingestors.execute(df)
        except Exception as e:
            raise ErrorHandler.csv_parse_error(str(resolved_path), e) from e

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

        logger.debug("Saving schema with mock data")
        schema_path = SchemaWriter(self.aws_config).save_schema_with_mock_data(
            schema_dict=schema,
            df=df,
            source=source,
            output_dir=Path("schemas"),
            document_id=self.config.document_id,
        )
        logger.info(f"Schema saved to: {schema_path}")

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
            schema_config = SchemaProviderConfig(
                type="parquet",
                connection_config={"path": str(resolved_path)},
            )
            parquet_schema_provider = ParquetSchemaProvider(schema_config)
            etl_schema = parquet_schema_provider.load_schema()
            logger.info(f"Generated ETL schema from Parquet: {etl_schema.schema_id}")

        from document_graph.pipeline.extract.extraction_result import ExtractionResult
        return ExtractionResult(
            document_id=self.config.document_id,
            extracted_schema=schema,
            nodes=nodes,
            dataframe=df,
            metadata={
                "source": source,
                "extraction_type": "parquet",
                "etl_schema_id": etl_schema.schema_id if etl_schema else None,
                "rows_processed": str(len(df)),
                "schema_fields": str(len(schema))
            }
        )
