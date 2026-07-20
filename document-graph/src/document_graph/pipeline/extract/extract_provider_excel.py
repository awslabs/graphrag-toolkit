# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Extract Provider Excel module for document graph operations."""

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
from document_graph.schema.providers.excel_schema_provider import ExcelSchemaProvider



class ExcelExtractProvider(ExtractProvider):
    """
    Provides functionality for extracting, processing, and analyzing Excel data.

    This class is responsible for managing the extraction of Excel data into structured formats including
    DataFrames and schema-based nodes. It supports resolving file paths, reading Excel data, applying
    ingestors for preprocessing, inferring schemas, saving schemas with mock data, and generating document
    nodes based on the content of each row. It also ensures handling of errors and validation during the
    entire extraction process.

    Attributes:
        config (ExtractProviderConfig): Configuration object for the extraction provider that stores
            various extraction-related settings and details.
        aws_config (DocumentGraphConfig): AWS-specific configuration for managing data storage, schema
            saving, and other AWS-related operations.
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
        Extracts data from an Excel file, processes it, and returns a comprehensive
        extraction result, including inferred schema, document nodes, and metadata.

        This method performs several actions, including resolving the file path,
        verifying the file's existence, reading the Excel file into a DataFrame,
        and applying specified ingestors if provided. It further processes the data
        to infer its schema, saves the schema with mock data, and creates document nodes
        from its rows. If an ETL schema is configured, it uses the schema; otherwise, it
        generates an ETL schema dynamically.

        Parameters:
            source (str): Source path to the Excel file to be extracted.
            ingestors (optional): An optional ingestors instance used to process the DataFrame.
            **kwargs: Arbitrary keyword arguments, e.g., `strict` for handling empty data.

        Raises:
            Exception: If an error occurs while reading the Excel file or applying the
            ingestors.
            Error: When the DataFrame is empty, depending on the strict mode configuration.

        Returns:
            ExtractionResult: An object encapsulating the document ID, extracted schema,
            document nodes, processed DataFrame, and associated metadata.

        """
        logger.info(f"Starting Excel extraction from source: {source}")
        resolved_path = PathResolver(self.aws_config).resolve(source)
        resolved_path = Path(resolved_path)
        logger.debug(f"Resolved path: {resolved_path}")

        # ErrorHandler removed
        ErrorHandler.check_file_exists(str(resolved_path), "Excel extraction")

        try:
            logger.debug(f"Reading Excel with args: {self.config.args}")
            df = DataFrameReader(path=resolved_path, **self.config.args).read()
            logger.info(f"Successfully read Excel with shape: {df.shape}")
            
            # Apply ingestors if provided
            if ingestors:
                logger.info("Applying ingestors to Excel data")
                df = ingestors.execute(df)
        except Exception as e:
            raise ErrorHandler.file_parse_error(str(resolved_path), "excel", e) from e

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

        # Load ETL schema if provided in config, otherwise fall back to dynamic schema generation
        etl_schema = self.config.load_schema_if_needed()
        if not etl_schema:
            schema_config = SchemaProviderConfig(
                type="excel",
                connection_config={"path": str(resolved_path)},
            )
            excel_schema_provider = ExcelSchemaProvider(schema_config)
            etl_schema = excel_schema_provider.load_schema()
            logger.info(f"Generated ETL schema from Excel: {etl_schema.schema_id}")

        from document_graph.pipeline.extract.extraction_result import ExtractionResult
        return ExtractionResult(
            document_id=self.config.document_id,
            extracted_schema=schema,
            nodes=nodes,
            dataframe=df,
            metadata={
                "source": source,
                "extraction_type": "excel",
                "etl_schema_id": etl_schema.schema_id if etl_schema else None,
                "rows_processed": str(len(df)),
                "schema_fields": str(len(schema))
            }
        )
