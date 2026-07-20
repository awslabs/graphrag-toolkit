# Copyright (c) Evan Erwee. All rights reserved.

"""Parquet Schema Discovery Provider Module for Document Graph Operations.

This module provides a schema discovery provider for Parquet files, a columnar storage
format commonly used in big data processing. It analyzes Parquet files to infer their
structure and generate appropriate ETL schemas for processing and loading the data
into a document graph.

The module includes the following components:
- ParquetSchemaDiscoveryProvider: Schema discovery provider for Parquet files

The Parquet schema discovery provider uses pandas to read and analyze Parquet files,
extracting column headers and other metadata to create an ETL schema. It supports
various Parquet parsing options through the args parameter, which is passed directly
to pandas.read_parquet.

Usage:
    # Get a Parquet discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    file_path = Path("data/sample.parquet")
    provider = get_discovery_provider(file_path)
    
    # Or create it directly with custom arguments
    from document_graph.schema.discovery.parquet_discovery_provider import ParquetSchemaDiscoveryProvider
    
    provider = ParquetSchemaDiscoveryProvider(
        source=file_path,
        args={"columns": ["id", "name", "value"]}
    )
    
    # Discover the schema
    schema = provider.discover_schema()
    
    # Use the schema for ETL operations
    print(f"Discovered schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""


# Import custom logging to ensure configuration is available


import logging
from typing import List
import pandas as pd

from document_graph.schema.etl_schema_model import *
from document_graph.schema.discovery.tabular_discovery_base import TabularSchemaDiscoveryProvider


logger = logging.getLogger(__name__)

class ParquetSchemaDiscoveryProvider(TabularSchemaDiscoveryProvider):
    """
    Schema discovery provider for Parquet files.
    
    This class analyzes Parquet files to infer their structure and generate appropriate
    ETL schemas for processing and loading the data into a document graph. It uses
    pandas to read and analyze Parquet files, extracting column headers and other metadata.
    
    Parquet is a columnar storage format commonly used in big data processing. It is
    designed for efficient data compression and encoding schemes, making it particularly
    well-suited for query performance and minimizing I/O operations.
    
    The provider supports various Parquet parsing options through the args parameter,
    which is passed directly to pandas.read_parquet. This allows for customization of
    the Parquet parsing process, such as specifying which columns to read.
    
    Attributes:
        source (Path): The path to the Parquet file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            These arguments are passed directly to pandas.read_parquet.
            
    Config options:
        - columns (List[str]): List of columns to read. If not provided, all columns are read.
        - Any other argument accepted by pandas.read_parquet.
    """
    
    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from a Parquet file.
        
        This method reads the Parquet file using pandas, extracts the column headers,
        and generates an appropriate ETL schema. It includes error handling to
        manage issues that might arise during the Parquet parsing process.
        
        The method performs the following steps:
        1. Validates that the source file exists
        2. Reads the Parquet file using pandas.read_parquet with the provided arguments
        3. Extracts the column headers
        4. Validates that the headers are not empty
        5. Logs the discovered columns
        6. Constructs and returns an ETLSchema object
        
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the Parquet file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the specified source file does not exist.
            ValueError: If the Parquet file has no columns or cannot be parsed.
            Exception: Any exception raised by pandas.read_parquet.
        """
        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

        try:
            df = pd.read_parquet(self.source, **self.args)
            headers: List[str] = list(df.columns)
            if not headers:
                raise ValueError("Validation error")

            logger.info(f"Discovered columns from Parquet: {headers}")
        except Exception as e:
            raise ErrorHandler.csv_parse_error(str(self.source), e)

        return ETLSchema(
            schema_id=f"discovered-{self.source.stem}",
            description=f"Discovered ETL schema from Parquet file: {self.source.name}",
            extract=ExtractConfig(source_type="file", file_type="parquet"),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="fixed_length", min_length=100),
                metadata_mapping=MetadataMapping(),
                entity_extraction=EntityExtractionConfig(method="ner"),
                normalize=NormalizeConfig()
            ),
            load=LoadConfig(
                document_node=NodeDefinition(type="Document", fields=headers),
                section_node=NodeDefinition(type="Row", fields=headers),
                relationships=[
                    RelationshipDefinition(type="contains", source="document_id", target="row_id")
                ]
            )
        )
