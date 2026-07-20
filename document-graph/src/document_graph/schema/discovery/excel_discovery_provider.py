# Copyright (c) Evan Erwee. All rights reserved.

"""Excel Schema Discovery Provider Module for Document Graph Operations.

This module provides a schema discovery provider for Excel files (.xlsx, .xls).
It analyzes Excel files to infer their structure and generate appropriate ETL schemas
for processing and loading the data into a document graph.

The module includes the following components:
- ExcelSchemaDiscoveryProvider: Schema discovery provider for Excel files

The Excel schema discovery provider uses pandas to read and analyze Excel files,
extracting column headers and other metadata to create an ETL schema. It supports
various Excel parsing options through the args parameter, which is passed directly
to pandas.read_excel.

Usage:
    # Get an Excel discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    file_path = Path("data/sample.xlsx")
    provider = get_discovery_provider(file_path)
    
    # Or create it directly with custom arguments
    from document_graph.schema.discovery.excel_discovery_provider import ExcelSchemaDiscoveryProvider
    
    provider = ExcelSchemaDiscoveryProvider(
        source=file_path,
        args={"sheet_name": "Sheet1", "header": 0}
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

class ExcelSchemaDiscoveryProvider(TabularSchemaDiscoveryProvider):
    """
    Schema discovery provider for Excel files (.xlsx, .xls).
    
    This class analyzes Excel files to infer their structure and generate appropriate
    ETL schemas for processing and loading the data into a document graph. It uses
    pandas to read and analyze Excel files, extracting column headers and other metadata.
    
    The provider supports various Excel parsing options through the args parameter,
    which is passed directly to pandas.read_excel. This allows for customization of
    the Excel parsing process, such as specifying sheet names, header rows, and
    other pandas.read_excel options.
    
    Attributes:
        source (Path): The path to the Excel file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            These arguments are passed directly to pandas.read_excel.
            
    Config options:
        - sheet_name (str or int): Name or index of the sheet to read. Defaults to 0.
        - header (int): Row to use for the column labels. Defaults to 0.
        - Any other argument accepted by pandas.read_excel.
    """
    
    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from an Excel file.
        
        This method reads the Excel file using pandas, extracts the column headers,
        and generates an appropriate ETL schema. It includes error handling to
        manage issues that might arise during the Excel parsing process.
        
        The method performs the following steps:
        1. Validates that the source file exists
        2. Reads the Excel file using pandas.read_excel with the provided arguments
        3. Extracts the column headers
        4. Validates that the headers are not empty
        5. Logs the discovered columns
        6. Constructs and returns an ETLSchema object
        
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the Excel file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the specified source file does not exist.
            ValueError: If the Excel file has no columns or cannot be parsed.
            Exception: Any exception raised by pandas.read_excel.
        """
        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

        try:
            df = pd.read_excel(self.source, **self.args)
            headers: List[str] = list(df.columns)
            if not headers:
                raise ValueError("Validation error")
            logger.info(f"Discovered columns from Excel: {headers}")
        except Exception as e:
            raise ErrorHandler.csv_parse_error(str(self.source), e)

        return ETLSchema(
            schema_id=f"discovered-{self.source.stem}",
            description=f"Discovered ETL schema from Excel file: {self.source.name}",
            extract=ExtractConfig(source_type="file", file_type="excel"),
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
