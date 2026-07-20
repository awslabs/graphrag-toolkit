# Copyright (c) Evan Erwee. All rights reserved.

"""JSON Schema Discovery Provider Module for Document Graph Operations.

This module provides a schema discovery provider for JSON (JavaScript Object Notation) files.
It analyzes JSON files to infer their structure and generate appropriate ETL schemas
for processing and loading the data into a document graph.

The module includes the following components:
- JSONSchemaDiscoveryProvider: Schema discovery provider for JSON files

The JSON schema discovery provider reads and analyzes JSON files, extracting object keys
from the first record (or the entire object if it's not a list) to create an ETL schema.
It handles both array-based JSON files (containing multiple records) and single-object
JSON files.

Usage:
    # Get a JSON discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    file_path = Path("data/sample.json")
    provider = get_discovery_provider(file_path)
    
    # Or create it directly
    from document_graph.schema.discovery.json_discovery_provider import JSONSchemaDiscoveryProvider
    
    provider = JSONSchemaDiscoveryProvider(source=file_path)
    
    # Discover the schema
    schema = provider.discover_schema()
    
    # Use the schema for ETL operations
    print(f"Discovered schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""


# Import custom logging to ensure configuration is available


import json
import logging
from typing import List

from document_graph.schema.etl_schema_model import *
from document_graph.schema.discovery.tabular_discovery_base import TabularSchemaDiscoveryProvider


logger = logging.getLogger(__name__)

class JSONSchemaDiscoveryProvider(TabularSchemaDiscoveryProvider):
    """
    Schema discovery provider for JSON (JavaScript Object Notation) files.
    
    This class analyzes JSON files to infer their structure and generate appropriate
    ETL schemas for processing and loading the data into a document graph. It reads
    JSON files and extracts object keys to determine the fields that should be included
    in the ETL schema.
    
    The provider handles both array-based JSON files (containing multiple records)
    and single-object JSON files. For array-based files, it examines the first record
    to determine the schema. For single-object files, it uses the keys of the object.
    
    Attributes:
        source (Path): The path to the JSON file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            Currently, no specific arguments are used for JSON discovery.
    """
    
    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from a JSON file.
        
        This method reads the JSON file, extracts the keys from the first record
        (or the entire object if it's not a list), and generates an appropriate
        ETL schema. It includes error handling to manage issues that might arise
        during the JSON parsing process.
        
        The method performs the following steps:
        1. Validates that the source file exists
        2. Reads the JSON file using json.load
        3. Determines if the data is an array or a single object
        4. Extracts the keys from the first record or the entire object
        5. Validates that the keys are not empty
        6. Logs the discovered fields
        7. Constructs and returns an ETLSchema object
        
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the JSON file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the specified source file does not exist.
            ValueError: If the JSON file has no keys or cannot be parsed.
            Exception: Any exception raised during JSON parsing.
        """
        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

        try:
            with self.source.open("r", encoding="utf-8") as f:
                # Try regular JSON first
                try:
                    data = json.load(f)
                    first_record = data[0] if isinstance(data, list) else data
                except json.JSONDecodeError:
                    # If regular JSON fails, try JSONL (newline-delimited JSON)
                    logger.debug("Regular JSON failed, trying JSONL format")
                    f.seek(0)  # Reset file pointer
                    first_line = f.readline().strip()
                    if first_line:
                        first_record = json.loads(first_line)
                    else:
                        raise ValueError("Empty JSONL file")

            headers: List[str] = list(first_record.keys())

            if not headers:
                raise ValueError("Validation error")

            logger.info(f"Discovered fields from JSON/JSONL: {headers}")
        except Exception as e:
            raise ErrorHandler.csv_parse_error(str(self.source), e)

        return ETLSchema(
            schema_id=f"discovered-{self.source.stem}",
            description=f"Discovered ETL schema from JSON file: {self.source.name}",
            extract=ExtractConfig(source_type="file", file_type="json"),
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
