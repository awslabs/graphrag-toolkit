# Copyright (c) Evan Erwee. All rights reserved.

"""YAML Schema Discovery Provider Module for Document Graph Operations.

This module provides a schema discovery provider for YAML (YAML Ain't Markup Language) files.
It analyzes YAML files to infer their structure and generate appropriate ETL schemas
for processing and loading the data into a document graph.

The module includes the following components:
- YAMLSchemaDiscoveryProvider: Schema discovery provider for YAML files

The YAML schema discovery provider reads and analyzes YAML files, extracting keys
from the structure to create an ETL schema. It handles both simple YAML structures
and more complex nested structures by flattening them into field names.

Usage:
    # Get a YAML discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    file_path = Path("data/sample.yaml")
    provider = get_discovery_provider(file_path)
    
    # Or create it directly
    from document_graph.schema.discovery.yaml_discovery_provider import YAMLSchemaDiscoveryProvider
    
    provider = YAMLSchemaDiscoveryProvider(source=file_path)
    
    # Discover the schema
    schema = provider.discover_schema()
    
    # Use the schema for ETL operations
    print(f"Discovered schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""

import logging
from typing import List, Set, Dict, Any, Union

# Import custom logging to ensure configuration is available

from document_graph.schema.etl_schema_model import *
from document_graph.schema.discovery.schema_discovery_base import SchemaDiscoveryProvider

logger = logging.getLogger(__name__)

try:
    import yaml
except ImportError:
    yaml = None


class YAMLSchemaDiscoveryProvider(SchemaDiscoveryProvider):
    """
    Schema discovery provider for YAML (YAML Ain't Markup Language) files.
    
    This class analyzes YAML files to infer their structure and generate appropriate
    ETL schemas for processing and loading the data into a document graph. It reads
    YAML files and extracts keys from the structure to determine the fields that
    should be included in the ETL schema.
    
    The provider handles both simple YAML structures and complex nested structures
    by flattening them into field names using dot notation (e.g., "parent.child").
    It supports both single-document and multi-document YAML files.
    
    Attributes:
        source (Path): The path to the YAML file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            Currently, no specific arguments are used for YAML discovery.
    """
    
    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from a YAML file.
        
        This method reads the YAML file, extracts keys from the structure,
        and generates an appropriate ETL schema. It includes error handling
        to manage issues that might arise during YAML parsing.
        
        The method performs the following steps:
        1. Validates that the source file exists and PyYAML is available
        2. Parses the YAML file using yaml.safe_load
        3. Extracts field names from the structure
        4. Handles nested structures by flattening them
        5. Validates that fields were discovered
        6. Logs the discovered fields
        7. Constructs and returns an ETLSchema object
        
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the YAML file and how it should be processed.
                
        Raises:
            ImportError: If PyYAML is not installed.
            FileNotFoundError: If the specified source file does not exist.
            ValueError: If the YAML file has no discoverable fields.
            Exception: Any exception raised during YAML parsing.
        """
        if yaml is None:
            raise ImportError("PyYAML is required for YAML schema discovery. Install with: pip install PyYAML")
            
        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

        try:
            with self.source.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            
            # Extract field names from YAML structure
            fields = self._extract_fields_from_data(data)
            
            if not fields:
                raise ValueError("Validation error")

            logger.info(f"Discovered fields from YAML: {sorted(fields)}")
            
        except yaml.YAMLError as e:
            raise ErrorHandler.file_parse_error(str(self.source), "yaml", e)
        except Exception as e:
            raise ErrorHandler.file_parse_error(str(self.source), "yaml", e)

        return ETLSchema(
            schema_id=f"discovered-{self.source.stem}",
            description=f"Discovered ETL schema from YAML file: {self.source.name}",
            extract=ExtractConfig(source_type="file", file_type="yaml"),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="fixed_length", min_length=100),
                metadata_mapping=MetadataMapping(),
                entity_extraction=EntityExtractionConfig(method="ner"),
                normalize=NormalizeConfig()
            ),
            load=LoadConfig(
                document_node=NodeDefinition(type="Document", fields=sorted(fields)),
                section_node=NodeDefinition(type="Section", fields=sorted(fields)),
                relationships=[
                    RelationshipDefinition(type="contains", source="document_id", target="section_id")
                ]
            )
        )
    
    def _extract_fields_from_data(self, data: Any, prefix: str = "") -> Set[str]:
        """
        Recursively extract field names from YAML data structure.
        
        This helper method traverses the YAML data structure and extracts field names
        from dictionaries, lists, and scalar values. It handles nested structures by
        using dot notation to create hierarchical field names.
        
        Args:
            data (Any): The YAML data to analyze (dict, list, or scalar).
            prefix (str): The prefix to use for nested field names.
            
        Returns:
            Set[str]: A set of field names discovered in the YAML structure.
        """
        fields = set()

        if isinstance(data, dict):
            for key, value in data.items():
                field_name = f"{prefix}.{key}" if prefix else key
                fields.add(field_name)

                # Recursively process nested structures (limit depth to avoid excessive nesting)
                if len(field_name.split('.')) < 3:  # Limit nesting depth
                    nested_fields = self._extract_fields_from_data(value, field_name)
                    fields.update(nested_fields)

        elif isinstance(data, list) and data:
            # For lists, analyze the first item to determine structure
            first_item = data[0]
            if isinstance(first_item, dict):
                # If it's a list of dictionaries, extract fields from the first dictionary
                nested_fields = self._extract_fields_from_data(first_item, prefix)
                fields.update(nested_fields)
            elif prefix:
                fields.add(prefix)

        if prefix:
            fields.add(prefix)

        return fields