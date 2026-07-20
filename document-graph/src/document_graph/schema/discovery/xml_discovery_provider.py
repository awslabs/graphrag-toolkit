# Copyright (c) Evan Erwee. All rights reserved.

"""XML Schema Discovery Provider Module for Document Graph Operations.

This module provides a schema discovery provider for XML (eXtensible Markup Language) files.
It analyzes XML files to infer their structure and generate appropriate ETL schemas
for processing and loading the data into a document graph.

The module includes the following components:
- XMLSchemaDiscoveryProvider: Schema discovery provider for XML files

The XML schema discovery provider reads and analyzes XML files, extracting element names
and attributes from the root elements to create an ETL schema. It handles both simple
XML structures and more complex nested structures by flattening them into field names.

Usage:
    # Get an XML discovery provider for a specific file
    from pathlib import Path
    from document_graph.schema.discovery.schema_discovery_registry import get_discovery_provider
    
    file_path = Path("data/sample.xml")
    provider = get_discovery_provider(file_path)
    
    # Or create it directly
    from document_graph.schema.discovery.xml_discovery_provider import XMLSchemaDiscoveryProvider
    
    provider = XMLSchemaDiscoveryProvider(source=file_path)
    
    # Discover the schema
    schema = provider.discover_schema()
    
    # Use the schema for ETL operations
    print(f"Discovered schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""

import logging
import xml.etree.ElementTree as ET
from typing import List, Set

# Import custom logging to ensure configuration is available

from document_graph.schema.etl_schema_model import *
from document_graph.schema.discovery.schema_discovery_base import SchemaDiscoveryProvider

logger = logging.getLogger(__name__)


class XMLSchemaDiscoveryProvider(SchemaDiscoveryProvider):
    """
    Schema discovery provider for XML (eXtensible Markup Language) files.
    
    This class analyzes XML files to infer their structure and generate appropriate
    ETL schemas for processing and loading the data into a document graph. It reads
    XML files and extracts element names and attributes to determine the fields that
    should be included in the ETL schema.
    
    The provider handles XML structures by examining the first few elements to
    determine the schema. It flattens nested structures into field names using
    dot notation (e.g., "parent.child").
    
    Attributes:
        source (Path): The path to the XML file to analyze.
        args (Dict[str, Any]): Optional arguments that control the discovery process.
            Currently, no specific arguments are used for XML discovery.
    """
    
    def discover_schema(self) -> ETLSchema:
        """
        Discover and return an ETL schema from an XML file.
        
        This method reads the XML file, extracts element names and attributes
        from the structure, and generates an appropriate ETL schema. It includes
        error handling to manage issues that might arise during XML parsing.
        
        The method performs the following steps:
        1. Validates that the source file exists
        2. Parses the XML file using ElementTree
        3. Extracts field names from elements and attributes
        4. Handles nested structures by flattening them
        5. Validates that fields were discovered
        6. Logs the discovered fields
        7. Constructs and returns an ETLSchema object
        
        Returns:
            ETLSchema: A complete ETL schema object that describes the structure
                of the XML file and how it should be processed.
                
        Raises:
            FileNotFoundError: If the specified source file does not exist.
            ValueError: If the XML file has no discoverable fields.
            Exception: Any exception raised during XML parsing.
        """
        if not self.source.exists():
            raise FileNotFoundError(f"File not found")

        try:
            tree = ET.parse(self.source)
            root = tree.getroot()
            
            # Extract field names from XML structure
            fields = self._extract_fields_from_element(root)
            
            if not fields:
                raise ValueError("Validation error")

            logger.info(f"Discovered fields from XML: {sorted(fields)}")
            
        except ET.ParseError as e:
            raise ErrorHandler.file_parse_error(str(self.source), "xml", e)
        except Exception as e:
            raise ErrorHandler.file_parse_error(str(self.source), "xml", e)

        return ETLSchema(
            schema_id=f"discovered-{self.source.stem}",
            description=f"Discovered ETL schema from XML file: {self.source.name}",
            extract=ExtractConfig(source_type="file", file_type="xml"),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="fixed_length", min_length=100),
                metadata_mapping=MetadataMapping(),
                entity_extraction=EntityExtractionConfig(method="ner"),
                normalize=NormalizeConfig()
            ),
            load=LoadConfig(
                document_node=NodeDefinition(type="Document", fields=sorted(fields)),
                section_node=NodeDefinition(type="Element", fields=sorted(fields)),
                relationships=[
                    RelationshipDefinition(type="contains", source="document_id", target="element_id")
                ]
            )
        )
    
    def _extract_fields_from_element(self, element: ET.Element, prefix: str = "") -> Set[str]:
        """
        Recursively extract field names from an XML element.
        
        This helper method traverses the XML structure and extracts field names
        from element tags and attributes. It handles nested structures by using
        dot notation to create hierarchical field names.
        
        Args:
            element (ET.Element): The XML element to analyze.
            prefix (str): The prefix to use for nested field names.
            
        Returns:
            Set[str]: A set of field names discovered in the XML structure.
        """
        fields = set()
        
        # Add the element's tag name
        tag_name = element.tag.split('}')[-1] if '}' in element.tag else element.tag
        field_name = f"{prefix}.{tag_name}" if prefix else tag_name
        fields.add(field_name)
        
        # Add attributes
        for attr_name in element.attrib:
            attr_field = f"{field_name}.{attr_name}"
            fields.add(attr_field)
        
        # Add text content if present
        if element.text and element.text.strip():
            text_field = f"{field_name}.text"
            fields.add(text_field)
        
        # Recursively process child elements (limit depth to avoid excessive nesting)
        if len(field_name.split('.')) < 3:  # Limit nesting depth
            for child in element:
                child_fields = self._extract_fields_from_element(child, field_name)
                fields.update(child_fields)
        
        return fields