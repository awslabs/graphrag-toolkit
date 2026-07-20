# Copyright (c) Evan Erwee. All rights reserved.

"""Static Schema Provider Module for Document Graph Operations.

This module provides a schema provider that returns a hardcoded, static ETL schema.
It's primarily used for testing, development, or as a fallback when no other schema
providers are available.

The static schema provider implements the SchemaProviderBase interface, making it
compatible with the rest of the document graph system. It provides a default schema
that includes configurations for extracting from S3, transforming with standard
operations, and loading into a graph with document and section nodes.

This provider is useful for:
- Testing document graph operations without setting up external schema sources
- Providing a fallback schema when user-defined schemas are not available
- Demonstrating the structure of a complete ETL schema
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from typing import Dict, Any
from document_graph.schema.providers.schema_provider_base import SchemaProviderBase
from document_graph.schema.etl_schema_model import ETLSchema, ExtractConfig, TransformConfig, LoadConfig, ChunkingConfig, MetadataMapping, EntityExtractionConfig, NormalizeConfig, NodeDefinition, RelationshipDefinition


class StaticSchemaProvider(SchemaProviderBase):
    """
    A schema provider that returns a hardcoded, static ETL schema.
    
    This class implements the SchemaProviderBase interface to provide a default
    ETL schema without requiring any external configuration sources. It initializes
    with a complete, hardcoded schema that includes all necessary components for
    the ETL process.
    
    The static schema includes:
    - Extract configuration for S3 PDF documents
    - Transform configuration with chunking, metadata mapping, entity extraction, and normalization
    - Load configuration with document and section nodes and their relationships
    
    This provider is particularly useful for:
    - Testing and development environments
    - Providing a fallback when no user-defined schema is available
    - Demonstrating a complete schema structure
    
    Attributes:
        _schema (ETLSchema): The hardcoded ETL schema instance.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize a static schema provider with a hardcoded ETL schema.
        
        This constructor creates a complete ETL schema with predefined settings for
        all components. The config parameter is accepted to maintain a consistent
        interface with other schema providers, but it is not used in this implementation.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary (unused in this provider).
                                    Included for interface consistency with other providers.
        """
        # Config is unused for static schema, but accepted to keep constructor consistent
        self._schema = ETLSchema(
            schema_id="static-default",
            description="Default hardcoded ETL schema",
            extract=ExtractConfig(
                source_type="s3",
                bucket="default-bucket",
                prefix="docs/",
                file_type="pdf",
                reader="pymupdf"
            ),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="by_heading", min_length=100),
                metadata_mapping=MetadataMapping(
                    title="document.title",
                    author="metadata.author"
                ),
                entity_extraction=EntityExtractionConfig(method="ner", model="spacy_en_core_web_lg"),
                normalize=NormalizeConfig(remove_headers=True)
            ),
            load=LoadConfig(
                document_node=NodeDefinition(
                    type="DocumentNode",
                    fields=["title", "document_type", "metadata"]
                ),
                section_node=NodeDefinition(
                    type="SectionNode",
                    fields=["title", "level", "text_units"]
                ),
                relationships=[
                    RelationshipDefinition(
                        type="has_section",
                        source="document_id",
                        target="section_id"
                    )
                ]
            )
        )

    @classmethod
    def from_config(cls, config):
        """Factory method — StaticSchemaProvider ignores config."""
        return cls(config if isinstance(config, dict) else {})

    def load_schema(self) -> ETLSchema:
        """
        Load and return the static ETL schema.
        
        This method implements the SchemaProviderBase interface's pipeline_schema method.
        Unlike other schema providers that might load from external sources, this method
        simply returns the hardcoded schema that was created during initialization.
        
        Returns:
            ETLSchema: The static, hardcoded ETL schema.
        """
        return self._schema

    def get_schema_id(self) -> str:
        """
        Get the unique identifier for the static schema.
        
        This method implements the SchemaProviderBase interface's get_schema_id method.
        It returns the schema_id property from the hardcoded schema.
        
        Returns:
            str: The unique identifier for the schema (e.g., "static-default").
        """
        return self._schema.schema_id
