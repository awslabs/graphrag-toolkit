# Copyright (c) Evan Erwee. All rights reserved.

"""Document Graph Schema Definitions.

This module provides a centralized import location for all ETL schema models
used in document graph processing. It re-exports the core schema classes from
the etl_schema_model module to provide a clean, unified interface for working
with document graph schemas.

The exported classes include:
- ETLSchema: The top-level schema definition for ETL operations
- ExtractConfig: Configuration for data extraction from various sources
- TransformConfig: Configuration for document transformation operations
- LoadConfig: Configuration for loading processed documents into a graph
- Supporting classes for specific aspects of the ETL process

This module simplifies imports by allowing users to import all schema-related
classes from a single location rather than from multiple modules.

Example:
    from document_graph.schema.document_graph_schema import (
        ETLSchema, ExtractConfig, LoadConfig
    )
    
    # Create a schema using the imported classes
    schema = ETLSchema(
        schema_id="example-schema",
        extract=ExtractConfig(...),
        transform=TransformConfig(...),
        load=LoadConfig(...)
    )
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

from typing import List
from .etl_schema_model import (
    ETLSchema,
    ExtractConfig,
    TransformConfig,
    LoadConfig,
    ChunkingConfig,
    MetadataMapping,
    EntityExtractionConfig,
    NormalizeConfig,
    NodeDefinition,
    RelationshipDefinition,
)

__all__: List[str] = [
    "ETLSchema",
    "ExtractConfig",
    "TransformConfig",
    "LoadConfig",
    "ChunkingConfig",
    "MetadataMapping",
    "EntityExtractionConfig",
    "NormalizeConfig",
    "NodeDefinition",
    "RelationshipDefinition",
]
