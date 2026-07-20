# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Package for Document Graph Operations.

This package provides the core schema definitions and utilities for working with
document graph ETL (Extract, Transform, Load) operations. It includes models for
defining extraction sources, transformation rules, and graph loading configurations.

The schema package contains:
- ETL schema models for defining document processing pipelines
- Schema providers for loading schemas from various sources
- Schema discovery for automatic schema inference
- Utilities for schema serialization and deserialization
- Static schema definitions for testing and default configurations

These components work together to provide a flexible and extensible way to define
how documents are processed and loaded into graph databases.
"""

# Core schema models
from .etl_schema_model import ETLSchema, ExtractConfig, TransformConfig, LoadConfig
from .document_graph_schema import (
    ChunkingConfig, MetadataMapping, EntityExtractionConfig, NormalizeConfig,
    NodeDefinition, RelationshipDefinition
)

# Schema I/O utilities
from .schema_io import save_schema, load_schema

# Static schema provider
from .static_schema_provider import StaticSchemaProvider

# Import submodules for convenience
from . import providers
from . import discovery

__all__ = [
    # Core schema models
    'ETLSchema',
    'ExtractConfig', 
    'TransformConfig',
    'LoadConfig',
    'ChunkingConfig',
    'MetadataMapping',
    'EntityExtractionConfig',
    'NormalizeConfig',
    'NodeDefinition',
    'RelationshipDefinition',
    
    # Schema I/O utilities
    'save_schema',
    'load_schema',
    
    # Utilities
    'StaticSchemaProvider',
    
    # Submodules
    'providers',
    'discovery'
]

