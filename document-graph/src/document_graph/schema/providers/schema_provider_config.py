# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Provider Config Module for Document Graph Operations.

This module defines the configuration model for schema providers used in document graph
operations. It provides a standardized way to configure different types of schema providers
through a Pydantic model.

The module includes the following components:
- SchemaProviderConfig: Base configuration model for all schema providers

The SchemaProviderConfig class is used to configure schema providers with information
about the provider type, schema ID, and connection-specific parameters. This configuration
is used by the schema provider factory to instantiate the appropriate provider.

Usage:
    # Create a configuration for a file-based schema provider
    from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
    
    config = SchemaProviderConfig(
        type="file",
        schema_id="my_schema",
        connection_config={
            "path": "/path/to/schema.yaml"
        }
    )
    
    # Use the configuration to create a schema provider
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    provider = get_schema_provider(config)
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available

from typing import Dict, Any, Literal, Optional
from pydantic import BaseModel, Field

class SchemaProviderConfig(BaseModel):
    """
    Base configuration for schema providers.
    
    This Pydantic model defines the configuration parameters for schema providers
    used in document graph operations. It provides a standardized way to configure
    different types of schema providers with information about the provider type,
    schema ID, and connection-specific parameters.
    
    The SchemaProviderConfig is used by the schema provider factory to instantiate
    the appropriate provider based on the specified type. Each provider type has
    its own specific connection configuration requirements.
    
    Attributes:
        type (str): The type of schema provider. Must be one of the supported types:
            "file", "s3", "static", "csv", "json", "excel", "glue", "parquet", "yaml", "xml".
        schema_id (Optional[str]): An optional unique identifier for the schema.
            If not provided, the provider will generate one based on the source.
        connection_config (Dict[str, Any]): A dictionary containing connection-specific
            configuration parameters. The exact structure depends on the provider type.
            For example:
            - "file" provider: {"path": "/path/to/schema.yaml"}
            - "s3" provider: {"bucket": "my-bucket", "key": "path/to/schema.yaml"}
    """
    type: Literal["file", "s3", "static", "csv", "json", "excel", "glue", "parquet", "yaml", "xml"] = Field(..., description="Type of schema provider")
    schema_id: Optional[str] = Field(None, description="Unique ID for the schema (optional override)")
    connection_config: Dict[str, Any] = Field(default_factory=dict, description="Connection-specific configuration")
