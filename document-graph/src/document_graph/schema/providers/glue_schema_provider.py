# Copyright (c) Evan Erwee. All rights reserved.

"""AWS Glue Schema Provider Module for Document Graph Operations.

This module provides a schema provider for loading ETL schemas from AWS Glue Schema Registry.
It retrieves AVRO schemas from Glue and converts them into ETL schemas for processing
and loading data into a document graph.

The module includes the following components:
- GlueSchemaProvider: Schema provider for AWS Glue Schema Registry

The Glue schema provider connects to AWS Glue Schema Registry, retrieves the latest
version of a specified schema, and converts it into an ETL schema. It extracts field
information from AVRO schemas and builds appropriate ETL schemas for document graph
operations.

Usage:
    # Get a Glue schema provider for a specific schema
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "glue",
        "schema_id": "my_glue_schema",
        "connection_config": {
            "registry_name": "my-registry",
            "schema_name": "my-schema",
            "region": "us-east-1"
        }
    }
    
    provider = get_schema_provider(config)
    
    # Load the schema
    schema = provider.pipeline_schema()
    
    # Use the schema for ETL operations
    print(f"Loaded schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
"""

# Import custom logging to ensure configuration is available

import json
import logging
from typing import Any, List, Optional

from document_graph.schema.providers.schema_provider_base import SchemaProviderBase
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.etl_schema_model import (
    ETLSchema, ExtractConfig, TransformConfig, LoadConfig,
    ChunkingConfig, MetadataMapping, EntityExtractionConfig, NormalizeConfig,
    NodeDefinition, RelationshipDefinition
)
from document_graph.config import DocumentGraphConfig

logger = logging.getLogger(__name__)


class GlueSchemaProvider(SchemaProviderBase):
    """
    Schema provider for AWS Glue Schema Registry.
    
    This class provides functionality to load ETL schemas from AWS Glue Schema Registry.
    It connects to the registry, retrieves the latest version of a specified schema,
    and converts it into an ETL schema for document graph operations.
    
    The provider extracts field information from AVRO schemas in the Glue registry
    and builds appropriate ETL schemas with those fields. It supports AWS authentication
    through boto3 sessions and can be configured with different AWS regions.
    
    Attributes:
        config (SchemaProviderConfig): Configuration for the provider.
        registry_name (str): The name of the Glue schema registry.
        schema_name (str): The name of the schema in the registry.
        region (str): The AWS region where the registry is located.
        session: The boto3 session for AWS authentication.
    """

    def __init__(self, config: SchemaProviderConfig):
        """
        Initialize a Glue schema provider with the given configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider, including
                the registry name and schema name in the connection_config.
                
        Raises:
            ValueError: If the registry name or schema name is not provided in the connection_config.
        """
        self.config = config
        conn = config.connection_config

        self.registry_name = conn.get("registry_name")
        self.schema_name = conn.get("schema_name")
        self.region = conn.get("region", DocumentGraphConfig.aws_region)
        self.session = conn.get("session") or DocumentGraphConfig.session

        if not self.registry_name or not self.schema_name:
            raise ErrorHandler.validation_error(
                "glue.connection_config",
                conn,
                "registry_name and schema_name must be defined"
            )

    @classmethod
    def from_config(cls, config: SchemaProviderConfig) -> "GlueSchemaProvider":
        """
        Factory method to construct a Glue provider from a configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider.
            
        Returns:
            GlueSchemaProvider: A new instance of the Glue schema provider.
        """
        return cls(config)

    def load_schema(self, source=None, **kwargs) -> ETLSchema:
        """
        Load an ETL schema from AWS Glue Schema Registry.
        
        This method connects to AWS Glue Schema Registry, retrieves the latest version
        of the specified schema, extracts field information from the AVRO schema,
        and builds an appropriate ETL schema for document graph operations.
        
        Args:
            source: Optional source override (not used in this provider).
            **kwargs: Additional provider-specific parameters (not used in this provider).
            
        Returns:
            ETLSchema: A complete ETL schema object built from the Glue schema.
            
        Raises:
            ValueError: If the schema does not exist in the registry or cannot be parsed.
            Exception: If there's an error connecting to AWS or retrieving the schema.
        """
        glue = self.session.client("glue", region_name=self.region)
        try:
            response = glue.get_schema_version(
                SchemaId={
                    "SchemaName": self.schema_name,
                    "RegistryName": self.registry_name
                },
                SchemaVersionNumber={"LatestVersion": True}
            )
            schema_definition = response["SchemaDefinition"]
            fields = self._parse_avro_fields(schema_definition)

            logger.info(f"Loaded schema from Glue registry: {self.registry_name}/{self.schema_name}")
            return self._build_minimal_etl_schema(fields)

        except glue.exceptions.EntityNotFoundException:
            raise ErrorHandler.validation_error(
                "glue_schema",
                f"{self.registry_name}/{self.schema_name}",
                "existing schema in Glue registry"
            )
        except Exception as e:
            logger.error(f"Failed to load Glue schema: {e}")
            raise ErrorHandler.database_error("Glue schema load", e)

    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema, either from the
        configuration or generated from the registry and schema names.
        
        Returns:
            str: A unique identifier for the schema.
        """
        return self.config.schema_id or f"{self.registry_name}:{self.schema_name}"

    def _parse_avro_fields(self, schema_def: str) -> List[str]:
        """
        Parse field names from an AVRO schema definition.
        
        This method parses the AVRO schema definition to extract field names,
        safely handling union types and defaults. It validates that the schema
        is of AVRO record type and contains fields.
        
        Args:
            schema_def (str): The AVRO schema definition as a JSON string.
            
        Returns:
            List[str]: A list of field names extracted from the AVRO schema.
            
        Raises:
            ValueError: If the schema is not of AVRO record type, is missing fields,
                or no fields could be extracted.
            Exception: If there's an error parsing the schema.
        """
        try:
            schema_json = json.loads(schema_def)

            # Handle AVRO record type at root
            if schema_json.get("type") != "record" or "fields" not in schema_json:
                raise ValueError("Schema is not of AVRO record type or missing 'fields'")

            field_names = []
            for field in schema_json["fields"]:
                name = field.get("name")
                if not name:
                    logger.warning(f"Skipping unnamed field in AVRO schema: {field}")
                    continue
                field_names.append(name)

            if not field_names:
                raise ValueError("No fields extracted from AVRO schema")

            return field_names
        except Exception as e:
            raise ErrorHandler.schema_error("parse", "glue_avro", e)

    def _build_minimal_etl_schema(self, field_names: List[str]) -> ETLSchema:
        """
        Build a minimal ETL schema from the extracted field names.
        
        This method creates a basic ETL schema with default configurations for
        extraction, transformation, and loading, using the field names extracted
        from the AVRO schema.
        
        Args:
            field_names (List[str]): The list of field names to include in the schema.
            
        Returns:
            ETLSchema: A complete ETL schema object with the specified fields.
        """
        return ETLSchema(
            schema_id=self.get_schema_id(),
            description=f"Autogenerated from Glue schema {self.registry_name}/{self.schema_name}",
            extract=ExtractConfig(source_type="s3"),
            transform=TransformConfig(
                chunking=ChunkingConfig(strategy="fixed_length"),
                metadata_mapping=MetadataMapping(),
                entity_extraction=EntityExtractionConfig(method="ner"),
                normalize=NormalizeConfig()
            ),
            load=LoadConfig(
                document_node=NodeDefinition(type="Document", fields=field_names),
                section_node=NodeDefinition(type="Section", fields=[]),
                relationships=[
                    RelationshipDefinition(type="contains", source="document_id", target="section_id")
                ]
            )
        )
