# Copyright (c) Evan Erwee. All rights reserved.

"""Schema Provider Config AWS Base Module for Document Graph Operations.

This module defines the base configuration model for AWS-backed schema providers
used in document graph operations. It provides a standardized way to configure
schema providers that interact with AWS services like S3 and AWS Glue.

The module includes the following components:
- AWSSchemaProviderConfig: Base configuration model for AWS-backed schema providers

The AWSSchemaProviderConfig class is used as a base for more specific AWS provider
configurations, providing common AWS connection parameters such as bucket, key,
region, and profile name. This configuration is extended by specific AWS provider
implementations like S3SchemaProvider and GlueSchemaProvider.

Usage:
    # Create a configuration for an S3-based schema provider
    from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
    
    config = SchemaProviderConfig(
        type="s3",
        schema_id="my_s3_schema",
        connection_config={
            "bucket": "my-schema-bucket",
            "key": "schemas/my-schema.json",
            "region": "us-west-2",
            "profile_name": "my-aws-profile"
        }
    )
    
    # Use the configuration to create a schema provider
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    provider = get_schema_provider(config)
"""

import logging


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available


from pydantic import BaseModel, Field
from typing import Optional


class AWSSchemaProviderConfig(BaseModel):
    """
    Configuration base class for AWS-backed schema providers (e.g., S3, Glue).

    Attributes:
        bucket (str): S3 bucket name where the schema is stored.
        key (str): S3 key (file path) to the schema JSON file.
        region (Optional[str]): AWS region of the S3 bucket (default: inferred from environment or session).
        profile_name (Optional[str]): Named AWS CLI profile to use for credentials (optional).
    """

    bucket: str = Field(..., description="S3 bucket name containing the schema")
    key: str = Field(..., description="S3 key (path) to the schema JSON file")
    region: Optional[str] = Field(None, description="AWS region for the S3 bucket")
    profile_name: Optional[str] = Field(None, description="AWS CLI profile to use for credentials (optional)")
