# Copyright (c) Evan Erwee. All rights reserved.

"""S3 Schema Provider Module for Document Graph Operations.

This module provides a schema provider for loading and saving ETL schemas from/to Amazon S3.
It retrieves schema files from S3 buckets and can also save schemas back to S3.

The module includes the following components:
- S3SchemaProvider: Schema provider for Amazon S3

The S3 schema provider loads ETL schemas directly from JSON files stored in S3 buckets
and can also save schemas back to S3. It's commonly used to store and retrieve pre-defined
or previously discovered schemas in cloud environments.

Usage:
    # Get an S3 schema provider for a specific schema file in S3
    from document_graph.schema.providers.schema_provider_factory import get_schema_provider
    
    config = {
        "provider_type": "s3",
        "schema_id": "my_s3_schema",
        "connection_config": {
            "bucket": "my-schema-bucket",
            "key": "schemas/my-schema.json",
            "region": "us-east-1"
        }
    }
    
    provider = get_schema_provider(config)
    
    # Load the schema
    schema = provider.pipeline_schema()
    
    # Use the schema for ETL operations
    print(f"Loaded schema ID: {schema.schema_id}")
    print(f"Fields: {schema.load.document_node.fields}")
    
    # Save a modified schema back to S3
    provider.save_schema()
"""

# Import custom logging to ensure configuration is available

import json
import logging
from typing import Any, Optional

import boto3
from botocore.config import Config as BotoConfig

from document_graph.schema.providers.schema_provider_base import SchemaProviderBase
from document_graph.schema.providers.schema_provider_config import SchemaProviderConfig
from document_graph.schema.etl_schema_model import ETLSchema

logger = logging.getLogger(__name__)


class S3SchemaProvider(SchemaProviderBase):
    """
    Schema provider for loading and saving ETL schemas from/to Amazon S3.
    
    This class provides functionality to load pre-defined ETL schemas from JSON files
    stored in S3 buckets and save schemas back to S3. It handles AWS authentication
    and S3 operations to retrieve and store schema files.
    
    The provider is commonly used in cloud environments to store and retrieve
    pre-defined or previously discovered schemas for reuse in ETL pipelines.
    
    Attributes:
        config (SchemaProviderConfig): Configuration for the provider.
        bucket (str): The name of the S3 bucket containing the schema file.
        key (str): The S3 key (path) to the schema JSON file.
        region (str): The AWS region where the S3 bucket is located.
        session: The boto3 session for AWS authentication.
        s3: The boto3 S3 client used for S3 operations.
    """

    def __init__(self, config: SchemaProviderConfig):
        """
        Initialize an S3 schema provider with the given configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider, including
                the bucket and key in the connection_config.
                
        Raises:
            ValueError: If the bucket or key is not provided in the connection_config,
                or if the key does not point to a JSON file.
        """
        self.config = config
        conn = config.connection_config

        self.bucket = conn.get("bucket")
        self.key = conn.get("key")
        self.region = conn.get("region", "us-east-1")
        self.session = conn.get("session") or boto3.Session()

        if not self.bucket or not self.key:
            raise ValueError(
                "s3.connection_config",
                conn,
                "bucket and key must be provided"
            )

        if not self.key.endswith(".json"):
            raise ValueError(
                "s3.key",
                self.key,
                "S3 key must point to a .json file"
            )

        self.s3 = self.session.client(
            "s3",
            region_name=self.region,
            config=BotoConfig(retries={"max_attempts": 3})
        )

    @classmethod
    def from_config(cls, config: SchemaProviderConfig) -> "S3SchemaProvider":
        """
        Factory method to construct an S3 provider from a configuration.
        
        Args:
            config (SchemaProviderConfig): Configuration for the provider.
            
        Returns:
            S3SchemaProvider: A new instance of the S3 schema provider.
        """
        return cls(config)

    def load_schema(self, source=None, **kwargs) -> ETLSchema:
        """
        Load the ETL schema from the S3 bucket and parse it into an ETLSchema object.
        
        This method retrieves the JSON schema file from S3, parses it, and returns
        an ETLSchema object. It handles AWS authentication and S3 operations to
        retrieve the schema file.
        
        Args:
            source: Optional source override (not used in this provider).
            **kwargs: Additional provider-specific parameters (not used in this provider).
            
        Returns:
            ETLSchema: The ETL schema loaded from the S3 file.
            
        Raises:
            FileNotFoundError: If the S3 object does not exist.
            ValueError: If the S3 object cannot be parsed or is not a valid ETL schema.
            Exception: If there's an error connecting to AWS or retrieving the object.
        """
        s3_path = f"s3://{self.bucket}/{self.key}"
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            schema_str = response["Body"].read().decode("utf-8")
            schema_json = json.loads(schema_str)
            logger.info(f"Successfully loaded schema from {s3_path}")
            return ETLSchema(**schema_json)
        except self.s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"File not found")
        except Exception as e:
            logger.error(f"Error loading schema from {s3_path}: {e}")
            raise RuntimeError(f"S3 schema load failed: {e}") from e

    def save_schema(self, output_key: Optional[str] = None) -> None:
        """
        Save the ETL schema to the S3 bucket under the given key.
        
        This method retrieves the current schema, serializes it to JSON, and saves
        it to the S3 bucket. If no output key is provided, the original key is used.
        
        Args:
            output_key (Optional[str]): The S3 key where the schema should be saved.
                If not provided, the original key is used.
            
        Raises:
            IOError: If there's an error writing to S3.
            Exception: If there's an error connecting to AWS or retrieving the schema.
        """
        schema = self.load_schema()
        key_to_use = output_key or self.key
        s3_path = f"s3://{self.bucket}/{key_to_use}"
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key_to_use,
                Body=json.dumps(schema.model_dump(mode="json", exclude_unset=True), indent=2).encode("utf-8"),
                ContentType="application/json"
            )
            logger.info(f"Schema saved to {s3_path}")
        except Exception as e:
            raise IOError(f"IO error: {s3_path, e}")

    def get_schema_id(self) -> str:
        """
        Return the unique identifier for the schema.
        
        This method returns a unique identifier for the schema, either from the
        configuration or generated from the S3 key.
        
        Returns:
            str: A unique identifier for the schema.
        """
        return self.config.schema_id or self.key.split("/")[-1].replace(".json", "")
