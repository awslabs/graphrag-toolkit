# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Load Provider Configuration Module.

This module defines configuration classes for load providers in the
document graph pipeline.
"""

import logging
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)
# Import custom logging to ensure configuration is available
# logging via graphrag-toolkit  # noqa: F401



class LoadProviderConfig(BaseModel):
    """
    Represents the configuration for a load provider.

    Provides detailed setup, validation, and pre-defined methods to configure
    load providers based on type. These providers can be file-based, database-based,
    or S3 configurations. The class supports defining and customizing configurations
    with specific attributes and capabilities depending on the provider type.

    Attributes:
        name (str): Unique name for this load provider.
        type (str): Load provider type (e.g., 'file', 'database', 's3').
        output_dir (Optional[str]): Output directory for file-based providers.
        output_format (str): Output format (json, csv, parquet).
        database_config (Optional[Dict[str, Any]]): Database connection configuration.
        file_config (Optional[Dict[str, Any]]): File output configuration.
        s3_config (Optional[Dict[str, Any]]): S3 output configuration.
        parameters (Dict[str, Any]): Additional provider parameters.
        batch_size (int): Batch size for processing.
        overwrite (bool): Whether to overwrite existing output.
    """
    
    name: str = Field(..., description="Unique name for this load provider")
    type: str = Field(..., description="Load provider type (e.g., 'file', 'database', 's3')")
    output_dir: Optional[str] = Field(None, description="Output directory for file-based providers")
    output_format: str = Field(default="json", description="Output format (json, csv, parquet)")
    
    # Database-specific configuration
    database_config: Optional[Dict[str, Any]] = Field(None, description="Database connection configuration")
    
    # File-specific configuration
    file_config: Optional[Dict[str, Any]] = Field(None, description="File output configuration")
    
    # S3-specific configuration
    s3_config: Optional[Dict[str, Any]] = Field(None, description="S3 output configuration")
    
    # General parameters
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Additional provider parameters")
    
    # Processing options
    batch_size: int = Field(default=1000, description="Batch size for processing")
    overwrite: bool = Field(default=False, description="Whether to overwrite existing output")
    
    class Config:
        """Pydantic configuration."""
        extra = "allow"  # Allow additional fields
        
    def __post_init__(self):
        """
        Logs initialization details of the LoadProviderConfig object.

        This method is automatically called after the object is fully
        initialized, providing a debug log entry with the name and type
        of the configuration.

        Raises:
            None
        """
        logger.debug(f"Initialized LoadProviderConfig: {self.name} ({self.type})")
        
    @classmethod
    def create_file_config(
        cls,
        name: str,
        output_dir: str,
        output_format: str = "json",
        **kwargs
    ) -> "LoadProviderConfig":
        """
        Creates a configuration for a file-based load provider.

        This class method generates and returns a LoadProviderConfig instance
        with configuration tailored for file-based data load. It includes specific
        parameters such as the file output directory and format and encapsulates
        additional configuration options if provided.

        Parameters:
            name: str
                The name of the configuration.
            output_dir: str
                The directory where files will be output.
            output_format: str, optional
                The format of the output files. Defaults to "json".
            kwargs:
                Additional keyword arguments for custom configurations.

        Returns:
            LoadProviderConfig
                An instance of LoadProviderConfig initialized with the specified
                parameters.

        """
        return cls(
            name=name,
            type="file",
            output_dir=output_dir,
            output_format=output_format,
            file_config={
                "output_dir": output_dir,
                "format": output_format
            },
            **kwargs
        )
    
    @classmethod
    def create_database_config(
        cls,
        name: str,
        database_type: str,
        connection_config: Dict[str, Any],
        **kwargs
    ) -> "LoadProviderConfig":
        """
        Creates a LoadProviderConfig instance configured for a database load provider.

        This class method initializes a LoadProviderConfig object with parameters suited
        to a database setup. The method associates the instance with a specific
        database type and a set of connection configurations. Additional keyword arguments
        can be supplied to further customize the configuration.

        Parameters:
            name: str
                The name of the load provider configuration.
            database_type: str
                The type of database being configured, such as 'PostgreSQL', 'MySQL', etc.
            connection_config: Dict[str, Any]
                A dictionary containing the database connection configuration settings.
            **kwargs
                Additional keyword arguments to customize the configuration.

        Returns:
            LoadProviderConfig
                An instance of LoadProviderConfig initialized with database-related settings.
        """
        return cls(
            name=name,
            type="database",
            database_config={
                "type": database_type,
                "connection": connection_config
            },
            **kwargs
        )
    
    @classmethod
    def create_s3_config(
        cls,
        name: str,
        bucket: str,
        prefix: str = "",
        output_format: str = "json",
        **kwargs
    ) -> "LoadProviderConfig":
        """
        Creates a LoadProviderConfig instance for an s3 load provider.

        The method acts as a factory method to generate configurations specifically for
        an s3-based data load provider. The s3 configuration includes the bucket name,
        optional prefix for object paths, and the desired output format for processing.

        Parameters:
        name: str
            The name of the load provider configuration.
        bucket: str
            The bucket name of the s3 storage where data resides.
        prefix: str, optional
            The prefix of object paths within the s3 bucket. Default is an empty string.
        output_format: str, optional
            The format of output data (e.g., "json"). Default is "json".
        **kwargs: dict
            Additional arguments which will be passed to the LoadProviderConfig
            constructor.

        Returns:
        LoadProviderConfig
            A configured instance of LoadProviderConfig representing an s3 load provider.
        """
        return cls(
            name=name,
            type="s3",
            output_format=output_format,
            s3_config={
                "bucket": bucket,
                "prefix": prefix,
                "format": output_format
            },
            **kwargs
        )