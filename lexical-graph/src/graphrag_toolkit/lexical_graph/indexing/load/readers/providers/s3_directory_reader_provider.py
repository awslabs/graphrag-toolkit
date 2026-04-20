# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from typing import List
from ..llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from ..reader_provider_config import S3DirectoryReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig

logger = logging.getLogger(__name__)

class S3DirectoryReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for S3 file(s) using LlamaIndex's S3Reader. Supports both single key and prefix."""

    def __init__(self, config: S3DirectoryReaderConfig):
        """Initialize with S3DirectoryReaderConfig."""
        try:
            from llama_index.readers.s3 import S3Reader
        except ImportError as e:
            logger.error("Failed to import S3Reader: missing boto3")
            raise ImportError(
                "llama-index-readers-s3 package not found, install with 'pip install llama-index-readers-s3'"
            ) from e

        if not config.key and not config.prefix:
            logger.error("Neither key nor prefix specified for S3DirectoryReaderProvider")
            raise ValueError("You must specify either `key` (for a file) or `prefix` (for a folder).")
        if config.key and config.prefix:
            logger.error("Both key and prefix specified for S3DirectoryReaderProvider")
            raise ValueError("Specify only one of `key` or `prefix`, not both.")

        region = (
            config.aws_region 
            or GraphRAGConfig.aws_region 
            or os.environ.get('AWS_REGION', 'us-east-1')
        )

        reader_kwargs = {
            "bucket": config.bucket,
            "region_name": region,
        }
        if config.key:
            reader_kwargs["key"] = config.key
        elif config.prefix:
            reader_kwargs["prefix"] = config.prefix

        super().__init__(config=config, reader_cls=S3Reader, **reader_kwargs)

        self.s3_config = config
        self.metadata_fn = config.metadata_fn
        logger.debug(f"Initialized S3DirectoryReaderProvider for bucket: {config.bucket}, region: {region}")

    def read(self, input_source=None) -> List[Document]:
        """Read S3 document(s) with optional metadata enhancement."""
        s3_path = f"s3://{self.s3_config.bucket}/" + (self.s3_config.key or self.s3_config.prefix or "")
        logger.info(f"Reading from S3: {s3_path}")
        
        try:
            documents = self._reader.load_data()
            logger.info(f"Successfully read {len(documents)} document(s) from S3")

            if self.metadata_fn:
                for doc in documents:
                    additional_metadata = self.metadata_fn(s3_path)
                    doc.metadata.update(additional_metadata)

            return documents
        except Exception as e:
            logger.error(f"Failed to read from S3 path {s3_path}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read from S3: {e}") from e
