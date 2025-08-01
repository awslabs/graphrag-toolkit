from typing import List
from ..llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from ..reader_provider_config import S3DirectoryReaderConfig
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig

class S3DirectoryReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for S3 file(s) using LlamaIndex's S3Reader. Supports both single key and prefix."""

    def __init__(self, config: S3DirectoryReaderConfig):
        """Initialize with S3DirectoryReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.s3 import S3Reader
        except ImportError as e:
            raise ImportError(
                "S3Reader requires 'boto3'. Install with: pip install boto3"
            ) from e

        # Ensure valid key/prefix combination
        if not config.key and not config.prefix:
            raise ValueError("You must specify either `key` (for a file) or `prefix` (for a folder).")
        if config.key and config.prefix:
            raise ValueError("Specify only one of `key` or `prefix`, not both.")

        # Get global AWS session
        aws_session = GraphRAGConfig.session

        # Build reader arguments
        reader_kwargs = {
            "bucket": config.bucket,
            "aws_session": aws_session
        }
        if config.key:
            reader_kwargs["key"] = config.key
        elif config.prefix:
            reader_kwargs["prefix"] = config.prefix

        # Pass to LlamaIndexReaderProviderBase
        super().__init__(config=config, reader_cls=S3Reader, **reader_kwargs)

        self.s3_config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source=None) -> List[Document]:
        """Read S3 document(s) with optional metadata enhancement."""
        documents = self._reader.load_data()

        if self.metadata_fn:
            s3_path = f"s3://{self.s3_config.bucket}/" + (self.s3_config.key or self.s3_config.prefix or "")
            for doc in documents:
                additional_metadata = self.metadata_fn(s3_path)
                doc.metadata.update(additional_metadata)

        return documents
