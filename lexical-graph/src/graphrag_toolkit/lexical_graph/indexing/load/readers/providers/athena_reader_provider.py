from typing import List
from ..llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from ..reader_provider_config import AthenaReaderConfig
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig

class AthenaReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for AWS Athena using LlamaIndex's AthenaReader."""

    def __init__(self, config: AthenaReaderConfig):
        """Initialize with AthenaReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.athena import AthenaReader
        except ImportError as e:
            raise ImportError(
                "AthenaReader requires 'boto3' and 'pyathena'. Install with: pip install boto3 pyathena"
            ) from e

        # Use GraphRAGConfig for AWS session management
        aws_session = None
        if config.aws_profile or config.aws_region:
            # Use config-specific session
            aws_session = config.get_boto3_session()
        else:
            # Use global GraphRAGConfig session
            aws_session = GraphRAGConfig.session
        
        reader_kwargs = {
            "database": config.database,
            "s3_output_location": config.s3_output_location,
            "aws_session": aws_session
        }
        
        super().__init__(config=config, reader_cls=AthenaReader, **reader_kwargs)
        self.athena_config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read Athena query results with metadata handling."""
        # Use input_source as SQL query
        query = input_source if isinstance(input_source, str) else str(input_source)
        documents = self._reader.load_data(query=query)
        
        # Apply metadata function if provided
        if self.metadata_fn:
            for doc in documents:
                additional_metadata = self.metadata_fn(query)
                doc.metadata.update(additional_metadata)
        
        return documents