from typing import List, Union
from ..llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from ..reader_provider_config import NotionReaderConfig
from llama_index.core.schema import Document

class NotionReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for Notion pages using LlamaIndex's NotionPageReader."""

    def __init__(self, config: NotionReaderConfig):
        """Initialize with NotionReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.notion import NotionPageReader
        except ImportError as e:
            raise ImportError(
                "NotionPageReader requires 'notion-client'. Install with: pip install notion-client"
            ) from e

        reader_kwargs = {
            "integration_token": config.integration_token
        }
        
        super().__init__(config=config, reader_cls=NotionPageReader, **reader_kwargs)
        self.metadata_fn = config.metadata_fn

    def read(self, input_source: Union[str, List[str]]) -> List[Document]:
        """Read Notion page documents with metadata handling."""
        # NotionPageReader expects page_ids as a list
        page_ids = [input_source] if isinstance(input_source, str) else input_source
        documents = self._reader.load_data(page_ids=page_ids)
        
        # Apply metadata function if provided
        if self.metadata_fn:
            for doc in documents:
                # Use the first page_id for metadata context
                page_context = page_ids[0] if page_ids else str(input_source)
                additional_metadata = self.metadata_fn(page_context)
                doc.metadata.update(additional_metadata)
        
        return documents