from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DirectoryReaderConfig
from llama_index.core.schema import Document

class DirectoryReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for directories using LlamaIndex's SimpleDirectoryReader."""

    def __init__(self, config: DirectoryReaderConfig):
        """Initialize with DirectoryReaderConfig."""
        # Lazy import
        try:
            from llama_index.core import SimpleDirectoryReader
        except ImportError as e:
            raise ImportError(
                "SimpleDirectoryReader requires 'llama-index'. Install with: pip install llama-index"
            ) from e

        reader_kwargs = {
            "input_dir": config.input_dir,
            "exclude_hidden": config.exclude_hidden,
            "recursive": config.recursive
        }
        
        # Add required_exts if specified
        if config.required_exts:
            reader_kwargs["required_exts"] = config.required_exts
        
        super().__init__(config=config, reader_cls=SimpleDirectoryReader, **reader_kwargs)
        self.directory_config = config
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read directory documents with metadata handling."""
        documents = self._reader.load_data()
        
        # Apply metadata function if provided
        if self.metadata_fn:
            for doc in documents:
                # Use input_source or config.input_dir for metadata
                source_path = input_source or self.directory_config.input_dir
                additional_metadata = self.metadata_fn(source_path)
                doc.metadata.update(additional_metadata)
        
        return documents