from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import CSVReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin
from llama_index.core.schema import Document

class CSVReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Reader provider for CSV files with S3 support using LlamaIndex's PandasCSVReader."""

    def __init__(self, config: CSVReaderConfig):
        """Initialize with CSVReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.file.tabular import PandasCSVReader
        except ImportError as e:
            raise ImportError(
                "PandasCSVReader requires 'pandas'. Install with: pip install pandas"
            ) from e

        reader_kwargs = {
            "concat_rows": config.concat_rows
        }
        
        super().__init__(config=config, reader_cls=PandasCSVReader, **reader_kwargs)
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read CSV documents from local files or S3 with metadata handling."""
        # Process file paths (handles S3 downloads)
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            documents = self._reader.load_data(file=processed_paths[0])
            
            # Apply metadata function if provided
            if self.metadata_fn:
                for doc in documents:
                    additional_metadata = self.metadata_fn(original_paths[0])
                    doc.metadata.update(additional_metadata)
                    # Add source type
                    doc.metadata['source'] = self._get_file_source_type(original_paths[0])
            
            return documents
        finally:
            self._cleanup_temp_files(temp_files)