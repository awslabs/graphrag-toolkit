from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import PDFReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin
from llama_index.core.schema import Document

class PDFReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Reader provider for PDF files with S3 support using LlamaIndex's PyMuPDFReader."""

    def __init__(self, config: PDFReaderConfig):
        """Initialize with PDFReaderConfig."""
        # Lazy import
        try:
            from llama_index.readers.file.pymu_pdf import PyMuPDFReader
        except ImportError as e:
            raise ImportError(
                "PyMuPDFReader requires 'pymupdf'. Install with: pip install pymupdf"
            ) from e

        # PyMuPDFReader doesn't accept constructor arguments
        super().__init__(config=config, reader_cls=PyMuPDFReader)
        self.return_full_document = config.return_full_document
        self.metadata_fn = config.metadata_fn

    def read(self, input_source) -> List[Document]:
        """Read PDF documents from local files or S3 with metadata handling."""
        # Process file paths (handles S3 downloads)
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            # PyMuPDFReader expects file_path parameter
            if self.return_full_document:
                documents = self._reader.load_data(file_path=processed_paths[0], return_full_document=True)
            else:
                documents = self._reader.load_data(file_path=processed_paths[0])
            
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