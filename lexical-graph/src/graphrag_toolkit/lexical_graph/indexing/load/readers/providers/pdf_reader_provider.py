# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import PDFReaderConfig
from graphrag_toolkit.lexical_graph.indexing.load.readers.s3_file_mixin import S3FileMixin
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class PDFReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Reader provider for PDF files with S3 support using LlamaIndex's PyMuPDFReader."""

    def __init__(self, config: PDFReaderConfig):
        """Initialize with PDFReaderConfig."""
        try:
            from llama_index.readers.file.pymu_pdf import PyMuPDFReader
        except ImportError as e:
            logger.error("Failed to import PyMuPDFReader: missing pymupdf")
            raise ImportError(
                "pymupdf package not found, install with 'pip install pymupdf'"
            ) from e

        super().__init__(config=config, reader_cls=PyMuPDFReader)
        self.return_full_document = config.return_full_document
        self.metadata_fn = config.metadata_fn
        logger.debug(f"Initialized PDFReaderProvider with return_full_document={config.return_full_document}")

    def read(self, input_source) -> List[Document]:
        """Read PDF documents from local files or S3 with metadata handling."""
        if not input_source:
            logger.error("No input source provided to PDFReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        logger.info(f"Reading PDF from: {input_source}")
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            logger.debug(f"Processing PDF file: {processed_paths[0]}")
            if self.return_full_document:
                documents = self._reader.load_data(file_path=processed_paths[0], return_full_document=True)
            else:
                documents = self._reader.load_data(file_path=processed_paths[0])
            
            logger.info(f"Successfully read {len(documents)} document(s) from PDF")
            
            if self.metadata_fn:
                for doc in documents:
                    additional_metadata = self.metadata_fn(original_paths[0])
                    doc.metadata.update(additional_metadata)
                    doc.metadata['source'] = self._get_file_source_type(original_paths[0])
            
            return documents
        except Exception as e:
            logger.error(f"Failed to read PDF from {input_source}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read PDF: {e}") from e
        finally:
            self._cleanup_temp_files(temp_files)