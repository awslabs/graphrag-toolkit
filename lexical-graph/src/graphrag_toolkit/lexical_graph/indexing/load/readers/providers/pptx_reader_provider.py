# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from ..llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from ..reader_provider_config import PPTXReaderConfig
from ..s3_file_mixin import S3FileMixin
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class PPTXReaderProvider(LlamaIndexReaderProviderBase, S3FileMixin):
    """Reader provider for PPTX files with S3 support using LlamaIndex's PptxReader."""

    def __init__(self, config: PPTXReaderConfig):
        """Initialize with PPTXReaderConfig."""
        try:
            from llama_index.readers.file.slides import PptxReader
        except ImportError as e:
            logger.error("Failed to import PptxReader: missing python-pptx")
            raise ImportError(
                "python-pptx package not found, install with 'pip install python-pptx'"
            ) from e

        super().__init__(config=config, reader_cls=PptxReader)
        self.metadata_fn = config.metadata_fn
        logger.debug("Initialized PPTXReaderProvider")

    def read(self, input_source) -> List[Document]:
        """Read PPTX documents from local files or S3 with metadata handling."""
        if not input_source:
            logger.error("No input source provided to PPTXReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        logger.info(f"Reading PPTX from: {input_source}")
        processed_paths, temp_files, original_paths = self._process_file_paths(input_source)
        
        try:
            documents = self._reader.load_data(file=processed_paths[0])
            logger.info(f"Successfully read {len(documents)} document(s) from PPTX")
            
            if self.metadata_fn:
                for doc in documents:
                    additional_metadata = self.metadata_fn(original_paths[0])
                    doc.metadata.update(additional_metadata)
                    doc.metadata['source'] = self._get_file_source_type(original_paths[0])
            
            return documents
        except Exception as e:
            logger.error(f"Failed to read PPTX from {input_source}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read PPTX: {e}") from e
        finally:
            self._cleanup_temp_files(temp_files)