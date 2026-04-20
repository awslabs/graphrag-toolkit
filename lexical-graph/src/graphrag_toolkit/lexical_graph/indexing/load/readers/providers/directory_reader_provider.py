# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DirectoryReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class DirectoryReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for directories using LlamaIndex's SimpleDirectoryReader."""

    def __init__(self, config: DirectoryReaderConfig):
        """Initialize with DirectoryReaderConfig."""
        try:
            from llama_index.core import SimpleDirectoryReader
        except ImportError as e:
            logger.error("Failed to import SimpleDirectoryReader")
            raise ImportError(
                "SimpleDirectoryReader requires 'llama-index'. Install with: pip install llama-index"
            ) from e

        reader_kwargs = {
            "input_dir": config.input_dir,
            "exclude_hidden": config.exclude_hidden,
            "recursive": config.recursive
        }
        
        if config.required_exts:
            reader_kwargs["required_exts"] = config.required_exts
        
        super().__init__(config=config, reader_cls=SimpleDirectoryReader, **reader_kwargs)
        self.directory_config = config
        self.metadata_fn = config.metadata_fn
        logger.debug(f"Initialized DirectoryReaderProvider for: {config.input_dir}")

    def read(self, input_source) -> List[Document]:
        """Read directory documents with metadata handling."""
        logger.info(f"Reading directory: {self.directory_config.input_dir}")
        
        try:
            documents = self._reader.load_data()
            logger.info(f"Successfully read {len(documents)} document(s) from directory")
            
            if self.metadata_fn:
                source_path = input_source or self.directory_config.input_dir
                for doc in documents:
                    additional_metadata = self.metadata_fn(source_path)
                    doc.metadata.update(additional_metadata)
            
            return documents
        except Exception as e:
            logger.error(f"Failed to read directory {self.directory_config.input_dir}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read directory: {e}") from e