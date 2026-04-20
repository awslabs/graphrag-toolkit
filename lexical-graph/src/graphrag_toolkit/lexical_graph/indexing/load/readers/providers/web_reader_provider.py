# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WebReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class WebReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for web pages using LlamaIndex's SimpleWebPageReader."""

    def __init__(self, config: WebReaderConfig):
        """Initialize with WebReaderConfig."""
        try:
            from llama_index.readers.web import SimpleWebPageReader
        except ImportError as e:
            logger.error("Failed to import SimpleWebPageReader: missing dependencies")
            raise ImportError(
                "llama-index-readers-web package not found, install with 'pip install llama-index-readers-web'"
            ) from e

        reader_kwargs = {"html_to_text": config.html_to_text}
        super().__init__(config=config, reader_cls=SimpleWebPageReader, **reader_kwargs)
        logger.debug(f"Initialized WebReaderProvider with html_to_text={config.html_to_text}")

    def read(self, input_source) -> List[Document]:
        """Read web page documents with proper parameter handling."""
        if not input_source:
            logger.error("No input source provided to WebReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        urls = [input_source] if isinstance(input_source, str) else input_source
        logger.info(f"Reading {len(urls)} web page(s)")
        
        try:
            docs = self._reader.load_data(urls=urls)
            logger.info(f"Successfully read {len(docs)} document(s) from {len(urls)} URL(s)")
            return docs
        except Exception as e:
            logger.error(f"Failed to read web pages from {urls}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read web pages: {e}") from e