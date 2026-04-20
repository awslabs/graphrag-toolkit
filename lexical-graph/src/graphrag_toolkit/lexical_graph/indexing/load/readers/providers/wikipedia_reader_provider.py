# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List, Union
from llama_index.core.schema import Document
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import WikipediaReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class WikipediaReaderProvider:
    """Reader provider for Wikipedia articles using LlamaIndex's WikipediaReader."""

    def __init__(self, config: WikipediaReaderConfig):

        try:
            from llama_index.readers.wikipedia import WikipediaReader
        except ImportError as e:
            logger.error("Failed to import WikipediaReader: missing wikipedia package")
            raise ImportError(
                "llama-index-readers-wikipedia package not found, install with 'pip install llama-index-readers-wikipedia'"
            ) from e
        
        self.config = config
        self.lang = config.lang
        self.metadata_fn = config.metadata_fn
        self._reader = None
        logger.debug(f"Initialized WikipediaReaderProvider with lang={config.lang}")

    def _init_reader(self):
        """Lazily initialize WikipediaReader if not already created."""
        if self._reader is None:
            
            self._reader = WikipediaReader()

    def read(self, input_source: Union[str, List[str]]) -> List[Document]:
        """Read Wikipedia documents with metadata handling and title correction."""
        if not input_source:
            logger.error("No input source provided to WikipediaReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        self._init_reader()

        try:
            import wikipedia
        except ImportError as e:
            logger.error("Failed to import wikipedia package")
            raise ImportError(
                "The 'wikipedia' package is required for WikipediaReaderProvider. Install it with: pip install wikipedia"
            ) from e

        pages = [input_source] if isinstance(input_source, str) else input_source
        logger.info(f"Reading {len(pages)} Wikipedia page(s)")
        validated_pages = []
        
        for page in pages:
            try:
                wikipedia.set_lang(self.lang)
                wikipedia.page(page)
                validated_pages.append(page)
                logger.debug(f"Validated Wikipedia page: {page}")
            except wikipedia.exceptions.PageError:
                try:
                    if search_results := wikipedia.search(page, results=1):
                        wikipedia.page(search_results[0])
                        validated_pages.append(search_results[0])
                        logger.info(f"Corrected page title: '{page}' -> '{search_results[0]}'")
                    else:
                        logger.warning(f"No Wikipedia page found for '{page}'")
                except (wikipedia.exceptions.PageError, wikipedia.exceptions.DisambiguationError) as e:
                    logger.warning(f"Could not resolve Wikipedia page for '{page}': {e}")

        if not validated_pages:
            logger.error(f"No valid Wikipedia pages found for: {pages}")
            raise ValueError(f"No valid Wikipedia pages found for: {pages}")

        try:
            documents = self._reader.load_data(pages=validated_pages)
            logger.info(f"Successfully read {len(documents)} document(s) from Wikipedia")

            if self.metadata_fn:
                for doc in documents:
                    page_context = validated_pages[0] if validated_pages else str(input_source)
                    additional_metadata = self.metadata_fn(page_context)
                    doc.metadata.update(additional_metadata)

            return documents
        except Exception as e:
            logger.error(f"Failed to read Wikipedia pages {validated_pages}: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read Wikipedia pages: {e}") from e
