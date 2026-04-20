# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from typing import List, Dict, Any
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DocumentGraphReaderConfig
from graphrag_toolkit.lexical_graph.logging import logging
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class DocumentGraphReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for document-graph data integration."""

    def __init__(self, config: DocumentGraphReaderConfig):
        """Initialize with DocumentGraphReaderConfig."""
        super().__init__(config=config, reader_cls=None)
        self.metadata_fn = config.metadata_fn or self._default_metadata_fn
        logger.debug("Initialized DocumentGraphReaderProvider")

    def read(self, input_source: List[Dict[str, Any]]) -> List[Document]:
        """Convert document-graph data into LlamaIndex Documents."""
        if not input_source:
            logger.error("No input source provided to DocumentGraphReaderProvider")
            raise ValueError("input_source cannot be None or empty")
        
        if not isinstance(input_source, list):
            logger.error(f"Invalid input type: {type(input_source)}")
            raise ValueError("DocumentGraphReader expects a list of document dictionaries")
        
        logger.info(f"Processing {len(input_source)} document(s) from graph data")
        documents = []
        
        try:
            for idx, doc_data in enumerate(input_source):
                try:
                    text_content = self._extract_text_content(doc_data)
                    metadata = self._generate_metadata(doc_data)
                    doc = Document(text=text_content, metadata=metadata)
                    documents.append(doc)
                except Exception as e:
                    logger.warning(f"Failed to process document at index {idx}: {e}")
            
            logger.info(f"Successfully processed {len(documents)} document(s)")
            return documents
        except Exception as e:
            logger.error(f"Failed to read document graph data: {e}", exc_info=True)
            raise RuntimeError(f"Failed to read document graph data: {e}") from e

    def _extract_text_content(self, doc_data: Dict[str, Any]) -> str:
        """Extract text content from document data."""
        content_fields = ['text', 'content', 'title', 'name']
        text_parts = []
        
        if 'title' in doc_data and doc_data['title']:
            text_parts.append(f"Title: {doc_data['title']}")
        
        for field in content_fields:
            if field in doc_data and doc_data[field] and field != 'title':
                text_parts.append(str(doc_data[field]))
                break
        
        return '\n'.join(text_parts) if text_parts else str(doc_data)

    def _default_metadata_fn(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Default metadata function."""
        return {
            'data_source': 'document_graph',
            'document_id': str(doc_data.get('document_id', doc_data.get('id', 'unknown'))),
            'node_id': str(doc_data.get('node_id', doc_data.get('id', 'unknown'))),
            'source_type': str(doc_data.get('source_type', 'unknown'))
        }

    def _generate_metadata(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate metadata using configured metadata function."""
        metadata = self.metadata_fn(doc_data)
        return {k: str(v) if v is not None else "" for k, v in metadata.items()}