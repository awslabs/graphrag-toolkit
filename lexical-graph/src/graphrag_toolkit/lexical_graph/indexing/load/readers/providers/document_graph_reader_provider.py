from typing import List, Dict, Any
from graphrag_toolkit.lexical_graph.indexing.load.readers.llama_index_reader_provider_base import LlamaIndexReaderProviderBase
from graphrag_toolkit.lexical_graph.indexing.load.readers.reader_provider_config import DocumentGraphReaderConfig
from llama_index.core.schema import Document

class DocumentGraphReaderProvider(LlamaIndexReaderProviderBase):
    """Reader provider for document-graph data integration."""

    def __init__(self, config: DocumentGraphReaderConfig):
        """Initialize with DocumentGraphReaderConfig."""
        # No external dependencies needed for document graph processing
        super().__init__(config=config, reader_cls=None)
        self.metadata_fn = config.metadata_fn or self._default_metadata_fn

    def read(self, input_source: List[Dict[str, Any]]) -> List[Document]:
        """Convert document-graph data into LlamaIndex Documents."""
        if not isinstance(input_source, list):
            raise ValueError("DocumentGraphReader expects a list of document dictionaries")
        
        documents = []
        
        for doc_data in input_source:
            # Extract text content
            text_content = self._extract_text_content(doc_data)
            
            # Generate metadata
            metadata = self._generate_metadata(doc_data)
            
            # Create Document
            doc = Document(text=text_content, metadata=metadata)
            documents.append(doc)
        
        return documents

    def _extract_text_content(self, doc_data: Dict[str, Any]) -> str:
        """Extract text content from document data."""
        content_fields = ['text', 'content', 'title', 'name']
        text_parts = []
        
        # Add title if available
        if 'title' in doc_data and doc_data['title']:
            text_parts.append(f"Title: {doc_data['title']}")
        
        # Add main content
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
        # Use configured metadata function
        metadata = self.metadata_fn(doc_data)
        
        # Ensure all values are strings for compatibility
        return {k: str(v) if v is not None else "" for k, v in metadata.items()}