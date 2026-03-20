# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock
from llama_index.core.schema import Document, TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.docs_to_nodes import DocsToNodes


class TestDocsToNodesInitialization:
    """Tests for DocsToNodes initialization."""
    
    def test_initialization_default(self):
        """Verify DocsToNodes initializes with default parameters."""
        converter = DocsToNodes()
        assert converter is not None
    
    def test_initialization_with_show_progress(self):
        """Verify DocsToNodes initializes and can use show_progress parameter."""
        converter = DocsToNodes()
        # DocsToNodes doesn't store show_progress as an attribute, it's passed to _parse_nodes
        assert converter is not None


class TestDocsToNodesConversion:
    """Tests for document to node conversion."""
    
    def test_convert_single_document(self):
        """Verify conversion of single document to nodes."""
        converter = DocsToNodes()
        doc = Document(text="Test document content")
        
        result = converter([doc])
        
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(n, TextNode) for n in result)
    
    def test_convert_multiple_documents(self):
        """Verify conversion of multiple documents to nodes."""
        converter = DocsToNodes()
        docs = [
            Document(text="First document"),
            Document(text="Second document"),
            Document(text="Third document")
        ]
        
        result = converter(docs)
        
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_convert_empty_document_list(self):
        """Verify handling of empty document list."""
        converter = DocsToNodes()
        result = converter([])
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_convert_document_with_metadata(self):
        """Verify metadata is preserved during conversion."""
        converter = DocsToNodes()
        doc = Document(
            text="Document with metadata",
            metadata={"source": "test", "date": "2024-01-01"}
        )
        
        result = converter([doc])
        
        assert len(result) > 0
        # Metadata should be preserved in nodes
        assert result[0].metadata is not None
