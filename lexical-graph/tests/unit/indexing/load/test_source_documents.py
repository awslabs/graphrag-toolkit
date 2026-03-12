# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core import Document
from graphrag_toolkit.lexical_graph.indexing.load.source_documents import SourceDocuments


class TestSourceDocumentsInitialization:
    """Tests for SourceDocuments initialization."""
    
    def test_initialization_default(self):
        """Verify SourceDocuments initializes with required parameters."""
        # SourceDocuments requires source_documents_fns parameter
        def sample_fn():
            return [Document(text="Sample document")]
        
        loader = SourceDocuments(source_documents_fns=[sample_fn])
        assert loader is not None
        assert loader.source_documents_fns is not None


class TestSourceDocumentsLoading:
    """Tests for document loading functionality."""
    
    def test_load_documents_from_list(self):
        """Verify loading documents from list."""
        docs = [
            Document(text="Document 1", metadata={"source": "test1"}),
            Document(text="Document 2", metadata={"source": "test2"})
        ]
        
        def docs_fn():
            return docs
        
        loader = SourceDocuments(source_documents_fns=[docs_fn])
        result = list(loader)
        
        assert result is not None
        assert len(result) == 2
        assert result[0].text == "Document 1"
    
    def test_load_empty_document_list(self):
        """Verify handling of empty document list."""
        def empty_fn():
            return []
        
        loader = SourceDocuments(source_documents_fns=[empty_fn])
        result = list(loader)
        
        assert result is not None
        assert len(result) == 0
    
    def test_load_documents_with_metadata(self):
        """Verify metadata is preserved during loading."""
        docs = [
            Document(
                text="Document with metadata",
                metadata={
                    "source": "test_source",
                    "date": "2024-01-01",
                    "author": "Test Author"
                }
            )
        ]
        
        def docs_fn():
            return docs
        
        loader = SourceDocuments(source_documents_fns=[docs_fn])
        result = list(loader)
        
        assert result is not None
        assert len(result) == 1
        assert result[0].metadata["source"] == "test_source"


class TestSourceDocumentsErrorHandling:
    """Tests for error handling in document loading."""
    
    def test_handle_invalid_document_format(self):
        """Verify handling of invalid document format."""
        def invalid_fn():
            raise ValueError("Invalid document format")
        
        loader = SourceDocuments(source_documents_fns=[invalid_fn])
        
        with pytest.raises(ValueError, match="Invalid document format"):
            list(loader)
    
    def test_handle_loading_error(self):
        """Verify handling of loading errors."""
        def error_fn():
            raise Exception("Loading failed")
        
        loader = SourceDocuments(source_documents_fns=[error_fn])
        
        with pytest.raises(Exception, match="Loading failed"):
            list(loader)
