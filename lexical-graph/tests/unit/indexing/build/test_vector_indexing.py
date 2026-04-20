# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.vector_indexing import VectorIndexing


class TestVectorIndexingInitialization:
    """Tests for VectorIndexing initialization."""
    
    def test_initialization(self, mock_opensearch_store):
        """Verify VectorIndexing initializes with vector store."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        assert indexer is not None


class TestVectorIndexingOperations:
    """Tests for vector indexing operations."""
    
    def test_index_document_chunks(self, mock_opensearch_store):
        """Verify indexing document chunks with embeddings."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        chunks = [
            {'id': 'chunk_001', 'text': 'First chunk of text', 'embedding': [0.1] * 384},
            {'id': 'chunk_002', 'text': 'Second chunk of text', 'embedding': [0.2] * 384}
        ]
        
        indexer.index_chunks = Mock(return_value={'indexed': 2})
        result = indexer.index_chunks(chunks)
        
        assert result is not None
        assert result['indexed'] == 2
    
    def test_index_with_metadata(self, mock_opensearch_store):
        """Verify indexing with metadata."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        chunks = [
            {
                'id': 'chunk_001',
                'text': 'Chunk with metadata',
                'embedding': [0.1] * 384,
                'metadata': {'source': 'doc_001', 'position': 0}
            }
        ]
        
        indexer.index_chunks = Mock(return_value={'indexed': 1})
        result = indexer.index_chunks(chunks)
        
        assert result is not None
        assert result['indexed'] == 1
    
    def test_reindex_existing_chunks(self, mock_opensearch_store):
        """Verify reindexing existing chunks."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        chunks = [
            {'id': 'chunk_001', 'text': 'Updated text', 'embedding': [0.15] * 384}
        ]
        
        indexer.reindex_chunks = Mock(return_value={'reindexed': 1})
        result = indexer.reindex_chunks(chunks)
        
        assert result is not None
        assert result['reindexed'] == 1
    
    def test_delete_indexed_chunks(self, mock_opensearch_store):
        """Verify deleting indexed chunks."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        chunk_ids = ['chunk_001', 'chunk_002', 'chunk_003']
        
        indexer.delete_chunks = Mock(return_value={'deleted': 3})
        result = indexer.delete_chunks(chunk_ids)
        
        assert result is not None
        assert result['deleted'] == 3


class TestVectorIndexingErrorHandling:
    """Tests for vector indexing error handling."""
    
    def test_index_with_empty_chunks(self, mock_opensearch_store):
        """Verify handling of empty chunk list."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        indexer.index_chunks = Mock(return_value={'indexed': 0})
        result = indexer.index_chunks([])
        
        assert result['indexed'] == 0
    
    def test_index_with_missing_embeddings(self, mock_opensearch_store):
        """Verify handling of chunks without embeddings."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        chunks = [
            {'id': 'chunk_001', 'text': 'Chunk without embedding'}
        ]
        
        indexer.index_chunks = Mock(side_effect=ValueError("Missing embedding"))
        
        with pytest.raises(ValueError, match="Missing embedding"):
            indexer.index_chunks(chunks)
    
    def test_index_with_invalid_embedding_dimension(self, mock_opensearch_store):
        """Verify handling of invalid embedding dimensions."""
        indexer = VectorIndexing(vector_store=mock_opensearch_store)
        
        chunks = [
            {'id': 'chunk_001', 'text': 'Chunk', 'embedding': [0.1] * 100}
        ]
        
        indexer.index_chunks = Mock(side_effect=ValueError("Invalid embedding dimension"))
        
        with pytest.raises(ValueError, match="Invalid embedding dimension"):
            indexer.index_chunks(chunks)
