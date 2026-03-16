# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.vector_batch_client import VectorBatchClient


class TestVectorBatchClientInitialization:
    """Tests for VectorBatchClient initialization."""
    
    def test_initialization(self, mock_opensearch_store):
        """Verify VectorBatchClient initializes with vector store."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        assert client is not None


class TestVectorBatchOperations:
    """Tests for vector batch operations."""
    
    def test_batch_add_embeddings(self, mock_opensearch_store):
        """Verify batch addition of embeddings."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        
        embeddings = [
            {'id': 'chunk_001', 'vector': [0.1] * 384, 'text': 'Chunk 1'},
            {'id': 'chunk_002', 'vector': [0.2] * 384, 'text': 'Chunk 2'},
            {'id': 'chunk_003', 'vector': [0.3] * 384, 'text': 'Chunk 3'}
        ]
        
        client.batch_add_embeddings = Mock(return_value={'added': 3})
        result = client.batch_add_embeddings(embeddings)
        
        assert result is not None
        assert result['added'] == 3
    
    def test_batch_update_embeddings(self, mock_opensearch_store):
        """Verify batch update of embeddings."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        
        updates = [
            {'id': 'chunk_001', 'vector': [0.15] * 384},
            {'id': 'chunk_002', 'vector': [0.25] * 384}
        ]
        
        client.batch_update_embeddings = Mock(return_value={'updated': 2})
        result = client.batch_update_embeddings(updates)
        
        assert result is not None
        assert result['updated'] == 2
    
    def test_batch_delete_embeddings(self, mock_opensearch_store):
        """Verify batch deletion of embeddings."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        
        ids_to_delete = ['chunk_001', 'chunk_002', 'chunk_003']
        
        client.batch_delete_embeddings = Mock(return_value={'deleted': 3})
        result = client.batch_delete_embeddings(ids_to_delete)
        
        assert result is not None
        assert result['deleted'] == 3


class TestVectorBatchClientErrorHandling:
    """Tests for vector batch client error handling."""
    
    def test_batch_add_with_empty_list(self, mock_opensearch_store):
        """Verify handling of empty batch."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        
        client.batch_add_embeddings = Mock(return_value={'added': 0})
        result = client.batch_add_embeddings([])
        
        assert result['added'] == 0
    
    def test_batch_add_with_invalid_vector_dimension(self, mock_opensearch_store):
        """Verify handling of invalid vector dimensions."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        
        invalid_embeddings = [
            {'id': 'chunk_001', 'vector': [0.1] * 100, 'text': 'Wrong dimension'}
        ]
        
        client.batch_add_embeddings = Mock(side_effect=ValueError("Invalid vector dimension"))
        
        with pytest.raises(ValueError, match="Invalid vector dimension"):
            client.batch_add_embeddings(invalid_embeddings)
    
    def test_batch_add_with_missing_vector(self, mock_opensearch_store):
        """Verify handling of missing vector data."""
        client = VectorBatchClient(vector_store=mock_opensearch_store)
        
        invalid_embeddings = [
            {'id': 'chunk_001', 'text': 'No vector'}
        ]
        
        client.batch_add_embeddings = Mock(side_effect=KeyError("Missing vector"))
        
        with pytest.raises(KeyError, match="Missing vector"):
            client.batch_add_embeddings(invalid_embeddings)
