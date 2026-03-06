"""Tests for DenseIndex and LocalFaissDenseIndex.

This module tests dense vector indexing functionality including
index creation, embedding addition, similarity search, and LLM integration.
"""

import pytest
import numpy as np
from unittest.mock import Mock
from graphrag_toolkit.byokg_rag.indexing.dense_index import (
    DenseIndex,
    LocalFaissDenseIndex
)


class TestDenseIndexCreation:
    """Tests for DenseIndex and LocalFaissDenseIndex initialization."""
    
    def test_local_faiss_dense_index_creation_l2(self):
        """Verify LocalFaissDenseIndex initializes with L2 distance."""
        mock_embedding = Mock()
        embedding_dim = 128
        
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        assert index.embedding is mock_embedding
        assert index.distance_type == "l2"
        assert index.doc_store == []
        assert index.doc_ids == []
        assert index.id2idx == {}
    
    def test_local_faiss_dense_index_creation_cosine(self):
        """Verify LocalFaissDenseIndex initializes with cosine distance."""
        mock_embedding = Mock()
        embedding_dim = 128
        
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="cosine",
            embedding_dim=embedding_dim
        )
        
        assert index.distance_type == "cosine"
    
    def test_local_faiss_dense_index_creation_inner_product(self):
        """Verify LocalFaissDenseIndex initializes with inner product distance."""
        mock_embedding = Mock()
        embedding_dim = 128
        
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="inner_product",
            embedding_dim=embedding_dim
        )
        
        assert index.distance_type == "inner_product"
    
    def test_local_faiss_dense_index_requires_positive_dim(self):
        """Verify LocalFaissDenseIndex requires positive embedding dimension."""
        mock_embedding = Mock()
        
        with pytest.raises(AssertionError, match="Embedding dimension size must be passed"):
            LocalFaissDenseIndex(
                embedding=mock_embedding,
                distance_type="l2",
                embedding_dim=-1
            )


class TestDenseIndexAddEmbeddings:
    """Tests for adding documents and embeddings to the index."""
    
    def test_dense_index_add_embeddings(self):
        """Verify adding documents with pre-computed embeddings."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        documents = ['Amazon', 'Microsoft', 'Google']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ])
        
        index.add(documents, embeddings=embeddings)
        
        assert len(index.doc_store) == 3
        assert index.doc_store == documents
        assert len(index.doc_ids) == 3
        assert index.doc_ids == ['doc0', 'doc1', 'doc2']
        assert index.faiss_index.ntotal == 3
    
    def test_dense_index_add_without_embeddings(self):
        """Verify adding documents generates embeddings via embedding object."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ]
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        documents = ['Amazon', 'Microsoft', 'Google']
        
        index.add(documents)
        
        mock_embedding.batch_embed.assert_called_once_with(documents)
        assert len(index.doc_store) == 3
        assert index.doc_ids == ['doc0', 'doc1', 'doc2']
    
    def test_dense_index_add_with_ids(self):
        """Verify adding documents with custom IDs."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        ids = ['company1', 'company2', 'company3']
        documents = ['Amazon', 'Microsoft', 'Google']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ])
        
        index.add_with_ids(ids, documents, embeddings=embeddings)
        
        assert index.doc_ids == ids
        assert index.id2idx == {'company1': 0, 'company2': 1, 'company3': 2}
    
    def test_dense_index_add_multiple_batches(self):
        """Verify adding documents in multiple batches."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # First batch
        documents1 = ['Amazon', 'Microsoft']
        embeddings1 = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ])
        index.add(documents1, embeddings=embeddings1)
        
        # Second batch
        documents2 = ['Google', 'Apple']
        embeddings2 = np.array([
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0]
        ])
        index.add(documents2, embeddings=embeddings2)
        
        assert len(index.doc_store) == 4
        assert index.doc_ids == ['doc0', 'doc1', 'doc2', 'doc3']
        assert index.faiss_index.ntotal == 4


class TestDenseIndexQuerySimilarity:
    """Tests for similarity search functionality."""
    
    def test_dense_index_query_similarity(self):
        """Verify similarity search returns closest matches."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # Add documents
        documents = ['Amazon', 'Microsoft', 'Google']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ])
        index.add(documents, embeddings=embeddings)
        
        # Query with embedding close to 'Amazon'
        query_embedding = [0.9, 0.1, 0.0, 0.0]
        mock_embedding.embed.return_value = query_embedding
        
        result = index.query('amazon query', topk=1)
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Amazon'
        assert result['hits'][0]['document_id'] == 'doc0'
        assert 'match_score' in result['hits'][0]
    
    def test_dense_index_query_topk(self):
        """Verify topk parameter limits results."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # Add documents
        documents = ['Amazon', 'Microsoft', 'Google', 'Apple', 'Meta']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
            [0.5, 0.5, 0.0, 0.0]
        ])
        index.add(documents, embeddings=embeddings)
        
        # Query
        query_embedding = [1.0, 0.0, 0.0, 0.0]
        mock_embedding.embed.return_value = query_embedding
        
        result = index.query('query', topk=3)
        
        assert len(result['hits']) == 3
    
    def test_dense_index_query_with_id_selector(self):
        """Verify id_selector filters results to specific IDs."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # Add documents with custom IDs
        ids = ['company1', 'company2', 'company3']
        documents = ['Amazon', 'Microsoft', 'Google']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ])
        index.add_with_ids(ids, documents, embeddings=embeddings)
        
        # Query with id_selector
        query_embedding = [1.0, 0.0, 0.0, 0.0]
        mock_embedding.embed.return_value = query_embedding
        
        result = index.query('query', topk=2, id_selector=['company1', 'company3'])
        
        # Should only return results from allowed IDs
        returned_ids = [hit['document_id'] for hit in result['hits']]
        assert all(doc_id in ['company1', 'company3'] for doc_id in returned_ids)
    
    def test_dense_index_query_empty_index(self):
        """Verify querying empty index behavior.
        
        NOTE: When the FAISS index is empty, it returns -1 for indices.
        The current implementation will raise IndexError when trying to
        access doc_ids[-1]. This test documents the current behavior.
        """
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        query_embedding = [1.0, 0.0, 0.0, 0.0]
        mock_embedding.embed.return_value = query_embedding
        
        # Current implementation raises IndexError for empty index
        # This is a known limitation that should be handled in the implementation
        with pytest.raises(IndexError):
            result = index.query('query', topk=1)


class TestDenseIndexQueryWithMockLLM:
    """Tests for dense index with mocked LLM embedding generation."""
    
    def test_dense_index_query_with_mock_llm(self, mock_bedrock_generator):
        """Verify dense index works with mocked LLM for embeddings."""
        # Create mock embedding that uses the mock LLM
        mock_embedding = Mock()
        mock_embedding.embed.return_value = [1.0, 0.0, 0.0, 0.0]
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ]
        
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # Add documents (will use batch_embed)
        documents = ['Amazon', 'Microsoft', 'Google']
        index.add(documents)
        
        # Query (will use embed)
        result = index.query('Amazon', topk=1)
        
        # Verify mocked embedding methods were called
        mock_embedding.batch_embed.assert_called_once_with(documents)
        mock_embedding.embed.assert_called_once_with('Amazon')
        
        # Verify results
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Amazon'


class TestDenseIndexMatch:
    """Tests for batch matching functionality."""
    
    def test_dense_index_match_multiple_queries(self):
        """Verify batch matching of multiple queries."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # Add documents
        documents = ['Amazon', 'Microsoft', 'Google']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0]
        ])
        index.add(documents, embeddings=embeddings)
        
        # Batch query
        query_embeddings = [
            [0.9, 0.1, 0.0, 0.0],  # Close to Amazon
            [0.0, 0.9, 0.1, 0.0]   # Close to Microsoft
        ]
        mock_embedding.batch_embed.return_value = query_embeddings
        
        result = index.match(['query1', 'query2'], topk=1)
        
        # Should return 2 results (1 per query)
        assert len(result['hits']) == 2
    
    def test_dense_index_match_with_id_selector_not_implemented(self):
        """Verify match with id_selector raises NotImplementedError."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        with pytest.raises(NotImplementedError):
            index.match(['query'], topk=1, id_selector=['id1'])


class TestDenseIndexReset:
    """Tests for reset functionality."""
    
    def test_dense_index_reset(self):
        """Verify reset clears all stored data."""
        mock_embedding = Mock()
        embedding_dim = 4
        index = LocalFaissDenseIndex(
            embedding=mock_embedding,
            distance_type="l2",
            embedding_dim=embedding_dim
        )
        
        # Add documents
        documents = ['Amazon', 'Microsoft']
        embeddings = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ])
        index.add(documents, embeddings=embeddings)
        
        # Reset
        index.reset()
        
        assert index.doc_store == []
        assert index.doc_ids == []
        assert index.id2idx == {}
