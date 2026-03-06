"""Tests for NeptuneAnalyticsGraphStoreIndex.

This module tests graph store index functionality including
initialization, querying, batch matching, and embedding management.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.byokg_rag.indexing.graph_store_index import (
    NeptuneAnalyticsGraphStoreIndex
)


class TestGraphStoreIndexInitialization:
    """Tests for NeptuneAnalyticsGraphStoreIndex initialization."""
    
    def test_graph_store_index_initialization_defaults(self, mock_graph_store):
        """Verify index initializes with default parameters."""
        mock_embedding = Mock()
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        assert index.graphstore is mock_graph_store
        assert index.embedding is mock_embedding
        assert index.distance_type == "l2"
        assert index.embedding_s3_save_path is None
    
    def test_graph_store_index_initialization_with_s3_path(self, mock_graph_store):
        """Verify index initializes with S3 save path."""
        mock_embedding = Mock()
        s3_path = "s3://bucket/embeddings.csv"
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding,
            embedding_s3_save_path=s3_path
        )
        
        assert index.embedding_s3_save_path == s3_path
    
    def test_graph_store_index_initialization_l2_distance(self, mock_graph_store):
        """Verify index accepts L2 distance type."""
        mock_embedding = Mock()
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding,
            distance_type="l2"
        )
        
        assert index.distance_type == "l2"
    
    def test_graph_store_index_initialization_cosine_fallback(self, mock_graph_store):
        """Verify cosine distance falls back to L2 with warning."""
        mock_embedding = Mock()
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding,
            distance_type="cosine"
        )
        
        # Cosine not supported, should fall back to L2
        assert index.distance_type == "l2"
    
    def test_graph_store_index_initialization_inner_product_fallback(self, mock_graph_store):
        """Verify inner_product distance falls back to L2 with warning."""
        mock_embedding = Mock()
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding,
            distance_type="inner_product"
        )
        
        # Inner product not supported, should fall back to L2
        assert index.distance_type == "l2"


class TestGraphStoreIndexQuery:
    """Tests for querying the graph store index."""
    
    def test_graph_store_index_query_basic(self, mock_graph_store):
        """Verify basic query returns results from graph store."""
        mock_embedding = Mock()
        mock_embedding.embed.return_value = [1.0, 0.0, 0.0, 0.0]
        
        # Mock graph store response
        mock_graph_store.execute_query.return_value = [
            {
                'node': {'~id': 'n1', 'name': 'Amazon'},
                'score': 0.95,
                'embedding': [1.0, 0.0, 0.0, 0.0]
            }
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        result = index.query('Amazon', topk=1)
        
        # Verify embedding was called
        mock_embedding.embed.assert_called_once_with('Amazon')
        
        # Verify graph store was queried
        mock_graph_store.execute_query.assert_called_once()
        
        # Verify result structure
        assert 'hits' in result
        assert len(result['hits']) == 1
        assert result['hits'][0]['document_id'] == 'n1'
        assert result['hits'][0]['document'] == {'~id': 'n1', 'name': 'Amazon'}
        assert result['hits'][0]['match_score'] == 0.95
    
    def test_graph_store_index_query_topk(self, mock_graph_store):
        """Verify topk parameter controls number of results."""
        mock_embedding = Mock()
        mock_embedding.embed.return_value = [1.0, 0.0, 0.0, 0.0]
        
        # Mock graph store response with multiple results
        mock_graph_store.execute_query.return_value = [
            {
                'node': {'~id': 'n1', 'name': 'Amazon'},
                'score': 0.95,
                'embedding': [1.0, 0.0, 0.0, 0.0]
            },
            {
                'node': {'~id': 'n2', 'name': 'AWS'},
                'score': 0.85,
                'embedding': [0.9, 0.1, 0.0, 0.0]
            },
            {
                'node': {'~id': 'n3', 'name': 'Amazon Web Services'},
                'score': 0.80,
                'embedding': [0.8, 0.2, 0.0, 0.0]
            }
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        result = index.query('Amazon', topk=3)
        
        # Verify topk was passed to query
        call_args = mock_graph_store.execute_query.call_args[0][0]
        assert 'topK: 3' in call_args
        
        # Verify all results returned
        assert len(result['hits']) == 3
    
    def test_graph_store_index_query_with_id_selector_ignored(self, mock_graph_store):
        """Verify id_selector parameter is ignored with warning."""
        mock_embedding = Mock()
        mock_embedding.embed.return_value = [1.0, 0.0, 0.0, 0.0]
        
        mock_graph_store.execute_query.return_value = [
            {
                'node': {'~id': 'n1', 'name': 'Amazon'},
                'score': 0.95,
                'embedding': [1.0, 0.0, 0.0, 0.0]
            }
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        # id_selector should be ignored (not supported)
        result = index.query('Amazon', topk=1, id_selector=['n1', 'n2'])
        
        # Should still return results
        assert len(result['hits']) == 1
    
    def test_graph_store_index_query_empty_results(self, mock_graph_store):
        """Verify query handles empty results from graph store."""
        mock_embedding = Mock()
        mock_embedding.embed.return_value = [1.0, 0.0, 0.0, 0.0]
        
        # Mock empty response
        mock_graph_store.execute_query.return_value = []
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        result = index.query('NonexistentEntity', topk=1)
        
        assert 'hits' in result
        assert len(result['hits']) == 0


class TestGraphStoreIndexMatch:
    """Tests for batch matching functionality."""
    
    def test_graph_store_index_match_multiple_queries(self, mock_graph_store):
        """Verify batch matching of multiple queries."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ]
        
        # Mock graph store responses for each query
        mock_graph_store.execute_query.side_effect = [
            [
                {
                    'node': {'~id': 'n1', 'name': 'Amazon'},
                    'score': 0.95,
                    'embedding': [1.0, 0.0, 0.0, 0.0]
                }
            ],
            [
                {
                    'node': {'~id': 'n2', 'name': 'Microsoft'},
                    'score': 0.90,
                    'embedding': [0.0, 1.0, 0.0, 0.0]
                }
            ]
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        result = index.match(['Amazon', 'Microsoft'], topk=1)
        
        # Verify batch_embed was called
        mock_embedding.batch_embed.assert_called_once_with(['Amazon', 'Microsoft'])
        
        # Verify graph store was queried twice (once per input)
        assert mock_graph_store.execute_query.call_count == 2
        
        # Verify results from both queries
        assert 'hits' in result
        assert len(result['hits']) == 2
        
        # Check both results are present
        doc_ids = [hit['document_id'] for hit in result['hits']]
        assert 'n1' in doc_ids
        assert 'n2' in doc_ids
    
    def test_graph_store_index_match_with_topk(self, mock_graph_store):
        """Verify match respects topk parameter."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0]
        ]
        
        # Mock multiple results per query
        mock_graph_store.execute_query.return_value = [
            {
                'node': {'~id': 'n1', 'name': 'Amazon'},
                'score': 0.95,
                'embedding': [1.0, 0.0, 0.0, 0.0]
            },
            {
                'node': {'~id': 'n2', 'name': 'AWS'},
                'score': 0.85,
                'embedding': [0.9, 0.1, 0.0, 0.0]
            }
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        result = index.match(['Amazon'], topk=2)
        
        # Verify topk was passed to query
        call_args = mock_graph_store.execute_query.call_args[0][0]
        assert 'topK: 2' in call_args
        
        # Verify results
        assert len(result['hits']) == 2
    
    def test_graph_store_index_match_with_id_selector_ignored(self, mock_graph_store):
        """Verify id_selector parameter is ignored with warning."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0]
        ]
        
        mock_graph_store.execute_query.return_value = [
            {
                'node': {'~id': 'n1', 'name': 'Amazon'},
                'score': 0.95,
                'embedding': [1.0, 0.0, 0.0, 0.0]
            }
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        # id_selector should be ignored (not supported)
        result = index.match(['Amazon'], topk=1, id_selector=['n1'])
        
        # Should still return results
        assert len(result['hits']) == 1


class TestGraphStoreIndexAdd:
    """Tests for adding documents to the index."""
    
    def test_graph_store_index_add_not_implemented(self, mock_graph_store):
        """Verify add() raises NotImplementedError."""
        mock_embedding = Mock()
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        with pytest.raises(NotImplementedError, match="index.add is ambiguous"):
            index.add(['Amazon', 'Microsoft'])


class TestGraphStoreIndexAddWithIds:
    """Tests for adding documents with IDs to the index."""
    
    def test_graph_store_index_add_with_ids_direct_upsert(self, mock_graph_store):
        """Verify add_with_ids upserts embeddings directly when no S3 path."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ]
        
        mock_graph_store.execute_query.return_value = [
            {'node': {'~id': 'n1'}, 'embedding': [1.0, 0.0, 0.0, 0.0], 'success': True}
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        ids = ['n1', 'n2']
        documents = ['Amazon', 'Microsoft']
        
        index.add_with_ids(ids, documents)
        
        # Verify batch_embed was called
        mock_embedding.batch_embed.assert_called_once_with(documents)
        
        # Verify execute_query was called twice (once per document)
        assert mock_graph_store.execute_query.call_count == 2
        
        # Verify upsert queries contain node IDs
        call_args_list = [call[0][0] for call in mock_graph_store.execute_query.call_args_list]
        assert any('n1' in call for call in call_args_list)
        assert any('n2' in call for call in call_args_list)
    
    def test_graph_store_index_add_with_ids_with_embeddings(self, mock_graph_store):
        """Verify add_with_ids accepts pre-computed embeddings."""
        mock_embedding = Mock()
        
        mock_graph_store.execute_query.return_value = [
            {'node': {'~id': 'n1'}, 'embedding': [1.0, 0.0, 0.0, 0.0], 'success': True}
        ]
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        ids = ['n1', 'n2']
        documents = ['Amazon', 'Microsoft']
        embeddings = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ]
        
        index.add_with_ids(ids, documents, embeddings=embeddings)
        
        # Verify batch_embed was NOT called (embeddings provided)
        mock_embedding.batch_embed.assert_not_called()
        
        # Verify execute_query was called twice
        assert mock_graph_store.execute_query.call_count == 2
    
    def test_graph_store_index_add_with_ids_s3_path(self, mock_graph_store):
        """Verify add_with_ids uses S3 CSV upload when path provided."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ]
        
        mock_graph_store._upload_to_s3 = Mock()
        mock_graph_store.read_from_csv = Mock()
        
        s3_path = "s3://bucket/embeddings.csv"
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding,
            embedding_s3_save_path=s3_path
        )
        
        ids = ['n1', 'n2']
        documents = ['Amazon', 'Microsoft']
        
        index.add_with_ids(ids, documents)
        
        # Verify S3 upload was called
        mock_graph_store._upload_to_s3.assert_called_once()
        upload_call_args = mock_graph_store._upload_to_s3.call_args
        assert upload_call_args[0][0] == s3_path
        
        # Verify CSV content includes header and embeddings
        csv_content = upload_call_args[1]['file_contents']
        assert '~id,embedding:vector' in csv_content
        assert 'n1' in csv_content
        assert 'n2' in csv_content
        
        # Verify read_from_csv was called
        mock_graph_store.read_from_csv.assert_called_once_with(s3_path=s3_path)
        
        # Verify execute_query was NOT called (using CSV instead)
        mock_graph_store.execute_query.assert_not_called()
    
    def test_graph_store_index_add_with_ids_override_s3_path(self, mock_graph_store):
        """Verify add_with_ids can override S3 path per call."""
        mock_embedding = Mock()
        mock_embedding.batch_embed.return_value = [
            [1.0, 0.0, 0.0, 0.0]
        ]
        
        mock_graph_store._upload_to_s3 = Mock()
        mock_graph_store.read_from_csv = Mock()
        
        # Index has default S3 path
        default_s3_path = "s3://bucket/default.csv"
        override_s3_path = "s3://bucket/override.csv"
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding,
            embedding_s3_save_path=default_s3_path
        )
        
        ids = ['n1']
        documents = ['Amazon']
        
        # Override S3 path in call
        index.add_with_ids(ids, documents, embedding_s3_save_path=override_s3_path)
        
        # Verify override path was used
        upload_call_args = mock_graph_store._upload_to_s3.call_args
        assert upload_call_args[0][0] == override_s3_path


class TestGraphStoreIndexReset:
    """Tests for reset functionality."""
    
    def test_graph_store_index_reset_not_supported(self, mock_graph_store):
        """Verify reset() logs warning and does nothing."""
        mock_embedding = Mock()
        
        index = NeptuneAnalyticsGraphStoreIndex(
            graphstore=mock_graph_store,
            embedding=mock_embedding
        )
        
        # Reset should not raise error, just log warning
        index.reset()
        
        # No exception should be raised
        # (Implementation logs warning but doesn't raise)
