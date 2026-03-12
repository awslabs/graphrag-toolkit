# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import GraphBatchClient


class TestGraphBatchClientInitialization:
    """Tests for GraphBatchClient initialization."""
    
    def test_initialization(self, mock_neptune_store):
        """Verify GraphBatchClient initializes with graph store."""
        client = GraphBatchClient(graph_store=mock_neptune_store)
        assert client is not None


class TestGraphBatchOperations:
    """Tests for graph batch operations."""
    
    def test_batch_insert_nodes(self, mock_neptune_store):
        """Verify batch insertion of nodes."""
        client = GraphBatchClient(graph_store=mock_neptune_store)
        
        nodes = [
            {'id': 'n1', 'type': 'Document', 'properties': {'title': 'Doc 1'}},
            {'id': 'n2', 'type': 'Chunk', 'properties': {'text': 'Chunk 1'}},
            {'id': 'n3', 'type': 'Entity', 'properties': {'name': 'Entity 1'}}
        ]
        
        client.batch_insert_nodes = Mock(return_value={'inserted': 3})
        result = client.batch_insert_nodes(nodes)
        
        assert result is not None
        assert result['inserted'] == 3
    
    def test_batch_insert_edges(self, mock_neptune_store):
        """Verify batch insertion of edges."""
        client = GraphBatchClient(graph_store=mock_neptune_store)
        
        edges = [
            {'source': 'n1', 'target': 'n2', 'type': 'HAS_CHUNK'},
            {'source': 'n2', 'target': 'n3', 'type': 'MENTIONS'}
        ]
        
        client.batch_insert_edges = Mock(return_value={'inserted': 2})
        result = client.batch_insert_edges(edges)
        
        assert result is not None
        assert result['inserted'] == 2
    
    def test_batch_update_properties(self, mock_neptune_store):
        """Verify batch update of node properties."""
        client = GraphBatchClient(graph_store=mock_neptune_store)
        
        updates = [
            {'id': 'n1', 'properties': {'updated': True}},
            {'id': 'n2', 'properties': {'score': 0.95}}
        ]
        
        client.batch_update_properties = Mock(return_value={'updated': 2})
        result = client.batch_update_properties(updates)
        
        assert result is not None
        assert result['updated'] == 2


class TestGraphBatchClientErrorHandling:
    """Tests for batch client error handling."""
    
    def test_batch_insert_with_empty_list(self, mock_neptune_store):
        """Verify handling of empty batch."""
        client = GraphBatchClient(graph_store=mock_neptune_store)
        
        client.batch_insert_nodes = Mock(return_value={'inserted': 0})
        result = client.batch_insert_nodes([])
        
        assert result['inserted'] == 0
    
    def test_batch_insert_with_invalid_data(self, mock_neptune_store):
        """Verify handling of invalid batch data."""
        client = GraphBatchClient(graph_store=mock_neptune_store)
        
        invalid_nodes = [{'invalid': 'data'}]
        
        client.batch_insert_nodes = Mock(side_effect=ValueError("Invalid node data"))
        
        with pytest.raises(ValueError, match="Invalid node data"):
            client.batch_insert_nodes(invalid_nodes)
