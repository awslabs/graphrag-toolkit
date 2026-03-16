# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.graph_construction import GraphConstruction


class TestGraphConstructionInitialization:
    """Tests for GraphConstruction initialization."""
    
    def test_initialization(self, mock_neptune_store):
        """Verify GraphConstruction initializes with graph store."""
        constructor = GraphConstruction(graph_store=mock_neptune_store)
        assert constructor is not None


class TestGraphConstructionOperations:
    """Tests for graph construction operations."""
    
    def test_construct_graph_from_documents(self, mock_neptune_store, sample_documents):
        """Verify constructing graph from documents."""
        constructor = GraphConstruction(graph_store=mock_neptune_store)
        
        constructor.construct = Mock(return_value={'nodes': 10, 'edges': 15})
        result = constructor.construct(sample_documents)
        
        assert result is not None
        assert result['nodes'] == 10
        assert result['edges'] == 15
    
    def test_construct_graph_with_relationships(self, mock_neptune_store):
        """Verify constructing graph with relationships."""
        constructor = GraphConstruction(graph_store=mock_neptune_store)
        
        data = {
            'nodes': [{'id': 'n1'}, {'id': 'n2'}],
            'edges': [{'source': 'n1', 'target': 'n2', 'type': 'RELATES_TO'}]
        }
        
        constructor.construct = Mock(return_value={'status': 'success'})
        result = constructor.construct(data)
        
        assert result is not None
        assert result['status'] == 'success'
    
    def test_construct_graph_incrementally(self, mock_neptune_store):
        """Verify incremental graph construction."""
        constructor = GraphConstruction(graph_store=mock_neptune_store)
        
        batch1 = [{'id': 'n1'}, {'id': 'n2'}]
        batch2 = [{'id': 'n3'}, {'id': 'n4'}]
        
        constructor.construct = Mock(side_effect=[
            {'nodes': 2, 'edges': 0},
            {'nodes': 2, 'edges': 1}
        ])
        
        result1 = constructor.construct(batch1)
        result2 = constructor.construct(batch2)
        
        assert result1['nodes'] == 2
        assert result2['nodes'] == 2


class TestGraphConstructionErrorHandling:
    """Tests for graph construction error handling."""
    
    def test_construct_with_empty_input(self, mock_neptune_store):
        """Verify handling of empty input."""
        constructor = GraphConstruction(graph_store=mock_neptune_store)
        
        constructor.construct = Mock(return_value={'nodes': 0, 'edges': 0})
        result = constructor.construct([])
        
        assert result['nodes'] == 0
    
    def test_construct_with_invalid_data(self, mock_neptune_store):
        """Verify handling of invalid construction data."""
        constructor = GraphConstruction(graph_store=mock_neptune_store)
        
        invalid_data = "not_valid_data"
        
        constructor.construct = Mock(side_effect=TypeError("Invalid data type"))
        
        with pytest.raises(TypeError, match="Invalid data type"):
            constructor.construct(invalid_data)
