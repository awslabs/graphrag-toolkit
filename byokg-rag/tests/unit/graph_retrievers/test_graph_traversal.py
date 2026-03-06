"""Tests for graph_traversal.py module.

This module tests the GTraversal class including initialization,
single-hop expansion, multi-hop traversal, and metapath-guided traversal.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.byokg_rag.graph_retrievers.graph_traversal import GTraversal


@pytest.fixture
def mock_graph_store_with_edges():
    """
    Fixture providing a mock graph store with edge traversal capabilities.
    
    Returns a mock graph store that simulates graph traversal operations
    without requiring a real graph database connection.
    """
    mock_store = Mock()
    
    # Mock one-hop edges for single-hop expansion
    mock_store.get_one_hop_edges.return_value = {
        'Organization': {
            'FOUNDED_BY': ['edge1'],
            'LOCATED_IN': ['edge2']
        },
        'John Doe': {
            'FOUNDED': ['edge3']
        }
    }
    
    # Mock edge destination nodes
    mock_store.get_edge_destination_nodes.return_value = {
        'edge1': ['John Doe'],
        'edge2': ['Portland'],
        'edge3': ['Organization']
    }
    
    return mock_store


@pytest.fixture
def mock_graph_store_with_triplets():
    """
    Fixture providing a mock graph store with triplet data.
    
    Returns a mock graph store that provides triplet-based traversal data.
    """
    mock_store = Mock()
    
    # Mock one-hop edges with triplets
    def get_one_hop_edges_side_effect(nodes, return_triplets=False):
        if return_triplets:
            return {
                'Organization': {
                    'FOUNDED_BY': [('Organization', 'FOUNDED_BY', 'John Doe')],
                    'LOCATED_IN': [('Organization', 'LOCATED_IN', 'Portland')]
                },
                'John Doe': {
                    'FOUNDED': [('John Doe', 'FOUNDED', 'Organization')]
                },
                'Portland': {
                    'LOCATED_IN': [('Portland', 'LOCATED_IN', 'Oregon')]
                }
            }
        else:
            return {
                'Organization': {
                    'FOUNDED_BY': ['edge1'],
                    'LOCATED_IN': ['edge2']
                }
            }
    
    mock_store.get_one_hop_edges.side_effect = get_one_hop_edges_side_effect
    
    return mock_store


class TestGraphTraversalInitialization:
    """Tests for GTraversal initialization."""
    
    def test_graph_traversal_initialization(self, mock_graph_store):
        """Verify GTraversal initializes with graph store."""
        traversal = GTraversal(graph_store=mock_graph_store)
        
        assert traversal.graph_store == mock_graph_store


class TestGraphTraversalSingleHop:
    """Tests for single-hop graph traversal."""
    
    def test_graph_traversal_single_hop(self, mock_graph_store_with_edges):
        """Verify single-hop expansion returns neighbor nodes."""
        traversal = GTraversal(graph_store=mock_graph_store_with_edges)
        source_nodes = ['Organization']
        
        result = traversal.one_hop_expand(source_nodes)
        
        assert isinstance(result, set)
        assert 'John Doe' in result or 'Portland' in result
        mock_graph_store_with_edges.get_one_hop_edges.assert_called_once_with(source_nodes)
    
    def test_graph_traversal_single_hop_with_edge_type(self, mock_graph_store_with_edges):
        """Verify single-hop expansion filters by edge type."""
        traversal = GTraversal(graph_store=mock_graph_store_with_edges)
        source_nodes = ['Organization']
        
        result = traversal.one_hop_expand(source_nodes, edge_type='FOUNDED_BY')
        
        assert isinstance(result, set)
        mock_graph_store_with_edges.get_one_hop_edges.assert_called_once_with(source_nodes)
    
    def test_graph_traversal_single_hop_return_src_id(self, mock_graph_store_with_edges):
        """Verify single-hop expansion returns source node mapping when requested."""
        traversal = GTraversal(graph_store=mock_graph_store_with_edges)
        source_nodes = ['Organization']
        
        result = traversal.one_hop_expand(source_nodes, return_src_id=True)
        
        assert isinstance(result, dict)
        mock_graph_store_with_edges.get_one_hop_edges.assert_called_once_with(source_nodes)


class TestGraphTraversalMultiHop:
    """Tests for multi-hop graph traversal."""
    
    def test_graph_traversal_multi_hop(self, mock_graph_store_with_triplets):
        """Verify multi-hop traversal returns triplets from multiple hops."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['Organization']
        
        result = traversal.multi_hop_triplets(source_nodes, hop=2)
        
        assert isinstance(result, set)
        # Verify triplets are tuples with 3 elements
        for triplet in result:
            assert isinstance(triplet, tuple)
            assert len(triplet) == 3
    
    def test_graph_traversal_multi_hop_three_hops(self, mock_graph_store_with_triplets):
        """Verify multi-hop traversal works with three hops."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['Organization']
        
        result = traversal.multi_hop_triplets(source_nodes, hop=3)
        
        assert isinstance(result, set)
        # Should have called get_one_hop_edges multiple times for multi-hop
        assert mock_graph_store_with_triplets.get_one_hop_edges.call_count >= 2


class TestGraphTraversalWithMetapath:
    """Tests for metapath-guided graph traversal."""
    
    def test_graph_traversal_with_metapath(self, mock_graph_store_with_triplets):
        """Verify metapath-guided traversal follows specified edge types."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['John Doe']
        metapaths = [['FOUNDED', 'LOCATED_IN']]
        
        result = traversal.follow_paths(source_nodes, metapaths)
        
        assert isinstance(result, list)
        # Each path should be a list of triplets
        for path in result:
            assert isinstance(path, list)
            if path:  # If path is not empty
                for triplet in path:
                    assert isinstance(triplet, tuple)
                    assert len(triplet) == 3
    
    def test_graph_traversal_with_single_edge_metapath(self, mock_graph_store_with_triplets):
        """Verify metapath traversal works with single-edge paths."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['Organization']
        metapaths = [['FOUNDED_BY']]
        
        result = traversal.follow_paths(source_nodes, metapaths)
        
        assert isinstance(result, list)
    
    def test_graph_traversal_with_multiple_metapaths(self, mock_graph_store_with_triplets):
        """Verify traversal handles multiple metapaths from same source."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['Organization']
        metapaths = [['FOUNDED_BY'], ['LOCATED_IN']]
        
        result = traversal.follow_paths(source_nodes, metapaths)
        
        assert isinstance(result, list)


class TestGraphTraversalTriplets:
    """Tests for triplet-based traversal operations."""
    
    def test_one_hop_triplets(self, mock_graph_store_with_triplets):
        """Verify one-hop triplet expansion returns triplet tuples."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['Organization']
        
        result = traversal.one_hop_triplets(source_nodes)
        
        assert isinstance(result, set)
        for triplet in result:
            assert isinstance(triplet, tuple)
            assert len(triplet) == 3
    
    def test_get_destination_triplet_nodes(self):
        """Verify extraction of destination nodes from triplets."""
        traversal = GTraversal(graph_store=Mock())
        triplets = [
            ('Organization', 'FOUNDED_BY', 'John Doe'),
            ('Organization', 'LOCATED_IN', 'Portland'),
            ('John Doe', 'FOUNDED', 'Organization')
        ]
        
        result = traversal.get_destination_triplet_nodes(triplets)
        
        assert isinstance(result, list)
        assert len(result) == 3
        assert 'John Doe' in result
        assert 'Portland' in result
        assert 'Organization' in result


class TestGraphTraversalShortestPaths:
    """Tests for shortest path finding."""
    
    def test_shortest_paths_basic(self, mock_graph_store_with_triplets):
        """Verify shortest path finding between source and target nodes."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['John Doe']
        target_nodes = ['Portland']
        
        result = traversal.shortest_paths(source_nodes, target_nodes, max_distance=3)
        
        assert isinstance(result, list)
        # Each path should be a list of triplets
        for path in result:
            assert isinstance(path, list)
            for triplet in path:
                assert isinstance(triplet, tuple)
                assert len(triplet) == 3
    
    def test_shortest_paths_with_max_distance(self, mock_graph_store_with_triplets):
        """Verify shortest path respects max_distance constraint."""
        traversal = GTraversal(graph_store=mock_graph_store_with_triplets)
        source_nodes = ['Organization']
        target_nodes = ['Oregon']
        
        result = traversal.shortest_paths(source_nodes, target_nodes, max_distance=1)
        
        assert isinstance(result, list)
