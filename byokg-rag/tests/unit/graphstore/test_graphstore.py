"""Tests for graphstore.py.

This module tests the GraphStore abstract base class and LocalKGStore implementation.
"""

import pytest
import tempfile
import os
from graphrag_toolkit.byokg_rag.graphstore.graphstore import (
    GraphStore,
    LocalKGStore
)


class TestGraphStoreAbstract:
    """Tests for GraphStore abstract base class."""
    
    def test_graphstore_is_abstract(self):
        """Verify GraphStore cannot be instantiated directly."""
        with pytest.raises(TypeError):
            GraphStore()
    
    def test_graphstore_has_required_methods(self):
        """Verify GraphStore defines required abstract methods."""
        required_methods = [
            'get_schema',
            'nodes',
            'get_nodes',
            'edges',
            'get_edges',
            'get_one_hop_edges',
            'get_edge_destination_nodes'
        ]
        
        for method in required_methods:
            assert hasattr(GraphStore, method)


class TestLocalKGStoreInitialization:
    """Tests for LocalKGStore initialization."""
    
    def test_initialization_empty(self):
        """Verify LocalKGStore initializes with empty graph."""
        store = LocalKGStore()
        
        assert store._graph == {}
    
    def test_initialization_with_graph(self):
        """Verify LocalKGStore initializes with provided graph."""
        initial_graph = {
            'Organization': {
                'FOUNDED_BY': {
                    'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]
                }
            }
        }
        
        store = LocalKGStore(graph=initial_graph)
        
        assert store._graph == initial_graph



class TestLocalKGStoreReadFromCSV:
    """Tests for LocalKGStore read_from_csv method."""
    
    def test_read_from_csv_basic(self):
        """Verify reading triplets from CSV file."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('source,relation,target\n')
            f.write('Organization,FOUNDED_BY,John Doe\n')
            f.write('Organization,LOCATED_IN,Portland\n')
            temp_path = f.name
        
        try:
            store = LocalKGStore()
            graph = store.read_from_csv(temp_path)
            
            assert 'Organization' in graph
            assert 'FOUNDED_BY' in graph['Organization']
            assert 'LOCATED_IN' in graph['Organization']
            assert len(graph['Organization']['FOUNDED_BY']['triplets']) == 1
            assert graph['Organization']['FOUNDED_BY']['triplets'][0] == ('Organization', 'FOUNDED_BY', 'John Doe')
        finally:
            os.unlink(temp_path)
    
    def test_read_from_csv_no_header(self):
        """Verify reading CSV without header."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('Organization,FOUNDED_BY,John Doe\n')
            f.write('Organization,LOCATED_IN,Portland\n')
            temp_path = f.name
        
        try:
            store = LocalKGStore()
            graph = store.read_from_csv(temp_path, has_header=False)
            
            assert 'Organization' in graph
            assert len(graph['Organization']) == 2
        finally:
            os.unlink(temp_path)
    
    def test_read_from_csv_custom_delimiter(self):
        """Verify reading CSV with custom delimiter."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('source|relation|target\n')
            f.write('Organization|FOUNDED_BY|John Doe\n')
            temp_path = f.name
        
        try:
            store = LocalKGStore()
            graph = store.read_from_csv(temp_path, delimiter='|')
            
            assert 'Organization' in graph
            assert 'FOUNDED_BY' in graph['Organization']
        finally:
            os.unlink(temp_path)
    
    def test_read_from_csv_invalid_rows(self):
        """Verify handling of invalid rows in CSV."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('source,relation,target\n')
            f.write('Organization,FOUNDED_BY,John Doe\n')
            f.write('Invalid,Row\n')  # Invalid row with only 2 columns
            f.write('DataCorp,FOUNDED_BY,John Smith\n')
            temp_path = f.name
        
        try:
            store = LocalKGStore()
            graph = store.read_from_csv(temp_path)
            
            assert 'Organization' in graph
            assert 'DataCorp' in graph
            # Invalid row should be skipped
        finally:
            os.unlink(temp_path)



class TestLocalKGStoreGetSchema:
    """Tests for LocalKGStore get_schema method."""
    
    def test_get_schema_empty_graph(self):
        """Verify schema for empty graph."""
        store = LocalKGStore()
        schema = store.get_schema()
        
        assert 'graphSummary' in schema
        assert 'edgeLabels' in schema['graphSummary']
        assert schema['graphSummary']['edgeLabels'] == []
    
    def test_get_schema_with_relations(self):
        """Verify schema extraction from graph."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]},
                'LOCATED_IN': {'triplets': [('Organization', 'LOCATED_IN', 'Portland')]}
            },
            'DataCorp': {
                'FOUNDED_BY': {'triplets': [('DataCorp', 'FOUNDED_BY', 'John Smith')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        schema = store.get_schema()
        
        assert 'graphSummary' in schema
        assert 'edgeLabels' in schema['graphSummary']
        edge_labels = schema['graphSummary']['edgeLabels']
        assert 'FOUNDED_BY' in edge_labels
        assert 'LOCATED_IN' in edge_labels


class TestLocalKGStoreNodes:
    """Tests for LocalKGStore nodes method."""
    
    def test_nodes_empty_graph(self):
        """Verify nodes returns empty list for empty graph."""
        store = LocalKGStore()
        nodes = store.nodes()
        
        assert nodes == []
    
    def test_nodes_with_data(self):
        """Verify nodes returns all node IDs."""
        graph = {
            'Organization': {},
            'DataCorp': {},
            'CloudCorp': {}
        }
        
        store = LocalKGStore(graph=graph)
        nodes = store.nodes()
        
        assert len(nodes) == 3
        assert 'Organization' in nodes
        assert 'DataCorp' in nodes
        assert 'CloudCorp' in nodes


class TestLocalKGStoreGetNodes:
    """Tests for LocalKGStore get_nodes method."""
    
    def test_get_nodes_existing(self):
        """Verify get_nodes returns details for existing nodes."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]}
            },
            'DataCorp': {
                'FOUNDED_BY': {'triplets': [('DataCorp', 'FOUNDED_BY', 'John Smith')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        nodes = store.get_nodes(['Organization', 'DataCorp'])
        
        assert 'Organization' in nodes
        assert 'DataCorp' in nodes
        assert 'FOUNDED_BY' in nodes['Organization']
    
    def test_get_nodes_nonexistent(self):
        """Verify get_nodes handles nonexistent nodes."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        nodes = store.get_nodes(['Organization', 'Nonexistent'])
        
        assert 'Organization' in nodes
        assert 'Nonexistent' not in nodes


class TestLocalKGStoreEdges:
    """Tests for LocalKGStore edges and get_edges methods."""
    
    def test_edges_not_implemented(self):
        """Verify edges raises NotImplementedError."""
        store = LocalKGStore()
        
        with pytest.raises(NotImplementedError, match="does not support a separate edge index"):
            store.edges()
    
    def test_get_edges_not_implemented(self):
        """Verify get_edges raises NotImplementedError."""
        store = LocalKGStore()
        
        with pytest.raises(NotImplementedError, match="does not support a separate edge index"):
            store.get_edges(['edge1'])


class TestLocalKGStoreGetTriplets:
    """Tests for LocalKGStore get_triplets method."""
    
    def test_get_triplets_empty_graph(self):
        """Verify get_triplets returns empty list for empty graph."""
        store = LocalKGStore()
        triplets = store.get_triplets()
        
        assert triplets == []
    
    def test_get_triplets_with_data(self):
        """Verify get_triplets returns all triplets."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]},
                'LOCATED_IN': {'triplets': [('Organization', 'LOCATED_IN', 'Portland')]}
            },
            'DataCorp': {
                'FOUNDED_BY': {'triplets': [('DataCorp', 'FOUNDED_BY', 'John Smith')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        triplets = store.get_triplets()
        
        assert len(triplets) == 3
        assert ('Organization', 'FOUNDED_BY', 'John Doe') in triplets
        assert ('Organization', 'LOCATED_IN', 'Portland') in triplets
        assert ('DataCorp', 'FOUNDED_BY', 'John Smith') in triplets



class TestLocalKGStoreGetOneHopEdges:
    """Tests for LocalKGStore get_one_hop_edges method."""
    
    def test_get_one_hop_edges_basic(self):
        """Verify get_one_hop_edges returns triplets for source nodes."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]},
                'LOCATED_IN': {'triplets': [('Organization', 'LOCATED_IN', 'Portland')]}
            },
            'DataCorp': {
                'FOUNDED_BY': {'triplets': [('DataCorp', 'FOUNDED_BY', 'John Smith')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        edges = store.get_one_hop_edges(['Organization'])
        
        assert 'Organization' in edges
        assert 'FOUNDED_BY' in edges['Organization']
        assert 'LOCATED_IN' in edges['Organization']
        assert len(edges['Organization']['FOUNDED_BY']) == 1
        assert edges['Organization']['FOUNDED_BY'][0] == ('Organization', 'FOUNDED_BY', 'John Doe')
    
    def test_get_one_hop_edges_multiple_sources(self):
        """Verify get_one_hop_edges handles multiple source nodes."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]}
            },
            'DataCorp': {
                'FOUNDED_BY': {'triplets': [('DataCorp', 'FOUNDED_BY', 'John Smith')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        edges = store.get_one_hop_edges(['Organization', 'DataCorp'])
        
        assert 'Organization' in edges
        assert 'DataCorp' in edges
    
    def test_get_one_hop_edges_nonexistent_node(self):
        """Verify get_one_hop_edges handles nonexistent nodes."""
        graph = {
            'Organization': {
                'FOUNDED_BY': {'triplets': [('Organization', 'FOUNDED_BY', 'John Doe')]}
            }
        }
        
        store = LocalKGStore(graph=graph)
        edges = store.get_one_hop_edges(['Organization', 'Nonexistent'])
        
        assert 'Organization' in edges
        assert 'Nonexistent' not in edges
    
    def test_get_one_hop_edges_return_triplets_false(self):
        """Verify get_one_hop_edges raises error when return_triplets=False."""
        store = LocalKGStore()
        
        with pytest.raises(ValueError, match="supports only triplet format"):
            store.get_one_hop_edges(['Organization'], return_triplets=False)


class TestLocalKGStoreGetEdgeDestinationNodes:
    """Tests for LocalKGStore get_edge_destination_nodes method."""
    
    def test_get_edge_destination_nodes_not_implemented(self):
        """Verify get_edge_destination_nodes raises NotImplementedError."""
        store = LocalKGStore()
        
        with pytest.raises(NotImplementedError, match="not implemented"):
            store.get_edge_destination_nodes(['edge1'])


class TestLocalKGStoreGetLinkerTasks:
    """Tests for LocalKGStore get_linker_tasks method."""
    
    def test_get_linker_tasks(self):
        """Verify get_linker_tasks returns expected tasks."""
        store = LocalKGStore()
        tasks = store.get_linker_tasks()
        
        assert isinstance(tasks, list)
        assert 'entity-extraction' in tasks
        assert 'path-extraction' in tasks
        assert 'draft-answer-generation' in tasks
