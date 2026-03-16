# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock
from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from llama_index.core.schema import NodeRelationship


class TestChunkGraphBuilderInitialization:
    """Tests for ChunkGraphBuilder initialization."""
    
    def test_initialization(self):
        """Verify ChunkGraphBuilder initializes correctly."""
        builder = ChunkGraphBuilder()
        
        assert builder is not None
    
    def test_index_key(self):
        """Verify index_key returns correct value."""
        key = ChunkGraphBuilder.index_key()
        
        assert key == 'chunk'


class TestChunkGraphBuilding:
    """Tests for chunk graph building functionality."""
    
    def test_build_chunk_node(self, mock_neptune_store):
        """Verify building chunk node with metadata."""
        builder = ChunkGraphBuilder()
        
        # Mock node with chunk metadata
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'This is a sample chunk of text for testing.'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_001',
                'metadata': {
                    'position': 0,
                    'length': 44
                }
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        # Verify execute_query_with_retry was called
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_chunk_with_source_relationship(self, mock_neptune_store):
        """Verify building chunk with source relationship."""
        builder = ChunkGraphBuilder()
        
        # Mock source relationship
        mock_source_info = Mock()
        mock_source_info.node_id = 'source_001'
        
        # Mock node with source relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Sample chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_001',
                'metadata': {}
            }
        }
        mock_node.relationships = {
            NodeRelationship.SOURCE: mock_source_info
        }
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        # Verify multiple queries were executed (chunk + source relationship)
        assert mock_graph_client.execute_query_with_retry.call_count >= 2
    
    def test_build_chunk_with_parent_relationship(self):
        """Verify building chunk with parent relationship."""
        builder = ChunkGraphBuilder()
        
        # Mock parent relationship
        mock_parent_info = Mock()
        mock_parent_info.node_id = 'parent_chunk_001'
        
        # Mock node with parent relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Child chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_002',
                'metadata': {}
            }
        }
        mock_node.relationships = {
            NodeRelationship.PARENT: mock_parent_info
        }
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        # Verify queries were executed
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_chunk_with_next_relationship(self):
        """Verify building chunk with next relationship."""
        builder = ChunkGraphBuilder()
        
        # Mock next relationship
        mock_next_info = Mock()
        mock_next_info.node_id = 'chunk_003'
        
        # Mock node with next relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Current chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_002',
                'metadata': {}
            }
        }
        mock_node.relationships = {
            NodeRelationship.NEXT: mock_next_info
        }
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_chunk_with_previous_relationship(self):
        """Verify building chunk with previous relationship."""
        builder = ChunkGraphBuilder()
        
        # Mock previous relationship
        mock_prev_info = Mock()
        mock_prev_info.node_id = 'chunk_001'
        
        # Mock node with previous relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Current chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_002',
                'metadata': {}
            }
        }
        mock_node.relationships = {
            NodeRelationship.PREVIOUS: mock_prev_info
        }
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_chunk_with_child_relationship(self):
        """Verify building chunk with child relationship."""
        builder = ChunkGraphBuilder()
        
        # Mock child relationship
        mock_child_info = Mock()
        mock_child_info.node_id = 'child_chunk_001'
        
        # Mock node with child relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Parent chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_001',
                'metadata': {}
            }
        }
        mock_node.relationships = {
            NodeRelationship.CHILD: mock_child_info
        }
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called


class TestChunkGraphBuilderErrorHandling:
    """Tests for chunk graph builder error handling."""
    
    def test_build_with_missing_chunk_id(self):
        """Verify handling of node with missing chunk ID."""
        builder = ChunkGraphBuilder()
        
        # Mock node without chunk ID
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Sample text'
        mock_node.metadata = {
            'chunk': {
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        # Should log warning but not crash
        builder.build(mock_node, mock_graph_client)
        
        # Should not execute queries without chunk ID
        assert not mock_graph_client.execute_query_with_retry.called
    
    def test_build_with_missing_source_info(self):
        """Verify handling of chunk without source relationship."""
        builder = ChunkGraphBuilder()
        
        # Mock node without source relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Sample chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_001',
                'metadata': {}
            }
        }
        mock_node.relationships = {}  # No source relationship
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        # Should log warning but still create chunk node
        builder.build(mock_node, mock_graph_client)
        
        # Should execute at least one query (for chunk node)
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_with_external_metadata_properties(self):
        """Verify handling of external metadata properties."""
        builder = ChunkGraphBuilder()
        
        # Mock node with external metadata
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Sample chunk text'
        mock_node.metadata = {
            'chunk': {
                'chunkId': 'chunk_001',
                'metadata': {
                    'custom_field': 'custom_value',
                    'score': 0.95,
                    'tags': ['important', 'reviewed']
                }
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='chunkId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        # Verify query was executed with external properties
        assert mock_graph_client.execute_query_with_retry.called
