# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock
from graphrag_toolkit.lexical_graph.indexing.build.entity_graph_builder import EntityGraphBuilder


class TestEntityGraphBuilderInitialization:
    """Tests for EntityGraphBuilder initialization."""
    
    def test_initialization(self):
        """Verify EntityGraphBuilder initializes correctly."""
        builder = EntityGraphBuilder()
        
        assert builder is not None
    
    def test_index_key(self):
        """Verify index_key returns correct value."""
        key = EntityGraphBuilder.index_key()
        
        assert key == 'entity'


class TestEntityGraphBuilding:
    """Tests for entity graph building functionality."""
    
    def test_build_entity_node(self, mock_neptune_store):
        """Verify building entity node with properties."""
        builder = EntityGraphBuilder()
        
        # Mock node with entity metadata
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'GraphRAG'
        mock_node.metadata = {
            'entity': {
                'entityId': 'entity_001',
                'name': 'GraphRAG',
                'type': 'Technology',
                'metadata': {
                    'category': 'AI Framework'
                }
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='entityId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_entity_with_chunk_relationship(self):
        """Verify building entity with chunk relationship."""
        builder = EntityGraphBuilder()
        
        # Mock node with chunk relationship
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Knowledge Graph'
        mock_node.metadata = {
            'entity': {
                'entityId': 'entity_002',
                'name': 'Knowledge Graph',
                'type': 'Concept',
                'metadata': {
                    'chunk_id': 'chunk_001'
                }
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='entityId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_multiple_entities(self):
        """Verify building multiple entity nodes."""
        builder = EntityGraphBuilder()
        
        entities = [
            Mock(metadata={'entity': {'entityId': 'e1', 'name': 'Entity1'}}),
            Mock(metadata={'entity': {'entityId': 'e2', 'name': 'Entity2'}}),
            Mock(metadata={'entity': {'entityId': 'e3', 'name': 'Entity3'}})
        ]
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='entityId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        for entity in entities:
            entity.node_id = entity.metadata['entity']['entityId']
            entity.text = entity.metadata['entity']['name']
            entity.relationships = {}
            builder.build(entity, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.call_count >= 3


class TestEntityGraphBuilderErrorHandling:
    """Tests for entity graph builder error handling."""
    
    def test_build_with_missing_entity_id(self):
        """Verify handling of node with missing entity ID."""
        builder = EntityGraphBuilder()
        
        # Mock node without entity ID
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = 'Entity Name'
        mock_node.metadata = {
            'entity': {
                'name': 'Entity Name',
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        # Should log warning but not crash
        builder.build(mock_node, mock_graph_client)
        
        # Should not execute queries without entity ID
        assert not mock_graph_client.execute_query_with_retry.called
    
    def test_build_with_empty_entity_name(self):
        """Verify handling of entity with empty name."""
        builder = EntityGraphBuilder()
        
        # Mock node with empty name
        mock_node = Mock()
        mock_node.node_id = 'node_123'
        mock_node.text = ''
        mock_node.metadata = {
            'entity': {
                'entityId': 'entity_001',
                'name': '',
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='entityId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        # Should still create entity node
        assert mock_graph_client.execute_query_with_retry.called
