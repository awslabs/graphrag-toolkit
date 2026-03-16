# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock
from graphrag_toolkit.lexical_graph.indexing.build.entity_relation_graph_builder import EntityRelationGraphBuilder


class TestEntityRelationGraphBuilderInitialization:
    """Tests for EntityRelationGraphBuilder initialization."""
    
    def test_initialization(self):
        """Verify EntityRelationGraphBuilder initializes correctly."""
        builder = EntityRelationGraphBuilder()
        
        assert builder is not None


class TestEntityRelationBuilding:
    """Tests for entity relation building functionality."""
    
    def test_build_entity_relation(self, mock_neptune_store):
        """Verify building entity relation between two entities."""
        builder = EntityRelationGraphBuilder()
        
        # Mock node with relation metadata
        mock_node = Mock()
        mock_node.node_id = 'relation_001'
        mock_node.metadata = {
            'relation': {
                'relationId': 'rel_001',
                'source_entity': 'entity_001',
                'target_entity': 'entity_002',
                'relation_type': 'RELATES_TO',
                'metadata': {
                    'confidence': 0.95
                }
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='relationId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_relation_with_properties(self):
        """Verify building relation with additional properties."""
        builder = EntityRelationGraphBuilder()
        
        # Mock node with relation properties
        mock_node = Mock()
        mock_node.node_id = 'relation_002'
        mock_node.metadata = {
            'relation': {
                'relationId': 'rel_002',
                'source_entity': 'entity_003',
                'target_entity': 'entity_004',
                'relation_type': 'MENTIONS',
                'metadata': {
                    'confidence': 0.88,
                    'context': 'mentioned in paragraph 2',
                    'weight': 0.75
                }
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='relationId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_multiple_relations(self):
        """Verify building multiple entity relations."""
        builder = EntityRelationGraphBuilder()
        
        relations = [
            Mock(metadata={'relation': {'relationId': 'r1', 'source_entity': 'e1', 'target_entity': 'e2', 'relation_type': 'RELATES_TO'}}),
            Mock(metadata={'relation': {'relationId': 'r2', 'source_entity': 'e2', 'target_entity': 'e3', 'relation_type': 'MENTIONS'}}),
            Mock(metadata={'relation': {'relationId': 'r3', 'source_entity': 'e1', 'target_entity': 'e3', 'relation_type': 'CONNECTS_TO'}})
        ]
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='relationId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        for relation in relations:
            relation.node_id = relation.metadata['relation']['relationId']
            relation.relationships = {}
            builder.build(relation, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.call_count >= 3


class TestEntityRelationGraphBuilderErrorHandling:
    """Tests for entity relation builder error handling."""
    
    def test_build_with_missing_relation_id(self):
        """Verify handling of relation with missing ID."""
        builder = EntityRelationGraphBuilder()
        
        # Mock node without relation ID
        mock_node = Mock()
        mock_node.node_id = 'relation_001'
        mock_node.metadata = {
            'relation': {
                'source_entity': 'entity_001',
                'target_entity': 'entity_002',
                'relation_type': 'RELATES_TO'
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        # Should log warning but not crash
        builder.build(mock_node, mock_graph_client)
        
        # Should not execute queries without relation ID
        assert not mock_graph_client.execute_query_with_retry.called
    
    def test_build_with_missing_source_entity(self):
        """Verify handling of relation with missing source entity."""
        builder = EntityRelationGraphBuilder()
        
        # Mock node without source entity
        mock_node = Mock()
        mock_node.node_id = 'relation_001'
        mock_node.metadata = {
            'relation': {
                'relationId': 'rel_001',
                'target_entity': 'entity_002',
                'relation_type': 'RELATES_TO'
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        # Should handle gracefully
        builder.build(mock_node, mock_graph_client)
    
    def test_build_with_missing_target_entity(self):
        """Verify handling of relation with missing target entity."""
        builder = EntityRelationGraphBuilder()
        
        # Mock node without target entity
        mock_node = Mock()
        mock_node.node_id = 'relation_001'
        mock_node.metadata = {
            'relation': {
                'relationId': 'rel_001',
                'source_entity': 'entity_001',
                'relation_type': 'RELATES_TO'
            }
        }
        mock_node.relationships = {}
        
        # Mock graph client
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        # Should handle gracefully
        builder.build(mock_node, mock_graph_client)
