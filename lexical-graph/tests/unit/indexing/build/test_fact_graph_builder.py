# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.fact_graph_builder import FactGraphBuilder


class TestFactGraphBuilderInitialization:
    """Tests for FactGraphBuilder initialization."""
    
    def test_initialization(self):
        """Verify FactGraphBuilder initializes correctly."""
        builder = FactGraphBuilder()
        assert builder is not None


class TestFactGraphBuilding:
    """Tests for fact graph building functionality."""
    
    def test_build_fact_node(self, mock_neptune_store):
        """Verify building fact node with metadata."""
        builder = FactGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'fact_001'
        mock_node.text = 'GraphRAG is an AI framework'
        mock_node.metadata = {
            'fact': {
                'factId': 'fact_001',
                'subject': 'GraphRAG',
                'predicate': 'is',
                'object': 'AI framework',
                'metadata': {'confidence': 0.95}
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='factId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_multiple_facts(self):
        """Verify building multiple fact nodes."""
        builder = FactGraphBuilder()
        
        facts = [
            Mock(metadata={'fact': {'factId': 'f1', 'subject': 'A', 'predicate': 'is', 'object': 'B'}}),
            Mock(metadata={'fact': {'factId': 'f2', 'subject': 'C', 'predicate': 'has', 'object': 'D'}})
        ]
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='factId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        for fact in facts:
            fact.node_id = fact.metadata['fact']['factId']
            fact.text = f"{fact.metadata['fact']['subject']} {fact.metadata['fact']['predicate']} {fact.metadata['fact']['object']}"
            fact.relationships = {}
            builder.build(fact, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.call_count >= 2


class TestFactGraphBuilderErrorHandling:
    """Tests for fact graph builder error handling."""
    
    def test_build_with_missing_fact_id(self):
        """Verify handling of fact with missing ID."""
        builder = FactGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'fact_001'
        mock_node.metadata = {'fact': {'subject': 'A', 'predicate': 'is', 'object': 'B'}}
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert not mock_graph_client.execute_query_with_retry.called
