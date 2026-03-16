# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.statement_graph_builder import StatementGraphBuilder


class TestStatementGraphBuilderInitialization:
    """Tests for StatementGraphBuilder initialization."""
    
    def test_initialization(self):
        """Verify StatementGraphBuilder initializes correctly."""
        builder = StatementGraphBuilder()
        assert builder is not None


class TestStatementGraphBuilding:
    """Tests for statement graph building functionality."""
    
    def test_build_statement_node(self, mock_neptune_store):
        """Verify building statement node with metadata."""
        builder = StatementGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'stmt_001'
        mock_node.text = 'GraphRAG combines knowledge graphs with RAG'
        mock_node.metadata = {
            'statement': {
                'statementId': 'stmt_001',
                'text': 'GraphRAG combines knowledge graphs with RAG',
                'metadata': {
                    'confidence': 0.92,
                    'source_chunk': 'chunk_001'
                }
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='statementId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_statement_with_entities(self):
        """Verify building statement with entity references."""
        builder = StatementGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'stmt_002'
        mock_node.text = 'Knowledge graphs store structured information'
        mock_node.metadata = {
            'statement': {
                'statementId': 'stmt_002',
                'text': 'Knowledge graphs store structured information',
                'entities': ['entity_001', 'entity_002'],
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='statementId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
    
    def test_build_multiple_statements(self):
        """Verify building multiple statement nodes."""
        builder = StatementGraphBuilder()
        
        statements = [
            Mock(metadata={'statement': {'statementId': 's1', 'text': 'Statement 1'}}),
            Mock(metadata={'statement': {'statementId': 's2', 'text': 'Statement 2'}})
        ]
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='statementId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        for stmt in statements:
            stmt.node_id = stmt.metadata['statement']['statementId']
            stmt.text = stmt.metadata['statement']['text']
            stmt.relationships = {}
            builder.build(stmt, mock_graph_client)
        
        assert mock_graph_client.execute_query_with_retry.call_count >= 2


class TestStatementGraphBuilderErrorHandling:
    """Tests for statement graph builder error handling."""
    
    def test_build_with_missing_statement_id(self):
        """Verify handling of statement with missing ID."""
        builder = StatementGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'stmt_001'
        mock_node.metadata = {'statement': {'text': 'Statement without ID'}}
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert not mock_graph_client.execute_query_with_retry.called
    
    def test_build_with_empty_statement_text(self):
        """Verify handling of statement with empty text."""
        builder = StatementGraphBuilder()
        
        mock_node = Mock()
        mock_node.node_id = 'stmt_001'
        mock_node.text = ''
        mock_node.metadata = {
            'statement': {
                'statementId': 'stmt_001',
                'text': '',
                'metadata': {}
            }
        }
        mock_node.relationships = {}
        
        mock_graph_client = Mock()
        mock_graph_client.node_id = Mock(return_value='statementId')
        mock_graph_client.execute_query_with_retry = Mock()
        
        builder.build(mock_node, mock_graph_client)
        assert mock_graph_client.execute_query_with_retry.called
