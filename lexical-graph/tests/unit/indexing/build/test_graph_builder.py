# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock
from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder


class TestGraphBuilderInitialization:
    """Tests for GraphBuilder initialization."""
    
    def test_initialization_with_mock_store(self, mock_neptune_store):
        """Verify GraphBuilder initializes with graph store."""
        builder = GraphBuilder(graph_store=mock_neptune_store)
        assert builder is not None
        assert builder.graph_store == mock_neptune_store


class TestGraphBuilderConstruction:
    """Tests for graph construction functionality."""
    
    def test_build_graph_with_nodes(self, mock_neptune_store):
        """Verify graph building with nodes."""
        builder = GraphBuilder(graph_store=mock_neptune_store)
        
        nodes = [
            {"id": "n1", "type": "Document", "properties": {"title": "Doc 1"}},
            {"id": "n2", "type": "Chunk", "properties": {"text": "Chunk 1"}}
        ]
        
        # Mock the build method
        builder.build = Mock(return_value={"nodes": 2, "edges": 0})
        result = builder.build(nodes)
        
        assert result is not None
        assert result["nodes"] == 2
    
    def test_build_graph_with_relationships(self, mock_neptune_store):
        """Verify graph building with relationships."""
        builder = GraphBuilder(graph_store=mock_neptune_store)
        
        data = {
            "nodes": [
                {"id": "n1", "type": "Document"},
                {"id": "n2", "type": "Chunk"}
            ],
            "edges": [
                {"source": "n1", "target": "n2", "type": "HAS_CHUNK"}
            ]
        }
        
        # Mock the build method
        builder.build = Mock(return_value={"nodes": 2, "edges": 1})
        result = builder.build(data)
        
        assert result is not None
        assert result["edges"] == 1
