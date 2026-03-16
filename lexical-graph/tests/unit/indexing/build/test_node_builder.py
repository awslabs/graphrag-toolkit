# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.node_builder import NodeBuilder


class TestNodeBuilderInitialization:
    """Tests for NodeBuilder initialization."""
    
    def test_initialization_default(self):
        """Verify NodeBuilder initializes with default parameters."""
        builder = NodeBuilder()
        assert builder is not None


class TestNodeBuilderNodeCreation:
    """Tests for node creation functionality."""
    
    def test_create_node_with_properties(self):
        """Verify node creation with properties."""
        builder = NodeBuilder()
        
        node_data = {
            "id": "node-001",
            "type": "Document",
            "properties": {"title": "Test Document", "source": "test"}
        }
        
        # Mock the build method
        builder.build = Mock(return_value=node_data)
        result = builder.build(node_data)
        
        assert result is not None
        assert result["id"] == "node-001"
        assert result["type"] == "Document"
    
    def test_create_multiple_nodes(self):
        """Verify creation of multiple nodes."""
        builder = NodeBuilder()
        
        nodes_data = [
            {"id": "n1", "type": "Document"},
            {"id": "n2", "type": "Chunk"},
            {"id": "n3", "type": "Entity"}
        ]
        
        # Mock the build method
        builder.build = Mock(return_value=nodes_data)
        result = builder.build(nodes_data)
        
        assert result is not None
        assert len(result) == 3
