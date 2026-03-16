# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from typing import List, Any, Generator
from unittest.mock import Mock
from llama_index.core.schema import BaseNode, TextNode
from graphrag_toolkit.lexical_graph.indexing.node_handler import NodeHandler


class ConcreteNodeHandler(NodeHandler):
    """Concrete implementation of NodeHandler for testing."""
    
    def accept(self, nodes: List[BaseNode], **kwargs: Any) -> Generator[BaseNode, None, None]:
        """Simple implementation that yields all nodes."""
        for node in nodes:
            yield node


class TestNodeHandlerInitialization:
    """Tests for NodeHandler initialization - Task 7.5."""
    
    def test_initialization_default(self):
        """Verify NodeHandler initializes with default show_progress."""
        handler = ConcreteNodeHandler()
        assert handler.show_progress is True
    
    def test_initialization_with_show_progress_false(self):
        """Verify NodeHandler initializes with show_progress=False."""
        handler = ConcreteNodeHandler(show_progress=False)
        assert handler.show_progress is False
    
    def test_initialization_with_show_progress_true(self):
        """Verify NodeHandler initializes with show_progress=True."""
        handler = ConcreteNodeHandler(show_progress=True)
        assert handler.show_progress is True


class TestNodeHandlerCallMethod:
    """Tests for NodeHandler __call__ method."""
    
    def test_call_with_empty_list(self):
        """Verify __call__ handles empty node list."""
        handler = ConcreteNodeHandler()
        result = handler([])
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_call_with_single_node(self):
        """Verify __call__ processes single node."""
        handler = ConcreteNodeHandler()
        node = TextNode(text="Test content")
        result = handler([node])
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == node
    
    def test_call_with_multiple_nodes(self):
        """Verify __call__ processes multiple nodes."""
        handler = ConcreteNodeHandler()
        nodes = [
            TextNode(text="Node 1"),
            TextNode(text="Node 2"),
            TextNode(text="Node 3")
        ]
        result = handler(nodes)
        
        assert isinstance(result, list)
        assert len(result) == 3
        assert result == nodes


class TestNodeHandlerAbstractMethod:
    """Tests for NodeHandler abstract method enforcement."""
    
    def test_abstract_accept_method_must_be_implemented(self):
        """Verify that NodeHandler cannot be instantiated without implementing accept."""
        # NodeHandler is abstract and requires accept() to be implemented
        # Attempting to instantiate it directly should fail
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            NodeHandler()


class TestNodeHandlerEdgeCases:
    """Tests for NodeHandler edge cases."""
    
    def test_handle_large_node_list(self):
        """Verify NodeHandler handles large number of nodes efficiently."""
        handler = ConcreteNodeHandler()
        nodes = [TextNode(text=f"Node {i}") for i in range(1000)]
        result = handler(nodes)
        
        assert len(result) == 1000
        assert all(isinstance(n, TextNode) for n in result)
    
    def test_handle_nodes_with_metadata(self):
        """Verify NodeHandler preserves node metadata."""
        handler = ConcreteNodeHandler()
        nodes = [
            TextNode(text="Node 1", metadata={"key": "value1"}),
            TextNode(text="Node 2", metadata={"key": "value2"})
        ]
        result = handler(nodes)
        
        assert len(result) == 2
        assert result[0].metadata["key"] == "value1"
        assert result[1].metadata["key"] == "value2"
    
    def test_handle_nodes_with_different_types(self):
        """Verify NodeHandler handles different node types."""
        handler = ConcreteNodeHandler()
        nodes = [
            TextNode(text="Text node 1"),
            TextNode(text="Text node 2"),
        ]
        result = handler(nodes)
        
        assert len(result) == 2
        assert all(isinstance(n, BaseNode) for n in result)


class TestNodeHandlerPropertyManagement:
    """Tests for NodeHandler property management."""
    
    def test_show_progress_property_access(self):
        """Verify show_progress property can be accessed."""
        handler = ConcreteNodeHandler(show_progress=True)
        assert handler.show_progress is True
        
        handler2 = ConcreteNodeHandler(show_progress=False)
        assert handler2.show_progress is False
    
    def test_show_progress_property_modification(self):
        """Verify show_progress property can be modified after initialization."""
        handler = ConcreteNodeHandler(show_progress=True)
        assert handler.show_progress is True
        
        handler.show_progress = False
        assert handler.show_progress is False


class TestNodeHandlerValidation:
    """Tests for NodeHandler validation behavior."""
    
    def test_validate_input_is_list(self):
        """Verify NodeHandler expects list input."""
        handler = ConcreteNodeHandler()
        nodes = [TextNode(text="Test")]
        
        # Should work with list
        result = handler(nodes)
        assert isinstance(result, list)
    
    def test_validate_output_is_list(self):
        """Verify NodeHandler returns list output."""
        handler = ConcreteNodeHandler()
        nodes = [TextNode(text="Test")]
        result = handler(nodes)
        
        assert isinstance(result, list)
        assert all(isinstance(n, BaseNode) for n in result)
    
    def test_empty_nodes_returns_empty_list(self):
        """Verify empty input returns empty output."""
        handler = ConcreteNodeHandler()
        result = handler([])
        
        assert result == []
        assert isinstance(result, list)


class TestNodeHandlerAcceptMethod:
    """Tests for NodeHandler accept method behavior."""
    
    def test_accept_yields_nodes(self):
        """Verify accept method yields nodes as generator."""
        handler = ConcreteNodeHandler()
        nodes = [TextNode(text="Node 1"), TextNode(text="Node 2")]
        
        # accept returns a generator
        gen = handler.accept(nodes)
        assert hasattr(gen, '__iter__')
        assert hasattr(gen, '__next__')
        
        # Consume generator
        result = list(gen)
        assert len(result) == 2
    
    def test_accept_preserves_node_order(self):
        """Verify accept method preserves node order."""
        handler = ConcreteNodeHandler()
        nodes = [
            TextNode(text="First"),
            TextNode(text="Second"),
            TextNode(text="Third")
        ]
        
        result = list(handler.accept(nodes))
        assert result[0].text == "First"
        assert result[1].text == "Second"
        assert result[2].text == "Third"


class TestNodeHandlerIntegration:
    """Integration tests for NodeHandler with realistic scenarios."""
    
    def test_process_document_chunks(self):
        """Verify NodeHandler can process document chunks."""
        handler = ConcreteNodeHandler()
        chunks = [
            TextNode(text="Chapter 1: Introduction", metadata={"chapter": 1}),
            TextNode(text="Chapter 2: Background", metadata={"chapter": 2}),
            TextNode(text="Chapter 3: Methods", metadata={"chapter": 3})
        ]
        
        result = handler(chunks)
        
        assert len(result) == 3
        assert all(isinstance(n, TextNode) for n in result)
        assert result[0].metadata["chapter"] == 1
        assert result[2].metadata["chapter"] == 3
    
    def test_process_nodes_with_ids(self):
        """Verify NodeHandler preserves node IDs."""
        handler = ConcreteNodeHandler()
        nodes = [
            TextNode(text="Node 1", id_="node-001"),
            TextNode(text="Node 2", id_="node-002")
        ]
        
        result = handler(nodes)
        
        assert len(result) == 2
        assert result[0].id_ == "node-001"
        assert result[1].id_ == "node-002"

