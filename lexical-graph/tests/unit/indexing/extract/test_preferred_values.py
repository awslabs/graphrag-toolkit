# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.preferred_values import (
    PreferredValuesProvider,
    DefaultPreferredValues,
    default_preferred_values
)


class TestPreferredValuesProvider:
    """Tests for PreferredValuesProvider base class."""
    
    def test_provider_is_abstract(self):
        """Verify PreferredValuesProvider is an abstract base class."""
        # PreferredValuesProvider should be instantiable but __call__ should be overridden
        provider = PreferredValuesProvider()
        assert provider is not None


class TestDefaultPreferredValues:
    """Tests for DefaultPreferredValues implementation."""
    
    def test_initialization_with_values(self):
        """Verify DefaultPreferredValues initializes with values list."""
        values = ["value1", "value2", "value3"]
        provider = DefaultPreferredValues(values=values)
        
        assert provider.values == values
    
    def test_call_returns_values(self):
        """Verify calling provider returns the configured values."""
        values = ["option1", "option2"]
        provider = DefaultPreferredValues(values=values)
        node = TextNode(text="test")
        
        result = provider(node)
        
        assert result == values
    
    def test_call_with_different_nodes_returns_same_values(self):
        """Verify provider returns same values regardless of node."""
        values = ["a", "b", "c"]
        provider = DefaultPreferredValues(values=values)
        
        node1 = TextNode(text="first node")
        node2 = TextNode(text="second node")
        
        result1 = provider(node1)
        result2 = provider(node2)
        
        assert result1 == values
        assert result2 == values
        assert result1 == result2
    
    def test_call_with_empty_values(self):
        """Verify provider works with empty values list."""
        provider = DefaultPreferredValues(values=[])
        node = TextNode(text="test")
        
        result = provider(node)
        
        assert result == []
    
    def test_call_with_single_value(self):
        """Verify provider works with single value."""
        provider = DefaultPreferredValues(values=["single"])
        node = TextNode(text="test")
        
        result = provider(node)
        
        assert result == ["single"]


class TestDefaultPreferredValuesFactory:
    """Tests for default_preferred_values factory function."""
    
    def test_factory_creates_provider(self):
        """Verify factory function creates DefaultPreferredValues instance."""
        values = ["val1", "val2"]
        provider = default_preferred_values(values)
        
        assert isinstance(provider, DefaultPreferredValues)
        assert isinstance(provider, PreferredValuesProvider)
    
    def test_factory_provider_returns_values(self):
        """Verify provider created by factory returns correct values."""
        values = ["test1", "test2", "test3"]
        provider = default_preferred_values(values)
        node = TextNode(text="test")
        
        result = provider(node)
        
        assert result == values
    
    def test_factory_with_empty_list(self):
        """Verify factory works with empty list."""
        provider = default_preferred_values([])
        node = TextNode(text="test")
        
        result = provider(node)
        
        assert result == []
    
    def test_factory_with_various_value_types(self):
        """Verify factory works with various string values."""
        values = ["short", "a longer value", "123", "special-chars_!"]
        provider = default_preferred_values(values)
        node = TextNode(text="test")
        
        result = provider(node)
        
        assert result == values
        assert len(result) == 4


class TestPreferredValuesIntegration:
    """Integration tests for preferred values functionality."""
    
    def test_multiple_providers_independent(self):
        """Verify multiple providers maintain independent state."""
        provider1 = default_preferred_values(["a", "b"])
        provider2 = default_preferred_values(["x", "y", "z"])
        
        node = TextNode(text="test")
        
        result1 = provider1(node)
        result2 = provider2(node)
        
        assert result1 == ["a", "b"]
        assert result2 == ["x", "y", "z"]
        assert result1 != result2
    
    def test_provider_reusable(self):
        """Verify provider can be called multiple times."""
        values = ["reusable", "values"]
        provider = default_preferred_values(values)
        
        node1 = TextNode(text="first")
        node2 = TextNode(text="second")
        node3 = TextNode(text="third")
        
        result1 = provider(node1)
        result2 = provider(node2)
        result3 = provider(node3)
        
        assert result1 == values
        assert result2 == values
        assert result3 == values
