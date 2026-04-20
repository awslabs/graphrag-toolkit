# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for DummyGraphStore (in-memory graph store).

This module tests the in-memory graph store implementation,
including query execution and logging.
"""

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.storage.graph import DummyGraphStore, GraphStore, RedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStoreFactory


class TestDummyGraphStoreFactory:
    """Tests for DummyGraphStoreFactory."""

    def test_try_create_with_dummy_prefix(self):
        """Verify factory creates DummyGraphStore for dummy:// prefix."""
        factory = DummyGraphStoreFactory()
        
        result = factory.try_create("dummy://")
        
        assert isinstance(result, DummyGraphStore)

    def test_try_create_with_non_dummy_prefix_returns_none(self):
        """Verify factory returns None for non-dummy prefixes."""
        factory = DummyGraphStoreFactory()
        
        result = factory.try_create("neptune-graph://test")
        
        assert result is None

    def test_try_create_with_log_formatting(self):
        """Verify factory passes log formatting to DummyGraphStore."""
        factory = DummyGraphStoreFactory()
        log_formatting = RedactedGraphQueryLogFormatting()
        
        result = factory.try_create("dummy://", log_formatting=log_formatting)
        
        assert isinstance(result, DummyGraphStore)
        assert result.log_formatting == log_formatting


class TestDummyGraphStore:
    """Tests for DummyGraphStore operations."""

    def test_initialization(self):
        """Verify DummyGraphStore initializes correctly."""
        store = DummyGraphStore()
        
        assert isinstance(store, GraphStore)
        assert store.log_formatting is not None

    def test_initialization_with_log_formatting(self):
        """Verify DummyGraphStore accepts custom log formatting."""
        log_formatting = RedactedGraphQueryLogFormatting()
        store = DummyGraphStore(log_formatting=log_formatting)
        
        assert store.log_formatting == log_formatting

    def test_execute_query_returns_empty_list(self):
        """Verify execute_query returns empty list (dummy implementation)."""
        store = DummyGraphStore()
        
        result = store.execute_query("MATCH (n) RETURN n")
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_execute_query_with_parameters(self):
        """Verify execute_query handles parameters."""
        store = DummyGraphStore()
        parameters = {"param1": "value1", "param2": 42}
        
        result = store.execute_query("MATCH (n) WHERE n.id = $param1 RETURN n", parameters)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_execute_query_with_correlation_id(self):
        """Verify execute_query handles correlation ID for logging."""
        store = DummyGraphStore()
        
        result = store.execute_query("MATCH (n) RETURN n", correlation_id="test-123")
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_context_manager_enter_exit(self):
        """Verify DummyGraphStore works as context manager."""
        store = DummyGraphStore()
        
        with store as s:
            assert s is store
            result = s.execute_query("MATCH (n) RETURN n")
            assert isinstance(result, list)

    def test_execute_query_logs_query(self):
        """Verify execute_query logs the query for debugging."""
        store = DummyGraphStore()
        
        with patch('graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store.logger') as mock_logger:
            store.execute_query("MATCH (n) RETURN n", {"param": "value"})
            
            # Should have logged the query
            assert mock_logger.debug.called

    def test_multiple_queries(self):
        """Verify multiple queries can be executed."""
        store = DummyGraphStore()
        
        result1 = store.execute_query("MATCH (n:Document) RETURN n")
        result2 = store.execute_query("MATCH (n:Chunk) RETURN n")
        result3 = store.execute_query("MATCH (n:Entity) RETURN n")
        
        assert all(isinstance(r, list) for r in [result1, result2, result3])
        assert all(len(r) == 0 for r in [result1, result2, result3])

    def test_execute_query_with_complex_parameters(self):
        """Verify execute_query handles complex parameter types."""
        store = DummyGraphStore()
        parameters = {
            "string_param": "test",
            "int_param": 42,
            "float_param": 3.14,
            "bool_param": True,
            "list_param": [1, 2, 3],
            "dict_param": {"key": "value"}
        }
        
        result = store.execute_query("MATCH (n) RETURN n", parameters)
        
        assert isinstance(result, list)
        assert len(result) == 0


class TestDummyGraphStoreErrorHandling:
    """Tests for DummyGraphStore error handling."""

    def test_execute_query_with_empty_query(self):
        """Verify execute_query handles empty query string."""
        store = DummyGraphStore()
        
        # Should not raise, just return empty list
        result = store.execute_query("")
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_execute_query_with_none_parameters(self):
        """Verify execute_query handles None parameters."""
        store = DummyGraphStore()
        
        # Should not raise, just return empty list
        result = store.execute_query("MATCH (n) RETURN n", None)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_execute_query_with_empty_parameters(self):
        """Verify execute_query handles empty parameters dict."""
        store = DummyGraphStore()
        
        result = store.execute_query("MATCH (n) RETURN n", {})
        
        assert isinstance(result, list)
        assert len(result) == 0
