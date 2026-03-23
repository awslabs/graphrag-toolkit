# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for DummyVectorIndex (in-memory vector store).

This module tests the in-memory vector index implementation.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex, VectorIndex
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndexFactory
from llama_index.core.schema import QueryBundle


class TestDummyVectorIndexFactory:
    """Tests for DummyVectorIndexFactory."""

    def test_try_create_with_dummy_prefix(self):
        """Verify factory creates DummyVectorIndex for dummy:// prefix."""
        factory = DummyVectorIndexFactory()
        
        result = factory.try_create(["chunk", "statement"], "dummy://")
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(idx, DummyVectorIndex) for idx in result)

    def test_try_create_with_non_dummy_prefix_returns_none(self):
        """Verify factory returns None for non-dummy prefixes."""
        factory = DummyVectorIndexFactory()
        
        result = factory.try_create(["chunk"], "aoss://test")
        
        assert result is None

    def test_try_create_creates_indexes_with_correct_names(self):
        """Verify factory creates indexes with correct index names."""
        factory = DummyVectorIndexFactory()
        
        result = factory.try_create(["chunk", "statement"], "dummy://")
        
        assert result[0].index_name == "chunk"
        assert result[1].index_name == "statement"


class TestDummyVectorIndex:
    """Tests for DummyVectorIndex operations."""

    def test_initialization(self):
        """Verify DummyVectorIndex initializes correctly."""
        index = DummyVectorIndex(index_name="chunk")
        
        assert isinstance(index, VectorIndex)
        assert index.index_name == "chunk"

    def test_add_embeddings_returns_nodes(self):
        """Verify add_embeddings returns the input nodes."""
        index = DummyVectorIndex(index_name="chunk")
        nodes = [Mock(), Mock(), Mock()]
        
        result = index.add_embeddings(nodes)
        
        assert result == nodes

    def test_top_k_returns_empty_list(self):
        """Verify top_k returns empty list (dummy implementation)."""
        index = DummyVectorIndex(index_name="chunk")
        query_bundle = QueryBundle(query_str="test query")
        
        result = index.top_k(query_bundle, top_k=5)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_top_k_with_filter_config(self):
        """Verify top_k handles filter configuration."""
        index = DummyVectorIndex(index_name="chunk")
        query_bundle = QueryBundle(query_str="test query")
        from graphrag_toolkit.lexical_graph.metadata import FilterConfig
        filter_config = FilterConfig()
        
        result = index.top_k(query_bundle, top_k=10, filter_config=filter_config)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_get_embeddings_returns_empty_list(self):
        """Verify get_embeddings returns empty list (dummy implementation)."""
        index = DummyVectorIndex(index_name="chunk")
        
        result = index.get_embeddings(ids=["id1", "id2", "id3"])
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_get_embeddings_with_empty_ids(self):
        """Verify get_embeddings handles empty ID list."""
        index = DummyVectorIndex(index_name="chunk")
        
        result = index.get_embeddings(ids=[])
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_update_versioning_returns_empty_list(self):
        """Verify update_versioning returns empty list."""
        index = DummyVectorIndex(index_name="chunk")
        
        result = index.update_versioning(versioning_timestamp=123456, ids=["id1"])
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_enable_for_versioning_returns_empty_list(self):
        """Verify enable_for_versioning returns empty list."""
        index = DummyVectorIndex(index_name="chunk")
        
        result = index.enable_for_versioning(ids=["id1", "id2"])
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_delete_embeddings_returns_empty_list(self):
        """Verify delete_embeddings returns empty list."""
        index = DummyVectorIndex(index_name="chunk")
        
        result = index.delete_embeddings(ids=["id1", "id2", "id3"])
        
        assert isinstance(result, list)
        assert len(result) == 0


class TestDummyVectorIndexMultipleOperations:
    """Tests for multiple operations on DummyVectorIndex."""

    def test_multiple_add_embeddings_calls(self):
        """Verify multiple add_embeddings calls work correctly."""
        index = DummyVectorIndex(index_name="chunk")
        
        nodes1 = [Mock(), Mock()]
        nodes2 = [Mock(), Mock(), Mock()]
        
        result1 = index.add_embeddings(nodes1)
        result2 = index.add_embeddings(nodes2)
        
        assert result1 == nodes1
        assert result2 == nodes2

    def test_multiple_top_k_queries(self):
        """Verify multiple top_k queries work correctly."""
        index = DummyVectorIndex(index_name="chunk")
        
        query1 = QueryBundle(query_str="query 1")
        query2 = QueryBundle(query_str="query 2")
        
        result1 = index.top_k(query1, top_k=5)
        result2 = index.top_k(query2, top_k=10)
        
        assert isinstance(result1, list)
        assert isinstance(result2, list)
        assert len(result1) == 0
        assert len(result2) == 0

    def test_mixed_operations(self):
        """Verify mixed operations work correctly."""
        index = DummyVectorIndex(index_name="chunk")
        
        # Add embeddings
        nodes = [Mock(), Mock()]
        add_result = index.add_embeddings(nodes)
        
        # Query
        query = QueryBundle(query_str="test")
        query_result = index.top_k(query, top_k=5)
        
        # Get embeddings
        get_result = index.get_embeddings(ids=["id1"])
        
        # Delete embeddings
        delete_result = index.delete_embeddings(ids=["id1"])
        
        assert add_result == nodes
        assert len(query_result) == 0
        assert len(get_result) == 0
        assert len(delete_result) == 0
