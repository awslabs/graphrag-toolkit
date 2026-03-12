# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for VectorStoreFactory.

This module tests the factory pattern for creating vector stores,
including OpenSearch, in-memory (dummy), and error handling.
"""

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.storage.vector_store_factory import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.vector import (
    VectorStore,
    VectorIndexFactoryMethod,
    VectorIndex,
    DummyVectorIndex
)
from graphrag_toolkit.lexical_graph.storage.constants import DEFAULT_EMBEDDING_INDEXES


class TestVectorStoreFactoryRegister:
    """Tests for VectorStoreFactory.register() method."""

    def test_register_factory_class(self):
        """Verify factory class can be registered."""
        # Create a mock factory class
        class MockVectorIndexFactory(VectorIndexFactoryMethod):
            def try_create(self, index_names, vector_index_info, **kwargs):
                return None

        # Register should not raise
        VectorStoreFactory.register(MockVectorIndexFactory)

    def test_register_factory_instance(self):
        """Verify factory instance can be registered."""
        # Create a mock factory instance
        class MockVectorIndexFactory(VectorIndexFactoryMethod):
            def try_create(self, index_names, vector_index_info, **kwargs):
                return None

        factory_instance = MockVectorIndexFactory()

        # Register should not raise
        VectorStoreFactory.register(factory_instance)

    def test_register_invalid_class_raises_error(self):
        """Verify ValueError raised for invalid factory class."""
        # Create a class that doesn't inherit from VectorIndexFactoryMethod
        class InvalidFactory:
            pass

        with pytest.raises(ValueError, match="must inherit from VectorIndexFactoryMethod"):
            VectorStoreFactory.register(InvalidFactory)

    def test_register_invalid_instance_raises_error(self):
        """Verify ValueError raised for invalid factory instance."""
        # Create an instance that doesn't inherit from VectorIndexFactoryMethod
        class InvalidFactory:
            pass

        invalid_instance = InvalidFactory()

        with pytest.raises(ValueError, match="must inherit from VectorIndexFactoryMethod"):
            VectorStoreFactory.register(invalid_instance)


class TestVectorStoreFactoryForVectorStore:
    """Tests for VectorStoreFactory.for_vector_store() method."""

    def test_factory_returns_existing_vector_store_instance(self):
        """Verify existing VectorStore instance is returned directly."""
        # Create a mock VectorStore instance
        mock_store = Mock(spec=VectorStore)

        result = VectorStoreFactory.for_vector_store(mock_store)

        assert result is mock_store

    def test_factory_creates_dummy_store(self):
        """Verify factory creates dummy/in-memory store."""
        result = VectorStoreFactory.for_vector_store("dummy://")

        assert isinstance(result, VectorStore)
        # Check that indexes were created
        assert len(result.indexes) > 0
        # Check that all indexes are DummyVectorIndex
        for index in result.indexes.values():
            assert isinstance(index, DummyVectorIndex)

    def test_factory_creates_dummy_store_with_single_index(self):
        """Verify factory creates dummy store with single index name."""
        result = VectorStoreFactory.for_vector_store("dummy://", index_names="chunk")

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes
        assert isinstance(result.indexes["chunk"], DummyVectorIndex)

    def test_factory_creates_dummy_store_with_multiple_indexes(self):
        """Verify factory creates dummy store with multiple index names."""
        index_names = ["chunk", "statement"]
        result = VectorStoreFactory.for_vector_store("dummy://", index_names=index_names)

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes
        assert "statement" in result.indexes
        assert isinstance(result.indexes["chunk"], DummyVectorIndex)
        assert isinstance(result.indexes["statement"], DummyVectorIndex)

    def test_factory_with_configuration(self):
        """Verify factory passes configuration to store creation."""
        result = VectorStoreFactory.for_vector_store("dummy://", index_names=["chunk"])

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes

    def test_factory_invalid_type_raises_error(self):
        """Verify ValueError raised for unrecognized vector store type."""
        with pytest.raises(ValueError, match="Unrecognized vector store info"):
            VectorStoreFactory.for_vector_store("invalid://unknown")

    def test_factory_empty_string_raises_error(self):
        """Verify ValueError raised for empty string vector store info."""
        with pytest.raises(ValueError, match="Unrecognized vector store info"):
            VectorStoreFactory.for_vector_store("")

    @patch('boto3.client')
    def test_factory_creates_opensearch_store(self, mock_boto3_client):
        """Verify factory creates OpenSearch store with mocked boto3."""
        # Skip if OpenSearch dependencies not installed
        pytest.importorskip("llama_index.vector_stores.opensearch")
        
        # Mock OpenSearch client
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # OpenSearch Serverless connection string format (correct format is aoss://)
        result = VectorStoreFactory.for_vector_store("aoss://domain-endpoint")

        # Should create a vector store (not raise an error)
        assert isinstance(result, VectorStore)


class TestVectorStoreFactoryForComposite:
    """Tests for VectorStoreFactory.for_composite() method."""

    def test_for_composite_combines_multiple_stores(self):
        """Verify for_composite combines multiple VectorStore instances."""
        # Create mock vector indexes
        index1 = Mock(spec=VectorIndex)
        index1.index_name = "chunk"
        index2 = Mock(spec=VectorIndex)
        index2.index_name = "statement"

        # Create vector stores
        store1 = VectorStore(indexes={"chunk": index1})
        store2 = VectorStore(indexes={"statement": index2})

        # Combine stores
        result = VectorStoreFactory.for_composite([store1, store2])

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes
        assert "statement" in result.indexes
        assert result.indexes["chunk"] is index1
        assert result.indexes["statement"] is index2

    def test_for_composite_with_single_store(self):
        """Verify for_composite works with single store."""
        index1 = Mock(spec=VectorIndex)
        index1.index_name = "chunk"
        store1 = VectorStore(indexes={"chunk": index1})

        result = VectorStoreFactory.for_composite([store1])

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes
        assert result.indexes["chunk"] is index1

    def test_for_composite_with_empty_list(self):
        """Verify for_composite handles empty list."""
        result = VectorStoreFactory.for_composite([])

        assert isinstance(result, VectorStore)
        assert len(result.indexes) == 0

    def test_for_composite_merges_overlapping_indexes(self):
        """Verify for_composite handles overlapping index names (last wins)."""
        index1 = Mock(spec=VectorIndex)
        index1.index_name = "chunk"
        index2 = Mock(spec=VectorIndex)
        index2.index_name = "chunk"

        store1 = VectorStore(indexes={"chunk": index1})
        store2 = VectorStore(indexes={"chunk": index2})

        result = VectorStoreFactory.for_composite([store1, store2])

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes
        # Last store's index should win
        assert result.indexes["chunk"] is index2


class TestVectorStoreFactoryCustomFactory:
    """Tests for custom factory registration and usage."""

    def test_custom_factory_can_create_store(self):
        """Verify custom registered factory can create stores."""
        # Create a custom factory
        class CustomVectorIndexFactory(VectorIndexFactoryMethod):
            def try_create(self, index_names, vector_index_info, **kwargs):
                if vector_index_info and vector_index_info.startswith("custom://"):
                    return [Mock(spec=VectorIndex, index_name=name) for name in index_names]
                return None

        # Register the custom factory
        VectorStoreFactory.register(CustomVectorIndexFactory)

        # Create store using custom factory
        result = VectorStoreFactory.for_vector_store("custom://test", index_names=["chunk"])

        assert isinstance(result, VectorStore)
        assert "chunk" in result.indexes

    def test_multiple_factories_tried_in_order(self):
        """Verify multiple factories are tried until one succeeds."""
        # Create a custom factory that handles a specific prefix
        class SpecificVectorIndexFactory(VectorIndexFactoryMethod):
            def try_create(self, index_names, vector_index_info, **kwargs):
                if vector_index_info and vector_index_info.startswith("specific://"):
                    return [Mock(spec=VectorIndex, index_name=name) for name in index_names]
                return None

        # Register the custom factory
        VectorStoreFactory.register(SpecificVectorIndexFactory)

        # Should use the specific factory
        result = VectorStoreFactory.for_vector_store("specific://test", index_names=["chunk"])
        assert isinstance(result, VectorStore)

        # Should fall back to dummy factory
        result = VectorStoreFactory.for_vector_store("dummy://")
        assert isinstance(result, VectorStore)
        for index in result.indexes.values():
            assert isinstance(index, DummyVectorIndex)
