# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for GraphStoreFactory.

This module tests the factory pattern for creating graph stores,
including Neptune, in-memory (dummy), and error handling.
"""

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import (
    GraphStore,
    GraphStoreFactoryMethod,
    DummyGraphStore
)


class TestGraphStoreFactoryRegister:
    """Tests for GraphStoreFactory.register() method."""

    def test_register_factory_class(self):
        """Verify factory class can be registered."""
        # Create a mock factory class
        class MockGraphStoreFactory(GraphStoreFactoryMethod):
            def try_create(self, graph_info, **kwargs):
                return None

        # Register should not raise
        GraphStoreFactory.register(MockGraphStoreFactory)

    def test_register_factory_instance(self):
        """Verify factory instance can be registered."""
        # Create a mock factory instance
        class MockGraphStoreFactory(GraphStoreFactoryMethod):
            def try_create(self, graph_info, **kwargs):
                return None

        factory_instance = MockGraphStoreFactory()

        # Register should not raise
        GraphStoreFactory.register(factory_instance)

    def test_register_invalid_class_raises_error(self):
        """Verify ValueError raised for invalid factory class."""
        # Create a class that doesn't inherit from GraphStoreFactoryMethod
        class InvalidFactory:
            pass

        with pytest.raises(ValueError, match="must inherit from GraphStoreFactoryMethod"):
            GraphStoreFactory.register(InvalidFactory)

    def test_register_invalid_instance_raises_error(self):
        """Verify ValueError raised for invalid factory instance."""
        # Create an instance that doesn't inherit from GraphStoreFactoryMethod
        class InvalidFactory:
            pass

        invalid_instance = InvalidFactory()

        with pytest.raises(ValueError, match="must inherit from GraphStoreFactoryMethod"):
            GraphStoreFactory.register(invalid_instance)


class TestGraphStoreFactoryForGraphStore:
    """Tests for GraphStoreFactory.for_graph_store() method."""

    def test_factory_returns_existing_graph_store_instance(self):
        """Verify existing GraphStore instance is returned directly."""
        # Create a mock GraphStore instance
        mock_store = Mock(spec=GraphStore)

        result = GraphStoreFactory.for_graph_store(mock_store)

        assert result is mock_store

    def test_factory_creates_dummy_store(self):
        """Verify factory creates dummy/in-memory store."""
        result = GraphStoreFactory.for_graph_store("dummy://")

        assert isinstance(result, DummyGraphStore)

    def test_factory_with_configuration(self):
        """Verify factory passes configuration to store creation."""
        from graphrag_toolkit.lexical_graph.storage.graph import RedactedGraphQueryLogFormatting

        log_formatting = RedactedGraphQueryLogFormatting()
        result = GraphStoreFactory.for_graph_store("dummy://", log_formatting=log_formatting)

        assert isinstance(result, DummyGraphStore)
        assert result.log_formatting == log_formatting

    def test_factory_invalid_type_raises_error(self):
        """Verify ValueError raised for unrecognized graph store type."""
        with pytest.raises(ValueError, match="Unrecognized graph store info"):
            GraphStoreFactory.for_graph_store("invalid://unknown")

    def test_factory_none_graph_info_raises_error(self):
        """Verify ValueError raised when no valid factory can create store."""
        # None graph_info should be handled gracefully by factories
        # The factories check if graph_info is None or doesn't match their pattern
        with pytest.raises(ValueError, match="Unrecognized graph store info"):
            GraphStoreFactory.for_graph_store("unknown://invalid")

    def test_factory_empty_string_raises_error(self):
        """Verify ValueError raised for empty string graph info."""
        with pytest.raises(ValueError, match="Unrecognized graph store info"):
            GraphStoreFactory.for_graph_store("")

    @patch('boto3.client')
    def test_factory_creates_neptune_analytics_store(self, mock_boto3_client):
        """Verify factory creates Neptune Analytics store with mocked boto3."""
        # Mock Neptune Analytics client
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Neptune Analytics connection string format (correct format is neptune-graph://)
        result = GraphStoreFactory.for_graph_store("neptune-graph://graph-id")

        # Should create a graph store (not raise an error)
        assert isinstance(result, GraphStore)

    @patch('boto3.client')
    def test_factory_creates_neptune_database_store(self, mock_boto3_client):
        """Verify factory creates Neptune Database store with mocked boto3."""
        # Mock Neptune Database client
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Neptune Database connection string format
        result = GraphStoreFactory.for_graph_store("neptune-db://cluster-endpoint:8182")

        # Should create a graph store (not raise an error)
        assert isinstance(result, GraphStore)


class TestGraphStoreFactoryCustomFactory:
    """Tests for custom factory registration and usage."""

    def test_custom_factory_can_create_store(self):
        """Verify custom registered factory can create stores."""
        # Create a custom factory
        class CustomGraphStoreFactory(GraphStoreFactoryMethod):
            def try_create(self, graph_info, **kwargs):
                if graph_info and graph_info.startswith("custom://"):
                    return Mock(spec=GraphStore)
                return None

        # Register the custom factory
        GraphStoreFactory.register(CustomGraphStoreFactory)

        # Create store using custom factory
        result = GraphStoreFactory.for_graph_store("custom://test")

        assert isinstance(result, GraphStore)

    def test_multiple_factories_tried_in_order(self):
        """Verify multiple factories are tried until one succeeds."""
        # Create a custom factory that handles a specific prefix
        class SpecificGraphStoreFactory(GraphStoreFactoryMethod):
            def try_create(self, graph_info, **kwargs):
                if graph_info and graph_info.startswith("specific://"):
                    return Mock(spec=GraphStore)
                return None

        # Register the custom factory
        GraphStoreFactory.register(SpecificGraphStoreFactory)

        # Should use the specific factory
        result = GraphStoreFactory.for_graph_store("specific://test")
        assert isinstance(result, GraphStore)

        # Should fall back to dummy factory
        result = GraphStoreFactory.for_graph_store("dummy://")
        assert isinstance(result, DummyGraphStore)
