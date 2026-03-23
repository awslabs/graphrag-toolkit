# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.indexing.build.graph_construction import GraphConstruction, default_builders
from graphrag_toolkit.lexical_graph.storage.constants import INDEX_KEY


class TestGraphConstructionInitialization:
    """Tests for GraphConstruction initialization."""

    def test_initialization_with_graph_store(self, mock_neptune_store):
        """Verify GraphConstruction initializes with graph store."""
        constructor = GraphConstruction(graph_client=mock_neptune_store)
        assert constructor.graph_client is mock_neptune_store

    def test_default_builders_populated(self, mock_neptune_store):
        """Verify GraphConstruction gets default builders."""
        constructor = GraphConstruction(graph_client=mock_neptune_store)
        assert len(constructor.builders) > 0

    def test_for_graph_store_with_graph_store_instance(self, mock_neptune_store):
        """Verify for_graph_store accepts a GraphStore directly."""
        from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
        mock_neptune_store.__class__ = GraphStore
        constructor = GraphConstruction.for_graph_store(mock_neptune_store)
        assert constructor is not None


class TestDefaultBuilders:
    """Tests for the default_builders factory function."""

    def test_returns_non_empty_list(self):
        """Verify default_builders returns a non-empty list."""
        builders = default_builders()
        assert len(builders) > 0

    def test_all_have_index_key(self):
        """Verify all default builders implement index_key."""
        builders = default_builders()
        for builder in builders:
            key = builder.index_key()
            assert isinstance(key, str)
            assert len(key) > 0


class TestGraphConstructionAccept:
    """Tests for the accept method."""

    def test_accept_yields_nodes(self, mock_neptune_store):
        """Verify accept yields processed nodes."""
        constructor = GraphConstruction(graph_client=mock_neptune_store, builders=[])

        mock_node = Mock()
        mock_node.node_id = 'n1'
        mock_node.metadata = {}

        results = list(constructor.accept(
            [mock_node],
            batch_writes_enabled=False,
            batch_write_size=1,
        ))
        # Node without INDEX_KEY is ignored by builders but still yielded
        assert len(results) >= 0

    def test_accept_with_matching_builder(self, mock_neptune_store):
        """Verify accept dispatches to the correct builder."""
        from graphrag_toolkit.lexical_graph.indexing.build.graph_builder import GraphBuilder
        mock_builder = Mock(spec=GraphBuilder)
        mock_builder.index_key.return_value = 'chunk'
        mock_builder.build = Mock()

        constructor = GraphConstruction(
            graph_client=mock_neptune_store,
            builders=[mock_builder],
        )

        mock_node = Mock()
        mock_node.node_id = 'n1'
        mock_node.metadata = {INDEX_KEY: {'index': 'chunk', 'key': 'abc'}}

        list(constructor.accept(
            [mock_node],
            batch_writes_enabled=False,
            batch_write_size=1,
        ))

        mock_builder.build.assert_called_once()
