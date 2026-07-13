# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import GraphBatchClient
from graphrag_toolkit.lexical_graph.storage.graph import GraphOperation, Query, QueryTree


class TestGraphBatchClientInitialization:
    """Tests for GraphBatchClient initialization."""

    def test_initialization(self, mock_neptune_store):
        """Verify GraphBatchClient initializes with graph store."""
        client = GraphBatchClient(
            graph_client=mock_neptune_store,
            batch_writes_enabled=True,
            batch_write_size=10,
        )
        assert client.graph_client is mock_neptune_store
        assert client.batch_writes_enabled is True
        assert client.batch_write_size == 10

    def test_initialization_batch_disabled(self, mock_neptune_store):
        """Verify GraphBatchClient initializes with batching disabled."""
        client = GraphBatchClient(
            graph_client=mock_neptune_store,
            batch_writes_enabled=False,
            batch_write_size=1,
        )
        assert client.batch_writes_enabled is False

    def test_initial_state_empty(self, mock_neptune_store):
        """Verify GraphBatchClient starts with empty batch state."""
        client = GraphBatchClient(
            graph_client=mock_neptune_store,
            batch_writes_enabled=True,
            batch_write_size=10,
        )
        assert client.batches == {}
        assert client.all_nodes == []
        assert client.parameterless_queries == {}


class TestGraphBatchClientContextManager:
    """Tests for GraphBatchClient context manager."""

    def test_context_manager_enter_returns_self(self, mock_neptune_store):
        """Verify __enter__ returns the client instance."""
        client = GraphBatchClient(
            graph_client=mock_neptune_store,
            batch_writes_enabled=True,
            batch_write_size=10,
        )
        result = client.__enter__()
        assert result is client

    def test_context_manager_exit_applies_batch(self, mock_neptune_store):
        """Verify __exit__ calls apply_batch_operations."""
        client = GraphBatchClient(
            graph_client=mock_neptune_store,
            batch_writes_enabled=True,
            batch_write_size=10,
        )
        client.__enter__()
        # Should not raise
        client.__exit__(None, None, None)


class TestGraphBatchClientNodeId:
    """Tests for node_id delegation."""

    def test_node_id_delegates_to_graph_client(self, mock_neptune_store):
        """Verify node_id delegates to the underlying graph client."""
        mock_neptune_store.node_id = Mock(return_value='params.entityId')
        client = GraphBatchClient(
            graph_client=mock_neptune_store,
            batch_writes_enabled=True,
            batch_write_size=10,
        )
        result = client.node_id('entityId')
        assert result == 'params.entityId'
        mock_neptune_store.node_id.assert_called_once_with('entityId')


class TestGraphBatchClientOperations:
    def test_batch_forwards_operation(self, mock_neptune_store):
        client = GraphBatchClient(mock_neptune_store, True, 10)

        client.execute_query_with_retry(
            'UPSERT',
            {'params': [{'chunk_id': 'c1'}]},
            operation=GraphOperation.UPSERT_CHUNK,
        )
        client.apply_batch_operations()

        kwargs = mock_neptune_store.execute_query_with_retry.call_args.kwargs
        assert kwargs['operation'] is GraphOperation.UPSERT_CHUNK

    def test_query_tree_batch_forwards_operation(self, mock_neptune_store):
        client = GraphBatchClient(mock_neptune_store, True, 10)
        tree = QueryTree(
            'lookup',
            Query('SELECT', operation=GraphOperation.FIND_COMPLEMENTS),
        )

        client.execute_query_with_retry(tree, {'params': [{'nId': 'e1'}]})
        client.apply_batch_operations()

        kwargs = mock_neptune_store.execute_query_with_retry.call_args.kwargs
        assert kwargs['operation'] is GraphOperation.FIND_COMPLEMENTS

    def test_empty_operation_is_a_noop(self, mock_neptune_store):
        client = GraphBatchClient(mock_neptune_store, True, 10)

        client.execute_query_with_retry(
            'UNWIND $params AS params',
            {'params': []},
            operation=GraphOperation.LINK_FACT_ENTITY,
        )
        client.apply_batch_operations()

        mock_neptune_store.execute_query_with_retry.assert_not_called()
