# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for `VectorBatchClient` and `BatchVectorIndex`.

Rewritten against the real API. The earlier version of this file called
`batch_add_embeddings`, `batch_update_embeddings` and `batch_delete_embeddings`,
none of which exist - and then assigned each one a `Mock` before calling it, so
every assertion was about the mock's own return value rather than about any code
in this repository. It also depended on a `mock_opensearch_store` fixture that
does not exist, so it errored at setup. Nothing noticed, because pytest's default
`norecursedirs` contains `build`, so this whole directory was skipped unless its
path was named explicitly.
"""

from unittest.mock import MagicMock

import pytest

from graphrag_toolkit.lexical_graph.indexing.build.vector_batch_client import (
    BatchVectorIndex,
    VectorBatchClient,
)
from graphrag_toolkit.lexical_graph.storage.constants import ALL_EMBEDDING_INDEXES
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex

def vector_index(index_name:str) -> MagicMock:
    index = MagicMock()
    index.index_name = index_name
    return index

def vector_store(*index_names:str) -> MagicMock:
    store = MagicMock()
    store.all_indexes.return_value = [vector_index(name) for name in index_names]
    return store

def batch_client(*index_names, batch_writes_enabled=True, batch_write_size=2) -> VectorBatchClient:
    return VectorBatchClient(
        vector_store=vector_store(*index_names),
        batch_writes_enabled=batch_writes_enabled,
        batch_write_size=batch_write_size,
    )

class TestVectorBatchClientInitialization:
    """Tests for VectorBatchClient initialization."""

    def test_wraps_every_index_the_store_reports(self):
        """Verify each of the store's indexes gets a batch wrapper, keyed by name."""
        client = batch_client('chunk', 'statement')

        assert set(client.indexes) == {'chunk', 'statement'}
        assert all(isinstance(index, BatchVectorIndex) for index in client.indexes.values())

    def test_starts_with_nothing_buffered(self):
        """Verify no nodes are deferred before anything is written."""
        client = batch_client('chunk')

        assert client.all_nodes == []
        assert client.indexes['chunk'].nodes == []

class TestGetIndex:
    """Tests for index lookup."""

    @pytest.mark.parametrize('index_name', ALL_EMBEDDING_INDEXES)
    def test_known_index_names_are_accepted(self, index_name):
        """Verify every documented index name resolves to something usable."""
        client = batch_client(*ALL_EMBEDDING_INDEXES)

        assert client.get_index(index_name) is client.indexes[index_name]

    def test_unknown_index_name_raises_listing_the_valid_ones(self):
        """Verify a typo says what was expected."""
        client = batch_client('chunk')

        with pytest.raises(ValueError) as excinfo:
            client.get_index('chunks')

        message = str(excinfo.value)
        assert 'chunks' in message
        for index_name in ALL_EMBEDDING_INDEXES:
            assert index_name in message

    def test_index_the_store_does_not_have_falls_back_to_a_dummy(self):
        """Verify a valid but absent index writes nowhere rather than failing.

        A store configured with only some of the indexes should not make a build
        that touches the others crash - the writes are simply discarded.
        """
        client = batch_client('chunk')

        assert isinstance(client.get_index('topic'), DummyVectorIndex)

    def test_batch_writes_disabled_returns_the_underlying_index(self):
        """Verify the wrapper is bypassed when batching is off."""
        client = batch_client('chunk', batch_writes_enabled=False)

        assert client.get_index('chunk') is client.indexes['chunk'].index

class TestBatchVectorIndex:
    """Tests for the per-index batch wrapper."""

    def test_add_embeddings_defers_rather_than_writing(self):
        """Verify nothing reaches the index until the batch is applied."""
        client = batch_client('chunk', batch_write_size=2)
        index = client.get_index('chunk')

        index.add_embeddings(['n1', 'n2'])

        client.indexes['chunk'].index.add_embeddings.assert_not_called()
        assert client.indexes['chunk'].nodes == ['n1', 'n2']

    def test_write_embeddings_chunks_by_batch_write_size(self):
        """Verify the buffer is written in `batch_write_size` slices, in order."""
        client = batch_client('chunk', batch_write_size=2)
        client.get_index('chunk').add_embeddings(['n1', 'n2', 'n3', 'n4', 'n5'])

        client.apply_batch_operations()

        underlying = client.indexes['chunk'].index
        assert [call.args[0] for call in underlying.add_embeddings.call_args_list] == [
            ['n1', 'n2'], ['n3', 'n4'], ['n5']
        ]

    def test_empty_buffer_writes_nothing(self):
        """Verify applying an empty batch does not call the index at all."""
        client = batch_client('chunk')

        client.apply_batch_operations()

        client.indexes['chunk'].index.add_embeddings.assert_not_called()

class TestAllowYield:
    """Tests for node yielding under batching."""

    def test_batching_defers_the_node(self):
        """Verify a node is held back and returned by `apply_batch_operations`."""
        client = batch_client('chunk')

        assert client.allow_yield('n1') is False
        assert client.allow_yield('n2') is False
        assert client.apply_batch_operations() == ['n1', 'n2']

    def test_no_batching_yields_immediately(self):
        """Verify nothing is held back when batching is off."""
        client = batch_client('chunk', batch_writes_enabled=False)

        assert client.allow_yield('n1') is True
        assert client.all_nodes == []

class TestContextManager:
    """Tests for use as a context manager."""

    def test_enter_returns_the_client_and_exit_does_not_apply(self):
        """Verify leaving the block does not flush.

        `__exit__` deliberately does nothing: the caller decides when to apply,
        because it needs the returned nodes.
        """
        client = batch_client('chunk')
        client.get_index('chunk').add_embeddings(['n1'])

        with client as entered:
            assert entered is client

        client.indexes['chunk'].index.add_embeddings.assert_not_called()
