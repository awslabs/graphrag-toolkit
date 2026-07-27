# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for InGraphChunkStore, which preserves the current chunk.value behavior."""

from unittest.mock import Mock

from graphrag_toolkit.lexical_graph.storage.chunk import ChunkStore, InGraphChunkStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import format_id


class TestInGraphChunkStoreGet:

    def test_get_returns_value_for_known_chunk(self):
        graph_client = Mock(spec=GraphStore)
        graph_client.node_id.side_effect = format_id
        graph_client.execute_query.return_value = [
            {'result': {'chunk': {'chunkId': 'chunk-1', 'value': 'hello world'}}}
        ]

        store = InGraphChunkStore(graph_client)

        assert store.get('chunk-1') == 'hello world'

    def test_get_returns_none_for_unknown_chunk(self):
        graph_client = Mock(spec=GraphStore)
        graph_client.node_id.side_effect = format_id
        graph_client.execute_query.return_value = []

        store = InGraphChunkStore(graph_client)

        assert store.get('missing-chunk') is None


class TestInGraphChunkStoreGetBatch:

    def test_get_batch_returns_values_keyed_by_chunk_id(self):
        graph_client = Mock(spec=GraphStore)
        graph_client.node_id.side_effect = format_id
        graph_client.execute_query.return_value = [
            {'result': {'chunk': {'chunkId': 'chunk-1', 'value': 'text one'}}},
            {'result': {'chunk': {'chunkId': 'chunk-2', 'value': 'text two'}}},
        ]

        store = InGraphChunkStore(graph_client)

        assert store.get_batch(['chunk-1', 'chunk-2']) == {
            'chunk-1': 'text one',
            'chunk-2': 'text two',
        }

    def test_get_batch_omits_chunks_with_no_match(self):
        graph_client = Mock(spec=GraphStore)
        graph_client.node_id.side_effect = format_id
        graph_client.execute_query.return_value = [
            {'result': {'chunk': {'chunkId': 'chunk-1', 'value': 'text one'}}},
        ]

        store = InGraphChunkStore(graph_client)

        assert store.get_batch(['chunk-1', 'chunk-2']) == {'chunk-1': 'text one'}

    def test_get_batch_empty_input_returns_empty_dict(self):
        graph_client = Mock(spec=GraphStore)

        store = InGraphChunkStore(graph_client)

        assert store.get_batch([]) == {}
        graph_client.execute_query.assert_not_called()


class TestInGraphChunkStorePut:

    def test_put_writes_chunk_value_with_retry(self):
        graph_client = Mock(spec=GraphStore)
        graph_client.node_id.side_effect = format_id

        store = InGraphChunkStore(graph_client)
        store.put('chunk-1', 'new text')

        graph_client.execute_query_with_retry.assert_called_once()
        query, params = graph_client.execute_query_with_retry.call_args.args[:2]
        assert 'chunk.value' in query
        assert params['params'] == [{'chunk_id': 'chunk-1', 'text': 'new text'}]


class TestInGraphChunkStoreIsChunkStore:

    def test_in_graph_chunk_store_is_a_chunk_store(self):
        graph_client = Mock(spec=GraphStore)
        store = InGraphChunkStore(graph_client)

        assert isinstance(store, ChunkStore)
