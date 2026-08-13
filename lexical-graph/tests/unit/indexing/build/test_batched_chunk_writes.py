# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import MagicMock, call, patch

from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import GraphBatchClient
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore


def _batch_client(batch_writes_enabled=True):
    graph_client = MagicMock()
    client = GraphBatchClient(
        graph_client, batch_writes_enabled=batch_writes_enabled, batch_write_size=100
    )
    return client, graph_client


class TestBufferChunkWrite(unittest.TestCase):

    def test_batch_mode_buffers_instead_of_writing(self):
        client, _ = _batch_client()
        store = MagicMock()

        client.buffer_chunk_write(store, 'c1', 'text one')
        client.buffer_chunk_write(store, 'c2', 'text two')

        store.put.assert_not_called()
        store.put_batch.assert_not_called()
        self.assertEqual(client.chunk_writes[store], {'c1': 'text one', 'c2': 'text two'})

    def test_non_batch_mode_writes_immediately(self):
        client, _ = _batch_client(batch_writes_enabled=False)
        store = MagicMock()

        client.buffer_chunk_write(store, 'c1', 'text one')

        store.put.assert_called_once_with('c1', 'text one')
        self.assertEqual(client.chunk_writes, {})

    def test_apply_flushes_buffered_chunks_once_and_clears(self):
        client, _ = _batch_client()
        store = MagicMock()

        client.buffer_chunk_write(store, 'c1', 'one')
        client.buffer_chunk_write(store, 'c2', 'two')
        client.apply_batch_operations()

        store.put_batch.assert_called_once_with({'c1': 'one', 'c2': 'two'})
        self.assertEqual(client.chunk_writes, {})

    def test_in_graph_store_flush_queues_into_the_same_batch(self):
        """The design property the wiring depends on: an in-graph store's
        put_batch at flush time is a graph query through this client, so it has
        to land in self.batches before the batch loop reads them - otherwise the
        chunk text silently never reaches the graph."""
        client, graph_client = _batch_client()
        store = InGraphChunkStore(graph_client=client)

        client.buffer_chunk_write(store, 'c1', 'one')
        client.buffer_chunk_write(store, 'c2', 'two')
        client.apply_batch_operations()

        executed = [
            (args[0][0], args[0][1])
            for args in graph_client.execute_query_with_retry.call_args_list
        ]
        chunk_queries = [(q, p) for q, p in executed if 'chunk.value' in q]
        self.assertEqual(len(chunk_queries), 1)
        written = {p['chunk_id']: p['text'] for p in chunk_queries[0][1]['params']}
        self.assertEqual(written, {'c1': 'one', 'c2': 'two'})

    def test_failed_external_flush_aborts_before_graph_writes(self):
        """A store whose put_batch raises (an S3 outage, say) must stop the
        flush while the graph is untouched. Text without a node is re-written
        on retry; a node without its text is a missing chunk at query time."""
        client, graph_client = _batch_client()
        store = MagicMock()
        store.put_batch.side_effect = RuntimeError('S3 unavailable')

        client.execute_query_with_retry('MERGE (n) // graph write', {'params': [{'x': 1}]})
        client.buffer_chunk_write(store, 'c1', 'one')

        with self.assertRaises(RuntimeError):
            client.apply_batch_operations()

        graph_client.execute_query_with_retry.assert_not_called()


class TestBuilderUsesBuffer(unittest.TestCase):

    def _chunk_node(self):
        node = MagicMock()
        node.text = 'chunk text'
        node.metadata = {'chunk': {'chunkId': 'c1', 'metadata': {}}}
        node.relationships = {}
        return node

    def test_build_buffers_through_a_batch_client(self):
        from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder

        builder = ChunkGraphBuilder()
        client = MagicMock(spec=GraphBatchClient)
        store = MagicMock()

        with patch.object(builder, '_chunk_store_for', return_value=store):
            builder.build(self._chunk_node(), client)

        client.buffer_chunk_write.assert_called_once_with(store, 'c1', 'chunk text')
        store.put.assert_not_called()

    def test_build_writes_immediately_against_a_plain_graph_store(self):
        from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
        from graphrag_toolkit.lexical_graph.storage.graph import GraphStore

        builder = ChunkGraphBuilder()
        client = MagicMock(spec=GraphStore)
        store = MagicMock()

        with patch.object(builder, '_chunk_store_for', return_value=store):
            builder.build(self._chunk_node(), client)

        store.put.assert_called_once_with('c1', 'chunk text')


if __name__ == '__main__':
    unittest.main()
