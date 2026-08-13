# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from typing import Dict, List
from unittest.mock import MagicMock, patch

from graphrag_toolkit.lexical_graph.storage.chunk.chunk_store import ChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk.in_graph_chunk_store import InGraphChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store import S3ChunkStore


class RecordingChunkStore(ChunkStore):
    """A backend that implements only put(), to exercise the default put_batch."""

    def __init__(self):
        self.written = []

    def put(self, chunk_id: str, text: str) -> None:
        self.written.append((chunk_id, text))

    def get_batch(self, chunk_ids: List[str]) -> Dict[str, str]:
        return {}


class TestDefaultPutBatch(unittest.TestCase):

    def test_default_writes_each_chunk(self):
        """A store predating put_batch must keep working through the default."""
        store = RecordingChunkStore()

        store.put_batch({'c1': 'one', 'c2': 'two'})

        self.assertEqual(sorted(store.written), [('c1', 'one'), ('c2', 'two')])

    def test_default_handles_empty_batch(self):
        store = RecordingChunkStore()

        store.put_batch({})

        self.assertEqual(store.written, [])


class TestInGraphPutBatch(unittest.TestCase):

    def setUp(self):
        self.graph_client = MagicMock()
        self.graph_client.node_id.side_effect = lambda name: name
        self.store = InGraphChunkStore(self.graph_client)

    def test_batch_is_written_in_one_query(self):
        """The point of the override: one round trip, not one per chunk."""
        self.store.put_batch({'c1': 'one', 'c2': 'two', 'c3': 'three'})

        self.graph_client.execute_query_with_retry.assert_called_once()
        params = self.graph_client.execute_query_with_retry.call_args[0][1]
        self.assertEqual(len(params['params']), 3)

    def test_batch_carries_every_chunk(self):
        self.store.put_batch({'c1': 'one', 'c2': 'two'})

        params = self.graph_client.execute_query_with_retry.call_args[0][1]
        written = {p['chunk_id']: p['text'] for p in params['params']}
        self.assertEqual(written, {'c1': 'one', 'c2': 'two'})

    def test_empty_batch_issues_no_query(self):
        self.store.put_batch({})

        self.graph_client.execute_query_with_retry.assert_not_called()

    def test_single_put_goes_through_the_batch_path(self):
        self.store.put('c1', 'one')

        params = self.graph_client.execute_query_with_retry.call_args[0][1]
        self.assertEqual(params['params'], [{'chunk_id': 'c1', 'text': 'one'}])

    def test_empty_text_is_written_not_skipped(self):
        """Empty text is a value, not a missing chunk."""
        self.store.put_batch({'c1': ''})

        params = self.graph_client.execute_query_with_retry.call_args[0][1]
        self.assertEqual(params['params'], [{'chunk_id': 'c1', 'text': ''}])


class TestS3PutBatch(unittest.TestCase):

    def setUp(self):
        self.store = S3ChunkStore(bucket_name='test-bucket', prefix='chunks')

    def test_every_chunk_is_written(self):
        s3 = MagicMock()

        with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
            config.s3 = s3
            config.extraction_num_threads_per_worker = 4
            self.store.put_batch({'c1': 'one', 'c2': 'two', 'c3': 'three'})

        keys = sorted(call.kwargs['Key'] for call in s3.put_object.call_args_list)
        self.assertEqual(keys, ['chunks/c1.txt', 'chunks/c2.txt', 'chunks/c3.txt'])

    def test_a_failed_write_propagates(self):
        """
        A partial batch would leave text in S3 with no matching graph node and
        no record of which chunks were missed.
        """
        s3 = MagicMock()
        s3.put_object.side_effect = [None, RuntimeError('boom'), None]

        with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
            config.s3 = s3
            config.extraction_num_threads_per_worker = 1

            with self.assertRaises(RuntimeError):
                self.store.put_batch({'c1': 'one', 'c2': 'two', 'c3': 'three'})

    def test_empty_batch_writes_nothing(self):
        s3 = MagicMock()

        with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
            config.s3 = s3
            self.store.put_batch({})

        s3.put_object.assert_not_called()


class TestS3WorkerSizing(unittest.TestCase):
    """
    get_batch also runs on the retrieval path, where the extraction thread
    setting has no particular meaning, and a two-chunk read should not spin up
    thirty-two threads.
    """

    def test_workers_never_exceed_the_batch_size(self):
        store = S3ChunkStore(bucket_name='test-bucket')

        with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
            config.extraction_num_threads_per_worker = 32

            self.assertEqual(store._num_workers(2), 2)
            self.assertEqual(store._num_workers(100), 32)

    def test_explicit_num_threads_overrides_the_extraction_setting(self):
        store = S3ChunkStore(bucket_name='test-bucket', num_threads=8)

        with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
            config.extraction_num_threads_per_worker = 32

            self.assertEqual(store._num_workers(100), 8)

    def test_workers_never_drop_below_one(self):
        store = S3ChunkStore(bucket_name='test-bucket')

        with patch('graphrag_toolkit.lexical_graph.storage.chunk.s3_chunk_store.GraphRAGConfig') as config:
            config.extraction_num_threads_per_worker = 32

            self.assertEqual(store._num_workers(0), 1)


class TestS3BucketValidation(unittest.TestCase):

    def test_missing_bucket_is_rejected_at_construction(self):
        with self.assertRaises(ValueError):
            S3ChunkStore(bucket_name=None)

    def test_empty_bucket_is_rejected_at_construction(self):
        with self.assertRaises(ValueError):
            S3ChunkStore(bucket_name='')


if __name__ == '__main__':
    unittest.main()
