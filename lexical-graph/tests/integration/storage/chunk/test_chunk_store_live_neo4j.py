# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live check of ChunkStore write/read against a real Neo4j instance.

Mocked unit tests assert on the shape of the query string, which can't
catch a query that is well-formed but matches the wrong thing. A store
whose put() output its own get_batch() can't see is exactly that failure,
and it only shows up against a real graph.

Skipped unless NEO4J_TEST_URI is set. To run locally:

    finch run -d --name chunk-store-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 neo4j:5
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/storage/chunk/test_chunk_store_live_neo4j.py
    finch stop chunk-store-test && finch rm chunk-store-test
"""

import os

import pytest

from graphrag_toolkit.lexical_graph.storage.chunk_store_factory import ChunkStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory

NEO4J_TEST_URI = os.environ.get('NEO4J_TEST_URI')

pytestmark = pytest.mark.skipif(
    not NEO4J_TEST_URI,
    reason='set NEO4J_TEST_URI to a running Neo4j instance to run this live test',
)


@pytest.fixture
def graph_client():
    client = GraphStoreFactory.for_graph_store(NEO4J_TEST_URI)
    client.execute_query('MATCH (n) DETACH DELETE n', {})
    yield client
    client.execute_query('MATCH (n) DETACH DELETE n', {})


@pytest.fixture
def chunk_store(graph_client):
    return ChunkStoreFactory.for_chunk_store(graph_store=graph_client)


class TestChunkStoreLiveRoundTrip:

    def test_put_then_get_returns_the_text(self, chunk_store):
        chunk_store.put('chunk-live-1', 'hello from a live neo4j chunk')

        assert chunk_store.get('chunk-live-1') == 'hello from a live neo4j chunk'

    def test_put_then_get_batch_returns_the_text(self, chunk_store):
        chunk_store.put('chunk-live-1', 'text one')
        chunk_store.put('chunk-live-2', 'text two')

        assert chunk_store.get_batch(['chunk-live-1', 'chunk-live-2']) == {
            'chunk-live-1': 'text one',
            'chunk-live-2': 'text two',
        }

    def test_put_overwrites_existing_text(self, chunk_store):
        chunk_store.put('chunk-live-1', 'first text')
        chunk_store.put('chunk-live-1', 'second text')

        assert chunk_store.get('chunk-live-1') == 'second text'

    def test_get_returns_none_for_unwritten_chunk(self, chunk_store):
        assert chunk_store.get('chunk-never-written') is None

    def test_get_batch_omits_unwritten_chunks(self, chunk_store):
        chunk_store.put('chunk-live-1', 'text one')

        assert chunk_store.get_batch(['chunk-live-1', 'chunk-never-written']) == {
            'chunk-live-1': 'text one',
        }
