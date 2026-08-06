# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks for S3ChunkStore against a real S3 endpoint.

Mocked tests can't catch a wrong key layout, an encryption parameter S3
rejects, or a decode that only breaks on a real streaming body. The dual-read
and baseline-regression cases additionally need a graph, so they are skipped
unless Neo4j is available too.

Skipped unless S3_TEST_BUCKET is set. To run locally:

    S3_TEST_BUCKET=my-bucket \\
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/storage/chunk/test_s3_chunk_store_live.py

Objects are written under a unique prefix per run and deleted afterwards.
"""

import os
import uuid

import pytest
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import (
    TraversalBasedBaseRetriever,
)
from graphrag_toolkit.lexical_graph.storage.chunk import InGraphChunkStore, S3ChunkStore
from graphrag_toolkit.lexical_graph.storage.chunk_store_factory import ChunkStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory

S3_TEST_BUCKET = os.environ.get('S3_TEST_BUCKET')
NEO4J_TEST_URI = os.environ.get('NEO4J_TEST_URI')

pytestmark = pytest.mark.skipif(
    not S3_TEST_BUCKET,
    reason='set S3_TEST_BUCKET to a writable bucket to run this live test',
)

needs_graph = pytest.mark.skipif(
    not NEO4J_TEST_URI,
    reason='set NEO4J_TEST_URI to a running Neo4j instance for the dual-read cases',
)


class _ConcreteRetriever(TraversalBasedBaseRetriever):
    def get_start_node_ids(self, query_bundle):
        return []

    def do_graph_search(self, query_bundle, start_node_ids):
        return SearchResultCollection()


@pytest.fixture
def prefix():
    run_prefix = f'chunk-store-tests/{uuid.uuid4()}'
    yield run_prefix

    s3_client = GraphRAGConfig.s3
    pages = s3_client.get_paginator('list_objects_v2').paginate(
        Bucket=S3_TEST_BUCKET, Prefix=run_prefix
    )
    keys = [{'Key': o['Key']} for page in pages for o in page.get('Contents', [])]
    if keys:
        s3_client.delete_objects(Bucket=S3_TEST_BUCKET, Delete={'Objects': keys})


@pytest.fixture
def graph_client():
    client = GraphStoreFactory.for_graph_store(NEO4J_TEST_URI)
    client.execute_query('MATCH (n) DETACH DELETE n', {})
    yield client
    client.execute_query('MATCH (n) DETACH DELETE n', {})


def _write_chunk(graph_client, chunk_id, text, source_id):
    node = TextNode(text=text)
    node.metadata = {'chunk': {'chunkId': chunk_id, 'metadata': {}}}
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
    ChunkGraphBuilder().build(node, graph_client)


class TestS3ChunkStoreLive:

    def test_put_then_get_round_trips_through_s3(self, prefix):
        store = S3ChunkStore(bucket_name=S3_TEST_BUCKET, prefix=prefix)

        store.put('chunk-1', 'hello from a live s3 chunk')

        assert store.get('chunk-1') == 'hello from a live s3 chunk'

    def test_get_batch_returns_only_the_chunks_that_exist(self, prefix):
        store = S3ChunkStore(bucket_name=S3_TEST_BUCKET, prefix=prefix)
        store.put('chunk-1', 'first')
        store.put('chunk-2', 'second')

        result = store.get_batch(['chunk-1', 'chunk-2', 'never-written'])

        assert result == {'chunk-1': 'first', 'chunk-2': 'second'}

    def test_empty_and_unicode_text_survive_the_round_trip(self, prefix):
        store = S3ChunkStore(bucket_name=S3_TEST_BUCKET, prefix=prefix)
        store.put('empty', '')
        store.put('unicode', 'café — naïve — 日本語')

        result = store.get_batch(['empty', 'unicode'])

        assert result == {'empty': '', 'unicode': 'café — naïve — 日本語'}


@needs_graph
class TestS3ChunkStoreDualReadLive:

    def test_chunk_written_before_migration_is_read_from_the_graph(self, prefix, graph_client):
        # Pre-migration state: text is inline on the graph node, nothing in S3.
        _write_chunk(graph_client, 'pre-migration', 'text held in the graph', 'source-1')

        store = S3ChunkStore(
            bucket_name=S3_TEST_BUCKET,
            prefix=prefix,
            fallback=InGraphChunkStore(graph_client),
        )

        assert store.get_batch(['pre-migration']) == {'pre-migration': 'text held in the graph'}

    def test_batch_spanning_both_backends_is_merged(self, prefix, graph_client):
        _write_chunk(graph_client, 'old-chunk', 'from the graph', 'source-1')

        store = S3ChunkStore(
            bucket_name=S3_TEST_BUCKET,
            prefix=prefix,
            fallback=InGraphChunkStore(graph_client),
        )
        store.put('new-chunk', 'from s3')

        result = store.get_batch(['new-chunk', 'old-chunk'])

        assert result == {'new-chunk': 'from s3', 'old-chunk': 'from the graph'}


@needs_graph
class TestQueryPathMatchesInGraphBaseline:
    """The whole point of moving chunk text off the graph is that queries don't
    notice. This builds and reads the same chunk under each backend and compares
    what the query path returns."""

    def _build_and_read(self, graph_client, chunk_id):
        _write_chunk(graph_client, chunk_id, 'chunk text for the regression check', 'source-1')

        graph_client.execute_query(
            '''
            MATCH (chunk:`__Chunk__` {chunkId: $chunk_id})
            MERGE (topic:`__Topic__` {topicId: $topic_id})
            ON CREATE SET topic.value = $topic_value
            MERGE (statement:`__Statement__` {statementId: $statement_id})
            ON CREATE SET statement.value = $statement_value, statement.details = ''
            MERGE (statement)-[:`__BELONGS_TO__`]->(topic)
            MERGE (statement)-[:`__MENTIONED_IN__`]->(chunk)
            ''',
            {
                'chunk_id': chunk_id,
                'topic_id': f'topic-{chunk_id}',
                'topic_value': 'Topic',
                'statement_id': f'statement-{chunk_id}',
                'statement_value': 'A statement about the chunk.',
            },
        )

        retriever = _ConcreteRetriever(
            graph_store=graph_client,
            vector_store=None,
            processor_args=ProcessorArgs(include_chunk_details=True),
            filter_config=FilterConfig(),
        )

        results = retriever.get_statements_by_topic_and_source([f'statement-{chunk_id}'])
        return results[0]['result']['topics'][0]['chunks'][0]

    def test_s3_backend_returns_what_the_in_graph_backend_returns(self, prefix, graph_client):
        original = GraphRAGConfig.chunk_store
        try:
            GraphRAGConfig.chunk_store = None
            baseline = self._build_and_read(graph_client, 'baseline-chunk')

            GraphRAGConfig.chunk_store = f's3://{S3_TEST_BUCKET}/{prefix}'
            with_s3 = self._build_and_read(graph_client, 's3-chunk')
        finally:
            GraphRAGConfig.chunk_store = original

        assert with_s3['value'] == baseline['value'] == 'chunk text for the regression check'
        assert with_s3.keys() == baseline.keys()

    def test_s3_backend_keeps_chunk_text_off_the_graph_node(self, prefix, graph_client):
        original = GraphRAGConfig.chunk_store
        try:
            GraphRAGConfig.chunk_store = f's3://{S3_TEST_BUCKET}/{prefix}'
            _write_chunk(graph_client, 'offloaded', 'text that belongs in s3', 'source-1')
        finally:
            GraphRAGConfig.chunk_store = original

        rows = graph_client.execute_query(
            'MATCH (chunk:`__Chunk__` {chunkId: $chunk_id}) RETURN chunk.value AS value',
            {'chunk_id': 'offloaded'},
        )

        assert rows[0]['value'] is None

        store = ChunkStoreFactory.for_chunk_store(
            f's3://{S3_TEST_BUCKET}/{prefix}', graph_store=graph_client
        )
        assert store.get('offloaded') == 'text that belongs in s3'
