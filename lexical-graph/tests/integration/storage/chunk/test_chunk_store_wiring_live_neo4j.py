# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live end-to-end check of the ChunkStore write/read wiring against a real
Neo4j instance. Mocked unit tests can't catch a malformed Cypher query or a
graph client that doesn't duck-type the way a factory expects - both bugs
this test's ancestor scratch runs actually caught during development.

Skipped unless NEO4J_TEST_URI is set. To run locally:

    finch run -d --name chunk-store-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 neo4j:5
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/storage/chunk/test_chunk_store_wiring_live_neo4j.py
    finch stop chunk-store-test && finch rm chunk-store-test
"""

import os

import pytest
from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.indexing.build.chunk_graph_builder import ChunkGraphBuilder
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_vss_provider import KeywordVSSProvider
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import (
    TraversalBasedBaseRetriever,
)
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection
from graphrag_toolkit.lexical_graph.storage.chunk_store_factory import ChunkStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory

NEO4J_TEST_URI = os.environ.get('NEO4J_TEST_URI')

pytestmark = pytest.mark.skipif(
    not NEO4J_TEST_URI,
    reason='set NEO4J_TEST_URI to a running Neo4j instance to run this live test',
)


class _ConcreteRetriever(TraversalBasedBaseRetriever):
    def get_start_node_ids(self, query_bundle):
        return []

    def do_graph_search(self, query_bundle, start_node_ids):
        return SearchResultCollection()


@pytest.fixture
def graph_client():
    client = GraphStoreFactory.for_graph_store(NEO4J_TEST_URI)
    client.execute_query('MATCH (n) DETACH DELETE n', {})
    yield client
    client.execute_query('MATCH (n) DETACH DELETE n', {})


class TestChunkStoreLiveWiring:

    def _write_chunk(self, graph_client, chunk_id, text, source_id, chunk_metadata=None):
        node = TextNode(text=text)
        node.metadata = {'chunk': {'chunkId': chunk_id, 'metadata': chunk_metadata or {}}}
        node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
        ChunkGraphBuilder().build(node, graph_client)

    def test_write_then_read_via_chunk_store(self, graph_client):
        self._write_chunk(graph_client, 'chunk-live-1', 'hello from a live neo4j chunk', 'source-live-1')

        chunk_store = ChunkStoreFactory.for_chunk_store(graph_store=graph_client)

        assert chunk_store.get('chunk-live-1') == 'hello from a live neo4j chunk'
        assert chunk_store.get_batch(['chunk-live-1']) == {'chunk-live-1': 'hello from a live neo4j chunk'}

    def test_write_then_read_via_keyword_vss_provider(self, graph_client):
        self._write_chunk(graph_client, 'chunk-live-2', 'the quick brown fox', 'source-live-2')

        provider = KeywordVSSProvider.__new__(KeywordVSSProvider)
        provider.graph_store = graph_client

        content = provider._get_chunk_content(['chunk-live-2'])

        assert content == ['the quick brown fox']

    def test_write_then_read_via_traversal_retriever_with_chunk_details(self, graph_client):
        self._write_chunk(
            graph_client, 'chunk-live-3', 'second chunk text', 'source-live-3',
            chunk_metadata={'page_number': 3},
        )

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
                'chunk_id': 'chunk-live-3',
                'topic_id': 'topic-live-3',
                'topic_value': 'Topic',
                'statement_id': 'statement-live-3',
                'statement_value': 'A statement about the chunk.',
            },
        )

        retriever = _ConcreteRetriever(
            graph_store=graph_client,
            vector_store=None,
            processor_args=ProcessorArgs(include_chunk_details=True),
            filter_config=FilterConfig(),
        )

        results = retriever.get_statements_by_topic_and_source(['statement-live-3'])
        chunk = results[0]['result']['topics'][0]['chunks'][0]

        assert chunk['value'] == 'second chunk text'
        assert 'value' not in chunk['metadata']
        assert chunk['metadata']['page_number'] == 3
