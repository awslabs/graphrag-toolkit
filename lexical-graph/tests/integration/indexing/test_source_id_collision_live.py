# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks for source id collision detection against a real graph.

Two Cypher pieces need a store to prove them: the source write keeps the first
document hash on a match (`coalesce` in ON MATCH SET), and the guard's lookup
matches ids through `node_id()`, which is a property on Neo4j and `~id` on
Neptune. Mocked stores match queries by substring and cannot catch either.

Skipped unless a graph is configured. To run locally against Neo4j:

    finch run -d --name source-id-collision-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 neo4j:5
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/indexing/test_source_id_collision_live.py
    finch stop source-id-collision-test && finch rm source-id-collision-test

Or against Neptune Analytics, with credentials for the account that holds it:

    NEPTUNE_GRAPH_TEST_ID=g-abc123 AWS_REGION=us-west-2 \\
        pytest tests/integration/indexing/test_source_id_collision_live.py

Each run uses its own tenant, so every node it writes carries a tenant-suffixed
label and is deleted afterwards. Nothing else in the graph is touched.
"""

import os
import uuid

import pytest

from llama_index.core.schema import Document, NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.source_id_collision import (
    DOCUMENT_HASH_PROPERTY,
    SourceIdCollisionError,
    SourceIdCollisionGuard,
)
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.graph_store_factory import GraphStoreFactory
from graphrag_toolkit.lexical_graph.tenant_id import TenantId

NEO4J_TEST_URI = os.environ.get('NEO4J_TEST_URI')
NEPTUNE_GRAPH_TEST_ID = os.environ.get('NEPTUNE_GRAPH_TEST_ID')

GRAPHS = {}
if NEO4J_TEST_URI:
    GRAPHS['neo4j'] = NEO4J_TEST_URI
if NEPTUNE_GRAPH_TEST_ID:
    GRAPHS['neptune-graph'] = f'neptune-graph://{NEPTUNE_GRAPH_TEST_ID}'

pytestmark = pytest.mark.skipif(
    not GRAPHS,
    reason='set NEO4J_TEST_URI or NEPTUNE_GRAPH_TEST_ID to run these live tests',
)

# md5(TEXT_A) and md5(TEXT_B) share their first eight characters, so at the
# legacy width these two documents get one source id.
TEXT_A = 'document 27347 body text'
TEXT_B = 'document 30059 body text'
COLLIDING_SOURCE_ID = 'aws::a4439cdb:d41d'


@pytest.fixture(params=list(GRAPHS.values()), ids=list(GRAPHS.keys()))
def graph(request):
    """An empty per-tenant view of a real graph, emptied again afterwards."""
    tenant = TenantId(f't{uuid.uuid4().hex[:8]}')
    store = MultiTenantGraphStore.wrap(
        GraphStoreFactory.for_graph_store(request.param), tenant
    )
    try:
        yield store, tenant
    finally:
        store.execute_query('MATCH (n:`__Source__`) DETACH DELETE n')


def source_node(text, file_path, source_id=COLLIDING_SOURCE_ID):
    """The source node SourceGraphBuilder writes, carrying the document's hash."""
    doc = Document(text=text, metadata={'file_path': file_path})
    node = TextNode(text='')
    node.metadata = {'source': {
        'sourceId': source_id,
        'metadata': {'file_path': file_path},
        DOCUMENT_HASH_PROPERTY: doc.hash,
    }}
    return node, doc.hash


def source_document(text, file_path, source_id=COLLIDING_SOURCE_ID):
    doc = Document(text=text, metadata={'file_path': file_path})
    chunk = TextNode(text=text)
    chunk.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(
        node_id=source_id, metadata=dict(doc.metadata), hash=doc.hash
    )
    return SourceDocument(nodes=[chunk])


def stored_hash(store, source_id):
    rows = store.execute_query(
        f'MATCH (s:`__Source__`) WHERE {store.node_id("s.sourceId")} = $id '
        f'RETURN s.{DOCUMENT_HASH_PROPERTY} AS h',
        {'id': source_id},
    )
    return rows[0]['h'] if rows else None


class TestTheWriteKeepsTheFirstHash:

    def test_the_first_write_records_the_hash(self, graph):
        store, _ = graph
        node, hash_a = source_node(TEXT_A, 'a.txt')

        SourceGraphBuilder().build(node, store)

        assert stored_hash(store, COLLIDING_SOURCE_ID) == hash_a

    def test_a_second_document_does_not_overwrite_it(self, graph):
        store, _ = graph
        node_a, hash_a = source_node(TEXT_A, 'a.txt')
        node_b, _ = source_node(TEXT_B, 'b.txt')
        SourceGraphBuilder().build(node_a, store)

        SourceGraphBuilder().build(node_b, store)

        assert stored_hash(store, COLLIDING_SOURCE_ID) == hash_a

    def test_a_source_written_without_a_hash_takes_the_first_one_offered(self, graph):
        store, _ = graph
        bare = TextNode(text='')
        bare.metadata = {'source': {'sourceId': COLLIDING_SOURCE_ID, 'metadata': {'file_path': 'old.txt'}}}
        SourceGraphBuilder().build(bare, store)
        assert stored_hash(store, COLLIDING_SOURCE_ID) is None
        node_a, hash_a = source_node(TEXT_A, 'a.txt')

        SourceGraphBuilder().build(node_a, store)

        assert stored_hash(store, COLLIDING_SOURCE_ID) == hash_a


class TestTheGuardReadsTheGraph:

    def test_a_different_document_already_in_the_graph_raises(self, graph):
        store, tenant = graph
        node_a, _ = source_node(TEXT_A, 'a.txt')
        SourceGraphBuilder().build(node_a, store)

        with pytest.raises(SourceIdCollisionError, match='already in the graph'):
            list(SourceIdCollisionGuard(graph_store=store, tenant_id=tenant)([source_document(TEXT_B, 'b.txt')]))

    def test_the_same_document_again_passes(self, graph):
        store, tenant = graph
        node_a, _ = source_node(TEXT_A, 'a.txt')
        SourceGraphBuilder().build(node_a, store)

        out = SourceIdCollisionGuard(graph_store=store, tenant_id=tenant)([source_document(TEXT_A, 'a.txt')])

        assert len(out) == 1

    def test_a_source_without_a_hash_passes(self, graph):
        store, tenant = graph
        bare = TextNode(text='')
        bare.metadata = {'source': {'sourceId': COLLIDING_SOURCE_ID, 'metadata': {}}}
        SourceGraphBuilder().build(bare, store)

        out = SourceIdCollisionGuard(graph_store=store, tenant_id=tenant)([source_document(TEXT_A, 'a.txt')])

        assert len(out) == 1
