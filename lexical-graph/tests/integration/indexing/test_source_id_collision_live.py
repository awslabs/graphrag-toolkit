# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks for source id collision detection against a real graph.

Two Cypher pieces need a store to prove them. The source write keeps the first
source hash on a match (`coalesce` in ON MATCH SET), which is what makes reading
the hash back afterwards say who owns the id. The read back matches ids through
`node_id()`, a property on Neo4j and `~id` on Neptune, and names the stored
document through a `coalesce` over properties a node may not have. Batching changes
the answer, so it is covered here too. Mocked stores match queries by substring and
cannot catch any of it.

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

from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.config import SourceIdWidth
from graphrag_toolkit.lexical_graph.indexing.build.graph_batch_client import GraphBatchClient
from graphrag_toolkit.lexical_graph.indexing.build.source_graph_builder import SourceGraphBuilder
from graphrag_toolkit.lexical_graph.indexing.id_generator import IdGenerator
from graphrag_toolkit.lexical_graph.indexing.constants import SOURCE_HASH_PROPERTY
from graphrag_toolkit.lexical_graph.indexing.source_id_collision import (
    SourceIdClaims,
    SourceIdCollisionError,
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

# md5 of these two texts agree on their first eight characters, so at the legacy
# width the two documents get one source id and are told apart only by the hash.
TEXT_A = 'document 27347 body text'
TEXT_B = 'document 30059 body text'

_ids = IdGenerator(source_id_width=SourceIdWidth.LEGACY)
HASH_A = _ids.create_source_hash(TEXT_A, '')
HASH_B = _ids.create_source_hash(TEXT_B, '')
COLLIDING_SOURCE_ID = _ids.create_source_id(TEXT_A, '')
assert COLLIDING_SOURCE_ID == _ids.create_source_id(TEXT_B, '')
assert HASH_A != HASH_B


@pytest.fixture(params=list(GRAPHS.values()), ids=list(GRAPHS.keys()))
def graph(request):
    """An empty per-tenant view of a real graph, emptied and closed afterwards."""
    tenant = TenantId(f't{uuid.uuid4().hex[:8]}')
    with GraphStoreFactory.for_graph_store(request.param) as base_store:
        store = MultiTenantGraphStore.wrap(base_store, tenant)
        try:
            yield store
        finally:
            store.execute_query('MATCH (n:`__Source__`) DETACH DELETE n')


def source_node(source_hash, metadata=None, source_id=COLLIDING_SOURCE_ID):
    """The source node SourceGraphBuilder writes, carrying the document's hash."""
    node = TextNode(id_=source_id, text='')
    source = {'sourceId': source_id, 'metadata': metadata if metadata is not None else {}}
    if source_hash:
        source[SOURCE_HASH_PROPERTY] = source_hash
    node.metadata = {'source': source}
    return node


def write(store, node):
    SourceGraphBuilder().build(node, store)


def claims_for(*nodes):
    claims = SourceIdClaims()
    for node in nodes:
        claims.add(node)
    return claims


def stored_name(store, source_id=COLLIDING_SOURCE_ID):
    rows = store.execute_query(
        f'MATCH (s:`__Source__`) WHERE {store.node_id("s.sourceId")} = $id '
        f'RETURN s.file_path AS name',
        {'id': source_id},
    )
    return rows[0]['name'] if rows else None


def stored_hash(store, source_id=COLLIDING_SOURCE_ID):
    rows = store.execute_query(
        f'MATCH (s:`__Source__`) WHERE {store.node_id("s.sourceId")} = $id '
        f'RETURN s.{SOURCE_HASH_PROPERTY} AS h',
        {'id': source_id},
    )
    return rows[0]['h'] if rows else None


class TestTheWriteKeepsTheFirstHash:

    def test_the_first_write_records_the_hash(self, graph):
        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))

        assert stored_hash(graph) == HASH_A

    def test_a_second_document_does_not_overwrite_it(self, graph):
        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))

        write(graph, source_node(HASH_B, {'file_path': 'b.txt'}))

        assert stored_hash(graph) == HASH_A

    def test_a_source_written_without_a_hash_takes_the_first_one_offered(self, graph):
        write(graph, source_node(None, {'file_path': 'old.txt'}))
        assert stored_hash(graph) is None

        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))

        assert stored_hash(graph) == HASH_A

    def test_a_metadata_key_of_the_same_name_does_not_displace_it(self, graph):
        write(graph, source_node(HASH_A, {SOURCE_HASH_PROPERTY: 'not-the-hash'}))

        assert stored_hash(graph) == HASH_A


class TestTheOwnersMetadataSurvives:

    def test_a_second_document_writes_no_metadata(self, graph):
        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))

        write(graph, source_node(HASH_B, {'file_path': 'b.txt'}))

        assert stored_name(graph) == 'a.txt'

    def test_the_owner_can_still_update_its_own_metadata(self, graph):
        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))

        write(graph, source_node(HASH_A, {'file_path': 'renamed.txt'}))

        assert stored_name(graph) == 'renamed.txt'

    def test_a_source_that_carries_no_hash_updates_as_before(self, graph):
        write(graph, source_node(None, {'file_path': 'old.txt'}))

        write(graph, source_node(None, {'file_path': 'newer.txt'}))

        assert stored_name(graph) == 'newer.txt'


class TestBothDocumentsInOneBatchedWrite:

    def test_the_last_row_of_a_batch_wins(self, graph):
        # One UNWIND reads owner for every row before any row's SET, so the filter
        # sees no owner and the last row's hash and metadata land on the node. The
        # write is first-writer-wins per query, not within a batch.
        with GraphBatchClient(graph, batch_writes_enabled=True, batch_write_size=100) as batch:
            write(batch, source_node(HASH_A, {'file_path': 'a.txt'}))
            write(batch, source_node(HASH_B, {'file_path': 'b.txt'}))
            batch.apply_batch_operations()

        assert stored_hash(graph) == HASH_B
        assert stored_name(graph) == 'b.txt'

    def test_the_in_batch_check_raises_before_the_second_write(self, graph):
        # Which is why claims are checked against each other as they are added,
        # rather than left to the read back.
        claims = SourceIdClaims()
        with GraphBatchClient(graph, batch_writes_enabled=True, batch_write_size=100) as batch:
            first = source_node(HASH_A, {'file_path': 'a.txt'})
            claims.add(first)
            write(batch, first)

            with pytest.raises(SourceIdCollisionError, match='both in this build'):
                claims.add(source_node(HASH_B, {'file_path': 'b.txt'}))

            batch.apply_batch_operations()

        assert stored_hash(graph) == HASH_A
        assert stored_name(graph) == 'a.txt'


class TestTheCheckReadsTheHashBack:

    def test_a_different_document_already_in_the_graph_raises(self, graph):
        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))
        losing = source_node(HASH_B, {'file_path': 'b.txt'})
        write(graph, losing)

        with pytest.raises(SourceIdCollisionError) as raised:
            claims_for(losing).verify(graph)

        # The write kept A's hash, so the error names A as the document in the
        # graph and B as the one this build brought.
        assert 'b.txt' in str(raised.value)
        assert 'a.txt already in the graph' in str(raised.value)

    def test_the_same_document_again_passes(self, graph):
        node = source_node(HASH_A, {'file_path': 'a.txt'})
        write(graph, node)

        claims_for(node).verify(graph)

    def test_a_source_without_a_hash_passes(self, graph):
        write(graph, source_node(None, {'file_path': 'old.txt'}))
        node = source_node(HASH_A, {'file_path': 'a.txt'})

        claims_for(node).verify(graph)

    def test_a_stored_document_with_none_of_the_name_properties_still_raises(self, graph):
        # The name comes from a coalesce over properties the node may not carry.
        # A store that rejects a missing property, or returns something other than
        # null for one, would break the read rather than the naming.
        write(graph, source_node(HASH_A, {'author': 'bob'}))
        losing = source_node(HASH_B, {'author': 'sue'})
        write(graph, losing)

        with pytest.raises(SourceIdCollisionError, match='a document already in the graph'):
            claims_for(losing).verify(graph)

    def test_a_source_id_outside_this_build_is_left_alone(self, graph):
        write(graph, source_node(HASH_A, {'file_path': 'a.txt'}))
        other = source_node(HASH_B, {'file_path': 'b.txt'}, source_id='aws::deadbeef:d41d')
        write(graph, other)

        claims_for(other).verify(graph)
