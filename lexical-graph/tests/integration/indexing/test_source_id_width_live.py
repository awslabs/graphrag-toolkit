# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks for source id width resolution against a real graph.

Mocked tests can't catch a Cypher clause a store rejects, a property that comes
back as text rather than a number, or an id column that is `~id` on one store
and a property on another. The width is read and written through all three of
those, so it needs a real graph.

Skipped unless a graph is configured. To run locally against Neo4j:

    finch run -d --name source-id-width-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 neo4j:5
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/indexing/test_source_id_width_live.py
    finch stop source-id-width-test && finch rm source-id-width-test

Or against Neptune Analytics, with credentials for the account that holds it:

    NEPTUNE_GRAPH_TEST_ID=g-abc123 AWS_REGION=us-west-2 \\
        pytest tests/integration/indexing/test_source_id_width_live.py

Each run uses its own tenant, so every node it writes carries a tenant-suffixed
label and is deleted afterwards. Nothing else in the graph is touched.
"""

import os
import uuid

import pytest

from llama_index.core.schema import NodeRelationship, RelatedNodeInfo, TextNode

from graphrag_toolkit.lexical_graph.config import SourceIdWidth
from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
from graphrag_toolkit.lexical_graph.indexing.source_id_width import (
    SourceIdWidthGuard,
    SourceIdWidthMismatchError,
    graph_source_id_width,
    record_graph_source_id_width,
    recorded_source_id_width,
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

LEGACY_ID = 'aws::5eb63bbb:d41d'
FULL_ID = 'aws::5eb63bbbe01eeed093cb22bb8f5acdc3:d41d'


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
        # The wrapper rewrites these to the tenant's own labels, so this deletes
        # only what the test wrote.
        for label in ('__SYS_Config__', '__Source__'):
            store.execute_query(f'MATCH (n:`{label}`) DETACH DELETE n')


def write_source(store, source_id):
    store.execute_query(
        f'MERGE (s:`__Source__`{{{store.node_id("sourceId")}: $sourceId}})',
        {'sourceId': source_id},
    )


def source_document(source_id):
    node = TextNode(text='chunk')
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id=source_id)
    return SourceDocument(nodes=[node])


class TestGraphWidth:

    def test_an_empty_graph_has_no_width(self, graph):
        store, _ = graph

        assert graph_source_id_width(store) is None

    def test_the_width_is_read_back_from_a_stored_source_id(self, graph):
        store, _ = graph
        write_source(store, LEGACY_ID)

        assert graph_source_id_width(store) is SourceIdWidth.LEGACY

    def test_a_graph_already_holding_two_widths_raises(self, graph):
        store, _ = graph
        write_source(store, LEGACY_ID)
        write_source(store, FULL_ID)

        with pytest.raises(SourceIdWidthMismatchError):
            graph_source_id_width(store)

    def test_a_recorded_width_is_read_back(self, graph):
        store, tenant = graph

        record_graph_source_id_width(store, tenant, SourceIdWidth.FULL)

        assert graph_source_id_width(store) is SourceIdWidth.FULL

    def test_recording_the_same_width_again_is_accepted(self, graph):
        store, tenant = graph
        record_graph_source_id_width(store, tenant, SourceIdWidth.FULL)

        record_graph_source_id_width(store, tenant, SourceIdWidth.FULL)

        assert graph_source_id_width(store) is SourceIdWidth.FULL

    def test_recording_another_width_raises(self, graph):
        store, tenant = graph
        record_graph_source_id_width(store, tenant, SourceIdWidth.LEGACY)

        with pytest.raises(SourceIdWidthMismatchError):
            record_graph_source_id_width(store, tenant, SourceIdWidth.FULL)

    def test_the_record_wins_over_stored_ids(self, graph):
        store, tenant = graph
        write_source(store, LEGACY_ID)
        store.execute_query(
            f'MERGE (c:`__SYS_Config__`{{{store.node_id("sysConfigId")}: $id}}) '
            'ON CREATE SET c.sourceIdWidth = 32',
            {'id': tenant.format_id('sys_config', 'source_id_width')},
        )

        assert graph_source_id_width(store) is SourceIdWidth.FULL


class TestGuard:

    def test_documents_at_the_graph_width_pass_through(self, graph):
        store, tenant = graph
        record_graph_source_id_width(store, tenant, SourceIdWidth.LEGACY)
        guard = SourceIdWidthGuard(graph_store=store, tenant_id=tenant)

        assert len(guard([source_document(LEGACY_ID)])) == 1

    def test_documents_at_another_width_stop_the_build(self, graph):
        store, tenant = graph
        record_graph_source_id_width(store, tenant, SourceIdWidth.LEGACY)
        guard = SourceIdWidthGuard(graph_store=store, tenant_id=tenant)

        with pytest.raises(SourceIdWidthMismatchError):
            guard([source_document(FULL_ID)])

    def test_the_first_document_records_the_width(self, graph):
        store, tenant = graph
        guard = SourceIdWidthGuard(graph_store=store, tenant_id=tenant)

        guard([source_document(FULL_ID)])

        assert graph_source_id_width(store) is SourceIdWidth.FULL


class TestASampledWidthIsRecorded:
    """
    A collection written before the record existed is sampled once and recorded,
    so the sampling window closes on the next run rather than staying open for
    the life of the collection.
    """

    def test_the_guard_records_a_width_it_sampled(self, graph):
        store, tenant = graph
        write_source(store, LEGACY_ID)
        assert recorded_source_id_width(store) is None

        list(SourceIdWidthGuard(graph_store=store, tenant_id=tenant)
             ([source_document(LEGACY_ID)]))

        assert recorded_source_id_width(store) is SourceIdWidth.LEGACY

    def test_a_second_run_reads_the_record_rather_than_the_sample(self, graph):
        store, tenant = graph
        write_source(store, LEGACY_ID)
        guard = SourceIdWidthGuard(graph_store=store, tenant_id=tenant)
        list(guard([source_document(LEGACY_ID)]))

        # Sources gone, record kept: the width must still resolve.
        store.execute_query('MATCH (n:`__Source__`) DETACH DELETE n')

        assert graph_source_id_width(store) is SourceIdWidth.LEGACY


class TestTheGuardHonoursAnExplicitWidth:

    def test_documents_contradicting_the_setting_stop_an_empty_collection(self, graph):
        store, tenant = graph
        guard = SourceIdWidthGuard(graph_store=store, tenant_id=tenant,
                                   configured=SourceIdWidth.FULL)

        with pytest.raises(SourceIdWidthMismatchError):
            list(guard([source_document(LEGACY_ID)]))

        assert recorded_source_id_width(store) is None
