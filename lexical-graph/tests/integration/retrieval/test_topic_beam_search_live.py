# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks for TopicBeamSearch's entity-overlap pre-expansion cap.

https://github.com/awslabs/graphrag-toolkit/issues/704: the entity-overlap
branch's `max_entity_neighbors` only trimmed the *result* of the entity ->
fact -> statement -> topic re-expansion, not the entity set fed into it, so a
densely-connected corpus could blow a handful of input rows up to hundreds of
thousands of output rows per topic. `max_entities_per_topic` caps `entities`
itself, before the `UNWIND` that fans each one out.

A mocked store can assert the generated Cypher contains the right slice text
(see test_topic_beam_search.py), but not that the slice actually constrains
what comes back from a real query engine — which is the only thing that
matters for the fix. This test seeds a small real graph where every entity
leads to its own distinct neighbour topic, so the cap's effect is directly
observable in the returned neighbour set.

Skipped unless a graph is configured. To run locally against Neo4j:

    finch run -d --name topic-beam-search-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 neo4j:5
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/retrieval/test_topic_beam_search_live.py
    finch stop topic-beam-search-test && finch rm topic-beam-search-test

Or against Neptune Analytics, with credentials for the account that holds it:

    NEPTUNE_GRAPH_TEST_ID=g-abc123 AWS_REGION=us-west-2 \\
        pytest tests/integration/retrieval/test_topic_beam_search_live.py

Each run uses its own tenant, so every node it writes carries a tenant-suffixed
label and is deleted afterwards. Nothing else in the graph is touched.
"""

import os
import uuid

import pytest

from unittest.mock import MagicMock

from graphrag_toolkit.lexical_graph.retrieval.retrievers.topic_beam_search import TopicBeamSearch
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


@pytest.fixture(params=list(GRAPHS.values()), ids=list(GRAPHS.keys()))
def graph(request):
    """An empty per-tenant view of a real graph, emptied again afterwards."""
    tenant = TenantId(f't{uuid.uuid4().hex[:8]}')
    with GraphStoreFactory.for_graph_store(request.param) as base_store:
        store = MultiTenantGraphStore.wrap(base_store, tenant)
        try:
            yield store
        finally:
            for label in ('__Topic__', '__Statement__', '__Fact__', '__Entity__'):
                store.execute_query(f'MATCH (n:`{label}`) DETACH DELETE n')


def _merge_node(store, label, id_name, id_value):
    # node_id() on a bare (undotted) name is the right-hand side of an id
    # assignment at creation time: a regular property on Neo4j, the `~id`
    # pseudo-property on Neptune. Using it here is what makes the ids chosen
    # below resolve correctly later through node_id('var.idName') in WHERE
    # clauses (a property read on Neo4j, id(var) on Neptune).
    store.execute_query(
        f'MERGE (n:`{label}` {{{store.node_id(id_name)}: $id}})',
        {'id': id_value},
    )


def _link(store, from_label, from_id_name, from_id, rel, to_label, to_id_name, to_id):
    store.execute_query(
        f'MATCH (a:`{from_label}`), (b:`{to_label}`) '
        f'WHERE {store.node_id(f"a.{from_id_name}")} = $from_id '
        f'AND {store.node_id(f"b.{to_id_name}")} = $to_id '
        f'MERGE (a)-[:`{rel}`]->(b)',
        {'from_id': from_id, 'to_id': to_id},
    )


def seed_topic_with_entity_fanout(store, topic_id, entity_count):
    """
    Build `topic_id` with `entity_count` distinct entities reachable through
    their own statements/facts, each entity ALSO reachable from exactly one
    other, distinct topic. The entity-overlap branch can reach exactly
    `entity_count` neighbour topics this way, one per entity, with no overlap
    — so capping which entities get expanded directly caps which neighbour
    topics are reachable.

    Returns the neighbour topic ids, one per entity.
    """
    _merge_node(store, '__Topic__', 'topicId', topic_id)
    neighbour_ids = []

    for i in range(entity_count):
        entity_id = f'{topic_id}-e{i}'
        own_statement, own_fact = f'{topic_id}-s{i}', f'{topic_id}-f{i}'
        neighbour_topic = f'{topic_id}-n{i}'
        neighbour_statement, neighbour_fact = f'{topic_id}-ns{i}', f'{topic_id}-nf{i}'
        neighbour_ids.append(neighbour_topic)

        _merge_node(store, '__Entity__', 'entityId', entity_id)
        _merge_node(store, '__Statement__', 'statementId', own_statement)
        _merge_node(store, '__Fact__', 'factId', own_fact)
        _merge_node(store, '__Topic__', 'topicId', neighbour_topic)
        _merge_node(store, '__Statement__', 'statementId', neighbour_statement)
        _merge_node(store, '__Fact__', 'factId', neighbour_fact)

        # entity -> own fact -> own statement -> seed topic
        _link(store, '__Statement__', 'statementId', own_statement, '__BELONGS_TO__', '__Topic__', 'topicId', topic_id)
        _link(store, '__Fact__', 'factId', own_fact, '__SUPPORTS__', '__Statement__', 'statementId', own_statement)
        _link(store, '__Entity__', 'entityId', entity_id, '__SUBJECT__', '__Fact__', 'factId', own_fact)

        # entity -> its own distinct neighbour fact -> statement -> neighbour topic
        _link(store, '__Statement__', 'statementId', neighbour_statement, '__BELONGS_TO__', '__Topic__', 'topicId', neighbour_topic)
        _link(store, '__Fact__', 'factId', neighbour_fact, '__SUPPORTS__', '__Statement__', 'statementId', neighbour_statement)
        _link(store, '__Entity__', 'entityId', entity_id, '__SUBJECT__', '__Fact__', 'factId', neighbour_fact)

    return neighbour_ids


def topic_beam_search(store, **kwargs):
    retriever = TopicBeamSearch(graph_store=store, vector_store=MagicMock(), **kwargs)
    retriever.use_entity_neighbors = True
    return retriever


class TestTheCapBoundsEntityOverlapFanoutOnARealGraph:

    def test_uncapped_reaches_every_entitys_neighbour_topic(self, graph):
        neighbour_ids = seed_topic_with_entity_fanout(graph, 'A', entity_count=5)
        retriever = topic_beam_search(graph, max_entities_per_topic=None, max_entity_neighbors=100)

        result = retriever.get_neighbors_batch(['A'])

        assert set(result['A']) == set(neighbour_ids)

    def test_the_cap_bounds_which_neighbour_topics_are_reached(self, graph):
        seed_topic_with_entity_fanout(graph, 'A', entity_count=5)
        retriever = topic_beam_search(graph, max_entities_per_topic=2, max_entity_neighbors=100)

        result = retriever.get_neighbors_batch(['A'])

        # Without the cap, all 5 neighbour topics are reachable (previous
        # test). Capped to 2 entities per topic, at most 2 of them can be —
        # each entity in this fixture leads to exactly one neighbour topic
        # and none are shared, so the two counts are exactly equal.
        assert len(result['A']) == 2

    def test_zero_cap_collects_no_entities_and_reaches_no_neighbour_topic(self, graph):
        seed_topic_with_entity_fanout(graph, 'A', entity_count=5)
        retriever = topic_beam_search(graph, max_entities_per_topic=0, max_entity_neighbors=100)

        result = retriever.get_neighbors_batch(['A'])

        assert result.get('A', []) == []
