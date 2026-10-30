# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for native lexical-graph SPARQL updates."""

import re

import pytest
from rdflib import Dataset, URIRef

from graphrag_toolkit.lexical_graph.storage.graph import GraphQueryOperation
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import (
    NamespaceConfig,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_updates import (
    render_update,
)


def _rows(row):
    return {'params': [row]}


def test_source_uses_explicit_id_and_metadata():
    update = render_update(
        GraphQueryOperation.UPSERT_SOURCE,
        _rows({'sourceId': 'aws::abc:def', 'url': 'https://x/y'}),
    )

    assert 'source/aws%3A%3Aabc%3Adef' in update
    assert 'https://x/y' in update
    assert 'DELETE WHERE' in update
    assert '_source_id' not in update


@pytest.mark.parametrize(('operation', 'params', 'expected'), [
    (GraphQueryOperation.UPSERT_CHUNK,
     {'chunk_id': 'c1', 'text': 'hello', 'seq': 3},
     ('chunk/c1', '"hello"')),
    (GraphQueryOperation.UPSERT_TOPIC,
     {'topic_id': 't1', 'title': 'T', 'chunk_ids': ['c1']},
     ('topic/t1', 'topicMentionedIn', 'chunk/c1')),
    (GraphQueryOperation.UPSERT_STATEMENT,
     {'statement_id': 's1', 'value': 'v', 'details': 'd'},
     ('statement/s1', '"v"', '"d"')),
    (GraphQueryOperation.UPSERT_ENTITY,
     {'e_id': 'e1', 'v': 'Alice', 'e_search_str': 'alice', 'ec': 'Person'},
     ('entity/e1', '"Alice"', '"alice"', '"Person"')),
])
def test_node_updates(operation, params, expected):
    update = render_update(operation, _rows(params))
    assert all(value in update for value in expected)


def test_chunk_metadata_preserves_underscored_keys():
    update = render_update(
        GraphQueryOperation.UPSERT_CHUNK,
        _rows({'chunk_id': 'c1', 'text': 'hello', '_custom': 'value'}),
    )

    assert '#_custom>' in update
    assert '#chunk_id>' not in update


@pytest.mark.parametrize(('operation', 'params', 'predicate'), [
    (GraphQueryOperation.LINK_CHUNK_SOURCE,
     {'chunk_id': 'c1', 'source_id': 's1'}, 'extractedFrom'),
    (GraphQueryOperation.LINK_CHUNKS,
     {'chunk_id': 'c2', 'target_id': 'c1', '_relationship_type': 'previous'},
     'chunkPrevious'),
    (GraphQueryOperation.LINK_STATEMENT_CHUNK,
     {'statement_id': 's1', 'chunk_id': 'c1'}, 'statementMentionedIn'),
    (GraphQueryOperation.LINK_STATEMENT_TOPIC,
     {'statement_id': 's1', 'topic_id': 't1'}, 'belongsTo'),
    (GraphQueryOperation.LINK_STATEMENTS,
     {'statement_id': 's2', 'prev_statement_id': 's1'}, 'statementPrevious'),
])
def test_link_updates(operation, params, predicate):
    assert predicate in render_update(operation, _rows(params))


def test_fact_is_reified_and_links_entities_from_fact():
    update = render_update(GraphQueryOperation.UPSERT_FACT, _rows({
        'statement_id': 's1',
        'fact_id': 'f1',
        'fact': 'a@x CONFIDENCE 95%',
        '_predicate': 'CONFIDENCE',
        '_subject_literal': 'a@x',
        '_object_literal': '95%',
    }))
    edge = render_update(GraphQueryOperation.LINK_FACT_ENTITY, _rows({
        'fact_id': 'f1',
        'entity_id': 'e1',
        '_relationship_type': 'subject',
    }))

    assert 'Relation>' in update and 'relation/confidence>' in update
    assert '#subject>' in update and '"a@x"' in update
    assert '#object>' in update and '"95%"' in update
    assert 'supports' in update and 'statement/s1' in update
    assert re.search(r'fact/f1>\s+<[^>]*#subject>\s+<[^>]*entity/e1>', edge)


def test_lpg_convenience_relation_is_not_duplicated_in_rdf():
    assert render_update(
        GraphQueryOperation.LINK_ENTITIES,
        _rows({'s_id': 'a', 'o_id': 'b', 'p': 'manages'}),
    ) is None


def test_graph_summary_updates_distinct_class_counters():
    update = render_update(GraphQueryOperation.UPDATE_GRAPH_SUMMARY, _rows({
        'sc_id': 'sys::Person',
        'oc_id': 'sys::Company',
        'sc': 'Person',
        'oc': 'Company',
        'p': 'MANAGES',
    }))

    assert 'SysRelation>' in update
    assert 'sysRelSubject>' in update and 'sysRelObject>' in update
    assert 'COALESCE' in update and update.count('DELETE') >= 2


def test_graph_summary_same_class_uses_increment_of_two():
    update = render_update(GraphQueryOperation.UPDATE_GRAPH_SUMMARY, _rows({
        'sc_id': 'sys::Person',
        'oc_id': 'sys::Person',
        'sc': 'Person',
        'oc': 'Person',
        'p': 'KNOWS',
    }))

    assert 'COALESCE(?c0, 0) + 2' in update


def test_domain_type_and_custom_namespace():
    namespace = NamespaceConfig(
        prefix='gt',
        schema_namespace='https://example.test/schema#',
        instance_namespace='https://example.test/data/',
    )
    update = render_update(
        GraphQueryOperation.ADD_ENTITY_TYPE,
        {'entityId': 'e1', '_classification': 'Person'},
        namespace,
    )

    assert '<https://example.test/data/entity/e1>' in update
    assert '<https://example.test/schema#Person>' in update


def test_tenant_routes_update_to_named_graph():
    update = render_update(
        GraphQueryOperation.UPSERT_ENTITY,
        _rows({'e_id': 'e1', 'v': 'Alice'}),
        tenant_id='acme',
    )
    assert 'GRAPH <https://awslabs.github.io/graphrag-toolkit/lexical/tenant/acme>' in update


@pytest.mark.parametrize('parameters', [None, {}, {'params': []}])
def test_empty_parameters_are_noop(parameters):
    assert render_update(GraphQueryOperation.UPSERT_CHUNK, parameters) is None


def test_missing_edge_parameter_is_noop():
    assert render_update(
        GraphQueryOperation.LINK_CHUNK_SOURCE,
        _rows({'chunk_id': 'c1'}),
    ) is None


def test_local_entity_rewrite_preserves_reified_fact():
    update = render_update(
        GraphQueryOperation.COPY_COMPLEMENT_RELATIONSHIPS,
        _rows({'n_id': 'resolved', 'c_id': 'local'}),
    )
    delete = render_update(
        GraphQueryOperation.DELETE_COMPLEMENT,
        _rows({'c_id': 'local'}),
    )

    assert 'DELETE' in update and 'INSERT' in update
    assert 'entity/local' in update and 'entity/resolved' in update
    assert '?subject ?predicate' in update
    assert 'entity/local' in delete and 'DELETE WHERE' in delete


@pytest.mark.filterwarnings(
    'ignore:Dataset.default_context is deprecated:DeprecationWarning'
)
def test_local_entity_rewrite_moves_fact_object_when_delete_runs_first():
    dataset = Dataset()
    tenant = 'acme'
    for operation, row in (
        (GraphQueryOperation.UPSERT_ENTITY,
         {'e_id': 'resolved', 'v': 'Resolved', 'ec': 'Entity'}),
        (GraphQueryOperation.UPSERT_ENTITY,
         {'e_id': 'local', 'v': 'Local', 'ec': '__LocalEntity__'}),
        (GraphQueryOperation.LINK_FACT_ENTITY,
         {'fact_id': 'f1', 'entity_id': 'local', '_relationship_type': 'object'}),
        (GraphQueryOperation.DELETE_COMPLEMENT, {'c_id': 'local'}),
        (GraphQueryOperation.COPY_COMPLEMENT_RELATIONSHIPS,
         {'n_id': 'resolved', 'c_id': 'local'}),
    ):
        dataset.update(render_update(operation, _rows(row), tenant_id=tenant))

    graph = dataset.graph(URIRef(
        'https://awslabs.github.io/graphrag-toolkit/lexical/tenant/acme'
    ))
    fact = URIRef('https://awslabs.github.io/graphrag-toolkit/lexical/fact/f1')
    predicate = URIRef('https://awslabs.github.io/graphrag-toolkit/lexical#object')
    resolved = URIRef('https://awslabs.github.io/graphrag-toolkit/lexical/entity/resolved')
    local = URIRef('https://awslabs.github.io/graphrag-toolkit/lexical/entity/local')

    assert (fact, predicate, resolved) in graph
    assert not list(graph.triples((None, None, local)))
    assert not list(graph.triples((local, None, None)))


def test_read_operation_cannot_be_rendered_as_update():
    with pytest.raises(NotImplementedError, match='get_facts'):
        render_update(GraphQueryOperation.GET_FACTS, {'statementIds': ['s1']})


@pytest.mark.parametrize('operation, row', [
    (GraphQueryOperation.RECORD_SOURCE_ID_WIDTH, {'configId': 'config', 'width': None}),
    (GraphQueryOperation.RECORD_SOURCE_ID_WIDTH, {'configId': None, 'width': 32}),
    (GraphQueryOperation.LINK_CHUNK_SOURCE, {'chunk_id': None, 'source_id': 's1'}),
    (GraphQueryOperation.LINK_CHUNK_SOURCE, {'chunk_id': 'c1', 'source_id': None}),
    (GraphQueryOperation.UPSERT_SOURCE, {'sourceId': None}),
    (GraphQueryOperation.UPSERT_ENTITY, {'e_id': None}),
    (GraphQueryOperation.UPSERT_FACT, {'fact_id': 'f1', 'statement_id': None}),
])
def test_null_required_values_are_rejected_before_rendering(operation, row):
    with pytest.raises(ValueError, match='must not be None'):
        render_update(operation, _rows(row))
