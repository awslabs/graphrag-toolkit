# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph.storage.graph import GraphOperation
from graphrag_toolkit.lexical_graph.versioning import (
    BUILD_TIMESTAMP,
    EXTRACT_TIMESTAMP,
    VALID_FROM,
    VALID_TO,
    VERSION_INDEPENDENT_ID_FIELDS,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import (
    DEFAULT_NAMESPACE,
    LEXICAL_SCHEMA,
    NamespaceConfig,
    sparql_literal,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_queries import (
    _entity_score_rows,
    _int_or_default,
    _local_name,
    _properties_by_id,
    run_query,
)


class FakeClient:
    def __init__(self):
        self.queries = []
        self.default_graphs = []

    def query(self, sparql, default_graph=None):
        self.queries.append(sparql)
        self.default_graphs.append(default_graph)
        if 'SELECT DISTINCT ?statementId' in sparql:
            return [{
                'statementId': 'stmt-1',
                'statementValue': 'Alice manages Bob',
                'details': 'detail',
                'chunkId': 'chunk-1',
                'topicId': 'topic-1',
                'topicValue': 'People',
                'sourceId': 'source-1',
            }]
        if 'SELECT DISTINCT ?l' in sparql:
            return [{'l': 'stmt-1'}]
        if 'SELECT ?content' in sparql:
            return [{'content': 'Chunk text'}]
        if 'SELECT ?statement ?details' in sparql:
            return [{'statement': 'Alice manages Bob', 'details': 'detail'}]
        if 'SELECT ?entityId ?value ?class ?otherId' in sparql:
            return [{
                'entityId': 'entity-1',
                'value': 'Alice',
                'class': 'Person',
                'otherId': 'entity-2',
                'score': 3,
            }]
        if 'SELECT ?entityId ?value ?class' in sparql:
            return [{
                'entityId': 'entity-1',
                'value': 'Alice',
                'class': 'Person',
                'score': 2,
            }]
        if 'a lg:Source' in sparql:
            return [
                {'id': 'source-1', 'prop': f'{LEXICAL_SCHEMA}title', 'value': 'Source title'},
                {'id': 'source-1', 'prop': f'{LEXICAL_SCHEMA}{VALID_FROM}', 'value': 10},
                {'id': 'source-1', 'prop': f'{LEXICAL_SCHEMA}{VALID_TO}', 'value': 20},
                {'id': 'source-1', 'prop': f'{LEXICAL_SCHEMA}{EXTRACT_TIMESTAMP}', 'value': 11},
                {'id': 'source-1', 'prop': f'{LEXICAL_SCHEMA}{BUILD_TIMESTAMP}', 'value': 12},
                {
                    'id': 'source-1',
                    'prop': f'{LEXICAL_SCHEMA}{VERSION_INDEPENDENT_ID_FIELDS}',
                    'value': 'doc;rev',
                },
            ]
        if 'a lg:Chunk' in sparql:
            return [{
                'id': 'chunk-1',
                'prop': f'{LEXICAL_SCHEMA}value',
                'value': 'Chunk text',
            }]
        if 'SELECT ?statementId ?factValue' in sparql:
            return [
                {'statementId': 's1', 'factValue': 'fa'},
                {'statementId': 's1', 'factValue': 'fa'},
                {'statementId': 's1'},
            ]
        return []


def test_statements_are_grouped_into_retriever_shape():
    client = FakeClient()
    rows = run_query(GraphOperation.GET_STATEMENTS, client, {
        'statementIds': ['stmt-1'],
        'limit': 5,
        '_include_chunk_details': True,
    })

    result = rows[0]['result']
    assert result['score'] == 1
    assert result['source']['metadata']['title'] == 'Source title'
    assert result['source']['versioning'] == {
        'valid_from': 10,
        'valid_to': 20,
        'extract_timestamp': 11,
        'build_timestamp': 12,
        'id_fields': ['doc', 'rev'],
    }
    assert result['topics'][0]['chunks'][0]['metadata']['value'] == 'Chunk text'
    assert result['topics'][0]['statements'][0]['statement'] == 'Alice manages Bob'


def test_facts_are_grouped_and_deduplicated():
    rows = run_query(
        GraphOperation.GET_FACTS,
        FakeClient(),
        {'statementIds': ['s1']},
    )
    assert rows == [{'statementId': 's1', 'facts': ['fa']}]


def test_chunk_and_topic_content_queries():
    chunk_client = FakeClient()
    topic_client = FakeClient()

    assert run_query(
        GraphOperation.GET_CHUNKS, chunk_client, {'nodeIds': ['chunk-1']},
    ) == [{'content': 'Chunk text'}]
    assert run_query(
        GraphOperation.GET_TOPIC,
        topic_client,
        {'topicId': 'topic-1', 'statementLimit': 3},
    ) == [{'statement': 'Alice manages Bob', 'details': 'detail'}]
    assert 'VALUES ?chunkId { "chunk-1" }' in chunk_client.queries[0]
    assert '?fact lg:supports ?statementNode .' in topic_client.queries[0]


@pytest.mark.parametrize(('operation', 'parameters', 'query_fragment'), [
    (GraphOperation.SEARCH_BY_CHUNK,
     {'chunkId': 'chunk-1', 'statementLimit': 3},
     'lg:statementMentionedIn ?chunk'),
    (GraphOperation.SEARCH_BY_CHUNK,
     {'nodeId': 'chunk-1', 'statementLimit': 3},
     'lg:id "chunk-1"'),
    (GraphOperation.SEARCH_BY_TOPIC,
     {'nodeId': 'topic-1', 'statementLimit': 3},
     'lg:belongsTo ?topic'),
    (GraphOperation.SEARCH_BY_ENTITY,
     {'startId': 'e1', 'statementLimit': 3},
     'lg:subject ?entity'),
    (GraphOperation.SEARCH_BY_ENTITIES,
     {'startId': 'e1', 'endIds': ['e2'], 'statementLimit': 3},
     'lg:object'),
])
def test_search_operations_return_statement_ids(operation, parameters, query_fragment):
    client = FakeClient()
    assert run_query(operation, client, parameters) == [{'l': 'stmt-1'}]
    assert query_fragment in client.queries[0]


def test_keyword_lookup_uses_explicit_match_modes():
    exact = FakeClient()
    prefix = FakeClient()

    run_query(GraphOperation.FIND_ENTITIES_BY_KEYWORD, exact, {'keyword': 'alice'})
    run_query(GraphOperation.FIND_ENTITIES_BY_KEYWORD, prefix, {
        'keyword': 'ali',
        'classification': 'Per',
        '_starts_with': True,
        '_classification_starts_with': True,
    })

    assert 'FILTER(?searchStr = "alice")' in exact.queries[0]
    assert 'FILTER(STRSTARTS(?searchStr, "ali"))' in prefix.queries[0]
    assert 'FILTER(STRSTARTS(?class, "Per"))' in prefix.queries[0]


@pytest.mark.parametrize(('operation', 'value_name', 'query_fragment'), [
    (GraphOperation.FIND_ENTITIES_BY_CHUNKS, 'chunkId', 'lg:statementMentionedIn ?chunk'),
    (GraphOperation.FIND_ENTITIES_BY_TOPICS, 'topicId', 'lg:belongsTo ?topic'),
])
def test_entity_lookups_from_nodes(operation, value_name, query_fragment):
    client = FakeClient()
    rows = run_query(operation, client, {'nodeIds': ['n1'], 'limit': 5})

    assert rows[0]['result']['entity']['entityId'] == 'entity-1'
    assert f'VALUES ?{value_name} {{ "n1" }}' in client.queries[0]
    assert query_fragment in client.queries[0]


def test_entity_neighbours_and_scores_use_reified_facts():
    neighbours = FakeClient()
    scores = FakeClient()

    neighbour_rows = run_query(GraphOperation.FIND_ENTITY_NEIGHBORS, neighbours, {
        'entityIds': ['entity-1'],
        'excludeEntityIds': ['entity-1'],
        'numNeighbours': 2,
    })
    score_rows = run_query(
        GraphOperation.SCORE_ENTITIES,
        scores,
        {'entityIds': ['entity-1']},
    )

    assert neighbour_rows[0]['result']['others'] == ['entity-2']
    assert score_rows[0]['result']['score'] == 2.0
    assert 'lg:subject ?entity' in neighbours.queries[0]
    assert 'lg:object ?other' in neighbours.queries[0]


def test_local_entity_lookup_queries_skip_incomplete_rows():
    complements = FakeClient()
    subjects = FakeClient()

    run_query(GraphOperation.FIND_COMPLEMENTS, complements, {
        'params': [{'nId': 'n1'}, {'nId': None}],
    })
    run_query(GraphOperation.FIND_SUBJECTS, subjects, {
        'params': [{'nId': 'n1', 'cId': 'c1'}, {'nId': None, 'cId': None}],
    })

    assert len(complements.queries) == 1 and 'search_str' in complements.queries[0]
    assert len(subjects.queries) == 1 and 'SELECT ?n_id ?c_id' in subjects.queries[0]


def test_custom_namespace_and_tenant_default_graph_are_forwarded():
    namespace = NamespaceConfig(
        prefix='gt',
        schema_namespace='https://example.test/schema#',
        instance_namespace='https://example.test/data/',
        extra_prefixes={'xsd': 'http://www.w3.org/2001/XMLSchema#'},
    )
    client = FakeClient()

    run_query(
        GraphOperation.SEARCH_BY_ENTITY,
        client,
        {'startId': 'e1'},
        namespace=namespace,
        tenant_id='acme',
    )

    assert 'PREFIX gt: <https://example.test/schema#>' in client.queries[0]
    assert 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>' in client.queries[0]
    assert client.default_graphs == ['https://example.test/data/tenant/acme']


@pytest.mark.parametrize(('operation', 'parameters'), [
    (GraphOperation.GET_FACTS, {'statementIds': []}),
    (GraphOperation.GET_CHUNKS, {'nodeIds': []}),
    (GraphOperation.GET_TOPIC, {}),
    (GraphOperation.SEARCH_BY_CHUNK, {}),
    (GraphOperation.SEARCH_BY_TOPIC, {}),
    (GraphOperation.SEARCH_BY_ENTITY, {}),
    (GraphOperation.SEARCH_BY_ENTITIES, {'startId': 'a', 'endIds': []}),
    (GraphOperation.FIND_ENTITIES_BY_KEYWORD, {}),
    (GraphOperation.FIND_ENTITIES_BY_CHUNKS, {'nodeIds': []}),
    (GraphOperation.FIND_ENTITIES_BY_TOPICS, {'nodeIds': []}),
    (GraphOperation.FIND_ENTITY_NEIGHBORS, {'entityIds': []}),
    (GraphOperation.SCORE_ENTITIES, {'entityIds': []}),
    (GraphOperation.GET_STATEMENTS, {'statementIds': []}),
    (GraphOperation.FIND_COMPLEMENTS, {'params': []}),
    (GraphOperation.FIND_SUBJECTS, {'params': []}),
])
def test_empty_inputs_return_no_results(operation, parameters):
    assert run_query(operation, FakeClient(), parameters) == []


def test_update_operation_cannot_be_run_as_query():
    with pytest.raises(NotImplementedError, match='upsert_chunk'):
        run_query(GraphOperation.UPSERT_CHUNK, FakeClient(), {'chunk_id': 'c1'})


def test_sparql_values_are_escaped_and_unsafe_namespaces_rejected():
    assert sparql_literal('a" . } DELETE { ?s ?p ?o } #') == (
        '"a\\" . } DELETE { ?s ?p ?o } #"'
    )
    with pytest.raises(ValueError):
        NamespaceConfig(schema_namespace='https://x/ns#>\nINSERT DATA { <a> <b> <c> } #')
    with pytest.raises(ValueError, match='must be absolute'):
        NamespaceConfig(schema_namespace='relative/schema')


@pytest.mark.parametrize(('operation', 'parameters'), [
    (GraphOperation.SEARCH_BY_CHUNK, {'chunkId': 'c1', 'statementLimit': 0}),
    (GraphOperation.GET_TOPIC, {'topicId': 't1', 'statementLimit': -1}),
    (GraphOperation.FIND_ENTITIES_BY_CHUNKS, {'nodeIds': ['c1'], 'limit': 0}),
    (GraphOperation.FIND_ENTITY_NEIGHBORS, {'entityIds': ['e1'], 'numNeighbours': 0}),
    (GraphOperation.GET_STATEMENTS, {'statementIds': ['s1'], 'limit': 0}),
])
def test_numeric_query_clauses_must_be_positive(operation, parameters):
    with pytest.raises(ValueError, match='must be greater than zero'):
        run_query(operation, FakeClient(), parameters)


def test_result_helpers_handle_missing_and_invalid_values():
    assert _entity_score_rows([
        {'entityId': None},
        {'entityId': 'e1', 'value': 'v', 'class': 'c', 'score': 2},
    ]) == [{
        'result': {
            'entity': {'entityId': 'e1', 'value': 'v', 'class': 'c'},
            'score': 2.0,
        },
    }]
    assert _properties_by_id(FakeClient(), 'Source', [], DEFAULT_NAMESPACE) == {}
    assert _int_or_default(None, 5) == 5
    assert _int_or_default('nope', 5) == 5
    assert _int_or_default('7', 0) == 7
    assert _local_name(None, DEFAULT_NAMESPACE) == ''
    assert _local_name('http://other.test/foo#bar', DEFAULT_NAMESPACE) == 'bar'
