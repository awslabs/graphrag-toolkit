# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for SPARQL operation dispatch and lifecycle (no server)."""

import logging

import pytest

from graphrag_toolkit.lexical_graph.storage.graph import GraphOperation
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import (
    SPARQLDatabaseClient,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_endpoint_client import (
    RDFLibHTTPStore,
    SPARQLEndpointClient,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.neptune_iam import (
    NeptuneIAMStore,
)


class _FakeClient:
    def __init__(self):
        self.updates = []
        self.queries = []
        self.default_graphs = []
        self.closed = False

    def update(self, sparql):
        self.updates.append(sparql)

    def query(self, sparql, default_graph=None):
        self.queries.append(sparql)
        self.default_graphs.append(default_graph)
        return [{'l': 'stmt-1'}]

    def close(self):
        self.closed = True


def _store(**kwargs):
    return SPARQLDatabaseClient(query_endpoint='http://ex.test/query', **kwargs)


def test_node_id_formats_id():
    assert _store().node_id('entityId')


def test_client_property_lazily_builds_and_caches_real_client():
    store = _store()
    client = store.client
    assert isinstance(client, SPARQLEndpointClient)
    assert isinstance(client.store, RDFLibHTTPStore)
    assert store.client is client  # cached on the private attr


def test_client_property_unwraps_secret_password():
    store = _store(username='u', password='pw')
    assert store.client.store._http.auth is not None


def test_client_property_uses_neptune_iam_store():
    store = SPARQLDatabaseClient(
        query_endpoint='https://example.test:8182',
        neptune_iam=True,
        region_name='eu-central-1',
    )
    assert isinstance(store.client.store, NeptuneIAMStore)
    assert store.client.store.query_endpoint == 'https://example.test:8182/sparql'
    store.client.close()


def test_execute_query_runs_caller_supplied_sparql_directly():
    store = _store()
    fake = _FakeClient()
    store._client = fake

    rows = store._execute_query('SELECT ?l WHERE { VALUES ?l { "stmt-1" } }')

    assert rows == [{'l': 'stmt-1'}]
    assert fake.queries == ['SELECT ?l WHERE { VALUES ?l { "stmt-1" } }']
    assert fake.updates == []


def test_execute_query_rejects_silently_ignored_parameters():
    store = _store()
    store._client = _FakeClient()

    with pytest.raises(ValueError, match='parameter binding is not supported'):
        store._execute_query('SELECT * WHERE { ?s ?p ?o }', {'value': 'ignored'})


def test_semantic_operation_uses_native_update_without_inspecting_query():
    store = _store()
    fake = _FakeClient()
    store._client = fake

    store._execute_operation(
        GraphOperation.UPSERT_ENTITY,
        'intentionally not parsed',
        {'params': [{'e_id': 'e1', 'v': 'Alice'}]},
    )

    assert len(fake.updates) == 1
    assert 'INSERT DATA' in fake.updates[0]
    assert 'entity/e1' in fake.updates[0]
    assert 'tenant/default_' in fake.updates[0]


def test_semantic_read_operation_uses_native_query():
    store = _store()
    fake = _FakeClient()
    store._client = fake

    rows = store._execute_operation(
        GraphOperation.SEARCH_BY_CHUNK,
        'also not parsed',
        {'chunkId': 'c1', 'statementLimit': 3},
    )

    assert rows == [{'l': 'stmt-1'}]
    assert 'SELECT DISTINCT ?l' in fake.queries[0]
    assert fake.default_graphs == [
        'https://awslabs.github.io/graphrag-toolkit/lexical/tenant/default_'
    ]


def test_empty_update_parameters_send_nothing():
    store = _store()
    fake = _FakeClient()
    store._client = fake
    store._execute_operation(
        GraphOperation.UPSERT_ENTITY,
        '',
        {'params': []},
    )
    assert fake.updates == []


def test_getstate_drops_client():
    store = _store()
    store._client = _FakeClient()
    state = store.__getstate__()
    assert store._client is None
    assert state is not None


def test_exit_closes_and_clears_client():
    store = _store()
    fake = _FakeClient()
    store._client = fake
    assert store.__exit__(None, None, None) is False
    assert fake.closed is True
    assert store._client is None


def test_execute_query_emits_debug_timing(caplog):
    store = _store()
    store._client = _FakeClient()
    logger_name = 'graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store'
    with caplog.at_level(logging.DEBUG, logger=logger_name):
        store._execute_operation(
            GraphOperation.SEARCH_BY_CHUNK,
            '',
            {'chunkId': 'c1'},
        )
    assert any('search_by_chunk' in record.getMessage() for record in caplog.records)


def test_namespace_kwargs_thread_into_writes_and_reads():
    store = _store(
        lexical_prefix='gt',
        lexical_schema_namespace='https://example.test/schema#',
        lexical_instance_namespace='https://example.test/data/',
        sparql_prefixes={'xsd': 'http://www.w3.org/2001/XMLSchema#'},
    )
    fake = _FakeClient()
    store._client = fake

    store._execute_operation(
        GraphOperation.UPSERT_ENTITY,
        '',
        {'params': [{'e_id': 'e1', 'v': 'Alice'}]},
    )
    assert '<https://example.test/data/entity/e1>' in fake.updates[0]

    store._execute_operation(
        GraphOperation.SEARCH_BY_ENTITIES,
        '',
        {'startId': 'e1', 'endIds': ['e2'], 'statementLimit': 3},
    )
    assert 'PREFIX gt: <https://example.test/schema#>' in fake.queries[0]
