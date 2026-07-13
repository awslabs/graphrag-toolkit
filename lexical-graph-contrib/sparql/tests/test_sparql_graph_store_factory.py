# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import (
    SPARQLDatabaseClient,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store_factory import (
    SPARQLGraphStoreFactory,
)


def test_sparql_factory_creates_generic_endpoint_store():
    store = SPARQLGraphStoreFactory().try_create(
        'sparql+https://alice:secret@example.test/sparql/query',
        update_endpoint='https://example.test/sparql/update',
    )

    assert isinstance(store, SPARQLDatabaseClient)
    assert store.query_endpoint == 'https://example.test/sparql/query'
    assert store.update_endpoint == 'https://example.test/sparql/update'
    assert store.username == 'alice'
    assert store.password.get_secret_value() == 'secret'


def test_sparql_factory_decodes_uri_credentials():
    store = SPARQLGraphStoreFactory().try_create(
        'sparql+https://alice%40example.test:p%40ss%3Aword@example.test/query'
    )

    assert store.username == 'alice@example.test'
    assert store.password.get_secret_value() == 'p@ss:word'


def test_sparql_factory_ignores_non_sparql_urls():
    assert SPARQLGraphStoreFactory().try_create('https://example.test/sparql/query') is None


def test_sparql_factory_accepts_https_scheme():
    store = SPARQLGraphStoreFactory().try_create('sparql+s://example.test/sparql/query')
    assert store.query_endpoint == 'https://example.test/sparql/query'


def test_sparql_factory_enables_neptune_iam():
    store = SPARQLGraphStoreFactory().try_create(
        'sparql+neptune://example.test:8182',
        region_name='eu-central-1',
    )
    assert store.query_endpoint == 'https://example.test:8182'
    assert store.neptune_iam is True
    assert store.region_name == 'eu-central-1'


def test_registered_factory_handles_real_neptune_hostname():
    GraphStoreFactory.register(SPARQLGraphStoreFactory)
    store = GraphStoreFactory.for_graph_store(
        'sparql+neptune://cluster.us-east-1.neptune.amazonaws.com:8182',
        region_name='us-east-1',
    )
    assert isinstance(store, SPARQLDatabaseClient)
    assert store.neptune_iam is True


def test_sparql_factory_consumes_auth_kwargs_when_uri_has_credentials():
    store = SPARQLGraphStoreFactory().try_create(
        'sparql+https://alice:secret@example.test/sparql/query',
        username='bob',
        password='ignored',
    )
    assert store.username == 'alice'
    assert store.password.get_secret_value() == 'secret'


def test_sparql_factory_preserves_endpoint_query_params():
    store = SPARQLGraphStoreFactory().try_create(
        'sparql+https://example.test/sparql/query?default-graph-uri=http%3A%2F%2Fexample.test%2Fg'
        '&update_endpoint=https%3A%2F%2Fexample.test%2Fsparql%2Fupdate'
    )
    assert store.query_endpoint == (
        'https://example.test/sparql/query?default-graph-uri=http%3A%2F%2Fexample.test%2Fg'
    )
    assert store.update_endpoint == 'https://example.test/sparql/update'


def test_factory_returns_none_for_non_string_input():
    assert SPARQLGraphStoreFactory().try_create(12345) is None


def test_factory_handles_ipv6_host_and_explicit_port():
    store = SPARQLGraphStoreFactory().try_create('sparql+http://[::1]:7200/repositories/lg')
    assert store.query_endpoint == 'http://[::1]:7200/repositories/lg'
