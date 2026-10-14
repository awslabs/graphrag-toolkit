# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import (
    SPARQLDatabaseClient,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store_factory import (
    SPARQLGraphStoreFactory,
)


@pytest.fixture(autouse=True)
def _clear_sparql_auth_environment(monkeypatch):
    monkeypatch.delenv('SPARQL_USER', raising=False)
    monkeypatch.delenv('SPARQL_PASSWORD', raising=False)


def _create(endpoint, **kwargs):
    return SPARQLGraphStoreFactory().try_create(
        endpoint,
        graph_store_type='sparql',
        **kwargs,
    )


def test_sparql_factory_preserves_ordinary_endpoints():
    store = _create(
        'https://example.test/sparql/query?default-graph-uri=https%3A%2F%2Fexample.test%2Fg',
        update_endpoint='https://example.test/sparql/update',
    )

    assert isinstance(store, SPARQLDatabaseClient)
    assert store.query_endpoint == (
        'https://example.test/sparql/query?default-graph-uri=https%3A%2F%2Fexample.test%2Fg'
    )
    assert store.update_endpoint == 'https://example.test/sparql/update'


def test_sparql_factory_requires_explicit_discriminator():
    factory = SPARQLGraphStoreFactory()

    assert factory.try_create('https://example.test/sparql') is None
    assert factory.try_create(
        'https://example.test/sparql', graph_store_type='neo4j'
    ) is None


def test_sparql_factory_rejects_synthetic_or_non_http_schemes():
    with pytest.raises(ValueError, match='absolute HTTP or HTTPS'):
        _create('sparql+https://example.test/sparql')


def test_sparql_factory_decodes_and_removes_uri_credentials():
    store = _create(
        'https://alice%40example.test:p%40ss%3Aword@example.test/query'
    )

    assert store.query_endpoint == 'https://example.test/query'
    assert store.username == 'alice@example.test'
    assert store.password.get_secret_value() == 'p@ss:word'


def test_sparql_factory_supports_basic_auth_arguments():
    store = _create(
        'https://example.test/query',
        auth_type='basic',
        username='alice',
        password='secret',
    )

    assert store.username == 'alice'
    assert store.password.get_secret_value() == 'secret'


def test_sparql_factory_supports_bearer_and_custom_headers():
    headers = {
        'Authorization': 'Bearer token',
        'X-Request-Origin': 'graphrag-toolkit',
    }
    store = _create('https://example.test/query', headers=headers)

    assert store.headers == headers


@pytest.mark.parametrize(('kwargs', 'message'), [
    ({'username': 'alice'}, 'both username and password'),
    ({'password': 'secret'}, 'both username and password'),
    ({'auth_type': 'basic'}, 'requires username and password'),
    ({'auth_type': 'oauth'}, 'Unsupported SPARQL auth_type'),
    ({
        'username': 'alice',
        'password': 'secret',
        'headers': {'Authorization': 'Bearer token'},
    }, 'cannot be combined with an Authorization header'),
])
def test_sparql_factory_rejects_invalid_auth_configuration(kwargs, message):
    with pytest.raises(ValueError, match=message):
        _create('https://example.test/query', **kwargs)


def test_sparql_factory_enables_neptune_iam_explicitly():
    store = _create(
        'https://example.test:8182/sparql',
        auth_type='aws_iam',
        region_name='eu-central-1',
    )

    assert store.query_endpoint == 'https://example.test:8182/sparql'
    assert store.neptune_iam is True
    assert store.region_name == 'eu-central-1'


def test_sparql_factory_rejects_iam_over_http():
    with pytest.raises(ValueError, match='requires an HTTPS endpoint'):
        _create('http://example.test:8182/sparql', auth_type='aws_iam')


@pytest.mark.parametrize('kwargs', [
    {'auth_type': 'aws_iam', 'username': 'alice', 'password': 'secret'},
    {'auth_type': 'aws_iam', 'headers': {'Authorization': 'Bearer token'}},
])
def test_sparql_factory_rejects_iam_with_other_auth(kwargs):
    with pytest.raises(ValueError, match='cannot be combined'):
        _create('https://example.test:8182/sparql', **kwargs)


def test_sparql_factory_rejects_distinct_iam_update_endpoint():
    with pytest.raises(ValueError, match='one SPARQL endpoint'):
        _create(
            'https://example.test:8182/sparql',
            auth_type='aws_iam',
            update_endpoint='https://example.test:8182/update',
        )


def test_registered_factory_handles_real_neptune_hostname_as_sparql():
    GraphStoreFactory.register(SPARQLGraphStoreFactory)
    store = GraphStoreFactory.for_graph_store(
        'https://cluster.us-east-1.neptune.amazonaws.com:8182/sparql',
        graph_store_type='sparql',
        auth_type='aws_iam',
        region_name='us-east-1',
    )

    assert isinstance(store, SPARQLDatabaseClient)
    assert store.neptune_iam is True


def test_factory_returns_none_for_non_string_input():
    assert SPARQLGraphStoreFactory().try_create(
        12345, graph_store_type='sparql'
    ) is None


def test_factory_handles_ipv6_host_and_explicit_port():
    store = _create('http://[::1]:7200/repositories/lg')
    assert store.query_endpoint == 'http://[::1]:7200/repositories/lg'


def test_factory_rejects_invalid_update_endpoint():
    with pytest.raises(ValueError, match='update_endpoint'):
        _create(
            'https://example.test/query',
            update_endpoint='file:///tmp/update',
        )
