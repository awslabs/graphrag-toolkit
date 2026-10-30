# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from rdflib import Graph
from requests.auth import HTTPBasicAuth

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_endpoint_client import (
    FORM_URLENCODED,
    SPARQL_JSON,
    RDFLibHTTPStore,
    SPARQLEndpointClient,
)


class _Response:
    status_code = 200
    text = ''
    content = b'{"head":{"vars":["value"]},"results":{"bindings":[{"value":{"type":"literal","value":"ok"}}]}}'
    headers = {'Content-Type': SPARQL_JSON}


class _Session:
    def __init__(self, response=None):
        self.auth = None
        self.calls = []
        self.closed = False
        self.response = response or _Response()

    def post(self, url, data, headers, timeout):
        self.calls.append({
            'url': url,
            'data': data,
            'headers': headers,
            'timeout': timeout,
        })
        return self.response

    def close(self):
        self.closed = True


def _client(session=None, **kwargs):
    client = SPARQLEndpointClient('http://example.test/query', **kwargs)
    client.store._http.close()
    client.store._http = session or _Session()
    return client


def test_client_sends_form_encoded_query_and_update_requests():
    session = _Session()
    client = _client(
        session,
        update_endpoint='http://example.test/update',
        headers={'Authorization': 'Bearer token', 'Content-Type': 'text/plain'},
        timeout=12,
    )

    assert client.query('SELECT ?value WHERE { VALUES ?value { "ok" } }') == [
        {'value': 'ok'},
    ]
    client.update('INSERT DATA { <urn:s> <urn:p> <urn:o> }')

    query_call, update_call = session.calls
    assert query_call['url'] == 'http://example.test/query'
    assert query_call['data']['query'].endswith(
        'SELECT ?value WHERE { VALUES ?value { "ok" } }'
    )
    assert query_call['headers']['Content-Type'] == FORM_URLENCODED
    assert query_call['headers']['Accept'].startswith(SPARQL_JSON)
    assert 'text/turtle' in query_call['headers']['Accept']
    assert query_call['headers']['Authorization'] == 'Bearer token'
    assert query_call['timeout'] == 12

    assert update_call['url'] == 'http://example.test/update'
    assert update_call['data']['update'].endswith(
        'INSERT DATA { <urn:s> <urn:p> <urn:o> }'
    )
    assert update_call['headers']['Content-Type'] == FORM_URLENCODED
    assert update_call['headers']['Authorization'] == 'Bearer token'


class _ErrorResponse(_Response):
    status_code = 500
    text = 'kaboom'


def test_query_returns_ask_boolean():
    response = _Response()
    response.content = b'{"head": {}, "boolean": true}'
    assert _client(_Session(response)).query('ASK { ?s ?p ?o }') == [
        {'boolean': True},
    ]


def test_rdflib_graph_can_parse_construct_results():
    response = _Response()
    response.content = b'<urn:s> <urn:p> <urn:o> .'
    response.headers = {'Content-Type': 'text/turtle'}
    store = RDFLibHTTPStore('http://example.test/query')
    store._http.close()
    store._http = _Session(response)
    graph = Graph(store=store)

    result = graph.query('CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }')

    assert result.type == 'CONSTRUCT'
    assert len(result.graph) == 1
    graph.close()


def test_query_can_scope_the_protocol_default_graph():
    session = _Session()
    client = _client(session)

    client.query('SELECT * WHERE { ?s ?p ?o }', default_graph='urn:tenant:acme')

    assert session.calls[0]['data']['default-graph-uri'] == 'urn:tenant:acme'


def test_query_defaults_update_endpoint_to_query_endpoint():
    client = _client()
    assert client.store.update_endpoint == 'http://example.test/query'


def test_raise_for_status_raises_on_http_error():
    with pytest.raises(RuntimeError, match='kaboom') as error:
        _client(_Session(_ErrorResponse())).query('SELECT * WHERE { ?s ?p ?o }')
    assert 'SELECT' not in str(error.value)


def test_close_closes_the_session():
    session = _Session()
    client = _client(session)
    client.close()
    assert session.closed is True


def test_client_uses_basic_auth_and_rdflib_store():
    client = SPARQLEndpointClient('http://example.test/query', username='u', password='pw')
    assert isinstance(client.store, RDFLibHTTPStore)
    assert isinstance(client.store._http.auth, HTTPBasicAuth)
    client.close()


def test_client_rejects_non_tabular_query_results():
    client = _client()
    client._graph.query = lambda _: SimpleNamespace(type='CONSTRUCT')

    with pytest.raises(ValueError, match='Only SELECT and ASK'):
        client.query('CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }')


def test_client_rejects_non_positive_timeout():
    with pytest.raises(ValueError, match='timeout must be greater than zero'):
        SPARQLEndpointClient('http://example.test/query', timeout=0)


def test_failed_update_is_not_retained_for_the_next_request():
    session = _Session(_ErrorResponse())
    client = _client(session)
    update = 'INSERT DATA { <urn:s> <urn:p> <urn:o> }'
    with pytest.raises(RuntimeError):
        client.update(update)
    session.response = _Response()
    client.update(update)
    assert session.calls[1]['data']['update'].count(update) == 1


def test_concurrent_updates_are_not_combined_or_replayed():
    class SlowSession(_Session):
        def post(self, *args, **kwargs):
            response = super().post(*args, **kwargs)
            time.sleep(0.01)
            return response

    session = SlowSession()
    client = _client(session)
    updates = [f'INSERT DATA {{ <urn:s{i}> <urn:p> <urn:o> }}' for i in range(16)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(client.update, updates))
    assert len(session.calls) == len(updates)
    assert all(call['data']['update'].count('INSERT DATA') == 1 for call in session.calls)
