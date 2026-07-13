# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import requests
from botocore.credentials import Credentials
from rdflib import Graph

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.neptune_iam import (
    NeptuneIAMAuth,
    NeptuneIAMStore,
    neptune_iam_graph,
)


class _AWSSession:
    region_name = 'eu-central-1'

    def get_credentials(self):
        return Credentials('access-key', 'secret-key', 'session-token')


class _Response:
    status_code = 200
    text = ''
    content = b'{"head": {}, "boolean": true}'
    headers = {'Content-Type': 'application/sparql-results+json'}

    def raise_for_status(self):
        pass


class _HTTPSession:
    def __init__(self):
        self.calls = []
        self.closed = False

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response()

    def close(self):
        self.closed = True


def test_auth_signs_with_neptune_service_and_session_token():
    request = requests.Request(
        'POST', 'https://example.test/sparql', data={'query': 'ASK {}'}
    ).prepare()

    NeptuneIAMAuth(aws_session=_AWSSession())(request)

    assert '/eu-central-1/neptune-db/aws4_request' in request.headers['Authorization']
    assert request.headers['X-Amz-Security-Token'] == 'session-token'


def test_store_queries_and_updates_neptune_sparql_endpoint():
    store = NeptuneIAMStore(
        'https://example.test:8182',
        aws_session=_AWSSession(),
        headers={'X-Application': 'graph-rag'},
    )
    store._http.close()
    store._http = http = _HTTPSession()
    graph = Graph(store=store)

    assert graph.query('ASK {}').askAnswer is True
    graph.update('INSERT DATA { <urn:s> <urn:p> <urn:o> }')
    graph.close()

    assert store.query_endpoint == 'https://example.test:8182/sparql'
    assert http.calls[0][1]['data']['query'].endswith('ASK {}')
    assert http.calls[1][1]['data']['update'].endswith(
        'INSERT DATA { <urn:s> <urn:p> <urn:o> }'
    )
    assert http.calls[0][1]['headers']['X-Application'] == 'graph-rag'
    assert http.closed is True


def test_store_rejects_unsigned_http_endpoint():
    with pytest.raises(ValueError, match='requires an HTTPS endpoint'):
        NeptuneIAMStore('http://example.test:8182', aws_session=_AWSSession())


def test_graph_helper_returns_rdflib_graph():
    graph = neptune_iam_graph(
        'https://example.test:8182/sparql/', aws_session=_AWSSession()
    )
    assert isinstance(graph, Graph)
    assert graph.store.query_endpoint == 'https://example.test:8182/sparql'
    graph.close()
