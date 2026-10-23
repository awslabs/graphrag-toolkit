# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import threading
import uuid
from urllib.parse import parse_qs

import pytest
from rdflib import Dataset, URIRef
from requests import Response
from requests.adapters import BaseAdapter

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import sparql_literal
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_endpoint_client import SPARQL_JSON
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import SPARQLDatabaseClient


class DatasetAdapter(BaseAdapter):
    def __init__(self):
        self.dataset = Dataset()
        self.lock = threading.RLock()

    def send(self, request, **kwargs):
        body = request.body.decode('utf-8') if isinstance(request.body, bytes) else request.body
        parameters = parse_qs(body)
        response = Response()
        response.request = request
        response.url = request.url
        with self.lock:
            if 'update' in parameters:
                self.dataset.update(parameters['update'][0])
                response.status_code = 204
                response._content = b''
            else:
                default_graph = parameters.get('default-graph-uri', [None])[0]
                graph = self.dataset.graph(URIRef(default_graph)) if default_graph else self.dataset
                result = graph.query(parameters['query'][0])
                is_result_set = result.type in ('SELECT', 'ASK')
                response.status_code = 200
                response.headers['Content-Type'] = SPARQL_JSON if is_result_set else 'text/turtle'
                response._content = result.serialize(format='json' if is_result_set else 'turtle')
        return response

    def close(self):
        self.dataset.close()


@pytest.fixture(params=['memory'] + (['http'] if os.environ.get('SPARQL_TEST_ENDPOINT') else []))
def graph_store(request):
    namespace = f'https://example.org/sparql-test/{uuid.uuid4().hex}/'
    in_memory = request.param == 'memory'
    store = SPARQLDatabaseClient(
        query_endpoint='http://unused.test/query' if in_memory else os.environ['SPARQL_TEST_ENDPOINT'],
        update_endpoint='http://unused.test/update' if in_memory else os.environ.get('SPARQL_TEST_UPDATE_ENDPOINT'),
        lexical_instance_namespace=namespace,
    )
    if in_memory:
        store.client.store._http.mount('http://unused.test/', DatasetAdapter())
    try:
        yield store
    finally:
        if request.param == 'http':
            graphs = store.client.query(f'''SELECT DISTINCT ?graph WHERE {{
                GRAPH ?graph {{ ?s ?p ?o }}
                FILTER(STRSTARTS(STR(?graph), {sparql_literal(namespace)}))
            }}''')
            for row in graphs:
                assert row['graph'].startswith(namespace)
                store.client.update(f'DROP GRAPH {URIRef(row["graph"]).n3()}')
        store.client.close()
