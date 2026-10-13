# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import threading
import uuid

import pytest
from rdflib import Dataset, URIRef

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.ontology import sparql_literal
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_endpoint_client import SPARQLEndpointClient
from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_graph_store import SPARQLDatabaseClient


class DatasetClient:
    def __init__(self):
        self.dataset = Dataset()
        self.lock = threading.RLock()

    def update(self, query):
        with self.lock:
            self.dataset.update(query)

    def query(self, query, default_graph=None):
        with self.lock:
            graph = self.dataset.graph(URIRef(default_graph)) if default_graph else self.dataset
            return [
                {str(key): SPARQLEndpointClient._coerce(value) for key, value in row.asdict().items()}
                for row in graph.query(query)
            ]

    def close(self):
        self.dataset.close()


@pytest.fixture(params=['memory'] + (['http'] if os.environ.get('SPARQL_TEST_ENDPOINT') else []))
def graph_store(request):
    namespace = f'https://example.org/sparql-test/{uuid.uuid4().hex}/'
    store = SPARQLDatabaseClient(
        query_endpoint=os.environ.get('SPARQL_TEST_ENDPOINT', 'http://unused.test/query'),
        update_endpoint=os.environ.get('SPARQL_TEST_UPDATE_ENDPOINT'),
        lexical_instance_namespace=namespace,
    )
    if request.param == 'memory':
        store._client = DatasetClient()
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
