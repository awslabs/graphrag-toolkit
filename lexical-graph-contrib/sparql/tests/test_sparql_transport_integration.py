# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit_contrib.lexical_graph.storage.graph.sparql.sparql_endpoint_client import SPARQLEndpointClient


def test_integration_fixture_uses_the_production_client(graph_store):
    assert isinstance(graph_store.client, SPARQLEndpointClient)


def test_transport_parses_typed_results_and_ask(graph_store):
    assert graph_store.client.query('''SELECT ?text ?number ?flag ?iri WHERE {
        VALUES (?text ?number ?flag ?iri) { ("č š ž" 42 true <urn:test>) }
    }''') == [{'text': 'č š ž', 'number': 42, 'flag': True, 'iri': 'urn:test'}]
    assert graph_store.client.query('ASK {}') == [{'boolean': True}]
    assert graph_store.client.query('ASK { <urn:missing> <urn:missing> <urn:missing> }') == [{'boolean': False}]


def test_transport_rejects_construct_results(graph_store):
    with pytest.raises(ValueError, match='Only SELECT and ASK'):
        graph_store.client.query('CONSTRUCT { <urn:s> <urn:p> <urn:o> } WHERE {}')
