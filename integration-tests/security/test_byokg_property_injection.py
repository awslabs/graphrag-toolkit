# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Security integration test: property-name injection in the byokg-rag Neptune store.

Runs with its parent fix PR. Drives the real get_nodes / get_one_hop_edges sinks
(with a query-capturing client) and runs the OpenCypher they emit against a live
openCypher engine, so a regression that reverts a sink to raw n.{prop}
interpolation is caught here. A legitimate property resolves, an injection-laden
name is inert, and the pre-fix raw interpolation raises a syntax error on the
engine.

Backend: NEO4J_TEST_URI. Skips when unset; CI provides a Neo4j service container.

The byokg store is imported inside the helpers, so a failure to import its ML
dependencies fails this module rather than aborting collection for the job.
"""

import os
import time
import json
import pytest
from unittest.mock import Mock, patch


@pytest.fixture(scope='session')
def neo4j_driver():
    """A connected neo4j driver for the live openCypher engine.

    Retries verify_connectivity because a service container can report healthy
    over HTTP before Bolt accepts authenticated connections.
    """
    uri = os.environ.get('NEO4J_TEST_URI')
    if not uri:
        pytest.skip('NEO4J_TEST_URI not set')
    from neo4j import GraphDatabase
    user = os.environ.get('NEO4J_TEST_USER', 'neo4j')
    password = os.environ.get('NEO4J_TEST_PASSWORD', 'testpassword123')
    driver = GraphDatabase.driver(uri, auth=(user, password))
    last_error = None
    for _ in range(30):
        try:
            driver.verify_connectivity()
            break
        except Exception as error:
            last_error = error
            time.sleep(2)
    else:
        driver.close()
        raise last_error
    yield driver
    driver.close()


def _emit(prop, method, node_ids):
    """Return the OpenCypher and parameters the real sink emits for a property."""
    from graphrag_toolkit.byokg_rag.graphstore.neptune import NeptuneAnalyticsGraphStore
    captured = {}

    def capture(**kwargs):
        captured.update(kwargs)
        return {'payload': Mock(read=lambda: json.dumps({'results': []}).encode())}

    with patch('graphrag_toolkit.byokg_rag.graphstore.neptune.boto3.Session') as mock_session:
        neptune_client = Mock()
        neptune_client.get_graph.return_value = {'id': 'test-graph-id', 'status': 'AVAILABLE'}
        neptune_client.execute_query.side_effect = capture
        session = Mock()
        mock_session.return_value = session
        session.client.side_effect = lambda service, **kw: (
            neptune_client if service == 'neptune-graph' else Mock()
        )
        store = NeptuneAnalyticsGraphStore(graph_identifier='test-graph-id', region='us-west-2')
        store.node_type_to_property_mapping = {'N': prop}
        getattr(store, method)(node_ids)

    return captured['queryString'], captured['parameters']


def _run(neo4j_driver, query, parameters):
    with neo4j_driver.session() as session:
        return list(session.run(query, **parameters))


@pytest.fixture(autouse=True)
def _seed(neo4j_driver):
    with neo4j_driver.session() as session:
        session.run('MATCH (n) DETACH DELETE n')
        session.run("CREATE (:N {name:'keep'})")


def test_legit_property_resolves(neo4j_driver):
    query, parameters = _emit('name', 'get_nodes', ['keep'])
    rows = _run(neo4j_driver, query, parameters)
    assert len(rows) == 1
    assert rows[0]['properties']['name'] == 'keep'


@pytest.mark.parametrize('method', ['get_nodes', 'get_one_hop_edges'])
def test_injected_property_name_is_inert(neo4j_driver, method):
    # A raw n.{prop} regression would emit invalid Cypher and raise here.
    query, parameters = _emit('a`b', method, ['keep'])
    assert _run(neo4j_driver, query, parameters) == []


def test_pre_fix_raw_interpolation_would_break_out(neo4j_driver):
    """Red-state: the old raw property interpolation breaks the query on the engine."""
    from neo4j.exceptions import CypherSyntaxError
    query = 'MATCH (n:N) WHERE n.a`b IN $ids RETURN n.name AS name'
    with neo4j_driver.session() as session:
        with pytest.raises(CypherSyntaxError):
            list(session.run(query, ids=['keep']))
