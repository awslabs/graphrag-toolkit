# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live check that Neo4jDatabaseClient sends each statement to Neo4j once.

A mocked driver can only show which methods were called. Whether a write
landed once or twice only shows in the graph, so this counts what a CREATE
left behind.

Skipped unless NEO4J_TEST_URI is set, and fails rather than runs if that URI
points at a database that already holds data. To run locally:

    finch run -d --name graph-store-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 neo4j:5
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/storage/graph/test_neo4j_graph_store_live.py
    finch stop graph-store-test && finch rm graph-store-test
"""

import os

import pytest

from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient

NEO4J_TEST_URI = os.environ.get('NEO4J_TEST_URI')

pytestmark = pytest.mark.skipif(
    not NEO4J_TEST_URI,
    reason='set NEO4J_TEST_URI to a running Neo4j instance to run this live test',
)


@pytest.fixture(params=['', '/neo4j'], ids=['default database', 'named database'])
def client(request):
    client = Neo4jDatabaseClient(endpoint_url=f'{NEO4J_TEST_URI.rstrip("/")}{request.param}')

    rows = client._execute_query('MATCH (n) RETURN count(n) AS node_count')
    if rows[0]['node_count']:
        pytest.fail(
            f'NEO4J_TEST_URI points at a database that is not empty (node count: '
            f'{rows[0]["node_count"]}). This test writes and deletes nodes, so it only runs '
            f'against an empty database. Point NEO4J_TEST_URI at a throwaway instance.'
        )

    yield client

    client._execute_query('MATCH (n:`__OnceCheck__`) DETACH DELETE n')
    client.client.close()


def test_a_create_leaves_one_node(client):
    created = client._execute_query('CREATE (n:`__OnceCheck__` {k: $k}) RETURN n.k AS k', {'k': 1})

    rows = client._execute_query('MATCH (n:`__OnceCheck__`) RETURN count(n) AS node_count')

    assert created == [{'k': 1}]
    assert rows[0]['node_count'] == 1
