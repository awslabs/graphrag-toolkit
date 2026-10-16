# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live checks that Neo4jDatabaseClient sends each statement to Neo4j once,
and to the database it is configured with.

A mocked driver can only show which methods were called. Whether a write
landed once, or in the right database, only shows in the graph, so these
count what a CREATE left behind.

Skipped unless NEO4J_TEST_URI is set, and stops with an error rather than
running if the server's default database already holds data. The
named-database test also needs a server that can create databases (Neo4j
Enterprise), and is skipped on Community. To run locally:

    finch run -d --name graph-store-test -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/testpassword123 \\
        -e NEO4J_ACCEPT_LICENSE_AGREEMENT=eval neo4j:5-enterprise
    NEO4J_TEST_URI=bolt://neo4j:testpassword123@localhost:7687 \\
        pytest tests/integration/storage/graph/test_neo4j_graph_store_live.py
    finch stop graph-store-test && finch rm graph-store-test
"""

import os
from urllib.parse import urlparse, urlunparse

import pytest

from graphrag_toolkit.lexical_graph.storage.graph.neo4j_graph_store import Neo4jDatabaseClient

NEO4J_TEST_URI = os.environ.get('NEO4J_TEST_URI')
NAMED_DATABASE = 'oncecheck'
LABEL = '__OnceCheck__'

pytestmark = pytest.mark.skipif(
    not NEO4J_TEST_URI,
    reason='set NEO4J_TEST_URI to a running Neo4j instance to run this live test',
)


def connect(database=None):
    """A client on the NEO4J_TEST_URI server, with the URI's own database replaced by `database`."""
    server_uri = urlunparse(urlparse(NEO4J_TEST_URI)._replace(path=''))
    return Neo4jDatabaseClient(endpoint_url=server_uri, database=database)


def count_nodes(client):
    return client._execute_query(f'MATCH (n:`{LABEL}`) RETURN count(n) AS node_count')[0]['node_count']


@pytest.fixture
def default_db():
    client = connect()

    node_count = client._execute_query('MATCH (n) RETURN count(n) AS node_count')[0]['node_count']
    if node_count:
        pytest.fail(
            f'NEO4J_TEST_URI points at a server whose default database is not empty (node count: '
            f'{node_count}). This test writes and deletes nodes, so it only runs against an empty '
            f'database. Point NEO4J_TEST_URI at a throwaway instance.'
        )

    yield client

    client._execute_query(f'MATCH (n:`{LABEL}`) DETACH DELETE n')
    client.client.close()


@pytest.fixture
def named_db(default_db):
    from neo4j.exceptions import ClientError

    system = connect('system')
    try:
        system._execute_query(f'CREATE DATABASE {NAMED_DATABASE} IF NOT EXISTS WAIT')
    except ClientError as e:
        system.client.close()
        pytest.skip(f'server cannot create a second database: {e.code}')

    client = connect(NAMED_DATABASE)

    yield client

    client.client.close()
    system._execute_query(f'DROP DATABASE {NAMED_DATABASE} IF EXISTS WAIT')
    system.client.close()


def test_a_create_leaves_one_node(default_db):
    created = default_db._execute_query(f'CREATE (n:`{LABEL}` {{k: $k}}) RETURN n.k AS k', {'k': 1})

    assert created == [{'k': 1}]
    assert count_nodes(default_db) == 1


def test_a_write_to_a_named_database_stays_in_it(named_db, default_db):
    named_db._execute_query(f'CREATE (n:`{LABEL}` {{k: $k}})', {'k': 1})

    assert count_nodes(named_db) == 1
    assert count_nodes(default_db) == 0
