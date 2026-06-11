# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live Cypher label-injection test against a real FalkorDB graph.

Exercises the exact query shape that ``EntityGraphBuilder`` emits for a domain
entity, driving a malicious classification through the real ``label_from`` and
``escape_cypher_label`` helpers, and confirms against a running FalkorDB that:

  * the escaped (patched) query leaves a seeded canary node untouched, and
  * the un-escaped (pre-patch) query DOES delete the canary - a red-state proof
    that this test actually detects the regression rather than passing vacuously.

The malicious classification is ``__...__`` wrapped so that ``label_from`` passes
it through unescaped; un-escaped it closes the SET label and runs a DETACH DELETE
on the canary, with a trailing ``//`` that comments out the rest of the line.

Marked ``integration`` and skips automatically when no FalkorDB is reachable, so
it is CI-safe.

Run a FalkorDB locally first, e.g. (Docker Hub blocked? use an internal mirror):

    finch run -d --name falkordb -p 6379:6379 docker.io/falkordb/falkordb:latest
    FALKORDB_URL=falkordb://localhost:6379 \
      pytest lexical-graph-contrib/falkordb/tests/test_label_injection_live.py -v
"""

import os
import uuid

import pytest

from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import (
    label_from,
    escape_cypher_label,
)
from graphrag_toolkit_contrib.lexical_graph.storage.graph.falkordb import (
    FalkorDBGraphStoreFactory,
)

pytestmark = pytest.mark.integration

FALKORDB_URL = os.environ.get('FALKORDB_URL', 'falkordb://localhost:6379')

CANARY_LABEL = '__InjCanary__'
CANARY_ID = 'inj-canary'

MALICIOUS_CLASSIFICATION = (
    f'__Evil` WITH e MATCH (c:`{CANARY_LABEL}`) DETACH DELETE c //__'
)


@pytest.fixture(scope='module')
def graph_store():
    GraphStoreFactory.register(FalkorDBGraphStoreFactory)
    try:
        store = GraphStoreFactory.for_graph_store(FALKORDB_URL)
        store.execute_query_with_retry('RETURN 1 AS ok', {})
    except Exception as e:  # noqa: BLE001 - any connection failure -> skip
        pytest.skip(f'FalkorDB not reachable at {FALKORDB_URL}: {e}')
    return store


def _canary_count(store):
    rows = store.execute_query_with_retry(
        f'MATCH (c:`{CANARY_LABEL}`) RETURN count(c) AS n', {}
    )
    return rows[0]['n'] if rows else 0


def _seed_canary(store):
    store.execute_query_with_retry(
        f'MERGE (c:`{CANARY_LABEL}` {{id: $id}})', {'id': CANARY_ID}
    )


def _cleanup(store):
    store.execute_query_with_retry(
        'MATCH (n) WHERE n.id = $id OR n.entityId = $eid DETACH DELETE n',
        {'id': CANARY_ID, 'eid': _entity_id()},
    )


def _entity_id():
    return f'ent-{uuid.uuid5(uuid.NAMESPACE_DNS, MALICIOUS_CLASSIFICATION).hex}'


def _patched_query(store, e_id, classification):
    """Mirror of the patched EntityGraphBuilder construction."""
    e_label = label_from(classification)
    safe = escape_cypher_label(e_label)
    return (
        f"MERGE (e:`__Entity__`{{{store.node_id('entityId')}: $entityId}}) "
        f"SET e :`{safe}` // awsqid"
    ), {'entityId': e_id}


def _vulnerable_query(store, e_id, classification):
    """Mirror of the PRE-patch construction (inlined id, un-escaped label)."""
    e_label = label_from(classification)
    return (
        f"MERGE (e:`__Entity__`{{{store.node_id('entityId')}: '{e_id}'}}) "
        f"SET e :`{e_label}` // awsqid"
    ), {}


def test_patched_query_does_not_inject(graph_store):
    """The escaped/parameterised query runs without the injected DETACH DELETE,
    so the seeded canary survives."""
    store = graph_store
    _cleanup(store)
    _seed_canary(store)
    assert _canary_count(store) == 1

    query, params = _patched_query(store, _entity_id(), MALICIOUS_CLASSIFICATION)
    store.execute_query_with_retry(query, params)

    assert _canary_count(store) == 1
    _cleanup(store)


def test_vulnerable_query_proves_canary_is_deletable(graph_store):
    """Red-state: the pre-patch query injects and deletes the canary, proving
    the canary mechanism is real and the patched test above is meaningful."""
    store = graph_store
    _cleanup(store)
    _seed_canary(store)
    assert _canary_count(store) == 1

    query, params = _vulnerable_query(store, _entity_id(), MALICIOUS_CLASSIFICATION)
    store.execute_query_with_retry(query, params)

    assert _canary_count(store) == 0
    _cleanup(store)
