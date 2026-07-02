# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Security integration test: SQL injection in the PGVector store.

Runs with its parent fix PR. Drives the real PGIndex sinks against a live
Postgres+pgvector engine and shows the fix working: legitimate filters resolve,
injected filter values, metadata keys, and id lists are inert, and the pre-fix
interpolation pattern still breaks out on the same engine.

Backend: PGVECTOR_TEST_DSN. Skips when unset; CI provides a Postgres+pgvector
service container.
"""

import os
import json
import numpy as np
import pytest

import graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes as pvi
from graphrag_toolkit.lexical_graph.tenant_id import TenantId
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from llama_index.core.schema import QueryBundle
from llama_index.core.vector_stores.types import (
    FilterCondition, FilterOperator, MetadataFilter, MetadataFilters,
)

VALUE_PAYLOAD = "' OR '1'='1"
KEY_PAYLOAD = "category' OR '1'='1"
ID_PAYLOAD = "x') OR '1'='1"

SEED_ROWS = [
    ('keep1', {'source': {'metadata': {'category': 'tech', 'count': 5}, 'versioning': {}}}, [1.0, 0.0, 0.0]),
    ('keep2', {'source': {'metadata': {'category': 'science', 'count': 1}, 'versioning': {}}}, [0.0, 1.0, 0.0]),
]


def _pg_kwargs_or_none():
    """Parse PGVECTOR_TEST_DSN into psycopg2 connect kwargs, or None when unset."""
    dsn = os.environ.get('PGVECTOR_TEST_DSN')
    if not dsn:
        return None
    import psycopg2.extensions
    kwargs = psycopg2.extensions.parse_dsn(dsn)
    if 'port' in kwargs:
        kwargs['port'] = int(kwargs['port'])
    return kwargs


@pytest.fixture(scope='session')
def pg_kwargs():
    """psycopg2 connect kwargs for the live Postgres+pgvector engine."""
    kwargs = _pg_kwargs_or_none()
    if kwargs is None:
        pytest.skip('PGVECTOR_TEST_DSN not set')
    return kwargs


@pytest.fixture(scope='session', autouse=True)
def _pg_extension_and_schema():
    """Ensure the pgvector extension and graphrag schema exist before the tests.
    A no-op when the backend is not configured."""
    kwargs = _pg_kwargs_or_none()
    if kwargs is None:
        return
    import psycopg2
    conn = psycopg2.connect(**kwargs)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute('CREATE EXTENSION IF NOT EXISTS vector;')
    cur.execute('CREATE SCHEMA IF NOT EXISTS graphrag;')
    cur.close()
    conn.close()


@pytest.fixture(scope='session')
def pg_index(pg_kwargs):
    """A PGIndex against the live engine, with its table created once per session."""
    index = pvi.PGIndex.model_construct(
        index_name='chunk', database=pg_kwargs['dbname'], schema_name='graphrag',
        host=pg_kwargs['host'], port=pg_kwargs['port'], username=pg_kwargs['user'],
        password=pg_kwargs['password'], dimensions=3, embed_model=None,
        enable_iam_db_auth=False, writeable=True, tenant_id=TenantId(), initialized=False,
    )
    index._get_connection()  # runs the CREATE TABLE / index DDL once
    index._table = index.underlying_index_name()
    return index


@pytest.fixture
def index(pg_index, pg_kwargs):
    """Reset the table to the seed rows before each test."""
    import psycopg2
    from pgvector.psycopg2 import register_vector
    conn = psycopg2.connect(**pg_kwargs)
    conn.set_session(autocommit=True)
    register_vector(conn)
    cur = conn.cursor()
    cur.execute(f'TRUNCATE graphrag.{pg_index._table};')  # nosec B608 - test-local identifier
    for cid, meta, emb in SEED_ROWS:
        cur.execute(
            f'INSERT INTO graphrag.{pg_index._table} (chunkId, value, metadata, embedding) '  # nosec B608 - test-local identifier
            'VALUES (%s, %s, %s, %s);',
            (cid, cid, json.dumps(meta), np.array(emb)),
        )
    conn.close()
    return pg_index


def _top_k(index, filter_config):
    bundle = QueryBundle(query_str='q', embedding=[1.0, 0.0, 0.0])
    original = pvi.to_embedded_query
    pvi.to_embedded_query = lambda qb, model: bundle
    try:
        return index.top_k(bundle, top_k=10, filter_config=filter_config)
    finally:
        pvi.to_embedded_query = original


def _eq(key, value):
    return FilterConfig(source_filters=MetadataFilters(
        filters=[MetadataFilter(key=key, value=value, operator=FilterOperator.EQ)],
        condition=FilterCondition.AND))


def _row_count(pg_kwargs, table, where=''):
    import psycopg2
    conn = psycopg2.connect(**pg_kwargs)
    cur = conn.cursor()
    cur.execute(f'SELECT count(*) FROM graphrag.{table} {where};')  # nosec B608 - test-local
    count = cur.fetchone()[0]
    conn.close()
    return count


def test_legit_text_filter_resolves(index):
    results = _top_k(index, _eq('category', 'tech'))
    assert [r['source']['metadata']['category'] for r in results] == ['tech']


def test_injected_filter_value_matches_nothing(index):
    assert _top_k(index, _eq('category', VALUE_PAYLOAD)) == []


def test_injected_filter_key_matches_nothing(index):
    # The fix binds the metadata key into the JSON path (->>%s); a crafted key
    # must resolve to a missing property, not break out of the clause.
    assert _top_k(index, _eq(KEY_PAYLOAD, 'tech')) == []


def test_injected_id_does_not_delete(index, pg_kwargs):
    index.delete_embeddings(ids=[ID_PAYLOAD])
    assert _row_count(pg_kwargs, index._table) == 2


def test_injected_id_does_not_update_versioning(index, pg_kwargs):
    index.update_versioning(versioning_timestamp=999, ids=[ID_PAYLOAD])
    assert _row_count(pg_kwargs, index._table, 'WHERE valid_to = 999') == 0


def test_pre_fix_value_interpolation_would_break_out(index, pg_kwargs):
    """Red-state: the old interpolated filter value bypasses on the same engine,
    so the inert results above are the fix working, not a lenient engine."""
    import psycopg2
    conn = psycopg2.connect(**pg_kwargs)
    cur = conn.cursor()
    old_where = "(((metadata->'source'->'metadata'->>'category')::text = '%s'))" % VALUE_PAYLOAD
    cur.execute(f"SELECT chunkId FROM graphrag.{index._table} WHERE {old_where};")  # nosec B608 - red-state demo
    bypassed = [r[0] for r in cur.fetchall()]
    conn.close()
    assert len(bypassed) == 2
