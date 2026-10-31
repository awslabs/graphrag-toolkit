# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live check that the Postgres vector store connection requires TLS by default.

The unit tests assert on the arguments passed to psycopg2.connect. They can't
show that libpq refuses a server that has no TLS, which is the point of the
default. This runs against a pgvector server that does not offer TLS.

Skipped unless PG_TEST_URI is set. To run locally:

    finch run -d --name pg-tls-test -p 5432:5432 \\
        -e POSTGRES_PASSWORD=password -e POSTGRES_DB=graphrag pgvector/pgvector:pg16
    PG_TEST_URI=postgresql://postgres:password@localhost:5432/graphrag \\
        pytest tests/integration/storage/vector/test_pg_vector_tls_live.py
    finch stop pg-tls-test && finch rm pg-tls-test

Run it on its own. The unit tests replace psycopg2 with a fake for the whole
session, and this file skips itself when it finds that fake.
"""

import os

import pytest
from llama_index.core.embeddings import MockEmbedding


psycopg2 = pytest.importorskip('psycopg2')
pytest.importorskip('pgvector')

if not getattr(psycopg2, '__file__', None):
    pytest.skip('psycopg2 is replaced by a unit-test fake; run this file on its own', allow_module_level=True)

from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes import PGIndex

PG_TEST_URI = os.environ.get('PG_TEST_URI')

pytestmark = pytest.mark.skipif(
    not PG_TEST_URI,
    reason='set PG_TEST_URI to a running Postgres without TLS to run this live test',
)


@pytest.fixture(autouse=True)
def no_pgsslmode(monkeypatch):
    monkeypatch.delenv('PGSSLMODE', raising=False)


@pytest.fixture(scope='module')
def vector_extension():
    conn = psycopg2.connect(PG_TEST_URI, sslmode='disable')
    conn.autocommit = True
    conn.cursor().execute('CREATE EXTENSION IF NOT EXISTS vector')
    conn.close()


def _index(**kwargs):
    index = PGIndex.for_index('chunk', PG_TEST_URI, embed_model=MockEmbedding(embed_dim=3), dimensions=3, **kwargs)
    index.writeable = False
    return index


def test_default_connection_is_refused_without_tls(vector_extension):
    with pytest.raises(psycopg2.OperationalError, match='server does not support SSL') as excinfo:
        _index()._get_connection()
    assert 'sslmode=disable' in str(excinfo.value)


def test_sslmode_disable_connects_without_tls(vector_extension):
    conn = _index(sslmode='disable')._get_connection()
    conn.close()


def test_pgsslmode_overrides_the_default(vector_extension, monkeypatch):
    monkeypatch.setenv('PGSSLMODE', 'disable')
    conn = _index()._get_connection()
    conn.close()
