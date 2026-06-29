# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Red-state tests for SQL injection in the PGVector store.

These tests pin the mitigation: metadata filter values, filter keys, and id
lists must reach psycopg2 as bound parameters (or, for identifiers, be
validated/escaped) rather than being string-interpolated into the SQL text.

Each test drives a real sink (`top_k`, `get_embeddings`, `update_versioning`,
`delete_embeddings`) against a mocked cursor and inspects the exact arguments
passed to `cur.execute`. The attacker payload must NOT appear as an inline
literal in the SQL string. While the values are interpolated (pre-fix), these
tests FAIL, demonstrating the payload reaches the SQL sink unescaped.
"""

import sys
import types
import numpy as np
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Bootstrap: inject fake psycopg2/pgvector modules before source module loads
# (mirrors test_pg_vector_connection.py)
# ---------------------------------------------------------------------------

def _install_pg_mocks():
    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = MagicMock()
    fake_psycopg2.errors = types.ModuleType("psycopg2.errors")
    fake_psycopg2.errors.UniqueViolation = Exception
    fake_psycopg2.errors.UndefinedTable = Exception
    fake_psycopg2.errors.DuplicateTable = Exception

    fake_psycopg2_errors = types.ModuleType("psycopg2.errors")
    fake_psycopg2_errors.UniqueViolation = Exception
    fake_psycopg2_errors.UndefinedTable = Exception
    fake_psycopg2_errors.DuplicateTable = Exception

    fake_pgvector = types.ModuleType("pgvector")
    fake_pgvector_psycopg2 = types.ModuleType("pgvector.psycopg2")
    fake_pgvector_psycopg2.register_vector = MagicMock()

    sys.modules["psycopg2"] = fake_psycopg2
    sys.modules["psycopg2.errors"] = fake_psycopg2_errors
    sys.modules["pgvector"] = fake_pgvector
    sys.modules["pgvector.psycopg2"] = fake_pgvector_psycopg2


_install_pg_mocks()

import graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes as pvi  # noqa: E402

from graphrag_toolkit.lexical_graph.metadata import FilterConfig  # noqa: E402
from llama_index.core.schema import QueryBundle  # noqa: E402
from llama_index.core.vector_stores.types import (  # noqa: E402
    FilterCondition,
    FilterOperator,
    MetadataFilter,
    MetadataFilters,
)


# Classic boolean-tautology break-out. If interpolated into a single-quoted
# SQL literal it closes the literal and appends an always-true clause.
SQL_VALUE_PAYLOAD = "' OR '1'='1"
# Break-out for an id inside an IN (...) list.
SQL_ID_PAYLOAD = "x') OR '1'='1"
# Break-out for a metadata key interpolated into a JSON path text literal.
SQL_KEY_PAYLOAD = "a' OR '1'='1"


def _make_pg_index():
    from graphrag_toolkit.lexical_graph.tenant_id import TenantId

    return pvi.PGIndex.model_construct(
        index_name="chunk",
        database="testdb",
        schema_name="graphrag",
        host="localhost",
        port=5432,
        username="user",
        password="pass",
        dimensions=1024,
        embed_model=MagicMock(),
        enable_iam_db_auth=False,
        writeable=True,
        tenant_id=TenantId(),
        initialized=True,
    )


def _mock_conn_cursor():
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cur
    return mock_conn, mock_cur


def _captured_execute(mock_cur):
    """Return (sql_text, params) from the last cur.execute call.

    params is the raw bound-parameter sequence (empty tuple if execute was
    called with SQL only).
    """
    assert mock_cur.execute.called, 'expected the sink to call cur.execute'
    args, _ = mock_cur.execute.call_args
    sql_text = args[0]
    params = tuple(args[1]) if len(args) > 1 else ()
    return sql_text, params


def _bound(value, params):
    """True if value is bound as a parameter (string-safe; avoids np.array eq)."""
    return any(isinstance(p, str) and p == value for p in params)


def _eq_filter_config(key, value):
    return FilterConfig(
        source_filters=MetadataFilters(
            filters=[MetadataFilter(key=key, value=value, operator=FilterOperator.EQ)],
            condition=FilterCondition.AND,
        )
    )


class TestTopKFilterInjection:
    """top_k() builds its WHERE clause from filter_config."""

    def test_filter_value_is_bound_not_interpolated(self):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()
        bundle = QueryBundle(query_str='q', embedding=[0.1, 0.2, 0.3])

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            with patch.object(pvi, 'to_embedded_query', return_value=bundle):
                index.top_k(bundle, top_k=5, filter_config=_eq_filter_config('category', SQL_VALUE_PAYLOAD))

        sql_text, params = _captured_execute(mock_cur)
        assert SQL_VALUE_PAYLOAD not in sql_text, (
            'filter value was interpolated into the SQL text — SQL injection. '
            f'SQL: {sql_text}'
        )
        assert _bound(SQL_VALUE_PAYLOAD, params), (
            'filter value must be passed as a bound parameter to cur.execute'
        )

    def test_filter_key_does_not_break_out_of_json_path(self):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()
        bundle = QueryBundle(query_str='q', embedding=[0.1, 0.2, 0.3])

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            with patch.object(pvi, 'to_embedded_query', return_value=bundle):
                index.top_k(bundle, top_k=5, filter_config=_eq_filter_config(SQL_KEY_PAYLOAD, 'tech'))

        sql_text, _ = _captured_execute(mock_cur)
        # The raw key (with its embedded quote) must not land in the JSON-path
        # string literal, where it would close the literal and inject SQL.
        assert SQL_KEY_PAYLOAD not in sql_text, (
            'filter key was interpolated raw into the JSON path — SQL injection. '
            f'SQL: {sql_text}'
        )


class TestIdListInjection:
    """get_embeddings / update_versioning / delete_embeddings build IN (...) lists."""

    def test_delete_embeddings_ids_are_bound(self):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            index.delete_embeddings(ids=[SQL_ID_PAYLOAD])

        sql_text, params = _captured_execute(mock_cur)
        assert SQL_ID_PAYLOAD not in sql_text, (
            'id was interpolated into the DELETE IN-list — SQL injection widens to DELETE. '
            f'SQL: {sql_text}'
        )
        assert _bound(SQL_ID_PAYLOAD, params), 'id must be a bound parameter'

    def test_get_embeddings_ids_are_bound(self):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            index.get_embeddings(ids=[SQL_ID_PAYLOAD])

        sql_text, params = _captured_execute(mock_cur)
        assert SQL_ID_PAYLOAD not in sql_text, (
            'id was interpolated into the SELECT IN-list — SQL injection. '
            f'SQL: {sql_text}'
        )
        assert _bound(SQL_ID_PAYLOAD, params), 'id must be a bound parameter'

    def test_update_versioning_ids_are_bound(self):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            index.update_versioning(versioning_timestamp=123, ids=[SQL_ID_PAYLOAD])

        sql_text, params = _captured_execute(mock_cur)
        assert SQL_ID_PAYLOAD not in sql_text, (
            'id was interpolated into the UPDATE IN-list — SQL injection widens to UPDATE. '
            f'SQL: {sql_text}'
        )
        assert _bound(SQL_ID_PAYLOAD, params), 'id must be a bound parameter'


@pytest.mark.parametrize('payload', [
    "' OR '1'='1",            # boolean tautology
    "'; DROP TABLE chunk;--",  # stacked statement
    "x' --",                   # comment terminator
    "a\x00b",                  # null byte
    "’ OR ’",        # unicode right single quote
    "back\\slash",             # backslash
    "newline\nvalue",          # embedded newline
])
class TestPayloadDiversity:
    """A range of break-out payloads must all be bound, never inlined."""

    def test_filter_value_payloads_are_bound(self, payload):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()
        bundle = QueryBundle(query_str='q', embedding=[0.1, 0.2, 0.3])

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            with patch.object(pvi, 'to_embedded_query', return_value=bundle):
                index.top_k(bundle, top_k=5, filter_config=_eq_filter_config('category', payload))

        sql_text, params = _captured_execute(mock_cur)
        assert payload not in sql_text
        assert _bound(payload, params)


class TestClauseBuilderContract:
    """parse_metadata_filters_recursive must emit %s placeholders + ordered params."""

    def test_numeric_value_not_quoted_and_bound_native(self):
        filters = MetadataFilters(
            filters=[MetadataFilter(key='count', value=5, operator=FilterOperator.GT)],
            condition=FilterCondition.AND,
        )
        clause, params = pvi.parse_metadata_filters_recursive(filters)
        assert '::bigint > %s' in clause
        # native int bound, not a quoted string
        assert 5 in params
        assert "'5'" not in clause

    def test_is_empty_binds_key_and_emits_is_null(self):
        filters = MetadataFilters(
            filters=[MetadataFilter(key='archived_at', value=None, operator=FilterOperator.IS_EMPTY)],
            condition=FilterCondition.AND,
        )
        clause, params = pvi.parse_metadata_filters_recursive(filters)
        assert 'IS NULL' in clause
        assert params == ['archived_at']

    def test_and_composition_preserves_param_order(self):
        filters = MetadataFilters(
            filters=[
                MetadataFilter(key='category', value='tech', operator=FilterOperator.EQ),
                MetadataFilter(key='lang', value='en', operator=FilterOperator.EQ),
            ],
            condition=FilterCondition.AND,
        )
        clause, params = pvi.parse_metadata_filters_recursive(filters)
        assert ' AND ' in clause
        # key, value pairs interleaved left-to-right with the placeholders
        assert params == ['category', 'tech', 'lang', 'en']

    def test_none_config_returns_empty_fragment_and_params(self):
        assert pvi.filter_config_to_sql_filters(None) == ('', [])


class TestLegitimateFilterStillWorks:
    """Positive path: a normal filter must still flow through to the sink."""

    def test_normal_value_round_trips(self):
        index = _make_pg_index()
        mock_conn, mock_cur = _mock_conn_cursor()
        bundle = QueryBundle(query_str='q', embedding=[0.1, 0.2, 0.3])

        with patch.object(pvi.PGIndex, '_get_connection', return_value=mock_conn):
            with patch.object(pvi, 'to_embedded_query', return_value=bundle):
                index.top_k(bundle, top_k=5, filter_config=_eq_filter_config('category', 'tech'))

        sql_text, params = _captured_execute(mock_cur)
        # Both key and value are now bound parameters, not inline SQL.
        assert '%s' in sql_text
        assert _bound('category', params)
        assert _bound('tech', params)
