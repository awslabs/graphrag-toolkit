# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for PGIndex._get_connection.

Covers:
  - Normal connection (no IAM auth)
  - IAM auth path (token generation)
  - Schema initialization on first connection (writeable=True)
  - Already initialized skips DDL
"""

import sys
import types
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Bootstrap: inject fake psycopg2/pgvector modules before source module loads
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


def _make_pg_index(writeable=True, enable_iam=False):
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
        enable_iam_db_auth=enable_iam,
        writeable=writeable,
        tenant_id=TenantId(),
        initialized=False,
    )


class TestPGIndexGetConnection:

    def _mock_psycopg2(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        return mock_conn, mock_cur

    def test_normal_connection_no_iam(self):
        index = _make_pg_index()
        mock_conn, mock_cur = self._mock_psycopg2()

        with patch.object(pvi, "psycopg2") as mock_pg:
            with patch.object(pvi, "register_vector"):
                mock_pg.connect.return_value = mock_conn
                conn = index._get_connection()
                assert conn is mock_conn
                mock_pg.connect.assert_called_once()

    def test_iam_auth_generates_token(self):
        index = _make_pg_index(enable_iam=True)
        mock_conn, mock_cur = self._mock_psycopg2()

        mock_rds = MagicMock()
        mock_rds.generate_db_auth_token.return_value = "iam-token"

        with patch.object(pvi, "psycopg2") as mock_pg:
            with patch.object(pvi, "register_vector"):
                with patch.object(pvi, "GraphRAGConfig") as mock_cfg:
                    mock_cfg.rds = mock_rds
                    mock_cfg.aws_region = "us-east-1"
                    mock_pg.connect.return_value = mock_conn
                    conn = index._get_connection()
                    assert conn is mock_conn
                    mock_rds.generate_db_auth_token.assert_called_once()

    def test_schema_initialized_on_first_writeable_connection(self):
        index = _make_pg_index(writeable=True)
        index.initialized = False
        mock_conn, mock_cur = self._mock_psycopg2()

        with patch.object(pvi, "psycopg2") as mock_pg:
            with patch.object(pvi, "register_vector"):
                mock_pg.connect.return_value = mock_conn
                mock_pg.errors = MagicMock()
                index._get_connection()
                assert mock_cur.execute.call_count >= 1
                assert index.initialized is True

    def test_already_initialized_skips_ddl(self):
        index = _make_pg_index(writeable=True)
        index.initialized = True
        mock_conn, mock_cur = self._mock_psycopg2()

        with patch.object(pvi, "psycopg2") as mock_pg:
            with patch.object(pvi, "register_vector"):
                mock_pg.connect.return_value = mock_conn
                index._get_connection()
                mock_cur.execute.assert_not_called()
