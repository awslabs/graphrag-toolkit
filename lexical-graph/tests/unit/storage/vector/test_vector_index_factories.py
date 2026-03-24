# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for vector index factory try_create methods.

Covers:
  - OpenSearchVectorIndexFactory.try_create (aoss://, https://...aoss, no match)
  - PGVectorIndexFactory.try_create (postgres://, postgresql://, no match)
  - S3VectorIndexFactory.try_create (s3vectors://, no match)
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# OpenSearchVectorIndexFactory.try_create
# ---------------------------------------------------------------------------

class TestOpenSearchVectorIndexFactory:

    def test_try_create_aoss_prefix_returns_indexes(self):
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        mock_index = MagicMock()
        with patch(
            "graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory.OpenSearchIndex",
            create=True
        ) as mock_cls:
            mock_cls.for_index.return_value = mock_index
            # Patch the import inside try_create
            with patch.dict("sys.modules", {
                "graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes": MagicMock(
                    OpenSearchIndex=mock_cls
                )
            }):
                factory = OpenSearchVectorIndexFactory()
                result = factory.try_create(
                    ["chunk", "statement"],
                    "aoss://https://abc.us-east-1.aoss.amazonaws.com"
                )
                assert result is not None
                assert len(result) == 2

    def test_try_create_non_matching_returns_none(self):
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory
        factory = OpenSearchVectorIndexFactory()
        result = factory.try_create(["chunk"], "neptune-graph://some-id")
        assert result is None

    def test_try_create_https_aoss_dns_returns_indexes(self):
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        mock_index = MagicMock()
        mock_cls = MagicMock()
        mock_cls.for_index.return_value = mock_index

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes": MagicMock(
                OpenSearchIndex=mock_cls
            )
        }):
            factory = OpenSearchVectorIndexFactory()
            result = factory.try_create(
                ["chunk"],
                "https://abc.us-east-1.aoss.amazonaws.com"
            )
            assert result is not None


# ---------------------------------------------------------------------------
# PGVectorIndexFactory.try_create
# ---------------------------------------------------------------------------

class TestPGVectorIndexFactory:

    def test_try_create_postgres_prefix_returns_indexes(self):
        from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_index_factory import PGVectorIndexFactory

        mock_index = MagicMock()
        mock_cls = MagicMock()
        mock_cls.for_index.return_value = mock_index

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes": MagicMock(
                PGIndex=mock_cls
            )
        }):
            factory = PGVectorIndexFactory()
            result = factory.try_create(
                ["chunk", "statement"],
                "postgres://user:pass@localhost:5432/mydb"
            )
            assert result is not None
            assert len(result) == 2

    def test_try_create_postgresql_prefix_returns_indexes(self):
        from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_index_factory import PGVectorIndexFactory

        mock_index = MagicMock()
        mock_cls = MagicMock()
        mock_cls.for_index.return_value = mock_index

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.pg_vector_indexes": MagicMock(
                PGIndex=mock_cls
            )
        }):
            factory = PGVectorIndexFactory()
            result = factory.try_create(
                ["chunk"],
                "postgresql://user:pass@localhost:5432/mydb"
            )
            assert result is not None

    def test_try_create_non_matching_returns_none(self):
        from graphrag_toolkit.lexical_graph.storage.vector.pg_vector_index_factory import PGVectorIndexFactory
        factory = PGVectorIndexFactory()
        result = factory.try_create(["chunk"], "neptune-graph://some-id")
        assert result is None


# ---------------------------------------------------------------------------
# S3VectorIndexFactory.try_create
# ---------------------------------------------------------------------------

class TestS3VectorIndexFactory:

    def test_try_create_s3vectors_prefix_returns_indexes(self):
        from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_index_factory import S3VectorIndexFactory

        mock_index = MagicMock()
        mock_cls = MagicMock()
        mock_cls.for_index.return_value = mock_index

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes": MagicMock(
                S3VectorIndex=mock_cls
            )
        }):
            factory = S3VectorIndexFactory()
            result = factory.try_create(
                ["chunk", "statement"],
                "s3vectors://my-bucket/my-prefix"
            )
            assert result is not None
            assert len(result) == 2

    def test_try_create_non_matching_returns_none(self):
        from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_index_factory import S3VectorIndexFactory
        factory = S3VectorIndexFactory()
        result = factory.try_create(["chunk"], "neptune-graph://some-id")
        assert result is None


# ---------------------------------------------------------------------------
# S3VectorIndex.client property
# ---------------------------------------------------------------------------

class TestS3VectorIndexClientProperty:

    def _make_index(self):
        from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes import S3VectorIndex
        index = S3VectorIndex.model_construct(
            index_name="chunk",
            bucket_name="my-bucket",
            prefix=None,
            kms_key_arn=None,
            embed_model=MagicMock(),
            dimensions=1024,
            writeable=False,
            initialized=True,
            _client=None,
        )
        return index

    def test_client_creates_lazily_via_session(self):
        from graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes import S3VectorIndex

        index = self._make_index()
        mock_s3vectors = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3vectors

        with patch(
            "graphrag_toolkit.lexical_graph.storage.vector.s3_vector_indexes.GraphRAGConfig"
        ) as mock_cfg:
            mock_cfg.session = mock_session
            with patch.object(index, "_init_index"):
                result = index.client
                assert result is mock_s3vectors
                mock_session.client.assert_called_once_with("s3vectors")

    def test_client_cached_on_second_access(self):
        index = self._make_index()
        existing = MagicMock()
        index._client = existing
        assert index.client is existing
