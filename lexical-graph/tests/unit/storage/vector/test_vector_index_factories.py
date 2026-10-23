# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for vector index factory try_create methods.

Covers:
  - OpenSearchVectorIndexFactory.try_create (aoss://, https://...aoss, http(s):// endpoint, no match)
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
            # a bare AOSS domain must stay on SigV4 — this branch is matched ahead of
            # the generic https:// branch, so the ordering is load-bearing
            _, kwargs = mock_cls.for_index.call_args
            assert kwargs.get("is_sigv4_auth") is True

    def test_try_create_https_endpoint_returns_indexes(self):
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
                ["chunk", "statement"],
                "https://localhost:9200"
            )
            assert result is not None
            assert len(result) == 2
            args, kwargs = mock_cls.for_index.call_args
            # endpoint passed through verbatim, no prefix stripping, non-AOSS auth
            assert args[1] == "https://localhost:9200"
            assert kwargs.get("is_sigv4_auth") is False

    def test_try_create_http_endpoint_passes_through_and_sets_non_sigv4(self):
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        mock_cls = MagicMock()
        mock_cls.for_index.return_value = MagicMock()

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes": MagicMock(
                OpenSearchIndex=mock_cls
            )
        }):
            factory = OpenSearchVectorIndexFactory()
            factory.try_create(["chunk"], "http://localhost:9200")
            args, kwargs = mock_cls.for_index.call_args
            assert args[1] == "http://localhost:9200"
            assert kwargs.get("is_sigv4_auth") is False

    def test_try_create_schemeless_endpoint_returns_none(self):
        # Without the opensearch:// prefix, a schemeless endpoint is no longer claimed
        # by this factory; the caller must supply an http:// or https:// scheme.
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        factory = OpenSearchVectorIndexFactory()
        assert factory.try_create(["chunk"], "localhost:9200") is None

    def test_try_create_opensearch_prefix_no_longer_matched(self):
        # opensearch:// was removed; such a string should fall through (return None)
        # so the dispatch reports it as unrecognized rather than mis-routing.
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        factory = OpenSearchVectorIndexFactory()
        assert factory.try_create(["chunk"], "opensearch://localhost:9200") is None

    def test_try_create_aoss_prefix_sets_is_sigv4_auth_true(self):
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        mock_cls = MagicMock()
        mock_cls.for_index.return_value = MagicMock()

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes": MagicMock(
                OpenSearchIndex=mock_cls
            )
        }):
            factory = OpenSearchVectorIndexFactory()
            factory.try_create(["chunk"], "aoss://https://abc.us-east-1.aoss.amazonaws.com")
            _, kwargs = mock_cls.for_index.call_args
            assert kwargs.get("is_sigv4_auth") is True

    def test_try_create_aoss_prefix_empty_endpoint_raises(self):
        import pytest
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        factory = OpenSearchVectorIndexFactory()
        with pytest.raises(ValueError, match="Empty endpoint"):
            factory.try_create(["chunk"], "aoss://")

    def test_try_create_caller_supplied_is_sigv4_auth_does_not_collide(self):
        from graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_index_factory import OpenSearchVectorIndexFactory

        mock_cls = MagicMock()
        mock_cls.for_index.return_value = MagicMock()

        with patch.dict("sys.modules", {
            "graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes": MagicMock(
                OpenSearchIndex=mock_cls
            )
        }):
            factory = OpenSearchVectorIndexFactory()
            factory.try_create(["chunk"], "https://localhost:9200", is_sigv4_auth=True)
            _, kwargs = mock_cls.for_index.call_args
            assert kwargs.get("is_sigv4_auth") is False


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
