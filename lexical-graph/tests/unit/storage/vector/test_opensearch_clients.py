# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for opensearch_vector_indexes client helpers.

Covers:
  - create_os_client (returns OpenSearch instance)
  - create_os_async_client (returns AsyncOpenSearch instance)
  - create_opensearch_vector_client (success, NotFoundError retry)
  - OpenSearchIndex.client property (index exists, index does not exist)
"""

import pytest
from unittest.mock import MagicMock, patch

from ._opensearch_test_support import install_opensearch_mocks, FakeNotFoundError as _NotFoundError

install_opensearch_mocks()

# Now import the module under test (mocks are in place)
import graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes as ovi  # noqa: E402


# ---------------------------------------------------------------------------
# create_os_client
# ---------------------------------------------------------------------------

class TestCreateOsClient:

    def test_returns_opensearch_instance(self):
        mock_os_instance = MagicMock()
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            mock_cfg.aws_region = "us-east-1"
            with patch.object(ovi, "OpenSearch", return_value=mock_os_instance):
                with patch.object(ovi, "Urllib3AWSV4SignerAuth"):
                    result = ovi.create_os_client("https://my-endpoint.aoss.amazonaws.com")
                    assert result is mock_os_instance

    def test_caller_kwargs_override_defaults(self):
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            mock_cfg.aws_region = "us-east-1"
            with patch.object(ovi, "OpenSearch") as mock_os:
                with patch.object(ovi, "Urllib3AWSV4SignerAuth"):
                    ovi.create_os_client(
                        "https://my-endpoint.aoss.amazonaws.com",
                        pool_maxsize=64,
                        timeout=60,
                    )
                    _, kwargs = mock_os.call_args
                    assert kwargs['pool_maxsize'] == 64
                    assert kwargs['timeout'] == 60
                    assert kwargs['use_ssl'] is True
                    assert kwargs['max_retries'] == 10


# ---------------------------------------------------------------------------
# create_os_async_client
# ---------------------------------------------------------------------------

class TestCreateOsAsyncClient:

    def test_returns_async_opensearch_instance(self):
        mock_async_instance = MagicMock()
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            mock_cfg.aws_region = "us-east-1"
            with patch.object(ovi, "AsyncOpenSearch", return_value=mock_async_instance):
                with patch.object(ovi, "AWSV4SignerAsyncAuth"):
                    result = ovi.create_os_async_client("https://my-endpoint.aoss.amazonaws.com")
                    assert result is mock_async_instance

    def test_caller_kwargs_override_defaults(self):
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            mock_cfg.aws_region = "us-east-1"
            with patch.object(ovi, "AsyncOpenSearch") as mock_async:
                with patch.object(ovi, "AWSV4SignerAsyncAuth"):
                    ovi.create_os_async_client(
                        "https://my-endpoint.aoss.amazonaws.com",
                        pool_maxsize=64,
                        timeout=60,
                    )
                    _, kwargs = mock_async.call_args
                    assert kwargs['pool_maxsize'] == 64
                    assert kwargs['timeout'] == 60
                    assert kwargs['use_ssl'] is True
                    assert kwargs['max_retries'] == 10


# ---------------------------------------------------------------------------
# create_opensearch_vector_client
# ---------------------------------------------------------------------------

class TestCreateOpensearchVectorClient:

    def test_returns_client_when_index_available(self):
        mock_vector_client = MagicMock()

        with patch.object(ovi, "OpensearchVectorClient", return_value=mock_vector_client):
            with patch.object(ovi, "create_os_client", return_value=MagicMock()):
                with patch.object(ovi, "create_os_async_client", return_value=MagicMock()):
                    with patch.object(ovi, "index_is_available", return_value=True):
                        result = ovi.create_opensearch_vector_client(
                            "https://endpoint", "my-index", 1024, MagicMock()
                        )
                        assert result is mock_vector_client

    def test_retries_on_not_found_error_then_succeeds(self):
        mock_vector_client = MagicMock()
        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _NotFoundError(404, "not found")
            return mock_vector_client

        with patch.object(ovi, "OpensearchVectorClient", side_effect=side_effect):
            with patch.object(ovi, "create_os_client", return_value=MagicMock()):
                with patch.object(ovi, "create_os_async_client", return_value=MagicMock()):
                    with patch.object(ovi, "index_is_available", return_value=True):
                        result = ovi.create_opensearch_vector_client(
                            "https://endpoint", "my-index", 1024, MagicMock()
                        )
                        assert result is mock_vector_client
                        assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# OpenSearchIndex.client property
# ---------------------------------------------------------------------------

class TestOpenSearchIndexClientProperty:

    def _make_index(self):
        from graphrag_toolkit.lexical_graph.tenant_id import TenantId
        # Use model_construct to bypass pydantic validation for embed_model
        return ovi.OpenSearchIndex.model_construct(
            index_name="chunk",
            endpoint="https://my-endpoint.aoss.amazonaws.com",
            dimensions=1024,
            embed_model=MagicMock(),
            tenant_id=TenantId(),
            writeable=False,
            _client=None,
        )

    def test_client_returns_existing_client(self):
        index = self._make_index()
        mock_client = MagicMock()
        mock_client._index = index.underlying_index_name()
        index._client = mock_client

        result = index.client
        assert result is mock_client

    def test_client_creates_dummy_when_index_not_exists(self):
        index = self._make_index()
        index._client = None

        with patch.object(ovi, "index_exists", return_value=False):
            result = index.client
            assert isinstance(result, ovi.DummyOpensearchVectorClient)

    def test_client_creates_real_client_when_index_exists(self):
        index = self._make_index()
        index._client = None
        mock_vector_client = MagicMock()

        with patch.object(ovi, "index_exists", return_value=True):
            with patch.object(ovi, "create_opensearch_vector_client", return_value=mock_vector_client):
                result = index.client
                assert result is mock_vector_client
