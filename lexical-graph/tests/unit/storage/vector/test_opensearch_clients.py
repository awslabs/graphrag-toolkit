# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for opensearch_vector_indexes client helpers.

Covers:
  - create_os_client (returns OpenSearch instance)
  - create_os_async_client (returns AsyncOpenSearch instance)
  - create_opensearch_vector_client (success, NotFoundError retry)
  - OpenSearchIndex.client property (index exists, index does not exist)
  - create_os_client/create_os_async_client is_local path (basic auth, no auth, no SigV4)
"""

import sys
import types
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Bootstrap: inject fake opensearch modules before the source module loads
# ---------------------------------------------------------------------------

def _install_opensearch_mocks():
    """Install fake opensearch modules into sys.modules so the source can import."""
    import llama_index

    fake_exceptions_mod = types.ModuleType("opensearchpy.exceptions")

    class _NotFoundError(Exception):
        def __init__(self, status_code=404, error="not found", info=None):
            super().__init__(error)
            self.status_code = status_code

    class _RequestError(Exception):
        pass

    fake_exceptions_mod.NotFoundError = _NotFoundError
    fake_exceptions_mod.RequestError = _RequestError

    fake_opensearch_mod = types.ModuleType("opensearchpy")
    fake_opensearch_mod.OpenSearch = MagicMock(name="OpenSearch")
    fake_opensearch_mod.AsyncOpenSearch = MagicMock(name="AsyncOpenSearch")
    fake_opensearch_mod.AWSV4SignerAsyncAuth = MagicMock(name="AWSV4SignerAsyncAuth")
    fake_opensearch_mod.AsyncHttpConnection = MagicMock(name="AsyncHttpConnection")
    fake_opensearch_mod.Urllib3AWSV4SignerAuth = MagicMock(name="Urllib3AWSV4SignerAuth")
    fake_opensearch_mod.Urllib3HttpConnection = MagicMock(name="Urllib3HttpConnection")
    fake_opensearch_mod.exceptions = fake_exceptions_mod

    fake_llama_vs_mod = types.ModuleType("llama_index.vector_stores")
    fake_llama_os_mod = types.ModuleType("llama_index.vector_stores.opensearch")

    class _FakeOpensearchVectorClient:
        _get_opensearch_version = None
        _bulk_ingest_embeddings = None

    fake_llama_os_mod.OpensearchVectorClient = _FakeOpensearchVectorClient
    fake_llama_vs_mod.opensearch = fake_llama_os_mod
    llama_index.vector_stores = fake_llama_vs_mod

    sys.modules["opensearchpy"] = fake_opensearch_mod
    sys.modules["opensearchpy.exceptions"] = fake_exceptions_mod
    sys.modules["llama_index.vector_stores"] = fake_llama_vs_mod
    sys.modules["llama_index.vector_stores.opensearch"] = fake_llama_os_mod

    return _NotFoundError


_NotFoundError = _install_opensearch_mocks()

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


# ---------------------------------------------------------------------------
# create_os_client / create_os_async_client: is_local path
# ---------------------------------------------------------------------------

class TestCreateOsClientLocal:

    def test_local_with_credentials_uses_basic_auth(self):
        mock_os_instance = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = "admin"
            mock_cfg.opensearch_password = "secret"
            with patch.object(ovi, "OpenSearch", return_value=mock_os_instance) as mock_os:
                result = ovi.create_os_client("https://localhost:9200", is_local=True)
                assert result is mock_os_instance
                _, kwargs = mock_os.call_args
                assert kwargs["http_auth"] == ("admin", "secret")

    def test_local_without_credentials_uses_no_auth(self):
        mock_os_instance = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = None
            mock_cfg.opensearch_password = None
            with patch.object(ovi, "OpenSearch", return_value=mock_os_instance) as mock_os:
                ovi.create_os_client("https://localhost:9200", is_local=True)
                _, kwargs = mock_os.call_args
                assert kwargs["http_auth"] is None

    def test_local_with_only_username_warns_and_uses_no_auth(self, caplog):
        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = "admin"
            mock_cfg.opensearch_password = None
            with patch.object(ovi, "OpenSearch") as mock_os:
                with caplog.at_level("WARNING", logger="graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes"):
                    ovi.create_os_client("https://localhost:9200", is_local=True)
                _, kwargs = mock_os.call_args
                assert kwargs["http_auth"] is None
                assert any("Only one of" in r.message for r in caplog.records)

    def test_local_with_only_password_warns_and_uses_no_auth(self, caplog):
        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = None
            mock_cfg.opensearch_password = "secret"
            with patch.object(ovi, "OpenSearch") as mock_os:
                with caplog.at_level("WARNING", logger="graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes"):
                    ovi.create_os_client("https://localhost:9200", is_local=True)
                _, kwargs = mock_os.call_args
                assert kwargs["http_auth"] is None
                assert any("Only one of" in r.message for r in caplog.records)

    def test_local_never_touches_aws_session_or_sigv4(self):
        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = None
            mock_cfg.opensearch_password = None
            with patch.object(ovi, "OpenSearch"):
                with patch.object(ovi, "Urllib3AWSV4SignerAuth") as mock_sigv4:
                    ovi.create_os_client("https://localhost:9200", is_local=True)
                    mock_sigv4.assert_not_called()
                    mock_cfg.session.assert_not_called()

    def test_default_is_local_false_preserves_sigv4(self):
        mock_session = MagicMock()
        mock_session.get_credentials.return_value = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.session = mock_session
            mock_cfg.aws_region = "us-east-1"
            with patch.object(ovi, "OpenSearch"):
                with patch.object(ovi, "Urllib3AWSV4SignerAuth") as mock_sigv4:
                    ovi.create_os_client("https://my-endpoint.aoss.amazonaws.com")
                    mock_sigv4.assert_called_once()


class TestCreateOsAsyncClientLocal:

    def test_local_with_credentials_uses_basic_auth(self):
        mock_async_instance = MagicMock()

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = "admin"
            mock_cfg.opensearch_password = "secret"
            with patch.object(ovi, "AsyncOpenSearch", return_value=mock_async_instance) as mock_async:
                result = ovi.create_os_async_client("https://localhost:9200", is_local=True)
                assert result is mock_async_instance
                _, kwargs = mock_async.call_args
                assert kwargs["http_auth"] == ("admin", "secret")

    def test_local_without_credentials_uses_no_auth(self):
        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = None
            mock_cfg.opensearch_password = None
            with patch.object(ovi, "AsyncOpenSearch") as mock_async:
                ovi.create_os_async_client("https://localhost:9200", is_local=True)
                _, kwargs = mock_async.call_args
                assert kwargs["http_auth"] is None

    def test_local_never_touches_aws_session_or_sigv4(self):
        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_username = None
            mock_cfg.opensearch_password = None
            with patch.object(ovi, "AsyncOpenSearch"):
                with patch.object(ovi, "AWSV4SignerAsyncAuth") as mock_sigv4:
                    ovi.create_os_async_client("https://localhost:9200", is_local=True)
                    mock_sigv4.assert_not_called()
                    mock_cfg.session.assert_not_called()


class TestCreateOpensearchVectorClientLocal:

    def test_is_local_threaded_to_both_clients(self):
        with patch.object(ovi, "OpensearchVectorClient", return_value=MagicMock()):
            with patch.object(ovi, "create_os_client", return_value=MagicMock()) as mock_sync:
                with patch.object(ovi, "create_os_async_client", return_value=MagicMock()) as mock_async:
                    with patch.object(ovi, "index_is_available", return_value=True):
                        ovi.create_opensearch_vector_client(
                            "https://localhost:9200", "my-index", 1024, MagicMock(), is_local=True
                        )
                        assert mock_sync.call_args.kwargs.get("is_local") is True
                        assert mock_async.call_args.kwargs.get("is_local") is True

    def test_is_local_sets_non_aoss_dummy_auth_service(self):
        captured = {}

        def fake_vector_client(*args, **kwargs):
            captured["http_auth"] = kwargs.get("http_auth")
            return MagicMock()

        with patch.object(ovi, "OpensearchVectorClient", side_effect=fake_vector_client):
            with patch.object(ovi, "create_os_client", return_value=MagicMock()):
                with patch.object(ovi, "create_os_async_client", return_value=MagicMock()):
                    with patch.object(ovi, "index_is_available", return_value=True):
                        ovi.create_opensearch_vector_client(
                            "https://localhost:9200", "my-index", 1024, MagicMock(), is_local=True
                        )
                        assert captured["http_auth"].service != "aoss"

    def test_default_is_local_false_keeps_aoss_dummy_auth_service(self):
        captured = {}

        def fake_vector_client(*args, **kwargs):
            captured["http_auth"] = kwargs.get("http_auth")
            return MagicMock()

        with patch.object(ovi, "OpensearchVectorClient", side_effect=fake_vector_client):
            with patch.object(ovi, "create_os_client", return_value=MagicMock()):
                with patch.object(ovi, "create_os_async_client", return_value=MagicMock()):
                    with patch.object(ovi, "index_is_available", return_value=True):
                        ovi.create_opensearch_vector_client(
                            "https://endpoint", "my-index", 1024, MagicMock()
                        )
                        assert captured["http_auth"].service == "aoss"


class TestOpenSearchIndexClientPropertyLocal:

    def _make_local_index(self):
        from graphrag_toolkit.lexical_graph.tenant_id import TenantId
        return ovi.OpenSearchIndex.model_construct(
            index_name="chunk",
            endpoint="https://localhost:9200",
            dimensions=1024,
            embed_model=MagicMock(),
            tenant_id=TenantId(),
            writeable=False,
            is_local=True,
            _client=None,
        )

    def test_client_property_threads_is_local_to_index_exists(self):
        index = self._make_local_index()

        with patch.object(ovi, "index_exists", return_value=False) as mock_index_exists:
            index.client
            assert mock_index_exists.call_args.kwargs.get("is_local") is True

    def test_client_property_threads_is_local_to_vector_client(self):
        index = self._make_local_index()
        mock_vector_client = MagicMock()

        with patch.object(ovi, "index_exists", return_value=True):
            with patch.object(ovi, "create_opensearch_vector_client", return_value=mock_vector_client) as mock_create:
                index.client
                assert mock_create.call_args.kwargs.get("is_local") is True

    def test_client_property_threads_client_kwargs_to_index_exists(self):
        from graphrag_toolkit.lexical_graph.tenant_id import TenantId
        index = ovi.OpenSearchIndex.model_construct(
            index_name="chunk",
            endpoint="http://localhost:9200",
            dimensions=1024,
            embed_model=MagicMock(),
            tenant_id=TenantId(),
            writeable=False,
            is_local=True,
            client_kwargs={"use_ssl": False, "verify_certs": False},
            _client=None,
        )

        with patch.object(ovi, "index_exists", return_value=False) as mock_index_exists:
            index.client
            assert mock_index_exists.call_args.kwargs.get("client_kwargs") == {"use_ssl": False, "verify_certs": False}


# ---------------------------------------------------------------------------
# index_exists: client_kwargs passthrough (plain-HTTP / self-signed local endpoints)
# ---------------------------------------------------------------------------

class TestIndexExistsClientKwargs:

    def _mock_client(self):
        client = MagicMock()
        client.indices.exists.return_value = True
        return client

    def test_default_client_kwargs_none_still_sets_pool_maxsize(self):
        with patch.object(ovi, "create_os_client", return_value=self._mock_client()) as mock_create:
            ovi.index_exists("https://endpoint", "my-index", 1024, writeable=False)
            _, kwargs = mock_create.call_args
            assert kwargs["pool_maxsize"] == 1

    def test_client_kwargs_forwarded_to_create_os_client(self):
        with patch.object(ovi, "create_os_client", return_value=self._mock_client()) as mock_create:
            ovi.index_exists(
                "http://localhost:9200", "my-index", 1024, writeable=False, is_local=True,
                client_kwargs={"use_ssl": False, "verify_certs": False},
            )
            _, kwargs = mock_create.call_args
            assert kwargs["use_ssl"] is False
            assert kwargs["verify_certs"] is False
            assert kwargs["is_local"] is True

    def test_client_kwargs_can_override_default_pool_maxsize(self):
        with patch.object(ovi, "create_os_client", return_value=self._mock_client()) as mock_create:
            ovi.index_exists(
                "https://endpoint", "my-index", 1024, writeable=False,
                client_kwargs={"pool_maxsize": 10},
            )
            _, kwargs = mock_create.call_args
            assert kwargs["pool_maxsize"] == 10
