# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for AOSS NextGen index mappings in index_exists().

Covers:
  - NextGen mapping omits the `method` block and moves `space_type` to the
    field level, with no `compression_level` unless configured
  - NextGen mapping includes `compression_level` when configured
  - Classic (faiss/nmslib) mappings are unchanged when nextgen is disabled
"""

import sys
import types
from unittest.mock import MagicMock, patch


def _install_opensearch_mocks():
    import llama_index

    fake_exceptions_mod = types.ModuleType("opensearchpy.exceptions")

    class _NotFoundError(Exception):
        def __init__(self, status_code=404, error="not found", info=None):
            super().__init__(error)
            self.status_code = status_code

    class _RequestError(Exception):
        def __init__(self, status_code=400, error="illegal_argument_exception", info=None):
            super().__init__(error)
            self.status_code = status_code
            self.error = error
            self.info = info

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

    return _RequestError


_RequestError = _install_opensearch_mocks()

import graphrag_toolkit.lexical_graph.storage.vector.opensearch_vector_indexes as ovi  # noqa: E402


def _capture_index_create_body(nextgen, compression=None, engine='nmslib'):
    """Run index_exists() with a mocked OpenSearch client; return the create() body."""
    mock_client = MagicMock()
    mock_client.indices.exists.return_value = False

    with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
        mock_cfg.opensearch_serverless_nextgen = nextgen
        mock_cfg.opensearch_serverless_nextgen_compression = compression
        mock_cfg.opensearch_engine = engine
        with patch.object(ovi, "create_os_client", return_value=mock_client):
            ovi.index_exists("https://endpoint", "chunk", 1024, writeable=True)

    _, kwargs = mock_client.indices.create.call_args
    return kwargs["body"]


class TestNextGenMapping:

    def test_no_method_block(self):
        body = _capture_index_create_body(nextgen=True)
        embedding = body["mappings"]["properties"]["embedding"]
        assert "method" not in embedding

    def test_space_type_at_field_level(self):
        body = _capture_index_create_body(nextgen=True)
        embedding = body["mappings"]["properties"]["embedding"]
        assert embedding["space_type"] == "l2"

    def test_no_ef_search_setting(self):
        body = _capture_index_create_body(nextgen=True)
        assert "knn.algo_param.ef_search" not in body["settings"]["index"]

    def test_no_compression_level_by_default(self):
        body = _capture_index_create_body(nextgen=True)
        embedding = body["mappings"]["properties"]["embedding"]
        assert "compression_level" not in embedding

    def test_compression_level_when_configured(self):
        body = _capture_index_create_body(nextgen=True, compression="8x")
        embedding = body["mappings"]["properties"]["embedding"]
        assert embedding["compression_level"] == "8x"

    def test_dimension_preserved(self):
        body = _capture_index_create_body(nextgen=True)
        embedding = body["mappings"]["properties"]["embedding"]
        assert embedding["dimension"] == 1024


class TestClassicMappingUnaffected:

    def test_nmslib_method_unchanged_when_nextgen_disabled(self):
        body = _capture_index_create_body(nextgen=False, engine='nmslib')
        embedding = body["mappings"]["properties"]["embedding"]
        assert embedding["method"]["engine"] == "nmslib"
        assert body["settings"]["index"]["knn.algo_param.ef_search"] == 100

    def test_faiss_method_unchanged_when_nextgen_disabled(self):
        body = _capture_index_create_body(nextgen=False, engine='faiss')
        embedding = body["mappings"]["properties"]["embedding"]
        assert embedding["method"]["engine"] == "faiss"
        assert "knn.algo_param.ef_search" not in body["settings"]["index"]


def _run_index_exists_with_create_error(nextgen, request_error):
    """Run index_exists() where client.indices.create() raises request_error; return (result, raised)."""
    mock_client = MagicMock()
    mock_client.indices.exists.return_value = False
    mock_client.indices.create.side_effect = request_error

    with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
        mock_cfg.opensearch_serverless_nextgen = nextgen
        mock_cfg.opensearch_serverless_nextgen_compression = None
        mock_cfg.opensearch_engine = 'nmslib'
        # `ovi.RequestError` is bound at whichever sibling test module first imports `ovi` in
        # this test session, so its class identity isn't reliable here. Patch it to our own
        # class for the duration of the call so `except RequestError` in the source matches.
        with patch.object(ovi, "create_os_client", return_value=mock_client), \
             patch.object(ovi, "RequestError", _RequestError):
            try:
                return ovi.index_exists("https://endpoint", "chunk", 1024, writeable=True), None
            except ValueError as e:
                return None, e


# Shape observed live against a real AOSS NextGen collection rejecting a Classic mapping.
_ENGINE_REJECTED_INFO = {
    'error': {
        'root_cause': [{
            'type': 'illegal_argument_exception',
            'reason': "OpenSearch exception [type=illegal_argument_exception, reason=Field parameter 'engine' is not supported]- server : [envoy]",
        }],
        'type': 'illegal_argument_exception',
        'reason': "OpenSearch exception [type=illegal_argument_exception, reason=Field parameter 'engine' is not supported]- server : [envoy]",
    },
    'status': 400,
}


class TestNextGenIncompatibleFieldError:

    def test_raises_constructive_error_for_engine_field_rejection(self):
        e = _RequestError(400, 'illegal_argument_exception', _ENGINE_REJECTED_INFO)
        result, raised = _run_index_exists_with_create_error(nextgen=False, request_error=e)
        assert raised is not None, "expected a ValueError to be raised"
        assert 'opensearch_serverless_nextgen' in str(raised)
        assert raised.__cause__ is e

    def test_raises_constructive_error_for_mode_field_rejection(self):
        info = {'error': {'reason': "Field parameter 'mode' is not supported"}}
        e = _RequestError(400, 'illegal_argument_exception', info)
        result, raised = _run_index_exists_with_create_error(nextgen=False, request_error=e)
        assert raised is not None, "expected a ValueError to be raised"
        assert 'opensearch_serverless_nextgen' in str(raised)

    def test_does_not_raise_when_nextgen_already_enabled(self):
        # Our NextGen mapping never sends 'engine'/'mode', so this scenario shouldn't occur in
        # practice, but the guard must not misfire and mask a genuinely different failure.
        e = _RequestError(400, 'illegal_argument_exception', _ENGINE_REJECTED_INFO)
        result, raised = _run_index_exists_with_create_error(nextgen=True, request_error=e)
        assert raised is None
        assert result is False

    def test_unrelated_illegal_argument_error_not_raised(self):
        info = {'error': {'reason': "some unrelated validation failure"}}
        e = _RequestError(400, 'illegal_argument_exception', info)
        result, raised = _run_index_exists_with_create_error(nextgen=False, request_error=e)
        assert raised is None
        assert result is False

    def test_unrelated_error_type_not_raised(self):
        e = _RequestError(409, 'some_other_exception', {'error': {'reason': "Field parameter 'engine' is not supported"}})
        result, raised = _run_index_exists_with_create_error(nextgen=False, request_error=e)
        assert raised is None
        assert result is False


class TestIndexExistsLifecycle:
    """Pre-existing index_exists() behavior, unrelated to NextGen, exercised here since the
    NextGen work touches the same function and this behavior had no test coverage before."""

    def test_index_already_exists_skips_create(self):
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = True

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_serverless_nextgen = False
            mock_cfg.opensearch_engine = 'nmslib'
            with patch.object(ovi, "create_os_client", return_value=mock_client):
                result = ovi.index_exists("https://endpoint", "chunk", 1024, writeable=True)

        assert result is True
        mock_client.indices.create.assert_not_called()
        mock_client.close.assert_called_once()

    def test_resource_already_exists_exception_handled_without_raising(self):
        e = _RequestError(400, 'resource_already_exists_exception', {'error': {'reason': 'already exists'}})
        result, raised = _run_index_exists_with_create_error(nextgen=False, request_error=e)
        assert raised is None
        assert result is False

    def test_client_closed_even_when_create_raises(self):
        e = _RequestError(400, 'illegal_argument_exception', {'error': {'reason': "Field parameter 'engine' is not supported"}})
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        mock_client.indices.create.side_effect = e

        with patch.object(ovi, "GraphRAGConfig") as mock_cfg:
            mock_cfg.opensearch_serverless_nextgen = False
            mock_cfg.opensearch_serverless_nextgen_compression = None
            mock_cfg.opensearch_engine = 'nmslib'
            with patch.object(ovi, "create_os_client", return_value=mock_client), \
                 patch.object(ovi, "RequestError", _RequestError):
                try:
                    ovi.index_exists("https://endpoint", "chunk", 1024, writeable=True)
                except ValueError:
                    pass

        mock_client.close.assert_called_once()


class TestRequestErrorReason:

    def test_falls_back_to_str_when_info_is_not_a_dict(self):
        assert ovi._request_error_reason(_RequestError(400, 'illegal_argument_exception', "plain string info")) == "plain string info"

    def test_falls_back_to_empty_string_when_info_is_none(self):
        assert ovi._request_error_reason(_RequestError(400, 'illegal_argument_exception', None)) == ""
