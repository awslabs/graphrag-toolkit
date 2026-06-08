# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for utils/bedrock_embedding.py (BedrockDirectEmbedding)."""

import io
import json
from unittest.mock import MagicMock, patch

import pytest

from graphrag_toolkit.lexical_graph.utils.bedrock_embedding import (
    BedrockDirectEmbedding,
    _extract_provider,
)


def _embedder(client=None, **kwargs):
    e = BedrockDirectEmbedding(model_name="amazon.titan-embed-text-v2:0", **kwargs)
    if client is not None:
        e._client = client
    return e


def _make_response(body_dict):
    return {"body": io.BytesIO(json.dumps(body_dict).encode())}


class TestExtractProvider:
    def test_standard_amazon(self):
        assert _extract_provider("amazon.titan-embed-text-v2:0") == "amazon"

    def test_standard_cohere(self):
        assert _extract_provider("cohere.embed-english-v3") == "cohere"

    def test_cross_region_profile(self):
        assert _extract_provider("us.amazon.titan-embed-text-v2:0") == "amazon"

    def test_cross_region_cohere(self):
        assert _extract_provider("eu.cohere.embed-multilingual-v3") == "cohere"


class TestGetTextEmbeddingTitan:
    def test_titan_response_format(self):
        client = MagicMock()
        client.invoke_model.return_value = _make_response({"embedding": [0.1, 0.2, 0.3]})
        result = _embedder(client=client)._get_text_embedding("hello")
        assert result == [0.1, 0.2, 0.3]


class TestGetTextEmbeddingCohereV3:
    def test_cohere_v3_format(self):
        client = MagicMock()
        client.invoke_model.return_value = _make_response({"embeddings": [[0.4, 0.5]]})
        e = BedrockDirectEmbedding(model_name="cohere.embed-english-v3")
        e._client = client
        result = e._get_text_embedding("hello")
        assert result == [0.4, 0.5]


class TestGetTextEmbeddingCohereV4:
    def test_cohere_v4_format(self):
        client = MagicMock()
        client.invoke_model.return_value = _make_response(
            {"embeddings": {"float": [[0.6, 0.7]]}}
        )
        e = BedrockDirectEmbedding(model_name="cohere.embed-english-v3")
        e._client = client
        result = e._get_text_embedding("hello")
        assert result == [0.6, 0.7]


class TestGetQueryEmbeddingCohere:
    def test_uses_search_query_input_type(self):
        client = MagicMock()
        client.invoke_model.return_value = _make_response({"embeddings": [[0.8]]})
        e = BedrockDirectEmbedding(model_name="cohere.embed-english-v3")
        e._client = client
        e._get_query_embedding("my query")
        call_body = json.loads(client.invoke_model.call_args[1]["body"])
        assert call_body["input_type"] == "search_query"


class TestRetryBehavior:
    @patch("graphrag_toolkit.lexical_graph.utils.bedrock_embedding.time")
    def test_retries_on_retryable_error_then_succeeds(self, mock_time):
        mock_time.sleep = MagicMock()
        client = MagicMock()
        client.invoke_model.side_effect = [
            RuntimeError("ThrottlingException"),
            _make_response({"embedding": [1.0]}),
        ]
        result = _embedder(client=client)._get_text_embedding("hi")
        assert result == [1.0]
        assert client.invoke_model.call_count == 2

    def test_raises_immediately_on_non_retryable_error(self):
        client = MagicMock()
        client.invoke_model.side_effect = ValueError("bad input")
        with pytest.raises(ValueError, match="bad input"):
            _embedder(client=client)._get_text_embedding("hi")
        assert client.invoke_model.call_count == 1

    @patch("graphrag_toolkit.lexical_graph.utils.bedrock_embedding.time")
    def test_raises_after_all_retries_exhausted(self, mock_time):
        mock_time.sleep = MagicMock()
        client = MagicMock()
        client.invoke_model.side_effect = RuntimeError("ThrottlingException")
        with pytest.raises(RuntimeError, match="ThrottlingException"):
            _embedder(client=client)._get_text_embedding("hi")
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import MAX_RETRIES
        assert client.invoke_model.call_count == MAX_RETRIES


class TestLazyClientInitialization:
    @patch("boto3.Session")
    def test_client_created_on_first_access(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        e = BedrockDirectEmbedding(model_name="amazon.titan-embed-text-v2:0")
        assert e._client is None
        _ = e.client
        mock_session_cls.assert_called_once()
        mock_session.client.assert_called_once_with("bedrock-runtime")
