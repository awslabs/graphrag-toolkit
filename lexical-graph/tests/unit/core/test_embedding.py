# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.embedding."""

import asyncio
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from graphrag_toolkit.core.embedding import (
    BedrockEmbeddingProvider,
    EmbeddingProvider,
)


def _make_streaming_body(data: dict) -> MagicMock:
    body = MagicMock()
    body.read.return_value = json.dumps(data).encode()
    return body


class TestEmbeddingProviderABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            EmbeddingProvider()


class TestBedrockEmbeddingProviderInit:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_stores_model_id_and_batch_size(self, mock_client):
        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0", batch_size=10)
        assert provider.model_id == "amazon.titan-embed-text-v2:0"
        assert provider.batch_size == 10


class TestBedrockEmbeddingProviderTitan:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_embed_texts_titan(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": _make_streaming_body({"embedding": [0.1, 0.2, 0.3]})
        }

        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0")
        result = provider.embed_texts(["hello"])

        assert result == [[0.1, 0.2, 0.3]]
        call_kwargs = mock_client.invoke_model.call_args[1]
        assert call_kwargs["modelId"] == "amazon.titan-embed-text-v2:0"
        body = json.loads(call_kwargs["body"])
        assert body == {"inputText": "hello"}

    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_embed_text_convenience(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": _make_streaming_body({"embedding": [1.0, 2.0]})
        }

        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v1")
        result = provider.embed_text("test")

        assert result == [1.0, 2.0]


class TestBedrockEmbeddingProviderCohere:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_embed_texts_cohere(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": _make_streaming_body({"embeddings": [[0.5, 0.6]]})
        }

        provider = BedrockEmbeddingProvider(model_id="cohere.embed-english-v3")
        result = provider.embed_texts(["world"])

        assert result == [[0.5, 0.6]]
        call_kwargs = mock_client.invoke_model.call_args[1]
        body = json.loads(call_kwargs["body"])
        assert body == {"texts": ["world"], "input_type": "search_document"}


class TestBedrockEmbeddingProviderBatching:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_multiple_texts_call_invoke_per_text(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": _make_streaming_body({"embedding": [0.1]})
        }

        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0", batch_size=10)
        texts = [f"text-{i}" for i in range(30)]
        result = provider.embed_texts(texts)

        assert len(result) == 30
        assert mock_client.invoke_model.call_count == 30


class TestBedrockEmbeddingProviderDimensions:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_explicit_dimensions(self, mock_client):
        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0", dimensions=512)
        assert provider.dimensions == 512

    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_titan_v2_default(self, mock_client):
        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0")
        assert provider.dimensions == 1024

    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_titan_v1_default(self, mock_client):
        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v1")
        assert provider.dimensions == 1536

    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_cohere_default(self, mock_client):
        provider = BedrockEmbeddingProvider(model_id="cohere.embed-english-v3")
        assert provider.dimensions == 1024


class TestBedrockEmbeddingProviderAsync:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_async_embed_texts(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": _make_streaming_body({"embedding": [0.1, 0.2]})
        }

        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0")
        result = asyncio.run(provider.async_embed_texts(["hello"]))

        assert result == [[0.1, 0.2]]


class TestBedrockEmbeddingProviderErrors:
    @patch("graphrag_toolkit.core.embedding.boto3.client")
    def test_client_error(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.invoke_model.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "Invalid"}},
            "InvokeModel",
        )

        provider = BedrockEmbeddingProvider(model_id="amazon.titan-embed-text-v2:0")
        with pytest.raises(ClientError):
            provider.embed_texts(["fail"])
