# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.llm."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from graphrag_toolkit.core.llm import (
    BedrockLLMProvider,
    LLMProvider,
    LLMResponse,
)


class TestLLMResponse:
    def test_creation_with_defaults(self):
        r = LLMResponse(content="hello")
        assert r.content == "hello"
        assert r.usage == {"prompt_tokens": 0, "completion_tokens": 0}

    def test_creation_with_usage(self):
        r = LLMResponse(content="hi", usage={"prompt_tokens": 10, "completion_tokens": 5})
        assert r.usage["prompt_tokens"] == 10


class TestLLMProviderABC:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            LLMProvider()


class TestBedrockLLMProvider:
    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_init_stores_model_id(self, mock_boto3_client):
        provider = BedrockLLMProvider(model_id="anthropic.claude-3-sonnet")
        assert provider.model_id == "anthropic.claude-3-sonnet"
        mock_boto3_client.assert_called_once()

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_predict(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "response text"}]}}
        }

        provider = BedrockLLMProvider(model_id="model-1")
        result = provider.predict("hello")

        assert result == "response text"
        mock_client.converse.assert_called_once_with(
            modelId="model-1",
            messages=[{"role": "user", "content": [{"text": "hello"}]}],
        )

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_predict_with_system_prompt(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "ok"}]}}
        }

        provider = BedrockLLMProvider(model_id="model-1")
        provider.predict("hello", system="You are helpful")

        call_kwargs = mock_client.converse.call_args[1]
        assert call_kwargs["system"] == [{"text": "You are helpful"}]

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_predict_with_messages(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "multi-turn"}]}}
        }

        messages = [
            {"role": "user", "content": [{"text": "hi"}]},
            {"role": "assistant", "content": [{"text": "hello"}]},
            {"role": "user", "content": [{"text": "how are you?"}]},
        ]
        provider = BedrockLLMProvider(model_id="model-1")
        result = provider.predict("ignored", messages=messages)

        assert result == "multi-turn"
        call_kwargs = mock_client.converse.call_args[1]
        assert call_kwargs["messages"] == messages

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_stream(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse_stream.return_value = {
            "stream": [
                {"contentBlockDelta": {"delta": {"text": "chunk1"}}},
                {"contentBlockDelta": {"delta": {"text": "chunk2"}}},
                {"contentBlockStop": {}},
            ]
        }

        provider = BedrockLLMProvider(model_id="model-1")
        chunks = list(provider.stream("hello"))

        assert chunks == ["chunk1", "chunk2"]

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_stream_empty(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse_stream.return_value = {"stream": []}

        provider = BedrockLLMProvider(model_id="model-1")
        chunks = list(provider.stream("hello"))

        assert chunks == []

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_async_predict(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse.return_value = {
            "output": {"message": {"content": [{"text": "async response"}]}}
        }

        provider = BedrockLLMProvider(model_id="model-1")
        result = asyncio.run(provider.async_predict("hello"))

        assert result == "async response"

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_predict_client_error(self, mock_boto3_client):
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.converse.side_effect = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
            "Converse",
        )

        provider = BedrockLLMProvider(model_id="model-1")
        with pytest.raises(ClientError):
            provider.predict("hello")

    @patch("graphrag_toolkit.core.llm.boto3.client")
    def test_init_with_region(self, mock_boto3_client):
        BedrockLLMProvider(model_id="model-1", region_name="us-west-2")
        call_kwargs = mock_boto3_client.call_args[1]
        assert call_kwargs["region_name"] == "us-west-2"
