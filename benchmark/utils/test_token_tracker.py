# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for the token_tracker module.

Tests the TokenTrackingLLMCache wrapper and extract_token_usage() function.
"""

import logging
from unittest.mock import Mock, patch, MagicMock

import pytest

from benchmark.utils.token_tracker import (
    TokenTrackingLLMCache,
    extract_token_usage,
)
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.core.llms.mock import MockLLM
from llama_index.core.prompts import PromptTemplate
from llama_index.core.base.llms.types import ChatResponse, ChatMessage, MessageRole


def _make_bedrock_llm_mock():
    """Create a mock that passes isinstance(x, BedrockConverse) checks."""
    mock_llm = MagicMock()
    mock_llm.__class__ = BedrockConverse
    mock_llm.to_json = Mock(return_value='{"model": "test"}')
    mock_llm.metadata = Mock()
    mock_llm.metadata.is_chat_model = True
    mock_llm.callback_manager = None
    return mock_llm


class TestExtractTokenUsageWithNonTrackingCache:
    """Tests for extract_token_usage when given a regular LLMCache."""

    def test_returns_none_none_for_regular_llm_cache(self):
        """extract_token_usage returns (None, None) for non-TokenTrackingLLMCache."""
        mock_llm = MockLLM()
        cache = LLMCache(llm=mock_llm, enable_cache=False)
        result = extract_token_usage(cache)
        assert result == (None, None)


class TestExtractTokenUsageWithNonBedrockLLM:
    """Tests for extract_token_usage when LLM is not BedrockConverse."""

    def test_returns_none_none_for_non_bedrock_llm(self):
        """extract_token_usage returns (None, None) when LLM is not BedrockConverse."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)
        result = extract_token_usage(cache)
        assert result == (None, None)


class TestTokenTrackingExtractTokensFromResponse:
    """Tests for _extract_tokens_from_response method directly."""

    def test_captures_token_usage_from_bedrock_response(self):
        """Verify tokens are captured from a Bedrock converse response."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)

        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="test response"),
            raw={
                'usage': {
                    'inputTokens': 150,
                    'outputTokens': 42,
                }
            },
        )
        cache._extract_tokens_from_response(mock_chat_response)

        assert cache._last_input_tokens == 150
        assert cache._last_output_tokens == 42

    def test_returns_none_when_usage_missing(self):
        """Verify None when Bedrock response has no usage field."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)

        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
            raw={'stopReason': 'end_turn'},
        )
        cache._extract_tokens_from_response(mock_chat_response)

        assert cache._last_input_tokens is None
        assert cache._last_output_tokens is None

    def test_returns_none_when_raw_is_none(self):
        """Verify None when ChatResponse has no raw field."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)

        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
            raw=None,
        )
        cache._extract_tokens_from_response(mock_chat_response)

        assert cache._last_input_tokens is None
        assert cache._last_output_tokens is None

    def test_token_values_are_integers(self):
        """Verify extracted token values are integers."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)

        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
            raw={'usage': {'inputTokens': 1000, 'outputTokens': 250}},
        )
        cache._extract_tokens_from_response(mock_chat_response)

        assert isinstance(cache._last_input_tokens, int)
        assert isinstance(cache._last_output_tokens, int)
        assert cache._last_input_tokens == 1000
        assert cache._last_output_tokens == 250

    def test_handles_partial_usage_data(self):
        """Verify partial token data (only input or only output)."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)

        # Only inputTokens present
        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
            raw={'usage': {'inputTokens': 500}},
        )
        cache._extract_tokens_from_response(mock_chat_response)

        assert cache._last_input_tokens == 500
        assert cache._last_output_tokens is None


class TestTokenTrackingLLMCachePredict:
    """Tests for TokenTrackingLLMCache.predict() with full predict flow."""

    def test_predict_with_bedrock_captures_tokens(self):
        """Verify predict captures tokens from a BedrockConverse LLM."""
        mock_llm = _make_bedrock_llm_mock()

        # Mock chat to return response with usage
        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="answer text"),
            raw={'usage': {'inputTokens': 200, 'outputTokens': 50}},
        )
        mock_llm.chat = Mock(return_value=mock_chat_response)

        # Mock predict to simulate what LLM.predict does (calls chat)
        def llm_predict(prompt, **kwargs):
            messages = [ChatMessage(role=MessageRole.USER, content="test")]
            mock_llm.chat(messages)
            return "answer text"

        mock_llm.predict = llm_predict

        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)
        prompt = PromptTemplate("{query}")

        result = cache.predict(prompt, query="test question")

        assert result == "answer text"
        assert cache._last_input_tokens == 200
        assert cache._last_output_tokens == 50
        assert extract_token_usage(cache) == (200, 50)

    def test_predict_logs_warning_when_tokens_unavailable(self, caplog):
        """Verify WARNING is logged when token metadata unavailable from live call."""
        mock_llm = _make_bedrock_llm_mock()

        # Mock chat to return response WITHOUT usage
        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="answer"),
            raw={'stopReason': 'end_turn'},
        )
        mock_llm.chat = Mock(return_value=mock_chat_response)

        def llm_predict(prompt, **kwargs):
            messages = [ChatMessage(role=MessageRole.USER, content="test")]
            mock_llm.chat(messages)
            return "answer"

        mock_llm.predict = llm_predict

        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)
        prompt = PromptTemplate("{query}")

        with caplog.at_level(logging.WARNING):
            cache.predict(prompt, query="test question")

        assert any(
            "Token metadata unavailable from live Bedrock invocation" in record.message
            for record in caplog.records
        )

    def test_predict_restores_original_chat_method(self):
        """Verify the original chat method is restored after predict completes."""
        mock_llm = _make_bedrock_llm_mock()

        mock_chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="test"),
            raw={'usage': {'inputTokens': 10, 'outputTokens': 5}},
        )
        original_chat = Mock(return_value=mock_chat_response)
        mock_llm.chat = original_chat

        def llm_predict(prompt, **kwargs):
            messages = [ChatMessage(role=MessageRole.USER, content="test")]
            mock_llm.chat(messages)
            return "test"

        mock_llm.predict = llm_predict

        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)
        prompt = PromptTemplate("{query}")

        cache.predict(prompt, query="test")

        # After predict, the chat method should be restored to original
        assert mock_llm.chat is original_chat

    def test_predict_with_non_bedrock_delegates_to_parent(self):
        """Verify predict works normally for non-BedrockConverse LLMs."""
        mock_llm = MockLLM()
        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)

        prompt = PromptTemplate("{query}")
        result = cache.predict(prompt, query="test")

        # Should still return (None, None) for token usage
        assert extract_token_usage(cache) == (None, None)

    def test_cache_hit_returns_none_none(self):
        """Verify (None, None) when response is served from file cache."""
        mock_llm = _make_bedrock_llm_mock()

        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=True)

        # Simulate a cache hit by setting the flag directly
        cache._last_was_cache_hit = True

        result = extract_token_usage(cache)
        assert result == (None, None)

    @patch('os.path.exists', return_value=True)
    def test_file_cache_hit_sets_flag(self, mock_exists):
        """Verify file cache hit sets _last_was_cache_hit flag."""
        mock_llm = _make_bedrock_llm_mock()
        mock_llm.predict = Mock(return_value="cached response")

        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=True)
        prompt = PromptTemplate("{query}")

        # The parent predict will read from cache file
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__ = Mock(return_value=Mock(read=Mock(return_value="cached")))
            mock_open.return_value.__exit__ = Mock(return_value=False)
            try:
                cache.predict(prompt, query="test")
            except Exception:
                pass  # May fail on file read, but flag should be set

        assert cache._last_was_cache_hit is True
        assert extract_token_usage(cache) == (None, None)

    def test_predict_resets_state_between_calls(self):
        """Verify token state is reset between predict calls."""
        mock_llm = _make_bedrock_llm_mock()

        # First call with tokens
        response_with_tokens = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="first"),
            raw={'usage': {'inputTokens': 100, 'outputTokens': 20}},
        )
        # Second call without tokens
        response_without_tokens = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="second"),
            raw={'stopReason': 'end_turn'},
        )

        call_count = [0]

        def llm_predict(prompt, **kwargs):
            messages = [ChatMessage(role=MessageRole.USER, content="test")]
            mock_llm.chat(messages)
            return "response"

        mock_llm.predict = llm_predict

        cache = TokenTrackingLLMCache(llm=mock_llm, enable_cache=False)
        prompt = PromptTemplate("{query}")

        # First call
        mock_llm.chat = Mock(return_value=response_with_tokens)
        cache.predict(prompt, query="first")
        assert extract_token_usage(cache) == (100, 20)

        # Second call - tokens should be reset
        mock_llm.chat = Mock(return_value=response_without_tokens)
        cache.predict(prompt, query="second")
        assert extract_token_usage(cache) == (None, None)


from hypothesis import given, settings
from hypothesis.strategies import integers


class TestTokenCountPreservationProperty:
    """
    Token count preservation

    For any non-negative integer token count present in the Bedrock converse API
    response (usage.inputTokens, usage.outputTokens), the corresponding field
    written to the output (input_tokens, output_tokens) SHALL equal that exact
    integer value.
    """

    @settings(max_examples=100)
    @given(
        input_tokens=integers(min_value=0),
        output_tokens=integers(min_value=0),
    )
    def test_token_counts_preserved_exactly(self, input_tokens, output_tokens):
        """
        For any non-negative integer token counts, verify they are preserved
        exactly after extraction from a ChatResponse via
        TokenTrackingLLMCache._extract_tokens_from_response().
        """
        from llama_index.core.llms.mock import MockLLM
        from llama_index.core.base.llms.types import ChatResponse, ChatMessage, MessageRole

        cache = TokenTrackingLLMCache(llm=MockLLM(), enable_cache=False)

        # Create a ChatResponse with the generated token counts
        chat_response = ChatResponse(
            message=ChatMessage(role=MessageRole.ASSISTANT, content="response"),
            raw={'usage': {'inputTokens': input_tokens, 'outputTokens': output_tokens}},
        )

        # Extract tokens from the response
        cache._extract_tokens_from_response(chat_response)

        # Verify exact preservation
        assert cache._last_input_tokens == input_tokens, (
            f"Input tokens not preserved: expected {input_tokens}, got {cache._last_input_tokens}"
        )
        assert cache._last_output_tokens == output_tokens, (
            f"Output tokens not preserved: expected {output_tokens}, got {cache._last_output_tokens}"
        )
