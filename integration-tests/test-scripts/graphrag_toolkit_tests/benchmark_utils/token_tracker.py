# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Token tracking utilities for capturing Bedrock converse API token usage.

Tracks two measurements:
- prompt_tokens_total: Full LLM prompt tokens from Bedrock usage metadata
  (system prompt + retrieval context + query).
- retrieval_context_tokens: Estimated token count of just the retrieval context
  (search_results param), measured via char/4 approximation before prompt assembly.
"""

import os
import logging
from hashlib import sha256
from typing import Optional, Tuple, Any

from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.core.prompts import BasePromptTemplate
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from botocore.config import Config

logger = logging.getLogger(__name__)

_TIMEOUT = 60.0
_MAX_ATTEMPTS = 10
# Approximate chars-per-token ratio, calibrated for Claude-family models on English text.
# Other model families or non-English content may have a different ratio.
# For structured text (JSON, code, lists) this can be off by 30-50%; acceptable for
# directional metrics but worth refining if tighter estimates are needed.
_CHARS_PER_TOKEN = 4


def estimate_token_count(text: str, chars_per_token: int = _CHARS_PER_TOKEN) -> int:
    """Estimate token count using ~4 chars/token heuristic for Claude models.

    Args:
        text: The text to estimate token count for.
        chars_per_token: Override the chars-per-token ratio. Defaults to _CHARS_PER_TOKEN (4),
            which is calibrated for Claude-family models on English text. Adjust for other
            model families, non-English content, or structured text (JSON, code).

    Returns:
        Estimated token count as a non-negative integer.
    """
    if not text:
        return 0
    return max(0, len(text) // chars_per_token)


class TokenTrackingLLMCache(LLMCache):
    """
    Wraps LLMCache to capture Bedrock token usage after each predict call.
    Also measures retrieval context tokens separately from full prompt tokens.
    """

    _last_input_tokens: Optional[int] = None
    _last_output_tokens: Optional[int] = None
    _last_retrieval_context_tokens: Optional[int] = None
    _last_was_cache_hit: bool = False

    def predict(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any,
    ) -> str:
        self._last_input_tokens = None
        self._last_output_tokens = None
        self._last_retrieval_context_tokens = None
        self._last_was_cache_hit = False

        search_results = prompt_args.get('search_results', '')
        # Intentionally treats both missing and empty search_results as "no context":
        # retrieval_context_tokens stays None to distinguish "not measured" from "zero tokens".
        if search_results:
            self._last_retrieval_context_tokens = estimate_token_count(search_results)

        if not isinstance(self.llm, BedrockConverse):
            return super().predict(prompt, **prompt_args)

        formatted_prompt = prompt.format(**prompt_args)

        if self.enable_cache:
            prompt_args_copy = prompt_args.copy()
            for key in prompt_args.get('exclude_cache_keys', []):
                del prompt_args_copy[key]

            cache_key = f'{self.llm.to_json()},{prompt.format(**prompt_args_copy)}'
            cache_hex = sha256(cache_key.encode('utf-8')).hexdigest()
            cache_file = f'cache/llm/{cache_hex}.txt'

            if os.path.exists(cache_file):
                self._last_was_cache_hit = True
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return f.read()

        if not hasattr(self.llm, '_client') or self.llm._client is None:
            config = Config(
                retries={'max_attempts': _MAX_ATTEMPTS, 'mode': 'standard'},
                connect_timeout=_TIMEOUT,
                read_timeout=_TIMEOUT,
            )
            session = GraphRAGConfig.session
            self.llm._client = session.client('bedrock-runtime', config=config)

        messages = [ChatMessage(role=MessageRole.USER, content=formatted_prompt)]
        chat_response = self.llm.chat(messages)

        response_text = chat_response.message.content if chat_response.message else ''
        self._extract_tokens_from_response(chat_response)

        if self.enable_cache:
            os.makedirs(os.path.dirname(os.path.realpath(cache_file)), exist_ok=True)
            with open(cache_file, 'w') as f:
                f.write(response_text)

        if self._last_input_tokens is None:
            logger.warning("Token metadata unavailable from live Bedrock invocation")

        return response_text

    def _extract_tokens_from_response(self, chat_response) -> None:
        """Extract token counts from a ChatResponse's raw Bedrock response."""
        try:
            raw = getattr(chat_response, 'raw', None)
            if raw is None or not isinstance(raw, dict):
                return

            usage = raw.get('usage')
            if usage is None or not isinstance(usage, dict):
                return

            input_tokens = usage.get('inputTokens')
            output_tokens = usage.get('outputTokens')

            if input_tokens is not None:
                self._last_input_tokens = int(input_tokens)
            if output_tokens is not None:
                self._last_output_tokens = int(output_tokens)
        except (TypeError, ValueError, AttributeError):
            pass


def extract_token_usage(llm_cache: LLMCache) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Extract token usage from a TokenTrackingLLMCache after a predict call.

    Args:
        llm_cache: The LLM cache instance to extract token usage from.

    Returns:
        A 3-tuple of (input_tokens, output_tokens, retrieval_context_tokens) where:
          - input_tokens: Full prompt token count from Bedrock usage metadata
            (system prompt + retrieval context + query). None on cache hits or if
            unavailable.
          - output_tokens: Generated output token count from Bedrock usage metadata.
            None on cache hits or if unavailable.
          - retrieval_context_tokens: Estimated token count of just the retrieval
            context (search_results), computed via char/token heuristic before prompt
            assembly. None if search_results was absent or empty.

        Returns (None, None, None) if llm_cache is not a TokenTrackingLLMCache,
        the LLM is not BedrockConverse, or usage metadata is unavailable.
        On cache hits, input/output tokens are None but retrieval_context_tokens
        may still be populated.
    """
    if not isinstance(llm_cache, TokenTrackingLLMCache):
        return (None, None, None)

    if llm_cache._last_was_cache_hit:
        return (None, None, llm_cache._last_retrieval_context_tokens)

    if not isinstance(llm_cache.llm, BedrockConverse):
        return (None, None, None)

    return (
        llm_cache._last_input_tokens,
        llm_cache._last_output_tokens,
        llm_cache._last_retrieval_context_tokens,
    )
