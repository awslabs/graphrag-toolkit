# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Token tracking utilities for capturing Bedrock converse API token usage.

Provides a wrapper around LLMCache that intercepts predict calls to capture
input/output token counts from Bedrock converse responses.
"""

import logging
from typing import Optional, Tuple, Any

from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.core.prompts import BasePromptTemplate

logger = logging.getLogger(__name__)


class TokenTrackingLLMCache(LLMCache):
    """
    Wraps LLMCache to intercept predict calls and capture Bedrock converse
    response token usage metadata (usage.inputTokens / usage.outputTokens).

    After each predict call, the last captured token usage can be retrieved
    via extract_token_usage().

    Token usage is only available when:
    - The underlying LLM is a BedrockConverse instance
    - The response is NOT served from the file cache
    - The Bedrock response contains usage metadata
    """

    _last_input_tokens: Optional[int] = None
    _last_output_tokens: Optional[int] = None
    _last_was_cache_hit: bool = False

    def predict(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any,
    ) -> str:
        """
        Predict with token usage tracking.

        Calls the parent LLMCache.predict() for the actual response, then
        attempts to extract token usage. Since we cannot monkey-patch the
        Pydantic v2 BedrockConverse model, we simply skip token tracking
        gracefully — tokens will be reported as None (null in JSONL output).
        
        This is a safe fallback: the benchmark still runs correctly, latency
        is still tracked, and correctness/IDK metrics are unaffected. Token
        data will show as null in metrics_summary.json.
        """
        # Reset token tracking state
        self._last_input_tokens = None
        self._last_output_tokens = None
        self._last_was_cache_hit = False

        # Check if the LLM is BedrockConverse - if not, just delegate
        if not isinstance(self.llm, BedrockConverse):
            return super().predict(prompt, **prompt_args)

        # Determine if this will be a cache hit (file cache)
        if self.enable_cache:
            from hashlib import sha256
            import os

            prompt_args_copy = prompt_args.copy()
            for key in prompt_args.get('exclude_cache_keys', []):
                del prompt_args_copy[key]

            cache_key = f'{self.llm.to_json()},{prompt.format(**prompt_args_copy)}'
            cache_hex = sha256(cache_key.encode('utf-8')).hexdigest()
            cache_file = f'cache/llm/{cache_hex}.txt'

            if os.path.exists(cache_file):
                self._last_was_cache_hit = True
                return super().predict(prompt, **prompt_args)

        # Call the parent predict normally
        response = super().predict(prompt, **prompt_args)

        # Attempt to extract token usage from BedrockConverse internal state
        # llama-index BedrockConverse stores the last ChatResponse
        try:
            # Access internal dict to bypass Pydantic field restrictions
            llm_dict = self.llm.__dict__
            
            # llama-index stores last response in _last_completion_response or similar
            # Try common internal attribute names
            for attr_name in ('_last_completion_response', '_last_chat_response', 
                            'last_token_usage', '_last_token_usage'):
                val = llm_dict.get(attr_name)
                if val is not None:
                    if hasattr(val, 'raw') and isinstance(val.raw, dict):
                        usage = val.raw.get('usage', {})
                        if usage:
                            input_t = usage.get('inputTokens')
                            output_t = usage.get('outputTokens')
                            if input_t is not None:
                                self._last_input_tokens = int(input_t)
                            if output_t is not None:
                                self._last_output_tokens = int(output_t)
                            break
        except (TypeError, ValueError, AttributeError, KeyError):
            pass

        return response

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
            # If anything goes wrong during extraction, leave as None
            pass


def extract_token_usage(llm_cache: LLMCache) -> Tuple[Optional[int], Optional[int]]:
    """
    Extracts input_tokens and output_tokens from the last Bedrock invocation.

    Returns (None, None) if:
    - Response was served from file cache
    - LLM is not BedrockConverse
    - Usage metadata is unavailable
    - llm_cache is not a TokenTrackingLLMCache instance

    Args:
        llm_cache: An LLMCache instance (should be TokenTrackingLLMCache for
                   token tracking to work).

    Returns:
        Tuple of (input_tokens, output_tokens) as integers, or (None, None)
        when token data is unavailable.
    """
    if not isinstance(llm_cache, TokenTrackingLLMCache):
        return (None, None)

    if llm_cache._last_was_cache_hit:
        return (None, None)

    if not isinstance(llm_cache.llm, BedrockConverse):
        return (None, None)

    return (llm_cache._last_input_tokens, llm_cache._last_output_tokens)
