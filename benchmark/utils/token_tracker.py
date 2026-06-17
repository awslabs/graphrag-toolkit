# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Token tracking utilities for capturing Bedrock converse API token usage.

Provides a wrapper around LLMCache that intercepts predict calls to capture
input/output token counts from Bedrock converse responses.
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

TIMEOUT = 60.0
MAX_ATTEMPTS = 10


class TokenTrackingLLMCache(LLMCache):
    """
    Wraps LLMCache to capture Bedrock converse response token usage
    (usage.inputTokens / usage.outputTokens) after each predict call.

    Token usage is available when:
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

        For BedrockConverse LLMs, calls chat() directly to access the full
        ChatResponse (which contains token usage in .raw['usage']).
        Falls back to parent predict() for non-Bedrock LLMs or cache hits.
        """
        self._last_input_tokens = None
        self._last_output_tokens = None
        self._last_was_cache_hit = False

        if not isinstance(self.llm, BedrockConverse):
            return super().predict(prompt, **prompt_args)

        # Format the prompt
        formatted_prompt = prompt.format(**prompt_args)

        # Check file cache
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

        # Ensure the Bedrock client is initialized
        if not hasattr(self.llm, '_client') or self.llm._client is None:
            config = Config(
                retries={'max_attempts': MAX_ATTEMPTS, 'mode': 'standard'},
                connect_timeout=TIMEOUT,
                read_timeout=TIMEOUT,
            )
            session = GraphRAGConfig.session
            self.llm._client = session.client('bedrock-runtime', config=config)

        # Call chat() directly to get the full ChatResponse with token metadata
        messages = [ChatMessage(role=MessageRole.USER, content=formatted_prompt)]
        chat_response = self.llm.chat(messages)

        # Extract the text response
        response_text = chat_response.message.content if chat_response.message else ''

        # Extract token usage from the raw Bedrock response
        self._extract_tokens_from_response(chat_response)

        # Write to cache if enabled
        if self.enable_cache:
            os.makedirs(os.path.dirname(os.path.realpath(cache_file)), exist_ok=True)
            with open(cache_file, 'w') as f:
                f.write(response_text)

        if self._last_input_tokens is None:
            logger.warning(
                "Token metadata unavailable from live Bedrock invocation"
            )

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


def extract_token_usage(llm_cache: LLMCache) -> Tuple[Optional[int], Optional[int]]:
    """
    Extracts input_tokens and output_tokens from the last Bedrock invocation.

    Returns (None, None) if:
    - Response was served from file cache
    - LLM is not BedrockConverse
    - Usage metadata is unavailable
    - llm_cache is not a TokenTrackingLLMCache instance
    """
    if not isinstance(llm_cache, TokenTrackingLLMCache):
        return (None, None)

    if llm_cache._last_was_cache_hit:
        return (None, None)

    if not isinstance(llm_cache.llm, BedrockConverse):
        return (None, None)

    return (llm_cache._last_input_tokens, llm_cache._last_output_tokens)
