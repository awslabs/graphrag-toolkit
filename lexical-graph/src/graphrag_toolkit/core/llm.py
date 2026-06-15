# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""LLM provider abstraction and Bedrock implementation."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterator

import boto3
from botocore.config import Config


@dataclass
class LLMResponse:
    """Response from an LLM provider."""

    content: str
    usage: dict = field(default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0})


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    def predict(self, prompt: str, **kwargs) -> str:
        """Generate a single completion."""

    @abstractmethod
    def stream(self, prompt: str, **kwargs) -> Iterator[str]:
        """Stream completion chunks."""

    async def async_predict(self, prompt: str, **kwargs) -> str:
        """Async completion. Default delegates to predict via thread."""
        return await asyncio.to_thread(self.predict, prompt, **kwargs)


class BedrockLLMProvider(LLMProvider):
    """LLM provider using AWS Bedrock runtime converse APIs."""

    def __init__(
        self,
        model_id: str,
        region_name: str | None = None,
        max_retries: int = 2,
        timeout: int = 60,
        temperature: float | None = None,
        max_tokens: int | None = None,
        top_p: float | None = None,
    ):
        self.model_id = model_id
        self._region_name = region_name
        self._max_retries = max_retries
        self._timeout = timeout
        self._inference_config: dict = {}
        if temperature is not None:
            self._inference_config['temperature'] = temperature
        if max_tokens is not None:
            self._inference_config['maxTokens'] = max_tokens
        if top_p is not None:
            self._inference_config['topP'] = top_p
        self._client = self._create_client()

    def _create_client(self):
        config = Config(
            retries={"max_attempts": self._max_retries, "mode": "adaptive"},
            read_timeout=self._timeout,
            connect_timeout=self._timeout,
        )
        kwargs = {"config": config}
        if self._region_name:
            kwargs["region_name"] = self._region_name
        return boto3.client("bedrock-runtime", **kwargs)

    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_client"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._client = self._create_client()

    def _build_params(self, prompt: str, **kwargs) -> dict:
        """Build converse API parameters from prompt and kwargs."""
        params: dict = {"modelId": self.model_id}

        if "messages" in kwargs:
            params["messages"] = kwargs["messages"]
        else:
            params["messages"] = [{"role": "user", "content": [{"text": prompt}]}]

        if "system" in kwargs:
            params["system"] = [{"text": kwargs["system"]}]

        if self._inference_config:
            params["inferenceConfig"] = self._inference_config

        return params

    @property
    def model(self) -> str:
        """Alias for model_id (batch inference compatibility)."""
        return self.model_id

    def _get_all_kwargs(self, **kwargs) -> dict:
        """Return inference parameters in snake_case for batch compatibility."""
        config = self._inference_config or {}
        # Map Bedrock API keys to snake_case expected by batch_inference_utils
        result = {}
        result['max_tokens'] = config.get('maxTokens', 4096)
        result['temperature'] = config.get('temperature', 0.0)
        if 'topP' in config:
            result['top_p'] = config['topP']
        return result

    def _get_messages(self, prompt, **kwargs) -> list:
        """Format a prompt into converse-style messages for batch compatibility."""
        if hasattr(prompt, 'format'):
            text = prompt.format(**kwargs)
        else:
            text = str(prompt)
        return [{"role": "user", "content": [{"text": text}]}]

    def predict(self, prompt: str, **kwargs) -> str:
        """Call Bedrock converse API and return response text."""
        params = self._build_params(prompt, **kwargs)
        response = self._client.converse(**params)
        return response["output"]["message"]["content"][0]["text"]

    def stream(self, prompt: str, **kwargs) -> Iterator[str]:
        """Call Bedrock converse_stream API and yield text chunks."""
        params = self._build_params(prompt, **kwargs)
        response = self._client.converse_stream(**params)
        for event in response.get("stream", []):
            if "contentBlockDelta" in event:
                delta = event["contentBlockDelta"].get("delta", {})
                if "text" in delta:
                    yield delta["text"]
