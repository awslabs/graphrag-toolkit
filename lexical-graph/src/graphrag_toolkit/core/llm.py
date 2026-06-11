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
    ):
        self.model_id = model_id
        config = Config(
            retries={"max_attempts": max_retries, "mode": "adaptive"},
            read_timeout=timeout,
            connect_timeout=timeout,
        )
        kwargs = {"config": config}
        if region_name:
            kwargs["region_name"] = region_name
        self._client = boto3.client("bedrock-runtime", **kwargs)

    def _build_params(self, prompt: str, **kwargs) -> dict:
        """Build converse API parameters from prompt and kwargs."""
        params: dict = {"modelId": self.model_id}

        if "messages" in kwargs:
            params["messages"] = kwargs["messages"]
        else:
            params["messages"] = [{"role": "user", "content": [{"text": prompt}]}]

        if "system" in kwargs:
            params["system"] = [{"text": kwargs["system"]}]

        return params

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
