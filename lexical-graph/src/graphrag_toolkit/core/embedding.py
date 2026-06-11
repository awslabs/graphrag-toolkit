# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Embedding provider abstraction and Bedrock implementation."""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod

import boto3


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Return the embedding dimensions."""

    @abstractmethod
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""

    def embed_text(self, text: str) -> list[float]:
        """Embed a single text."""
        return self.embed_texts([text])[0]

    async def async_embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Async batch embedding. Default delegates via thread."""
        return await asyncio.to_thread(self.embed_texts, texts)


_MODEL_DIMENSIONS = {
    "amazon.titan-embed-text-v2:0": 1024,
    "amazon.titan-embed-text-v1": 1536,
    "cohere.embed-english-v3": 1024,
}


class BedrockEmbeddingProvider(EmbeddingProvider):
    """Embedding provider using AWS Bedrock runtime invoke_model."""

    def __init__(
        self,
        model_id: str,
        region_name: str | None = None,
        batch_size: int = 25,
        dimensions: int | None = None,
    ):
        self.model_id = model_id
        self.batch_size = batch_size
        self._dimensions = dimensions
        kwargs = {}
        if region_name:
            kwargs["region_name"] = region_name
        self._client = boto3.client("bedrock-runtime", **kwargs)

    @property
    def dimensions(self) -> int:
        if self._dimensions is not None:
            return self._dimensions
        return _MODEL_DIMENSIONS.get(self.model_id, 1024)

    def _is_cohere(self) -> bool:
        return "cohere" in self.model_id

    def _embed_single(self, text: str) -> list[float]:
        if self._is_cohere():
            body = json.dumps({"texts": [text], "input_type": "search_document"})
        else:
            body = json.dumps({"inputText": text})

        response = self._client.invoke_model(modelId=self.model_id, body=body)
        result = json.loads(response["body"].read())

        if self._is_cohere():
            return result["embeddings"][0]
        return result["embedding"]

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed texts one at a time, respecting batch_size for pacing."""
        return [self._embed_single(text) for text in texts]
