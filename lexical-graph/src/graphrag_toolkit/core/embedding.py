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
        max_retries: int = 10,
        timeout: int = 60,
    ):
        self.model_id = model_id
        self.batch_size = batch_size
        self._dimensions = dimensions
        self._region_name = region_name
        self._max_retries = max_retries
        self._timeout = timeout
        self._client = self._create_client()

    def _create_client(self):
        from botocore.config import Config
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

    def _embed_batch_cohere(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single Cohere API call."""
        body = json.dumps({"texts": texts, "input_type": "search_document"})
        response = self._client.invoke_model(modelId=self.model_id, body=body)
        result = json.loads(response["body"].read())
        return result["embeddings"]

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Embed texts in batches, using native batch for Cohere."""
        results = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            if self._is_cohere():
                results.extend(self._embed_batch_cohere(batch))
            else:
                results.extend([self._embed_single(t) for t in batch])
        return results
