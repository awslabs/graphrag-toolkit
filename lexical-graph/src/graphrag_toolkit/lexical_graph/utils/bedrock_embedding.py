# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Built-in Bedrock embedding implementation using boto3 directly.

Replaces the llama-index-embeddings-bedrock dependency with a lightweight,
maintained embedding class that supports Amazon Titan and Cohere model families.
Supports cross-region inference profiles and includes retry logic for transient errors.

Users who prefer the llama-index async wrapper can install the optional extra:
    pip install graphrag-lexical-graph[bedrock-embeddings]
"""

import json
import logging
import time
import random
from typing import Any, List, Optional

from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.bridge.pydantic import Field, PrivateAttr
from llama_index.core.callbacks import CallbackManager

from graphrag_toolkit.lexical_graph.utils.bedrock_utils import (
    MAX_RETRIES,
    BASE_DELAY,
    MAX_DELAY,
    RETRYABLE_ERRORS,
)

logger = logging.getLogger(__name__)


def _extract_provider(model_name: str) -> str:
    """Extract provider from model name, handling cross-region inference profiles.
    
    Examples:
        'amazon.titan-embed-text-v2:0' -> 'amazon'
        'us.amazon.titan-embed-text-v2:0' -> 'amazon'
        'cohere.embed-english-v3' -> 'cohere'
    """
    # Strip region prefix (e.g. 'us.' or 'eu.')
    parts = model_name.split(".")
    if len(parts) >= 3:
        # e.g. 'us.amazon.titan-embed-text-v2:0' -> provider is parts[1]
        return parts[1]
    return parts[0]


class BedrockDirectEmbedding(BaseEmbedding):
    """Bedrock embedding using boto3 directly.
    
    Supports Amazon Titan and Cohere embedding models via the Bedrock
    invoke_model API. Handles Cohere v3 and v4 response formats.
    The client is lazily initialized on first use.
    """

    model_name: str = Field(description="Bedrock model ID")
    region_name: Optional[str] = Field(default=None, description="AWS region")
    profile_name: Optional[str] = Field(default=None, description="AWS profile")
    botocore_config: Optional[Any] = Field(default=None, description="Botocore Config object")
    botocore_session: Optional[Any] = Field(default=None, description="Botocore session", exclude=True)

    _client: Any = PrivateAttr(default=None)

    def __init__(
        self,
        model_name: str,
        botocore_session: Optional[Any] = None,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
        botocore_config: Optional[Any] = None,
        callback_manager: Optional[CallbackManager] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            model_name=model_name,
            region_name=region_name,
            profile_name=profile_name,
            botocore_config=botocore_config,
            botocore_session=botocore_session,
            callback_manager=callback_manager,
            **kwargs,
        )
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import boto3
            session_kwargs = {}
            if self.botocore_session:
                session_kwargs["botocore_session"] = self.botocore_session
            if self.profile_name:
                session_kwargs["profile_name"] = self.profile_name
            if self.region_name:
                session_kwargs["region_name"] = self.region_name
            session = boto3.Session(**session_kwargs)
            client_kwargs = {}
            if self.botocore_config:
                client_kwargs["config"] = self.botocore_config
            self._client = session.client("bedrock-runtime", **client_kwargs)
        return self._client

    @classmethod
    def class_name(cls) -> str:
        return "BedrockDirectEmbedding"

    def _is_retryable_error(self, error: Exception) -> bool:
        error_str = str(type(error).__name__)
        error_msg = str(error)
        for retryable in RETRYABLE_ERRORS:
            if retryable in error_str or retryable in error_msg:
                return True
        return False

    def _invoke(self, text: str, input_type: Optional[str] = None) -> List[float]:
        """Call Bedrock invoke_model and parse the embedding from the response.
        
        Retries on transient errors with exponential backoff.
        """
        if not text or not text.strip():
            raise ValueError("Input text must be non-empty and non-whitespace.")

        provider = _extract_provider(self.model_name)

        if provider == "amazon":
            body = {"inputText": text}
        elif provider == "cohere":
            body = {"texts": [text], "input_type": input_type or "search_document"}
        else:
            body = {"inputText": text}

        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.invoke_model(
                    modelId=self.model_name,
                    body=json.dumps(body),
                    contentType="application/json",
                    accept="application/json",
                )
                response_body = json.loads(response["body"].read())

                if provider == "amazon":
                    return response_body["embedding"]
                elif provider == "cohere":
                    # v4 format: {"embeddings": {"float": [[...]]}}
                    embeddings = response_body["embeddings"]
                    if isinstance(embeddings, dict):
                        return embeddings["float"][0]
                    # v3 format: {"embeddings": [[...]]}
                    return embeddings[0]
                else:
                    embedding = response_body.get("embedding")
                    if embedding is None:
                        raise ValueError(
                            f"Unsupported provider '{provider}': response has no 'embedding' key. "
                            f"Response keys: {list(response_body.keys())}"
                        )
                    return embedding

            except Exception as e:
                last_error = e
                if not self._is_retryable_error(e):
                    raise
                if attempt < MAX_RETRIES - 1:
                    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                    jitter = random.uniform(0, delay * 0.1)
                    logger.warning(
                        f"[BedrockDirectEmbedding] Retryable error (attempt {attempt + 1}/{MAX_RETRIES}): {e}"
                    )
                    time.sleep(delay + jitter)

        raise last_error

    def _get_text_embedding(self, text: str) -> List[float]:
        return self._invoke(text, input_type="search_document")

    def _get_query_embedding(self, query: str) -> List[float]:
        return self._invoke(query, input_type="search_query")

    async def _aget_text_embedding(self, text: str) -> List[float]:
        return self._get_text_embedding(text)

    async def _aget_query_embedding(self, query: str) -> List[float]:
        return self._get_query_embedding(query)
