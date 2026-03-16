# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import time
import random
from typing import Any, List, Optional, Callable

import llama_index.llms.bedrock_converse.utils
from llama_index.core.base.embeddings.base import BaseEmbedding
from llama_index.core.bridge.pydantic import Field, PrivateAttr
from llama_index.core.callbacks import CallbackManager

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)

# Retry configuration for transient Bedrock errors
MAX_RETRIES = 5
BASE_DELAY = 1.0  # seconds
MAX_DELAY = 30.0  # seconds
RETRYABLE_ERRORS = [
    "ModelErrorException",
    "ThrottlingException", 
    "ServiceUnavailableException",
    "InternalServerException",
    "ServiceException",
]


class Nova2MultimodalEmbedding(BaseEmbedding):
    """
    Custom embedding class for Amazon Nova 2 multimodal embeddings.
    
    Nova 2 multimodal embeddings use a different API format:
    {
        "taskType": "SINGLE_EMBEDDING",
        "singleEmbeddingParams": {
            "embeddingDimension": 3072,
            "embeddingPurpose": "TEXT_RETRIEVAL",
            "text": {
                "truncationMode": "END",
                "value": "text to embed"
            }
        }
    }
    
    This class handles the API format conversion while maintaining
    compatibility with LlamaIndex's embedding interface.
    
    Usage:
        from graphrag_toolkit.lexical_graph import GraphRAGConfig
        from graphrag_toolkit.lexical_graph.utils.bedrock_utils import Nova2MultimodalEmbedding
        
        GraphRAGConfig.embed_model = Nova2MultimodalEmbedding('amazon.nova-2-multimodal-embeddings-v1:0')
        GraphRAGConfig.embed_dimensions = 3072
    """
    
    model_name: str = Field(description="Bedrock model ID for Nova 2 multimodal embeddings")
    embed_dimensions: int = Field(default=3072, description="Embedding dimensions (1024 or 3072)")
    embed_purpose: str = Field(default="TEXT_RETRIEVAL", description="Embedding purpose: TEXT_RETRIEVAL, GENERIC_RETRIEVAL, CLASSIFICATION, CLUSTERING, etc.")
    truncation_mode: str = Field(default="END", description="Truncation mode: END or NONE")
    
    _client: Any = PrivateAttr(default=None)
    
    def __init__(
        self,
        model_name: str,
        embed_dimensions: int = 3072,
        embed_purpose: str = "TEXT_RETRIEVAL",
        truncation_mode: str = "END",
        callback_manager: Optional[CallbackManager] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Nova2MultimodalEmbedding."""
        super().__init__(
            model_name=model_name,
            embed_dimensions=embed_dimensions,
            embed_purpose=embed_purpose,
            truncation_mode=truncation_mode,
            callback_manager=callback_manager,
            **kwargs,
        )
        self._client = None
        logger.info(f"[Nova2MultimodalEmbedding] Initialized with model: {model_name}, dimensions: {embed_dimensions}")
    
    def __getstate__(self):
        """Custom pickle support - exclude the client."""
        state = super().__getstate__()
        if '__pydantic_private__' in state and '_client' in state['__pydantic_private__']:
            state['__pydantic_private__']['_client'] = None
        return state
    
    def __setstate__(self, state):
        """Custom unpickle support - client will be recreated on first use."""
        super().__setstate__(state)
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of bedrock-runtime client."""
        if self._client is None:
            from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
            session = GraphRAGConfig.session
            self._client = session.client('bedrock-runtime')
            logger.debug("[Nova2MultimodalEmbedding] Created bedrock-runtime client")
        return self._client
    
    @classmethod
    def class_name(cls) -> str:
        return "Nova2MultimodalEmbedding"
    
    def _build_request_body(self, text: str) -> dict:
        """Build the Nova 2 multimodal embedding request body."""
        return {
            "taskType": "SINGLE_EMBEDDING",
            "singleEmbeddingParams": {
                "embeddingDimension": self.embed_dimensions,
                "embeddingPurpose": self.embed_purpose,
                "text": {
                    "truncationMode": self.truncation_mode,
                    "value": text
                }
            }
        }
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Check if the error is retryable (transient Bedrock error)."""
        error_str = str(type(error).__name__)
        error_msg = str(error)
        
        for retryable in RETRYABLE_ERRORS:
            if retryable in error_str or retryable in error_msg:
                return True
        
        # Also check for common transient error messages
        transient_messages = [
            "unexpected error during processing",
            "try your request again",
            "service unavailable",
            "internal server error",
            "throttl",
        ]
        error_msg_lower = error_msg.lower()
        return any(msg in error_msg_lower for msg in transient_messages)
    
    def _get_embedding(self, text: str) -> List[float]:
        """Get embedding for a single text using Nova 2 API format with retry logic.
        
        Args:
            text: The text to embed. Must be non-empty and contain at least one
                  non-whitespace character.
        
        Returns:
            List of floats representing the embedding vector.
        
        Raises:
            ValueError: If text is empty or contains only whitespace characters.
        """
        # Validate input before calling Bedrock API
        # Bug condition: text.strip() == "" (empty or whitespace-only)
        if not text or not text.strip():
            logger.warning(
                f"[Nova2MultimodalEmbedding] Empty or whitespace-only text received for embedding: {repr(text)[:50]}"
            )
            raise ValueError(
                f"Text cannot be empty or whitespace-only for embedding. Received: {repr(text)[:50]}"
            )
        
        request_body = self._build_request_body(text)
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.invoke_model(
                    modelId=self.model_name,
                    body=json.dumps(request_body),
                    contentType="application/json",
                    accept="application/json"
                )
                
                response_body = json.loads(response['body'].read())
                # Nova 2 response format: {"embeddings": [{"embeddingType": "TEXT", "embedding": [...]}]}
                embeddings = response_body.get('embeddings', [])
                if embeddings and len(embeddings) > 0:
                    embedding = embeddings[0].get('embedding', [])
                else:
                    embedding = []
                
                logger.debug(f"[Nova2MultimodalEmbedding] Got embedding with {len(embedding)} dimensions")
                return embedding
                
            except Exception as e:
                last_error = e
                
                if not self._is_retryable_error(e):
                    logger.error(f"[Nova2MultimodalEmbedding] Non-retryable error: {e}")
                    raise
                
                if attempt < MAX_RETRIES - 1:
                    # Exponential backoff with jitter
                    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                    jitter = random.uniform(0, delay * 0.1)
                    sleep_time = delay + jitter
                    
                    logger.warning(
                        f"[Nova2MultimodalEmbedding] Retryable error (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                        f"Retrying in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                else:
                    logger.error(
                        f"[Nova2MultimodalEmbedding] Error getting embedding after {MAX_RETRIES} attempts: {e}"
                    )
        
        # If we get here, all retries failed
        raise last_error
    
    def _get_text_embedding(self, text: str) -> List[float]:
        """Required by BaseEmbedding - get embedding for single text."""
        return self._get_embedding(text)
    
    def _get_query_embedding(self, query: str) -> List[float]:
        """Required by BaseEmbedding - get embedding for query."""
        return self._get_embedding(query)
    
    async def _aget_text_embedding(self, text: str) -> List[float]:
        """Async version - falls back to sync."""
        return self._get_text_embedding(text)
    
    async def _aget_query_embedding(self, query: str) -> List[float]:
        """Async version - falls back to sync."""
        return self._get_query_embedding(query)


def _create_retry_decorator(client: Any, max_retries: int) -> Callable[[Any], Any]:  # pragma: no cover
    """
    Creates a retry decorator with exponential backoff strategy.

    This function returns a retry decorator based on the specified maximum
    number of retries and the provided client. It uses exponential backoff
    and wait times between retry attempts. This ensures handling of temporary
    failures and throttling exceptions raised by the specified client.

    Args:
        client: A client object that has exception classes for handling
            specific errors such as ThrottlingException, ModelTimeoutException,
            and ModelErrorException.
        max_retries: An integer specifying the maximum number of retry attempts
            to make before giving up.

    Returns:
        A callable retry decorator with specified retry policies.
    """
    min_seconds = 4
    max_seconds = 30
    # Wait 2^x * 1 second between each retry starting with
    # 4 seconds, then up to 30 seconds, then 30 seconds afterwards
    try:
        import boto3  # noqa
    except ImportError as e:
        raise ImportError(
            "boto3 package not found, install with 'pip install boto3'"
        ) from e
    return retry(
        reraise=True,
        stop=stop_after_attempt(max_retries),
        wait=wait_random_exponential(multiplier=1, min=min_seconds, max=max_seconds),
        retry=(
            retry_if_exception_type(
                (
                    client.exceptions.ThrottlingException,
                    client.exceptions.InternalServerException,
                    client.exceptions.ServiceUnavailableException,
                    client.exceptions.ModelTimeoutException,
                    client.exceptions.ModelErrorException
                )
            )
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    
llama_index.llms.bedrock_converse.utils._create_retry_decorator = _create_retry_decorator
