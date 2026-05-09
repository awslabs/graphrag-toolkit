# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
BYOKG-RAG Foundation Model Configuration.

Supported environment variables (checked on first access, cached thereafter):

    BYOKG_LLM_MODEL        - LLM model ID (default: global.anthropic.claude-sonnet-4-6)
    BYOKG_REGION           - AWS region override (optional — boto3 resolves from AWS config if not set)
    BYOKG_EMBED_MODEL      - Embedding model ID (default: cohere.embed-english-v3)
    BYOKG_EMBED_DIMENSIONS - Embedding dimensions (default: 1024)
    BYOKG_RERANKING_MODEL  - Reranking model ID (default: BAAI/bge-reranker-base)
    BYOKG_MAX_TOKENS       - Max tokens for LLM generation (default: 4096)
    BYOKG_MAX_RETRIES      - Max retry attempts (default: 10)
"""

import os
import threading
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict

import boto3
from botocore import exceptions as botocore_exceptions
from botocore.exceptions import SSOTokenLoadError
from boto3 import Session as Boto3Session

logger = logging.getLogger(__name__)

__all__ = ['ByoKGConfig']

DEFAULT_LLM_MODEL = 'global.anthropic.claude-sonnet-4-6'
DEFAULT_EMBED_MODEL = 'cohere.embed-english-v3'
DEFAULT_EMBED_DIMENSIONS = 1024
DEFAULT_RERANKING_MODEL = 'BAAI/bge-reranker-base'
DEFAULT_MAX_TOKENS = 4096
DEFAULT_MAX_RETRIES = 10


class ResilientClient:
    """
    A wrapper for boto3 clients that automatically refreshes credentials when they expire.
    """
    def __init__(self, config, service_name):
        self.config = config
        self.service_name = service_name
        self._client = self._create_client()
        self._lock = threading.Lock()

    def _create_client(self):
        try:
            kwargs = {}
            if self.config.region_name:
                kwargs["region_name"] = self.config.region_name
            return self.config.session.client(self.service_name, **kwargs)
        except SSOTokenLoadError as e:
            raise RuntimeError(
                f"[ResilientClient] SSO token is missing or expired.\n"
                f"Please run: aws sso login\n\n"
                f"Original error: {str(e)}"
            ) from e

    @staticmethod
    def _is_expired(error):
        error_code = getattr(error, 'response', {}).get('Error', {}).get('Code', '')
        return error_code in ['ExpiredToken', 'RequestExpired', 'InvalidClientTokenId']

    def _refresh_client(self):
        with self._lock:
            self._client = self._create_client()

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr
        def wrapper(*args, **kwargs):
            try:
                return getattr(self._client, name)(*args, **kwargs)
            except (botocore_exceptions.ClientError, SSOTokenLoadError) as e:
                if isinstance(e, SSOTokenLoadError) or self._is_expired(e):
                    logger.warning(f"[ResilientClient] Refreshing expired client for {self.service_name}")
                    self._refresh_client()
                    return getattr(self._client, name)(*args, **kwargs)
                raise
        return wrapper


@dataclass
class _ByoKGConfig:
    """
    Singleton configuration for BYOKG-RAG foundation model settings.
    Provides env var support and factory methods for creating generators,
    embeddings, and rerankers.
    """
    _llm_model: Optional[str] = None
    _region_name: Optional[str] = field(default=None, init=False, repr=False)
    # _region_checked distinguishes 'not yet read' from 'read and found None'
    _region_checked: bool = field(default=False, init=False, repr=False)
    _embed_model: Optional[str] = None
    _embed_dimensions: Optional[int] = None
    _reranking_model: Optional[str] = None
    _max_tokens: Optional[int] = None
    _max_retries: Optional[int] = None
    _aws_clients: Dict = field(default_factory=dict)
    _boto3_session: Optional[Boto3Session] = field(default=None, init=False, repr=False)

    @property
    def llm_model(self) -> str:
        if self._llm_model is None:
            self._llm_model = os.environ.get('BYOKG_LLM_MODEL', DEFAULT_LLM_MODEL)
        return self._llm_model

    @llm_model.setter
    def llm_model(self, value: str) -> None:
        self._llm_model = value

    @property
    def region_name(self) -> Optional[str]:
        # Unlike other properties, None is a valid resolved value here
        # (meaning "let boto3 resolve region"). _region_checked prevents re-reading env on every access.
        if not self._region_checked:
            self._region_name = os.environ.get('BYOKG_REGION')
            self._region_checked = True
        return self._region_name

    @region_name.setter
    def region_name(self, value: Optional[str]) -> None:
        self._region_name = value
        self._region_checked = True
        self._boto3_session = None
        self._aws_clients.clear()

    @property
    def embed_model(self) -> str:
        if self._embed_model is None:
            self._embed_model = os.environ.get('BYOKG_EMBED_MODEL', DEFAULT_EMBED_MODEL)
        return self._embed_model

    @embed_model.setter
    def embed_model(self, value: str) -> None:
        self._embed_model = value

    @property
    def embed_dimensions(self) -> int:
        if self._embed_dimensions is None:
            self._embed_dimensions = int(os.environ.get('BYOKG_EMBED_DIMENSIONS', DEFAULT_EMBED_DIMENSIONS))
        return self._embed_dimensions

    @embed_dimensions.setter
    def embed_dimensions(self, value: int) -> None:
        self._embed_dimensions = value

    @property
    def reranking_model(self) -> str:
        if self._reranking_model is None:
            self._reranking_model = os.environ.get('BYOKG_RERANKING_MODEL', DEFAULT_RERANKING_MODEL)
        return self._reranking_model

    @reranking_model.setter
    def reranking_model(self, value: str) -> None:
        self._reranking_model = value

    @property
    def max_tokens(self) -> int:
        if self._max_tokens is None:
            self._max_tokens = int(os.environ.get('BYOKG_MAX_TOKENS', DEFAULT_MAX_TOKENS))
        return self._max_tokens

    @max_tokens.setter
    def max_tokens(self, value: int) -> None:
        self._max_tokens = value

    @property
    def max_retries(self) -> int:
        if self._max_retries is None:
            self._max_retries = int(os.environ.get('BYOKG_MAX_RETRIES', DEFAULT_MAX_RETRIES))
        return self._max_retries

    @max_retries.setter
    def max_retries(self, value: int) -> None:
        self._max_retries = value

    @property
    def session(self) -> Boto3Session:
        if self._boto3_session is None:
            kwargs = {}
            if self.region_name:
                kwargs["region_name"] = self.region_name
            self._boto3_session = Boto3Session(**kwargs)
        return self._boto3_session

    def _get_or_create_client(self, service_name: str):
        if service_name not in self._aws_clients:
            self._aws_clients[service_name] = ResilientClient(self, service_name)
        return self._aws_clients[service_name]

    def to_generator(self, **kwargs):
        """
        Create a BedrockGenerator with config defaults.

        kwargs override config values. Supported: model_name, region_name,
        max_tokens, max_retries, client, inference_config, reasoning_config.
        """
        from .llm.bedrock_llms import BedrockGenerator
        client = kwargs.pop('client', self._get_or_create_client("bedrock-runtime"))
        return BedrockGenerator(
            model_name=kwargs.pop('model_name', self.llm_model),
            region_name=kwargs.pop('region_name', self.region_name),
            max_tokens=kwargs.pop('max_tokens', self.max_tokens),
            max_retries=kwargs.pop('max_retries', self.max_retries),
            client=client,
            **kwargs
        )

    def to_embedding(self, **kwargs):
        """
        Create a BedrockEmbedding (byokg_rag variant) with config defaults.

        kwargs are passed to langchain_aws.BedrockEmbeddings via the wrapper.
        """
        from .indexing.embedding import BedrockEmbedding
        defaults = {
            'model_id': kwargs.pop('model_id', self.embed_model),
        }
        if self.region_name:
            defaults['region_name'] = self.region_name
        defaults.update(kwargs)
        return BedrockEmbedding(**defaults)

    def to_reranker(self, **kwargs):
        """
        Create a LocalGReranker with config defaults.

        kwargs override config values. Supported: model_name, topk, device.
        """
        from .graph_retrievers.graph_reranker import LocalGReranker
        return LocalGReranker(
            model_name=kwargs.pop('model_name', self.reranking_model),
            **kwargs
        )


ByoKGConfig = _ByoKGConfig()
