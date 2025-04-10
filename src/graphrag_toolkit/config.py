# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import json
from dataclasses import dataclass
from typing import Optional, Union, Set, Type

from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.embeddings.bedrock import BedrockEmbedding
from llama_index.core.settings import Settings
from llama_index.core.llms import LLM
from llama_index.core.base.embeddings.base import BaseEmbedding

LLMType = Union[LLM, str]
EmbeddingType = Union[BaseEmbedding, str]

DEFAULT_EXTRACTION_MODEL = 'anthropic.claude-3-sonnet-20240229-v1:0'
DEFAULT_RESPONSE_MODEL = 'anthropic.claude-3-sonnet-20240229-v1:0'
DEFAULT_EMBEDDINGS_MODEL = 'cohere.embed-english-v3'
DEFAULT_RERANKING_MODEL = 'mixedbread-ai/mxbai-rerank-xsmall-v1'
DEFAULT_EMBEDDINGS_DIMENSIONS = 1024
DEFAULT_EXTRACTION_NUM_WORKERS = 2
DEFAULT_EXTRACTION_BATCH_SIZE = 4
DEFAULT_EXTRACTION_NUM_THREADS_PER_WORKER = 4
DEFAULT_BUILD_NUM_WORKERS = 2
DEFAULT_BUILD_BATCH_SIZE = 4
DEFAULT_BUILD_BATCH_WRITE_SIZE = 25
DEFAULT_BATCH_WRITES_ENABLED = True
DEFAULT_INCLUDE_DOMAIN_LABELS = False
DEFAULT_ENABLE_CACHE = False

def _is_json_string(s):
    try:
        json.loads(s)
        return True
    except ValueError:
        return False
    
def string_to_bool(s, default_value:bool):
    if not s:
        return default_value
    else:
        return s.lower() in ['true']

@dataclass
class _GraphRAGConfig:

    _extraction_llm: Optional[LLM] = None
    _response_llm: Optional[LLM] = None 
    _embed_model: Optional[BaseEmbedding] = None
    _embed_dimensions: Optional[int] = None
    _reranking_model: Optional[str] = None
    _extraction_num_workers: Optional[int] = None
    _extraction_num_threads_per_worker: Optional[int] = None
    _extraction_batch_size: Optional[int] = None
    _build_num_workers: Optional[int] = None
    _build_batch_size: Optional[int] = None
    _build_batch_write_size: Optional[int] = None
    _batch_writes_enabled: Optional[bool] = None
    _include_domain_labels: Optional[bool] = None
    _enable_cache: Optional[bool] = None

    @property
    def extraction_num_workers(self) -> int:
        if self._extraction_num_workers is None:
            self.extraction_num_workers = int(os.environ.get('EXTRACTION_NUM_WORKERS', DEFAULT_EXTRACTION_NUM_WORKERS)) 

        return self._extraction_num_workers

    @extraction_num_workers.setter
    def extraction_num_workers(self, num_workers:int) -> None:
        self._extraction_num_workers = num_workers

    @property
    def extraction_num_threads_per_worker(self) -> int:
        if self._extraction_num_threads_per_worker is None:
            self.extraction_num_threads_per_worker = int(os.environ.get('EXTRACTION_NUM_THREADS_PER_WORKER', DEFAULT_EXTRACTION_NUM_THREADS_PER_WORKER)) 

        return self._extraction_num_threads_per_worker

    @extraction_num_threads_per_worker.setter
    def extraction_num_threads_per_worker(self, num_threads:int) -> None:
        self._extraction_num_threads_per_worker = num_threads

    @property
    def extraction_batch_size(self) -> int:
        if self._extraction_batch_size is None:
            self.extraction_batch_size = int(os.environ.get('EXTRACTION_BATCH_SIZE', DEFAULT_EXTRACTION_BATCH_SIZE))  

        return self._extraction_batch_size

    @extraction_batch_size.setter
    def extraction_batch_size(self, batch_size:int) -> None:
        self._extraction_batch_size = batch_size

    @property
    def build_num_workers(self) -> int:
        if self._build_num_workers is None:
            self.build_num_workers = int(os.environ.get('BUILD_NUM_WORKERS', DEFAULT_BUILD_NUM_WORKERS))

        return self._build_num_workers

    @build_num_workers.setter
    def build_num_workers(self, num_workers:int) -> None:
        self._build_num_workers = num_workers

    @property
    def build_batch_size(self) -> int:
        if self._build_batch_size is None:
            self.build_batch_size = int(os.environ.get('BUILD_BATCH_SIZE', DEFAULT_BUILD_BATCH_SIZE))  

        return self._build_batch_size

    @build_batch_size.setter
    def build_batch_size(self, batch_size:int) -> None:
        self._build_batch_size = batch_size

    @property
    def build_batch_write_size(self) -> int:
        if self._build_batch_write_size is None:
            self.build_batch_write_size = int(os.environ.get('BUILD_BATCH_WRITE_SIZE', DEFAULT_BUILD_BATCH_WRITE_SIZE))  

        return self._build_batch_write_size

    @build_batch_write_size.setter
    def build_batch_write_size(self, batch_size:int) -> None:
        self._build_batch_write_size = batch_size

    @property
    def batch_writes_enabled(self) -> bool:
        if self._batch_writes_enabled is None:
            self.batch_writes_enabled = string_to_bool(os.environ.get('BATCH_WRITES_ENABLED'), DEFAULT_BATCH_WRITES_ENABLED)

        return self._batch_writes_enabled

    @batch_writes_enabled.setter
    def batch_writes_enabled(self, batch_writes_enabled:bool) -> None:
        self._batch_writes_enabled = batch_writes_enabled

    @property
    def include_domain_labels(self) -> bool:
        if self._include_domain_labels is None:
            self.include_domain_labels = string_to_bool(os.environ.get('INCLUDE_DOMAIN_LABELS'), DEFAULT_INCLUDE_DOMAIN_LABELS)  
        return self._include_domain_labels

    @include_domain_labels.setter
    def include_domain_labels(self, include_domain_labels:bool) -> None:
        self._include_domain_labels = include_domain_labels

    @property
    def enable_cache(self) -> bool:
        if self._enable_cache is None:
            self.enable_cache = string_to_bool(os.environ.get('ENABLE_CACHE'), DEFAULT_ENABLE_CACHE)  
        return self._enable_cache

    @enable_cache.setter
    def enable_cache(self, enable_cache:bool) -> None:
        self._enable_cache = enable_cache
   
    @property
    def extraction_llm(self) -> LLM:
        if self._extraction_llm is None:
            self.extraction_llm = os.environ.get('EXTRACTION_MODEL', DEFAULT_EXTRACTION_MODEL)
        return self._extraction_llm

    @extraction_llm.setter
    def extraction_llm(self, llm: LLMType) -> None:
        if isinstance(llm, LLM):
            self._extraction_llm = llm
        else:
            if _is_json_string(llm):
                self._extraction_llm = BedrockConverse.from_json(llm)
            else:
                json_str = f'''{{
                    "model": "{llm}",
                    "temperature": 0.0,
                    "max_tokens": 4096
                }}'''
                self._extraction_llm = BedrockConverse.from_json(json_str)
        self._extraction_llm.callback_manager = Settings.callback_manager

    @property
    def response_llm(self) -> LLM:
        if self._response_llm is None:
            self.response_llm = os.environ.get('RESPONSE_MODEL', DEFAULT_RESPONSE_MODEL)
            
        return self._response_llm

    @response_llm.setter
    def response_llm(self, llm: LLMType) -> None:
        if isinstance(llm, LLM):
            self._response_llm = llm
        else:
            if _is_json_string(llm):
                self._response_llm = BedrockConverse.from_json(llm)
            else:
                json_str = f'''{{
                    "model": "{llm}",
                    "temperature": 0.0,
                    "max_tokens": 4096
                }}'''
                self._response_llm = BedrockConverse.from_json(json_str)
        self._response_llm.callback_manager = Settings.callback_manager
       
    @property
    def embed_model(self) -> BaseEmbedding:
        if self._embed_model is None:
            self.embed_model = os.environ.get('EMBEDDINGS_MODEL', DEFAULT_EMBEDDINGS_MODEL)

        return self._embed_model

    @embed_model.setter
    def embed_model(self, embed_model: EmbeddingType) -> None:
        if isinstance(embed_model, str):
            if _is_json_string(embed_model):
                self._embed_model = BedrockEmbedding.from_json(embed_model) 
            else:
                json_str = f'''{{
                    "model_name": "{embed_model}"
                }}'''
                self._embed_model = BedrockEmbedding.from_json(json_str) 
        else:
            self._embed_model = embed_model
        self._embed_model.callback_manager = Settings.callback_manager

    @property
    def embed_dimensions(self) -> int:
       if self._embed_dimensions is None:
           self.embed_dimensions = int(os.environ.get('EMBEDDINGS_DIMENSIONS', DEFAULT_EMBEDDINGS_DIMENSIONS))
           
       return self._embed_dimensions

    @embed_dimensions.setter
    def embed_dimensions(self, embed_dimensions: int) -> None:
       self._embed_dimensions = embed_dimensions

    @property
    def reranking_model(self) -> str:
       if self._reranking_model is None:
           self._reranking_model = os.environ.get('RERANKING_MODEL', DEFAULT_RERANKING_MODEL)
           
       return self._reranking_model

    @reranking_model.setter
    def reranking_model(self, reranking_model: str) -> None:
       self._reranking_model = reranking_model
    

    
GraphRAGConfig = _GraphRAGConfig()