# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import threading

from botocore.config import Config
from hashlib import sha256
from typing import Optional, Any, Union

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import *
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig, BOTOCORE_DEFAULT_MAX_POOL_CONNECTIONS
from graphrag_toolkit.lexical_graph.utils.llm_concurrency import pool_size, MIN_POOL_SIZE, MAX_POOL_SIZE

from llama_index.core.llms.llm import LLM
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.core.bridge.pydantic import BaseModel, Field
from llama_index.core.prompts import BasePromptTemplate
from llama_index.core.types import TokenGen


logger = logging.getLogger(__name__) 

c_red, c_blue, c_green, c_cyan, c_norm = "\x1b[31m",'\033[94m','\033[92m', '\033[96m', '\033[0m'

MAX_ATTEMPTS = 2
TIMEOUT = 60.0


_client_lock = threading.Lock()


def _bedrock_client(llm, num_threads:int):
    """
    Build the bedrock-runtime client these calls share.

    botocore defaults the pool to 10, below what extraction and the retrievers
    drive through it, and past the pool it discards and reopens connections. Size
    it at twice the thread count as `ResilientClient._client_config` does, with
    the executor's floor added because `pool_size()` reads 0 until something
    creates the pool.
    """
    config = Config(
        retries={'max_attempts': MAX_ATTEMPTS, 'mode': 'standard'},
        connect_timeout=TIMEOUT,
        read_timeout=TIMEOUT,
        max_pool_connections=max(
            BOTOCORE_DEFAULT_MAX_POOL_CONNECTIONS, num_threads * 2, MIN_POOL_SIZE
        ),
    )

    return GraphRAGConfig.session.client(
        'bedrock-runtime', config=config, region_name=llm.region_name
    )


class LLMCache(BaseModel):

    llm:LLM = Field(description='LLM whose responses may be cached')
    enable_cache:Optional[bool] = Field(description='Whether the cache is enabled or disabled', default=False)
    num_threads:Optional[int] = Field(description='Concurrent calls to size the client connection pool for', default=None, ge=0)
    verbose_prompt:Optional[bool] = Field(default=False)
    verbose_response:Optional[bool] = Field(default=False)

    def _pool_threads(self) -> int:
        """
        Concurrent calls to size the client connection pool for.

        The constructor value, else the LLM call pool's size, which is the only
        one surviving spawn. The configured count is last because `predict` and
        `stream` also run on the retrieval path, where it means nothing.
        """
        if self.num_threads is not None:
            # A caller cannot have more calls in flight than the pool has threads.
            return min(self.num_threads, MAX_POOL_SIZE)

        return pool_size() or GraphRAGConfig.extraction_num_threads_per_worker

    def _ensure_client(self) -> None:
        """
        Give the LLM a bedrock-runtime client if it does not already have one.

        BedrockConverse drops `_client` when pickled, so every spawned worker
        rebuilds it while the pool releases calls into this path at once. The
        check and the build are one step under a lock because boto3 client
        creation is not thread safe.
        """
        if not isinstance(self.llm, BedrockConverse) or hasattr(self.llm, '_client'):
            return

        with _client_lock:
            if not hasattr(self.llm, '_client'):
                self.llm._client = _bedrock_client(self.llm, self._pool_threads())

    def stream(
         self,
        prompt: BasePromptTemplate,
        **prompt_args: Any
    ) -> TokenGen:
        response = None

        if self.verbose_prompt:
            logger.info('%s%s%s', c_blue, prompt.format(**prompt_args), c_norm)

        try:
            self._ensure_client()
            response = self.llm.stream(prompt, **prompt_args)
        except Exception as e:
            raise ModelError(f'{e!s} [Model config: {self.llm.to_json()}]') from e
            
        return response

    def predict(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any
    ) -> str:
        """
        Predicts a response based on the given prompt and dynamic arguments using the configured
        language model (LLM). Supports caching of responses to enhance efficiency for repeated
        queries and handles verbose logging for debugging or monitoring purposes.

        The function dynamically adapts caching behavior depending on the configuration. If caching
        is disabled, responses are generated directly using the LLM. If caching is enabled, it calculates
        a unique cache key based on the prompt and LLM configuration, then fetches responses from the
        cache, if available, or generates and stores them for future use.

        The function ensures proper handling of potential errors during model execution and writes
        extensive logs when verbosity options are enabled, aiding in thorough tracking during execution.

        Args:
            prompt: A pre-formatted BasePromptTemplate instance containing the template definition
                to generate the LLM response.
            **prompt_args: Arbitrary keyword arguments that provide dynamic content to fill
                in the placeholders of the given prompt template.

        Returns:
            str: The generated or cached response from the LLM.

        Raises:
            ModelError: If there is any exception while interacting with the LLM, detailed
                configuration information is included to aid debugging.
        """
        response = None

        if self.verbose_prompt:
            logger.info('%s%s%s', c_blue, prompt.format(**prompt_args), c_norm)

        if not self.enable_cache:
            try:
                self._ensure_client()
                response = self.llm.predict(prompt, **prompt_args)
            except Exception as e:
                raise ModelError(f'{e!s} [Model config: {self.llm.to_json()}]') from e
        else:

            prompt_args_copy = prompt_args.copy()
            for key in prompt_args.get('exclude_cache_keys', []):
                del prompt_args_copy[key]

            cache_key = f'{self.llm.to_json()},{prompt.format(**prompt_args_copy)}'
            cache_hex = sha256(cache_key.encode('utf-8')).hexdigest()
            cache_file = f'cache/llm/{cache_hex}.txt'

            if os.path.exists(cache_file):
                logger.debug('%sCached response %s%s', c_blue, cache_file, c_norm)
                with open(cache_file, 'r', encoding='utf-8') as f:
                    response = f.read()
            else:
                try:
                    self._ensure_client()
                    response = self.llm.predict(prompt, **prompt_args)
                except Exception as e:
                    raise ModelError(f'{e!s} Model config: {self.llm.to_json()}') from e
                os.makedirs(os.path.dirname(os.path.realpath(cache_file)), exist_ok=True)
                with open(cache_file, 'w') as f:
                    f.write(response)

        if self.verbose_response:
            logger.info('%s%s%s', c_green, response, c_norm)
            
        return response
    
    @property
    def model(self):
        if not isinstance(self.llm, BedrockConverse):
            raise ModelError(f'Invalid LLM type: {type(self.llm)} does not support model')
        return self.llm.model

    
LLMCacheType = Union[LLM, LLMCache]
    



    