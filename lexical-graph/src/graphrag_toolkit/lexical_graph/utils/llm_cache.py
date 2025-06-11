# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os

from botocore.config import Config
from hashlib import sha256
from typing import Optional, Any, Union

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.lexical_graph.utils.bedrock_utils import *
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig

from llama_index.core.llms.llm import LLM
from llama_index.llms.bedrock_converse import BedrockConverse
from llama_index.core.bridge.pydantic import BaseModel, Field
from llama_index.core.prompts import BasePromptTemplate


logger = logging.getLogger(__name__) 

c_red, c_blue, c_green, c_cyan, c_norm = "\x1b[31m",'\033[94m','\033[92m', '\033[96m', '\033[0m'

MAX_ATTEMPTS = 2
TIMEOUT = 60.0

class LLMCache(BaseModel):

    llm:LLM = Field(desc='LLM whose responses may be cached')
    enable_cache:Optional[bool] = Field(desc='Whether the cache is enabled or disabled', default=False)
    verbose_prompt:Optional[bool] = Field(default=False)
    verbose_response:Optional[bool] = Field(default=False)

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
                if isinstance(self.llm, BedrockConverse):
                    if not hasattr(self.llm, '_client'):
                        config = Config(
                            retries={'max_attempts': MAX_ATTEMPTS, 'mode': 'standard'},
                            connect_timeout=TIMEOUT,
                            read_timeout=TIMEOUT,
                        )
                        
                        session = GraphRAGConfig.session
                        self.llm._client = session.client('bedrock-runtime', config=config)
                response = self.llm.predict(prompt, **prompt_args)
            except Exception as e:
                raise ModelError(f'{e!s} [Model config: {self.llm.to_json()}]') from e
        else:
            
            cache_key = f'{self.llm.to_json()},{prompt.format(**prompt_args)}'
            cache_hex = sha256(cache_key.encode('utf-8')).hexdigest()
            cache_file = f'cache/llm/{cache_hex}.txt'

            if os.path.exists(cache_file):
                logger.debug('%sCached response %s%s', c_blue, cache_file, c_norm)
                with open(cache_file, 'r', encoding='utf-8') as f:
                    response = f.read()
            else:
                try:
                    if isinstance(self.llm, BedrockConverse):
                        if not hasattr(self.llm, '_client'):
                            config = Config(
                                retries={'max_attempts': MAX_ATTEMPTS, 'mode': 'standard'},
                                connect_timeout=TIMEOUT,
                                read_timeout=TIMEOUT,
                            )
                            
                            session = GraphRAGConfig.session
                            self.llm._client = session.client('bedrock-runtime', config=config)
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
    



    