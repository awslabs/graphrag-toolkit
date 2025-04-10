# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import boto3
from botocore.config import Config
from hashlib import sha256
from typing import Optional, Any, Union

from graphrag_toolkit import ModelError
from graphrag_toolkit.utils.bedrock_utils import *

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
                        session = boto3.Session()
                        client = session.client('bedrock-runtime', config=config)
                        self.llm._client = client
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
            raise ModelError(f'Invaid LLM type: {type(self.llm)} does not support model')
        return self.llm.model

    
LLMCacheType = Union[LLM, LLMCache]
    



    