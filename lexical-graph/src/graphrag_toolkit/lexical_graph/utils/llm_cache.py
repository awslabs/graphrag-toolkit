# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os

from hashlib import sha256
from typing import Optional, Any, Union, Iterator

from graphrag_toolkit.lexical_graph import ModelError
from graphrag_toolkit.core.llm import LLMProvider, BedrockLLMProvider
from pydantic import BaseModel, Field, ConfigDict


logger = logging.getLogger(__name__) 

c_red, c_blue, c_green, c_cyan, c_norm = "\x1b[31m",'\033[94m','\033[92m', '\033[96m', '\033[0m'

MAX_ATTEMPTS = 2
TIMEOUT = 60.0

def _format_prompt(prompt, prompt_args):
    """Format a prompt (PromptTemplate or ChatPromptTemplate) into a string."""
    if prompt_args:
        result = prompt.format(**prompt_args)
    elif hasattr(prompt, 'format') and callable(prompt.format):
        result = prompt.format()
    else:
        return str(prompt)
    # ChatPromptTemplate.format() returns list[dict] — flatten to string
    if isinstance(result, list):
        return "\n".join(f"{m.get('role', 'user')}: {m.get('content', '')}" for m in result)
    return result


class LLMCache(BaseModel):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any = Field(description='LLM whose responses may be cached')
    enable_cache: Optional[bool] = Field(description='Whether the cache is enabled or disabled', default=False)
    verbose_prompt: Optional[bool] = Field(default=False)
    verbose_response: Optional[bool] = Field(default=False)

    def stream(
         self,
        prompt,
        **prompt_args: Any
    ) -> Iterator[str]:
        formatted = _format_prompt(prompt, prompt_args)

        if self.verbose_prompt:
            logger.info('%s%s%s', c_blue, formatted, c_norm)

        try:
            response = self.llm.stream(formatted)
        except Exception as e:
            raise ModelError(f'{e!s} [Model: {self.model_id}]') from e
            
        return response

    def predict(
        self,
        prompt,
        **prompt_args: Any
    ) -> str:
        """
        Predicts a response based on the given prompt and dynamic arguments using the configured
        language model (LLM). Supports caching of responses to enhance efficiency for repeated
        queries and handles verbose logging for debugging or monitoring purposes.

        Args:
            prompt: A prompt template with a .format(**kwargs) method, or a plain string.
            **prompt_args: Keyword arguments to format the prompt template.

        Returns:
            str: The generated or cached response from the LLM.

        Raises:
            ModelError: If there is any exception while interacting with the LLM.
        """
        formatted = _format_prompt(prompt, prompt_args)

        if self.verbose_prompt:
            logger.info('%s%s%s', c_blue, formatted, c_norm)

        if not self.enable_cache:
            try:
                response = self.llm.predict(formatted)
            except Exception as e:
                raise ModelError(f'{e!s} [Model: {self.model_id}]') from e
        else:
            prompt_args_copy = prompt_args.copy()
            for key in prompt_args.get('exclude_cache_keys', []):
                del prompt_args_copy[key]

            cache_formatted = _format_prompt(prompt, prompt_args_copy) if prompt_args_copy else formatted
            cache_key = f'{self.model_id},{cache_formatted}'
            cache_hex = sha256(cache_key.encode('utf-8')).hexdigest()
            cache_file = f'cache/llm/{cache_hex}.txt'

            if os.path.exists(cache_file):
                logger.debug('%sCached response %s%s', c_blue, cache_file, c_norm)
                with open(cache_file, 'r', encoding='utf-8') as f:
                    response = f.read()
            else:
                try:
                    response = self.llm.predict(formatted)
                except Exception as e:
                    raise ModelError(f'{e!s} Model: {self.model_id}') from e
                os.makedirs(os.path.dirname(os.path.realpath(cache_file)), exist_ok=True)
                with open(cache_file, 'w') as f:
                    f.write(response)

        if self.verbose_response:
            logger.info('%s%s%s', c_green, response, c_norm)
            
        return response
    
    @property
    def model_id(self) -> str:
        if isinstance(self.llm, BedrockLLMProvider):
            return self.llm.model_id
        return str(type(self.llm).__name__)

    @property
    def model(self) -> str:
        if not isinstance(self.llm, BedrockLLMProvider):
            raise ModelError(f'Invalid LLM type: {type(self.llm)} does not support model')
        return self.llm.model_id

    
LLMCacheType = Union[Any, LLMCache]
