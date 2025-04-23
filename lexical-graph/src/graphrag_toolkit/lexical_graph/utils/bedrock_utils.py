# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import copy
import llama_index.llms.bedrock_converse.utils
import llama_index.embeddings.bedrock
from typing import Any, Callable, Union, List, Literal
from llama_index.core.base.embeddings.base import Embedding


from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

llama_index.embeddings.bedrock.BedrockEmbedding._inner_get_embedding = copy.deepcopy(llama_index.embeddings.bedrock.BedrockEmbedding._get_embedding)

def _get_embedding(
        self, payload: Union[str, List[str]], type: Literal['text', 'query']
    ) -> Union[Embedding, List[Embedding]]:
    count = 0
    embedding = None
    while not embedding:
        try:
            embedding = self._inner_get_embedding(payload, type)
        except Exception as err:
            count += 1
            logger.error(f'Error while getting embedding: {err}')
            if count >= 3:
                raise
            else:
                time.sleep(2)
    return embedding

llama_index.embeddings.bedrock.BedrockEmbedding._get_embedding = _get_embedding

def _create_retry_decorator(client: Any, max_retries: int) -> Callable[[Any], Any]:
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
        wait=wait_exponential(multiplier=1, min=min_seconds, max=max_seconds),
        retry=(
            retry_if_exception_type(client.exceptions.ThrottlingException) | 
            retry_if_exception_type(client.exceptions.ModelTimeoutException) |
            retry_if_exception_type(client.exceptions.ModelErrorException)
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    
llama_index.llms.bedrock_converse.utils._create_retry_decorator = _create_retry_decorator
