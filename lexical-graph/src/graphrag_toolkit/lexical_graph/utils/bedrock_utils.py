# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import llama_index.llms.bedrock_converse.utils
from typing import Any, Callable

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)



def _create_retry_decorator(client: Any, max_retries: int) -> Callable[[Any], Any]:
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
