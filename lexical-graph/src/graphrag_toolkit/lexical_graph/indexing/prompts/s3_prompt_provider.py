# graphrag_toolkit/lexical_graph/prompts/s3_prompt_provider.py
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_config import S3PromptProviderConfig
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class S3PromptProvider(PromptProvider):
    """
    Loads system and user prompts from an S3 bucket using provided configuration.

    Attributes:
        config (S3PromptProviderConfig): Configuration object including bucket, prefix,
                                         and optionally custom file names for prompts.
    """

    def __init__(self, config: S3PromptProviderConfig):
        self.config = config

    def _load_prompt(self, filename: str) -> str:
        """
        Loads a prompt file from the configured S3 bucket and returns its contents as a string.

        Args:
            filename: The name of the prompt file to load from S3.

        Returns:
            The contents of the prompt file as a UTF-8 string.
        """
        key = f"{self.config.prefix.rstrip('/')}/{filename}"
        logger.info(f"[Prompt Debug] Loading prompt from S3: s3://{self.config.bucket}/{key}")
        s3_client = self.config.s3  # session-aware S3 client from config
        response = s3_client.get_object(Bucket=self.config.bucket, Key=key)
        return response["Body"].read().decode("utf-8").rstrip()

    def get_system_prompt(self) -> str:
        """
        Retrieves the system prompt from S3.

        Returns:
            The contents of the system prompt file.
        """
        return self._load_prompt(self.config.system_prompt_file)

    def get_user_prompt(self) -> str:
        """
        Retrieves the user prompt from S3.

        Returns:
            The contents of the user prompt file.
        """
        return self._load_prompt(self.config.user_prompt_file)
