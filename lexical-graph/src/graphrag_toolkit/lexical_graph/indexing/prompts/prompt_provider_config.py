# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import os
import boto3
from boto3.session import Session as Boto3Session

from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# AWSConfig – base for config classes needing AWS session/client support
# ------------------------------------------------------------------------------
@dataclass(init=True, kw_only=True)
class AWSConfig(ABC):
    """
    Base configuration class for AWS-related prompt providers.

    This class manages AWS session and client creation for use by provider configs.
    It provides properties for common AWS services and caches clients for reuse.
    """
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = None
    _aws_clients: Dict[str, Any] = field(default_factory=dict)
    _boto3_session: Optional[Boto3Session] = field(default=None, init=False)


    @property
    def session(self) -> Boto3Session:
        """
        Returns a boto3 session for AWS API access.

        If a session does not already exist, this property creates one using the configured AWS profile and region.
        The session is cached for reuse in subsequent calls.

        Returns:
            Boto3Session: The boto3 session object for AWS API access.
        """
        if self._boto3_session is None:
            if self.aws_profile:
                self._boto3_session = Boto3Session(
                    profile_name=self.aws_profile,
                    region_name=self.aws_region,
                )
            else:
                self._boto3_session = Boto3Session(region_name=self.aws_region)
        return self._boto3_session

    def _get_or_create_client(self, service_name: str) -> Any:
        """
        Returns an AWS service client for the specified service name.

        If a client for the service already exists, it is returned from the cache.
        Otherwise, a new client is created using the current session and cached for future use.

        Args:
            service_name: The name of the AWS service for which to get a client.

        Returns:
            Any: The AWS service client instance.
        """
        if service_name in self._aws_clients:
            return self._aws_clients[service_name]
        client = self.session.client(service_name)
        self._aws_clients[service_name] = client
        return client

    @property
    def s3(self) -> Any:
        """
        Returns an AWS S3 client instance.

        This property provides access to a cached or newly created S3 client for interacting with AWS S3 services.

        Returns:
            Any: The AWS S3 client instance.
        """
        return self._get_or_create_client("s3")

    @property
    def bedrock(self) -> Any:
        """
        Returns an AWS Bedrock Agent client instance.

        This property provides access to a cached or newly created Bedrock Agent client for interacting with AWS Bedrock services.

        Returns:
            Any: The AWS Bedrock Agent client instance.
        """
        return self._get_or_create_client("bedrock-agent")

    @property
    def sts(self) -> Any:
        """
        Returns an AWS STS (Security Token Service) client instance.

        This property provides access to a cached or newly created STS client for interacting with AWS STS services.

        Returns:
            Any: The AWS STS client instance.
        """
        return self._get_or_create_client("sts")


# ------------------------------------------------------------------------------
# ProviderConfig – abstract interface for building PromptProviders
# ------------------------------------------------------------------------------
@dataclass(kw_only=True)
class ProviderConfig(AWSConfig):
    """
    Abstract base configuration class for building PromptProvider instances.

    This class defines the interface for provider configs that can construct PromptProvider objects.
    """

    @abstractmethod
    def build(self) -> PromptProvider:
        pass


# ------------------------------------------------------------------------------
# BedrockPromptProviderConfig
# ------------------------------------------------------------------------------
@dataclass
class BedrockPromptProviderConfig(ProviderConfig):
    """
    Configuration class for Bedrock prompt providers.

    This class manages ARNs, versions, and resolution logic for system and user prompts in AWS Bedrock.
    It provides properties and methods to resolve ARNs and build BedrockPromptProvider instances.
    """
    system_prompt_arn: str = field(default_factory=lambda: os.environ["SYSTEM_PROMPT_ARN"])
    user_prompt_arn: str = field(default_factory=lambda: os.environ["USER_PROMPT_ARN"])
    system_prompt_version: Optional[str] = field(default_factory=lambda: os.getenv("SYSTEM_PROMPT_VERSION"))
    user_prompt_version: Optional[str] = field(default_factory=lambda: os.getenv("USER_PROMPT_VERSION"))

    @property
    def resolved_system_prompt_arn(self) -> str:
        return self._resolve_prompt_arn(self.system_prompt_arn)

    @property
    def resolved_user_prompt_arn(self) -> str:
        return self._resolve_prompt_arn(self.user_prompt_arn)

    def _resolve_prompt_arn(self, identifier: str) -> str:
        # If it's already a full ARN, return it
        if identifier.startswith("arn:"):
            return identifier

        # Get caller identity and extract the partition from the ARN
        caller_arn = self.sts.get_caller_identity()["Arn"]
        # Example ARN: arn:aws-us-gov:sts::123456789012:assumed-role/...
        partition = caller_arn.split(":")[1]
        account_id = self.sts.get_caller_identity()["Account"]

        return f"arn:{partition}:bedrock:{self.aws_region}:{account_id}:prompt/{identifier}"

    def build(self) -> PromptProvider:
        from graphrag_toolkit.lexical_graph.prompts.bedrock_prompt_provider import BedrockPromptProvider
        return BedrockPromptProvider(config=self)


# ------------------------------------------------------------------------------
# S3PromptProviderConfig
# ------------------------------------------------------------------------------
@dataclass(kw_only=True)
class S3PromptProviderConfig(ProviderConfig):
    """
    Configuration class for S3 prompt providers.

    This class manages S3 bucket, prefix, and prompt file names for loading prompts from AWS S3.
    It provides a method to build an S3PromptProvider instance using the current configuration.
    """
    bucket: str = field(default_factory=lambda: os.environ["PROMPT_S3_BUCKET"])
    prefix: str = field(default_factory=lambda: os.getenv("PROMPT_S3_PREFIX", "prompts/"))
    system_prompt_file: str = "system_prompt.txt"
    user_prompt_file: str = "user_prompt.txt"


    def build(self) -> PromptProvider:
        from graphrag_toolkit.lexical_graph.prompts.s3_prompt_provider import S3PromptProvider
        return S3PromptProvider(config=self)


# ------------------------------------------------------------------------------
# FilePromptProviderConfig
# ------------------------------------------------------------------------------
@dataclass
class FilePromptProviderConfig(ProviderConfig):
    """
    Configuration class for file-based prompt providers.

    This class manages the base path and prompt file names for loading prompts from the local filesystem.
    It provides a method to build a FilePromptProvider instance using the current configuration.
    """
    base_path: str = field(default_factory=lambda: os.getenv("PROMPT_PATH", "./prompts"))
    system_prompt_file: str = "system_prompt.txt"
    user_prompt_file: str = "user_prompt.txt"

    def build(self) -> PromptProvider:
        from graphrag_toolkit.lexical_graph.prompts.file_prompt_provider import FilePromptProvider
        return FilePromptProvider(
            config=self,
            system_prompt_file=self.system_prompt_file,
            user_prompt_file=self.user_prompt_file
        )


# ------------------------------------------------------------------------------
# StaticPromptProviderConfig
# ------------------------------------------------------------------------------
@dataclass
class StaticPromptProviderConfig:
    """
    Configuration class for static prompt providers.

    This class provides a static method to build a StaticPromptProvider instance.
    """
    @staticmethod
    def build() -> PromptProvider:
        from graphrag_toolkit.lexical_graph.prompts.static_prompt_provider import StaticPromptProvider
        return StaticPromptProvider()


# ------------------------------------------------------------------------------
# Suggested Next Enhancements (Optional)
# ------------------------------------------------------------------------------

# 1. Unified PromptProviderRegistry or Factory
#    - Introduce a registry that maps provider types to config classes, e.g.,
#        registry = {
#            "static": StaticPromptProviderConfig,
#            "file": FilePromptProviderConfig,
#            "s3": S3PromptProviderConfig,
#            "bedrock": BedrockPromptProviderConfig
#        }
#    - Enable initialization from a config dict: registry[type](**params).build()

# 2. Config Serialization
#    - Add `.to_dict()` and `.from_dict()` methods to each config class for CLI/JSON compatibility.
#    - Useful for web UIs or YAML-driven orchestration.

# 3. Validation & Type Enforcement
#    - Use Pydantic or `__post_init__()` methods to validate inputs (e.g., ARN format, S3 bucket name).
#    - Example: validate AWS region format or prompt ARN prefix.

# 4. Logging Enhancements
#    - Add verbose logging on each provider (e.g., which prompt path or ARN was loaded).
#    - Include diagnostics for STS calls and client creation failures.

# 5. Caching Layer
#    - Cache resolved prompt text in memory or on disk (especially for S3 and Bedrock).
#    - Avoid unnecessary repeated fetches in batch queries.

# 6. Runtime Provider Switching
#    - Allow query-time override of prompt provider (e.g., via `query_engine.query(..., prompt_provider=...)`).
#    - Enables experimentation with different prompt strategies.

# 7. Prompt Fallback Strategy
#    - Support fallback to defaults or static provider if S3/Bedrock fails.
#    - Enables robust operation in partially degraded environments.

# 8. Custom Prompt Variables
#    - Support variable interpolation in prompt templates (e.g., using `{tenant_id}` or `{user_role}`).
#    - Useful for multi-tenant or role-specific prompting.

# 9. Multi-Language Prompt Support
#    - Load prompt variants based on locale/language code.
#    - Supports internationalization of RAG applications.

# 10. Bedrock Caching with Prompt Versioning
#     - Cache based on (ARN, version) tuple.
#     - Useful when managing multiple versions in experiments or A/B testing.
