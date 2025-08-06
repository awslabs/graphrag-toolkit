# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
This module exposes the core prompt provider interface and registry entry point.

To avoid circular import errors, concrete provider classes (S3, Bedrock, File, Static)
are not imported here. Use `prompt_provider_config.py` to dynamically construct providers.
"""

from .prompt_provider_base import PromptProvider
from .prompt_provider_registry import PromptProviderRegistry

__all__ = [
    "PromptProvider",
    "PromptProviderRegistry",
]
