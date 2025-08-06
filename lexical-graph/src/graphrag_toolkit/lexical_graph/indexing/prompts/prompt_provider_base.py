# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class PromptProvider(ABC):
    """
    Abstract base class for loading prompts from various sources.
    """

    @abstractmethod
    def get_system_prompt(self) -> str:
        """
        Returns the system prompt as a string.
        """
        pass

    @abstractmethod
    def get_user_prompt(self) -> str:
        """
        Returns the user prompt as a string.
        """
        pass
