# static_prompt_provider.py

from graphrag_toolkit.lexical_graph.prompts.prompt_provider_base import PromptProvider
from graphrag_toolkit.lexical_graph.retrieval.prompts import (
    ANSWER_QUESTION_SYSTEM_PROMPT,
    ANSWER_QUESTION_USER_PROMPT,
)
from graphrag_toolkit.lexical_graph.logging import logging

logger = logging.getLogger(__name__)

class StaticPromptProvider(PromptProvider):
    """
    Provides static system and user prompts for use in the application.
    This class returns predefined prompt strings that do not change at runtime.
    """
    def __init__(self):
        """
        Initializes a StaticPromptProvider with predefined system and user prompts.
        This constructor sets the system and user prompts to static values for consistent retrieval.
        """
        self._system_prompt = ANSWER_QUESTION_SYSTEM_PROMPT
        self._user_prompt = ANSWER_QUESTION_USER_PROMPT
        logger.debug(f"System Prompt (truncated): {self._system_prompt[:60]}...")
        logger.debug(f"User Prompt (truncated): {self._user_prompt[:60]}...")

    def get_system_prompt(self) -> str:
        """
        Returns the static system prompt string.
        This method provides the system prompt that is set during initialization.

        Returns:
            The system prompt as a string.
        """
        return self._system_prompt

    def get_user_prompt(self) -> str:
        """
        Returns the static user prompt string.
        This method provides the user prompt that is set during initialization.

        Returns:
            The user prompt as a string.
        """
        return self._user_prompt
