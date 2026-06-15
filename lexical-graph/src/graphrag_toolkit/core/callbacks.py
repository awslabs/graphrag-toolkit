# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Callback registry for observability events."""

from __future__ import annotations

from typing import Callable

# Event type constants
LLM_START = "llm_start"
LLM_END = "llm_end"
EMBEDDING_START = "embedding_start"
EMBEDDING_END = "embedding_end"
RETRIEVAL_START = "retrieval_start"
RETRIEVAL_END = "retrieval_end"
TRANSFORM_START = "transform_start"
TRANSFORM_END = "transform_end"


class CallbackRegistry:
    """Class-level registry for event handlers."""

    _handlers: list[Callable[[str, dict], None]] = []

    @classmethod
    def register(cls, handler: Callable[[str, dict], None]) -> None:
        """Add a handler."""
        cls._handlers.append(handler)

    @classmethod
    def emit(cls, event_type: str, payload: dict) -> None:
        """Call all registered handlers."""
        for handler in list(cls._handlers):  # snapshot for thread safety
            try:
                handler(event_type, payload)
            except Exception:
                pass  # Don't let observer errors kill the pipeline

    @classmethod
    def clear(cls) -> None:
        """Remove all handlers."""
        cls._handlers.clear()

    @classmethod
    def get_handlers(cls) -> list[Callable[[str, dict], None]]:
        """Return registered handlers."""
        return cls._handlers
