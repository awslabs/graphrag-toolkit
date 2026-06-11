# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Native message format converters for batch inference.

Replaces llama_index.llms.bedrock_converse.utils.messages_to_converse_messages
and llama_index.llms.anthropic.utils.messages_to_anthropic_messages with
lightweight implementations that handle the simple system+user message
patterns used in batch extraction.
"""

from typing import Any, List, Sequence, Tuple, Dict


def messages_to_converse_messages(
    messages: Sequence[Any],
) -> Tuple[List[Dict[str, Any]], Any]:
    """Convert chat messages to AWS Bedrock Converse format.

    Args:
        messages: Sequence of message objects with .role and .content attributes.

    Returns:
        Tuple of (converse_messages, system_prompt_text_or_None).
    """
    converse_messages = []
    system_prompt = None

    for message in messages:
        if isinstance(message, dict):
            role = message.get("role", "user")
            content = message.get("content", "")
        else:
            role = message.role.value if hasattr(message.role, "value") else str(message.role)
            content = message.content

        if role == "system":
            system_prompt = content
        else:
            # content may already be in converse format [{"text": "..."}]
            if isinstance(content, list):
                converse_messages.append({"role": role, "content": content})
            else:
                converse_messages.append(
                    {"role": role, "content": [{"text": content}]}
                )

    return converse_messages, system_prompt


def messages_to_anthropic_messages(
    messages: Sequence[Any],
) -> Tuple[List[Dict[str, Any]], Any]:
    """Convert chat messages to Anthropic API format.

    Args:
        messages: Sequence of message objects with .role and .content attributes.

    Returns:
        Tuple of (anthropic_messages, system_prompt_text_or_None).
    """
    anthropic_messages = []
    system_prompt = None

    for message in messages:
        if isinstance(message, dict):
            role = message.get("role", "user")
            content = message.get("content", "")
        else:
            role = message.role.value if hasattr(message.role, "value") else str(message.role)
            content = message.content

        if role == "system":
            # Extract text from system prompt (may be string or converse content blocks)
            if isinstance(content, list):
                system_prompt = " ".join(block.get("text", "") for block in content if isinstance(block, dict))
            else:
                system_prompt = content
        else:
            # Convert converse content blocks [{"text": "..."}] to plain string for anthropic
            if isinstance(content, list):
                text_parts = [block.get("text", "") for block in content if isinstance(block, dict)]
                anthropic_messages.append({"role": role, "content": " ".join(text_parts)})
            else:
                anthropic_messages.append({"role": role, "content": content})

    return anthropic_messages, system_prompt
