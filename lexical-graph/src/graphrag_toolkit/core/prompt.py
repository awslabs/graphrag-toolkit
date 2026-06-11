# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prompt template classes replacing llama_index.core.prompts."""

from __future__ import annotations

import string


class _PartialFormatMapping(dict):
    """Mapping that returns {key} for missing keys during partial formatting."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class PromptTemplate:
    """Simple prompt template using str.format() syntax."""

    def __init__(self, template: str):
        self.template = template

    @property
    def template_vars(self) -> set[str]:
        """Extract variable names from the template."""
        return {
            name
            for _, name, _, _ in string.Formatter().parse(self.template)
            if name is not None
        }

    def format(self, **kwargs) -> str:
        """Format the template with provided variables."""
        return self.template.format(**kwargs)

    def format_messages(self, **kwargs) -> list[dict]:
        """Format and wrap as a user message."""
        return [{"role": "user", "content": self.format(**kwargs)}]

    def partial_format(self, **kwargs) -> PromptTemplate:
        """Return a new template with some variables filled."""
        new_template = self.template.format_map(_PartialFormatMapping(kwargs))
        return PromptTemplate(new_template)

    def __repr__(self) -> str:
        preview = self.template[:50]
        if len(self.template) > 50:
            preview += "..."
        return f"PromptTemplate({preview!r})"


class ChatPromptTemplate:
    """Multi-message prompt template."""

    def __init__(self, message_templates: list[dict]):
        self.message_templates = message_templates

    @property
    def template_vars(self) -> set[str]:
        """Collect variable names from all message templates."""
        vars_ = set()
        for msg in self.message_templates:
            for _, name, _, _ in string.Formatter().parse(msg["content"]):
                if name is not None:
                    vars_.add(name)
        return vars_

    def format(self, **kwargs) -> list[dict]:
        """Format all message templates."""
        return [
            {"role": msg["role"], "content": msg["content"].format(**kwargs)}
            for msg in self.message_templates
        ]

    def __repr__(self) -> str:
        return f"ChatPromptTemplate(messages={len(self.message_templates)})"
