# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for graphrag_toolkit.core.prompt."""

import pytest

from graphrag_toolkit.core.prompt import ChatPromptTemplate, PromptTemplate


class TestPromptTemplate:
    def test_basic_format(self):
        pt = PromptTemplate("Hello {name}")
        assert pt.format(name="world") == "Hello world"

    def test_multiple_vars(self):
        pt = PromptTemplate("{greeting} {name}, you are {age}")
        assert pt.format(greeting="Hi", name="Alice", age="30") == "Hi Alice, you are 30"

    def test_format_messages(self):
        pt = PromptTemplate("Tell me about {topic}")
        result = pt.format_messages(topic="graphs")
        assert result == [{"role": "user", "content": "Tell me about graphs"}]

    def test_template_vars(self):
        pt = PromptTemplate("{a} and {b} with {c}")
        assert pt.template_vars == {"a", "b", "c"}

    def test_partial_format(self):
        pt = PromptTemplate("{greeting} {name}")
        partial = pt.partial_format(greeting="Hello")
        assert "{name}" in partial.template
        assert "Hello" in partial.template

    def test_partial_format_then_full(self):
        pt = PromptTemplate("{greeting} {name}")
        partial = pt.partial_format(greeting="Hi")
        assert partial.format(name="Bob") == "Hi Bob"

    def test_empty_template(self):
        pt = PromptTemplate("")
        assert pt.format() == ""

    def test_no_vars(self):
        pt = PromptTemplate("static text")
        assert pt.format() == "static text"
        assert pt.template_vars == set()

    def test_missing_var_raises(self):
        pt = PromptTemplate("{x}")
        with pytest.raises(KeyError):
            pt.format()

    def test_extra_vars_ignored(self):
        pt = PromptTemplate("{x}")
        assert pt.format(x="1", y="2") == "1"

    def test_repr(self):
        pt = PromptTemplate("Hello {name}")
        assert "Hello" in repr(pt)


class TestChatPromptTemplate:
    def test_format_multi_message(self):
        cpt = ChatPromptTemplate([
            {"role": "system", "content": "You are {persona}"},
            {"role": "user", "content": "Tell me about {topic}"},
        ])
        result = cpt.format(persona="helpful", topic="graphs")
        assert result == [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Tell me about graphs"},
        ]

    def test_template_vars_collects_all(self):
        cpt = ChatPromptTemplate([
            {"role": "system", "content": "You are {persona}"},
            {"role": "user", "content": "{query}"},
        ])
        assert cpt.template_vars == {"persona", "query"}

    def test_repr(self):
        cpt = ChatPromptTemplate([{"role": "user", "content": "hi"}])
        assert "1" in repr(cpt)
