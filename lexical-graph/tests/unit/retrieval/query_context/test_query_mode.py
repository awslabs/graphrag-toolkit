# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/query_mode."""

from unittest.mock import MagicMock

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context.query_mode import (
    QueryMode,
    QueryModeProvider,
)
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache


def _provider(llm_response):
    llm = MagicMock(spec=LLMCache)
    llm.predict.return_value = llm_response
    return QueryModeProvider(args=ProcessorArgs(no_cache=True), llm=llm), llm


class TestQueryModeProvider:
    def test_simple_when_response_contains_single(self):
        provider, _ = _provider('single')
        assert provider.get_query_mode('hello?') is QueryMode.SIMPLE

    def test_complex_when_response_says_multipart(self):
        provider, _ = _provider('multipart')
        assert provider.get_query_mode('a and b and c?') is QueryMode.COMPLEX

    def test_response_is_lowercased_and_stripped(self):
        provider, _ = _provider('  Single  \n')
        assert provider.get_query_mode('q') is QueryMode.SIMPLE

    def test_ambiguous_response_defaults_to_complex(self):
        provider, _ = _provider('unknown')
        assert provider.get_query_mode('q') is QueryMode.COMPLEX
