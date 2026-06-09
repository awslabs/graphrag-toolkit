# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/keyword_provider."""

from unittest.mock import MagicMock

from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_provider import (
    KeywordProvider,
    KeywordProviderMode,
)
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache


def _provider(llm_responses, mode=KeywordProviderMode.ALL, max_keywords=4):
    llm = MagicMock(spec=LLMCache)
    llm.predict.side_effect = llm_responses
    provider = KeywordProvider(
        args=ProcessorArgs(no_cache=True, max_keywords=max_keywords),
        llm=llm,
        mode=mode,
    )
    return provider, llm


class TestKeywordProvider:
    def test_simple_mode_calls_llm_once(self):
        provider, llm = _provider(['alpha^beta'], mode=KeywordProviderMode.SIMPLE)
        result = provider.get_keywords(QueryBundle('q'))
        assert result == ['alpha', 'beta']
        assert llm.predict.call_count == 1

    def test_all_mode_combines_simple_and_enriched(self):
        provider, llm = _provider(
            ['alpha^beta', 'gamma^delta'], mode=KeywordProviderMode.ALL,
        )
        result = provider.get_keywords(QueryBundle('q'))
        assert set(result) == {'alpha', 'beta', 'gamma', 'delta'}
        assert llm.predict.call_count == 2

    def test_dedups_case_insensitively(self):
        provider, _ = _provider(
            ['Alpha^Beta', 'alpha^GAMMA'], mode=KeywordProviderMode.ALL,
        )
        result = provider.get_keywords(QueryBundle('q'))
        assert sorted(result) == ['alpha', 'beta', 'gamma']

    def test_truncates_to_max_keywords(self):
        provider, _ = _provider(
            ['a^b^c^d^e', 'f^g^h^i^j'],
            mode=KeywordProviderMode.ALL,
            max_keywords=3,
        )
        result = provider.get_keywords(QueryBundle('q'))
        assert len(result) == 3

    def test_num_keywords_passed_as_half_of_max(self):
        provider, llm = _provider(['k1'], mode=KeywordProviderMode.SIMPLE, max_keywords=6)
        provider.get_keywords(QueryBundle('q'))
        assert llm.predict.call_args.kwargs['max_keywords'] == 3

    def test_num_keywords_minimum_one(self):
        provider, llm = _provider(['k1'], mode=KeywordProviderMode.SIMPLE, max_keywords=1)
        provider.get_keywords(QueryBundle('q'))
        assert llm.predict.call_args.kwargs['max_keywords'] == 1
