# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/utils/query_decomposition."""

from unittest.mock import MagicMock

from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.utils.query_decomposition import (
    SINGLE_QUESTION_THRESHOLD,
    QueryDecomposition,
)


def _decomposer(llm_response):
    args = ProcessorArgs(no_cache=True)
    llm = MagicMock()
    llm.predict.return_value = llm_response
    # Bypass LLMCache wrapping by injecting a pre-built MagicMock that already
    # quacks like LLMCache from the decomposer's perspective.
    from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache
    llm_cache = MagicMock(spec=LLMCache)
    llm_cache.predict = llm.predict
    return QueryDecomposition(args=args, llm=llm_cache), llm


class TestDecomposeQuery:
    def test_short_query_returns_single_bundle(self):
        decomposer, llm = _decomposer('whatever')
        result = decomposer.decompose_query(QueryBundle('short question'))
        assert len(result) == 1
        assert result[0].query_str == 'short question'
        llm.predict.assert_not_called()

    def test_long_non_multipart_query_returns_original(self):
        long_q = ' '.join(['word'] * (SINGLE_QUESTION_THRESHOLD + 1))
        decomposer, _ = _decomposer('yes, this is one question')
        result = decomposer.decompose_query(QueryBundle(long_q))
        assert len(result) == 1
        assert result[0].query_str == long_q

    def test_long_multipart_query_is_decomposed(self):
        long_q = ' '.join(['word'] * (SINGLE_QUESTION_THRESHOLD + 1))
        decomposer, llm = _decomposer('no, multipart')
        llm.predict.side_effect = [
            'no, multipart',
            'subquery one\nsubquery two\n',
        ]
        result = decomposer.decompose_query(QueryBundle(long_q))
        assert [r.query_str for r in result] == ['subquery one', 'subquery two']

    def test_extract_subqueries_drops_blank_lines(self):
        decomposer, llm = _decomposer('')
        llm.predict.return_value = 'a\n\nb\n'
        result = decomposer._extract_subqueries('q')
        assert [r.query_str for r in result] == ['a', 'b']

    def test_is_multipart_question_true_when_response_starts_with_no(self):
        decomposer, llm = _decomposer('No, this asks several things')
        assert decomposer._is_multipart_question('q') is True

    def test_is_multipart_question_false_when_response_starts_with_yes(self):
        decomposer, llm = _decomposer('Yes, single question')
        assert decomposer._is_multipart_question('q') is False
