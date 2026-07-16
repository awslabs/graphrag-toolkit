# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/processors/reranker_chain."""

import pytest

from graphrag_toolkit.lexical_graph.retrieval.processors.reranker_chain import normalize_reranker_chain


class TestNormalizeRerankerChain:
    @pytest.mark.parametrize(
        'reranker, expected',
        [
            (None, []),
            (False, []),
            (0, []),
            ({}, []),
            ([], []),
            (' TFIDF ', ['tfidf']),
            ('  ', []),
            (['Bedrock', ' tfidf'], ['bedrock', 'tfidf']),
            (['', 'tfidf'], ['tfidf']),
            ([' '], []),
            (['none'], ['none']),
        ],
    )
    def test_normalizes_supported_values(self, reranker, expected):
        assert normalize_reranker_chain(reranker) == expected

    @pytest.mark.parametrize(
        'reranker, error_type, match',
        [
            (('bedrock', 'tfidf'), TypeError, 'string or list'),
            (42, TypeError, 'string or list'),
            ({'reranker': 'tfidf'}, TypeError, 'string or list'),
            ([42, 'tfidf'], ValueError, 'must be strings'),
            ([None, 'tfidf'], ValueError, 'must be strings'),
            (['bedrock', 'none', 'tfidf'], ValueError, 'none can only be used'),
            (['bedrock', 'typo'], ValueError, 'Unknown reranker'),
        ],
    )
    def test_rejects_invalid_chain_values(self, reranker, error_type, match):
        with pytest.raises(error_type, match=match):
            normalize_reranker_chain(reranker)

    def test_unknown_single_string_is_preserved_for_legacy_behaviour(self):
        # Legacy string form is not validated against known rerankers.
        assert normalize_reranker_chain('something-weird') == ['something-weird']
