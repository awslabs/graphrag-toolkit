# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for utils/reranker_utils."""

from unittest.mock import patch

import numpy
import pandas as pd

from graphrag_toolkit.lexical_graph.utils import reranker_utils
from graphrag_toolkit.lexical_graph.utils.reranker_utils import (
    score_values_with_tfidf,
    to_float,
)


class TestToFloat:
    def test_numpy_float64_unwraps(self):
        v = numpy.float64(1.5)
        result = to_float(v)
        assert result == 1.5
        assert isinstance(result, float)

    def test_numpy_float32_unwraps(self):
        v = numpy.float32(2.5)
        result = to_float(v)
        assert result == 2.5

    def test_plain_float_passthrough(self):
        assert to_float(3.14) == 3.14

    def test_int_passthrough(self):
        assert to_float(7) == 7


def _matcher_df(rows):
    """Build a DataFrame shaped like tm.matcher output: column 0 is the
    original name, then triples (Lookup, Confidence, ???) per match."""
    return pd.DataFrame(rows)


class TestScoreValuesWithTfidf:
    def test_value_error_falls_back_to_zero_scores(self):
        with patch.object(reranker_utils.tm, 'matcher', side_effect=ValueError):
            result = score_values_with_tfidf(['a', 'b'], ['x'])
        assert result == {'a': 0.0, 'b': 0.0}

    def test_value_error_drops_empty_padding(self):
        # Internal code pads values_to_score with '' to reach max_num_values_to_score.
        # The fallback excludes those padded blanks.
        with patch.object(reranker_utils.tm, 'matcher', side_effect=ValueError):
            result = score_values_with_tfidf(['a'], ['x', 'y'])
        assert '' not in result
        assert result == {'a': 0.0}

    def test_results_sorted_descending_by_score(self):
        df = _matcher_df([
            ['x', 'a', 0.3, None],
            ['y', 'b', 0.9, None],
        ])
        with patch.object(reranker_utils.tm, 'matcher', return_value=df):
            result = score_values_with_tfidf(['a', 'b'], ['x', 'y'])
        assert list(result.keys()) == ['b', 'a']
        assert result['b'] > result['a']

    def test_secondary_match_score_is_dampened(self):
        # num_primary_match_values=1 means row 0 keeps full score, row 1 gets *0.9.
        df = _matcher_df([
            ['x', 'a', 1.0, None],
            ['y', 'b', 1.0, None],
        ])
        with patch.object(reranker_utils.tm, 'matcher', return_value=df):
            result = score_values_with_tfidf(
                ['a', 'b'], ['x', 'y'], num_primary_match_values=1,
            )
        assert result['a'] == 1.0
        assert result['b'] == 0.9

    def test_zero_base_score_skips_multiplier(self):
        df = _matcher_df([
            ['x', 'a', 0.0, None],
        ])
        with patch.object(reranker_utils.tm, 'matcher', return_value=df):
            result = score_values_with_tfidf(
                ['a'], ['x'], num_primary_match_values=0,
            )
        assert result['a'] == 0.0

    def test_repeated_value_averages_scores(self):
        df = _matcher_df([
            ['x', 'a', 0.4, None],
            ['y', 'a', 0.8, None],
        ])
        with patch.object(reranker_utils.tm, 'matcher', return_value=df):
            result = score_values_with_tfidf(['a'], ['x', 'y'])
        import pytest
        assert result['a'] == pytest.approx(0.6)
