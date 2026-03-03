"""Tests for reranker utilities (reranker_utils.py).

This module provides two utilities used during retrieval ranking:

  to_float   — coerces numpy scalar types to plain Python floats so that
               scores can be safely serialized to JSON or compared with
               standard Python operators. Non-numpy values pass through unchanged.

  score_values_with_tfidf — ranks a list of candidate strings against a list of
               query terms using TF-IDF character n-gram matching (via tfidf_matcher).
               Returns a dict of {value: score} sorted highest-first. The primary
               use case is fuzzy-matching extracted entity/topic names back to a
               canonical vocabulary.
"""

import numpy
import pytest
from graphrag_toolkit.lexical_graph.utils.reranker_utils import to_float, score_values_with_tfidf


# --- to_float ---
# numpy float scalars are not plain Python floats. Storing them in dicts or
# returning them over an API can cause JSON serialization errors. to_float
# converts them via .item(), which returns the equivalent Python primitive.


def test_to_float_numpy_float64():
    """numpy.float64 is unwrapped to a Python float via .item()."""
    result = to_float(numpy.float64(3.14))
    assert isinstance(result, float)
    assert result == pytest.approx(3.14)


def test_to_float_numpy_float32():
    """numpy.float32 is unwrapped to a Python float via .item()."""
    result = to_float(numpy.float32(2.5))
    assert isinstance(result, float)
    assert result == pytest.approx(2.5)


def test_to_float_python_float_passthrough():
    """A plain Python float is returned as-is (no conversion needed)."""
    assert to_float(3.14) == 3.14


def test_to_float_int_passthrough():
    """A plain int is returned as-is (int is not a numpy type)."""
    assert to_float(1) == 1


# --- score_values_with_tfidf ---
# The function uses character trigram TF-IDF to score each value in `values`
# against the query terms in `match_values`. Results are returned as a dict
# sorted by score descending. An exact string match should always score highest.


def test_score_values_with_tfidf_exact_match_highest():
    """A value identical to the query term should rank first in the result dict.

    The returned dict is sorted highest-score-first, so the first key is the
    top-ranked result. An exact match should outscore all dissimilar candidates.
    """
    values = ["apple pie", "banana split", "cherry tart"]
    result = score_values_with_tfidf(values, ["apple pie"])
    top_value = list(result.keys())[0]
    assert top_value == "apple pie"


def test_score_values_with_tfidf_return_type():
    """Result is a dict with string keys and numeric (int or float) score values."""
    result = score_values_with_tfidf(["alpha", "beta", "gamma"], ["alpha"])
    assert isinstance(result, dict)
    for k, v in result.items():
        assert isinstance(k, str)
        assert isinstance(v, (int, float))


def test_score_values_with_tfidf_non_negative():
    """All scores must be >= 0. TF-IDF cosine similarity is bounded [0, 1],
    and the ranked-score multiplier (1.0 or 0.9) preserves non-negativity.
    """
    result = score_values_with_tfidf(["one", "two", "three"], ["one"])
    for score in result.values():
        assert score >= 0.0
