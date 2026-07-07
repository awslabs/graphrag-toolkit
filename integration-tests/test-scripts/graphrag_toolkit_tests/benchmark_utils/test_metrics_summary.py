# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests for the metrics_summary module.
"""

from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.strategies import lists, integers, floats, fixed_dictionaries

from graphrag_toolkit_tests.benchmark_utils.metrics_summary import (
    _compute_latency_stats,
    compute_metrics_summary,
    BEDROCK_PRICING,
)


def _reference_percentile(sorted_values, p):
    """Reference implementation of linear interpolation percentile matching _percentile in metrics_summary.py."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return float(sorted_values[0])

    rank = (p / 100.0) * (n - 1)
    lower = int(rank)
    upper = lower + 1
    fraction = rank - lower

    if upper >= n:
        return float(sorted_values[-1])

    return sorted_values[lower] + fraction * (sorted_values[upper] - sorted_values[lower])


class TestAggregateLatencyStatistics:
    """
    Aggregate latency statistics computation

    For any non-empty list of non-null integer latency values, the computed aggregate
    statistics shall satisfy: avg equals the arithmetic mean rounded to 2 decimal places,
    p50 equals the median rounded to 2 decimal places, and p95 equals the 95th percentile
    rounded to 2 decimal places.
    """

    @settings(max_examples=100)
    @given(values=lists(integers(min_value=0, max_value=100000), min_size=1))
    def test_avg_equals_arithmetic_mean_rounded_to_2dp(self, values):
        """avg equals the arithmetic mean of the values rounded to 2 decimal places."""
        result = _compute_latency_stats(values)
        assert result is not None

        expected_avg = round(sum(values) / len(values), 2)
        assert result['avg'] == expected_avg

    @settings(max_examples=100)
    @given(values=lists(integers(min_value=0, max_value=100000), min_size=1))
    def test_p50_equals_median_rounded_to_2dp(self, values):
        """p50 equals the median (50th percentile) rounded to 2 decimal places."""
        result = _compute_latency_stats(values)
        assert result is not None

        sorted_values = sorted(values)
        expected_p50 = round(_reference_percentile(sorted_values, 50), 2)
        assert result['p50'] == expected_p50

    @settings(max_examples=100)
    @given(values=lists(integers(min_value=0, max_value=100000), min_size=1))
    def test_p95_equals_95th_percentile_rounded_to_2dp(self, values):
        """p95 equals the 95th percentile rounded to 2 decimal places."""
        result = _compute_latency_stats(values)
        assert result is not None

        sorted_values = sorted(values)
        expected_p95 = round(_reference_percentile(sorted_values, 95), 2)
        assert result['p95'] == expected_p95


# Strategy to generate a per-query entry with either valid token counts or None
def per_query_entry_strategy():
    """Generate a per-query dict where input_tokens, output_tokens, and retrieval_context_tokens may be None."""
    return st.fixed_dictionaries({
        'retrieval_ms': st.just(100),
        'response_ms': st.just(200),
        'total_ms': st.just(300),
        'input_tokens': st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),
        'output_tokens': st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),
        'retrieval_context_tokens': st.one_of(st.none(), st.integers(min_value=0, max_value=1_000_000)),
    })


class TestNullTokenExclusionProperty:
    """
    Null-token exclusion in aggregation

    For any list of per-query results where some entries have null token counts,
    the aggregate token sums SHALL include only non-null entries, and the
    num_missing_token_metadata count SHALL equal the number of entries with null tokens.
    """

    @settings(max_examples=100)
    @given(
        per_query_data=st.lists(
            per_query_entry_strategy(),
            min_size=1,
            max_size=50,
        )
    )
    def test_null_token_exclusion(self, per_query_data):
        """
        Generate lists with random null positions, verify sums include only non-null
        entries and num_missing_token_metadata equals count of null entries.
        """
        result = compute_metrics_summary(
            per_query_data=per_query_data,
            retriever_id='traversal',
            dataset='test_dataset',
            model_id='us.anthropic.claude-haiku-4-5-20251001-v1:0',
            num_empty=0,
        )

        # Compute expected values manually
        expected_input_sum = 0
        expected_output_sum = 0
        expected_missing_count = 0

        for entry in per_query_data:
            input_tokens = entry.get('input_tokens')
            output_tokens = entry.get('output_tokens')

            if input_tokens is None or output_tokens is None:
                expected_missing_count += 1
            else:
                expected_input_sum += input_tokens
                expected_output_sum += output_tokens

        # Verify token sums include only non-null entries
        assert result['tokens']['total_input_tokens'] == expected_input_sum, (
            f"Expected total_input_tokens={expected_input_sum}, "
            f"got {result['tokens']['total_input_tokens']}"
        )
        assert result['tokens']['total_output_tokens'] == expected_output_sum, (
            f"Expected total_output_tokens={expected_output_sum}, "
            f"got {result['tokens']['total_output_tokens']}"
        )

        # Verify num_missing_token_metadata equals count of entries with null tokens
        assert result['num_missing_token_metadata'] == expected_missing_count, (
            f"Expected num_missing_token_metadata={expected_missing_count}, "
            f"got {result['num_missing_token_metadata']}"
        )


class TestRetrievalContextTokenAggregation:
    """
    Retrieval context token aggregation

    For any list of per-query results, the aggregate retrieval context tokens
    SHALL include only non-null entries, and avg_retrieval_context_tokens_per_query
    SHALL equal the arithmetic mean of non-null context token values.
    """

    @settings(max_examples=100)
    @given(
        per_query_data=st.lists(
            per_query_entry_strategy(),
            min_size=1,
            max_size=50,
        )
    )
    def test_retrieval_context_token_aggregation(self, per_query_data):
        """
        Verify total_retrieval_context_tokens sums only non-null entries and
        avg_retrieval_context_tokens_per_query is computed correctly.
        """
        result = compute_metrics_summary(
            per_query_data=per_query_data,
            retriever_id='traversal',
            dataset='test_dataset',
            model_id='us.anthropic.claude-haiku-4-5-20251001-v1:0',
            num_empty=0,
        )

        # Compute expected values manually
        expected_context_sum = 0
        expected_missing_context_count = 0

        for entry in per_query_data:
            context_tokens = entry.get('retrieval_context_tokens')
            if context_tokens is None:
                expected_missing_context_count += 1
            else:
                expected_context_sum += context_tokens

        num_with_context = len(per_query_data) - expected_missing_context_count

        assert result['tokens']['total_retrieval_context_tokens'] == expected_context_sum
        assert result['num_missing_context_token_metadata'] == expected_missing_context_count

        if num_with_context > 0:
            expected_avg = round(expected_context_sum / num_with_context, 2)
            assert result['tokens']['avg_retrieval_context_tokens_per_query'] == expected_avg
        else:
            assert result['tokens']['avg_retrieval_context_tokens_per_query'] is None


class TestAggregateCostComputationProperty:
    """
    Aggregate cost computation

    For any list of per-query token counts (excluding null entries) and a known
    model pricing entry, the estimated cost SHALL equal
    (total_input_tokens / 1,000,000 * input_price_per_million) +
    (total_output_tokens / 1,000,000 * output_price_per_million).
    """

    @settings(max_examples=100)
    @given(
        per_query_tokens=st.lists(
            fixed_dictionaries({
                'input_tokens': integers(min_value=0, max_value=10_000_000),
                'output_tokens': integers(min_value=0, max_value=10_000_000),
            }),
            min_size=1,
            max_size=50,
        ),
        input_price=floats(min_value=0.01, max_value=100.0, allow_nan=False, allow_infinity=False),
        output_price=floats(min_value=0.01, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_cost_equals_formula(self, per_query_tokens, input_price, output_price):
        """
        Generate random token counts and pricing entries, verify
        cost = (total_input / 1M * input_price) + (total_output / 1M * output_price)
        """
        test_model_id = '__test_model_for_property_7__'

        # Build per_query_data with the generated token counts
        per_query_data = [
            {'input_tokens': entry['input_tokens'], 'output_tokens': entry['output_tokens']}
            for entry in per_query_tokens
        ]

        # Compute expected totals
        total_input = sum(entry['input_tokens'] for entry in per_query_tokens)
        total_output = sum(entry['output_tokens'] for entry in per_query_tokens)

        # Compute expected cost using the same formula as the implementation
        expected_cost = round(
            (total_input / 1_000_000 * input_price) + (total_output / 1_000_000 * output_price),
            2,
        )

        # Patch BEDROCK_PRICING to include our test model with generated pricing
        patched_pricing = dict(BEDROCK_PRICING)
        patched_pricing[test_model_id] = {
            'input_per_million': input_price,
            'output_per_million': output_price,
        }

        with patch(
            'graphrag_toolkit_tests.benchmark_utils.metrics_summary.BEDROCK_PRICING',
            patched_pricing,
        ):
            result = compute_metrics_summary(
                per_query_data=per_query_data,
                retriever_id='test_retriever',
                dataset='test_dataset',
                model_id=test_model_id,
                num_empty=0,
            )

        assert result['estimated_cost_usd'] == expected_cost, (
            f"Cost mismatch: expected {expected_cost}, got {result['estimated_cost_usd']}. "
            f"total_input={total_input}, total_output={total_output}, "
            f"input_price={input_price}, output_price={output_price}"
        )

    @settings(max_examples=100)
    @given(
        per_query_tokens=st.lists(
            fixed_dictionaries({
                'input_tokens': integers(min_value=0, max_value=10_000_000),
                'output_tokens': integers(min_value=0, max_value=10_000_000),
            }),
            min_size=0,
            max_size=50,
        ),
    )
    def test_unknown_model_produces_null_cost(self, per_query_tokens):
        """
        Verify that unknown model IDs produce null cost regardless of token counts.
        """
        unknown_model_id = '__unknown_model_not_in_pricing__'

        per_query_data = [
            {'input_tokens': entry['input_tokens'], 'output_tokens': entry['output_tokens']}
            for entry in per_query_tokens
        ]

        result = compute_metrics_summary(
            per_query_data=per_query_data,
            retriever_id='test_retriever',
            dataset='test_dataset',
            model_id=unknown_model_id,
            num_empty=0,
        )

        assert result['estimated_cost_usd'] is None, (
            f"Expected null cost for unknown model, got {result['estimated_cost_usd']}"
        )
