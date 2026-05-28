# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests for the comparison_report module.
"""

import json
import os
import tempfile

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from graphrag_toolkit_tests.benchmark_utils.comparison_report import (
    _compute_cost_per_query,
    _compute_cost_efficiency,
    _compute_latency_efficiency,
    _rank_by_efficiency,
    _compute_multi_hop_breakdown,
)


def evaluated_result_strategy():
    """Generate a single evaluated result with hop classification and correctness grade."""
    return st.fixed_dictionaries({
        'hop_classification': st.sampled_from(['single-hop', 'multi-hop', 'unknown']),
        'llmCorrectnessGrade': st.sampled_from(['correct', 'incorrect']),
    })


class TestHopSpecificScorePartitioning:
    """
    Hop-specific score partitioning

    For any list of evaluated results with hop classifications, the single-hop
    correctness score SHALL be computed exclusively from results classified as
    'single-hop', the multi-hop correctness score exclusively from 'multi-hop'
    results, and results classified as 'unknown' SHALL be excluded from both.
    """

    @settings(max_examples=100)
    @given(
        results=st.lists(
            evaluated_result_strategy(),
            min_size=1,
            max_size=50,
        )
    )
    def test_multi_hop_score_computed_only_from_multi_hop_results(self, results):
        """
        Verify multi-hop correctness is computed exclusively from 'multi-hop' results.
        """
        # Filter to only multi-hop results
        multi_hop_results = [r for r in results if r['hop_classification'] == 'multi-hop']
        assume(len(multi_hop_results) > 0)

        # Compute expected multi-hop correctness
        multi_hop_correct = sum(
            1 for r in multi_hop_results if r['llmCorrectnessGrade'] == 'correct'
        )
        expected_multi_hop_correctness = round(multi_hop_correct / len(multi_hop_results), 4)

        # Set up temp directory with responses.jsonl and correctness_evals.json
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            # Write responses.jsonl (each line has hop_classification)
            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            # Write correctness_evals.json (list of eval dicts)
            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            # Call _compute_multi_hop_breakdown
            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            # Verify multi-hop correctness
            assert breakdown is not None, "Expected breakdown to be non-None with multi-hop data"
            assert retriever in breakdown['retrievers'], (
                f"Expected retriever '{retriever}' in breakdown"
            )
            actual_correctness = breakdown['retrievers'][retriever]['multi_hop_correctness']
            assert actual_correctness == expected_multi_hop_correctness, (
                f"Expected multi_hop_correctness={expected_multi_hop_correctness}, "
                f"got {actual_correctness}"
            )

    @settings(max_examples=100)
    @given(
        results=st.lists(
            evaluated_result_strategy(),
            min_size=1,
            max_size=50,
        )
    )
    def test_unknown_results_excluded_from_multi_hop_score(self, results):
        """
        Verify 'unknown' classified results are excluded from multi-hop correctness.
        """
        # Ensure we have at least one multi-hop and one unknown result
        multi_hop_results = [r for r in results if r['hop_classification'] == 'multi-hop']
        unknown_results = [r for r in results if r['hop_classification'] == 'unknown']
        assume(len(multi_hop_results) > 0)
        assume(len(unknown_results) > 0)

        # Expected: only multi-hop results contribute to multi_hop_correctness
        multi_hop_correct = sum(
            1 for r in multi_hop_results if r['llmCorrectnessGrade'] == 'correct'
        )
        expected_correctness = round(multi_hop_correct / len(multi_hop_results), 4)

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None
            actual_correctness = breakdown['retrievers'][retriever]['multi_hop_correctness']
            assert actual_correctness == expected_correctness, (
                f"Unknown results should be excluded. "
                f"Expected {expected_correctness}, got {actual_correctness}. "
                f"multi_hop_count={len(multi_hop_results)}, unknown_count={len(unknown_results)}"
            )

    @settings(max_examples=100)
    @given(
        results=st.lists(
            evaluated_result_strategy(),
            min_size=1,
            max_size=50,
        )
    )
    def test_single_hop_results_excluded_from_multi_hop_score(self, results):
        """
        Verify 'single-hop' classified results are excluded from multi-hop correctness.
        """
        multi_hop_results = [r for r in results if r['hop_classification'] == 'multi-hop']
        single_hop_results = [r for r in results if r['hop_classification'] == 'single-hop']
        assume(len(multi_hop_results) > 0)
        assume(len(single_hop_results) > 0)

        # Expected: only multi-hop results contribute
        multi_hop_correct = sum(
            1 for r in multi_hop_results if r['llmCorrectnessGrade'] == 'correct'
        )
        expected_correctness = round(multi_hop_correct / len(multi_hop_results), 4)

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None
            actual_correctness = breakdown['retrievers'][retriever]['multi_hop_correctness']
            assert actual_correctness == expected_correctness, (
                f"Single-hop results should be excluded from multi-hop score. "
                f"Expected {expected_correctness}, got {actual_correctness}. "
                f"multi_hop_count={len(multi_hop_results)}, single_hop_count={len(single_hop_results)}"
            )

    @settings(max_examples=100)
    @given(
        results=st.lists(
            st.fixed_dictionaries({
                'hop_classification': st.sampled_from(['single-hop', 'unknown']),
                'llmCorrectnessGrade': st.sampled_from(['correct', 'incorrect']),
            }),
            min_size=1,
            max_size=50,
        )
    )
    def test_no_multi_hop_results_returns_none(self, results):
        """
        Verify that when no multi-hop results exist, breakdown returns None (no data).
        """
        # Ensure no multi-hop results
        assert all(r['hop_classification'] != 'multi-hop' for r in results)

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            # No multi-hop data means breakdown should be None
            assert breakdown is None, (
                f"Expected None when no multi-hop results exist, got {breakdown}"
            )

    @settings(max_examples=100)
    @given(
        results=st.lists(
            evaluated_result_strategy(),
            min_size=1,
            max_size=50,
        )
    )
    def test_multi_hop_count_reflects_only_multi_hop_results(self, results):
        """
        Verify multi_hop_count equals the number of 'multi-hop' classified results.
        """
        multi_hop_results = [r for r in results if r['hop_classification'] == 'multi-hop']
        assume(len(multi_hop_results) > 0)

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None
            assert breakdown['multi_hop_count'] == len(multi_hop_results), (
                f"Expected multi_hop_count={len(multi_hop_results)}, "
                f"got {breakdown['multi_hop_count']}"
            )


def retriever_with_efficiency_strategy(key='cost_efficiency'):
    """Generate a retriever dict with a random efficiency value (possibly None)."""
    return st.fixed_dictionaries({
        'retriever': st.text(
            alphabet=st.characters(whitelist_categories=('L', 'N'), whitelist_characters='_-'),
            min_size=1,
            max_size=20,
        ),
        key: st.one_of(st.none(), st.floats(min_value=0.001, max_value=1000.0, allow_nan=False)),
    })


class TestEfficiencyRankingSortOrder:
    """
    Efficiency ranking sort order

    For any list of retrievers with computed efficiency scores, the ranking SHALL
    be sorted in descending order of efficiency value, with null-valued entries
    placed last.
    """

    @settings(max_examples=100)
    @given(
        retrievers=st.lists(
            retriever_with_efficiency_strategy('cost_efficiency'),
            min_size=1,
            max_size=20,
        )
    )
    def test_non_null_values_sorted_descending(self, retrievers):
        """
        Verify non-null efficiency values appear in descending order in the ranking.
        """
        # Ensure unique retriever names
        seen = set()
        unique_retrievers = []
        for r in retrievers:
            if r['retriever'] not in seen:
                seen.add(r['retriever'])
                unique_retrievers.append(r)
        assume(len(unique_retrievers) >= 1)

        ranked = _rank_by_efficiency(unique_retrievers, 'cost_efficiency')

        # Extract the efficiency values in ranked order
        retriever_to_efficiency = {r['retriever']: r['cost_efficiency'] for r in unique_retrievers}
        non_null_ranked = [
            retriever_to_efficiency[name]
            for name in ranked
            if retriever_to_efficiency[name] is not None
        ]

        # Verify descending order for non-null values
        for i in range(len(non_null_ranked) - 1):
            assert non_null_ranked[i] >= non_null_ranked[i + 1], (
                f"Non-null values not in descending order: "
                f"{non_null_ranked[i]} < {non_null_ranked[i + 1]} at positions {i}, {i + 1}. "
                f"Full ranking: {ranked}"
            )

    @settings(max_examples=100)
    @given(
        retrievers=st.lists(
            retriever_with_efficiency_strategy('cost_efficiency'),
            min_size=1,
            max_size=20,
        )
    )
    def test_null_values_placed_last(self, retrievers):
        """
        Verify null-valued entries are placed last in the ranking.
        """
        # Ensure unique retriever names
        seen = set()
        unique_retrievers = []
        for r in retrievers:
            if r['retriever'] not in seen:
                seen.add(r['retriever'])
                unique_retrievers.append(r)
        assume(len(unique_retrievers) >= 1)

        # Ensure we have at least one null and one non-null for a meaningful test
        has_null = any(r['cost_efficiency'] is None for r in unique_retrievers)
        has_non_null = any(r['cost_efficiency'] is not None for r in unique_retrievers)
        assume(has_null and has_non_null)

        ranked = _rank_by_efficiency(unique_retrievers, 'cost_efficiency')

        retriever_to_efficiency = {r['retriever']: r['cost_efficiency'] for r in unique_retrievers}

        # Find the position of the last non-null and first null in the ranking
        first_null_idx = None
        last_non_null_idx = None
        for i, name in enumerate(ranked):
            if retriever_to_efficiency[name] is None:
                if first_null_idx is None:
                    first_null_idx = i
            else:
                last_non_null_idx = i

        # All non-null entries must come before all null entries
        assert last_non_null_idx < first_null_idx, (
            f"Null values not placed last: last non-null at index {last_non_null_idx}, "
            f"first null at index {first_null_idx}. Ranking: {ranked}"
        )

    @settings(max_examples=100)
    @given(
        retrievers=st.lists(
            retriever_with_efficiency_strategy('cost_efficiency'),
            min_size=1,
            max_size=20,
        )
    )
    def test_all_retriever_names_appear_exactly_once(self, retrievers):
        """
        Verify all retriever names appear exactly once in the ranking.
        """
        # Ensure unique retriever names
        seen = set()
        unique_retrievers = []
        for r in retrievers:
            if r['retriever'] not in seen:
                seen.add(r['retriever'])
                unique_retrievers.append(r)
        assume(len(unique_retrievers) >= 1)

        ranked = _rank_by_efficiency(unique_retrievers, 'cost_efficiency')

        # Verify all names appear exactly once
        expected_names = {r['retriever'] for r in unique_retrievers}
        ranked_set = set(ranked)

        assert ranked_set == expected_names, (
            f"Ranking names mismatch. Expected: {expected_names}, Got: {ranked_set}"
        )
        assert len(ranked) == len(unique_retrievers), (
            f"Ranking length mismatch. Expected: {len(unique_retrievers)}, Got: {len(ranked)}"
        )

    @settings(max_examples=100)
    @given(
        retrievers=st.lists(
            retriever_with_efficiency_strategy('latency_efficiency'),
            min_size=1,
            max_size=20,
        )
    )
    def test_latency_efficiency_ranking_descending_with_nulls_last(self, retrievers):
        """
        Verify ranking works correctly for latency_efficiency key as well.
        """
        # Ensure unique retriever names
        seen = set()
        unique_retrievers = []
        for r in retrievers:
            if r['retriever'] not in seen:
                seen.add(r['retriever'])
                unique_retrievers.append(r)
        assume(len(unique_retrievers) >= 2)

        ranked = _rank_by_efficiency(unique_retrievers, 'latency_efficiency')

        retriever_to_efficiency = {r['retriever']: r['latency_efficiency'] for r in unique_retrievers}

        # Verify: non-null values in descending order, then nulls at the end
        non_null_values = [
            retriever_to_efficiency[name]
            for name in ranked
            if retriever_to_efficiency[name] is not None
        ]
        null_names = [
            name for name in ranked if retriever_to_efficiency[name] is None
        ]

        # Non-null values should be descending
        for i in range(len(non_null_values) - 1):
            assert non_null_values[i] >= non_null_values[i + 1], (
                f"Latency efficiency not in descending order at positions {i}, {i + 1}: "
                f"{non_null_values[i]} < {non_null_values[i + 1]}"
            )

        # Null entries should be at the end
        if null_names and non_null_values:
            last_non_null_idx = len(ranked) - len(null_names) - 1
            first_null_idx = len(ranked) - len(null_names)
            assert last_non_null_idx < first_null_idx, (
                f"Null entries not at end of ranking"
            )


class TestEfficiencyCalculationWithNullHandling:
    """
    Efficiency calculation with null handling

    For any retriever with a correctness score and cost/latency values,
    cost-efficiency SHALL equal correctness / cost_per_query and
    latency-efficiency SHALL equal correctness / (avg_total_ms / 1000).
    If cost_per_query is zero or null, or correctness is zero or negative,
    cost-efficiency SHALL be null. If avg_total_ms is zero or null,
    latency-efficiency SHALL be null.
    """

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=0.001, max_value=1.0, allow_nan=False, allow_infinity=False),
        cost_per_query=st.floats(min_value=0.0001, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_cost_efficiency_formula_positive_inputs(self, correctness, cost_per_query):
        """
        Verify cost_efficiency == correctness / cost_per_query for positive inputs.
        """
        result = _compute_cost_efficiency(correctness, cost_per_query)
        expected = correctness / cost_per_query
        assert result is not None, "Expected non-None result for positive inputs"
        assert abs(result - expected) < 1e-9, (
            f"Expected cost_efficiency={expected}, got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=0.001, max_value=1.0, allow_nan=False, allow_infinity=False),
        avg_total_ms=st.floats(min_value=0.001, max_value=100000.0, allow_nan=False, allow_infinity=False),
    )
    def test_latency_efficiency_formula_positive_inputs(self, correctness, avg_total_ms):
        """
        Verify latency_efficiency == correctness / (avg_total_ms / 1000) for positive inputs.
        """
        result = _compute_latency_efficiency(correctness, avg_total_ms)
        expected = correctness / (avg_total_ms / 1000.0)
        assert result is not None, "Expected non-None result for positive inputs"
        assert abs(result - expected) < 1e-9, (
            f"Expected latency_efficiency={expected}, got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=0.001, max_value=1.0, allow_nan=False, allow_infinity=False),
    )
    def test_cost_efficiency_null_when_cost_per_query_is_none(self, correctness):
        """
        Verify cost_efficiency is None when cost_per_query is None.
        """
        result = _compute_cost_efficiency(correctness, None)
        assert result is None, (
            f"Expected None when cost_per_query is None, got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=0.001, max_value=1.0, allow_nan=False, allow_infinity=False),
    )
    def test_cost_efficiency_null_when_cost_per_query_is_zero(self, correctness):
        """
        Verify cost_efficiency is None when cost_per_query is zero.
        """
        result = _compute_cost_efficiency(correctness, 0.0)
        assert result is None, (
            f"Expected None when cost_per_query is zero, got {result}"
        )

    @settings(max_examples=100)
    @given(
        cost_per_query=st.floats(min_value=0.0001, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_cost_efficiency_null_when_correctness_is_zero(self, cost_per_query):
        """
        Verify cost_efficiency is None when correctness is zero.
        """
        result = _compute_cost_efficiency(0.0, cost_per_query)
        assert result is None, (
            f"Expected None when correctness is zero, got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=-100.0, max_value=-0.001, allow_nan=False, allow_infinity=False),
        cost_per_query=st.floats(min_value=0.0001, max_value=100.0, allow_nan=False, allow_infinity=False),
    )
    def test_cost_efficiency_null_when_correctness_is_negative(self, correctness, cost_per_query):
        """
        Verify cost_efficiency is None when correctness is negative.
        """
        result = _compute_cost_efficiency(correctness, cost_per_query)
        assert result is None, (
            f"Expected None when correctness is negative ({correctness}), got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=0.001, max_value=1.0, allow_nan=False, allow_infinity=False),
    )
    def test_latency_efficiency_null_when_avg_total_ms_is_none(self, correctness):
        """
        Verify latency_efficiency is None when avg_total_ms is None.
        """
        result = _compute_latency_efficiency(correctness, None)
        assert result is None, (
            f"Expected None when avg_total_ms is None, got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=0.001, max_value=1.0, allow_nan=False, allow_infinity=False),
    )
    def test_latency_efficiency_null_when_avg_total_ms_is_zero(self, correctness):
        """
        Verify latency_efficiency is None when avg_total_ms is zero.
        """
        result = _compute_latency_efficiency(correctness, 0.0)
        assert result is None, (
            f"Expected None when avg_total_ms is zero, got {result}"
        )

    @settings(max_examples=100)
    @given(
        correctness=st.floats(min_value=-100.0, max_value=-0.001, allow_nan=False, allow_infinity=False),
        avg_total_ms=st.floats(min_value=0.001, max_value=100000.0, allow_nan=False, allow_infinity=False),
    )
    def test_latency_efficiency_null_when_correctness_is_negative(self, correctness, avg_total_ms):
        """
        Verify latency_efficiency is None when correctness is negative.
        """
        result = _compute_latency_efficiency(correctness, avg_total_ms)
        assert result is None, (
            f"Expected None when correctness is negative ({correctness}), got {result}"
        )

    @settings(max_examples=100)
    @given(
        avg_total_ms=st.floats(min_value=0.001, max_value=100000.0, allow_nan=False, allow_infinity=False),
    )
    def test_latency_efficiency_null_when_correctness_is_zero(self, avg_total_ms):
        """
        Verify latency_efficiency is None when correctness is zero.
        """
        result = _compute_latency_efficiency(0.0, avg_total_ms)
        assert result is None, (
            f"Expected None when correctness is zero, got {result}"
        )


class TestMultiHopWarningThreshold:
    """
    Multi-hop warning threshold

    For any dataset where the count of multi-hop classified questions is less than 10,
    the comparison report SHALL include a warning flag for that dataset. For counts >= 10,
    no warning SHALL be present.
    """

    @settings(max_examples=100)
    @given(
        multi_hop_count=st.integers(min_value=1, max_value=9),
        single_hop_count=st.integers(min_value=0, max_value=20),
    )
    def test_warning_present_when_count_below_threshold(self, multi_hop_count, single_hop_count):
        """
        Verify warning is present when multi-hop count < 10.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            # Build results: multi_hop_count multi-hop + single_hop_count single-hop
            results = []
            for _ in range(multi_hop_count):
                results.append({
                    'hop_classification': 'multi-hop',
                    'llmCorrectnessGrade': 'correct',
                })
            for _ in range(single_hop_count):
                results.append({
                    'hop_classification': 'single-hop',
                    'llmCorrectnessGrade': 'correct',
                })

            # Write responses.jsonl
            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            # Write correctness_evals.json
            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None, "Expected breakdown to be non-None with multi-hop data"
            assert breakdown['warning'] is not None, (
                f"Expected warning to be present when multi_hop_count={multi_hop_count} < 10, "
                f"but got warning=None"
            )
            assert str(multi_hop_count) in breakdown['warning'], (
                f"Expected warning to mention the count {multi_hop_count}, "
                f"got: {breakdown['warning']}"
            )

    @settings(max_examples=100)
    @given(
        multi_hop_count=st.integers(min_value=10, max_value=100),
        single_hop_count=st.integers(min_value=0, max_value=20),
    )
    def test_warning_absent_when_count_at_or_above_threshold(self, multi_hop_count, single_hop_count):
        """
        Verify warning is absent (None) when multi-hop count >= 10.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            # Build results: multi_hop_count multi-hop + single_hop_count single-hop
            results = []
            for _ in range(multi_hop_count):
                results.append({
                    'hop_classification': 'multi-hop',
                    'llmCorrectnessGrade': 'correct',
                })
            for _ in range(single_hop_count):
                results.append({
                    'hop_classification': 'single-hop',
                    'llmCorrectnessGrade': 'correct',
                })

            # Write responses.jsonl
            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            # Write correctness_evals.json
            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None, "Expected breakdown to be non-None with multi-hop data"
            assert breakdown['warning'] is None, (
                f"Expected warning to be None when multi_hop_count={multi_hop_count} >= 10, "
                f"but got: {breakdown['warning']}"
            )

    @settings(max_examples=100)
    @given(
        multi_hop_count=st.integers(min_value=1, max_value=9),
        correctness_grades=st.lists(
            st.sampled_from(['correct', 'incorrect']),
            min_size=1,
            max_size=9,
        ),
    )
    def test_warning_present_regardless_of_correctness_distribution(self, multi_hop_count, correctness_grades):
        """
        Verify warning is based solely on count, not on correctness distribution.
        """
        # Use the smaller of multi_hop_count and len(correctness_grades)
        actual_count = min(multi_hop_count, len(correctness_grades))
        assume(actual_count > 0 and actual_count < 10)

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            results = []
            for i in range(actual_count):
                results.append({
                    'hop_classification': 'multi-hop',
                    'llmCorrectnessGrade': correctness_grades[i],
                })

            # Write responses.jsonl
            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            # Write correctness_evals.json
            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None
            assert breakdown['warning'] is not None, (
                f"Expected warning when multi_hop_count={actual_count} < 10"
            )

    @settings(max_examples=100)
    @given(
        multi_hop_count=st.integers(min_value=10, max_value=50),
        correctness_grades=st.lists(
            st.sampled_from(['correct', 'incorrect']),
            min_size=10,
            max_size=50,
        ),
    )
    def test_no_warning_regardless_of_correctness_distribution(self, multi_hop_count, correctness_grades):
        """
        Verify no warning when count >= 10, regardless of correctness distribution.
        """
        actual_count = min(multi_hop_count, len(correctness_grades))
        assume(actual_count >= 10)

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            results = []
            for i in range(actual_count):
                results.append({
                    'hop_classification': 'multi-hop',
                    'llmCorrectnessGrade': correctness_grades[i],
                })

            # Write responses.jsonl
            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            # Write correctness_evals.json
            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None
            assert breakdown['warning'] is None, (
                f"Expected no warning when multi_hop_count={actual_count} >= 10, "
                f"but got: {breakdown['warning']}"
            )

    @settings(max_examples=100)
    @given(
        multi_hop_count=st.integers(min_value=1, max_value=100),
        single_hop_count=st.integers(min_value=0, max_value=50),
        unknown_count=st.integers(min_value=0, max_value=20),
    )
    def test_warning_threshold_boundary_exact_at_10(self, multi_hop_count, single_hop_count, unknown_count):
        """
        Verify the exact boundary: count=10 means no warning, count=9 means warning.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            dataset = 'test_dataset'
            retriever = 'test_retriever'
            retriever_path = os.path.join(tmpdir, dataset, retriever)
            os.makedirs(retriever_path)

            results = []
            for _ in range(multi_hop_count):
                results.append({
                    'hop_classification': 'multi-hop',
                    'llmCorrectnessGrade': 'correct',
                })
            for _ in range(single_hop_count):
                results.append({
                    'hop_classification': 'single-hop',
                    'llmCorrectnessGrade': 'correct',
                })
            for _ in range(unknown_count):
                results.append({
                    'hop_classification': 'unknown',
                    'llmCorrectnessGrade': 'correct',
                })

            # Write responses.jsonl
            responses_path = os.path.join(retriever_path, 'responses.jsonl')
            with open(responses_path, 'w') as f:
                for r in results:
                    f.write(json.dumps({'hop_classification': r['hop_classification']}) + '\n')

            # Write correctness_evals.json
            evals_path = os.path.join(retriever_path, 'correctness_evals.json')
            with open(evals_path, 'w') as f:
                json.dump(
                    [{'llmCorrectnessGrade': r['llmCorrectnessGrade']} for r in results],
                    f,
                )

            breakdown = _compute_multi_hop_breakdown(dataset, tmpdir, [retriever])

            assert breakdown is not None
            if multi_hop_count < 10:
                assert breakdown['warning'] is not None, (
                    f"Expected warning when multi_hop_count={multi_hop_count} < 10"
                )
            else:
                assert breakdown['warning'] is None, (
                    f"Expected no warning when multi_hop_count={multi_hop_count} >= 10, "
                    f"but got: {breakdown['warning']}"
                )
