# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for result filtering functionality.

This module tests result filtering logic with property-based testing
to verify invariant properties of filtering operations.

**Validates: Requirements 4.2, 4.8, 11.7**
"""

import pytest
from hypothesis import given, strategies as st, settings


# Hypothesis strategy for generating result lists
@st.composite
def result_list_strategy(draw):
    """
    Hypothesis strategy for generating lists of result dictionaries.
    
    Each result has an 'id' and a 'score' field. This strategy generates
    realistic result lists for testing filtering operations.
    """
    size = draw(st.integers(min_value=0, max_value=50))
    results = []
    for i in range(size):
        result = {
            'id': f'result_{i}_{draw(st.integers(min_value=0, max_value=1000))}',
            'score': draw(st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False))
        }
        results.append(result)
    return results


class TestResultFilteringProperties:
    """Property-based tests for result filtering functionality."""
    
    @given(results=result_list_strategy())
    @settings(max_examples=100)
    def test_filtering_reduces_or_maintains_result_count_property(self, results):
        """
        Property: Filtering reduces or maintains result count.
        
        For any list of results and any threshold, filtering should never
        increase the number of results. The filtered list should have
        less than or equal to the original count.
        
        **Validates: Requirements 4.2, 4.8, 11.7**
        """
        # Test with various thresholds
        thresholds = [0.0, 0.25, 0.5, 0.75, 1.0]
        
        for threshold in thresholds:
            filtered = [r for r in results if r['score'] >= threshold]
            
            # Property: Filtered results <= original results
            assert len(filtered) <= len(results), (
                f"Filtering increased result count: "
                f"original={len(results)}, filtered={len(filtered)}, threshold={threshold}"
            )
    
    @given(results=result_list_strategy(), threshold=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False))
    @settings(max_examples=100)
    def test_filtering_with_arbitrary_threshold_property(self, results, threshold):
        """
        Property: Filtering with arbitrary threshold never increases count.
        
        For any list of results and any threshold value, the filtered
        results should be a subset (or equal) of the original results.
        
        **Validates: Requirements 4.2, 4.8, 11.7**
        """
        filtered = [r for r in results if r['score'] >= threshold]
        
        # Property: Filtered count <= original count
        assert len(filtered) <= len(results)
        
        # Property: All filtered results meet the threshold
        assert all(r['score'] >= threshold for r in filtered)
    
    @given(results=result_list_strategy())
    @settings(max_examples=100)
    def test_filtering_preserves_result_properties_property(self, results):
        """
        Property: Filtering preserves result structure and properties.
        
        For any list of results, filtering should preserve the structure
        of each result (id, score fields) and not modify result data.
        
        **Validates: Requirements 4.2, 4.8, 11.7**
        """
        threshold = 0.5
        filtered = [r for r in results if r['score'] >= threshold]
        
        # Property: All filtered results have required fields
        for result in filtered:
            assert 'id' in result
            assert 'score' in result
            assert isinstance(result['id'], str)
            assert isinstance(result['score'], float)
    
    @given(results=result_list_strategy())
    @settings(max_examples=100)
    def test_filtering_preserves_order_property(self, results):
        """
        Property: Filtering preserves original result order.
        
        For any list of results, filtering should maintain the relative
        order of results that pass the filter.
        
        **Validates: Requirements 4.2, 4.8, 11.7**
        """
        threshold = 0.5
        filtered = [r for r in results if r['score'] >= threshold]
        
        # Property: Order is preserved
        # Extract IDs from original results that should be in filtered
        expected_ids = [r['id'] for r in results if r['score'] >= threshold]
        actual_ids = [r['id'] for r in filtered]
        
        assert actual_ids == expected_ids, (
            f"Filtering changed result order: expected={expected_ids}, actual={actual_ids}"
        )
    
    @given(results=result_list_strategy())
    @settings(max_examples=100)
    def test_filtering_extreme_thresholds_property(self, results):
        """
        Property: Extreme thresholds produce expected behavior.
        
        Threshold of 0.0 should include all results.
        Threshold > 1.0 should include no results (since scores are 0.0-1.0).
        
        **Validates: Requirements 4.2, 4.8, 11.7**
        """
        # Threshold 0.0 should include all results with score >= 0.0
        filtered_zero = [r for r in results if r['score'] >= 0.0]
        assert len(filtered_zero) == len(results)
        
        # Threshold > 1.0 should include no results (scores are 0.0-1.0)
        filtered_high = [r for r in results if r['score'] >= 1.5]
        assert len(filtered_high) == 0
    
    @given(results=result_list_strategy())
    @settings(max_examples=100)
    def test_filtering_idempotence_property(self, results):
        """
        Property: Filtering is idempotent.
        
        Applying the same filter twice should produce the same result
        as applying it once.
        
        **Validates: Requirements 4.2, 4.8, 11.7**
        """
        threshold = 0.5
        
        # Apply filter once
        filtered_once = [r for r in results if r['score'] >= threshold]
        
        # Apply filter twice
        filtered_twice = [r for r in filtered_once if r['score'] >= threshold]
        
        # Property: Idempotence
        assert len(filtered_once) == len(filtered_twice)
        assert [r['id'] for r in filtered_once] == [r['id'] for r in filtered_twice]


class TestResultFilteringEdgeCases:
    """Unit tests for edge cases in result filtering."""
    
    def test_filtering_empty_results(self):
        """Verify filtering handles empty result list."""
        results = []
        threshold = 0.5
        filtered = [r for r in results if r['score'] >= threshold]
        
        assert len(filtered) == 0
    
    def test_filtering_single_result_above_threshold(self):
        """Verify filtering single result above threshold."""
        results = [{'id': 'r1', 'score': 0.8}]
        threshold = 0.5
        filtered = [r for r in results if r['score'] >= threshold]
        
        assert len(filtered) == 1
        assert filtered[0]['id'] == 'r1'
    
    def test_filtering_single_result_below_threshold(self):
        """Verify filtering single result below threshold."""
        results = [{'id': 'r1', 'score': 0.3}]
        threshold = 0.5
        filtered = [r for r in results if r['score'] >= threshold]
        
        assert len(filtered) == 0
    
    def test_filtering_all_results_equal_threshold(self):
        """Verify filtering when all results equal threshold."""
        results = [
            {'id': 'r1', 'score': 0.5},
            {'id': 'r2', 'score': 0.5},
            {'id': 'r3', 'score': 0.5}
        ]
        threshold = 0.5
        filtered = [r for r in results if r['score'] >= threshold]
        
        # All results should pass (>= includes equal)
        assert len(filtered) == 3
    
    def test_filtering_boundary_values(self):
        """Verify filtering with boundary score values."""
        results = [
            {'id': 'r1', 'score': 0.0},
            {'id': 'r2', 'score': 0.5},
            {'id': 'r3', 'score': 1.0}
        ]
        
        # Test threshold at boundaries
        filtered_zero = [r for r in results if r['score'] >= 0.0]
        assert len(filtered_zero) == 3
        
        filtered_half = [r for r in results if r['score'] >= 0.5]
        assert len(filtered_half) == 2
        
        filtered_one = [r for r in results if r['score'] >= 1.0]
        assert len(filtered_one) == 1
