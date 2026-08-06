# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests for the agentic_retriever module.
"""

import pytest
from hypothesis import given, settings, assume
from hypothesis.strategies import integers

from benchmarks.utils.agentic_retriever import _validate_max_iterations


class TestAgenticIterationBoundProperty:
    """
    Agentic iteration bound invariant

    For any agentic retrieval execution with a configured max_iterations value (1-10),
    the recorded retrieval_iterations SHALL be a positive integer less than or equal to
    max_iterations, and AGENTIC_MAX_ITERATIONS values outside the range [1, 10] SHALL
    cause a ValueError.
    """

    @settings(max_examples=100)
    @given(max_iterations=integers(min_value=1, max_value=10))
    def test_valid_max_iterations_does_not_raise(self, max_iterations):
        """
        Generate random integers in [1, 10], verify _validate_max_iterations()
        does NOT raise ValueError.
        """
        # Should not raise for valid values in [1, 10]
        _validate_max_iterations(max_iterations)

    @settings(max_examples=100)
    @given(max_iterations=integers())
    def test_invalid_max_iterations_raises_value_error(self, max_iterations):
        """
        Generate random integers outside [1, 10], verify _validate_max_iterations()
        raises ValueError.
        """
        assume(max_iterations < 1 or max_iterations > 10)

        with pytest.raises(ValueError) as exc_info:
            _validate_max_iterations(max_iterations)

        error_message = str(exc_info.value)

        # Verify the error message mentions the invalid value
        assert str(max_iterations) in error_message, (
            f"Error message should contain the invalid value '{max_iterations}', "
            f"but got: {error_message}"
        )

        # Verify the error message mentions the valid range
        assert "[1, 10]" in error_message, (
            f"Error message should mention the valid range [1, 10], "
            f"but got: {error_message}"
        )
