# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for arg_utils.py functions.

This module tests argument utility functions including coalesce.
"""

import pytest
from hypothesis import given, strategies as st, settings

from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce


class TestCoalesce:
    """Tests for coalesce function."""
    
    def test_coalesce_returns_first_non_none(self):
        """Verify coalesce returns first non-None value."""
        assert coalesce(None, None, 3) == 3
        assert coalesce(None, 2, 3) == 2
        assert coalesce(1, 2, 3) == 1
    
    def test_coalesce_with_boolean_values(self):
        """Verify coalesce handles boolean values correctly."""
        assert coalesce(None, False, True) == False
        assert coalesce(None, True, False) == True
        assert coalesce(False, True) == False
        assert coalesce(True, False) == True
    
    def test_coalesce_all_none_returns_none(self):
        """Verify coalesce returns None when all values are None."""
        assert coalesce(None, None, None) is None
        assert coalesce(None) is None
    
    def test_coalesce_single_value(self):
        """Verify coalesce with single value."""
        assert coalesce(42) == 42
        assert coalesce(None) is None
        assert coalesce("test") == "test"
    
    def test_coalesce_with_zero(self):
        """Verify coalesce treats zero as valid value."""
        assert coalesce(None, 0, 1) == 0
        assert coalesce(0, None, 1) == 0
    
    def test_coalesce_with_empty_string(self):
        """Verify coalesce treats empty string as valid value."""
        assert coalesce(None, "", "default") == ""
        assert coalesce("", None, "default") == ""
    
    def test_coalesce_with_empty_collections(self):
        """Verify coalesce treats empty collections as valid values."""
        assert coalesce(None, [], [1, 2]) == []
        assert coalesce(None, {}, {"key": "value"}) == {}
        assert coalesce(None, (), (1, 2)) == ()
    
    def test_coalesce_with_mixed_types(self):
        """Verify coalesce works with mixed types."""
        assert coalesce(None, "string", 42) == "string"
        assert coalesce(None, 42, "string") == 42
        assert coalesce(None, [1, 2], {"key": "value"}) == [1, 2]
    
    def test_coalesce_no_arguments(self):
        """Verify coalesce with no arguments returns None."""
        assert coalesce() is None
    
    def test_coalesce_with_objects(self):
        """Verify coalesce works with custom objects."""
        obj1 = object()
        obj2 = object()
        assert coalesce(None, obj1, obj2) is obj1
        assert coalesce(obj1, None, obj2) is obj1


# Property-based tests

@given(
    values=st.lists(
        st.one_of(st.none(), st.integers(), st.text(), st.booleans()),
        min_size=1,
        max_size=10
    )
)
@settings(max_examples=100)
def test_coalesce_returns_first_non_none_property(values):
    """
    Property: coalesce returns first non-None value.
    
    For any list of values, coalesce should return the first non-None value,
    or None if all values are None.
    
    Validates: Requirement 11.1 (argument utility functions)
    """
    result = coalesce(*values)
    
    # Find expected result (first non-None value)
    expected = None
    for value in values:
        if value is not None:
            expected = value
            break
    
    assert result == expected


@given(value=st.one_of(st.integers(), st.text(), st.booleans()))
@settings(max_examples=100)
def test_coalesce_single_non_none_value_property(value):
    """
    Property: coalesce with single non-None value returns that value.
    
    For any non-None value, coalesce should return that value.
    
    Validates: Requirement 11.1 (argument utility functions)
    """
    assert coalesce(value) == value
    assert coalesce(None, value) == value
    assert coalesce(None, None, value) == value


@given(count=st.integers(min_value=1, max_value=20))
@settings(max_examples=50)
def test_coalesce_all_none_property(count):
    """
    Property: coalesce with all None values returns None.
    
    For any number of None values, coalesce should return None.
    
    Validates: Requirement 11.1 (argument utility functions)
    """
    result = coalesce(*([None] * count))
    assert result is None