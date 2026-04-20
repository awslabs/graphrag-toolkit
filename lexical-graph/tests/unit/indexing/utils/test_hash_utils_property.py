# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Property-based tests for hash utilities using Hypothesis.

These tests verify that hash functions behave correctly across a wide range
of inputs, including edge cases that might not be covered by example-based tests.
"""

import pytest
from hypothesis import given, strategies as st
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash


class TestGetHashProperties:
    """Property-based tests for get_hash function."""

    @given(st.text())
    def test_get_hash_is_deterministic(self, text):
        """Verify get_hash returns the same hash for the same input."""
        hash1 = get_hash(text)
        hash2 = get_hash(text)
        assert hash1 == hash2, "Hash should be deterministic"

    @given(st.text())
    def test_get_hash_returns_hex_string(self, text):
        """Verify get_hash returns a valid hexadecimal string."""
        result = get_hash(text)
        assert isinstance(result, str), "Hash should be a string"
        # Verify it's valid hex by trying to convert it
        try:
            int(result, 16)
        except ValueError:
            pytest.fail(f"Hash '{result}' is not a valid hexadecimal string")

    @given(st.text(), st.text())
    def test_get_hash_different_inputs_likely_different_hashes(self, text1, text2):
        """Verify different inputs produce different hashes (with high probability)."""
        # Skip if inputs are the same
        if text1 == text2:
            return
        
        hash1 = get_hash(text1)
        hash2 = get_hash(text2)
        
        # Different inputs should produce different hashes
        # (collision is theoretically possible but extremely unlikely with SHA-256)
        assert hash1 != hash2, f"Different inputs should produce different hashes: '{text1}' vs '{text2}'"

    @given(st.text(min_size=1))
    def test_get_hash_non_empty_result(self, text):
        """Verify get_hash always returns a non-empty string."""
        result = get_hash(text)
        assert len(result) > 0, "Hash should not be empty"

    @given(st.text())
    def test_get_hash_consistent_length(self, text):
        """Verify get_hash returns hashes of consistent length."""
        result = get_hash(text)
        # MD5 produces 32 hex characters (16 bytes * 2 hex chars per byte)
        assert len(result) == 32, f"Hash should be 32 characters long, got {len(result)}"

    @given(st.text(alphabet=st.characters(blacklist_categories=('Cs',))))
    def test_get_hash_handles_unicode(self, text):
        """Verify get_hash handles Unicode characters correctly."""
        # Should not raise any exceptions
        result = get_hash(text)
        assert isinstance(result, str)
        assert len(result) == 32

    @given(st.text(min_size=0, max_size=0))
    def test_get_hash_handles_empty_string(self, text):
        """Verify get_hash handles empty strings."""
        result = get_hash(text)
        assert isinstance(result, str)
        assert len(result) == 32

    @given(st.text(min_size=1000, max_size=2000))
    def test_get_hash_handles_long_strings(self, text):
        """Verify get_hash handles very long strings."""
        result = get_hash(text)
        assert isinstance(result, str)
        assert len(result) == 32
