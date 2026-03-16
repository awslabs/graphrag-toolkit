# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Performance tests for hash utility functions.

These tests measure the performance characteristics of the get_hash function
to ensure it meets performance requirements and detect regressions.
"""

import time
from graphrag_toolkit.lexical_graph.indexing.utils.hash_utils import get_hash


class TestGetHashPerformance:
    """Performance tests for get_hash function."""

    def test_get_hash_short_string_performance(self):
        """Test performance of hashing short strings (typical node IDs)."""
        test_string = "node_12345"
        iterations = 10000
        
        start = time.perf_counter()
        for _ in range(iterations):
            result = get_hash(test_string)
        elapsed = time.perf_counter() - start
        
        assert len(result) == 32
        # Should complete 10k hashes in under 0.1 seconds (very generous threshold)
        assert elapsed < 0.1, f"Hashing {iterations} short strings took {elapsed:.4f}s"

    def test_get_hash_medium_string_performance(self):
        """Test performance of hashing medium strings (typical text chunks)."""
        test_string = "This is a medium-length string that represents a typical text chunk. " * 5
        iterations = 1000
        
        start = time.perf_counter()
        for _ in range(iterations):
            result = get_hash(test_string)
        elapsed = time.perf_counter() - start
        
        assert len(result) == 32
        # Should complete 1k hashes in under 0.1 seconds
        assert elapsed < 0.1, f"Hashing {iterations} medium strings took {elapsed:.4f}s"

    def test_get_hash_long_string_performance(self):
        """Test performance of hashing long strings (typical documents)."""
        test_string = "This is a long document with lots of text. " * 100
        iterations = 100
        
        start = time.perf_counter()
        for _ in range(iterations):
            result = get_hash(test_string)
        elapsed = time.perf_counter() - start
        
        assert len(result) == 32
        # Should complete 100 hashes in under 0.1 seconds
        assert elapsed < 0.1, f"Hashing {iterations} long strings took {elapsed:.4f}s"

    def test_get_hash_unicode_performance(self):
        """Test performance of hashing Unicode strings."""
        test_string = "Hello 世界 🌍 Привет مرحبا"
        iterations = 10000
        
        start = time.perf_counter()
        for _ in range(iterations):
            result = get_hash(test_string)
        elapsed = time.perf_counter() - start
        
        assert len(result) == 32
        # Should complete 10k hashes in under 0.1 seconds
        assert elapsed < 0.1, f"Hashing {iterations} unicode strings took {elapsed:.4f}s"

    def test_get_hash_batch_processing_performance(self):
        """Test performance of batch hashing (realistic workload)."""
        test_strings = [f"node_{i}" for i in range(1000)]
        
        start = time.perf_counter()
        results = [get_hash(s) for s in test_strings]
        elapsed = time.perf_counter() - start
        
        assert len(results) == 1000
        assert all(len(r) == 32 for r in results)
        # Should complete 1k hashes in under 0.1 seconds
        assert elapsed < 0.1, f"Batch hashing 1000 strings took {elapsed:.4f}s"
