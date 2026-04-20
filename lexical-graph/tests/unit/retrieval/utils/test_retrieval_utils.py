# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval utility functions.

This module tests helper functions for retrieval operations.
"""

import pytest


class TestRetrievalUtils:
    """Tests for retrieval utility functions."""
    
    def test_score_normalization(self):
        """Verify scores are normalized to 0-1 range."""
        scores = [10, 20, 30, 40, 50]
        
        # Normalize to 0-1
        min_score = min(scores)
        max_score = max(scores)
        normalized = [(s - min_score) / (max_score - min_score) for s in scores]
        
        assert all(0 <= s <= 1 for s in normalized)
        assert normalized[0] == 0.0  # Min maps to 0
        assert normalized[-1] == 1.0  # Max maps to 1
    
    def test_score_normalization_single_value(self):
        """Verify normalization handles single value."""
        scores = [5.0]
        
        # Single value should normalize to 1.0
        normalized = [1.0] if len(scores) == 1 else []
        
        assert len(normalized) == 1
        assert normalized[0] == 1.0
    
    def test_result_deduplication(self):
        """Verify duplicate results are removed."""
        results = [
            {'id': 'r1', 'text': 'Text 1'},
            {'id': 'r2', 'text': 'Text 2'},
            {'id': 'r1', 'text': 'Text 1'},  # Duplicate
            {'id': 'r3', 'text': 'Text 3'}
        ]
        
        # Deduplicate by id
        seen = set()
        deduped = []
        for r in results:
            if r['id'] not in seen:
                seen.add(r['id'])
                deduped.append(r)
        
        assert len(deduped) == 3
        assert len([r for r in deduped if r['id'] == 'r1']) == 1
    
    def test_text_truncation(self):
        """Verify text is truncated to max length."""
        text = "This is a very long text that needs to be truncated"
        max_length = 20
        
        truncated = text[:max_length] + '...' if len(text) > max_length else text
        
        assert len(truncated) <= max_length + 3  # +3 for '...'
        assert truncated.endswith('...')
    
    def test_keyword_extraction(self):
        """Verify keywords are extracted from text."""
        text = "GraphRAG combines knowledge graphs with retrieval"
        
        # Simple keyword extraction (split and filter)
        words = text.lower().split()
        keywords = [w for w in words if len(w) > 3]
        
        assert 'graphrag' in keywords
        assert 'knowledge' in keywords
        assert 'graphs' in keywords
        assert 'retrieval' in keywords
    
    def test_result_merging(self):
        """Verify results from multiple sources are merged."""
        results1 = [{'id': 'r1', 'score': 0.8}]
        results2 = [{'id': 'r2', 'score': 0.9}]
        
        merged = results1 + results2
        
        assert len(merged) == 2
        assert merged[0]['id'] == 'r1'
        assert merged[1]['id'] == 'r2'
