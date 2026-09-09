# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for FuzzyStringIndex.

This module tests fuzzy string matching functionality including
vocabulary management, exact matching, fuzzy matching, and topk retrieval.
"""

import subprocess
import sys

import pytest
from graphrag_toolkit.byokg_rag.indexing.fuzzy_string import FuzzyStringIndex


class TestFuzzyStringIndexInitialization:
    """Tests for FuzzyStringIndex initialization."""
    
    def test_initialization_empty_vocab(self):
        """Verify index initializes with empty vocabulary."""
        index = FuzzyStringIndex()
        assert index.vocab == []
    
    def test_reset_clears_vocab(self):
        """Verify reset() clears the vocabulary."""
        index = FuzzyStringIndex()
        index.add(['item1', 'item2'])
        
        index.reset()
        
        assert index.vocab == []


class TestFuzzyStringIndexAdd:
    """Tests for adding vocabulary to the index."""
    
    def test_add_single_item(self):
        """Verify adding a single vocabulary item."""
        index = FuzzyStringIndex()
        index.add(['Amazon'])
        
        assert 'Amazon' in index.vocab
        assert len(index.vocab) == 1
    
    def test_add_multiple_items(self):
        """Verify adding multiple vocabulary items."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        assert len(index.vocab) == 3
        assert all(item in index.vocab for item in ['Amazon', 'Microsoft', 'Google'])
    
    def test_add_duplicate_items(self):
        """Verify duplicate items are deduplicated."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Amazon', 'Microsoft'])
        
        assert len(index.vocab) == 2
        assert index.vocab.count('Amazon') == 1
    
    def test_add_with_ids_not_implemented(self):
        """Verify add_with_ids raises NotImplementedError."""
        index = FuzzyStringIndex()

        with pytest.raises(NotImplementedError):
            index.add_with_ids(['id1'], ['Amazon'])

    def test_add_vocab_is_sorted(self):
        """Vocab must be in a stable (sorted) order, not set-iteration order.

        process.extract breaks equal-score ties by vocab position, so a
        hash-dependent order makes matching nondeterministic across runs.
        """
        index = FuzzyStringIndex()
        index.add(['Microsoft', 'Amazon', 'Google'])
        index.add(['Apple', 'Amazon'])

        assert index.vocab == sorted(index.vocab)
        assert index.vocab == ['Amazon', 'Apple', 'Google', 'Microsoft']


# Query that ties three candidates at score 100 (thefuzz lowercases before
# scoring, so the casings are indistinguishable). With topk < 3 the tie must
# be broken, which is exactly where hash-dependent vocab order used to leak.
_DETERMINISM_SNIPPET = """
from graphrag_toolkit.byokg_rag.indexing.fuzzy_string import FuzzyStringIndex
index = FuzzyStringIndex()
index.add(['Amazon', 'amazon', 'AMAZON', 'Google', 'Microsoft'])
hits = index.query('amazon', topk=2)['hits']
print('|'.join(h['document_id'] for h in hits))
"""


class TestFuzzyStringIndexDeterminism:
    """Regression tests for run-to-run reproducibility (nondeterminism fix)."""

    def _run_with_hashseed(self, seed):
        out = subprocess.run(
            [sys.executable, '-c', _DETERMINISM_SNIPPET],
            capture_output=True, text=True, check=True,
            env={'PYTHONHASHSEED': str(seed)},
        )
        return out.stdout.strip()

    def test_matching_is_deterministic_across_hash_seeds(self):
        """Same vocab + query + topk returns identical hits regardless of seed.

        Runs in subprocesses because PYTHONHASHSEED only takes effect at
        interpreter start; each seed shuffles set() iteration order differently.
        Pre-fix this returned different candidates per seed among the tie group.

        Note: the child imports the indexing package, which pulls faiss. That's
        incidental to what we assert (only thefuzz matters here).
        """
        results = {self._run_with_hashseed(seed) for seed in range(5)}

        assert len(results) == 1, f"nondeterministic hits across hash seeds: {results}"

    def test_scores_independent_of_insertion_order(self):
        """The fix only reorders vocab; per-candidate scores must not change.

        Two indexes built with the same items in different insertion order must
        return identical (document, score) pairs — pins the acceptance criterion
        that scores are unchanged, only ordering/tie-breaks are made stable.
        """
        vocab = ['Amazon', 'Amazonian', 'Amazing', 'Google', 'Meta']
        a = FuzzyStringIndex()
        a.add(vocab)
        b = FuzzyStringIndex()
        b.add(list(reversed(vocab)))

        hits_a = [(h['document_id'], h['match_score']) for h in a.query('Amazon', topk=5)['hits']]
        hits_b = [(h['document_id'], h['match_score']) for h in b.query('Amazon', topk=5)['hits']]

        assert hits_a == hits_b


class TestFuzzyStringIndexQuery:
    """Tests for querying the index."""
    
    def test_query_exact_match(self):
        """Verify exact string matching returns 100% match score."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        result = index.query('Amazon', topk=1)
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Amazon'
        assert result['hits'][0]['match_score'] == 100
    
    def test_query_fuzzy_match(self):
        """Verify fuzzy matching handles typos."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        result = index.query('Amazn', topk=1)  # Missing 'o'
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Amazon'
        assert result['hits'][0]['match_score'] > 80  # High but not perfect
    
    def test_query_topk_limiting(self):
        """Verify topk parameter limits results."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google', 'Apple', 'Meta'])
        
        result = index.query('Tech', topk=3)
        
        assert len(result['hits']) == 3
    
    def test_query_empty_vocab(self):
        """Verify querying empty index returns empty results."""
        index = FuzzyStringIndex()
        
        result = index.query('Amazon', topk=1)
        
        assert len(result['hits']) == 0
    
    def test_query_with_id_selector_not_implemented(self):
        """Verify id_selector parameter raises NotImplementedError."""
        index = FuzzyStringIndex()
        index.add(['Amazon'])
        
        with pytest.raises(NotImplementedError):
            index.query('Amazon', topk=1, id_selector=['id1'])


class TestFuzzyStringIndexMatch:
    """Tests for batch matching functionality."""
    
    def test_match_multiple_inputs(self):
        """Verify batch matching of multiple queries."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        result = index.match(['Amazon', 'Google'], topk=1)
        
        assert len(result['hits']) == 2
        documents = [hit['document'] for hit in result['hits']]
        assert 'Amazon' in documents
        assert 'Google' in documents
    
    def test_match_length_filtering(self):
        """Verify max_len_difference filters short matches."""
        index = FuzzyStringIndex()
        index.add(['Amazon Web Services', 'AWS', 'Amazon'])
        
        # Query for long string, should filter out 'AWS' (too short)
        result = index.match(['Amazon Web Services'], topk=3, max_len_difference=4)
        
        documents = [hit['document'] for hit in result['hits']]
        assert 'AWS' not in documents  # Too short compared to query
    
    def test_match_sorted_by_score(self):
        """Verify results are sorted by match score descending."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Amazonian', 'Amazing'])
        
        result = index.match(['Amazon'], topk=3)
        
        scores = [hit['match_score'] for hit in result['hits']]
        assert scores == sorted(scores, reverse=True)
    
    def test_match_with_id_selector_not_implemented(self):
        """Verify id_selector parameter raises NotImplementedError."""
        index = FuzzyStringIndex()
        index.add(['Amazon'])
        
        with pytest.raises(NotImplementedError):
            index.match(['Amazon'], topk=1, id_selector=['id1'])
