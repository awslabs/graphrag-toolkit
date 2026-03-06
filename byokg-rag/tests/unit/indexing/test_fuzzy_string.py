"""Tests for FuzzyStringIndex.

This module tests fuzzy string matching functionality including
vocabulary management, exact matching, fuzzy matching, and topk retrieval.
"""

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
