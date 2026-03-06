"""Tests for graph_verbalizer.py module.

This module tests the GVerbalizer, TripletGVerbalizer, and PathVerbalizer classes
including triplet formatting, path formatting, and empty input handling.
"""

import pytest
from graphrag_toolkit.byokg_rag.graph_retrievers.graph_verbalizer import (
    GVerbalizer,
    TripletGVerbalizer,
    PathVerbalizer
)


class TestTripletGVerbalizerInitialization:
    """Tests for TripletGVerbalizer initialization."""
    
    def test_initialization_defaults(self):
        """Verify TripletGVerbalizer initializes with default delimiters."""
        verbalizer = TripletGVerbalizer()
        
        assert verbalizer.delimiter == '->'
        assert verbalizer.merge_delimiter == '|'
    
    def test_initialization_custom_delimiters(self):
        """Verify TripletGVerbalizer accepts custom delimiters."""
        verbalizer = TripletGVerbalizer(delimiter='--', merge_delimiter=',')
        
        assert verbalizer.delimiter == '--'
        assert verbalizer.merge_delimiter == ','


class TestTripletVerbalizerFormat:
    """Tests for triplet verbalization formatting."""
    
    def test_triplet_verbalizer_format(self):
        """Verify triplet verbalizer formats triplets correctly."""
        verbalizer = TripletGVerbalizer()
        triplets = [
            ('Organization', 'FOUNDED_BY', 'John Doe'),
            ('Organization', 'LOCATED_IN', 'Portland')
        ]
        
        result = verbalizer.verbalize(triplets)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == 'Organization -> FOUNDED_BY -> John Doe'
        assert result[1] == 'Organization -> LOCATED_IN -> Portland'
    
    def test_triplet_verbalizer_custom_delimiter(self):
        """Verify triplet verbalizer uses custom delimiter."""
        verbalizer = TripletGVerbalizer(delimiter='-->')
        triplets = [('Organization', 'FOUNDED_BY', 'John Doe')]
        
        result = verbalizer.verbalize(triplets)
        
        assert result[0] == 'Organization --> FOUNDED_BY --> John Doe'
    
    def test_triplet_verbalizer_single_triplet(self):
        """Verify triplet verbalizer handles single triplet."""
        verbalizer = TripletGVerbalizer()
        triplets = [('John Doe', 'FOUNDED', 'Organization')]
        
        result = verbalizer.verbalize(triplets)
        
        assert len(result) == 1
        assert result[0] == 'John Doe -> FOUNDED -> Organization'


class TestTripletVerbalizerValidation:
    """Tests for triplet validation."""
    
    def test_verbalizer_invalid_triplet_length(self):
        """Verify ValueError raised for invalid triplet length."""
        verbalizer = TripletGVerbalizer()
        invalid_triplets = [('Organization', 'FOUNDED_BY')]  # Only 2 elements
        
        with pytest.raises(ValueError, match="No valid triplets found"):
            verbalizer.verbalize(invalid_triplets)
    
    def test_verbalizer_mixed_valid_invalid_triplets(self):
        """Verify verbalizer filters out invalid triplets and processes valid ones."""
        verbalizer = TripletGVerbalizer()
        mixed_triplets = [
            ('Organization', 'FOUNDED_BY', 'John Doe'),  # Valid
            ('Organization', 'LOCATED_IN'),  # Invalid - only 2 elements
            ('Portland', 'IN', 'Oregon')  # Valid
        ]
        
        result = verbalizer.verbalize(mixed_triplets)
        
        assert len(result) == 2
        assert 'Organization -> FOUNDED_BY -> John Doe' in result
        assert 'Portland -> IN -> Oregon' in result


class TestTripletVerbalizerEmpty:
    """Tests for empty input handling."""
    
    def test_verbalizer_empty_input(self):
        """Verify verbalizer handles empty input list."""
        verbalizer = TripletGVerbalizer()
        empty_triplets = []
        
        with pytest.raises(ValueError, match="No valid triplets found"):
            verbalizer.verbalize(empty_triplets)


class TestTripletVerbalizerRelations:
    """Tests for relation-only verbalization."""
    
    def test_verbalize_relations(self):
        """Verify verbalize_relations returns only relation strings."""
        verbalizer = TripletGVerbalizer()
        triplets = [
            ('Organization', 'FOUNDED_BY', 'John Doe'),
            ('Organization', 'LOCATED_IN', 'Portland')
        ]
        
        result = verbalizer.verbalize_relations(triplets)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == 'FOUNDED_BY'
        assert result[1] == 'LOCATED_IN'


class TestTripletVerbalizerHeadRelations:
    """Tests for head-relation verbalization."""
    
    def test_verbalize_head_relations(self):
        """Verify verbalize_head_relations returns head and relation strings."""
        verbalizer = TripletGVerbalizer()
        triplets = [
            ('Organization', 'FOUNDED_BY', 'John Doe'),
            ('Organization', 'LOCATED_IN', 'Portland')
        ]
        
        result = verbalizer.verbalize_head_relations(triplets)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == 'Organization -> FOUNDED_BY'
        assert result[1] == 'Organization -> LOCATED_IN'


class TestTripletVerbalizerMerge:
    """Tests for merged triplet verbalization."""
    
    def test_verbalize_merge_triplets(self):
        """Verify verbalize_merge_triplets merges tails with same head and relation."""
        verbalizer = TripletGVerbalizer()
        triplets = [
            ('Organization', 'SELLS', 'Software'),
            ('Organization', 'SELLS', 'Hardware'),
            ('Organization', 'SELLS', 'Services'),
            ('DataCorp', 'SELLS', 'Analytics')
        ]
        
        result = verbalizer.verbalize_merge_triplets(triplets)
        
        assert isinstance(result, list)
        # Should merge the three Organization SELLS triplets into one
        organization_sells = [r for r in result if r.startswith('Organization -> SELLS')]
        assert len(organization_sells) == 1
        assert 'Software' in organization_sells[0]
        assert 'Hardware' in organization_sells[0]
        assert 'Services' in organization_sells[0]
        assert '|' in organization_sells[0]  # Default merge delimiter
    
    def test_verbalize_merge_triplets_with_max_retain(self):
        """Verify verbalize_merge_triplets respects max_retain_num parameter."""
        verbalizer = TripletGVerbalizer()
        triplets = [
            ('Organization', 'SELLS', 'Software'),
            ('Organization', 'SELLS', 'Hardware'),
            ('Organization', 'SELLS', 'Services'),
            ('Organization', 'SELLS', 'Consulting'),
            ('Organization', 'SELLS', 'Training')
        ]
        
        result = verbalizer.verbalize_merge_triplets(triplets, max_retain_num=3)
        
        assert isinstance(result, list)
        assert len(result) == 1
        # Should only retain 3 tails
        tail_count = result[0].count('|') + 1  # Number of items = delimiters + 1
        assert tail_count == 3


class TestPathVerbalizerInitialization:
    """Tests for PathVerbalizer initialization."""
    
    def test_initialization_defaults(self):
        """Verify PathVerbalizer initializes with default values."""
        verbalizer = PathVerbalizer()
        
        assert verbalizer.delimiter == '->'
        assert verbalizer.merge_delimiter == '>'
        assert isinstance(verbalizer.graph_verbalizer, TripletGVerbalizer)
    
    def test_initialization_custom_verbalizer(self):
        """Verify PathVerbalizer accepts custom graph verbalizer."""
        custom_verbalizer = TripletGVerbalizer(delimiter='--')
        verbalizer = PathVerbalizer(graph_verbalizer=custom_verbalizer)
        
        assert verbalizer.graph_verbalizer == custom_verbalizer


class TestPathVerbalizerFormat:
    """Tests for path verbalization formatting."""
    
    def test_path_verbalizer_format(self):
        """Verify path verbalizer formats paths correctly."""
        verbalizer = PathVerbalizer()
        paths = [
            [
                ('John Doe', 'FOUNDED', 'Organization'),
                ('Organization', 'LOCATED_IN', 'Portland')
            ]
        ]
        
        result = verbalizer.verbalize(paths)
        
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_path_verbalizer_single_hop_path(self):
        """Verify path verbalizer handles single-hop paths."""
        verbalizer = PathVerbalizer()
        paths = [
            [('Organization', 'FOUNDED_BY', 'John Doe')]
        ]
        
        result = verbalizer.verbalize(paths)
        
        assert isinstance(result, list)
        assert len(result) > 0
    
    def test_path_verbalizer_multi_hop_path(self):
        """Verify path verbalizer handles multi-hop paths."""
        verbalizer = PathVerbalizer()
        paths = [
            [
                ('John Doe', 'FOUNDED', 'Organization'),
                ('Organization', 'LOCATED_IN', 'Portland'),
                ('Portland', 'IN', 'Oregon')
            ]
        ]
        
        result = verbalizer.verbalize(paths)
        
        assert isinstance(result, list)
        assert len(result) > 0


class TestPathVerbalizerEmpty:
    """Tests for empty path handling."""
    
    def test_path_verbalizer_empty_input(self):
        """Verify path verbalizer handles empty input list."""
        verbalizer = PathVerbalizer()
        empty_paths = []
        
        result = verbalizer.verbalize(empty_paths)
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    def test_path_verbalizer_empty_path(self):
        """Verify path verbalizer raises error for empty paths."""
        verbalizer = PathVerbalizer()
        paths = [[]]  # List containing one empty path
        
        # PathVerbalizer skips invalid paths but then raises error if no valid paths remain
        with pytest.raises(ValueError, match="No valid triplets found"):
            verbalizer.verbalize(paths)


class TestPathVerbalizerValidation:
    """Tests for path validation."""
    
    def test_path_verbalizer_invalid_triplet_in_path(self):
        """Verify path verbalizer raises error for paths with invalid triplets."""
        verbalizer = PathVerbalizer()
        paths = [
            [
                ('Organization', 'FOUNDED_BY', 'John Doe'),  # Valid
                ('Organization', 'LOCATED_IN')  # Invalid - only 2 elements
            ]
        ]
        
        # PathVerbalizer skips invalid paths but then raises error if no valid paths remain
        with pytest.raises(ValueError, match="No valid triplets found"):
            verbalizer.verbalize(paths)


class TestGVerbalizerAbstract:
    """Tests for abstract GVerbalizer base class."""
    
    def test_gverbalizer_is_abstract(self):
        """Verify GVerbalizer is an abstract class that cannot be instantiated."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            GVerbalizer()
