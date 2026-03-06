"""Tests for index.py module.

This module tests the Index abstract base class, Retriever, and EntityMatcher classes.
"""

import pytest
from unittest.mock import Mock, MagicMock
from graphrag_toolkit.byokg_rag.indexing.index import (
    Index,
    Retriever,
    EntityMatcher
)


class TestIndexAbstract:
    """Tests for abstract Index base class."""
    
    def test_index_is_abstract(self):
        """Verify Index is an abstract class that cannot be instantiated."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            Index()
    
    def test_index_subclass_must_implement_reset(self):
        """Verify Index subclass must implement reset method."""
        class IncompleteIndex(Index):
            def query(self, input, topk=1):
                pass
            def add(self, documents):
                pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteIndex()
    
    def test_index_subclass_must_implement_query(self):
        """Verify Index subclass must implement query method."""
        class IncompleteIndex(Index):
            def reset(self):
                pass
            def add(self, documents):
                pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteIndex()
    
    def test_index_subclass_must_implement_add(self):
        """Verify Index subclass must implement add method."""
        class IncompleteIndex(Index):
            def reset(self):
                pass
            def query(self, input, topk=1):
                pass
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteIndex()
    
    def test_complete_index_subclass_can_be_instantiated(self):
        """Verify complete Index subclass can be instantiated."""
        class CompleteIndex(Index):
            def reset(self):
                pass
            def query(self, input, topk=1):
                return {'hits': []}
            def add(self, documents):
                pass
        
        index = CompleteIndex()
        assert isinstance(index, Index)


class TestIndexMethods:
    """Tests for Index base class methods."""
    
    def test_add_with_ids_default_implementation(self):
        """Verify add_with_ids has a default implementation."""
        class TestIndex(Index):
            def reset(self):
                pass
            def query(self, input, topk=1):
                return {'hits': []}
            def add(self, documents):
                pass
        
        index = TestIndex()
        result = index.add_with_ids(['id1', 'id2'], ['doc1', 'doc2'])
        
        assert result is None
    
    def test_as_retriever_returns_retriever(self):
        """Verify as_retriever returns a Retriever instance."""
        class TestIndex(Index):
            def reset(self):
                pass
            def query(self, input, topk=1):
                return {'hits': []}
            def add(self, documents):
                pass
        
        index = TestIndex()
        retriever = index.as_retriever()
        
        assert isinstance(retriever, Retriever)
        assert retriever.index == index
    
    def test_as_entity_matcher_returns_entity_matcher(self):
        """Verify as_entity_matcher returns an EntityMatcher instance."""
        class TestIndex(Index):
            def reset(self):
                pass
            def query(self, input, topk=1):
                return {'hits': []}
            def add(self, documents):
                pass
        
        index = TestIndex()
        matcher = index.as_entity_matcher()
        
        assert isinstance(matcher, EntityMatcher)
        assert matcher.index == index


class TestRetriever:
    """Tests for Retriever base class."""
    
    def test_initialization(self):
        """Verify Retriever initializes with an index."""
        mock_index = Mock()
        retriever = Retriever(index=mock_index)
        
        assert retriever.index == mock_index
    
    def test_retrieve_single_query(self):
        """Verify retrieve processes single query."""
        mock_index = Mock()
        mock_index.query.return_value = {'hits': [{'id': 'doc1', 'score': 0.9}]}
        
        retriever = Retriever(index=mock_index)
        results = retriever.retrieve(['query1'], topk=5)
        
        assert len(results) == 1
        assert results[0]['hits'][0]['id'] == 'doc1'
        mock_index.query.assert_called_once_with('query1', 5)
    
    def test_retrieve_multiple_queries(self):
        """Verify retrieve processes multiple queries."""
        mock_index = Mock()
        mock_index.query.side_effect = [
            {'hits': [{'id': 'doc1', 'score': 0.9}]},
            {'hits': [{'id': 'doc2', 'score': 0.8}]}
        ]
        
        retriever = Retriever(index=mock_index)
        results = retriever.retrieve(['query1', 'query2'], topk=3)
        
        assert len(results) == 2
        assert results[0]['hits'][0]['id'] == 'doc1'
        assert results[1]['hits'][0]['id'] == 'doc2'
        assert mock_index.query.call_count == 2
    
    def test_retrieve_with_id_selectors_list_of_lists(self):
        """Verify retrieve handles id_selectors as list of lists."""
        mock_index = Mock()
        mock_index.query.side_effect = [
            {'hits': [{'id': 'doc1', 'score': 0.9}]},
            {'hits': [{'id': 'doc2', 'score': 0.8}]}
        ]
        
        retriever = Retriever(index=mock_index)
        results = retriever.retrieve(
            ['query1', 'query2'], 
            topk=5, 
            id_selectors=[['id1', 'id2'], ['id3', 'id4']]
        )
        
        assert len(results) == 2
        assert mock_index.query.call_count == 2
    
    def test_retrieve_with_empty_id_selector(self):
        """Verify retrieve skips queries with empty id_selector."""
        mock_index = Mock()
        mock_index.query.return_value = {'hits': [{'id': 'doc1', 'score': 0.9}]}
        
        retriever = Retriever(index=mock_index)
        results = retriever.retrieve(
            ['query1', 'query2'], 
            topk=5, 
            id_selectors=[['id1'], []]
        )
        
        assert len(results) == 2
        assert results[0]['hits'][0]['id'] == 'doc1'
        assert results[1]['hits'] == []
        mock_index.query.assert_called_once()
    
    def test_retrieve_with_invalid_id_selectors(self):
        """Verify retrieve ignores invalid id_selectors (non-list values)."""
        mock_index = Mock()
        mock_index.query.return_value = {'hits': [{'id': 'doc1', 'score': 0.9}]}
        
        retriever = Retriever(index=mock_index)
        # When id_selectors is not a list, it should be ignored and queries processed normally
        results = retriever.retrieve(['query1'], topk=5, id_selectors='invalid')
        
        assert len(results) == 1
        mock_index.query.assert_called_once_with('query1', 5)
    
    def test_retrieve_with_kwargs(self):
        """Verify retrieve passes additional kwargs to index.query."""
        mock_index = Mock()
        mock_index.query.return_value = {'hits': []}
        
        retriever = Retriever(index=mock_index)
        retriever.retrieve(['query1'], topk=5, custom_param='value')
        
        mock_index.query.assert_called_once_with('query1', 5, custom_param='value')


class TestEntityMatcher:
    """Tests for EntityMatcher class."""
    
    def test_initialization(self):
        """Verify EntityMatcher initializes with an index."""
        mock_index = Mock()
        matcher = EntityMatcher(index=mock_index)
        
        assert matcher.index == mock_index
    
    def test_retrieve_calls_index_match(self):
        """Verify retrieve calls index.match method."""
        mock_index = Mock()
        mock_index.match.return_value = [
            {'entity': 'Amazon', 'matched': 'Amazon Inc.'},
            {'entity': 'Seattle', 'matched': 'Seattle, WA'}
        ]
        
        matcher = EntityMatcher(index=mock_index)
        results = matcher.retrieve(['Amazon', 'Seattle'])
        
        assert len(results) == 2
        assert results[0]['entity'] == 'Amazon'
        mock_index.match.assert_called_once_with(['Amazon', 'Seattle'])
    
    def test_retrieve_with_kwargs(self):
        """Verify retrieve passes kwargs to index.match."""
        mock_index = Mock()
        mock_index.match.return_value = []
        
        matcher = EntityMatcher(index=mock_index)
        matcher.retrieve(['entity1'], threshold=0.8, max_matches=5)
        
        mock_index.match.assert_called_once_with(
            ['entity1'], 
            threshold=0.8, 
            max_matches=5
        )
    
    def test_retrieve_empty_queries(self):
        """Verify retrieve handles empty query list."""
        mock_index = Mock()
        mock_index.match.return_value = []
        
        matcher = EntityMatcher(index=mock_index)
        results = matcher.retrieve([])
        
        assert results == []
        mock_index.match.assert_called_once_with([])


class TestRetrieverIntegration:
    """Integration tests for Retriever with Index."""
    
    def test_retriever_with_complete_index(self):
        """Verify Retriever works with a complete Index implementation."""
        class TestIndex(Index):
            def __init__(self):
                super().__init__()
                self.documents = ['doc1', 'doc2', 'doc3']
            
            def reset(self):
                self.documents = []
            
            def query(self, input, topk=1):
                return {'hits': self.documents[:topk]}
            
            def add(self, documents):
                self.documents.extend(documents)
        
        index = TestIndex()
        retriever = index.as_retriever()
        
        results = retriever.retrieve(['query1', 'query2'], topk=2)
        
        assert len(results) == 2
        assert len(results[0]['hits']) == 2
        assert len(results[1]['hits']) == 2


class TestEntityMatcherIntegration:
    """Integration tests for EntityMatcher with Index."""
    
    def test_entity_matcher_with_complete_index(self):
        """Verify EntityMatcher works with a complete Index implementation."""
        class TestIndex(Index):
            def __init__(self):
                super().__init__()
                self.entities = {'Amazon': 'Amazon Inc.', 'Seattle': 'Seattle, WA'}
            
            def reset(self):
                self.entities = {}
            
            def query(self, input, topk=1):
                return {'hits': []}
            
            def add(self, documents):
                pass
            
            def match(self, queries, **kwargs):
                return [{'entity': q, 'matched': self.entities.get(q, None)} for q in queries]
        
        index = TestIndex()
        matcher = index.as_entity_matcher()
        
        results = matcher.retrieve(['Amazon', 'Seattle'])
        
        assert len(results) == 2
        assert results[0]['matched'] == 'Amazon Inc.'
        assert results[1]['matched'] == 'Seattle, WA'
