# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_linker.py module.

This module tests the EntityLinker and Linker classes including
initialization, linking functionality, return formats, and error handling.
"""

import pytest
from unittest.mock import Mock
from graphrag_toolkit.byokg_rag.graph_retrievers.entity_linker import (
    Linker,
    EntityLinker
)


@pytest.fixture
def mock_retriever():
    """
    Fixture providing a mock retriever for entity linking tests.
    
    Returns a mock retriever that simulates entity matching without
    requiring a real index or database connection.
    """
    mock_ret = Mock()
    mock_ret.retrieve.return_value = {
        'hits': [
            {
                'document_id': ['entity1', 'entity2'],
                'document': ['Amazon', 'Amazon Web Services'],
                'match_score': [95.0, 85.0]
            }
        ]
    }
    return mock_ret


class TestEntityLinkerInitialization:
    """Tests for EntityLinker initialization."""
    
    def test_initialization_with_retriever(self, mock_retriever):
        """Verify EntityLinker initializes with retriever and topk."""
        linker = EntityLinker(retriever=mock_retriever, topk=5)
        
        assert linker.retriever == mock_retriever
        assert linker.topk == 5
    
    def test_initialization_defaults(self):
        """Verify EntityLinker initializes with default values."""
        linker = EntityLinker()
        
        assert linker.retriever is None
        assert linker.topk == 3


class TestEntityLinkerLink:
    """Tests for EntityLinker link method."""
    
    def test_link_return_dict(self, mock_retriever):
        """Verify link returns dictionary format when return_dict=True."""
        linker = EntityLinker(retriever=mock_retriever, topk=3)
        query_entities = [['Amazon', 'AWS']]
        
        result = linker.link(query_entities, return_dict=True)
        
        assert isinstance(result, dict)
        assert 'hits' in result
        mock_retriever.retrieve.assert_called_once_with(
            queries=query_entities,
            topk=3
        )
    
    def test_link_return_list(self, mock_retriever):
        """Verify link returns list of entity ID lists when return_dict=False."""
        linker = EntityLinker(retriever=mock_retriever, topk=3)
        query_entities = [['Amazon']]
        
        result = linker.link(query_entities, return_dict=False)
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == ['entity1', 'entity2']
        mock_retriever.retrieve.assert_called_once_with(
            queries=query_entities,
            topk=3
        )
    
    def test_link_with_custom_topk(self, mock_retriever):
        """Verify link uses custom topk parameter when provided."""
        linker = EntityLinker(retriever=mock_retriever, topk=3)
        query_entities = [['Amazon']]
        
        linker.link(query_entities, topk=10, return_dict=True)
        
        mock_retriever.retrieve.assert_called_once_with(
            queries=query_entities,
            topk=10
        )
    
    def test_link_with_custom_retriever(self, mock_retriever):
        """Verify link uses custom retriever parameter when provided."""
        linker = EntityLinker(topk=3)  # No default retriever
        custom_retriever = Mock()
        custom_retriever.retrieve.return_value = {
            'hits': [{'document_id': ['custom1'], 'document': ['Custom'], 'match_score': [90.0]}]
        }
        query_entities = [['Test']]
        
        result = linker.link(query_entities, retriever=custom_retriever, return_dict=True)
        
        custom_retriever.retrieve.assert_called_once()
        assert isinstance(result, dict)
    
    def test_link_no_retriever_error(self):
        """Verify ValueError raised when no retriever is available."""
        linker = EntityLinker()  # No retriever
        query_entities = [['Amazon']]
        
        with pytest.raises(ValueError, match="Either 'retriever' or 'self.retriever' must be provided"):
            linker.link(query_entities)
    
    def test_link_multiple_queries(self, mock_retriever):
        """Verify link handles multiple query entity lists."""
        mock_retriever.retrieve.return_value = {
            'hits': [
                {'document_id': ['e1'], 'document': ['Entity1'], 'match_score': [95.0]},
                {'document_id': ['e2'], 'document': ['Entity2'], 'match_score': [90.0]}
            ]
        }
        linker = EntityLinker(retriever=mock_retriever, topk=3)
        query_entities = [['Amazon'], ['Microsoft']]
        
        result = linker.link(query_entities, return_dict=False)
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == ['e1']
        assert result[1] == ['e2']


class TestEntityLinkerLinkGrouped:
    """Tests for EntityLinker.link_grouped (per-mention candidate grouping)."""

    def _index_backed_retriever(self):
        # Real matchers return a scalar document_id per hit (not a list), so the
        # mock mirrors that shape. index.query is called once per mention.
        mock_ret = Mock()

        def fake_query(mention, topk):
            return {
                'Amazon': {'hits': [
                    {'document_id': 'Amazon', 'document': 'Amazon', 'match_score': 100},
                    {'document_id': 'Amazon Web Services', 'document': 'Amazon Web Services', 'match_score': 85},
                ]},
                'Google': {'hits': [
                    {'document_id': 'Google', 'document': 'Google', 'match_score': 100},
                ]},
            }[mention]

        mock_ret.index.query.side_effect = fake_query
        return mock_ret

    def test_link_grouped_preserves_per_mention_grouping(self):
        """Each mention gets its own best-first candidate list, in input order."""
        mock_ret = self._index_backed_retriever()
        linker = EntityLinker(retriever=mock_ret, topk=3)

        grouped = linker.link_grouped(['Amazon', 'Google'])

        assert grouped == [['Amazon', 'Amazon Web Services'], ['Google']]
        # index.query, not the flattening matcher, and with the configured topk
        assert mock_ret.index.query.call_args_list[0].args == ('Amazon', 3)
        assert mock_ret.index.query.call_args_list[1].args == ('Google', 3)

    def test_link_grouped_no_retriever_error(self):
        """ValueError when no retriever is available."""
        linker = EntityLinker()

        with pytest.raises(ValueError, match="Either 'retriever' or 'self.retriever' must be provided"):
            linker.link_grouped(['Amazon'])

    def test_link_grouped_requires_index_backed_retriever(self):
        """AttributeError when the retriever exposes no index to query."""
        linker = EntityLinker()
        retriever = Mock(spec=[])  # no .index attribute

        with pytest.raises(AttributeError, match="index"):
            linker.link_grouped(['Amazon'], retriever=retriever)


class TestLinkerAbstract:
    """Tests for abstract Linker base class."""
    
    def test_linker_is_abstract(self):
        """Verify Linker is an abstract class that cannot be instantiated."""
        # Linker is abstract with @abstractmethod on link()
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            Linker()
    
    def test_linker_default_implementation(self):
        """Verify Linker subclass can use default link implementation."""
        # Create a concrete subclass that doesn't override link()
        class ConcreteLinker(Linker):
            def link(self, queries, return_dict=True, **kwargs):
                # Use parent's default implementation
                return super().link(queries, return_dict, **kwargs)
        
        linker = ConcreteLinker()
        
        # Test return_dict=True
        result_dict = linker.link(['query1'], return_dict=True)
        assert result_dict == [{'hits': [{'document_id': [], 'document': [], 'match_score': []}]}]
        
        # Test return_dict=False
        result_list = linker.link(['query1'], return_dict=False)
        assert result_list == [[]]
