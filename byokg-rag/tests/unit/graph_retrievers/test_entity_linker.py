# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_linker.py module.

This module tests the EntityLinker and Linker classes including
initialization, linking functionality, return formats, and error handling.
"""

import pytest
from collections import Counter
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
    
    def test_link_accepts_id_selector_kwarg(self, mock_retriever):
        """id_selector stays in the signature (unused) so existing callers don't break."""
        linker = EntityLinker(retriever=mock_retriever, topk=3)

        result = linker.link([['Amazon']], id_selector=['entity1'], return_dict=True)

        assert isinstance(result, dict)

    def test_link_positional_argument_order(self, mock_retriever):
        """Pins the positional order: the 4th positional arg is id_selector, not return_dict."""
        linker = EntityLinker(topk=3)

        # link(entities, retriever, topk, id_selector, return_dict)
        result = linker.link([['Amazon']], mock_retriever, 3, None, False)

        assert result == [['entity1', 'entity2']]

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


class TestEntityLinkerGroupByMention:
    """Tests for link(group_by_mention=True) (per-mention candidate grouping)."""

    def _matcher(self):
        # Matches one mention at a time through the same matcher link() uses.
        # The matcher returns one dict with a scalar document_id per hit.
        mock_ret = Mock()

        def fake_retrieve(queries, topk):
            (mention,) = queries
            return {
                'Amazon': {'hits': [
                    {'document_id': 'Amazon', 'document': 'Amazon', 'match_score': 100},
                    {'document_id': 'Amazon Web Services', 'document': 'Amazon Web Services', 'match_score': 85},
                ]},
                'Google': {'hits': [
                    {'document_id': 'Google', 'document': 'Google', 'match_score': 100},
                ]},
            }[mention]

        mock_ret.retrieve.side_effect = fake_retrieve
        return mock_ret

    def test_group_by_mention_preserves_per_mention_grouping(self):
        """Each mention gets its own best-first candidate list, in input order."""
        mock_ret = self._matcher()
        linker = EntityLinker(retriever=mock_ret, topk=3)

        grouped = linker.link(['Amazon', 'Google'], group_by_mention=True)

        assert grouped == [['Amazon', 'Amazon Web Services'], ['Google']]
        # one matcher call per mention (not one batched call), with configured topk
        assert mock_ret.retrieve.call_args_list[0].kwargs == {'queries': ['Amazon'], 'topk': 3}
        assert mock_ret.retrieve.call_args_list[1].kwargs == {'queries': ['Google'], 'topk': 3}

    def test_group_by_mention_takes_precedence_over_return_dict(self):
        """group_by_mention wins over return_dict, as the docstring states."""
        linker = EntityLinker(retriever=self._matcher(), topk=3)

        grouped = linker.link(['Amazon'], return_dict=True, group_by_mention=True)

        assert grouped == [['Amazon', 'Amazon Web Services']]

    def test_group_by_mention_dedups_repeated_mentions(self):
        """A repeated mention is looked up once; parse_response does not dedup."""
        mock_ret = self._matcher()
        linker = EntityLinker(retriever=mock_ret, topk=3)

        grouped = linker.link(['Amazon', 'Google', 'Amazon'], group_by_mention=True)

        # result still has one entry per input position, duplicates included
        assert grouped == [
            ['Amazon', 'Amazon Web Services'],
            ['Google'],
            ['Amazon', 'Amazon Web Services'],
        ]
        # but only two retriever round trips
        assert mock_ret.retrieve.call_count == 2

    def test_group_by_mention_no_retriever_error(self):
        """ValueError when no retriever is available."""
        linker = EntityLinker()

        with pytest.raises(ValueError, match="Either 'retriever' or 'self.retriever' must be provided"):
            linker.link(['Amazon'], group_by_mention=True)


class TestEntityLinkerGroupByMentionRealIndex:
    """link(group_by_mention=True) against a real FuzzyStringIndex, no mocks.

    The mocked tests above pin the call pattern but would stay green if the
    per-mention loop were swapped for a batched retrieve. These run the real
    matcher, where FuzzyStringIndex.match concatenates every mention's hits and
    re-sorts them globally, so a batched implementation cannot reproduce them.
    """

    # Mixed-length vocab so the matcher's max_len_difference filter engages:
    # match() drops a candidate when len(candidate) + 4 < len(mention).
    VOCAB = [
        'Amazon',
        'Amazon Web Services',
        'Amazon River',
        'African American National Biography Project',
        'USA',
        'United States of America',
        'Seattle',
        'Seattle Mariners',
    ]
    MENTIONS = [
        'Amazon',
        'United States of America',
        'African American Foundation',
        'Seatle',
    ]

    @pytest.fixture
    def linker(self):
        from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex
        index = FuzzyStringIndex()
        index.add(self.VOCAB)
        return EntityLinker(index.as_entity_matcher(), topk=3)

    def test_each_group_equals_linking_that_mention_alone(self, linker):
        """grouped[i] must be exactly what link() returns for that mention alone.

        This is the attribution guarantee. A batched retrieve sliced into
        topk-sized chunks fails here, because the fuzzy matcher's length filter
        removes candidates before the global sort, so chunk boundaries do not
        line up with mentions.
        """
        grouped = linker.link(self.MENTIONS, group_by_mention=True)

        assert len(grouped) == len(self.MENTIONS)
        for mention, candidates in zip(self.MENTIONS, grouped):
            # a single-mention batch cannot be reordered across mentions, so its
            # flat result is that mention's candidate list
            assert candidates == linker.link([mention], return_dict=False)

    def test_flattened_groups_equal_links_union_as_a_multiset(self, linker):
        """Grouping changes only attribution, never which candidates survive.

        Compared as a multiset, not a list: link() re-sorts all hits globally by
        score, while grouping keeps mention order, so the two orders differ.
        """
        grouped = linker.link(self.MENTIONS, group_by_mention=True)
        flattened = [c for group in grouped for c in group]
        union = linker.link(self.MENTIONS, return_dict=False)

        assert Counter(flattened) == Counter(union)

    def test_length_filter_can_empty_a_group(self, linker):
        """A mention whose only candidates are too short yields no seeds.

        Pins the behaviour that makes topk=1 unsafe: the length filter runs
        after process.extract(limit=topk), so a narrow search can return nothing.
        """
        at_topk_1 = linker.link(['United States of America'], topk=1, group_by_mention=True)
        at_topk_3 = linker.link(['United States of America'], topk=3, group_by_mention=True)

        # 'USA' is 21 chars shorter than the mention, so it is filtered out
        assert 'USA' not in at_topk_1[0]
        assert 'United States of America' in at_topk_3[0]


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
