# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ByoKGQueryEngine.

This module tests the query engine orchestration including initialization,
query processing, context deduplication, and response generation.
"""

import logging

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from graphrag_toolkit.byokg_rag.byokg_query_engine import ByoKGQueryEngine


@pytest.fixture
def mock_graph_store_with_schema():
    """Fixture providing a mock graph store with schema and execute_query."""
    mock_store = Mock()
    mock_store.get_schema.return_value = {
        'node_types': ['Person', 'Organization', 'Location'],
        'edge_types': ['WORKS_FOR', 'FOUNDED', 'LOCATED_IN']
    }
    mock_store.nodes.return_value = ['Organization', 'John Doe', 'Portland']
    mock_store.execute_query = Mock(return_value=[])
    return mock_store


@pytest.fixture
def mock_llm_generator():
    """Fixture providing a mock LLM generator."""
    mock_gen = Mock()
    mock_gen.generate.return_value = "<entity-extraction>Organization</entity-extraction><task-completion>FINISH</task-completion>"
    return mock_gen


@pytest.fixture
def mock_entity_linker():
    """Fixture providing a mock entity linker."""
    mock_linker = Mock()
    mock_linker.link.return_value = ['Organization', 'Portland']
    return mock_linker


@pytest.fixture
def mock_kg_linker():
    """Fixture providing a mock KG linker."""
    mock_linker = Mock()
    mock_linker.task_prompts = "Mock task prompts"
    mock_linker.task_prompts_iterative = "Mock iterative task prompts"
    mock_linker.generate_response.return_value = (
        "<entity-extraction>Organization</entity-extraction>"
        "<task-completion>FINISH</task-completion>"
    )
    mock_linker.parse_response.return_value = {
        'entity-extraction': ['Organization'],
        'draft-answer-generation': []
    }
    return mock_linker


class TestQueryEngineInitialization:
    """Tests for query engine initialization."""
    
    def test_initialization_with_defaults(self, mock_graph_store_with_schema):
        """Verify query engine initializes with default components."""
        with patch('graphrag_toolkit.byokg_rag.indexing.FuzzyStringIndex') as mock_fuzzy, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.EntityLinker') as mock_entity, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.PathRetriever') as mock_path, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.GraphQueryRetriever') as mock_graph_query, \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.GTraversal'), \
             patch('graphrag_toolkit.byokg_rag.graph_retrievers.PathVerbalizer'):
            
            mock_fuzzy_instance = Mock()
            mock_fuzzy.return_value = mock_fuzzy_instance
            mock_fuzzy_instance.add.return_value = None
            mock_fuzzy_instance.as_entity_matcher.return_value = Mock()
            
            mock_entity_instance = Mock()
            mock_entity.return_value = mock_entity_instance
            
            engine = ByoKGQueryEngine(graph_store=mock_graph_store_with_schema)
            
            assert engine.graph_store == mock_graph_store_with_schema
            assert engine.schema is not None
            assert engine.llm_generator is None
            assert engine.entity_linker is not None
            assert engine.kg_linker is None
            assert engine.triplet_retriever is None
    
    def test_initialization_with_custom_components(
        self, mock_graph_store_with_schema, mock_llm_generator, 
        mock_entity_linker, mock_kg_linker
    ):
        """Verify query engine accepts custom components."""
        mock_triplet_retriever = Mock()
        mock_path_retriever = Mock()
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            path_retriever=mock_path_retriever,
            kg_linker=mock_kg_linker
        )
        
        assert engine.llm_generator == mock_llm_generator
        assert engine.entity_linker == mock_entity_linker
        assert engine.triplet_retriever == mock_triplet_retriever
        assert engine.path_retriever == mock_path_retriever
        assert engine.kg_linker == mock_kg_linker


class TestQueryEngineQuery:
    """Tests for query processing."""
    
    def test_query_single_iteration(
        self, mock_graph_store_with_schema, mock_llm_generator, 
        mock_entity_linker, mock_kg_linker
    ):
        """Verify single iteration query processing."""
        mock_triplet_retriever = Mock()
        mock_triplet_retriever.retrieve.return_value = ['John Doe founded Organization']
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            kg_linker=mock_kg_linker
        )
        
        result = engine.query("Who founded Organization?", iterations=1)
        
        assert isinstance(result, list)
        mock_kg_linker.generate_response.assert_called_once()
        mock_kg_linker.parse_response.assert_called_once()
    
    def test_query_context_deduplication(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify context deduplication in _add_to_context."""
        with patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator
            )
            
            context = ['item1', 'item2']
            engine._add_to_context(context, ['item2', 'item3', 'item1'])
            
            assert context == ['item1', 'item2', 'item3']
            assert context.count('item1') == 1
            assert context.count('item2') == 1


class TestQueryEngineSeedOrdering:
    """Tests that seed sets fed to retrievers are deterministic (nondeterminism fix)."""

    def test_source_entities_dedup_preserves_order(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """source_entities must dedup while preserving first-seen order.

        Guards the switch from list(set(...)) to list(dict.fromkeys(...)) at the
        seed union, and sorted(explored_entities) for the path retriever.
        """
        mock_entity_linker = Mock()
        # First call = extracted entities (has an internal duplicate),
        # second call = draft answers (overlaps the first).
        mock_entity_linker.link.side_effect = [
            ['Organization', 'Portland', 'Organization'],
            ['Portland', 'John Doe'],
        ]

        mock_kg_linker = Mock()
        mock_kg_linker.task_prompts = "test"
        mock_kg_linker.task_prompts_iterative = "test"
        mock_kg_linker.generate_response.return_value = "<task-completion>FINISH</task-completion>"
        mock_kg_linker.parse_response.return_value = {
            'entity-extraction': ['Organization'],
            'draft-answer-generation': ['Portland'],
            'path-extraction': ['Organization->Portland'],
        }

        captured = {}
        mock_triplet_retriever = Mock()
        mock_triplet_retriever.retrieve.side_effect = (
            lambda query, source_entities: captured.__setitem__('seeds', source_entities) or ['ctx']
        )
        mock_path_retriever = Mock()
        mock_path_retriever.retrieve.side_effect = (
            lambda entities, metapaths, answers: captured.__setitem__('path_entities', entities) or []
        )

        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            path_retriever=mock_path_retriever,
            kg_linker=mock_kg_linker,
        )

        engine.query("Who founded Organization?", iterations=1)

        # deduped, first-seen order preserved (not set() ordering)
        assert captured['seeds'] == ['Organization', 'Portland', 'John Doe']
        # path retriever gets a stable sorted order
        assert captured['path_entities'] == sorted({'Organization', 'Portland'})


def _mention_kg_linker():
    """A KG linker extracting two mentions and one draft answer, then FINISH."""
    mock_kg_linker = Mock()
    mock_kg_linker.task_prompts = "test"
    mock_kg_linker.task_prompts_iterative = "test"
    mock_kg_linker.generate_response.return_value = "<task-completion>FINISH</task-completion>"
    mock_kg_linker.parse_response.return_value = {
        'entity-extraction': ['MentionA', 'MentionB'],
        'draft-answer-generation': ['DraftAnswer'],
    }
    return mock_kg_linker


def _grouping_entity_linker():
    """A linker that honours group_by_mention, like EntityLinker."""
    mock_entity_linker = Mock()

    def fake_link(entities, return_dict=True, group_by_mention=False):
        if group_by_mention:
            # MentionA -> [A1, A2], MentionB -> [B1]
            return [['A1', 'A2'], ['B1']]
        return ['ans1']

    mock_entity_linker.link.side_effect = fake_link
    return mock_entity_linker


class TestQueryEngineSingleBestMatch:
    """Tests for the single_best_match seed-pruning flag."""

    def _kg_linker(self):
        return _mention_kg_linker()

    @staticmethod
    def _grouping_linker():
        return _grouping_entity_linker()

    def test_flag_off_keeps_the_flat_union(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """With the flag off, seeds come from link()'s flat union."""
        mock_entity_linker = Mock()
        # link() is called for extracted mentions then draft answers (current behavior).
        mock_entity_linker.link.side_effect = [
            ['A1', 'A2', 'B1'],   # extracted mentions, flattened across mentions
            ['ans1'],             # draft answers
        ]

        captured = {}
        mock_triplet_retriever = Mock()
        mock_triplet_retriever.retrieve.side_effect = (
            lambda query, source_entities: captured.__setitem__('seeds', source_entities) or ['ctx']
        )

        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            kg_linker=self._kg_linker(),
            single_best_match=False,
        )
        assert engine.single_best_match is False

        engine.query("q", iterations=1)

        # never asks for grouping when the flag is off
        for call in mock_entity_linker.link.call_args_list:
            assert not call.kwargs.get('group_by_mention', False)
        assert set(captured['seeds']) == {'A1', 'A2', 'B1', 'ans1'}

    def test_flag_on_keeps_one_seed_per_mention_and_leaves_answers(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """With the flag on, each mention yields at most one seed; draft answers untouched."""
        mock_entity_linker = self._grouping_linker()

        captured = {}
        mock_triplet_retriever = Mock()
        mock_triplet_retriever.retrieve.side_effect = (
            lambda query, source_entities: captured.__setitem__('seeds', source_entities) or ['ctx']
        )

        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=mock_triplet_retriever,
            kg_linker=self._kg_linker(),
            single_best_match=True,
        )

        engine.query("q", iterations=1)

        # one seed per mention (A2 dropped) plus the untouched draft answer
        assert set(captured['seeds']) == {'A1', 'B1', 'ans1'}
        assert mock_entity_linker.link.call_args_list[0] == call(
            ['MentionA', 'MentionB'], return_dict=False, group_by_mention=True
        )
        # draft answers go through the ungrouped path
        assert mock_entity_linker.link.call_args_list[1] == call(
            ['DraftAnswer'], return_dict=False
        )

    def test_flag_on_also_narrows_the_path_retriever_seeds(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """explored_entities feeds the path retriever, so pruning narrows paths too.

        Documented behaviour, not just a triplet-seed change.
        """
        kg_linker = self._kg_linker()
        kg_linker.parse_response.return_value = {
            'entity-extraction': ['MentionA', 'MentionB'],
            'draft-answer-generation': ['DraftAnswer'],
            'path-extraction': ['A1->B1'],
        }

        captured = {}
        mock_path_retriever = Mock()
        mock_path_retriever.retrieve.side_effect = (
            lambda entities, metapaths, answers:
                captured.__setitem__('path_entities', entities) or []
        )

        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=self._grouping_linker(),
            triplet_retriever=Mock(**{'retrieve.return_value': ['ctx']}),
            path_retriever=mock_path_retriever,
            kg_linker=kg_linker,
            single_best_match=True,
        )

        engine.query("q", iterations=1)

        # A2 is absent from the path seeds as well, not just the triplet seeds
        assert captured['path_entities'] == ['A1', 'B1']

    def test_flag_on_with_linker_ignoring_group_by_mention_raises(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """A linker that ignores group_by_mention must fail loudly, not seed the flat union.

        This one passes the __init__ signature check (its link() accepts **kwargs)
        and is caught by the call-site shape check instead. The two checks cover
        different failures; neither subsumes the other.
        """
        mock_entity_linker = Mock()
        # Third-party linker with link(queries, return_dict=True, **kwargs): the
        # unknown flag lands in **kwargs and is dropped, so it returns a flat list.
        mock_entity_linker.link.return_value = ['A1', 'A2', 'B1']

        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=Mock(**{'retrieve.return_value': ['ctx']}),
            kg_linker=self._kg_linker(),
            single_best_match=True,
        )

        with pytest.raises(TypeError, match="one candidate list per mention"):
            engine.query("q", iterations=1)

    def test_reassigning_entity_linker_after_construction_is_still_caught(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """The guard holds even when the linker is swapped in after __init__."""
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=self._grouping_linker(),
            triplet_retriever=Mock(**{'retrieve.return_value': ['ctx']}),
            kg_linker=self._kg_linker(),
            single_best_match=True,
        )

        bad_linker = Mock()
        bad_linker.link.return_value = ['A1', 'A2']
        engine.entity_linker = bad_linker

        with pytest.raises(TypeError, match="one candidate list per mention"):
            engine.query("q", iterations=1)

    def test_flag_on_with_linker_whose_link_cannot_group_raises_at_construction(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """A link() that can't take group_by_mention fails at __init__, not mid-query.

        Without this the error surfaces only after kg_linker.generate_response()
        has run — and after the cypher loop when a cypher_kg_linker is set.
        """
        class UngroupableLinker:
            def link(self, queries, return_dict=True):
                return ['A1', 'A2']

        with pytest.raises(TypeError, match="requires an entity_linker whose link"):
            ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                entity_linker=UngroupableLinker(),
                kg_linker=self._kg_linker(),
                single_best_match=True,
            )

    def test_flag_on_rejects_a_grouping_whose_length_is_not_the_mention_count(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """One list per *mention*, not one per hit.

        A linker whose document_id is a list returns list-of-lists from its flat
        path, which satisfies the element-type check. The length check is what
        separates that from a real grouping.
        """
        mock_entity_linker = Mock()
        # three hits for two mentions
        mock_entity_linker.link.return_value = [['e1'], ['e2'], ['e3']]

        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            entity_linker=mock_entity_linker,
            triplet_retriever=Mock(**{'retrieve.return_value': ['ctx']}),
            kg_linker=self._kg_linker(),
            single_best_match=True,
        )

        with pytest.raises(TypeError, match="of length 3 for 2 mention"):
            engine.query("q", iterations=1)


class _FlatLinker:
    """A pre-3.20 custom linker: a fixed link() signature without group_by_mention."""

    def __init__(self, result=('A1', 'A2')):
        self.result, self.calls = list(result), []

    def link(self, queries, return_dict=True):
        self.calls.append(list(queries))
        return list(self.result)


class _ForwardingLinker:
    """Wraps another linker and forwards every argument, as thin adapters do."""

    def __init__(self, inner):
        self.inner = inner

    def link(self, *args, **kwargs):
        return self.inner.link(*args, **kwargs)


class TestQueryEngineSingleBestMatchDefault:
    """single_best_match=None (the default): on when link() takes group_by_mention,
    otherwise the flat union with a warning, never an error."""

    LOGGER = 'graphrag_toolkit.byokg_rag.byokg_query_engine'

    @staticmethod
    def _engine(store, llm, linker, captured, kg_linker=None):
        retriever = Mock()
        retriever.retrieve.side_effect = (
            lambda query, source_entities: captured.__setitem__('seeds', source_entities) or ['ctx'])
        return ByoKGQueryEngine(graph_store=store, llm_generator=llm, entity_linker=linker,
                                triplet_retriever=retriever,
                                kg_linker=kg_linker or _mention_kg_linker())

    def test_on_for_a_linker_that_groups(self, mock_graph_store_with_schema, mock_llm_generator, caplog):
        """A grouping linker turns it on, silently."""
        captured = {}
        with caplog.at_level(logging.WARNING, logger=self.LOGGER):
            engine = self._engine(mock_graph_store_with_schema, mock_llm_generator,
                                  _grouping_entity_linker(), captured)
            engine.query("q", iterations=1)
        assert engine.single_best_match is True
        assert set(captured['seeds']) == {'A1', 'B1', 'ans1'}
        assert caplog.text == ''

    def test_real_entity_linker_seeds_one_node_per_mention(self, mock_graph_store_with_schema, mock_llm_generator):
        """The shipped EntityLinker end to end, including a mention that matches nothing."""
        from graphrag_toolkit.byokg_rag.graph_retrievers import EntityLinker
        from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex
        index = FuzzyStringIndex()
        index.add(['Portland', 'Portland Trail Blazers', 'Organization'])
        kg_linker = _mention_kg_linker()
        # The second mention is too long for any node to pass the fuzzy length filter.
        kg_linker.parse_response.return_value = {
            'entity-extraction': ['Portland', 'a mention far longer than any node in the graph'],
            'draft-answer-generation': [],
        }
        captured = {}
        engine = self._engine(mock_graph_store_with_schema, mock_llm_generator,
                              EntityLinker(index.as_entity_matcher()), captured, kg_linker)
        engine.query("q", iterations=1)
        assert engine.single_best_match is True
        assert captured['seeds'] == ['Portland'], 'top candidate only; the empty group adds nothing'

    def test_off_with_a_warning_for_a_linker_that_cannot_group(
        self, mock_graph_store_with_schema, mock_llm_generator, caplog
    ):
        """No TypeError, unlike single_best_match=True: the flat union, with a warning."""
        linker, captured = _FlatLinker(), {}
        with caplog.at_level(logging.WARNING, logger=self.LOGGER):
            engine = self._engine(mock_graph_store_with_schema, mock_llm_generator, linker, captured)
        assert engine.single_best_match is False
        assert '_FlatLinker.link() does not take group_by_mention' in caplog.text
        assert 'single_best_match=False to silence this' in caplog.text
        engine.query("q", iterations=1)
        assert set(captured['seeds']) == {'A1', 'A2'}

    def test_no_construction_warning_without_mention_linking_or_when_explicitly_off(
        self, mock_graph_store_with_schema, caplog
    ):
        """Graph-only engines never link mentions, and False is a choice, not a fallback."""
        with caplog.at_level(logging.WARNING, logger=self.LOGGER):
            ByoKGQueryEngine(graph_store=mock_graph_store_with_schema, entity_linker=_FlatLinker())
            ByoKGQueryEngine(graph_store=mock_graph_store_with_schema, entity_linker=_FlatLinker(),
                             llm_generator=Mock(), kg_linker=_mention_kg_linker(),
                             single_best_match=False)
        assert caplog.text == ''

    def test_falls_back_once_when_the_linker_ignores_group_by_mention(
        self, mock_graph_store_with_schema, mock_llm_generator, caplog
    ):
        """A link() that drops the flag has returned its flat union: reused, then off for good."""
        linker = Mock()  # accepts any kwargs, drops group_by_mention, returns its flat result
        linker.link.side_effect = [['A1', 'A2', 'B1'], ['ans1'], ['A1', 'A2', 'B1'], ['ans1']]
        captured = {}
        engine = self._engine(mock_graph_store_with_schema, mock_llm_generator, linker, captured)
        assert engine.single_best_match is True
        with caplog.at_level(logging.WARNING, logger=self.LOGGER):
            engine.query("q", iterations=1)
        assert 'returned list of length 3 for 2 mention(s)' in caplog.text
        assert set(captured['seeds']) == {'A1', 'A2', 'B1', 'ans1'}
        assert engine.single_best_match is False
        engine.query("q", iterations=1)
        assert linker.link.call_count == 4, 'no extra link() call to recover the flat result'
        assert 'group_by_mention' not in linker.link.call_args_list[2].kwargs, 'later queries skip grouping'

    def test_relinks_flat_when_the_grouping_has_the_wrong_shape(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Per-hit lists are not a flat result to reuse: link() is called again, flat, and stays flat."""
        mentions = ['MentionA', 'MentionB']
        linker = Mock()
        linker.link.side_effect = [[['e1'], ['e2'], ['e3']], ['A1', 'B1'], ['ans1'], ['A1', 'B1'], ['ans1']]
        captured = {}
        engine = self._engine(mock_graph_store_with_schema, mock_llm_generator, linker, captured)
        engine.query("q", iterations=1)
        assert linker.link.call_args_list[1] == call(mentions, return_dict=False)
        assert set(captured['seeds']) == {'A1', 'B1', 'ans1'}
        engine.query("q", iterations=1)
        assert linker.link.call_args_list[3] == call(mentions, return_dict=False), 'still flat'

    def test_a_forwarding_linker_whose_target_rejects_the_flag_falls_back(
        self, mock_graph_store_with_schema, mock_llm_generator, caplog
    ):
        """A **kwargs wrapper passes the signature check; the TypeError from its target is caught."""
        inner, captured = _FlatLinker(), {}
        engine = self._engine(mock_graph_store_with_schema, mock_llm_generator, _ForwardingLinker(inner), captured)
        assert engine.single_best_match is True, 'precondition: the wrapper looks like it can group'
        with caplog.at_level(logging.WARNING, logger=self.LOGGER):
            engine.query("q", iterations=1)
        assert '_ForwardingLinker.link() rejected group_by_mention' in caplog.text
        assert set(captured['seeds']) == {'A1', 'A2'} and engine.single_best_match is False

    def test_a_flat_linker_swapped_in_later_falls_back_instead_of_raising(
        self, mock_graph_store_with_schema, mock_llm_generator, caplog
    ):
        """The default re-checks the current linker, so a swapped-in flat one is never asked to group."""
        captured = {}
        engine = self._engine(mock_graph_store_with_schema, mock_llm_generator,
                              _grouping_entity_linker(), captured)
        assert engine.single_best_match is True, 'precondition: the default turned it on'
        engine.entity_linker = flat = _FlatLinker()
        with caplog.at_level(logging.WARNING, logger=self.LOGGER):
            engine.query("q", iterations=1)
        assert '_FlatLinker.link() does not take group_by_mention' in caplog.text
        assert set(captured['seeds']) == {'A1', 'A2'} and engine.single_best_match is False
        assert flat.calls[0] == ['MentionA', 'MentionB'], 'called flat, never with the flag'

    def test_explicit_true_still_raises_when_a_swapped_in_linker_rejects_the_flag(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """The fallback is only for the default: True keeps main's error for a strict linker."""
        engine = ByoKGQueryEngine(graph_store=mock_graph_store_with_schema, llm_generator=mock_llm_generator,
                                  entity_linker=_grouping_entity_linker(),
                                  triplet_retriever=Mock(**{'retrieve.return_value': []}),
                                  kg_linker=_mention_kg_linker(), single_best_match=True)
        engine.entity_linker = _FlatLinker()
        with pytest.raises(TypeError, match="group_by_mention"):
            engine.query("q", iterations=1)

    def test_truthy_values_are_as_strict_as_true(self, mock_graph_store_with_schema, mock_llm_generator):
        """single_best_match=1 (e.g. from config) raises like True rather than falling back."""
        linker = Mock()
        linker.link.return_value = ['A1', 'A2', 'B1']
        strict = ByoKGQueryEngine(graph_store=mock_graph_store_with_schema, llm_generator=mock_llm_generator,
                                  entity_linker=linker, triplet_retriever=Mock(**{'retrieve.return_value': []}),
                                  kg_linker=_mention_kg_linker(), single_best_match=1)
        assert strict.single_best_match is True
        with pytest.raises(TypeError, match="one candidate list per mention"):
            strict.query("q", iterations=1)


class TestQueryEngineGenerateResponse:
    """Tests for response generation."""
    
    def test_generate_response_default_prompt(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify response generation with default prompt."""
        mock_llm_generator.generate.return_value = (
            "<answers>Organization was founded by John Doe</answers>"
        )
        
        with patch('graphrag_toolkit.byokg_rag.byokg_query_engine.load_yaml') as mock_load_yaml, \
             patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            
            mock_load_yaml.return_value = {
                "generate-response-qa": "Question: {question}\nContext: {graph_context}\nUser Input: {user_input}\nAnswer:"
            }
            
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator
            )
            
            answers, response = engine.generate_response(
                query="Who founded Organization?",
                graph_context="John Doe founded Organization"
            )
            
            assert isinstance(answers, list)
            assert isinstance(response, str)
            assert "Organization was founded by John Doe" in response
            mock_llm_generator.generate.assert_called_once()



class TestQueryEngineWithCypherLinker:
    """Tests for query engine with cypher linker."""
    
    def test_initialization_with_cypher_linker(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify initialization with cypher linker."""
        mock_cypher_linker = Mock()
        mock_cypher_linker.is_cypher_linker = True
        mock_cypher_linker.task_prompts = "cypher prompts"
        mock_cypher_linker.task_prompts_iterative = "cypher iterative prompts"
        
        mock_graph_query_executor = Mock()
        
        with patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                cypher_kg_linker=mock_cypher_linker,
                graph_query_executor=mock_graph_query_executor
            )
            
            assert engine.cypher_kg_linker == mock_cypher_linker
            assert engine.graph_query_executor == mock_graph_query_executor
    
    def test_initialization_cypher_linker_without_attribute(
        self, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify error when cypher linker lacks is_cypher_linker attribute."""
        mock_cypher_linker = Mock(spec=[])
        
        with pytest.raises(AssertionError, match="must be an instance of CypherKGLinker"):
            ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                cypher_kg_linker=mock_cypher_linker
            )
    
    @patch('graphrag_toolkit.byokg_rag.byokg_query_engine.parse_response')
    def test_query_with_cypher_linker_finish(
        self, mock_parse_response, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify query with cypher linker that finishes early."""
        mock_cypher_linker = Mock()
        mock_cypher_linker.is_cypher_linker = True
        mock_cypher_linker.task_prompts = "cypher prompts"
        mock_cypher_linker.task_prompts_iterative = "cypher iterative prompts"
        mock_cypher_linker.generate_response.return_value = (
            "<opencypher>MATCH (n) RETURN n</opencypher>"
            "<task-completion>FINISH</task-completion>"
        )
        mock_cypher_linker.parse_response.return_value = {
            'opencypher': ['MATCH (n) RETURN n']
        }
        
        mock_graph_query_executor = Mock()
        mock_graph_query_executor.retrieve.return_value = (
            ['Query result'], [{'name': 'Organization'}]
        )
        
        # Mock parse_response to return FINISH
        mock_parse_response.return_value = ['FINISH']
        
        with patch('graphrag_toolkit.byokg_rag.graph_connectors.KGLinker') as mock_kg:
            mock_kg_instance = Mock()
            mock_kg_instance.task_prompts = "test"
            mock_kg_instance.task_prompts_iterative = "test"
            mock_kg.return_value = mock_kg_instance
            
            engine = ByoKGQueryEngine(
                graph_store=mock_graph_store_with_schema,
                llm_generator=mock_llm_generator,
                cypher_kg_linker=mock_cypher_linker,
                graph_query_executor=mock_graph_query_executor
            )
            
            result = engine.query("test query", cypher_iterations=2)
            
            assert isinstance(result, list)
            # Should finish early, so only called once
            assert mock_cypher_linker.generate_response.call_count == 1
    
    @patch('graphrag_toolkit.byokg_rag.byokg_query_engine.parse_response')
    def test_query_with_cypher_linker_no_kg_linker(
        self, mock_parse_response, mock_graph_store_with_schema, mock_llm_generator
    ):
        """Verify query with only cypher linker (no kg_linker)."""
        mock_cypher_linker = Mock()
        mock_cypher_linker.is_cypher_linker = True
        mock_cypher_linker.task_prompts = "cypher prompts"
        mock_cypher_linker.task_prompts_iterative = "cypher iterative prompts"
        mock_cypher_linker.generate_response.return_value = (
            "<opencypher>MATCH (n) RETURN n</opencypher>"
        )
        mock_cypher_linker.parse_response.return_value = {
            'opencypher': ['MATCH (n) RETURN n']
        }
        
        mock_graph_query_executor = Mock()
        mock_graph_query_executor.retrieve.return_value = (
            ['Query result'], [{'name': 'Amazon'}]
        )
        
        mock_parse_response.return_value = []
        
        engine = ByoKGQueryEngine(
            graph_store=mock_graph_store_with_schema,
            llm_generator=mock_llm_generator,
            cypher_kg_linker=mock_cypher_linker,
            graph_query_executor=mock_graph_query_executor,
            kg_linker=None
        )
        
        result = engine.query("test query", cypher_iterations=1)
        
        assert isinstance(result, list)
        # Should return cypher context directly
        assert len(result) > 0
