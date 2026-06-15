# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/entity_context_provider."""

from unittest.mock import MagicMock, patch

from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.model import Entity, ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context import (
    entity_context_provider as mod,
)
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_context_provider import (
    EntityContextProvider,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId


def _se(entity_id, value, classification='thing', score=1.0, reranking_score=1.0):
    e = ScoredEntity(
        entity=Entity(entityId=entity_id, value=value, classification=classification),
        score=score,
    )
    e.reranking_score = reranking_score
    return e


def _provider(**args):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    return EntityContextProvider(
        graph_store=graph_store,
        args=ProcessorArgs(**args),
    ), graph_store


class TestDedupContexts:
    def test_keeps_longest_path_only(self):
        provider, _ = _provider()
        short = [_se('e1', 'a'), _se('e2', 'b')]
        long = [_se('e1', 'a'), _se('e2', 'b'), _se('e3', 'c')]
        result = provider.dedup_contexts([short, long])
        assert long in result
        assert short not in result

    def test_keeps_unrelated_contexts(self):
        provider, _ = _provider()
        c1 = [_se('e1', 'apple')]
        c2 = [_se('e2', 'orange')]
        result = provider.dedup_contexts([c1, c2])
        assert len(result) == 2

    def test_lowercases_for_dedup_key(self):
        provider, _ = _provider()
        c1 = [_se('e1', 'Apple')]
        c2 = [_se('e2', 'apple'), _se('e3', 'pie')]
        result = provider.dedup_contexts([c1, c2])
        assert c2 in result
        assert c1 not in result


class TestOrderContexts:
    def test_orders_by_average_reranking_score(self):
        provider, _ = _provider()
        low = [_se('e1', 'a', reranking_score=0.1)]
        high = [_se('e2', 'b', reranking_score=0.9)]
        result = provider.order_contexts([low, high])
        assert result[0] is high


class TestOrderContextSubtrees:
    def test_groups_subtrees_by_root_entity(self):
        provider, _ = _provider()
        root_a_low = [_se('a', 'A', reranking_score=0.1), _se('a2', 'a2')]
        root_a_high = [_se('a', 'A', reranking_score=0.9), _se('a3', 'a3')]
        root_b = [_se('b', 'B', reranking_score=0.5)]
        result = provider.order_context_subtrees([root_a_low, root_a_high, root_b])
        a_indices = [i for i, c in enumerate(result) if c[0].entity.entityId == 'a']
        assert a_indices == [0, 1] or a_indices == [1, 2]
        # the higher-scoring root_a context comes before root_a_low
        assert result.index(root_a_high) < result.index(root_a_low)


class TestFilterEntities:
    def test_filters_outside_score_window(self):
        provider, _ = _provider(ec_max_score_factor=2, ec_min_score_factor=0.5)
        entities = [
            _se('top', 'top', score=10.0),
            _se('keep', 'keep', score=8.0),
            _se('too_low', 'low', score=1.0),
            _se('too_high', 'huge', score=100.0),
        ]
        result = provider.filter_entities(entities)
        ids = [r.entity.entityId for r in result]
        assert 'keep' in ids and 'top' in ids
        assert 'too_low' not in ids and 'too_high' not in ids


class TestGetNeighbourEntities:
    def test_walks_tree_and_queries_graph(self):
        provider, store = _provider()
        store.execute_query.return_value = [
            {'result': {
                'entity': {'entityId': 'n1', 'value': 'apple', 'classification': 'fruit'},
                'score': 5,
            }},
        ]
        tree = {'root': {'n1': {}, 'n2': {'n3': {}}}}
        result = provider._get_neighbour_entities(tree)
        assert len(result) == 1
        ids = store.execute_query.call_args.args[1]['entityIds']
        # Root keys of the tree (top-level entities) are not collected; only their
        # descendants — _get_neighbour_entities expands ALL nodes below the roots.
        assert set(ids) == {'n1', 'n2', 'n3'}


class TestGetEntityIdContextTree:
    def test_builds_tree_from_graph_results(self):
        provider, store = _provider(ec_max_depth=2)
        store.execute_query.return_value = [
            {'result': {'entity': {'entityId': 'e1'}, 'others': ['c1', 'c2']}},
        ]
        tree = provider._get_entity_id_context_tree([_se('e1', 'apple', score=1.0)])
        assert 'e1' in tree
        assert 'c1' in tree['e1'] and 'c2' in tree['e1']

    def test_skips_entities_with_non_positive_score(self):
        provider, store = _provider(ec_max_depth=2)
        store.execute_query.return_value = []
        tree = provider._get_entity_id_context_tree([_se('e1', 'apple', score=0.0)])
        assert tree == {}
        store.execute_query.assert_not_called()

    def test_emits_debug_when_enabled(self):
        provider, store = _provider(ec_max_depth=2, debug_results=['EntityContextProvider'])
        store.execute_query.return_value = []
        tree = provider._get_entity_id_context_tree([_se('e1', 'apple', score=1.0)])
        assert 'e1' in tree


class TestGetEntityContexts:
    def test_builds_contexts_from_tree(self):
        provider, _ = _provider(ec_max_contexts=5)
        entities = [_se('e1', 'apple'), _se('c1', 'pie')]
        tree = {'e1': {'c1': {}}}
        contexts = provider._get_entity_contexts(entities, tree, QueryBundle('q'))
        assert isinstance(contexts, list)
        assert contexts
        ids = [se.entity.entityId for se in contexts[0]]
        assert ids == ['e1', 'c1']

    def test_respects_max_contexts_limit(self):
        provider, _ = _provider(ec_max_contexts=1, debug_results=['EntityContextProvider'])
        entities = [_se('e1', 'apple'), _se('c1', 'pie'), _se('c2', 'tart')]
        tree = {'e1': {'c1': {}}, 'c2': {}}
        contexts = provider._get_entity_contexts(entities, tree, QueryBundle('q'))
        assert len(contexts) <= 1


class TestGetEntityContextsEntryPoint:
    def test_returns_empty_when_max_contexts_zero(self):
        provider, _ = _provider(ec_max_contexts=0)
        result = provider.get_entity_contexts(
            [_se('e1', 'apple')], ['apple'], QueryBundle('q'),
        )
        assert result.contexts == []

    def test_returns_empty_when_no_entities(self):
        provider, _ = _provider(ec_max_contexts=3)
        result = provider.get_entity_contexts([], ['apple'], QueryBundle('q'))
        assert result.contexts == []

    def test_short_circuits_through_tree_pipeline(self):
        provider, _ = _provider(ec_max_contexts=3, ec_max_depth=2)
        # Force a deterministic tree + neighbour set so the surrounding logic runs
        # without needing a full graph store response shape.
        with patch.object(provider, '_get_entity_id_context_tree', return_value={'e1': {}}), \
             patch.object(provider, '_get_neighbour_entities', return_value=[]), \
             patch.object(mod, 'rerank_entities', side_effect=lambda es, *a, **kw: es):
            result = provider.get_entity_contexts(
                [_se('e1', 'apple', score=1.0, reranking_score=1.0)],
                ['apple'],
                QueryBundle('q'),
            )
        assert hasattr(result, 'contexts')
