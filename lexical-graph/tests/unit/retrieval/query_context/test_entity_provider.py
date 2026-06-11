# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/entity_provider."""

from unittest.mock import MagicMock

from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_provider import (
    EntityProvider,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId


def _entity_row(entity_id, value, classification, score):
    return {
        'result': {
            'entity': {'entityId': entity_id, 'value': value, 'classification': classification},
            'score': score,
        }
    }


def _provider(execute_responses):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    graph_store.execute_query.side_effect = execute_responses
    return EntityProvider(
        graph_store=graph_store,
        args=ProcessorArgs(num_workers=1, ec_max_entities=10),
    ), graph_store


class TestGetEntitiesForKeyword:
    def test_simple_keyword_runs_exact_match_query(self):
        provider, store = _provider([[_entity_row('e1', 'apple', 'fruit', 5)]])
        entities = provider._get_entities_for_keyword('apple')
        assert len(entities) == 1
        cypher, params = store.execute_query.call_args.args
        assert 'entity.search_str = $keyword' in cypher
        assert 'class <> ' in cypher
        assert params == {'keyword': 'apple'}

    def test_classified_keyword_filters_by_class(self):
        provider, store = _provider([[_entity_row('e1', 'apple', 'fruit', 5)]])
        provider._get_entities_for_keyword('apple|fruit')
        _, params = store.execute_query.call_args.args
        assert params == {'keyword': 'apple', 'classification': 'fruit'}

    def test_zero_score_results_dropped(self):
        # Both the exact and STARTS WITH fallback return only zero-score rows.
        zero = [_entity_row('e1', 'apple', 'fruit', 0)]
        provider, _ = _provider([zero, zero])
        entities = provider._get_entities_for_keyword('apple')
        assert entities == []

    def test_falls_back_to_starts_with_when_no_exact_match(self):
        provider, store = _provider([
            [],
            [_entity_row('e2', 'apple-pie', 'fruit', 3)],
        ])
        entities = provider._get_entities_for_keyword('apple')
        assert len(entities) == 1
        assert store.execute_query.call_count == 2
        cypher2, _ = store.execute_query.call_args_list[1].args
        assert 'STARTS WITH' in cypher2


class TestGetEntities:
    def test_dedups_and_sums_scores_across_keywords(self):
        # Both keyword lookups return entity e1 with scores 3 and 5;
        # provider should sum to 8 and rank above any other.
        provider, _ = _provider([
            [_entity_row('e1', 'apple', 'fruit', 3)],
            [_entity_row('e1', 'apple', 'fruit', 5), _entity_row('e2', 'orange', 'fruit', 2)],
        ])
        result = provider._get_entities(['apple', 'apple'], QueryBundle('q'))
        ids_scores = [(r.entity.entityId, r.score) for r in result]
        assert ids_scores[0] == ('e1', 8)
        assert ('e2', 2) in ids_scores

    def test_sorts_descending_by_score(self):
        provider, _ = _provider([
            [_entity_row('e1', 'a', 'x', 1), _entity_row('e2', 'b', 'x', 5)],
        ])
        result = provider._get_entities(['a'], QueryBundle('q'))
        assert [r.entity.entityId for r in result] == ['e2', 'e1']
