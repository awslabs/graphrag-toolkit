# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for entity_based_search, entity_network_search, entity_context_search."""

from unittest.mock import MagicMock, patch

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

from graphrag_toolkit.lexical_graph.retrieval.model import (
    Entity,
    EntityContext,
    EntityContexts,
    ScoredEntity,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers import (
    entity_network_search as ens_mod,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.entity_based_search import (
    EntityBasedSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.entity_context_search import (
    EntityContextSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.entity_network_search import (
    EntityNetworkSearch,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import (
    DummyVectorIndex,
)


def _stores(use_chunk_index=False):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    vector_store = MagicMock()
    topic_index = MagicMock() if not use_chunk_index else DummyVectorIndex(index_name='topic')
    vector_store.get_index.side_effect = lambda index_name: {
        'topic': topic_index,
        'chunk': MagicMock(),
    }[index_name]
    return graph_store, vector_store


def _ec(*scored_entities):
    return EntityContexts(contexts=[EntityContext(entities=list(scored_entities))])


def _se(entity_id, value='v', classification='c'):
    return ScoredEntity(
        entity=Entity(entityId=entity_id, value=value, classification=classification),
        score=1.0,
    )


class TestEntityBasedSearch:
    def test_get_start_node_ids_returns_unique_entity_ids(self):
        graph_store, vector_store = _stores()
        ec = _ec(_se('e1'), _se('e2'), _se('e1'))
        retriever = EntityBasedSearch(graph_store, vector_store, entity_contexts=ec)
        ids = retriever.get_start_node_ids(QueryBundle('q'))
        assert sorted(ids) == ['e1', 'e2']

    def test_for_each_disjoint_yields_value_plus_others(self):
        graph_store, vector_store = _stores()
        retriever = EntityBasedSearch(graph_store, vector_store)
        pairs = list(retriever._for_each_disjoint(['a', 'b', 'c']))
        assert len(pairs) == 3
        for value, others in pairs:
            assert value not in others

    def test_for_each_disjoint_unique_yields_upper_triangle_pairs(self):
        graph_store, vector_store = _stores()
        retriever = EntityBasedSearch(graph_store, vector_store)
        pairs = list(retriever._for_each_disjoint_unique(['a', 'b', 'c']))
        assert pairs == [('a', ['b', 'c']), ('b', ['c'])]

    def test_single_entity_based_graph_search_returns_statement_ids(self):
        graph_store, vector_store = _stores()
        graph_store.execute_query.return_value = [{'l': 's1'}]
        retriever = EntityBasedSearch(graph_store, vector_store)
        assert retriever._single_entity_based_graph_search('e1', QueryBundle('q')) == ['s1']

    def test_multiple_entity_based_graph_search_returns_statement_ids(self):
        graph_store, vector_store = _stores()
        graph_store.execute_query.return_value = [{'l': 's2'}, {'l': 's3'}]
        retriever = EntityBasedSearch(graph_store, vector_store)
        result = retriever._multiple_entity_based_graph_search('e1', ['e2'], QueryBundle('q'))
        assert result == ['s2', 's3']

    def test_do_graph_search_runs_both_single_and_multi(self):
        graph_store, vector_store = _stores()
        retriever = EntityBasedSearch(graph_store, vector_store, num_workers=2)
        with patch.object(retriever, '_single_entity_based_graph_search', return_value=['s1']) as single, \
             patch.object(retriever, '_multiple_entity_based_graph_search', return_value=['s2']) as multi, \
             patch.object(retriever, 'get_statements_by_topic_and_source', return_value=[]):
            retriever.do_graph_search(QueryBundle('q'), ['e1', 'e2'])
        # Single for each start id; multi for each pairing.
        assert single.call_count == 2
        assert multi.call_count == 2

    def test_single_entity_based_graph_search_handles_empty_results(self):
        graph_store, vector_store = _stores()
        graph_store.execute_query.return_value = []
        retriever = EntityBasedSearch(graph_store, vector_store)
        assert retriever._single_entity_based_graph_search('e1', QueryBundle('q')) == []

    def test_single_entity_based_graph_search_propagates_store_exception(self):
        import pytest
        graph_store, vector_store = _stores()
        graph_store.execute_query.side_effect = RuntimeError('graph store down')
        retriever = EntityBasedSearch(graph_store, vector_store)
        with pytest.raises(RuntimeError, match='graph store down'):
            retriever._single_entity_based_graph_search('e1', QueryBundle('q'))

    def test_get_start_node_ids_returns_empty_when_no_entity_contexts(self):
        graph_store, vector_store = _stores()
        retriever = EntityBasedSearch(graph_store, vector_store)
        assert retriever.get_start_node_ids(QueryBundle('q')) == []


class TestEntityNetworkSearch:
    def test_index_name_falls_back_to_chunk(self):
        graph_store, vector_store = _stores(use_chunk_index=True)
        retriever = EntityNetworkSearch(graph_store, vector_store)
        assert retriever.index_name == 'chunk'

    def test_graph_search_topic_returns_statement_ids(self):
        graph_store, vector_store = _stores()
        graph_store.execute_query.return_value = [{'l': 's1'}]
        retriever = EntityNetworkSearch(graph_store, vector_store)
        assert retriever._graph_search('t1') == ['s1']

    def test_graph_search_chunk_runs_chunk_cypher(self):
        graph_store, vector_store = _stores(use_chunk_index=True)
        graph_store.execute_query.return_value = [{'l': 's1'}]
        retriever = EntityNetworkSearch(graph_store, vector_store)
        retriever._graph_search('c1')
        cypher, _ = graph_store.execute_query.call_args.args
        assert 'chunk' in cypher.lower()

    def test_get_node_ids_extracts_from_diverse(self):
        graph_store, vector_store = _stores()
        retriever = EntityNetworkSearch(graph_store, vector_store)
        with patch.object(ens_mod, 'get_diverse_vss_elements') as gd:
            gd.return_value = [{'topic': {'topicId': 't1'}}]
            assert retriever._get_node_ids(QueryBundle('q')) == ['t1']

    def test_get_start_node_ids_iterates_entity_contexts(self):
        graph_store, vector_store = _stores()
        retriever = EntityNetworkSearch(
            graph_store, vector_store,
            entity_contexts=_ec(_se('e1'), _se('e2')),
            num_workers=1,
        )
        with patch.object(retriever, '_get_node_ids', return_value=['t1']):
            result = retriever.get_start_node_ids(QueryBundle('q'))
        assert result == ['t1']

    def test_do_graph_search_aggregates_then_dedups(self):
        graph_store, vector_store = _stores()
        retriever = EntityNetworkSearch(graph_store, vector_store, num_workers=2)
        with patch.object(retriever, '_graph_search') as gs, \
             patch.object(retriever, 'get_statements_by_topic_and_source', return_value=[]) as gsts:
            gs.side_effect = [['s1', 's2'], ['s2', 's3']]
            retriever.do_graph_search(QueryBundle('q'), ['t1', 't2'])
        passed = gsts.call_args.args[0]
        assert sorted(passed) == ['s1', 's2', 's3']


class TestEntityContextSearch:
    def test_get_start_node_ids_returns_empty(self):
        graph_store, vector_store = _stores()
        retriever = EntityContextSearch(graph_store, vector_store)
        assert retriever.get_start_node_ids(QueryBundle('q')) == []

    def test_do_graph_search_returns_empty_when_no_contexts(self):
        graph_store, vector_store = _stores()
        retriever = EntityContextSearch(graph_store, vector_store)
        result = retriever.do_graph_search(QueryBundle('q'), [])
        assert result.results == []

    def test_do_graph_search_uses_sub_retriever_with_each_context(self):
        from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import (
            TraversalBasedBaseRetriever,
        )
        graph_store, vector_store = _stores()
        sub = MagicMock(spec=TraversalBasedBaseRetriever)
        node = NodeWithScore(node=TextNode(text='x', metadata={'result': {
            'source': {'sourceId': 's1', 'metadata': {}, 'versioning': {'valid_from': 0, 'valid_to': 9}},
            'topics': [],
        }}))
        sub.retrieve.return_value = [node]

        ec = EntityContexts(contexts=[
            EntityContext(entities=[_se('e1', 'alpha'), _se('e2', 'beta')]),
        ])
        retriever = EntityContextSearch(
            graph_store, vector_store,
            sub_retriever=sub,
            entity_contexts=ec,
            ec_max_contexts=5,
        )
        retriever.do_graph_search(QueryBundle('q'), [])
        sub.retrieve.assert_called()
