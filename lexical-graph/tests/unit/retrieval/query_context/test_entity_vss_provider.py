# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/entity_vss_provider."""

from unittest.mock import MagicMock, patch

from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.model import Entity, ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context import entity_vss_provider
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_vss_provider import (
    EntityVSSProvider,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import (
    DummyVectorIndex,
)


def _scored(entity_id, value, classification, score):
    return ScoredEntity(
        entity=Entity(entityId=entity_id, value=value, classification=classification),
        score=score,
    )


def _provider(index_name='topic'):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    vector_store = MagicMock()
    topic_index = MagicMock() if index_name == 'topic' else DummyVectorIndex(index_name='topic')
    chunk_index = MagicMock()
    vector_store.get_index.side_effect = lambda index_name: {
        'topic': topic_index,
        'chunk': chunk_index,
    }[index_name]
    provider = EntityVSSProvider(
        graph_store=graph_store,
        vector_store=vector_store,
        args=ProcessorArgs(intermediate_limit=5, ec_max_entities=10, reranker='tfidf'),
    )
    return provider, graph_store, topic_index, chunk_index


class TestEntityVSSProvider:
    def test_index_name_falls_back_to_chunk_when_topic_is_dummy(self):
        provider, _, _, _ = _provider(index_name='chunk')
        assert provider.index_name == 'chunk'

    def test_index_name_is_topic_when_topic_index_present(self):
        provider, _, _, _ = _provider(index_name='topic')
        assert provider.index_name == 'topic'

    def test_get_node_ids_extracts_ids_from_topk_results(self):
        provider, _, topic_index, _ = _provider(index_name='topic')
        topic_index.top_k.return_value = [
            {'topic': {'topicId': 't1'}},
            {'topic': {'topicId': 't2'}},
        ]
        assert provider._get_node_ids(['kw1', 'kw2']) == ['t1', 't2']

    def test_get_entities_for_nodes_picks_topic_cypher(self):
        provider, graph_store, _, _ = _provider(index_name='topic')
        graph_store.execute_query.return_value = [
            {'result': {'entity': {'entityId': 'e1', 'value': 'apple', 'classification': 'fruit'}, 'score': 5}},
        ]
        result = provider._get_entities_for_nodes(['t1'])
        assert len(result) == 1
        assert result[0].entity.entityId == 'e1'
        assert 'topic ids' in graph_store.execute_query.call_args.args[0]

    def test_get_entities_for_nodes_picks_chunk_cypher_when_chunk_mode(self):
        provider, graph_store, _, _ = _provider(index_name='chunk')
        graph_store.execute_query.return_value = []
        provider._get_entities_for_nodes(['c1'])
        assert 'chunk ids' in graph_store.execute_query.call_args.args[0]

    def test_get_entities_dedups_across_threads_and_reranks(self):
        provider, _, _, _ = _provider(index_name='topic')

        e1 = _scored('e1', 'apple', 'fruit', 0.9)
        e2 = _scored('e2', 'orange', 'fruit', 0.5)
        # Two of the three lookups return the same entity, one returns a fresh entity.
        with patch.object(provider, '_get_entities_by_keyword_match', return_value=[e1]), \
             patch.object(provider, '_get_entities_for_values', side_effect=[[e1, e2], [e2]]), \
             patch.object(entity_vss_provider, 'rerank_entities', side_effect=lambda entities, *a, **kw: entities):
            result = provider._get_entities(['fruit'], QueryBundle('q'))
        ids = sorted(r.entity.entityId for r in result)
        assert ids == ['e1', 'e2']
