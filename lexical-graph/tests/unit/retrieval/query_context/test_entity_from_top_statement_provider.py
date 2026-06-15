# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/entity_from_top_statement_provider."""

from unittest.mock import MagicMock, patch

from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context import (
    entity_from_top_statement_provider as mod,
)
from graphrag_toolkit.lexical_graph.retrieval.query_context.entity_from_top_statement_provider import (
    EntityFromTopStatementProvider,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import (
    DummyVectorIndex,
)


def _provider(topic_results=None, statement_results=None, entity_results=None,
              use_chunk_index=False):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    side_effect = []
    if statement_results is not None:
        side_effect.append(statement_results)
    if entity_results is not None:
        side_effect.append(entity_results)
    graph_store.execute_query.side_effect = side_effect

    vector_store = MagicMock()
    topic_index = MagicMock() if not use_chunk_index else DummyVectorIndex(index_name='topic')
    chunk_index = MagicMock()
    if topic_results is not None:
        if use_chunk_index:
            chunk_index.top_k.return_value = topic_results
        else:
            topic_index.top_k.return_value = topic_results
    vector_store.get_index.side_effect = lambda index_name: {
        'topic': topic_index,
        'chunk': chunk_index,
    }[index_name]

    return EntityFromTopStatementProvider(
        graph_store=graph_store,
        vector_store=vector_store,
        args=ProcessorArgs(ec_max_entities=10),
    ), graph_store


class TestEntityFromTopStatementProvider:
    def test_uses_chunk_index_when_topic_index_is_dummy(self):
        provider, _ = _provider(use_chunk_index=True)
        assert provider.index_name == 'chunk'

    def test_get_top_statement_id_runs_topic_cypher(self):
        provider, store = _provider(
            topic_results=[{'topic': {'topicId': 't1'}}, {'topic': {'topicId': 't2'}}],
            statement_results=[
                {'result': {'statement': 'apple is a fruit', 'statementId': 'stmt-1'}},
                {'result': {'statement': 'orange is a fruit', 'statementId': 'stmt-2'}},
            ],
        )
        with patch.object(mod, 'score_values_with_tfidf') as scorer:
            scorer.return_value = {'apple is a fruit': 0.9, 'orange is a fruit': 0.2}
            sid = provider._get_top_statement_id(QueryBundle('apple?'))
        assert sid == 'stmt-1'
        cypher, _ = store.execute_query.call_args.args
        assert 'topic' in cypher.lower()

    def test_get_top_statement_id_runs_chunk_cypher_when_chunk_mode(self):
        provider, store = _provider(
            use_chunk_index=True,
            topic_results=[{'chunk': {'chunkId': 'c1'}}],
            statement_results=[{'result': {'statement': 'x', 'statementId': 's1'}}],
        )
        with patch.object(mod, 'score_values_with_tfidf', return_value={'x': 0.5}):
            provider._get_top_statement_id(QueryBundle('q'))
        cypher, _ = store.execute_query.call_args.args
        assert 'chunk' in cypher.lower()

    def test_get_entities_returns_empty_when_no_top_statement(self):
        provider, _ = _provider()
        with patch.object(provider, '_get_top_statement_id', return_value=None):
            assert provider._get_entities([], QueryBundle('q')) == []

    def test_get_entities_for_statement_validates_results(self):
        provider, store = _provider()
        store.execute_query.side_effect = None
        store.execute_query.return_value = [
            {'result': {
                'entity': {'entityId': 'e1', 'value': 'apple', 'classification': 'fruit'},
                'score': 7,
            }},
        ]
        result = provider._get_entities_for_statement('stmt-1')
        assert len(result) == 1
        assert result[0].entity.entityId == 'e1'
