# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/query_context/keyword_vss_provider."""

from unittest.mock import MagicMock, patch

from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.query_context import (
    keyword_vss_provider as mod,
)
from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_vss_provider import (
    KeywordVSSProvider,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import (
    DummyVectorIndex,
)
from graphrag_toolkit.lexical_graph.utils.llm_cache import LLMCache


def _provider(use_chunk_index=False, llm_response=''):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    vector_store = MagicMock()
    topic_index = MagicMock() if not use_chunk_index else DummyVectorIndex(index_name='topic')
    vector_store.get_index.side_effect = lambda index_name: {
        'topic': topic_index,
        'chunk': MagicMock(),
    }[index_name]

    llm = MagicMock(spec=LLMCache)
    llm.predict.return_value = llm_response

    provider = KeywordVSSProvider(
        graph_store=graph_store,
        vector_store=vector_store,
        args=ProcessorArgs(no_cache=True, max_keywords=5, num_workers=1, intermediate_limit=5),
        llm=llm,
    )
    return provider, graph_store, llm


class TestKeywordVSSProvider:
    def test_index_name_falls_back_to_chunk_when_topic_dummy(self):
        provider, _, _ = _provider(use_chunk_index=True)
        assert provider.index_name == 'chunk'

    def test_get_node_ids_extracts_from_diverse_vss_elements(self):
        provider, _, _ = _provider()
        with patch.object(mod, 'get_diverse_vss_elements') as gd:
            gd.return_value = [{'topic': {'topicId': 't1'}}, {'topic': {'topicId': 't2'}}]
            result = provider._get_node_ids(QueryBundle('q'))
        assert result == ['t1', 't2']

    def test_get_chunk_content_returns_content_strings(self):
        provider, store, _ = _provider(use_chunk_index=True)
        store.execute_query.return_value = [{'content': 'foo'}, {'content': 'bar'}]
        result = provider._get_chunk_content(['c1', 'c2'])
        assert result == ['foo', 'bar']

    def test_get_content_dispatches_by_index_name(self):
        provider, store, _ = _provider(use_chunk_index=True)
        store.execute_query.return_value = [{'content': 'hello'}]
        assert provider._get_content(['c1']) == ['hello']

    def test_get_keywords_from_content_splits_response(self):
        provider, _, llm = _provider(llm_response='apple\norange\n')
        keywords = provider._get_keywords_from_content('q', ['ctx'])
        assert keywords == ['apple', 'orange']
        assert llm.predict.call_args.kwargs['question'] == 'q'

    def test_get_keywords_pipeline_chains_steps(self):
        provider, _, llm = _provider(use_chunk_index=True, llm_response='apple\norange')
        with patch.object(provider, '_get_node_ids', return_value=['c1']) as g1, \
             patch.object(provider, '_get_content', return_value=['ctx']) as g2:
            result = provider.get_keywords(QueryBundle('q'))
        g1.assert_called_once()
        g2.assert_called_once_with(['c1'])
        assert result == ['apple', 'orange']

    def test_get_topic_content_runs_per_topic(self):
        provider, store, _ = _provider()  # topic index
        store.execute_query.return_value = [{'statement': 'fact', 'details': 'a/nb'}]
        result = provider._get_topic_content(['t1'])
        assert isinstance(result, list)
        store.execute_query.assert_called()

    def test_get_content_dispatches_to_topic_index(self):
        provider, store, _ = _provider()  # topic index
        store.execute_query.return_value = []
        assert provider._get_content(['t1']) == []

    def test_get_node_ids_emits_debug_when_enabled(self):
        provider, _, _ = _provider()
        provider.args.debug_results = ['KeywordVSSProvider']
        with patch.object(mod, 'get_diverse_vss_elements') as gd:
            gd.return_value = [{'topic': {'topicId': 't1'}}]
            assert provider._get_node_ids(QueryBundle('q')) == ['t1']

    def test_get_keywords_from_content_emits_debug_when_enabled(self):
        provider, _, _ = _provider(llm_response='apple')
        provider.args.debug_results = ['KeywordVSSProvider']
        assert provider._get_keywords_from_content('q', ['ctx']) == ['apple']
