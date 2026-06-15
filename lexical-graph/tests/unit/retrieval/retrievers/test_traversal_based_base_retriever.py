# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/retrievers/traversal_based_base_retriever."""

from unittest.mock import MagicMock, patch

import pytest
from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import (
    EntityContexts,
    SearchResult,
    Source,
    Versioning,
)
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.retrievers import (
    traversal_based_base_retriever as mod,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import (
    DEFAULT_FORMATTING_PROCESSORS,
    DEFAULT_PROCESSORS,
    TraversalBasedBaseRetriever,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId


class _Concrete(TraversalBasedBaseRetriever):
    """Minimal concrete subclass exposing the abstract methods."""
    def get_start_node_ids(self, query_bundle):
        return ['n1']

    def do_graph_search(self, query_bundle, start_node_ids):
        return self._to_search_results_collection([])


def _retriever(**args):
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    vector_store = MagicMock()
    return _Concrete(
        graph_store=graph_store, vector_store=vector_store, **args,
    ), graph_store


class TestInit:
    def test_defaults_install_default_processor_lists(self):
        retriever, _ = _retriever()
        assert retriever.processors is DEFAULT_PROCESSORS
        assert retriever.formatting_processors is DEFAULT_FORMATTING_PROCESSORS

    def test_processor_args_override(self):
        args = ProcessorArgs(max_keywords=99)
        retriever, _ = _retriever(processor_args=args)
        assert retriever.args.max_keywords == 99

    def test_custom_processors_replace_defaults(self):
        retriever, _ = _retriever(processors=[])
        assert retriever.processors == []

    def test_default_filter_config_and_entity_contexts(self):
        retriever, _ = _retriever()
        assert isinstance(retriever.filter_config, FilterConfig)
        assert isinstance(retriever.entity_contexts, EntityContexts)


class TestInitKeywordProviderDispatch:
    def _patch_pipeline(self, monkeypatch, keyword_provider_cls):
        kp = MagicMock()
        kp.get_keywords.return_value = []
        monkeypatch.setattr(mod, keyword_provider_cls, MagicMock(return_value=kp))

        ep = MagicMock()
        ep.get_entities.return_value = []
        monkeypatch.setattr(mod, 'EntityProvider', MagicMock(return_value=ep))
        monkeypatch.setattr(mod, 'EntityVSSProvider', MagicMock(return_value=ep))

        ecp = MagicMock()
        ecp.get_entity_contexts.return_value = EntityContexts(contexts=[], keywords=[])
        monkeypatch.setattr(mod, 'EntityContextProvider', MagicMock(return_value=ecp))

    def test_vss_keyword_provider(self, monkeypatch):
        self._patch_pipeline(monkeypatch, 'KeywordVSSProvider')
        retriever, _ = _retriever(ec_keyword_provider='vss', ec_entity_provider='graph')
        retriever._init(QueryBundle('q'))
        mod.KeywordVSSProvider.assert_called_once()

    def test_llm_keyword_provider(self, monkeypatch):
        self._patch_pipeline(monkeypatch, 'KeywordProvider')
        retriever, _ = _retriever(ec_keyword_provider='llm', ec_entity_provider='graph')
        retriever._init(QueryBundle('q'))
        mod.KeywordProvider.assert_called_once()

    def test_nlp_keyword_provider(self, monkeypatch):
        self._patch_pipeline(monkeypatch, 'KeywordNLPProvider')
        retriever, _ = _retriever(ec_keyword_provider='nlp', ec_entity_provider='graph')
        retriever._init(QueryBundle('q'))
        mod.KeywordNLPProvider.assert_called_once()

    def test_passthru_keyword_provider(self, monkeypatch):
        self._patch_pipeline(monkeypatch, 'PassThruKeywordProvider')
        retriever, _ = _retriever(ec_keyword_provider='passthru', ec_entity_provider='graph')
        retriever._init(QueryBundle('q'))
        mod.PassThruKeywordProvider.assert_called_once()

    def test_invalid_keyword_provider_raises(self):
        retriever, _ = _retriever(ec_keyword_provider='bogus')
        with pytest.raises(ValueError, match='ec_keyword_provider'):
            retriever._init(QueryBundle('q'))

    def test_invalid_entity_provider_raises(self, monkeypatch):
        self._patch_pipeline(monkeypatch, 'KeywordVSSProvider')
        retriever, _ = _retriever(ec_keyword_provider='vss', ec_entity_provider='bogus')
        with pytest.raises(ValueError, match='ec_entity_provider'):
            retriever._init(QueryBundle('q'))

    def test_skips_init_when_keywords_already_present(self):
        retriever, _ = _retriever(ec_keyword_provider='vss', ec_entity_provider='graph')
        retriever.entity_contexts.keywords.append('preexisting')
        with patch.object(mod, 'KeywordVSSProvider') as kp:
            retriever._init(QueryBundle('q'))
        kp.assert_not_called()


class TestToSearchResultsCollection:
    def test_passes_through_existing_search_results(self):
        retriever, _ = _retriever()
        sr = SearchResult(
            source=Source(
                sourceId='s1',
                metadata={},
                versioning=Versioning(valid_from=0, valid_to=9999999999),
            ),
            topics=[],
        )
        result = retriever._to_search_results_collection([sr])
        assert len(result.results) == 1

    def test_validates_dict_with_source(self):
        retriever, _ = _retriever()
        raw = {
            'result': {
                'source': {
                    'sourceId': 's1',
                    'metadata': {},
                    'versioning': {'valid_from': 0, 'valid_to': 9999999999},
                },
                'topics': [],
            }
        }
        result = retriever._to_search_results_collection([raw])
        assert len(result.results) == 1

    def test_filters_dict_without_source(self):
        retriever, _ = _retriever()
        result = retriever._to_search_results_collection([{'result': {'topics': []}}])
        assert result.results == []


class TestGetStatementsByTopicAndSource:
    def test_passes_params_and_attaches_facts(self):
        retriever, store = _retriever(intermediate_limit=20, query_limit=5)
        store.execute_query.side_effect = [
            [{'result': {
                'source': {'sourceId': 's1'},
                'topics': [{
                    'topic': 'T',
                    'topicId': 't1',
                    'chunks': [],
                    'statements': [{'statementId': 'stmt-1', 'score': 0, 'facts': []}],
                }],
                'score': 1,
            }}],
            [{'statementId': 'stmt-1', 'facts': ['f1', 'f2']}],
        ]
        result = retriever.get_statements_by_topic_and_source(['stmt-1'])
        topic = result[0]['result']['topics'][0]
        statement = topic['statements'][0]
        assert statement['facts'] == ['f1', 'f2']
        assert statement['score'] == 2
