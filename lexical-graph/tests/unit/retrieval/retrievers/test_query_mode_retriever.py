# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/retrievers/query_mode_retriever."""

from unittest.mock import MagicMock, patch

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

from graphrag_toolkit.lexical_graph.retrieval.query_context.query_mode import QueryMode
from graphrag_toolkit.lexical_graph.retrieval.retrievers import query_mode_retriever as mod
from graphrag_toolkit.lexical_graph.retrieval.retrievers.query_mode_retriever import (
    QueryModeRetriever,
)


def _node(text):
    return NodeWithScore(node=TextNode(text=text), score=1.0)


class TestQueryModeRetriever:
    def test_simple_mode_runs_single_retriever(self):
        inner = MagicMock()
        inner.retrieve.return_value = [_node('hit')]
        retriever_fn = MagicMock(return_value=inner)

        qmr = QueryModeRetriever(retriever_fn=retriever_fn, enable_multipart_queries=False)
        result = qmr._retrieve(QueryBundle('hello?'))

        retriever_fn.assert_called_once()
        inner.retrieve.assert_called_once()
        assert len(result) == 1

    def test_multipart_simple_query_runs_single_retriever(self):
        inner = MagicMock()
        inner.retrieve.return_value = [_node('hit')]
        retriever_fn = MagicMock(return_value=inner)

        with patch.object(mod, 'QueryModeProvider') as qmp_cls:
            qmp = MagicMock()
            qmp.get_query_mode.return_value = QueryMode.SIMPLE
            qmp_cls.return_value = qmp

            qmr = QueryModeRetriever(
                retriever_fn=retriever_fn, enable_multipart_queries=True, no_cache=True,
            )
            result = qmr._retrieve(QueryBundle('q'))

        retriever_fn.assert_called_once()
        assert len(result) == 1

    def test_multipart_complex_query_fans_out_per_keyword(self):
        inner = MagicMock()
        inner.retrieve.side_effect = [[_node('a')], [_node('b')]]
        retriever_fn = MagicMock(return_value=inner)

        with patch.object(mod, 'QueryModeProvider') as qmp_cls, \
             patch.object(mod, 'KeywordProvider') as kp_cls:
            qmp = MagicMock()
            qmp.get_query_mode.return_value = QueryMode.COMPLEX
            qmp_cls.return_value = qmp
            kp = MagicMock()
            kp.get_keywords.return_value = ['k1', 'k2']
            kp_cls.return_value = kp

            qmr = QueryModeRetriever(
                retriever_fn=retriever_fn, enable_multipart_queries=True,
                no_cache=True, max_search_results=10,
            )
            result = qmr._retrieve(QueryBundle('q'))

        assert inner.retrieve.call_count == 2
        assert len(result) == 2

    def test_complex_query_passes_passthru_keyword_provider_arg(self):
        inner = MagicMock()
        inner.retrieve.return_value = []
        retriever_fn = MagicMock(return_value=inner)

        with patch.object(mod, 'QueryModeProvider') as qmp_cls, \
             patch.object(mod, 'KeywordProvider') as kp_cls:
            qmp = MagicMock()
            qmp.get_query_mode.return_value = QueryMode.COMPLEX
            qmp_cls.return_value = qmp
            kp = MagicMock()
            kp.get_keywords.return_value = ['k1']
            kp_cls.return_value = kp

            qmr = QueryModeRetriever(
                retriever_fn=retriever_fn, enable_multipart_queries=True,
                no_cache=True, max_search_results=4,
            )
            qmr._retrieve(QueryBundle('q'))

        passed = retriever_fn.call_args.kwargs
        assert passed['ec_keyword_provider'] == 'passthru'
        # max_search_results / len(keywords) + 1
        assert passed['max_search_results'] == 5
