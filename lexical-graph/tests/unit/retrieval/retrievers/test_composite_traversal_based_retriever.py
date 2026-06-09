# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/retrievers/composite_traversal_based_retriever."""

from unittest.mock import MagicMock, patch

import pytest
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

from graphrag_toolkit.lexical_graph.retrieval.retrievers.composite_traversal_based_retriever import (
    CompositeTraversalBasedRetriever,
    WeightedTraversalBasedRetriever,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import (
    TraversalBasedBaseRetriever,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore


def _stores():
    return MagicMock(spec=DummyGraphStore), MagicMock()


def _sub_retriever(search_result_json):
    sub = MagicMock(spec=TraversalBasedBaseRetriever)
    sub.retrieve.return_value = [
        NodeWithScore(node=TextNode(text=search_result_json), score=1.0),
    ]
    return sub


_VALID_SEARCH_RESULT = (
    '{"source": {"sourceId": "s1", "metadata": {}, '
    '"versioning": {"valid_from": 0, "valid_to": 9999999999}}, '
    '"topics": []}'
)


class TestCompositeTraversalBasedRetriever:
    @pytest.fixture(autouse=True)
    def _stub_query_decomposition(self):
        # The constructor builds a default QueryDecomposition when none is passed,
        # which reads GraphRAGConfig.response_llm and tries to init BedrockConverse
        # (needs an AWS region). Stub it so these tests stay offline.
        with patch(
            "graphrag_toolkit.lexical_graph.retrieval.retrievers."
            "composite_traversal_based_retriever.QueryDecomposition"
        ):
            yield

    def test_wraps_bare_retrievers_with_weight_1(self):
        gs, vs = _stores()
        sub = _sub_retriever(_VALID_SEARCH_RESULT)
        retriever = CompositeTraversalBasedRetriever(gs, vs, retrievers=[sub])
        assert isinstance(retriever.weighted_retrievers[0], WeightedTraversalBasedRetriever)
        assert retriever.weighted_retrievers[0].weight == 1.0
        assert retriever.weighted_retrievers[0].retriever is sub

    def test_preserves_existing_weighted_retrievers(self):
        gs, vs = _stores()
        sub = _sub_retriever(_VALID_SEARCH_RESULT)
        wr = WeightedTraversalBasedRetriever(retriever=sub, weight=0.5)
        retriever = CompositeTraversalBasedRetriever(gs, vs, retrievers=[wr])
        assert retriever.weighted_retrievers[0] is wr

    def test_get_start_node_ids_returns_empty(self):
        gs, vs = _stores()
        retriever = CompositeTraversalBasedRetriever(gs, vs, retrievers=[_sub_retriever(_VALID_SEARCH_RESULT)])
        assert retriever.get_start_node_ids(QueryBundle('q')) == []

    def test_get_search_results_uses_each_retriever(self):
        gs, vs = _stores()
        sub1 = _sub_retriever(_VALID_SEARCH_RESULT)
        sub2 = _sub_retriever(_VALID_SEARCH_RESULT)
        retriever = CompositeTraversalBasedRetriever(
            gs, vs, retrievers=[sub1, sub2], num_workers=2,
        )
        result = retriever._get_search_results_for_query(QueryBundle('q'))
        assert len(result.results) == 2
        sub1.retrieve.assert_called_once()
        sub2.retrieve.assert_called_once()

    def test_do_graph_search_with_decomposition_disabled_runs_once(self):
        gs, vs = _stores()
        sub = _sub_retriever(_VALID_SEARCH_RESULT)
        retriever = CompositeTraversalBasedRetriever(
            gs, vs, retrievers=[sub], derive_subqueries=False, num_workers=1,
        )
        result = retriever.do_graph_search(QueryBundle('q'), [])
        assert len(result.results) == 1
        sub.retrieve.assert_called_once()

    def test_do_graph_search_decomposes_into_subqueries(self):
        gs, vs = _stores()
        sub = _sub_retriever(_VALID_SEARCH_RESULT)
        decomposition = MagicMock()
        decomposition.decompose_query.return_value = [
            QueryBundle('q1'), QueryBundle('q2'),
        ]
        retriever = CompositeTraversalBasedRetriever(
            gs, vs,
            retrievers=[sub],
            query_decomposition=decomposition,
            derive_subqueries=True,
            num_workers=2,
        )
        result = retriever.do_graph_search(QueryBundle('big q'), [])
        decomposition.decompose_query.assert_called_once()
        assert sub.retrieve.call_count == 2
        assert len(result.results) == 2
