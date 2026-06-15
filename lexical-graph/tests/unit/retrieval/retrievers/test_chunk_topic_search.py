# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ChunkBasedSearch and TopicBasedSearch."""

from unittest.mock import MagicMock, patch

from graphrag_toolkit.core.types import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.retrievers import chunk_based_search as cb_mod
from graphrag_toolkit.lexical_graph.retrieval.retrievers import topic_based_search as tb_mod
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_search import (
    ChunkBasedSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.topic_based_search import (
    TopicBasedSearch,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId


def _stores():
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    vector_store = MagicMock()
    return graph_store, vector_store


class TestChunkBasedSearch:
    def test_get_start_node_ids_extracts_chunk_ids(self):
        graph_store, vector_store = _stores()
        with patch.object(cb_mod, 'get_diverse_vss_elements') as gd:
            gd.return_value = [{'chunk': {'chunkId': 'c1'}}, {'chunk': {'chunkId': 'c2'}}]
            retriever = ChunkBasedSearch(graph_store, vector_store, vss_top_k=2)
            ids = retriever.get_start_node_ids(QueryBundle('q'))
        assert ids == ['c1', 'c2']

    def test_chunk_based_graph_search_returns_statement_ids(self):
        graph_store, vector_store = _stores()
        graph_store.execute_query.return_value = [
            {'l': 'stmt-1'}, {'l': 'stmt-2'},
        ]
        retriever = ChunkBasedSearch(graph_store, vector_store)
        assert retriever.chunk_based_graph_search('c1') == ['stmt-1', 'stmt-2']

    def test_do_graph_search_aggregates_statement_ids_then_dedups(self):
        graph_store, vector_store = _stores()
        retriever = ChunkBasedSearch(graph_store, vector_store, num_workers=2)
        with patch.object(retriever, 'chunk_based_graph_search') as cbs, \
             patch.object(retriever, 'get_statements_by_topic_and_source') as gsts:
            cbs.side_effect = [['s1', 's2'], ['s2', 's3']]
            gsts.return_value = []
            retriever.do_graph_search(QueryBundle('q'), ['c1', 'c2'])
        passed = gsts.call_args.args[0]
        assert sorted(passed) == ['s1', 's2', 's3']


class TestTopicBasedSearch:
    def test_get_start_node_ids_extracts_topic_ids(self):
        graph_store, vector_store = _stores()
        with patch.object(tb_mod, 'get_diverse_vss_elements') as gd:
            gd.return_value = [{'topic': {'topicId': 't1'}}]
            retriever = TopicBasedSearch(graph_store, vector_store)
            assert retriever.get_start_node_ids(QueryBundle('q')) == ['t1']

    def test_topic_based_graph_search_returns_statement_ids(self):
        graph_store, vector_store = _stores()
        graph_store.execute_query.return_value = [{'l': 'stmt-1'}]
        retriever = TopicBasedSearch(graph_store, vector_store)
        assert retriever.topic_based_graph_search('t1') == ['stmt-1']

    def test_do_graph_search_aggregates(self):
        graph_store, vector_store = _stores()
        retriever = TopicBasedSearch(graph_store, vector_store, num_workers=2)
        with patch.object(retriever, 'topic_based_graph_search') as tbs, \
             patch.object(retriever, 'get_statements_by_topic_and_source') as gsts:
            tbs.side_effect = [['s1'], ['s2', 's1']]
            gsts.return_value = []
            retriever.do_graph_search(QueryBundle('q'), ['t1', 't2'])
        passed = gsts.call_args.args[0]
        assert sorted(passed) == ['s1', 's2']
