# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for ChunkBasedSemanticSearch and SemanticChunkBeamGraphSearch."""

from unittest.mock import MagicMock

import numpy as np
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_semantic_search import (
    ChunkBasedSemanticSearch,
)
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_chunk_beam_search import (
    SemanticChunkBeamGraphSearch,
)
from graphrag_toolkit.lexical_graph.storage.graph.dummy_graph_store import DummyGraphStore
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId


def _stores():
    graph_store = MagicMock(spec=DummyGraphStore)
    graph_store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
    vector_store = MagicMock()
    return graph_store, vector_store


def _chunk_node(chunk_id):
    return NodeWithScore(
        node=TextNode(text='', metadata={'chunk': {'chunkId': chunk_id}}),
        score=1.0,
    )


class TestChunkBasedSemanticSearch:
    def test_creates_default_initial_and_graph_retrievers(self):
        gs, vs = _stores()
        retriever = ChunkBasedSemanticSearch(gs, vs)
        assert len(retriever.initial_retrievers) >= 1
        assert len(retriever.graph_retrievers) >= 1

    def test_chunk_based_graph_search_returns_statement_ids(self):
        gs, vs = _stores()
        gs.execute_query.return_value = [{'l': 's1'}]
        retriever = ChunkBasedSemanticSearch(gs, vs)
        assert retriever.chunk_based_graph_search('c1') == ['s1']

    def test_get_start_node_ids_dedups_initial_retriever_outputs(self):
        gs, vs = _stores()
        initial1 = MagicMock()
        initial1.retrieve.return_value = [_chunk_node('c1'), _chunk_node('c2')]
        initial2 = MagicMock()
        # initial2 returns a duplicate c1; should be deduped.
        initial2.retrieve.return_value = [_chunk_node('c1'), _chunk_node('c3')]

        retriever = ChunkBasedSemanticSearch(gs, vs, retrievers=[initial1, initial2])
        # Force no graph retrievers — both got classified as initial because they
        # are bare MagicMocks (not SemanticChunkBeamGraphSearch instances).
        retriever.graph_retrievers = []

        result = retriever.get_start_node_ids(QueryBundle('q'))
        assert sorted(result) == ['c1', 'c2', 'c3']

    def test_get_start_node_ids_returns_empty_when_no_initial_nodes(self):
        gs, vs = _stores()
        initial = MagicMock()
        initial.retrieve.return_value = []
        retriever = ChunkBasedSemanticSearch(gs, vs, retrievers=[initial])
        retriever.graph_retrievers = []
        assert retriever.get_start_node_ids(QueryBundle('q')) == []

    def test_do_graph_search_aggregates_chunks_then_fetches_statements(self):
        gs, vs = _stores()
        retriever = ChunkBasedSemanticSearch(gs, vs)

        def _stmt_ids(_id):
            return ['s1']

        retriever.chunk_based_graph_search = _stmt_ids
        retriever.get_statements_by_topic_and_source = lambda ids: []
        result = retriever.do_graph_search(QueryBundle('q'), ['c1', 'c2'])
        assert result.results == []


class TestSemanticChunkBeamGraphSearch:
    def test_get_neighbors_returns_chunk_ids_from_second_query(self):
        gs, vs = _stores()
        gs.execute_query.side_effect = [
            [{'entityId': 'e1', 'count(r)': 5}],
            [{'chunkId': 'c2'}, {'chunkId': 'c3'}],
        ]
        beam = SemanticChunkBeamGraphSearch(vs, gs, beam_width=3, max_depth=2)
        result = beam.get_neighbors('c1')
        assert result == ['c2', 'c3']

    def test_beam_search_visits_initial_chunks_and_neighbours(self):
        gs, vs = _stores()
        cache = MagicMock()
        cache.get_embeddings.return_value = {
            'c1': np.array([1.0, 0.0]),
            'c2': np.array([0.9, 0.1]),
        }
        beam = SemanticChunkBeamGraphSearch(
            vs, gs, embedding_cache=cache, beam_width=2, max_depth=1,
        )
        beam.get_neighbors = MagicMock(return_value=['c2'])

        results = beam.beam_search(np.array([1.0, 0.0]), ['c1'])
        chunk_ids = [r[0] for r in results]
        assert 'c1' in chunk_ids

    def test_retrieve_uses_shared_nodes_when_provided(self):
        gs, vs = _stores()
        cache = MagicMock()
        cache.get_embeddings.return_value = {'c1': np.array([1.0, 0.0])}
        beam = SemanticChunkBeamGraphSearch(
            vs, gs, embedding_cache=cache, max_depth=1, beam_width=1,
            shared_nodes=[_chunk_node('c1')],
        )
        beam.get_neighbors = MagicMock(return_value=[])

        nodes = beam._retrieve(QueryBundle(query_str='q', embedding=[1.0, 0.0]))
        # Initial chunk c1 is excluded since it's in initial_ids set.
        assert nodes == []

    def test_retrieve_falls_back_to_vector_store_when_no_shared_nodes(self):
        gs, vs = _stores()
        chunk_index = MagicMock()
        chunk_index.top_k.return_value = [{'chunk': {'chunkId': 'c1'}}]
        vs.get_index.return_value = chunk_index
        cache = MagicMock()
        cache.get_embeddings.return_value = {'c1': np.array([1.0, 0.0])}
        beam = SemanticChunkBeamGraphSearch(
            vs, gs, embedding_cache=cache, max_depth=1, beam_width=1,
        )
        beam.get_neighbors = MagicMock(return_value=[])

        beam._retrieve(QueryBundle(query_str='q', embedding=[1.0, 0.0]))
        chunk_index.top_k.assert_called_once()

    def test_retrieve_returns_empty_when_no_chunks(self):
        gs, vs = _stores()
        chunk_index = MagicMock()
        chunk_index.top_k.return_value = []
        vs.get_index.return_value = chunk_index
        beam = SemanticChunkBeamGraphSearch(vs, gs)
        assert beam._retrieve(QueryBundle(query_str='q', embedding=[1.0, 0.0])) == []
