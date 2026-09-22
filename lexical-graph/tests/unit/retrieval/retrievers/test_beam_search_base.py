# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for BeamSearch, the graph traversal base class."""

import numpy as np

from graphrag_toolkit.lexical_graph.retrieval.retrievers.beam_search_base import BeamSearch


class _FakeBeamSearch(BeamSearch):
    """A minimal BeamSearch over an in-memory graph, for testing the base class alone."""

    def __init__(self, graph, embeddings, **kwargs):
        super().__init__(**kwargs)
        self.graph = graph
        self.embeddings = embeddings

    def get_neighbors(self, node_id):
        return self.graph.get(node_id, [])

    def get_neighbors_batch(self, node_ids):
        return {nid: self.graph.get(nid, []) for nid in node_ids}

    def _get_embeddings(self, ids):
        return {i: self.embeddings[i] for i in ids if i in self.embeddings}

    def _get_top_k(self, query_embedding, embeddings, top_k):
        return self._score_neighbors(query_embedding, None, embeddings, top_k)


def _diamond_graph():
    # A and B both seed the search and both link to the same neighbor C,
    # so C is reachable from two beam members in the same expansion round.
    graph = {"A": ["C"], "B": ["C"], "C": []}
    embeddings = {
        "A": [1.0, 0.0],
        "B": [0.9, 0.1],
        "C": [0.5, 0.5],
    }
    return graph, embeddings


def test_beam_search_does_not_duplicate_a_node_reached_from_two_parents_cosine():
    graph, embeddings = _diamond_graph()
    bs = _FakeBeamSearch(graph, embeddings, beam_width=10, max_depth=3, scoring_mode='cosine')
    results = bs.beam_search(np.array([1.0, 0.0]), ["A", "B"])
    ids = [node_id for node_id, _path in results]
    assert len(ids) == len(set(ids))
    assert ids.count("C") == 1


def test_beam_search_does_not_duplicate_a_node_reached_from_two_parents_path_weighted():
    graph, embeddings = _diamond_graph()
    bs = _FakeBeamSearch(graph, embeddings, beam_width=10, max_depth=3, scoring_mode='path_weighted')
    results = bs.beam_search(np.array([1.0, 0.0]), ["A", "B"])
    ids = [node_id for node_id, _path in results]
    assert len(ids) == len(set(ids))
    assert ids.count("C") == 1
