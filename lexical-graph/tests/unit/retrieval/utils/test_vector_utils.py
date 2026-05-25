# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/utils/vector_utils."""

from unittest.mock import MagicMock

from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.utils.vector_utils import (
    get_diverse_vss_elements,
)


def _vector_store(elements):
    index = MagicMock()
    index.top_k.return_value = elements
    vs = MagicMock()
    vs.get_index.return_value = index
    return vs, index


class TestGetDiverseVssElements:
    def test_no_diversity_returns_top_k_directly(self):
        elements = [{'source': {'sourceId': 's1'}}]
        vs, index = _vector_store(elements)
        result = get_diverse_vss_elements(
            'chunk', QueryBundle('q'), vs,
            diversity_factor=0, vss_top_k=5, filter_config=None,
        )
        assert result == elements
        index.top_k.assert_called_once()
        assert index.top_k.call_args.kwargs['top_k'] == 5

    def test_negative_diversity_factor_returns_top_k_directly(self):
        vs, index = _vector_store([])
        get_diverse_vss_elements(
            'chunk', QueryBundle('q'), vs,
            diversity_factor=-1, vss_top_k=3, filter_config=None,
        )
        assert index.top_k.call_args.kwargs['top_k'] == 3

    def test_diversity_factor_expands_fetch_window(self):
        vs, index = _vector_store([])
        get_diverse_vss_elements(
            'chunk', QueryBundle('q'), vs,
            diversity_factor=4, vss_top_k=2, filter_config=None,
        )
        assert index.top_k.call_args.kwargs['top_k'] == 8

    def test_round_robins_across_sources(self):
        # Two sources, each with multiple chunks. Diversity should interleave them.
        elements = [
            {'source': {'sourceId': 's1'}, 'chunk': {'chunkId': 'c1'}},
            {'source': {'sourceId': 's1'}, 'chunk': {'chunkId': 'c2'}},
            {'source': {'sourceId': 's1'}, 'chunk': {'chunkId': 'c3'}},
            {'source': {'sourceId': 's2'}, 'chunk': {'chunkId': 'c4'}},
            {'source': {'sourceId': 's2'}, 'chunk': {'chunkId': 'c5'}},
        ]
        vs, _ = _vector_store(elements)
        result = get_diverse_vss_elements(
            'chunk', QueryBundle('q'), vs,
            diversity_factor=2, vss_top_k=4, filter_config=None,
        )
        assert len(result) == 4
        sources = [e['source']['sourceId'] for e in result]
        # First two picks must come from distinct sources, given two sources available.
        assert sources[0] != sources[1]

    def test_caps_at_vss_top_k(self):
        elements = [
            {'source': {'sourceId': f's{i}'}, 'chunk': {'chunkId': f'c{i}'}}
            for i in range(10)
        ]
        vs, _ = _vector_store(elements)
        result = get_diverse_vss_elements(
            'chunk', QueryBundle('q'), vs,
            diversity_factor=2, vss_top_k=3, filter_config=None,
        )
        assert len(result) == 3
