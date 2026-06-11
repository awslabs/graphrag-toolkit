# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/utils/vector_utils."""

from unittest.mock import MagicMock

from llama_index.core.schema import QueryBundle

from graphrag_toolkit.lexical_graph.retrieval.utils.vector_utils import (
    get_diverse_vss_elements,
    _resolve_source_ids,
    _topic_source_cache,
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


def _fake_graph_store(mapping):
    """Graph store whose query returns {topicId, sourceId} for requested ids."""
    gs = MagicMock()
    gs.node_id.side_effect = lambda x: x
    gs.execute_query.side_effect = lambda cypher, params: [
        {'topicId': t, 'sourceId': mapping[t]} for t in params['topicIds'] if t in mapping
    ]
    return gs


class TestSourceResolution:
    """Topic-index results lack a 'source' key; these cover the graph-based fallback."""

    def test_missing_source_without_graph_store_does_not_crash(self):
        # Regression: previously raised when an element had no 'source'.
        elements = [{'topic': {'topicId': 't1'}}]
        vs, _ = _vector_store(elements)
        result = get_diverse_vss_elements(
            'topic', QueryBundle('q'), vs,
            diversity_factor=2, vss_top_k=5, filter_config=None,
        )
        assert result == elements[:5]  # graceful: returns un-diversified slice

    def test_missing_source_resolved_via_graph_store(self):
        _topic_source_cache.clear()
        elements = [{'topic': {'topicId': 'tA'}}, {'topic': {'topicId': 'tB'}}]
        _resolve_source_ids(_fake_graph_store({'tA': 'sX', 'tB': 'sY'}), 'topic', elements)
        assert elements[0]['source'] == {'sourceId': 'sX'}
        assert elements[1]['source'] == {'sourceId': 'sY'}

    def test_existing_source_not_overwritten(self):
        _topic_source_cache.clear()
        elements = [{'topic': {'topicId': 'tC'}, 'source': {'sourceId': 'orig'}}]
        _resolve_source_ids(_fake_graph_store({'tC': 'other'}), 'topic', elements)
        assert elements[0]['source'] == {'sourceId': 'orig'}

    def test_resolution_is_cached(self):
        _topic_source_cache.clear()
        gs = _fake_graph_store({'tD': 'sD'})
        _resolve_source_ids(gs, 'topic', [{'topic': {'topicId': 'tD'}}])
        # Second call for the same id should hit the cache, not re-query the graph.
        _resolve_source_ids(gs, 'topic', [{'topic': {'topicId': 'tD'}}])
        assert gs.execute_query.call_count == 1
        assert _topic_source_cache['tD'] == 'sD'

    def test_metadata_source_used_without_graph_call(self):
        # If the element already carries source in its metadata, it is lifted directly and
        # the graph is NOT queried (the normal, correctly-built-index path).
        _topic_source_cache.clear()
        gs = _fake_graph_store({'tE': 'unused'})
        elements = [{'topic': {'topicId': 'tE'},
                     'metadata': {'source': {'sourceId': 'sFromMeta'}}}]
        _resolve_source_ids(gs, 'topic', elements)
        assert elements[0]['source'] == {'sourceId': 'sFromMeta'}
        gs.execute_query.assert_not_called()

    def test_cache_is_bounded(self):
        # The LRU cache caps its size and evicts the oldest entries.
        import graphrag_toolkit.lexical_graph.retrieval.utils.vector_utils as vu
        vu._topic_source_cache.clear()
        original = vu._TOPIC_SOURCE_CACHE_MAXSIZE
        vu._TOPIC_SOURCE_CACHE_MAXSIZE = 3
        try:
            for i in range(5):
                vu._cache_put(f't{i}', f's{i}')
            assert len(vu._topic_source_cache) == 3      # capped
            assert 't0' not in vu._topic_source_cache    # oldest evicted
            assert 't4' in vu._topic_source_cache        # newest retained
        finally:
            vu._TOPIC_SOURCE_CACHE_MAXSIZE = original
            vu._topic_source_cache.clear()
