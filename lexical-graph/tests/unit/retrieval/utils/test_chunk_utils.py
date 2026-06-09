# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/utils/chunk_utils."""

from unittest.mock import MagicMock

import numpy as np
import pytest

from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.retrieval.utils.chunk_utils import (
    SharedChunkEmbeddingCache,
    cosine_similarity,
    get_chunks_query,
    get_top_k,
)


class TestCosineSimilarity:
    def test_empty_embeddings_returns_empty(self):
        sims, ids = cosine_similarity([1.0, 0.0], {})
        assert sims.size == 0
        assert ids == []

    def test_identical_vectors_score_one(self):
        sims, ids = cosine_similarity([1.0, 0.0], {'c1': [1.0, 0.0]})
        assert ids == ('c1',)
        assert pytest.approx(sims[0]) == 1.0

    def test_orthogonal_vectors_score_zero(self):
        sims, _ = cosine_similarity([1.0, 0.0], {'c1': [0.0, 1.0]})
        assert pytest.approx(sims[0]) == 0.0


class TestGetTopK:
    def test_empty_returns_empty_list(self):
        assert get_top_k([1.0, 0.0], {}, top_k=3) == []

    def test_descending_score_order(self):
        emb = {'a': [1.0, 0.0], 'b': [0.0, 1.0], 'c': [1.0, 1.0]}
        result = get_top_k([1.0, 0.0], emb, top_k=3)
        scores = [s for s, _ in result]
        assert scores == sorted(scores, reverse=True)
        assert result[0][1] == 'a'

    def test_caps_at_available_results(self):
        result = get_top_k([1.0, 0.0], {'a': [1.0, 0.0]}, top_k=99)
        assert len(result) == 1


class TestGetChunksQuery:
    def _store(self, returned):
        store = MagicMock()
        store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
        store.execute_query.return_value = returned
        return store

    def test_returns_matches_in_input_order(self):
        store = self._store([
            {'result': {'chunk': {'chunkId': 'c2'}, 'value': 'two'}},
            {'result': {'chunk': {'chunkId': 'c1'}, 'value': 'one'}},
        ])
        result = get_chunks_query(store, ['c1', 'c2'])
        assert [r['result']['value'] for r in result] == ['one', 'two']

    def test_drops_ids_with_no_match(self):
        store = self._store([
            {'result': {'chunk': {'chunkId': 'c1'}, 'value': 'one'}},
        ])
        result = get_chunks_query(store, ['c1', 'missing'])
        assert len(result) == 1

    def test_passes_chunk_ids_as_query_param(self):
        store = self._store([])
        get_chunks_query(store, ['c1', 'c2'])
        _, params = store.execute_query.call_args.args
        assert params == {'chunk_ids': ['c1', 'c2']}


class TestSharedChunkEmbeddingCache:
    def _vs(self, embeddings):
        index = MagicMock()
        index.get_embeddings.return_value = [
            {'chunk': {'chunkId': cid}, 'embedding': vec}
            for cid, vec in embeddings.items()
        ]
        vs = MagicMock()
        vs.get_index.return_value = index
        return vs, index

    def test_hits_cache_when_all_ids_present(self):
        vs, index = self._vs({})
        cache = SharedChunkEmbeddingCache(vs)
        cache._cache = {'c1': np.array([1.0]), 'c2': np.array([2.0])}
        result = cache.get_embeddings(['c1', 'c2'])
        assert set(result) == {'c1', 'c2'}
        index.get_embeddings.assert_not_called()

    def test_fetches_missing_and_populates_cache(self):
        vs, index = self._vs({'c2': [0.5, 0.5]})
        cache = SharedChunkEmbeddingCache(vs)
        cache._cache = {'c1': np.array([1.0, 0.0])}
        result = cache.get_embeddings(['c1', 'c2'])
        assert set(result) == {'c1', 'c2'}
        index.get_embeddings.assert_called_once_with(['c2'])
        assert 'c2' in cache._cache

    def test_returns_cached_on_fetch_failure(self):
        cache = SharedChunkEmbeddingCache(MagicMock())
        cache._cache = {'c1': np.array([1.0])}
        cache._fetch_embeddings = lambda _ids: (_ for _ in ()).throw(RuntimeError('down'))
        result = cache.get_embeddings(['c1', 'c2'])
        assert set(result) == {'c1'}

    def test_fetch_embeddings_shapes_response(self):
        vs, _ = self._vs({'c1': [0.1], 'c2': [0.2]})
        cache = SharedChunkEmbeddingCache(vs)
        result = cache._fetch_embeddings.__wrapped__(cache, ['c1', 'c2'])
        assert set(result) == {'c1', 'c2'}
        assert isinstance(result['c1'], np.ndarray)
