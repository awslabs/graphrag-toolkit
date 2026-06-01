# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for retrieval/utils/statement_utils."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest

from graphrag_toolkit.lexical_graph.retrieval.utils import statement_utils
from graphrag_toolkit.lexical_graph.storage.graph.graph_store import NodeId
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import (
    SharedEmbeddingCache,
    cosine_similarity,
    get_free_memory,
    get_statements_query,
    get_top_free_gpus,
    get_top_k,
)


class TestCosineSimilarity:
    def test_empty_embeddings_returns_empty(self):
        sims, ids = cosine_similarity([1.0, 0.0], {})
        assert isinstance(sims, np.ndarray)
        assert sims.size == 0
        assert ids == []

    def test_identical_vectors_score_one(self):
        emb = {'s1': [1.0, 0.0]}
        sims, ids = cosine_similarity([1.0, 0.0], emb)
        assert ids == ('s1',)
        assert pytest.approx(sims[0]) == 1.0

    def test_orthogonal_vectors_score_zero(self):
        emb = {'s1': [0.0, 1.0]}
        sims, _ = cosine_similarity([1.0, 0.0], emb)
        assert pytest.approx(sims[0]) == 0.0

    def test_opposite_vectors_score_minus_one(self):
        emb = {'s1': [-1.0, 0.0]}
        sims, _ = cosine_similarity([1.0, 0.0], emb)
        assert pytest.approx(sims[0]) == -1.0

    def test_multiple_statements_preserve_order(self):
        emb = {'a': [1.0, 0.0], 'b': [0.0, 1.0], 'c': [1.0, 1.0]}
        sims, ids = cosine_similarity([1.0, 0.0], emb)
        assert ids == ('a', 'b', 'c')
        assert pytest.approx(sims[0]) == 1.0
        assert pytest.approx(sims[1]) == 0.0
        assert pytest.approx(sims[2], rel=1e-3) == 1 / np.sqrt(2)


class TestGetTopK:
    def test_empty_embeddings_returns_empty_list(self):
        assert get_top_k([1.0, 0.0], {}, top_k=5) == []

    def test_returns_top_k_in_descending_order(self):
        emb = {
            'a': [1.0, 0.0],
            'b': [0.0, 1.0],
            'c': [1.0, 1.0],
        }
        result = get_top_k([1.0, 0.0], emb, top_k=2)
        assert len(result) == 2
        scores = [s for s, _ in result]
        assert scores == sorted(scores, reverse=True)
        assert result[0][1] == 'a'

    def test_top_k_caps_at_available_results(self):
        emb = {'a': [1.0, 0.0]}
        result = get_top_k([1.0, 0.0], emb, top_k=10)
        assert len(result) == 1


class TestGetStatementsQuery:
    def _store(self, returned_statements):
        store = MagicMock()
        store.node_id.side_effect = lambda s: NodeId('id', s, is_property_based=False)
        store.execute_query.return_value = returned_statements
        return store

    def test_returns_matches_in_input_order(self):
        statements = [
            {'result': {'statement': {'statementId': 's2'}, 'value': 'two'}},
            {'result': {'statement': {'statementId': 's1'}, 'value': 'one'}},
        ]
        store = self._store(statements)
        result = get_statements_query(store, ['s1', 's2'])
        assert [r['result']['value'] for r in result] == ['one', 'two']

    def test_drops_ids_with_no_match(self):
        statements = [
            {'result': {'statement': {'statementId': 's1'}, 'value': 'one'}},
        ]
        store = self._store(statements)
        result = get_statements_query(store, ['s1', 'missing'])
        assert len(result) == 1
        assert result[0]['result']['value'] == 'one'

    def test_passes_statement_ids_as_query_param(self):
        store = self._store([])
        get_statements_query(store, ['s1', 's2'])
        _, params = store.execute_query.call_args.args
        assert params == {'statement_ids': ['s1', 's2']}


class TestGetFreeMemory:
    def test_missing_pynvml_raises_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, 'pynvml', None)
        with pytest.raises(ImportError, match='pynvml package not found'):
            get_free_memory(0)

    def test_returns_free_memory_in_mb(self, monkeypatch):
        fake_pynvml = SimpleNamespace(
            nvmlInit=MagicMock(),
            nvmlDeviceGetHandleByIndex=MagicMock(return_value='handle'),
            nvmlDeviceGetMemoryInfo=MagicMock(
                return_value=SimpleNamespace(free=4 * 1024 * 1024)
            ),
        )
        monkeypatch.setitem(sys.modules, 'pynvml', fake_pynvml)
        assert get_free_memory(0) == 4
        fake_pynvml.nvmlDeviceGetHandleByIndex.assert_called_once_with(0)


class TestGetTopFreeGpus:
    def test_missing_torch_raises_import_error(self, monkeypatch):
        monkeypatch.setitem(sys.modules, 'torch', None)
        with pytest.raises(ImportError, match='torch package not found'):
            get_top_free_gpus()

    def test_returns_top_n_gpu_indices_by_free_memory(self, monkeypatch):
        fake_torch = SimpleNamespace(
            cuda=SimpleNamespace(device_count=lambda: 3),
        )
        monkeypatch.setitem(sys.modules, 'torch', fake_torch)
        free = {0: 100, 1: 500, 2: 250}
        monkeypatch.setattr(statement_utils, 'get_free_memory', lambda i: free[i])
        assert get_top_free_gpus(n=2) == [1, 2]


class TestSharedEmbeddingCache:
    def _vector_store(self, embeddings):
        index = MagicMock()
        index.get_embeddings.return_value = [
            {'statement': {'statementId': sid}, 'embedding': vec}
            for sid, vec in embeddings.items()
        ]
        vs = MagicMock()
        vs.get_index.return_value = index
        return vs, index

    def test_hits_cache_when_all_ids_present(self):
        vs, index = self._vector_store({})
        cache = SharedEmbeddingCache(vs)
        cache._cache = {'s1': np.array([1.0]), 's2': np.array([2.0])}
        result = cache.get_embeddings(['s1', 's2'])
        assert set(result) == {'s1', 's2'}
        index.get_embeddings.assert_not_called()

    def test_fetches_missing_and_populates_cache(self):
        vs, index = self._vector_store({'s2': [0.5, 0.5]})
        cache = SharedEmbeddingCache(vs)
        cache._cache = {'s1': np.array([1.0, 0.0])}
        result = cache.get_embeddings(['s1', 's2'])
        assert set(result) == {'s1', 's2'}
        index.get_embeddings.assert_called_once_with(['s2'])
        assert 's2' in cache._cache

    def test_returns_cached_on_fetch_failure(self):
        vs = MagicMock()
        cache = SharedEmbeddingCache(vs)
        cache._cache = {'s1': np.array([1.0])}

        def boom(_ids):
            raise RuntimeError('upstream down')

        cache._fetch_embeddings = boom
        result = cache.get_embeddings(['s1', 's2'])
        assert set(result) == {'s1'}

    def test_fetch_embeddings_shapes_vector_store_response(self):
        vs, _ = self._vector_store({'s1': [0.1, 0.2], 's2': [0.3, 0.4]})
        cache = SharedEmbeddingCache(vs)
        result = cache._fetch_embeddings.__wrapped__(cache, ['s1', 's2'])
        assert set(result) == {'s1', 's2'}
        assert isinstance(result['s1'], np.ndarray)
