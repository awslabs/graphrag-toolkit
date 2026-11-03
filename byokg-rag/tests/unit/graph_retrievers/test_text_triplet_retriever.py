# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import numpy as np
import pytest

from graphrag_toolkit.byokg_rag.graph_retrievers import TextTripletRetriever
from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
from graphrag_toolkit.byokg_rag.indexing.graph_embeddings import GraphEmbeddings, top_k_indices
from tests.unit.hashing_embedding import HashingEmbedding

TRIPLETS = [
    ('Pat LaFontaine', 'award received (1995)', 'Lester Patrick Trophy'),
    ('Pat LaFontaine', 'award received (1990)', 'Bill Masterton Memorial Trophy'),
    ('Pat LaFontaine', 'member of sports team (1983 - 1991)', 'New York Islanders'),
    ('New York Islanders', 'league', 'National Hockey League'),
    ('m.0abc', 'location.location.contains', 'm.0xyz'),
]


def _retriever(triplets=TRIPLETS, topk=10):
    emb = HashingEmbedding()
    return TextTripletRetriever(triplets, GraphEmbeddings.build(triplets, emb), emb, topk=topk), emb


class TestRanking:
    def test_date_in_relation_label_ranks_first(self):
        r, _ = _retriever()
        top = r.retrieve('Which award did Pat LaFontaine receive in 1990?', topk=2)
        assert top[0] == 'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy'
        assert top[1] == 'Pat LaFontaine -> award received (1995) -> Lester Patrick Trophy'

    def test_topk_limits_and_verbalizes_single_triplets(self):
        r, _ = _retriever(topk=3)
        out = r.retrieve('New York Islanders league')
        assert len(out) == 3 and all(s.count(' -> ') == 2 for s in out)
        assert out[0] == 'New York Islanders -> league -> National Hockey League'

    def test_source_nodes_are_ignored(self):
        r, _ = _retriever()
        q = 'Which award did Pat LaFontaine receive in 1990?'
        assert r.retrieve(q, source_nodes=['National Hockey League']) == r.retrieve(q)

    def test_opaque_ids_contribute_zero(self):
        r, _ = _retriever()
        # The opaque-only triplet still scores through its relation label alone.
        assert r.retrieve('location contains', topk=1) == ['m.0abc -> location.location.contains -> m.0xyz']
        scores = r._node_vectors.shape[0]
        assert r._heads[-1] == scores and r._tails[-1] == scores  # the appended zero slot

    def test_question_goes_through_embed_query(self):
        r, emb = _retriever()
        r.retrieve('Who won in 1990?')
        assert emb.query_calls == ['Who won in 1990?']

    def test_duplicate_triplets_counted_once(self):
        r, _ = _retriever(TRIPLETS + [TRIPLETS[1]])
        assert len(r.retrieve('award 1990', topk=10)) == len(TRIPLETS)

    def test_topk_zero_and_empty_graph(self):
        r, _ = _retriever()
        assert r.retrieve('anything', topk=0) == []
        emb = HashingEmbedding()
        g = GraphEmbeddings.build(TRIPLETS, emb)
        assert TextTripletRetriever([], g, emb).retrieve('anything') == []


class TestFailsLoud:
    def test_embeddings_from_another_graph(self):
        emb = HashingEmbedding()
        other = GraphEmbeddings.build(TRIPLETS[:1], emb)
        with pytest.raises(ValueError, match='built from a different graph'):
            TextTripletRetriever(TRIPLETS, other, emb)

    def test_query_dimension_mismatch(self):
        r, _ = _retriever()
        r.embedding = HashingEmbedding(dim=8)
        with pytest.raises(ValueError, match=r'shape \(8,\), expected \(1024,\)'):
            r.retrieve('award')

    def test_zero_query_vector(self):
        r, _ = _retriever()

        class Zero(HashingEmbedding):
            def embed_query(self, text):
                return [0.0] * self.dim
        r.embedding = Zero()
        with pytest.raises(ValueError, match='zero or non-finite'):
            r.retrieve('award')

    @pytest.mark.parametrize('topk', [0, -1, 2.5])
    def test_invalid_topk(self, topk):
        emb = HashingEmbedding()
        with pytest.raises(ValueError, match='topk must be a positive integer'):
            TextTripletRetriever(TRIPLETS, GraphEmbeddings.build(TRIPLETS, emb), emb, topk=topk)

    def test_long_question_truncated_for_the_model(self):
        r, emb = _retriever()
        r.retrieve('award ' * 1000)
        assert len(emb.query_calls[-1]) == 2048

    def test_malformed_triplet(self):
        emb = HashingEmbedding()
        with pytest.raises(ValueError, match=r'\(head, relation, tail\)'):
            TextTripletRetriever([('a', 'b')], GraphEmbeddings.build(TRIPLETS, emb), emb)


class TestFromGraphStore:
    def test_local_kg_store(self, tmp_path):
        graph = {}
        for h, rel, t in TRIPLETS:
            graph.setdefault(h, {}).setdefault(rel, {'triplets': []})['triplets'].append([h, rel, t])
        r = TextTripletRetriever.from_graph_store(LocalKGStore(graph), HashingEmbedding(),
                                                  cache_dir=str(tmp_path), topk=1)
        assert r.retrieve('Which award did Pat LaFontaine receive in 1990?') == [
            'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy']
        assert (tmp_path / 'nodes' / 'vectors.npy').exists()

    def test_single_use_iterator_is_materialized(self):
        class GeneratorStore:
            def get_triplets(self):
                return (t for t in TRIPLETS)
        r = TextTripletRetriever.from_graph_store(GeneratorStore(), HashingEmbedding(), topk=1)
        assert len(r._triplets) == len(TRIPLETS)
        assert r.retrieve('Which award did Pat LaFontaine receive in 1990?') != []

    def test_model_id_reaches_the_cache(self, tmp_path):
        class Store:
            def get_triplets(self):
                return list(TRIPLETS)
        TextTripletRetriever.from_graph_store(Store(), HashingEmbedding(), cache_dir=str(tmp_path),
                                              model_id='cohere.embed-english-v3')
        import json
        assert json.loads((tmp_path / 'nodes' / 'meta.json').read_text())['model_id'] == 'cohere.embed-english-v3'

    def test_store_without_get_triplets(self):
        class NoTriplets:
            pass
        with pytest.raises(TypeError, match='cannot enumerate its triplets'):
            TextTripletRetriever.from_graph_store(NoTriplets(), HashingEmbedding())


class TestTopKIndices:
    def test_best_first(self):
        assert top_k_indices(np.array([0.1, 0.9, 0.5]), 2).tolist() == [1, 2]

    def test_ties_at_cutoff_go_to_lower_index(self):
        scores = np.array([0.5, 0.9, 0.5, 0.5, 0.1], dtype=np.float32)
        assert top_k_indices(scores, 3).tolist() == [1, 0, 2]

    def test_k_larger_than_scores_and_non_positive(self):
        assert top_k_indices(np.array([0.2, 0.3]), 5).tolist() == [1, 0]
        assert top_k_indices(np.array([0.2]), 0).tolist() == []
