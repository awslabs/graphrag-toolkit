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

    def test_topk_zero(self):
        r, _ = _retriever()
        assert r.retrieve('anything', topk=0) == []

    def test_empty_graph_returns_nothing_without_embedding(self):
        emb = HashingEmbedding()
        r = TextTripletRetriever([], GraphEmbeddings.build([], emb), emb)
        assert r.retrieve('anything') == []
        assert (emb.batch_calls, emb.query_calls) == (0, [])

    def test_all_opaque_graph_ranks_by_relation_labels(self):
        # The match is listed second: an all-tied ranking would return the first.
        opaque = [('m.0c', 'film.film.directed_by', 'm.0d'), ('m.0a', 'people.person.place_of_birth', 'm.0b')]
        r, _ = _retriever(opaque)
        assert r.retrieve('place of birth', topk=1) == ['m.0a -> people.person.place_of_birth -> m.0b']


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

    def test_empty_store(self):
        emb = HashingEmbedding()
        assert TextTripletRetriever.from_graph_store(LocalKGStore({}), emb).retrieve('anything') == []
        assert (emb.batch_calls, emb.query_calls) == (0, [])

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


class _YearBlindEmbedding(HashingEmbedding):
    """Embeds like HashingEmbedding but ignores digits, as embeddings blur years."""

    def _vector(self, text):
        return super()._vector(''.join(c for c in text if not c.isdigit()) or text)


AWARDS = [('Pat LaFontaine', 'award received (1995)', 'Lester Patrick Trophy'),
          ('Mario Lemieux', 'award received (1993)', 'Hart Memorial Trophy'),
          ('Gord Kluzak', 'award received (1990)', 'Bill Masterton Memorial Trophy'),
          ('New York Islanders', 'league', 'National Hockey League')]


class TestLexical:
    def _pair(self, triplets=TRIPLETS, **kw):
        emb = _YearBlindEmbedding()
        table = GraphEmbeddings.build(triplets, emb)
        return (TextTripletRetriever(triplets, table, emb, **kw),
                TextTripletRetriever(triplets, table, emb, lexical=True, **kw))

    def test_an_exact_year_the_embeddings_cut_off_enters_the_top_k(self):
        dense, hybrid = self._pair(AWARDS)
        q = 'Who received an award in 1990?'
        target = 'Gord Kluzak -> award received (1990) -> Bill Masterton Memorial Trophy'
        assert target not in dense.retrieve(q, topk=2), 'precondition: the year-blind ranking cuts it'
        top = hybrid.retrieve(q, topk=2)
        assert target in top and top[0] == dense.retrieve(q, topk=1)[0], "the embedding ranking's first stays first"

    def test_off_by_default(self):
        dense, _ = self._pair()
        assert dense._lexical is None

    def test_question_words_alone_leave_the_embedding_order(self):
        dense, hybrid = self._pair()
        assert hybrid.retrieve('what is the', topk=5) == dense.retrieve('what is the', topk=5)

    def test_opaque_ids_are_not_searched(self):
        from graphrag_toolkit.byokg_rag.graph_retrievers.text_triplet_retriever import _triplet_words
        assert _triplet_words(('m.0abc', 'location.location.contains', 'm.0xyz')) == ['location', 'location', 'contains']

    def test_reciprocal_rank_fusion(self):
        from graphrag_toolkit.byokg_rag.graph_retrievers.text_triplet_retriever import _fuse
        # 2 is in both rankings; 7 only lexically, yet still reachable; ties go to the dense rank.
        assert _fuse([1, 2, 3], [2, 7], 4, 60) == [2, 1, 7, 3]
        assert _fuse([1, 2], [], 5, 60) == [1, 2]
        assert _fuse([], [4, 5], 1, 60) == [4]

    @pytest.mark.parametrize('kw, field', [({'candidate_depth': 0}, 'candidate_depth'), ({'rrf_k': 0}, 'rrf_k'),
                                           ({'candidate_depth': 2.5}, 'candidate_depth')])
    def test_settings_are_validated(self, kw, field):
        emb = HashingEmbedding()
        with pytest.raises(ValueError, match=field):
            TextTripletRetriever(TRIPLETS, GraphEmbeddings.build(TRIPLETS, emb), emb, **kw)

    def test_from_graph_store_passes_it_on(self, tmp_path):
        graph = {'Pat LaFontaine': {'award received (1990)': {'triplets': [
            ['Pat LaFontaine', 'award received (1990)', 'Bill Masterton Memorial Trophy']]}}}
        r = TextTripletRetriever.from_graph_store(LocalKGStore(graph), HashingEmbedding(), lexical=True)
        assert r._lexical is not None and r.retrieve('award 1990', topk=1) == [
            'Pat LaFontaine -> award received (1990) -> Bill Masterton Memorial Trophy']


class TestFusionContract:
    def test_question_words_never_reach_bm25(self):
        _, hybrid = TestLexical()._pair()
        searched = []
        top = hybrid._lexical.top
        hybrid._lexical.top = lambda words, k: searched.append(words) or top(words, k)
        hybrid.retrieve('Who is a member of the team?')
        assert searched == [['member', 'team']]

    def test_a_name_made_of_question_words_is_not_promoted(self):
        triplets = AWARDS + [('Who Did You Think I Was', 'music.album.release_type', 'Album')]
        dense, hybrid = TestLexical()._pair(triplets)
        q = 'Who did Gord Kluzak receive an award from in 1990?'
        assert 'Who Did You Think I Was -> music.album.release_type -> Album' not in hybrid.retrieve(q, topk=2)

    def test_ties_go_to_the_embedding_rank(self):
        from graphrag_toolkit.byokg_rag.graph_retrievers.text_triplet_retriever import _fuse
        assert _fuse([3, 4], [1, 2], 4, 60) == [3, 1, 4, 2]  # each first place ties; dense goes first
        assert _fuse([6, 5], [5, 6], 2, 60) == [6, 5]        # crossed ranks tie

    def test_rrf_k_sets_how_much_agreement_counts(self):
        from graphrag_toolkit.byokg_rag.graph_retrievers.text_triplet_retriever import _fuse
        dense = ['A'] + [f'x{i}' for i in range(8)] + ['B']   # B tenth
        lexical = [f'y{i}' for i in range(9)] + ['B']         # B tenth
        assert _fuse(dense, lexical, 1, 1) == ['A'] and _fuse(dense, lexical, 1, 60) == ['B']

    def test_retrieve_fuses_with_its_own_rrf_k(self, monkeypatch):
        import graphrag_toolkit.byokg_rag.graph_retrievers.text_triplet_retriever as ttr
        _, hybrid = TestLexical()._pair(rrf_k=5)
        seen = []
        real_fuse = ttr._fuse
        monkeypatch.setattr(ttr, '_fuse', lambda d, l, k, rrf_k: seen.append(rrf_k) or real_fuse(d, l, k, rrf_k))
        hybrid.retrieve('award 1990', topk=2)
        assert seen == [5]

    def test_each_ranking_is_cut_at_candidate_depth_not_topk(self, monkeypatch):
        import graphrag_toolkit.byokg_rag.graph_retrievers.text_triplet_retriever as ttr
        _, hybrid = TestLexical()._pair(candidate_depth=7)
        depths = []
        real_top = hybrid._lexical.top
        hybrid._lexical.top = lambda words, k: depths.append(('lexical', k)) or real_top(words, k)
        real_dense = ttr.top_k_indices
        monkeypatch.setattr(ttr, 'top_k_indices', lambda scores, k: depths.append(('dense', k)) or real_dense(scores, k))
        hybrid.retrieve('award 1990', topk=2)
        assert depths == [('dense', 7), ('lexical', 7)]
