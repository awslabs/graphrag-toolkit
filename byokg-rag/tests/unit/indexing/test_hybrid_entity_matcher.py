# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from graphrag_toolkit.byokg_rag.graph_retrievers import EntityLinker
from graphrag_toolkit.byokg_rag.indexing import FuzzyStringIndex
from graphrag_toolkit.byokg_rag.indexing.graph_embeddings import GraphEmbeddings
from graphrag_toolkit.byokg_rag.indexing.hybrid_entity_matcher import HybridEntityMatcher
from tests.unit.hashing_embedding import HashingEmbedding

NODES = ['Pat LaFontaine', 'Patrick Roy', 'New York Islanders', 'Bill Masterton Memorial Trophy',
         'Jordan', 'Michael Jordan', 'm.0abc']


class _StubFuzzy:
    """FuzzyStringIndex's surface: a sorted vocab and match() hits in a fixed order."""

    def __init__(self, vocab, ranked):
        self.vocab, self._ranked = sorted(vocab), ranked

    def match(self, inputs, topk=1, **kwargs):
        return {'hits': [{'document_id': n, 'document': n, 'match_score': 100 - i}
                         for i, n in enumerate(self._ranked[:topk])]}


class _FixedEmbedding(HashingEmbedding):
    """Embeds the mention 'jordan' next to 'Michael Jordan', far from 'Jordan'."""

    def _vector(self, text):
        v = [0.0] * self.dim
        v[0 if text.lower() in ('jordan', 'michael jordan') else 1] = 1.0
        if text == 'Jordan':
            v = [0.0] * self.dim
            v[2] = 1.0
        return v


class _RankedEmbedding(HashingEmbedding):
    """'the mention' is most similar to order[0], then order[1], ...; other texts score 0."""

    def __init__(self, order):
        super().__init__()
        self.order = list(order)

    def _vector(self, text):
        v = [0.0] * self.dim
        if text == 'the mention':
            v[0] = 1.0
        elif text in self.order:
            p = self.order.index(text)
            v[0], v[1 + p] = 1.0, p + 1.0  # cosine with the mention falls as p grows
        else:
            v[-1] = 1.0
        return v


def _fused(fuzzy_ranked, dense_ranked, topk, candidate_depth):
    nodes = sorted(set(fuzzy_ranked) | set(dense_ranked))
    emb = _RankedEmbedding(dense_ranked)
    table = GraphEmbeddings.build([(n, 'r', n) for n in nodes], emb).nodes
    m = HybridEntityMatcher(_StubFuzzy(nodes, fuzzy_ranked), table, emb, candidate_depth=candidate_depth)
    return [h['document_id'] for h in m.retrieve(['the mention'], topk=topk)['hits']]


def _matcher():
    emb = HashingEmbedding()
    table = GraphEmbeddings.build([(n, 'r', n) for n in NODES], emb).nodes
    return HybridEntityMatcher.from_nodes(NODES, emb, table)


class TestFusion:
    def test_hit_shape_matches_entity_matcher_contract(self):
        hits = _matcher().retrieve(['Pat LaFontaine'], topk=2)['hits']
        assert len(hits) == 2
        assert all(set(h) == {'document_id', 'document', 'match_score'} for h in hits)
        assert hits[0]['match_score'] >= hits[1]['match_score']

    def test_exact_match_overrides_both_matchers(self):
        # Both matchers rank 'Michael Jordan' first; only the override can put the
        # exact node 'Jordan' on top.
        fuzzy = _StubFuzzy(['Jordan', 'Michael Jordan'], ranked=['Michael Jordan', 'Jordan'])
        table = GraphEmbeddings.build([('Michael Jordan', 'r', 'Jordan')], _FixedEmbedding()).nodes
        m = HybridEntityMatcher(fuzzy, table, _FixedEmbedding())
        assert m._dense_candidates(['jordan'], 2)[0][0] == 'Michael Jordan', 'precondition: dense prefers the other node'
        assert [h['document_id'] for h in m.retrieve(['jordan'], topk=2)['hits']] == ['Jordan', 'Michael Jordan']

    def test_verbatim_match_beats_its_case_variant(self):
        nodes = ['MORGAN FREEMAN', 'Morgan Freeman']  # sorted: the upper-case one comes first
        emb = HashingEmbedding()
        m = HybridEntityMatcher.from_nodes(nodes, emb, GraphEmbeddings.build([(n, 'r', n) for n in nodes], emb).nodes)
        assert m.retrieve(['Morgan Freeman'], topk=1)['hits'][0]['document_id'] == 'Morgan Freeman'
        assert m.retrieve(['MORGAN FREEMAN'], topk=1)['hits'][0]['document_id'] == 'MORGAN FREEMAN'
        assert m.retrieve(['morgan freeman'], topk=1)['hits'][0]['document_id'] == 'MORGAN FREEMAN'

    def test_match_score_order_is_rank_order(self):
        fuzzy = _StubFuzzy(['Jordan', 'Michael Jordan'], ranked=['Michael Jordan', 'Jordan'])
        table = GraphEmbeddings.build([('Michael Jordan', 'r', 'Jordan')], _FixedEmbedding()).nodes
        hits = HybridEntityMatcher(fuzzy, table, _FixedEmbedding()).retrieve(['Jordan'], topk=2)['hits']
        scores = [h['match_score'] for h in hits]
        assert scores == sorted(scores, reverse=True) and scores[0] > scores[1]

    def test_disagreeing_matchers_tie_to_fuzzy_not_to_the_name(self):
        # Disjoint lists: both firsts score 1/61. Either way round, fuzzy's wins.
        assert _fused(['Zeta Trophy', 'f2', 'f3'], ['Alpha Cup', 'd2', 'd3'], topk=3, candidate_depth=3)[:2] == [
            'Zeta Trophy', 'Alpha Cup']
        assert _fused(['Alpha Cup', 'f2', 'f3'], ['Zeta Trophy', 'd2', 'd3'], topk=3, candidate_depth=3)[:2] == [
            'Alpha Cup', 'Zeta Trophy']

    def test_agreement_below_the_first_place_wins(self):
        # 'Both' is second for fuzzy and third for dense: 1/62 + 1/63 beats either first (1/61).
        fuzzy, dense = ['F1', 'Both', 'F3'], ['D1', 'D2', 'Both']
        assert _fused(fuzzy, dense, topk=1, candidate_depth=3) == ['Both']
        # Fusing only the top 1 of each never sees the agreement.
        assert _fused(fuzzy, dense, topk=1, candidate_depth=1) == ['F1']

    @pytest.mark.parametrize('depth', [0, -1, 2.5])
    def test_invalid_candidate_depth(self, depth):
        with pytest.raises(ValueError, match='candidate_depth must be a positive integer'):
            _fused(['a'], ['b'], topk=1, candidate_depth=depth)

    def test_dense_recovers_what_fuzzy_drops(self):
        # Fuzzy drops candidates more than 4 characters shorter than the mention, so a
        # long paraphrase gets nothing from it; shared words still link it densely.
        mention = 'the ice hockey player LaFontaine Pat'
        fuzzy = FuzzyStringIndex()
        fuzzy.add(NODES)
        assert 'Pat LaFontaine' not in [h['document_id'] for h in fuzzy.match([mention], topk=3)['hits']]
        assert _matcher().retrieve([mention], topk=1)['hits'][0]['document_id'] == 'Pat LaFontaine'

    def test_opaque_ids_never_come_from_dense(self):
        ids = [h['document_id'] for h in _matcher().retrieve(['Islanders'], topk=7)['hits']]
        assert 'New York Islanders' in ids
        # Only fuzzy can surface m.0abc, and it does not resemble the mention.
        assert ids[0] == 'New York Islanders'

    def test_topk_beyond_dense_index_size_is_safe(self):
        hits = _matcher().retrieve(['Pat LaFontaine'], topk=50)['hits']
        ids = [h['document_id'] for h in hits]
        assert len(ids) == len(set(ids)) and set(ids) <= set(NODES)

    def test_deterministic(self):
        m = _matcher()
        assert m.retrieve(['Pat'], topk=3) == m.retrieve(['Pat'], topk=3)


class TestInputs:
    def test_hits_stay_contiguous_per_input_in_order(self):
        hits = _matcher().retrieve(['Pat LaFontaine', 'Jordan'], topk=2)['hits']
        assert [h['document_id'] for h in hits][0] == 'Pat LaFontaine'
        assert [h['document_id'] for h in hits][2] == 'Jordan'

    def test_empty_queries(self):
        assert _matcher().retrieve([], topk=3) == {'hits': []}

    @pytest.mark.parametrize('bad', ['Pat', [['Pat']], [None]])
    def test_rejects_non_list_of_strings(self, bad):
        with pytest.raises(TypeError, match='list of mention strings'):
            _matcher().retrieve(bad, topk=1)


class TestThroughEntityLinker:
    def test_group_by_mention_keeps_attribution(self):
        linker = EntityLinker(_matcher(), topk=2)
        grouped = linker.link(['Pat LaFontaine', 'Jordan'], return_dict=False, group_by_mention=True)
        assert [g[0] for g in grouped] == ['Pat LaFontaine', 'Jordan']
        assert all(len(g) == 2 for g in grouped)

    def test_flat_path(self):
        linker = EntityLinker(_matcher(), topk=1)
        assert linker.link(['Pat LaFontaine', 'Jordan'], return_dict=False) == ['Pat LaFontaine', 'Jordan']
