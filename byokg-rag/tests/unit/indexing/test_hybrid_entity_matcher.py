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


class TestWithAConfiguredFuzzyIndex:
    def test_verbatim_names_rank_above_dense_only_candidates(self):
        emb = HashingEmbedding()
        nodes = ['Australia', 'Austria', 'Buffyverse']
        table = GraphEmbeddings.build([(n, 'r', n) for n in nodes], emb).nodes
        fuzzy = FuzzyStringIndex(keep_verbatim_names=True)
        fuzzy.add(nodes)
        matcher = HybridEntityMatcher(fuzzy, table, emb)
        hits = matcher.retrieve(['Buffyverse, Australia, language'], topk=3)['hits']
        # Dense returns all three; only the two the line names also come from fuzzy, and
        # a node only one matcher returns scores at most 1 / (rrf_k + 1).
        assert {h['document_id'] for h in hits[:2]} == {'Australia', 'Buffyverse'}
        assert all(h['match_score'] > 1 / (matcher.rrf_k + 1) for h in hits[:2])

    def test_a_name_written_twice_is_ranked_once(self):
        emb = HashingEmbedding()
        nodes = ['Chile', 'Peru']
        table = GraphEmbeddings.build([(n, 'r', n) for n in nodes], emb).nodes
        fuzzy = FuzzyStringIndex(keep_verbatim_names=True)
        fuzzy.add(nodes)
        hits = HybridEntityMatcher(fuzzy, table, emb).retrieve(['Peru, Peru, Chile'], topk=2)['hits']
        # Dense ranks Peru first and fuzzy ranks Chile first: a tie, which fuzzy breaks.
        assert [h['document_id'] for h in hits] == ['Chile', 'Peru']

    def test_an_empty_node_table_links_by_fuzzy_alone(self):
        emb = HashingEmbedding()
        table = GraphEmbeddings.build([('m.1', 'r', 'm.2')], emb).nodes  # opaque ids are not embedded
        assert table.texts == []
        fuzzy = FuzzyStringIndex()
        fuzzy.add(['m.1', 'm.2'])
        hits = HybridEntityMatcher(fuzzy, table, emb).retrieve(['m.1x'], topk=2)['hits']
        assert [h['document_id'] for h in hits] == [h['document_id'] for h in fuzzy.match(['m.1x'], topk=2)['hits']]

    def test_mention_embeddings_of_another_dimension_fail_loud(self):
        table = GraphEmbeddings.build([(n, 'r', n) for n in NODES], HashingEmbedding()).nodes
        with pytest.raises(ValueError, match='mention embeddings have dim 8, node table has 1024'):
            HybridEntityMatcher.from_nodes(NODES, HashingEmbedding(dim=8), table).retrieve(['Jordan'])


class _CountingEmbedding(HashingEmbedding):
    """Records the texts of every batch_embed call."""

    def __init__(self):
        super().__init__()
        self.embedded = []

    def batch_embed(self, text_inputs):
        self.embedded.append(list(text_inputs))
        return super().batch_embed(text_inputs)


class TestOneNodeSet:
    def test_a_node_the_fuzzy_index_lacks_is_never_a_dense_candidate(self):
        from graphrag_toolkit.byokg_rag.graphstore.graphstore import LocalKGStore
        graph = {'Pat LaFontaine': {'award received': {'triplets': [
            ['Pat LaFontaine', 'award received', 'Bill Masterton Memorial Trophy']]}}}
        store = LocalKGStore(graph)
        assert store.nodes() == ['Pat LaFontaine'], 'precondition: nodes() lists sources only'
        emb = HashingEmbedding()
        # The table embeds every named node, the trophy (a destination only) included.
        table = GraphEmbeddings.build(store.get_triplets(), emb, extra_nodes=store.nodes()).nodes
        assert 'Bill Masterton Memorial Trophy' in table.texts
        matcher = HybridEntityMatcher.from_nodes(store.nodes(), emb, table)
        assert matcher._dense_candidates(['Bill Masterton Memorial Trophy'], 5) == [['Pat LaFontaine']]
        ids = [h['document_id'] for h in matcher.retrieve(['Bill Masterton Memorial Trophy'], topk=3)['hits']]
        assert ids == ['Pat LaFontaine']

    def test_an_excluded_node_does_not_take_a_candidate_slot(self):
        # The trophy (a destination only) is dense's best match; the two cups it would
        # displace are what dense returns, still topk of them.
        cups = ['Masterton Cup', 'Masterton Memorial Cup']
        emb = HashingEmbedding()
        table = GraphEmbeddings.build([(c, 'awarded with', 'Bill Masterton Memorial Trophy') for c in cups], emb).nodes
        matcher = HybridEntityMatcher.from_nodes(cups, emb, table)
        assert matcher._dense_candidates(['Bill Masterton Memorial Trophy'], 2) == [
            ['Masterton Memorial Cup', 'Masterton Cup']]

    def test_once_the_fuzzy_index_has_the_node_dense_finds_it(self):
        nodes = ['Pat LaFontaine', 'Bill Masterton Memorial Trophy']
        emb = HashingEmbedding()
        table = GraphEmbeddings.build([(nodes[0], 'award received', nodes[1])], emb).nodes
        matcher = HybridEntityMatcher.from_nodes(nodes, emb, table)
        assert matcher._dense_candidates(['Masterton Trophy'], 1) == [['Bill Masterton Memorial Trophy']]


class TestArguments:
    def test_max_len_difference_reaches_the_fuzzy_index(self):
        seen = []

        class Recording(_StubFuzzy):
            def match(self, inputs, topk=1, **kwargs):
                seen.append(kwargs)
                return super().match(inputs, topk=topk)
        emb = HashingEmbedding()
        table = GraphEmbeddings.build([(n, 'r', n) for n in NODES], emb).nodes
        matcher = HybridEntityMatcher(Recording(NODES, NODES), table, emb)
        matcher.retrieve(['Pat'], topk=1)
        matcher.retrieve(['Pat'], topk=1, max_len_difference=10)
        assert seen == [{}, {'max_len_difference': 10}], "the fuzzy index's own default when unset"

    def test_max_len_difference_changes_what_fuzzy_returns(self):
        # 'Jordan' is 8 characters shorter than the mention: dropped at the default 4.
        fuzzy = FuzzyStringIndex()
        fuzzy.add(['Jordan'])
        emb = HashingEmbedding()
        table = GraphEmbeddings.build([('Pat LaFontaine', 'r', 'Patrick Roy')], emb).nodes
        matcher = HybridEntityMatcher(fuzzy, table, emb)
        assert matcher.retrieve(['jordan river'], topk=1)['hits'] == []
        assert matcher.retrieve(['jordan river'], topk=1, max_len_difference=10)['hits'][0]['document_id'] == 'Jordan'

    def test_id_selector_is_refused_like_the_fuzzy_index(self):
        with pytest.raises(NotImplementedError, match='id_selector not implemented for HybridEntityMatcher'):
            _matcher().retrieve(['Pat'], topk=1, id_selector=['Pat LaFontaine'])

    def test_unknown_arguments_fail_loud(self):
        with pytest.raises(TypeError, match=r"unexpected arguments \['top_k'\]"):
            _matcher().retrieve(['Pat'], top_k=1)

    def test_the_fuzzy_index_is_exposed_as_index(self):
        m = _matcher()
        assert m.index is m.fuzzy_index

    @pytest.mark.parametrize('rrf_k', [0, -5, True, '60'])
    def test_invalid_rrf_k(self, rrf_k):
        with pytest.raises(ValueError, match='rrf_k must be a positive number'):
            HybridEntityMatcher(_StubFuzzy(['a'], ['a']), GraphEmbeddings.build([('a', 'r', 'a')], HashingEmbedding()).nodes,
                                HashingEmbedding(), rrf_k=rrf_k)

    @pytest.mark.parametrize('size', [-1, 2.5, True])
    def test_invalid_cache_size(self, size):
        with pytest.raises(ValueError, match='cache_size must be a non-negative integer'):
            HybridEntityMatcher(_StubFuzzy(['a'], ['a']), GraphEmbeddings.build([('a', 'r', 'a')], HashingEmbedding()).nodes,
                                HashingEmbedding(), cache_size=size)


class TestMentionCache:
    def _matcher(self, cache_size=10_000):
        emb = _CountingEmbedding()
        table = GraphEmbeddings.build([(n, 'r', n) for n in NODES], HashingEmbedding()).nodes
        return HybridEntityMatcher.from_nodes(NODES, emb, table, cache_size=cache_size), emb

    def test_a_repeated_mention_is_embedded_once(self):
        matcher, emb = self._matcher()
        first = matcher.retrieve(['Pat', 'Jordan'], topk=2)
        assert matcher.retrieve(['Jordan', 'Pat'], topk=2)['hits'] == first['hits'][2:] + first['hits'][:2]
        matcher.retrieve(['Pat', 'Islanders', 'Pat'], topk=1)
        matcher.retrieve(['Roy', 'Roy'], topk=1)
        assert emb.embedded == [['Pat', 'Jordan'], ['Islanders'], ['Roy']], 'only new mentions, each once'

    def test_cached_and_fresh_results_agree(self):
        cached, _ = self._matcher()
        uncached, _ = self._matcher(cache_size=0)
        for mentions in (['Pat'], ['Pat', 'Roy'], ['Roy', 'Pat']):
            assert cached.retrieve(mentions, topk=3) == uncached.retrieve(mentions, topk=3)

    def test_the_least_recently_used_mention_is_evicted(self):
        matcher, emb = self._matcher(cache_size=2)
        for mentions in (['a1'], ['a2'], ['a1'], ['a3'], ['a1'], ['a2']):
            matcher.retrieve(mentions, topk=1)
        # a2 was evicted by a3 (a1 had been used since); a1 stayed.
        assert emb.embedded == [['a1'], ['a2'], ['a3'], ['a2']]
        assert list(matcher._cache) == ['a1', 'a2']

    def test_through_entity_linker_per_mention_calls(self):
        matcher, emb = self._matcher()
        linker = EntityLinker(matcher, topk=2)
        for _ in range(3):  # three iterations of the single_best_match path
            linker.link(['Pat LaFontaine', 'Jordan'], return_dict=False, group_by_mention=True)
        assert sorted(t for call in emb.embedded for t in call) == ['Jordan', 'Pat LaFontaine']
