# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import numpy as np
import pytest

from graphrag_toolkit.byokg_rag.graph_retrievers import ContextSelector


class OverlapReranker:
    """LocalGReranker's return shape: (reranked lines, np.argsort indices), scored here
    by how many question words a line contains."""

    def __init__(self):
        self.calls = 0

    def rerank_input_with_query(self, query, input, topk=None, return_scores=False):
        self.calls += 1
        words = set(query.lower().split())
        scores = np.array([-sum(w in line.lower() for w in words) for line in input])
        ids = np.argsort(scores, kind='stable')[:topk]
        return [input[i] for i in ids], ids


CHAIN = [
    'Ethiopia -> capital -> Addis Ababa',
    'China -> location.statistical_region.places_exported_to -> m.04cp1q4 | m.0x9',
    'm.04cp1q4 -> location.imports_and_exports.exported_to -> Djibouti',
    'm.04cp1q4 -> location.imports_and_exports.currency -> m.0kz1h',
    'm.0kz1h -> finance.currency.name -> Djiboutian franc',
    'Mars -> orbits -> Sun',
]


class TestSelection:
    def test_keeps_the_top_lines_best_first(self):
        lines = ['a -> r -> x', 'china exports -> r -> y', 'china -> r -> z']
        assert ContextSelector(OverlapReranker(), max_lines=2).select('china exports', lines) == [
            'china exports -> r -> y', 'china -> r -> z']

    def test_chain_lines_follow_the_line_that_led_to_them(self):
        kept = ContextSelector(OverlapReranker(), max_lines=1, max_chain_lines=10).select(
            'china places exported to', CHAIN)
        # The top line names m.04cp1q4; both of its facts come with it, then m.0kz1h's.
        assert kept == [CHAIN[1], CHAIN[2], CHAIN[3], CHAIN[4]]

    def test_chain_depth_limits_the_hops_followed(self):
        assert ContextSelector(OverlapReranker(), max_lines=1, chain_depth=1, max_chain_lines=10).select(
            'china places exported to', CHAIN) == [CHAIN[1], CHAIN[2], CHAIN[3]]
        assert ContextSelector(OverlapReranker(), max_lines=1, chain_depth=0).select(
            'china places exported to', CHAIN) == [CHAIN[1]]

    def test_a_line_reached_again_by_a_shorter_chain_has_its_chains_followed(self):
        lines = ['alpha first -> r -> m.1', 'm.1 -> r -> m.2', 'm.2 -> r -> m.3', 'm.3 -> r -> Leaf',
                 'beta first -> r -> m.2']
        # alpha reaches m.2's line at depth 2 (too deep to go on); beta reaches it at depth 1.
        kept = ContextSelector(OverlapReranker(), max_lines=2, chain_depth=2, max_chain_lines=10).select(
            'alpha beta first', lines)
        assert 'm.3 -> r -> Leaf' in kept and len(kept) == len(set(kept)) == 5

    def test_a_chain_shared_by_two_kept_lines_is_kept_once(self):
        lines = ['x first -> r -> m.1', 'y first -> r -> m.1', 'm.1 -> r -> Z']
        assert ContextSelector(OverlapReranker(), max_lines=2).select('x y first', lines) == [
            'x first -> r -> m.1', 'm.1 -> r -> Z', 'y first -> r -> m.1']

    def test_result_is_a_reordered_subset_never_larger(self):
        kept = ContextSelector(OverlapReranker(), max_lines=3).select('china exported', CHAIN + CHAIN[:2])
        assert len(kept) == len(set(kept)) <= len(CHAIN) and set(kept) <= set(CHAIN)

    def test_short_context_is_reordered_not_pruned(self):
        kept = ContextSelector(OverlapReranker(), max_lines=50).select('sun', CHAIN)
        assert kept[0] == 'Mars -> orbits -> Sun' and sorted(kept) == sorted(CHAIN)

    def test_named_entities_are_not_followed_as_chains(self):
        # 'Djibouti' heads no line here, and 'China' is a name, not an opaque id.
        lines = ['Ethiopia -> trades_with -> China', 'China -> capital -> Beijing']
        assert ContextSelector(OverlapReranker(), max_lines=1).select('ethiopia trades', lines) == [lines[0]]

    def test_empty_context_skips_the_reranker(self):
        reranker = OverlapReranker()
        assert ContextSelector(reranker).select('anything', []) == [] and reranker.calls == 0


class TestChainBudget:
    HUB = ['china first -> r -> m.hub'] + [f'm.hub -> r{i} -> Leaf{i}' for i in range(600)]

    def test_a_hub_cannot_bring_back_the_whole_context(self):
        # One top line pointing at a hub with 600 lines: chains default to max_lines more.
        kept = ContextSelector(OverlapReranker(), max_lines=1).select('china first', self.HUB)
        assert kept == self.HUB[:2]

    @pytest.mark.parametrize('max_lines, max_chain_lines', [(1, 0), (5, 3), (20, 40)])
    def test_output_never_exceeds_both_budgets(self, max_lines, max_chain_lines):
        kept = ContextSelector(OverlapReranker(), max_lines=max_lines,
                               max_chain_lines=max_chain_lines).select('china first', self.HUB)
        assert len(kept) <= max_lines + max_chain_lines

    def test_no_chain_budget_keeps_only_the_top_lines(self):
        assert ContextSelector(OverlapReranker(), max_lines=1, max_chain_lines=0).select(
            'china places exported to', CHAIN) == [CHAIN[1]]

    def test_a_capped_chain_keeps_its_best_ranked_lines(self):
        lines = ['china first -> r -> m.1', 'm.1 -> r -> Leaf', 'm.1 -> r -> china Leaf']
        assert ContextSelector(OverlapReranker(), max_lines=1, max_chain_lines=1).select(
            'china first', lines) == [lines[0], lines[2]]


class TestChainDirections:
    def test_the_line_leading_to_a_kept_id_is_followed_back(self):
        # m.04cp1q4's fact ranks first; the line joining it to China comes with it.
        kept = ContextSelector(OverlapReranker(), max_lines=1, chain_depth=1).select('djibouti', CHAIN)
        assert kept[:2] == [CHAIN[2], CHAIN[1]]

    def test_back_then_forward_reaches_the_siblings(self):
        kept = ContextSelector(OverlapReranker(), max_lines=1, chain_depth=2, max_chain_lines=10).select(
            'djibouti', CHAIN)
        # m.0kz1h's own fact is a third hop, past chain_depth.
        assert kept == [CHAIN[2], CHAIN[1], CHAIN[3]]

    def test_pipe_verbalized_lines_are_chained(self):
        lines = ['china first | r | m.1', 'm.1 | r | Leaf', 'Mars | orbits | Sun']
        assert ContextSelector(OverlapReranker(), max_lines=1).select('china first', lines) == lines[:2]


class TestFailsLoud:
    @pytest.mark.parametrize('max_lines', [0, -1, 2.5])
    def test_invalid_max_lines(self, max_lines):
        with pytest.raises(ValueError, match='max_lines must be a positive integer'):
            ContextSelector(OverlapReranker(), max_lines=max_lines)

    @pytest.mark.parametrize('depth', [-1, 1.5])
    def test_invalid_chain_depth(self, depth):
        with pytest.raises(ValueError, match='chain_depth must be a non-negative integer'):
            ContextSelector(OverlapReranker(), chain_depth=depth)

    @pytest.mark.parametrize('cap', [-1, 1.5, '3'])
    def test_invalid_max_chain_lines(self, cap):
        with pytest.raises(ValueError, match='max_chain_lines must be a non-negative integer'):
            ContextSelector(OverlapReranker(), max_chain_lines=cap)

    def test_a_reranker_that_drops_lines_is_rejected(self):
        class Truncating(OverlapReranker):
            def rerank_input_with_query(self, query, input, topk=None, return_scores=False):
                return input[:1], np.array([0])
        with pytest.raises(ValueError, match='expected a ranking of every line'):
            ContextSelector(Truncating()).select('q', ['a -> r -> b', 'c -> r -> d'])
