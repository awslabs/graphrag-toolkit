# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for FuzzyStringIndex.

This module tests fuzzy string matching functionality including
vocabulary management, exact matching, fuzzy matching, and topk retrieval.
"""

import os
import subprocess
import sys

import pytest
from graphrag_toolkit.byokg_rag.indexing.fuzzy_string import FuzzyStringIndex


class TestFuzzyStringIndexInitialization:
    """Tests for FuzzyStringIndex initialization."""
    
    def test_initialization_empty_vocab(self):
        """Verify index initializes with empty vocabulary."""
        index = FuzzyStringIndex()
        assert index.vocab == []
    
    def test_reset_clears_vocab(self):
        """Verify reset() clears the vocabulary."""
        index = FuzzyStringIndex()
        index.add(['item1', 'item2'])
        
        index.reset()
        
        assert index.vocab == []


class TestFuzzyStringIndexAdd:
    """Tests for adding vocabulary to the index."""
    
    def test_add_single_item(self):
        """Verify adding a single vocabulary item."""
        index = FuzzyStringIndex()
        index.add(['Amazon'])
        
        assert 'Amazon' in index.vocab
        assert len(index.vocab) == 1
    
    def test_add_multiple_items(self):
        """Verify adding multiple vocabulary items."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        assert len(index.vocab) == 3
        assert all(item in index.vocab for item in ['Amazon', 'Microsoft', 'Google'])
    
    def test_add_duplicate_items(self):
        """Verify duplicate items are deduplicated."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Amazon', 'Microsoft'])
        
        assert len(index.vocab) == 2
        assert index.vocab.count('Amazon') == 1
    
    def test_add_with_ids_not_implemented(self):
        """Verify add_with_ids raises NotImplementedError."""
        index = FuzzyStringIndex()

        with pytest.raises(NotImplementedError):
            index.add_with_ids(['id1'], ['Amazon'])

    def test_add_vocab_is_sorted(self):
        """Vocab must be in a stable (sorted) order, not set-iteration order.

        process.extract breaks equal-score ties by vocab position, so a
        hash-dependent order makes matching nondeterministic across runs.
        """
        index = FuzzyStringIndex()
        index.add(['Microsoft', 'Amazon', 'Google'])
        index.add(['Apple', 'Amazon'])

        assert index.vocab == sorted(index.vocab)
        assert index.vocab == ['Amazon', 'Apple', 'Google', 'Microsoft']


# Query that ties three candidates at score 100 (thefuzz lowercases before
# scoring, so the casings are indistinguishable). With topk < 3 the tie must
# be broken, which is exactly where hash-dependent vocab order used to leak.
_DETERMINISM_SNIPPET = """
from graphrag_toolkit.byokg_rag.indexing.fuzzy_string import FuzzyStringIndex
index = FuzzyStringIndex()
index.add(['Amazon', 'amazon', 'AMAZON', 'Google', 'Microsoft'])
hits = index.query('amazon', topk=2)['hits']
print('|'.join(h['document_id'] for h in hits))
"""


class TestFuzzyStringIndexDeterminism:
    """Regression tests for run-to-run reproducibility (nondeterminism fix)."""

    def _run_with_hashseed(self, seed):
        # Inherit the parent env (PATH/VIRTUAL_ENV/PYTHONPATH) so the child can
        # import the package; only override PYTHONHASHSEED. Replacing the whole
        # env breaks import resolution in CI.
        env = {**os.environ, 'PYTHONHASHSEED': str(seed)}
        out = subprocess.run(
            [sys.executable, '-c', _DETERMINISM_SNIPPET],
            capture_output=True, text=True, check=True,
            env=env,
        )
        return out.stdout.strip()

    def test_matching_is_deterministic_across_hash_seeds(self):
        """Same vocab + query + topk returns identical hits regardless of seed.

        Runs in subprocesses because PYTHONHASHSEED only takes effect at
        interpreter start; each seed shuffles set() iteration order differently.
        Pre-fix this returned different candidates per seed among the tie group.

        Note: the child imports the indexing package, which pulls faiss. That's
        incidental to what we assert (only thefuzz matters here).
        """
        results = {self._run_with_hashseed(seed) for seed in range(5)}

        assert len(results) == 1, f"nondeterministic hits across hash seeds: {results}"

    def test_scores_independent_of_insertion_order(self):
        """The fix only reorders vocab; per-candidate scores must not change.

        Two indexes built with the same items in different insertion order must
        return identical (document, score) pairs — pins the acceptance criterion
        that scores are unchanged, only ordering/tie-breaks are made stable.
        """
        vocab = ['Amazon', 'Amazonian', 'Amazing', 'Google', 'Meta']
        a = FuzzyStringIndex()
        a.add(vocab)
        b = FuzzyStringIndex()
        b.add(list(reversed(vocab)))

        hits_a = [(h['document_id'], h['match_score']) for h in a.query('Amazon', topk=5)['hits']]
        hits_b = [(h['document_id'], h['match_score']) for h in b.query('Amazon', topk=5)['hits']]

        assert hits_a == hits_b


class TestFuzzyStringIndexQuery:
    """Tests for querying the index."""
    
    def test_query_exact_match(self):
        """Verify exact string matching returns 100% match score."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        result = index.query('Amazon', topk=1)
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Amazon'
        assert result['hits'][0]['match_score'] == 100
    
    def test_query_fuzzy_match(self):
        """Verify fuzzy matching handles typos."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        result = index.query('Amazn', topk=1)  # Missing 'o'
        
        assert len(result['hits']) == 1
        assert result['hits'][0]['document'] == 'Amazon'
        assert result['hits'][0]['match_score'] > 80  # High but not perfect
    
    def test_query_topk_limiting(self):
        """Verify topk parameter limits results."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google', 'Apple', 'Meta'])
        
        result = index.query('Tech', topk=3)
        
        assert len(result['hits']) == 3
    
    def test_query_empty_vocab(self):
        """Verify querying empty index returns empty results."""
        index = FuzzyStringIndex()
        
        result = index.query('Amazon', topk=1)
        
        assert len(result['hits']) == 0
    
    def test_query_with_id_selector_not_implemented(self):
        """Verify id_selector parameter raises NotImplementedError."""
        index = FuzzyStringIndex()
        index.add(['Amazon'])
        
        with pytest.raises(NotImplementedError):
            index.query('Amazon', topk=1, id_selector=['id1'])


class TestFuzzyStringIndexMatch:
    """Tests for batch matching functionality."""
    
    def test_match_multiple_inputs(self):
        """Verify batch matching of multiple queries."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Microsoft', 'Google'])
        
        result = index.match(['Amazon', 'Google'], topk=1)
        
        assert len(result['hits']) == 2
        documents = [hit['document'] for hit in result['hits']]
        assert 'Amazon' in documents
        assert 'Google' in documents
    
    def test_match_length_filtering(self):
        """Verify max_len_difference filters short matches."""
        index = FuzzyStringIndex()
        index.add(['Amazon Web Services', 'AWS', 'Amazon'])
        
        # Query for long string, should filter out 'AWS' (too short)
        result = index.match(['Amazon Web Services'], topk=3, max_len_difference=4)
        
        documents = [hit['document'] for hit in result['hits']]
        assert 'AWS' not in documents  # Too short compared to query
    
    def test_match_sorted_by_score(self):
        """Verify results are sorted by match score descending."""
        index = FuzzyStringIndex()
        index.add(['Amazon', 'Amazonian', 'Amazing'])
        
        result = index.match(['Amazon'], topk=3)
        
        scores = [hit['match_score'] for hit in result['hits']]
        assert scores == sorted(scores, reverse=True)
    
    def test_match_with_id_selector_not_implemented(self):
        """Verify id_selector parameter raises NotImplementedError."""
        index = FuzzyStringIndex()
        index.add(['Amazon'])
        
        with pytest.raises(NotImplementedError):
            index.match(['Amazon'], topk=1, id_selector=['id1'])



class TestKeepVerbatimNames:
    """keep_verbatim_names: an input the length filter leaves empty links the names written in it."""

    LINE = 'Buffyverse, Australia, language'  # as an LLM wrote it on one line

    @staticmethod
    def _hits(vocab, mention, keep=True, topk=3):
        index = FuzzyStringIndex(keep_verbatim_names=keep)
        index.add(vocab)
        return [(hit['document_id'], hit['match_score']) for hit in index.match([mention], topk=topk)['hits']]

    def _match(self, *args, **kwargs):
        return [name for name, _ in self._hits(*args, **kwargs)]

    def test_off_by_default(self):
        assert FuzzyStringIndex().keep_verbatim_names is False
        assert self._match(['Australia', 'Buffyverse'], self.LINE, keep=False) == []

    def test_names_listed_on_one_line_are_linked(self):
        # Each covers a third of the line's words; equal runs go longer name first.
        assert self._hits(['Australia', 'Buffyverse', 'Language'], self.LINE) == [
            ('Buffyverse', pytest.approx(100 / 3)), ('Australia', pytest.approx(100 / 3)),
            ('Language', pytest.approx(100 / 3))]

    def test_a_mention_fuzzy_matching_links_is_unchanged(self):
        # Fuzzy keeps 'Francisco de Goya'; names written in the mention would give 'Francisco'.
        vocab = ['Francisco', 'Francisco de Goya']
        assert self._hits(vocab, 'Francisco Goya') == self._hits(vocab, 'Francisco Goya', keep=False)
        assert self._match(vocab, 'Francisco Goya') == ['Francisco de Goya']

    def test_the_longest_name_comes_first_and_names_inside_it_are_skipped(self):
        vocab = ['Boat', 'Galápagos Islands', 'Islands']
        assert self._hits(vocab, 'the galapagos islands boat tours') == [('Galápagos Islands', 40.0), ('Boat', 20.0)]
        assert self._match(vocab, 'the galapagos islands boat tours', topk=1) == ['Galápagos Islands']

    def test_longest_run_then_longer_name_then_leftmost(self):
        assert self._match(['Boat', 'Galápagos Islands'], 'Boat tours of the Galapagos Islands', topk=1) == [
            'Galápagos Islands']
        assert self._match(['Argentina', 'Peru'], 'Peru, Argentina, football rivalry', topk=1) == ['Argentina']
        assert self._match(['Cuba', 'Peru'], 'Peru, Cuba, trade routes', topk=1) == ['Peru']

    def test_a_name_inside_a_longer_one_is_skipped_and_repeats_count_once(self):
        assert self._match(['Francisco', 'Francisco Goya', 'Prado'], 'Francisco Goya paintings in the Prado') == [
            'Francisco Goya', 'Prado']
        assert self._match(['Chile', 'Peru'], 'Peru, Peru, Chile') == ['Chile', 'Peru']

    def test_the_spelling_written_comes_first_and_variants_dont_crowd_out_other_names(self):
        vocab = ['Lima', 'PARIS', 'Paris', 'Paris.']
        assert self._match(vocab, 'Paris, Lima, Peru') == ['Paris', 'Lima', 'PARIS']
        assert self._match(vocab, 'Paris, Lima, Peru', topk=2) == ['Paris', 'Lima']
        assert self._match(['Brazil', 'Sao Paulo', 'São Paulo'], 'São Paulo, Brazil, football', topk=1) == ['São Paulo']

    def test_names_that_cant_name_an_entity_alone_are_skipped(self):
        assert self._match(['1982', 'Peru', 'World Cup'], 'Peru, 1982, World Cup') == ['World Cup', 'Peru']
        assert self._match(['153', '489', 'Population'], 'population 9,153,489') == ['Population']
        assert self._match(['Jr.', 'Las Vegas', 'Sammy Davis'], 'Sammy Davis Jr. Las Vegas show') == [
            'Sammy Davis', 'Las Vegas']

    def test_a_name_not_written_in_the_input_is_not_linked(self):
        # Fuzzy-close to Australia, but not written in the line.
        assert self._match(['Austria'], self.LINE) == []

    def test_case_and_accents_are_ignored(self):
        assert self._match(['Galápagos Islands'], 'the GALAPAGOS islands boat tours') == ['Galápagos Islands']
        assert self._match(['Sao Paulo'], 'São Paulo, Brazil, football') == ['Sao Paulo']

    def test_whole_words_only(self):
        assert self._match(['Rome', 'Romeo'], 'Romeo and Juliet by Shakespeare') == ['Romeo']
        assert self._match(['Rome'], 'Jerome Kern, Broadway, musicals') == []

    def test_other_scripts_and_names_with_punctuation(self):
        assert self._match(['Москва'], 'Москва, Россия, столица страны') == ['Москва']
        # Vowel signs stay inside words: Rama is not a word of Ramayana.
        assert self._match(['राम'], 'रामायण, वाल्मीकि, संस्कृत') == []
        assert self._match(['राम'], 'राम, सीता, अयोध्या') == ['राम']
        assert self._match(['Washington, D.C.'], 'Washington, D.C. Peru Lima travel') == ['Washington, D.C.']

    def test_one_character_and_blank_names_are_skipped(self):
        assert self._match(['Girl', 'I'], 'Because I Got a Girl') == ['Girl']
        assert self._match([' ', '...'], self.LINE) == []

    def test_at_most_topk_names(self):
        assert self._match(['Australia', 'Buffyverse', 'Language'], self.LINE, topk=2) == ['Buffyverse', 'Australia']

    def test_batched_inputs_each_get_their_names(self):
        # The flat path EntityLinker takes without single_best_match.
        index = FuzzyStringIndex(keep_verbatim_names=True)
        index.add(['Australia', 'Buffyverse', 'Peru'])
        assert [h['document_id'] for h in index.match(['Peru', self.LINE], topk=1)['hits']] == ['Peru', 'Buffyverse']

    def test_names_added_later_are_found_and_reset_forgets_them(self):
        index = FuzzyStringIndex(keep_verbatim_names=True)
        index.add(['Australia'])
        assert [h['document_id'] for h in index.match([self.LINE], topk=3)['hits']] == ['Australia']
        index.add(['Buffyverse'])
        assert [h['document_id'] for h in index.match([self.LINE], topk=3)['hits']] == ['Buffyverse', 'Australia']
        index.reset()
        assert index.match([self.LINE], topk=3)['hits'] == []
