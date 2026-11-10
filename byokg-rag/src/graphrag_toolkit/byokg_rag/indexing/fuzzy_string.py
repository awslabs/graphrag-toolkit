# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import unicodedata
from collections import defaultdict
from itertools import groupby

from thefuzz import fuzz, process
from abc import ABC, abstractmethod
from typing import List
from .index import Index
from ..utils import can_name_an_entity


def _in_word(c: str) -> bool:
    # Like \w, plus the vowel signs and joiners that sit inside words in scripts such as Devanagari.
    return c.isalnum() or c == '_' or unicodedata.category(c)[0] == 'M' or c in '\u200c\u200d'


def _words(text: str) -> str:
    """text's words, space-joined, ignoring case and accents."""
    folded = ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c)).casefold()
    return ' '.join(''.join(chars) for in_word, chars in groupby(folded, _in_word) if in_word)


class FuzzyStringIndex(Index):
    """
    A class for fuzzy string matching and indexing.
    """

    def __init__(self, keep_verbatim_names: bool = False):
        """
        :param keep_verbatim_names: when the length filter in match() leaves an input
            with no candidate, as when an LLM lists several entities on one line, return
            the names written in it word for word instead (see _names_written_in). Off
            by default.
        """
        super().__init__()  # Ensure proper initialization of the base class.
        self.vocab = []
        self.keep_verbatim_names = keep_verbatim_names
        self._word_index = None  # (words of a name -> names, most words in a name); built on first use

    def reset(self):
        self.vocab = []
        self._word_index = None

    def query(self, input, topk=1, id_selector=None):
        """
        match a query to items in the index and return the topk results

        :param input: str the query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """

        if id_selector is not None:
            raise NotImplementedError(f"id_selector not implemented for FuzzyString")

        # string matching process from thefuzz library https://pypi.org/project/thefuzz/
        results = process.extract(input, self.vocab, limit=topk)

        return {'hits': [{'document_id': match_string,
                         'document': match_string,
                         'match_score': match_score}
                for match_string, match_score in results]}


    def match(self, inputs, topk=1, id_selector=None, max_len_difference=4):
        """
        match entity inputs to vocab

        :param input: list(str) of entities per query to match
        :param topk: number of items to return
        :param id_selector: a list of ids to retrieve the topk from i.e an allowlist
        :return:
        """

        if id_selector is not None:
            raise NotImplementedError(f"id_selector not implemented for {self.__class__.__name__}")

        results = []
        for input in inputs:
            # string matching process from thefuzz library https://pypi.org/project/thefuzz/
            intermediate_results = process.extract(input, self.vocab, limit=topk)
            #skip much shorter strings
            kept = [result for result in intermediate_results
                    if not len(result[0]) + max_len_difference < len(input)]
            if not kept and self.keep_verbatim_names:
                kept = self._names_written_in(input, topk)
            results.extend(kept)

        results = sorted(results, key=lambda x: x[1], reverse=True)

        return {'hits': [{'document_id': match_string,
                         'document': match_string,
                         'match_score': match_score}
                for match_string, match_score in results]}

    def add(self, vocab_list):
        """
        add vocab instances to the index

        :param vocab_list: list of vocab instances to add

        """
        # sorted() gives a hash-independent vocab order; process.extract breaks
        # equal-score ties by vocab position, so a stable order is what makes
        # matching reproducible.
        self.vocab = sorted(set(self.vocab) | set(vocab_list))
        self._word_index = None

    def _names_written_in(self, text, limit):
        """(name, score) for names whose words occur in text as a run of whole words,
        ignoring case and accents; score is the share of text's words the run covers
        (0-100). Longest run first, then the longer name, then the leftmost. A run inside
        a longer matching run is skipped ('Francisco' in 'Francisco Goya'), and so are
        names of one character or that can't name an entity alone ('1982', 'Jr.'). Of
        names with the same words, the spelling written in text comes first, and every
        run's first name comes before any run's second."""
        if self._word_index is None:
            by_words = defaultdict(list)
            for name in self.vocab:  # sorted, so equal names keep a stable order
                key = _words(name)
                if len(key) > 1 and can_name_an_entity(key):
                    by_words[key].append(name)
            # One assignment, so a concurrent caller never sees half an index.
            self._word_index = (dict(by_words), max((key.count(' ') + 1 for key in by_words), default=0))
        by_words, max_words = self._word_index
        words = _words(text).split()
        runs = [(i, j) for i in range(len(words)) for j in range(i + 1, min(len(words), i + max_words) + 1)
                if ' '.join(words[i:j]) in by_words]
        runs = [(i, j) for i, j in runs if not any(a <= i and j <= b and (a, b) != (i, j) for a, b in runs)]
        runs.sort(key=lambda run: (run[0] - run[1], -len(' '.join(words[run[0]:run[1]])), run[0]))
        folded_text = text.casefold()
        ranked = [(100 * (j - i) / len(words),
                   sorted(by_words[' '.join(words[i:j])],
                          key=lambda name: (name not in text, name.casefold() not in folded_text)))
                  for i, j in runs]
        found, seen = [], set()
        for depth in range(max((len(names) for _, names in ranked), default=0)):
            for score, names in ranked:
                if depth < len(names) and names[depth] not in seen:
                    seen.add(names[depth])
                    found.append((names[depth], score))
        return found[:limit]

    def add_with_ids(self, ids, vocab_list):
        raise NotImplementedError(f"add_with_ids not implemented for {self.__class__.__name__}")
