# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import math
import random

import pytest

from graphrag_toolkit.byokg_rag.indexing import BM25Index, tokenize


def _reference(docs, query, k1=1.5, b=0.75):
    """Okapi BM25 written out per document, to check the index against."""
    n, avgdl = len(docs), sum(len(d) for d in docs) / len(docs)
    scores = []
    for doc in docs:
        score = 0.0
        for term in set(query):
            df = sum(term in d for d in docs)
            tf = doc.count(term)
            if tf:
                idf = math.log(1 + (n - df + 0.5) / (df + 0.5))
                score += idf * tf * (k1 + 1) / (tf + k1 * (1 - b + b * len(doc) / avgdl))
        scores.append(score)
    return scores


class TestTokenize:
    def test_letter_and_digit_runs(self):
        assert tokenize('award received (1990)') == ['award', 'received', '1990']
        assert tokenize('people.person.place_of_birth') == ['people', 'person', 'place', 'of', 'birth']
        assert tokenize('Jaromír JÁGR') == ['jaromír', 'jágr']


class TestScoring:
    DOCS = [tokenize('Pat LaFontaine award received 1995 Lester Patrick Trophy'),
            tokenize('Pat LaFontaine award received 1990 Bill Masterton Memorial Trophy'),
            tokenize('New York Islanders league National Hockey League'),
            []]

    def test_best_first_and_only_matching_documents(self):
        index = BM25Index(self.DOCS)
        assert index.top(tokenize('award 1990'), 10) == [1, 0]
        assert index.top(['islanders'], 10) == [2]

    def test_matches_the_formula(self):
        rng = random.Random(7)
        vocab = [f'w{i}' for i in range(30)]
        docs = [[rng.choice(vocab) for _ in range(rng.randint(0, 12))] for _ in range(200)]
        index = BM25Index(docs)
        for _ in range(20):
            query = [rng.choice(vocab) for _ in range(rng.randint(1, 4))]
            expected = _reference(docs, query)
            order = sorted((i for i, s in enumerate(expected) if s > 0), key=lambda i: (-expected[i], i))
            got = index.top(query, 15)
            assert [round(expected[i], 4) for i in got] == [round(expected[i], 4) for i in order[:15]]

    def test_ties_go_to_the_lower_index(self):
        index = BM25Index([['x'], ['y'], ['x']])
        assert index.top(['x'], 2) == [0, 2]

    def test_unknown_repeated_and_empty_queries(self):
        index = BM25Index(self.DOCS)
        assert index.top(['nothing'], 3) == [] and index.top([], 3) == []
        assert index.top(['award', 'award'], 3) == index.top(['award'], 3)
        assert index.top(['award'], 0) == []

    def test_empty_corpus(self):
        assert BM25Index([]).top(['x'], 3) == []

    @pytest.mark.parametrize('k1, b', [(-0.1, 0.75), (1.5, 1.2), (1.5, -0.1)])
    def test_parameters_are_validated(self, k1, b):
        with pytest.raises(ValueError, match='k1'):
            BM25Index([['x']], k1=k1, b=b)
