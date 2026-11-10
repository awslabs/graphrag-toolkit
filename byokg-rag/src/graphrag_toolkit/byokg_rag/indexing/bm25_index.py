# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import re
from array import array
from typing import Iterable, List, Sequence

import numpy as np

from .graph_embeddings import top_k_indices

__all__ = ['BM25Index', 'tokenize', 'QUERY_STOPWORDS']

_TOKEN = re.compile(r'[^\W_]+')

# Frequent in questions and rare in graph facts, so BM25's idf would otherwise rank
# them above a question's content words. Applied to queries only.
QUERY_STOPWORDS = frozenset(
    'a an and are as at be been by did do does for from had has have how in is it its of '
    'on or that the their this to was were what when where which who whom whose why with'.split())


def tokenize(text: str) -> List[str]:
    """Lowercased runs of letters or digits: 'award received (1990)' -> ['award', 'received', '1990']."""
    return _TOKEN.findall(text.casefold())


class BM25Index:
    """Okapi BM25 over a fixed list of tokenized documents, in numpy.

    Postings are kept per term, so a query reads only the documents that contain one
    of its terms; a corpus of millions of short documents fits in a few hundred MB.
    """

    def __init__(self, documents: Iterable[Sequence[str]], k1: float = 1.5, b: float = 0.75):
        if k1 < 0 or not 0 <= b <= 1:
            raise ValueError(f'expected k1 >= 0 and 0 <= b <= 1, got k1={k1!r}, b={b!r}')
        vocab = {}
        terms, docs, lengths = array('i'), array('i'), array('i')
        for d, tokens in enumerate(documents):
            lengths.append(len(tokens))
            for token in tokens:
                terms.append(vocab.setdefault(token, len(vocab)))
                docs.append(d)
        self.n = len(lengths)
        self._vocab = vocab
        self._k1 = np.float32(k1)
        # One posting per (term, document), carrying the term's frequency there.
        width = max(self.n, 1)
        keys, tf = np.unique(np.frombuffer(terms, dtype=np.int32).astype(np.int64) * width
                             + np.frombuffer(docs, dtype=np.int32), return_counts=True)
        self._docs = (keys % width).astype(np.int32)
        self._tf = tf.astype(np.float32)
        self._start = np.searchsorted(keys // width, np.arange(len(vocab) + 1))
        df = np.diff(self._start).astype(np.float64)
        self._idf = np.log1p((self.n - df + 0.5) / (df + 0.5)).astype(np.float32)
        length = np.frombuffer(lengths, dtype=np.int32).astype(np.float32)
        avgdl = float(length.mean()) if self.n else 0.0
        self._norm = (k1 * (1 - b + b * length / avgdl) if avgdl else np.full(self.n, k1)).astype(np.float32)

    def top(self, query_tokens: Sequence[str], k: int) -> List[int]:
        """Up to k document indices with a positive score, best first; ties go to the lower index."""
        term_ids = sorted({self._vocab[t] for t in query_tokens if t in self._vocab})
        if not term_ids or k < 1:
            return []
        scores = np.zeros(self.n, dtype=np.float32)
        for term in term_ids:
            start, end = self._start[term], self._start[term + 1]
            docs, tf = self._docs[start:end], self._tf[start:end]  # docs are distinct within a term
            scores[docs] += self._idf[term] * tf * (self._k1 + 1) / (tf + self._norm[docs])
        candidates = np.flatnonzero(scores > 0)
        return [int(candidates[i]) for i in top_k_indices(scores[candidates], k)]
