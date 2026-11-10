# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Deterministic test embedding: bag of words hashed into the real (1024) dimension."""

import hashlib
import re

from graphrag_toolkit.byokg_rag.indexing.embedding import Embedding

DIM = 1024


def _tokens(text):
    return re.findall(r'[a-z0-9]+', text.lower())


class HashingEmbedding(Embedding):
    """Texts sharing words get similar vectors, so ranking tests are meaningful.

    Returns List[float] like the real wrappers, and records which method embedded
    what, so tests can check that questions go through embed_query.
    """

    def __init__(self, dim=DIM):
        self.dim = dim
        self.batch_calls = 0
        self.query_calls = []

    def _vector(self, text):
        vector = [0.0] * self.dim
        tokens = _tokens(text) or [text]  # keep token-free text from being a zero vector
        for token in tokens:
            vector[int(hashlib.md5(token.encode()).hexdigest(), 16) % self.dim] += 1.0
        return vector

    def embed(self, text_input):
        return self._vector(text_input)

    def batch_embed(self, text_inputs):
        self.batch_calls += 1
        return [self._vector(t) for t in text_inputs]

    def embed_query(self, text_input):
        self.query_calls.append(text_input)
        return self._vector(text_input)
