# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Sequence, Tuple

import numpy as np

from .graph_retrievers import GRetriever
from .graph_verbalizer import TripletGVerbalizer
from ..indexing.bm25_index import BM25Index, QUERY_STOPWORDS, tokenize
from ..indexing.graph_embeddings import (
    GraphEmbeddings, is_opaque_id, top_k_indices, truncate_for_embedding,
)

__all__ = ['TextTripletRetriever']


class TextTripletRetriever(GRetriever):
    """Retrieves the triplets most similar to the question, independent of linked seeds.

    A triplet's score is cos(q, head) + cos(q, relation) + cos(q, tail), from
    precomputed node and relation embeddings rather than one embedding per triplet.
    This reaches facts that traversal from linked entities misses — e.g. dates
    folded into relation labels such as 'award received (1952)'. Opaque ids are not
    embedded and contribute 0.

    With lexical=True, BM25 over each triplet's words (opaque ids left out) runs
    alongside, and the two rankings are fused by reciprocal rank: score = sum of
    1 / (rrf_k + rank) over the rankings that hold the triplet, each cut at
    candidate_depth (at least topk), ties going to the better embedding rank. Exact
    names and years that embeddings blur then still rank. Question words are not
    searched.
    """

    def __init__(self, triplets: Sequence[Sequence[str]], graph_embeddings: GraphEmbeddings,
                 embedding, graph_verbalizer=None, topk: int = 10, lexical: bool = False,
                 candidate_depth: int = 50, rrf_k: int = 60):
        if not isinstance(topk, int) or topk < 1:
            raise ValueError(f'topk must be a positive integer, got {topk!r}')
        if not isinstance(candidate_depth, int) or candidate_depth < 1:
            raise ValueError(f'candidate_depth must be a positive integer, got {candidate_depth!r}')
        if not isinstance(rrf_k, (int, float)) or rrf_k <= 0:
            raise ValueError(f'rrf_k must be a positive number, got {rrf_k!r}')
        self.candidate_depth = candidate_depth
        self.rrf_k = rrf_k
        self.embedding = embedding
        self.graph_verbalizer = graph_verbalizer or TripletGVerbalizer()
        self.topk = topk
        self.dim = graph_embeddings.dim
        # Duplicate triplets would fill top-k with copies of one fact.
        self._triplets: List[Tuple[str, str, str]] = list(dict.fromkeys(tuple(t) for t in triplets))

        nodes, relations = graph_embeddings.nodes, graph_embeddings.relations
        zero = len(nodes.texts)  # index of the 0-similarity slot appended at query time

        def node_row(node: str) -> int:
            row = nodes.row(node)
            if row is not None:
                return row
            if is_opaque_id(node):
                return zero
            raise ValueError(f'node {node!r} has no embedding; graph_embeddings was built '
                             f'from a different graph')

        def relation_row(relation: str) -> int:
            row = relations.row(relation)
            if row is None:
                raise ValueError(f'relation {relation!r} has no embedding; graph_embeddings '
                                 f'was built from a different graph')
            return row

        for t in self._triplets:
            if len(t) != 3:
                raise ValueError(f'triplet must be (head, relation, tail), got {t!r}')
        self._heads = np.fromiter((node_row(h) for h, _, _ in self._triplets), dtype=np.int32,
                                  count=len(self._triplets))
        self._relations = np.fromiter((relation_row(r) for _, r, _ in self._triplets),
                                      dtype=np.int32, count=len(self._triplets))
        self._tails = np.fromiter((node_row(t) for _, _, t in self._triplets), dtype=np.int32,
                                  count=len(self._triplets))
        self._node_vectors = nodes.vectors
        self._relation_vectors = relations.vectors
        self._lexical = BM25Index(map(_triplet_words, self._triplets)) if lexical else None

    @classmethod
    def from_graph_store(cls, graph_store, embedding, graph_embeddings: Optional[GraphEmbeddings] = None,
                         cache_dir: Optional[str] = None, model_id: Optional[str] = None,
                         **kwargs) -> 'TextTripletRetriever':
        """Build over every triplet of a store that can enumerate them (LocalKGStore)."""
        get_triplets = getattr(graph_store, 'get_triplets', None)
        if get_triplets is None:
            raise TypeError(f'{type(graph_store).__name__} cannot enumerate its triplets '
                            f'(no get_triplets()); text triplet retrieval needs them all')
        # Materialized once: a single-use iterator would be spent embedding the graph
        # and leave the retriever with no triplets.
        triplets = list(get_triplets())
        if graph_embeddings is None:
            graph_embeddings = GraphEmbeddings.build(triplets, embedding, cache_dir, model_id=model_id)
        return cls(triplets, graph_embeddings, embedding, **kwargs)

    def _query_vector(self, query: str) -> np.ndarray:
        vector = np.asarray(self.embedding.embed_query(truncate_for_embedding(query)), dtype=np.float32)
        if vector.shape != (self.dim,):
            raise ValueError(f'query embedding has shape {vector.shape}, expected ({self.dim},)')
        norm = np.linalg.norm(vector)
        if norm == 0 or not np.isfinite(norm):
            raise ValueError('query embedding is zero or non-finite')
        return vector / norm

    def retrieve(self, query: str, source_nodes=None, topk: Optional[int] = None, **kwargs) -> List[str]:
        """Top-k triplets for the question, verbalized as 'head -> relation -> tail'.

        source_nodes is accepted for interface compatibility and ignored: this
        retriever does not depend on entity linking.
        """
        if not self._triplets:
            return []
        q = self._query_vector(query)
        node_sim = np.append(self._node_vectors @ q, np.float32(0.0))
        relation_sim = self._relation_vectors @ q
        scores = node_sim[self._heads] + relation_sim[self._relations] + node_sim[self._tails]
        k = topk if topk is not None else self.topk
        if self._lexical is None:
            top = top_k_indices(scores, k)
        else:
            depth = max(k, self.candidate_depth)
            words = [w for w in tokenize(query) if w not in QUERY_STOPWORDS]
            top = _fuse([int(i) for i in top_k_indices(scores, depth)], self._lexical.top(words, depth),
                        k, self.rrf_k)
        if not len(top):  # verbalize() rejects an empty list
            return []
        return self.graph_verbalizer.verbalize([self._triplets[i] for i in top])


def _triplet_words(triplet: Tuple[str, str, str]) -> List[str]:
    head, relation, tail = triplet
    return ((tokenize(head) if not is_opaque_id(head) else []) + tokenize(relation)
            + (tokenize(tail) if not is_opaque_id(tail) else []))


def _fuse(dense: List[int], lexical: List[int], k: int, rrf_k: float) -> List[int]:
    """Reciprocal-rank fusion; ties go to the better dense rank, then lexical rank, then index."""
    positions = {}
    for name, ranking in (('dense', dense), ('lexical', lexical)):
        for position, index in enumerate(ranking, start=1):
            positions.setdefault(index, {})[name] = position
    absent = float('inf')

    def key(index):
        found = positions[index]
        return (-sum(1.0 / (rrf_k + p) for p in found.values()),
                found.get('dense', absent), found.get('lexical', absent), index)
    return sorted(positions, key=key)[:k]
