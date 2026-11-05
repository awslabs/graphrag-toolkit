# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional, Sequence, Tuple

import numpy as np

from .graph_retrievers import GRetriever
from .graph_verbalizer import TripletGVerbalizer
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
    """

    def __init__(self, triplets: Sequence[Sequence[str]], graph_embeddings: GraphEmbeddings,
                 embedding, graph_verbalizer=None, topk: int = 10):
        if not isinstance(topk, int) or topk < 1:
            raise ValueError(f'topk must be a positive integer, got {topk!r}')
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
        top = top_k_indices(scores, topk if topk is not None else self.topk)
        if not len(top):  # verbalize() rejects an empty list
            return []
        return self.graph_verbalizer.verbalize([self._triplets[i] for i in top])
