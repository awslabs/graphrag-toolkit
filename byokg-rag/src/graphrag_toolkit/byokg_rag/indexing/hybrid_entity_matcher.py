# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from typing import Dict, List, Sequence

from .index import EntityMatcher
from .fuzzy_string import FuzzyStringIndex
from .graph_embeddings import EmbeddingTable, embed_texts, top_k_indices

__all__ = ['HybridEntityMatcher']


class HybridEntityMatcher(EntityMatcher):
    """Links a mention to graph nodes by fusing fuzzy-string and embedding candidates.

    Candidates are fused per mention by reciprocal rank (score = sum of
    1 / (rrf_k + rank) over the matchers that returned them): fuzzy similarity is
    0-100 and dense similarity is cosine, so ranks are the only comparable signal.
    Each matcher contributes its top candidate_depth (at least topk), so a node both
    rank highly can win even if neither ranks it first. Equal scores go to the
    better fuzzy rank: fuzzy's first choice is displaced only by a node both
    matchers return, or when fuzzy returns nothing. A node whose name equals the
    mention always ranks first — verbatim if one exists, else case-insensitively.

    Returns {'hits': [{'document_id', 'document', 'match_score'}]} with each input's
    hits contiguous and in input order; match_score is the fused score (higher is
    better), raised for an exact match so it agrees with the order. EntityLinker's
    flat and group_by_mention paths both work with it.

    Dense search is an exact cosine scan in numpy rather than LocalFaissDenseIndex:
    IndexFlatIP is the same brute force, and loading faiss alongside torch (the
    rerankers) aborts on macOS with two OpenMP runtimes (OMP Error #15).
    """

    def __init__(self, fuzzy_index: FuzzyStringIndex, node_table: EmbeddingTable, embedding,
                 rrf_k: int = 60, candidate_depth: int = 20):
        super().__init__(index=None)
        if not isinstance(candidate_depth, int) or candidate_depth < 1:
            raise ValueError(f'candidate_depth must be a positive integer, got {candidate_depth!r}')
        self.fuzzy_index = fuzzy_index
        self.node_table = node_table
        self.embedding = embedding
        self.rrf_k = rrf_k
        self.candidate_depth = candidate_depth
        self._nodes = set(fuzzy_index.vocab)
        # First node per case-folded name, for mentions with no verbatim node; the
        # fuzzy vocab is sorted, so ties resolve the same way every run.
        self._casefolded: Dict[str, str] = {}
        for node in fuzzy_index.vocab:
            self._casefolded.setdefault(node.casefold(), node)

    def _exact_node(self, mention: str):
        mention = mention.strip()
        if mention in self._nodes:
            return mention
        return self._casefolded.get(mention.casefold())

    @classmethod
    def from_nodes(cls, node_ids: Sequence[str], embedding, node_table: EmbeddingTable,
                   **kwargs) -> 'HybridEntityMatcher':
        """Fuzzy index over every node; dense candidates from the embedded (named) ones.

        Node ids are used as their names, as in LocalKGStore.
        """
        fuzzy = FuzzyStringIndex()
        fuzzy.add(list(node_ids))
        return cls(fuzzy, node_table, embedding, **kwargs)

    def _dense_candidates(self, queries: List[str], topk: int) -> List[List[str]]:
        # Mentions are embedded as documents, like the node names they are matched to:
        # name-to-name is a symmetric match, unlike question-to-fact retrieval.
        mention_vectors = embed_texts(self.embedding, queries)
        if mention_vectors.shape[1] != self.node_table.dim:
            raise ValueError(f'mention embeddings have dim {mention_vectors.shape[1]}, '
                             f'node table has {self.node_table.dim}')
        similarities = self.node_table.vectors @ mention_vectors.T  # (nodes, mentions)
        texts = self.node_table.texts
        return [[texts[i] for i in top_k_indices(similarities[:, j], topk)]
                for j in range(len(queries))]

    def retrieve(self, queries: List[str], topk: int = 1, **kwargs):
        if not isinstance(queries, list) or not all(isinstance(q, str) for q in queries):
            raise TypeError(f'queries must be a list of mention strings, got {queries!r}')
        if not queries:
            return {'hits': []}
        depth = max(topk, self.candidate_depth)
        dense = self._dense_candidates(queries, depth)

        hits = []
        for query, dense_ranked in zip(queries, dense):
            # One mention per fuzzy call: its batched match re-sorts all inputs' hits
            # together and loses which mention each came from.
            fuzzy_ranked = [h['document_id'] for h in self.fuzzy_index.match([query], topk=depth)['hits']]
            fuzzy_rank = {}
            for rank, node in enumerate(fuzzy_ranked, start=1):
                fuzzy_rank.setdefault(node, rank)
            scores = defaultdict(float)
            for ranked in (fuzzy_ranked, dense_ranked):
                for rank, node in enumerate(ranked, start=1):
                    scores[node] += 1.0 / (self.rrf_k + rank)
            exact = self._exact_node(query)
            if exact is not None:
                # Above any fused score, so match_score order is the rank order.
                scores[exact] = max(scores.values(), default=0.0) + 1.0 / (self.rrf_k + 1)
            order = sorted(scores, key=lambda node: (
                node != exact, -scores[node], fuzzy_rank.get(node, float('inf')), node))
            hits.extend({'document_id': node, 'document': node, 'match_score': scores[node]}
                        for node in order[:topk])
        return {'hits': hits}
