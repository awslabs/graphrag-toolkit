# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import abstractmethod
from queue import PriorityQueue
from typing import Dict, List, Set, Tuple

import numpy as np

logger = logging.getLogger(__name__)


class BeamSearch:
    """Base class for beam search over graph structures.
    
    Subclasses must implement get_neighbors() and get_neighbors_batch()
    to define the graph traversal pattern.
    """

    def __init__(self, beam_width=100, max_depth=8, scoring_mode='cosine', **kwargs):
        self.beam_width = beam_width
        self.max_depth = max_depth
        self.scoring_mode = scoring_mode

    @abstractmethod
    def get_neighbors(self, node_id: str) -> List[str]:
        """Get neighbor IDs for a single node."""
        ...

    @abstractmethod
    def get_neighbors_batch(self, node_ids: List[str]) -> Dict[str, List[str]]:
        """Get neighbor IDs for multiple nodes. Returns {source_id: [neighbor_ids]}."""
        ...

    @abstractmethod
    def _get_embeddings(self, ids: List[str]) -> dict:
        """Get embeddings for the given IDs from the cache."""
        ...

    @abstractmethod
    def _get_top_k(self, query_embedding, embeddings, top_k):
        """Get top-k scored items from embeddings dict."""
        ...

    def _score_neighbors(self, query_embedding, parent_embedding, neighbor_embeddings, top_k):
        """Score neighbors using the configured scoring mode.
        
        Args:
            query_embedding: Query vector
            parent_embedding: Embedding of the parent node
            neighbor_embeddings: Dict of {id: embedding}
            top_k: Number of top results to return
        
        Returns:
            List of (score, neighbor_id) tuples, sorted descending
        """
        if not neighbor_embeddings:
            return []
        
        ids = list(neighbor_embeddings.keys())
        embeddings = np.array([neighbor_embeddings[sid] for sid in ids])
        
        query_norm = np.linalg.norm(query_embedding)
        norms = np.linalg.norm(embeddings, axis=1)
        safe_denom = np.maximum(norms * query_norm, 1e-10)
        query_scores = np.dot(embeddings, query_embedding) / safe_denom
        
        if self.scoring_mode == 'path_weighted' and parent_embedding is not None:
            parent_norm = np.linalg.norm(parent_embedding)
            parent_denom = np.maximum(norms * parent_norm, 1e-10)
            parent_scores = np.dot(embeddings, parent_embedding) / parent_denom
            scores = query_scores * (1.0 + parent_scores) / 2.0
        elif self.scoring_mode == 'path_propagated' and parent_embedding is not None:
            # cosine(query, parent) × cosine(parent, neighbor)
            parent_norm = np.linalg.norm(parent_embedding)
            query_parent_sim = np.dot(query_embedding, parent_embedding) / np.maximum(query_norm * parent_norm, 1e-10)
            parent_denom = np.maximum(norms * parent_norm, 1e-10)
            parent_neighbor_scores = np.dot(embeddings, parent_embedding) / parent_denom
            scores = query_parent_sim * parent_neighbor_scores
        elif self.scoring_mode == 'attention' and parent_embedding is not None:
            dim = len(parent_embedding)
            attn_logits = np.dot(embeddings, parent_embedding) / np.sqrt(dim)
            attn_logits = attn_logits - np.max(attn_logits)
            attn_weights = np.exp(attn_logits) / np.sum(np.exp(attn_logits))
            scores = query_scores * (1.0 + attn_weights)
        else:
            scores = query_scores
        
        top_k = min(top_k, len(scores))
        top_indices = np.argsort(scores)[::-1][:top_k]
        return [(scores[idx], ids[idx]) for idx in top_indices]

    def beam_search(
        self,
        query_embedding: np.ndarray,
        start_ids: List[str]
    ) -> List[Tuple[str, List[str]]]:
        """Perform beam search starting from seed IDs.
        
        Returns list of (node_id, path) tuples.
        """
        visited: Set[str] = set()
        results: List[Tuple[str, List[str]]] = []
        queue: PriorityQueue = PriorityQueue()
        batch_size = 10

        start_embeddings = self._get_embeddings(start_ids)
        start_scores = self._get_top_k(query_embedding, start_embeddings, len(start_ids))

        for similarity, node_id in start_scores:
            queue.put((-similarity, 0, node_id, [node_id]))

        while not queue.empty() and len(results) < self.beam_width:
            batch = []
            while not queue.empty() and len(batch) < batch_size:
                item = queue.get()
                neg_score, depth, current_id, path = item
                if current_id not in visited:
                    batch.append(item)

            if not batch:
                break

            expand_ids = []
            expand_items = []
            for neg_score, depth, current_id, path in batch:
                visited.add(current_id)
                results.append((current_id, path))
                if len(results) >= self.beam_width:
                    break
                if depth < self.max_depth:
                    expand_ids.append(current_id)
                    expand_items.append((depth, path))

            if not expand_ids:
                continue

            neighbors_map = self.get_neighbors_batch(expand_ids)
            all_neighbor_ids = list({nid for nids in neighbors_map.values() for nid in nids if nid not in visited})
            
            if all_neighbor_ids:
                neighbor_embeddings = self._get_embeddings(all_neighbor_ids)

                if self.scoring_mode == 'cosine':
                    scored = self._get_top_k(query_embedding, neighbor_embeddings, self.beam_width)
                    scored_dict = {sid: sim for sim, sid in scored}
                    for expand_id, (depth, path) in zip(expand_ids, expand_items):
                        for neighbor_id in neighbors_map.get(expand_id, []):
                            if neighbor_id not in visited and neighbor_id in scored_dict:
                                queue.put((-scored_dict[neighbor_id], depth + 1, neighbor_id, path + [neighbor_id]))
                else:
                    parent_embeddings = self._get_embeddings(expand_ids)
                    for expand_id, (depth, path) in zip(expand_ids, expand_items):
                        parent_emb = parent_embeddings.get(expand_id)
                        if parent_emb is not None:
                            parent_emb = np.array(parent_emb)
                        local_neighbors = {
                            nid: neighbor_embeddings[nid]
                            for nid in neighbors_map.get(expand_id, [])
                            if nid not in visited and nid in neighbor_embeddings
                        }
                        scored = self._score_neighbors(query_embedding, parent_emb, local_neighbors, self.beam_width)
                        for score, neighbor_id in scored:
                            queue.put((-score, depth + 1, neighbor_id, path + [neighbor_id]))

        return results
