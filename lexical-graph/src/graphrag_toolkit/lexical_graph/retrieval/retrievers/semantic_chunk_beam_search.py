# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Set, Tuple, Optional, Any
from queue import PriorityQueue
import numpy as np
import logging
import time

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.chunk_utils import get_top_k, SharedChunkEmbeddingCache
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_chunk_retriever import SemanticGuidedBaseChunkRetriever

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)

class SemanticChunkBeamGraphSearch(SemanticGuidedBaseChunkRetriever):

    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        embedding_cache:Optional[SharedChunkEmbeddingCache]=None,
        max_depth:int=3,
        beam_width:int=10,
        shared_nodes:Optional[List[NodeWithScore]]=None,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:

        super().__init__(vector_store, graph_store, filter_config, **kwargs)
        self.embedding_cache = embedding_cache
        self.max_depth = max_depth
        self.beam_width = beam_width
        self.shared_nodes = shared_nodes

    def get_neighbors(self, chunk_id: str) -> List[str]:

        cypher = f"""
        // get top entities for chunk (semantic beam search)
        MATCH (e)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(`__Statement__`)-[:`__MENTIONED_IN__`]->(c)
        WHERE {self.graph_store.node_id('c.chunkId')} = $chunkId
        WITH DISTINCT e AS entity
        MATCH (entity)-[r:`__SUBJECT__`|`__OBJECT__`]->()
        RETURN {self.graph_store.node_id('entity.entityId')} AS entityId, count(r) AS score ORDER BY score DESC LIMIT $limit
        """

        results = self.graph_store.execute_query(cypher, {'chunkId': chunk_id, 'limit': 5})
        entity_ids = [r['entityId'] for r in results]

        cypher = f"""
        // get neighboring chunks for common entities (semantic beam search)
        MATCH (entity)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(`__Statement__`)-[:`__MENTIONED_IN__`]->(e_neighbors)
        WHERE {self.graph_store.node_id('entity.entityId')} IN $entityIds
        AND {self.graph_store.node_id('e_neighbors.chunkId')} <> $chunkId
        WITH DISTINCT e_neighbors AS neighbors, entity
        RETURN {self.graph_store.node_id('neighbors.chunkId')} as chunkId, count(entity) ORDER BY count(entity) DESC LIMIT $limit
        """

        results = self.graph_store.execute_query(cypher, {'entityIds': entity_ids, 'chunkId': chunk_id, 'limit': self.beam_width * 2})
        chunk_ids = [r['chunkId'] for r in results]

        return chunk_ids

    def beam_search(
        self, 
        query_embedding: np.ndarray,
        start_chunk_ids: List[str]
    ) -> List[Tuple[str, List[str]]]:  # [(statement_id, path), ...]
        
        visited: Set[str] = set()
        results: List[Tuple[str, List[str]]] = []
        queue: PriorityQueue = PriorityQueue()

        logger.debug(f'Beam search [beam_width: {self.beam_width}, max_depth: {self.max_depth}]')
        logger.debug(f'Getting {len(start_chunk_ids)} top k chunk ids' )

        # Get initial embeddings and scores
        start_embeddings = self.embedding_cache.get_embeddings(start_chunk_ids)
        start_scores = get_top_k(
            query_embedding,
            start_embeddings,
            len(start_chunk_ids)
        )

        # Initialize queue with start chunks
        for similarity, chunk_id in start_scores:
            queue.put((-similarity, 0, chunk_id, [chunk_id]))
            

        iteration = 0

        while not queue.empty() and len(results) < self.beam_width:
            
            iteration += 1

            neg_score, depth, current_id, path = queue.get()

            if current_id in visited:
                continue

            visited.add(current_id)
            results.append((current_id, path))

            added = 0

            if depth < self.max_depth:
                neighbor_ids = self.get_neighbors(current_id)
                
                if neighbor_ids:
                    # Get embeddings for neighbors using shared cache
                    neighbor_embeddings = self.embedding_cache.get_embeddings(neighbor_ids)

                    logger.debug(f'Getting {self.beam_width} top k chunk ids' )
                    
                    # Score neighbors
                    scored_neighbors = get_top_k(
                        query_embedding,
                        neighbor_embeddings,
                        self.beam_width
                    )

                    # Add neighbors to queue
                    for similarity, neighbor_id in scored_neighbors:
                        if neighbor_id not in visited:
                            new_path = path + [neighbor_id]
                            added += 1
                            queue.put(
                                (-similarity, depth + 1, neighbor_id, new_path)
                            )

            logger.debug(f'iteration: {iteration}, added: {added}')

        return results

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:

        start = time.time()

        # 1. Get initial nodes (either shared or fallback)
        initial_chunk_ids = []
        if self.shared_nodes:
            initial_chunk_ids = [
                n.node.metadata['chunk']['chunkId'] 
                for n in self.shared_nodes
            ]
        else:
            # Fallback to vector similarity
            results = self.vector_store.get_index('chunk').top_k(
                query_bundle,
                top_k=self.beam_width * 2,
                filter_config=self.filter_config
            )
            initial_chunk_ids = [
                r['chunk']['chunkId'] for r in results
            ]

        end_initial = time.time()
        initial_ms = (end_initial-start) * 1000

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:    
            logger.debug(f'initial_chunk_ids: {initial_chunk_ids} [{initial_ms:.2f}ms]')
        else:
            logger.debug(f'num initial_chunk_ids: {len(initial_chunk_ids)} [{initial_ms:.2f}ms]')
        

        if not initial_chunk_ids:
            return []

        # 2. Perform beam search
        beam_results = self.beam_search(
            query_bundle.embedding,
            initial_chunk_ids
        )

        end_beam = time.time()
        beam_ms = (end_beam-end_initial) * 1000
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:  
            logger.debug(f'beam_results: {beam_results} [{beam_ms:.2f}ms]')
        else:
            logger.debug(f'num beam_results: {len(beam_results)} [{beam_ms:.2f}ms]')

        # 3. Create nodes for new chunks only
        nodes = []
        initial_ids = set(initial_chunk_ids)
        for chunk_id, path in beam_results:
            if chunk_id not in initial_ids:
                node = TextNode(
                    text="",  # Placeholder
                    metadata={
                        'chunk': {'chunkId': chunk_id},
                        'search_type': 'beam_search',
                        'depth': len(path),
                        'path': path
                    }
                )
                nodes.append(NodeWithScore(node=node, score=0.0))

        end_nodes = time.time()
        nodes_ms = (end_nodes-end_beam) * 1000

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:      
            logger.debug(f'nodes: {nodes} [{nodes_ms:.2f}ms]')
        else:
            logger.debug(f'num nodes: {len(nodes)} [{nodes_ms:.2f}ms]')

        return nodes
