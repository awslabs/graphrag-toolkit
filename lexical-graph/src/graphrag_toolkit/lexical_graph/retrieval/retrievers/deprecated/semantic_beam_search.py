# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Set, Tuple, Optional, Any
from queue import PriorityQueue
import numpy as np
import logging

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_top_k, SharedEmbeddingCache
from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_base_retriever import SemanticGuidedBaseRetriever

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)

class SemanticBeamGraphSearch(SemanticGuidedBaseRetriever):
    """
    A class that implements a semantic-guided beam search algorithm on a graph-based data store,
    facilitating retrieval tasks involving entity relationships and embedding-based similarity.

    The `SemanticBeamGraphSearch` class builds upon a base retriever to utilize a combination of graph-based
    neighbor expansion and beam search heuristic optimization. It is designed to process queries, traverse
    entities and their connections in a graph, and return the most relevant statements using an embedding-based
    ranking approach. The class also integrates optional cache mechanisms for embeddings and allows flexibility
    in configuration such as search depth and beam width.

    Attributes:
        embedding_cache (Optional[SharedEmbeddingCache]): Shared cache of embeddings for faster retrieval.
        max_depth (int): Maximum number of graph traversal levels during beam search.
        beam_width (int): Number of candidate states to consider at each step in the beam search.
        shared_nodes (Optional[List[NodeWithScore]]): Initial shared nodes to derive the starting points.
    """
    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        embedding_cache:Optional[SharedEmbeddingCache]=None,
        max_depth:int=3,
        beam_width:int=10,
        shared_nodes:Optional[List[NodeWithScore]]=None,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes a class that manages the connection between a vector store, a graph store, and
        an optional embedding cache, allowing complex data structure processing through specific
        search depth and beam width configurations.

        Args:
            vector_store: The storage facility adhering to the VectorStore interface for
                vector data retrieval and management.
            graph_store: The storage facility adhering to the GraphStore interface for
                graph structure data retrieval and manipulation.
            embedding_cache: An optional shared embedding cache for reusing embeddings,
                reducing computation for repeated queries.
            max_depth: The maximum depth for graph traversals, controlling the scope of
                the search or processing operations.
            beam_width: The beam width determining the number of nodes considered at
                each level of the traversal during the search operation.
            shared_nodes: An optional list of node-score mappings representing pre-shared
                nodes to be considered during operation.
            filter_config: Optional configuration for applying filters to the results or
                operations performed within the system.
            **kwargs: Additional keyword arguments passed to the superclass initializer.
        """
        super().__init__(vector_store, graph_store, filter_config, **kwargs)
        self.embedding_cache = embedding_cache
        self.max_depth = max_depth
        self.beam_width = beam_width
        self.shared_nodes = shared_nodes

    def get_neighbors(self, statement_id: str) -> List[str]:
        """
        Fetches the neighboring statement IDs in the graph, where neighboring statements are associated
        with any entities connected to the given statement.

        This method queries the graph database to identify entities that are directly linked to the
        provided statement and retrieves other statements supported by these entities.

        Args:
            statement_id: The ID of the statement for which to fetch neighboring statements.

        Returns:
            List[str]: A list of statement IDs that are neighbors to the given statement ID.
        """
        cypher = f"""
        // get statement neighbours (semantic beam search)
        MATCH (e)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(s:`__Statement__`)
        WHERE {self.graph_store.node_id('s.statementId')} = $statementId
        WITH s, COLLECT(DISTINCT e) AS entities
        UNWIND entities AS entity
        MATCH (entity)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(e_neighbors)
        RETURN DISTINCT {self.graph_store.node_id('e_neighbors.statementId')} as statementId
        """
        
        neighbors = self.graph_store.execute_query(cypher, {'statementId': statement_id})
        return [n['statementId'] for n in neighbors]

    def beam_search(
        self, 
        query_embedding: np.ndarray,
        start_statement_ids: List[str]
    ) -> List[Tuple[str, List[str]]]:  # [(statement_id, path), ...]
        """
        Performs a beam search to find the most relevant paths based on the provided query embedding.

        The search starts from given initial statement IDs and explores their neighbors up to a maximum
        depth. It uses a priority queue to ensure that the paths with the highest similarity scores
        are evaluated first. The method stops when the beam width (the maximum number of results)
        is reached or when no more relevant neighbors are found.

        Args:
            query_embedding (np.ndarray): The embedding vector representing the query for
                similarity comparison.
            start_statement_ids (List[str]): A list of statement IDs to start the beam search from.

        Returns:
            List[Tuple[str, List[str]]]: A list of tuples where each tuple consists of a statement ID
            and its corresponding path of IDs that led to it. The length of the returned list does
            not exceed the beam width.
        """
        visited: Set[str] = set()
        results: List[Tuple[str, List[str]]] = []
        queue: PriorityQueue = PriorityQueue()

        # Get initial embeddings and scores
        start_embeddings = self.embedding_cache.get_embeddings(start_statement_ids)
        start_scores = get_top_k(
            query_embedding,
            start_embeddings,
            len(start_statement_ids)
        )

        # Initialize queue with start statements
        for similarity, statement_id in start_scores:
            queue.put((-similarity, 0, statement_id, [statement_id]))

        while not queue.empty() and len(results) < self.beam_width:
            neg_score, depth, current_id, path = queue.get()

            if current_id in visited:
                continue

            visited.add(current_id)
            results.append((current_id, path))

            if depth < self.max_depth:
                neighbor_ids = self.get_neighbors(current_id)
                
                if neighbor_ids:
                    # Get embeddings for neighbors using shared cache
                    neighbor_embeddings = self.embedding_cache.get_embeddings(neighbor_ids)
                    
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
                            queue.put(
                                (-similarity, depth + 1, neighbor_id, new_path)
                            )

        return results

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """
        Retrieves nodes relevant to a query by performing an initial extraction of
        statement IDs either from shared nodes or through a fallback vector similarity
        search, followed by a beam search to further refine the results. Constructs
        new nodes for any statements not already processed.

        Args:
            query_bundle (QueryBundle): The input query packaged as a QueryBundle
                object, containing the query and its associated metadata.

        Returns:
            List[NodeWithScore]: A list of `NodeWithScore` objects representing the
                nodes corresponding to the retrieved statements, along with their
                associated scores.
        """
        # 1. Get initial nodes (either shared or fallback)
        initial_statement_ids = []
        if self.shared_nodes:
            initial_statement_ids = [
                n.node.metadata['statement']['statementId'] 
                for n in self.shared_nodes
            ]
        else:
            # Fallback to vector similarity
            results = self.vector_store.get_index('statement').top_k(
                query_bundle,
                top_k=self.beam_width * 2,
                filter_config=self.filter_config
            )
            initial_statement_ids = [
                r['statement']['statementId'] for r in results
            ]

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:    
            logger.debug(f'initial_statement_ids: {initial_statement_ids}')
        else:
            logger.debug(f'num initial_statement_ids: {len(initial_statement_ids)}')
        

        if not initial_statement_ids:
            return []

        # 2. Perform beam search
        beam_results = self.beam_search(
            query_bundle.embedding,
            initial_statement_ids
        )
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:  
            logger.debug(f'beam_results: {beam_results}')
        else:
            logger.debug(f'num beam_results: {len(beam_results)}')

        # 3. Create nodes for new statements only
        nodes = []
        initial_ids = set(initial_statement_ids)
        for statement_id, path in beam_results:
            if statement_id not in initial_ids:
                node = TextNode(
                    text="",  # Placeholder
                    metadata={
                        'statement': {'statementId': statement_id},
                        'search_type': 'beam_search',
                        'depth': len(path),
                        'path': path
                    }
                )
                nodes.append(NodeWithScore(node=node, score=0.0))

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:      
            logger.debug(f'nodes: {nodes}')
        else:
            logger.debug(f'num nodes: {len(nodes)}')

        return nodes
