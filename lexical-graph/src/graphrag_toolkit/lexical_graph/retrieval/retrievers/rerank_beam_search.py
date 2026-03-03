# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from queue import PriorityQueue
from typing import List, Dict, Set, Tuple, Optional, Any, Union, Type

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_statements_query
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_retriever import SemanticGuidedBaseRetriever
from graphrag_toolkit.lexical_graph.retrieval.post_processors import RerankerMixin
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)

class RerankingBeamGraphSearch(SemanticGuidedBaseRetriever):
    """
    RerankingBeamGraphSearch class leverages a reranking mechanism combined with beam search
    to perform graph-structured retrieval of statements. The search process is guided by
    semantic relevance through reranking layers.

    This class combines multiple retrieval strategies such as vector search, direct graph
    neighbors traversal, and reranking to fetch statements most relevant to a given query.
    The beam search explores graph paths to a limited depth, aiming to retrieve high-quality
    candidates for downstream processing. An intermediate caching system for retrieved
    statements and scores ensures improved performance across retrieval stages.

    Attributes:
        reranker (RerankerMixin): Reranking module that computes relevance scores between
            queries and statements.
        max_depth (int): Maximum depth of the beam search graph traversal.
        beam_width (int): Beam width determining the number of candidates explored at
            each depth during search.
        shared_nodes (Optional[List[NodeWithScore]]): Predefined shared nodes available for
            the current retriever to initialize. May be empty.
        score_cache (Dict[str, float]): Cache storing relevance scores previously computed
            by the reranker to optimize repeated queries.
        statement_cache (Dict[str, Dict]): Cache mapping statement IDs to their
            corresponding detailed metadata fetched from the graph store.
        initial_retrievers (List[SemanticGuidedBaseRetriever]): List of initial retrievers
            capable of providing starting statements for further reranking and beam searches.
    """
    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        reranker:RerankerMixin,
        initial_retrievers:Optional[List[Union[SemanticGuidedBaseRetriever, Type[SemanticGuidedBaseRetriever]]]]=None,
        shared_nodes:Optional[List[NodeWithScore]] = None,
        max_depth:int=3,
        beam_width:int=10,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes an object that handles retrieval, graph navigation, and reranking
        with a beam search approach. It incorporates customization options such as initial
        retrievers, depth constraint for exploration, and beam width for limiting
        concurrent node evaluations. The class also supports shared nodes for result
        aggregation and caches scores and statements for efficiency.

        Args:
            vector_store: Underlying storage mechanism for vectorized representations
                used in approximate nearest neighbor searches.
            graph_store: Graph representation database that stores entities,
                relationships, or nodes with interconnections for navigation.
            reranker: Component responsible for ranking nodes or items based on
                customized scoring criteria.
            initial_retrievers: Optional sequence of retriever instances or types that
                guide the initial stage of the retrieval process before using the
                primary retriever. Each retriever in this list must accept vector_store,
                graph_store, and filter_config as construction parameters.
            shared_nodes: Optional sequence of nodes with scores that are used across
                multiple retrievals or beam searches as common input. Allows result
                propagation or consistency.
            max_depth: Maximum allowable depth for search or traversal within the graph.
                Deep graphs may be restricted to this depth limit for computational
                efficiency.
            beam_width: The maximum number of nodes retained after each iteration
                during the beam search; higher values expand search breadth at a cost
                to performance.
            filter_config: Optional configuration that defines the filtering or pruning
                criteria applied during retrieval, ranking, or navigation.
            **kwargs: Arbitrary additional keyword arguments, passed to base class
                initialization or specific retriever construction.

        """
        super().__init__(vector_store, graph_store, filter_config, **kwargs)
        self.reranker = reranker 
        self.max_depth = max_depth
        self.beam_width = beam_width
        self.shared_nodes = shared_nodes
        self.score_cache = {}
        self.statement_cache = {} 

        # Initialize initial retrievers if provided
        self.initial_retrievers = []
        if initial_retrievers:
            for retriever in initial_retrievers:
                if isinstance(retriever, type):
                    self.initial_retrievers.append(
                        retriever(vector_store, graph_store, filter_config, **kwargs)
                    )
                else:
                    self.initial_retrievers.append(retriever)


    def get_statements(self, statement_ids: List[str]) -> Dict[str, Dict]:
        """
        Retrieves statements by their IDs, utilizing a cache for efficiency. If any of the provided
        IDs are not found in the cache, it fetches them from the graph store, updates the cache,
        and then returns all requested results.

        Args:
            statement_ids (List[str]): A list of statement IDs to fetch.

        Returns:
            Dict[str, Dict]: A dictionary mapping each statement ID to its corresponding statement
            data. The data is retrieved either from the local cache or by querying the graph store.
        """
        uncached_ids = [sid for sid in statement_ids if sid not in self.statement_cache]
        if uncached_ids:
            new_results = get_statements_query(self.graph_store, uncached_ids)
            for result in new_results:
                sid = result['result']['statement']['statementId']
                self.statement_cache[sid] = result['result']
        
        return {sid: self.statement_cache[sid] for sid in statement_ids}
        
    def get_neighbors(self, statement_id: str) -> List[str]:
        """
        Retrieves the neighbors of a given statement in the graph database. A neighbor is
        defined as a statement that is directly connected via shared entities acting
        as subjects or objects across facts and supported relationships.

        Args:
            statement_id (str): The unique identifier of the statement whose neighbors
                are to be retrieved.

        Returns:
            List[str]: A list of statement IDs representing the neighboring statements.

        """
        cypher = f"""
        // get statement neighbours (rerank beam search)
        MATCH (e)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(s:`__Statement__`)
        WHERE {self.graph_store.node_id('s.statementId')} = $statementId
        WITH s, COLLECT(DISTINCT e) AS entities
        UNWIND entities AS entity
        MATCH (entity)-[:`__SUBJECT__`|`__OBJECT__`]->()-[:`__SUPPORTS__`]->(e_neighbors)
        RETURN DISTINCT {self.graph_store.node_id('e_neighbors.statementId')} as statementId
        """
        
        neighbors = self.graph_store.execute_query(
            cypher, 
            {'statementId': statement_id}
        )
        return [n['statementId'] for n in neighbors]
    
    def rerank_statements(
        self,
        query: str,
        statement_ids: List[str],
        statement_texts: Dict[str, str]
    ) -> List[Tuple[float, str]]:
        """
        Rerank statements based on the relevance scores generated by a reranker.

        This method takes a query, a list of statement IDs, and a dictionary mapping
        statement IDs to statement texts. For statements not yet cached, it computes
        relevance scores using the reranker component, caches the scores, and then
        returns the statement IDs paired with their scores, sorted in descending
        order of relevance.

        Args:
            query (str): The query string for which the statements need to be reranked.
            statement_ids (List[str]): A list of unique IDs corresponding to the
                statements to be evaluated.
            statement_texts (Dict[str, str]): A dictionary mapping each statement ID
                to its associated text.

        Returns:
            List[Tuple[float, str]]: A list of tuples where each tuple contains the
                score (float) and the statement ID (str), sorted in descending order
                based on the scores.
        """
        uncached_statements = [statement_texts[sid] for sid in statement_ids if statement_texts[sid] not in self.score_cache]
        
        if uncached_statements:
            pairs = [
                (query, statement_text)
                for statement_text in uncached_statements
            ]

            scores = self.reranker.rerank_pairs(
                pairs=pairs,
                batch_size=self.reranker.batch_size*2
            )

            for statement_text, score in zip(uncached_statements, scores):
                self.score_cache[statement_text] = score
            
        scored_pairs = []
        for sid in statement_ids:
            score = self.score_cache[statement_texts[sid]]
            scored_pairs.append(
                (score, sid)
            )

        scored_pairs.sort(reverse=True)
        return scored_pairs

    def beam_search(
        self, 
        query_bundle: QueryBundle,
        start_statement_ids: List[str]
    ) -> List[Tuple[str, List[str]]]:
        """
        Executes a beam search algorithm using a priority queue to explore a network of statements,
        starting from provided initial statement IDs. The method scores and prioritizes paths
        according to a reranker function and keeps track of visited nodes to avoid revisits.
        Results consist of statement IDs and their respective paths.

        Args:
            query_bundle (QueryBundle): A bundle containing query details such as the query string.
            start_statement_ids (List[str]): A list of IDs representing the starting points
                for the beam search.

        Returns:
            List[Tuple[str, List[str]]]: A list of tuples, where each tuple consists of a
                statement ID and the path taken to reach it.
        """
        visited: Set[str] = set()
        results: List[Tuple[str, List[str]]] = []
        queue: PriorityQueue = PriorityQueue()

        # Get texts for all start statements
        start_statements = self.get_statements(start_statement_ids)
        statement_texts = {
            sid: statement['statement']['value']
            for sid, statement in start_statements.items()
        }

        # Score initial statements using reranker
        start_scores = self.rerank_statements(
            query_bundle.query_str,
            start_statement_ids,
            statement_texts
        )

        # Initialize queue with start statements
        for score, statement_id in start_scores:
            queue.put((-score, 0, statement_id, [statement_id]))

        while not queue.empty() and len(results) < self.beam_width:
            neg_score, depth, current_id, path = queue.get()

            if current_id in visited:
                continue

            visited.add(current_id)
            results.append((current_id, path))

            if depth < self.max_depth:
                # Get and score neighbors
                neighbor_ids = self.get_neighbors(current_id)
                if neighbor_ids:
                    # Get texts for neighbors
                    neighbor_statements = self.get_statements(neighbor_ids)
                    neighbor_texts = {
                        sid: str(statement['statement']['value']+'\n'+statement['statement']['details'])
                        for sid, statement in neighbor_statements.items()
                    }

                    # Score neighbors using reranker
                    scored_neighbors = self.rerank_statements(
                        query_bundle.query_str,
                        neighbor_ids,
                        neighbor_texts
                    )

                    # Add top-k neighbors to queue
                    for score, neighbor_id in scored_neighbors[:self.beam_width]:
                        if neighbor_id not in visited:
                            new_path = path + [neighbor_id]
                            queue.put((-score, depth + 1, neighbor_id, new_path))

        return results
    
    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """
        Retrieves a list of nodes based on the given query bundle using a combination
        of shared nodes, initial retrievers, and beam search techniques. The retrieval
        process utilizes metadata and cached information to generate a ranked list of
        potential matches. The retrieved nodes are sorted by their scores in descending
        order before returning.

        Args:
            query_bundle (QueryBundle): Contains the query data required for retrieval.

        Returns:
            List[NodeWithScore]: A list of nodes with associated scores, representing
            relevant matches for the query.

        Raises:
            None
        """
 
        # Get initial nodes (either shared or from initial retrievers)
        initial_statement_ids = set()
        
        if self.shared_nodes is not None:
            # Use shared nodes if available
            for node in self.shared_nodes:
                initial_statement_ids.add(
                    node.node.metadata['statement']['statementId']
                )
        elif self.initial_retrievers:
            # Get nodes from initial retrievers
            for retriever in self.initial_retrievers:
                nodes = retriever.retrieve(query_bundle)
                for node in nodes:
                    initial_statement_ids.add(
                        node.node.metadata['statement']['statementId']
                    )
        else:
            # Fallback to vector similarity if no initial nodes
            results = self.vector_store.get_index('statement').top_k(
                query_bundle,
                top_k=self.beam_width * 2,
                filter_config=self.filter_config
            )
            initial_statement_ids = {
                r['statement']['statementId'] for r in results
            }

        if not initial_statement_ids:
            logger.warning("No initial statements found for the query.")
            return []

        # Perform beam search
        beam_results = self.beam_search(
            query_bundle,
            list(initial_statement_ids)
        )

        # Collect all new statement IDs from beam search
        new_statement_ids = [
            statement_id for statement_id, _ in beam_results
            if statement_id not in initial_statement_ids
        ]

        if not new_statement_ids:
            logger.info("Beam search did not find any new statements.")
            return []

        # Create nodes from results
        nodes = []
        statement_to_path = {
            statement_id: path 
            for statement_id, path in beam_results 
            if statement_id not in initial_statement_ids
        }
        
        for statement_id, path in statement_to_path.items():
            statement_data = self.statement_cache.get(statement_id)
            if statement_data:
                node = TextNode(
                    text=statement_data['statement']['value'],
                    metadata={
                        'statement': statement_data['statement'],
                        'chunk': statement_data['chunk'],
                        'source': statement_data['source'],
                        'search_type': 'beam_search',
                        'depth': len(path),
                        'path': path
                    }
                )
                score = self.score_cache.get(statement_data['statement']['value'], 0.0)
                nodes.append(NodeWithScore(node=node, score=score))
            else:
                logger.warning(f"Statement data not found in cache for ID: {statement_id}")

        nodes.sort(key=lambda x: coalesce(x.score, 0.0), reverse=True)

        logger.info(f"Retrieved {len(nodes)} new nodes through beam search.")
        return nodes