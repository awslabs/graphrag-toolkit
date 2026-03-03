# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import concurrent.futures
import logging
from collections import defaultdict
from typing import List, Optional, Any, Union, Type
from itertools import repeat

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore

from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_retriever import SemanticGuidedBaseRetriever
from graphrag_toolkit.lexical_graph.retrieval.retrievers.keyword_ranking_search import KeywordRankingSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.statement_cosine_seach import StatementCosineSimilaritySearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_beam_search import SemanticBeamGraphSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.rerank_beam_search import RerankingBeamGraphSearch
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_statements_query, SharedEmbeddingCache
from graphrag_toolkit.lexical_graph.utils.arg_utils import coalesce

logger = logging.getLogger(__name__)

SemanticGuidedRetrieverType = Union[SemanticGuidedBaseRetriever, Type[SemanticGuidedBaseRetriever]]

class SemanticGuidedRetriever(SemanticGuidedBaseRetriever):
    """
    Implementation of a retrieval class that combines semantic-guided search mechanisms to retrieve data
    from multiple sources, such as vector and graph stores. The class integrates various retrievers for
    initial search and graph-based expansion, offering a flexible and configurable retrieval system.

    This class is designed for retrieving nodes relevant to a given query, applying semantic similarity,
    keyword ranking, graph expansion, and filtering strategies. It ensures efficient and comprehensive
    information retrieval, leveraging shared caches, metadata filtering, and source grouping to organize
    the resulting nodes.

    Attributes:
        share_results (bool): Indicates if results from initial retrieval should be shared and used for
            graph-based expansion.
        shared_embedding_cache (SharedEmbeddingCache): Caches embeddings to optimize retrieval performance
            across multiple retriever instances.
        initial_retrievers (list): Contains instances of retrievers used for the initial search phase.
        graph_retrievers (list): Contains instances of graph-based retrievers for result expansion.
    """
    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        retrievers:Optional[List[Union[SemanticGuidedBaseRetriever, Type[SemanticGuidedBaseRetriever]]]]=None,
        share_results:bool=True,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes a composite retriever system that integrates multiple retriever classes for
        semantic and keyword-based search using shared embedding caches and vector/graph
        stores. The initialization process allows for custom retrievers or defaults to predefined
        retrievers with specific configurations.

        Args:
            vector_store: The vector storage backend used for embedding search.
            graph_store: The graph storage backend used for retrieving graph-based results.
            retrievers: Optional list of retriever instances or retriever classes. If classes
                are provided, they will be initialized with the provided `vector_store`,
                `graph_store`, and additional keyword arguments.
            share_results: Boolean flag indicating whether the results from different retrievers
                should be shared.
            filter_config: Optional configuration used for filtering retriever results.
            **kwargs: Additional keyword arguments used to initialize retrievers.

        """
        super().__init__(vector_store, graph_store, filter_config, **kwargs)

        self.share_results = share_results
        
        # Create shared embedding cache
        self.shared_embedding_cache = SharedEmbeddingCache(vector_store)

        self.initial_retrievers = []
        self.graph_retrievers = []
        
        # initialize retrievers
        if retrievers:
            for retriever in retrievers:
                if isinstance(retriever, type):
                    instance = retriever(
                        vector_store, 
                        graph_store, 
                        **kwargs
                    )
                else:
                    instance = retriever
                
                # Inject shared cache if not already set
                if hasattr(instance, 'embedding_cache') and instance.embedding_cache is None:
                    instance.embedding_cache = self.shared_embedding_cache
                
                if isinstance(instance, (SemanticBeamGraphSearch, RerankingBeamGraphSearch)):
                    self.graph_retrievers.append(instance)
                else:
                    self.initial_retrievers.append(instance)
        else:
            # Default configuration
            self.initial_retrievers = [
                StatementCosineSimilaritySearch(
                    vector_store=vector_store,
                    graph_store=graph_store,
                    embedding_cache=self.shared_embedding_cache,
                    **kwargs
                ),
                KeywordRankingSearch(
                    vector_store=vector_store,
                    graph_store=graph_store,
                    embedding_cache=self.shared_embedding_cache,
                    **kwargs
                )
            ]

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """
        Retrieves and processes nodes based on a query, leveraging multiple retrievers and
        optional graph expansion. The method executes in several stages, including initial
        retrieval, deduplication of nodes, optional graph-based expansion, fetching detailed
        statement data, filtering nodes based on metadata, and grouping nodes by their source
        for final ordering. It returns an ordered list of nodes, maximizing relevance and
        context.

        Args:
            query_bundle (QueryBundle): The query bundle containing the necessary information
                for performing the retrieval process.

        Returns:
            List[NodeWithScore]: A list of nodes ordered by score, with detailed metadata
                included for contextual relevance.
        """
        # 1. Get initial results in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.initial_retrievers)) as p:
            initial_results = list(p.map(
                lambda r, query: r.retrieve(query), 
                self.initial_retrievers, 
                repeat(query_bundle)
            ))
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'initial_results: {initial_results}')
        else:
            logger.debug(f'num initial_results: {len(initial_results)}')

        # 2. Collect unique initial nodes
        seen_statement_ids = set()
        initial_nodes = []
        for nodes in initial_results:
            for node in nodes:
                statement_id = node.node.metadata['statement']['statementId']
                if statement_id not in seen_statement_ids:
                    seen_statement_ids.add(statement_id)
                    initial_nodes.append(node)

        all_nodes = initial_nodes.copy()

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'all_nodes (before expansion): {all_nodes}')
        else:
            logger.debug(f'num all_nodes (before expansion): {len(all_nodes)}')

        # 3. Graph expansion if enabled
        if self.share_results and initial_nodes:
            for retriever in self.graph_retrievers:
                try:
                    retriever.shared_nodes = initial_nodes
                    graph_nodes = retriever.retrieve(query_bundle)
                    for node in graph_nodes:
                        statement_id = node.node.metadata['statement']['statementId']
                        if statement_id not in seen_statement_ids:
                            seen_statement_ids.add(statement_id)
                            all_nodes.append(node)
                except Exception as e:
                    logger.error(f"Error in graph retriever {retriever.__class__.__name__}: {e}")
                    continue

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:            
            logger.debug(f'all_nodes (after expansion): {all_nodes}')
        else:
            logger.debug(f'num all_nodes (after expansion): {len(all_nodes)}')

        # 4. Fetch statements once
        if not all_nodes:
            return []

        statement_ids = [
            node.node.metadata['statement']['statementId'] 
            for node in all_nodes
        ]
        statements = get_statements_query(self.graph_store, statement_ids)

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'statements: {statements}')
        else:
            logger.debug(f'num statements: {len(statements)}')
        

        # 5. Create final nodes with full data
        final_nodes = []
        statements_map = {
            s['result']['statement']['statementId']: s['result'] 
            for s in statements
        }
        
        for node in all_nodes:
            statement_id = node.node.metadata['statement']['statementId']
            if statement_id in statements_map:
                result = statements_map[statement_id]
                new_node = TextNode(
                    text=result['statement']['value'],
                    metadata={
                        **node.node.metadata,  # Preserve retriever metadata
                        'statement': result['statement'],
                        'chunk': result['chunk'],
                        'source': result['source']                     
                    }
                )
                final_nodes.append(NodeWithScore(
                    node=new_node,
                    score=node.score
                ))

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:       
            logger.debug(f'final_nodes: {final_nodes}')
        else:
            logger.debug(f'num final_nodes: {len(final_nodes)}')

        # 6. Apply metadata filters
        filtered_nodes = [
            node 
            for node in final_nodes 
            if self.filter_config.filter_source_metadata_dictionary(node.node.metadata['source']['metadata'])    
        ]

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:       
            logger.debug(f'filter_nodes: {filtered_nodes}')
        else:
            logger.debug(f'num filter_nodes: {len(filtered_nodes)}')

        # 7. Group by source for better context
        source_nodes = defaultdict(list)
        for node in filtered_nodes:
            source_id = node.node.metadata['source']['sourceId']
            source_nodes[source_id].append(node)

        # 8. Create final ordered list
        ordered_nodes = []
        for source_id, nodes in source_nodes.items():
            nodes.sort(key=lambda x: coalesce(x.score, 0.0), reverse=True)
            ordered_nodes.extend(nodes)

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:    
            logger.debug(f'ordered_nodes: {ordered_nodes}')
        else:
            logger.debug(f'num ordered_nodes: {len(ordered_nodes)}')

        return ordered_nodes
