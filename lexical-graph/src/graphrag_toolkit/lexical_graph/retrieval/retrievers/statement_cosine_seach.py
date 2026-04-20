# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Any, Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.statement_utils import get_top_k, SharedEmbeddingCache
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_retriever import SemanticGuidedBaseRetriever

from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)

class StatementCosineSimilaritySearch(SemanticGuidedBaseRetriever):
    """
    Implements a semantic-guided retriever using cosine similarity for statement search.

    This class performs semantic-guided retrieval of statements utilizing cosine similarity as the
    metric for ranking. It integrates with vector and graph stores for candidate selection, employs
    an optional shared embedding cache for efficient embedding retrieval, and identifies the most
    relevant statements by calculating cosine similarity scores. It is specifically designed to process
    query bundles and return a ranked list of matched statements wrapped as nodes with associated scores.

    Attributes:
        embedding_cache (Optional[SharedEmbeddingCache]): Shared cache for efficiently retrieving
            statement embeddings.
        top_k (int): Number of top statements to retrieve based on cosine similarity.
    """

    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        embedding_cache:Optional[SharedEmbeddingCache]=None,
        top_k:int=100,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes a class instance with the given parameters and sets up the required
        stores, configurations, and cache for processing tasks.

        Args:
            vector_store: VectorStore instance responsible for handling vector-related
                operations and data storage.
            graph_store: GraphStore instance responsible for handling storage and
                operations related to the graph structure.
            embedding_cache: Optional embedding cache (SharedEmbeddingCache) to store
                and retrieve precomputed embeddings for faster access.
            top_k: An integer indicating the number of top elements to consider during
                specific computations or querying.
            filter_config: Optional FilterConfig instance to define the filtering
                criteria or configuration for data processing.
            **kwargs: Additional keyword arguments for further custom configuration
                or initialization.
        """
        super().__init__(vector_store, graph_store, filter_config, **kwargs)
        self.embedding_cache = embedding_cache
        self.top_k = top_k

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """
        Retrieves the relevant nodes for a given query by first performing a nearest neighbor search
        on the vector store and then re-ranking the results based on cosine similarity.
        The process involves multiple stages: fetching candidates, retrieving embeddings,
        re-ranking by similarity, and preparing the output nodes.

        Args:
            query_bundle (QueryBundle): Input query bundle containing the query and its embedding.

        Returns:
            List[NodeWithScore]: List of nodes paired with their respective similarity scores. The nodes
            are initialized with minimal data, primarily serving metadata about the retrieval process.
        """
        # 1. Get initial candidates from vector store via L2 Norm
        statement_results = self.vector_store.get_index('statement').top_k(
            query_bundle, 
            top_k=self.top_k,
            filter_config=self.filter_config
        )
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'statement_results: {statement_results}')
        else:
            logger.debug(f'num statement_results: {len(statement_results)}')
        
        # 2. Get statement IDs and embeddings using shared cache
        statement_ids = [r['statement']['statementId'] for r in statement_results]
        statement_embeddings = self.embedding_cache.get_embeddings(statement_ids)

        # 3. Get top-k statements by cosine similarity
        top_k_statements = get_top_k(
            query_bundle.embedding,
            statement_embeddings,
            self.top_k
        )
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'top_k_statements: {top_k_statements}')
        else:
            logger.debug(f'num top_k_statements: {len(top_k_statements)}')

        # 4. Create nodes with minimal data
        nodes = []
        for score, statement_id in top_k_statements:
            node = TextNode(
                text="",  # Placeholder - will be populated by StatementGraphRetriever
                metadata={
                    'statement': {'statementId': statement_id},
                    'search_type': 'cosine_similarity'
                }
            )
            nodes.append(NodeWithScore(node=node, score=score))

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results: 
            logger.debug(f'nodes: {nodes}')
        else:
            logger.debug(f'num nodes: {len(nodes)}')

        return nodes
