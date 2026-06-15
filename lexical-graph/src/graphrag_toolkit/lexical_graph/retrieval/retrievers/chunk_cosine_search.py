# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from typing import List, Any, Optional

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.utils.chunk_utils import get_top_k, SharedChunkEmbeddingCache
from graphrag_toolkit.lexical_graph.retrieval.retrievers.deprecated.semantic_guided_base_chunk_retriever import SemanticGuidedBaseChunkRetriever

from graphrag_toolkit.core.types import NodeWithScore, QueryBundle, Node

logger = logging.getLogger(__name__)

class ChunkCosineSimilaritySearch(SemanticGuidedBaseChunkRetriever):


    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        embedding_cache:Optional[SharedChunkEmbeddingCache]=None,
        top_k:int=100,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:

        super().__init__(vector_store, graph_store, filter_config, **kwargs)
        self.embedding_cache = embedding_cache
        self.top_k = top_k

    def retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:

        start = time.time()

        # 1. Get initial candidates from vector store via L2 Norm
        chunk_results = self.vector_store.get_index('chunk').top_k(
            query_bundle, 
            top_k=5,
            filter_config=self.filter_config
        )

        end_initial = time.time()
        initial_ms = (end_initial-start) * 1000
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'chunk_results: {chunk_results} [{initial_ms:.2f}ms]')
        else:
            logger.debug(f'num chunk_results: {len(chunk_results)} [{initial_ms:.2f}ms]')
        
        # 2. Get chunk IDs and embeddings using shared cache
        chunk_ids = [r['chunk']['chunkId'] for r in chunk_results]
        chunk_embeddings = self.embedding_cache.get_embeddings(chunk_ids)

        # 3. Get top-k chunks by cosine similarity
        top_k_chunks = get_top_k(
            query_bundle.embedding,
            chunk_embeddings,
            self.top_k
        )

        end_top_k = time.time()
        top_k_ms = (end_top_k-end_initial) * 1000
        
        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'top_k_chunks: {top_k_chunks} [{top_k_ms:.2f}ms]')
        else:
            logger.debug(f'num top_k_chunks: {len(top_k_chunks)} [{top_k_ms:.2f}ms]')

        # 4. Create nodes with minimal data
        nodes = []
        for score, chunk_id in top_k_chunks:
            node = Node(
                text="",  # Placeholder - will be populated by StatementGraphRetriever
                metadata={
                    'chunk': {'chunkId': chunk_id},
                    'search_type': 'cosine_similarity'
                }
            )
            nodes.append(NodeWithScore(node=node, score=score))

        end_nodes = time.time()
        nodes_ms = (end_nodes-end_top_k) * 1000

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results: 
            logger.debug(f'nodes: {nodes} [{nodes_ms:.2f}ms]')
        else:
            logger.debug(f'num nodes: {len(nodes)} [{nodes_ms:.2f}ms]')

        return nodes
