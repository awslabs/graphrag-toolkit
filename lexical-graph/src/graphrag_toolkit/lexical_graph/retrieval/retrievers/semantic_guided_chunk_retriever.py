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

from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_chunk_retriever import SemanticGuidedBaseChunkRetriever
#from graphrag_toolkit.lexical_graph.retrieval.retrievers.keyword_ranking_search import KeywordRankingSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_cosine_search import ChunkCosineSimilaritySearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_chunk_beam_search import SemanticChunkBeamGraphSearch
#from graphrag_toolkit.lexical_graph.retrieval.retrievers.rerank_beam_search import RerankingBeamGraphSearch
from graphrag_toolkit.lexical_graph.retrieval.utils.chunk_utils import get_chunks_query, SharedChunkEmbeddingCache

logger = logging.getLogger(__name__)

SemanticGuidedChunkRetrieverType = Union[SemanticGuidedBaseChunkRetriever, Type[SemanticGuidedBaseChunkRetriever]]

class SemanticGuidedChunkRetriever(SemanticGuidedBaseChunkRetriever):

    def __init__(
        self,
        vector_store:VectorStore,
        graph_store:GraphStore,
        retrievers:Optional[List[Union[SemanticGuidedBaseChunkRetriever, Type[SemanticGuidedBaseChunkRetriever]]]]=None,
        share_results:bool=True,
        filter_config:Optional[FilterConfig]=None,
        **kwargs: Any,
    ) -> None:


        super().__init__(vector_store, graph_store, filter_config, **kwargs)

        self.share_results = share_results
        
        # Create shared embedding cache
        self.shared_embedding_cache = SharedChunkEmbeddingCache(vector_store)

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
                
                if isinstance(instance, (SemanticChunkBeamGraphSearch)):
                    self.graph_retrievers.append(instance)
                else:
                    self.initial_retrievers.append(instance)
        else:
            # Default configuration
            self.initial_retrievers = [
                ChunkCosineSimilaritySearch(
                    vector_store=vector_store,
                    graph_store=graph_store,
                    embedding_cache=self.shared_embedding_cache,
                    **kwargs
                )
            ]

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:

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
        seen_chunk_ids = set()
        initial_nodes = []
        for nodes in initial_results:
            for node in nodes:
                chunk_id = node.node.metadata['chunk']['chunkId']
                if chunk_id not in seen_chunk_ids:
                    seen_chunk_ids.add(chunk_id)
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
                        chunk_id = node.node.metadata['chunk']['chunkId']
                        if chunk_id not in seen_chunk_ids:
                            seen_chunk_ids.add(chunk_id)
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

        chunk_ids = [
            node.node.metadata['chunk']['chunkId'] 
            for node in all_nodes
        ]
        chunks = get_chunks_query(self.graph_store, chunk_ids)

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:
            logger.debug(f'chunks: {chunks}')
        else:
            logger.debug(f'num chunks: {len(chunks)}')
        

        # 5. Create final nodes with full data
        final_nodes = []
        chunks_map = {
            s['result']['chunk']['chunkId']: s['result'] 
            for s in chunks
        }
        
        for node in all_nodes:
            chunk_id = node.node.metadata['chunk']['chunkId']
            if chunk_id in chunks_map:
                result = chunks_map[chunk_id]
                new_node = TextNode(
                    text=result['chunk']['value'],
                    metadata={
                        **node.node.metadata,  # Preserve retriever metadata
                        'statement': result['chunk'],
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
            nodes.sort(key=lambda x: x.score or 0.0, reverse=True)
            ordered_nodes.extend(nodes)

        if logger.isEnabledFor(logging.DEBUG) and self.debug_results:    
            logger.debug(f'ordered_nodes: {ordered_nodes}')
        else:
            logger.debug(f'num ordered_nodes: {len(ordered_nodes)}')

        return ordered_nodes
