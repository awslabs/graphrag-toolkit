# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import concurrent.futures
from typing import List, Optional, Type, Union
from itertools import repeat

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import TraversalBasedBaseRetriever
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_guided_base_chunk_retriever import SemanticGuidedBaseChunkRetriever
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_cosine_search import ChunkCosineSimilaritySearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.semantic_chunk_beam_search import SemanticChunkBeamGraphSearch
from graphrag_toolkit.lexical_graph.retrieval.utils.chunk_utils import SharedChunkEmbeddingCache

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

SemanticGuidedChunkRetrieverType = Union[SemanticGuidedBaseChunkRetriever, Type[SemanticGuidedBaseChunkRetriever]]

class ChunkBasedSemanticSearch(TraversalBasedBaseRetriever):
    
    def __init__(self,
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 processor_args:Optional[ProcessorArgs]=None,
                 processors:Optional[List[Type[ProcessorBase]]]=None,
                 filter_config:Optional[FilterConfig]=None,
                 retrievers:Optional[List[Union[SemanticGuidedBaseChunkRetriever, Type[SemanticGuidedBaseChunkRetriever]]]]=None,
                 share_results:bool=True,
                 **kwargs):

        super().__init__(
            graph_store=graph_store, 
            vector_store=vector_store,
            processor_args=processor_args,
            processors=processors,
            filter_config=filter_config,
            **kwargs
        )

        self.share_results = share_results
        
        # Create shared embedding cache
        self.shared_embedding_cache = SharedChunkEmbeddingCache(vector_store)

        self.initial_retrievers = []
        self.graph_retrievers = []

        retrievers = retrievers or [
            ChunkCosineSimilaritySearch(
                vector_store=vector_store,
                graph_store=graph_store,
                top_k=self.args.chunk_cosine_top_k,
                filter_config=filter_config
            ),
            SemanticChunkBeamGraphSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                max_depth=self.args.chunk_beam_max_depth,
                beam_width=self.args.chunk_beam_width,
                filter_config=filter_config
            )
        ]
        
        # initialize retrievers
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
        
            
    
    def chunk_based_graph_search(self, chunk_id):


        cypher = f'''// chunk-based semantic graph search                                  
        MATCH (l)-[:`__BELONGS_TO__`]->()-[:`__MENTIONED_IN__`]->(c:`__Chunk__`)
        WHERE {self.graph_store.node_id("c.chunkId")} = $chunkId
        RETURN DISTINCT {self.graph_store.node_id("l.statementId")} AS l LIMIT $statementLimit
        '''

        properties = {
            'chunkId': chunk_id,
            'statementLimit': self.args.intermediate_limit
        }

        results = self.graph_store.execute_query(cypher, properties)
        statement_ids = [r['l'] for r in results]

        return self.get_statements_by_topic_and_source(statement_ids)


    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:

        start = time.time()

        logger.debug('Getting start node ids for chunk-based semantic search...')

        # 1. Get initial results in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.initial_retrievers)) as p:
            initial_results = list(p.map(
                lambda r, query: r.retrieve(query), 
                self.initial_retrievers, 
                repeat(query_bundle)
            ))

        end_initial = time.time()
        initial_ms = (end_initial-start) * 1000
        
        if logger.isEnabledFor(logging.DEBUG) and self.args.debug_results:
            logger.debug(f'initial_results: {initial_results} [{initial_ms:.2f}ms]')
        else:
            logger.debug(f'num initial_results: {len(initial_results)} [{initial_ms:.2f}ms]')

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

        end_collect = time.time()
        collect_ms = (end_collect-end_initial) * 1000

        if logger.isEnabledFor(logging.DEBUG) and self.args.debug_results:
            logger.debug(f'all_nodes (before expansion): {all_nodes} [{collect_ms:.2f}ms]')
        else:
            logger.debug(f'num all_nodes (before expansion): {len(all_nodes)} [{collect_ms:.2f}ms]')

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

        end_expansion = time.time()
        expansion_ms = (end_expansion-end_collect) * 1000
       
        if logger.isEnabledFor(logging.DEBUG) and self.args.debug_results:            
            logger.debug(f'all_nodes (after expansion): {all_nodes} [{expansion_ms:.2f}ms]')
        else:
            logger.debug(f'num all_nodes (after expansion): {len(all_nodes)} [{expansion_ms:.2f}ms]')

        # 4. Fetch statements once
        if not all_nodes:
            return []

        chunk_ids = [
            node.node.metadata['chunk']['chunkId'] 
            for node in all_nodes
        ]
        
        end_semantic_search = time.time()
        semantic_search_ms = (end_semantic_search-start) * 1000
        
        logger.debug(f'End getting start node ids for chunk-based semantic search [{semantic_search_ms:.2f}ms]')

        return chunk_ids
        
    def do_graph_search(self, query_bundle: QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:

        start = time.time()
        
        chunk_ids = start_node_ids

        logger.debug('Running chunk-based semantic search...')
        
        search_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.num_workers) as executor:

            futures = [
                executor.submit(self.chunk_based_graph_search, chunk_id)
                for chunk_id in chunk_ids
            ]
            
            executor.shutdown()

            for future in futures:
                for result in future.result():
                    search_results.append(result)
                    
        search_results_collection = self._to_search_results_collection(search_results) 
        
        retriever_name = type(self).__name__
        if retriever_name in self.args.debug_results and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'''Chunk-based semantic search results: {search_results_collection.model_dump_json(
                    indent=2, 
                    exclude_unset=True, 
                    exclude_defaults=True, 
                    exclude_none=True, 
                    warnings=False)
                }''')
            
        end_semantic_search = time.time()
        semantic_search_ms = (end_semantic_search-start) * 1000
        
        logger.debug(f'End chunk-based semantic search [{semantic_search_ms:.2f}ms]')
                   
        
        return search_results_collection
    
