# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import concurrent.futures
from typing import List, Optional, Type

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector.dummy_vector_index import DummyVectorIndex
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import TraversalBasedBaseRetriever

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

class EntityNetworkSearch(TraversalBasedBaseRetriever):
   
    def __init__(self,
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 processor_args:Optional[ProcessorArgs]=None,
                 processors:Optional[List[Type[ProcessorBase]]]=None,
                 filter_config:Optional[FilterConfig]=None,
                 **kwargs):
        """
        Initializes the class instance with the provided parameters and ensures that
        the base class is properly initialized as well. This constructor prepares the
        object with necessary configurations and data stores for its intended purpose.

        Args:
            graph_store: The GraphStore instance responsible for managing graph-
                based data and interactions.
            vector_store: The VectorStore instance used for handling vector-based
                storage and similarity operations.
            processor_args: Optional. A ProcessorArgs instance containing additional
                arguments for configuring processors, if provided.
            processors: Optional. A list of ProcessorBase class types for defining
                the processing pipeline, if applicable.
            filter_config: Optional. A FilterConfig instance used for applying
                filtering rules, if specified.
            **kwargs: Extra keyword arguments passed to the superclass for extended
                configurability.
        """

        self.vector_type = 'topic' if not isinstance(vector_store.get_index('topic'), DummyVectorIndex) else 'chunk'

        super().__init__(
            graph_store=graph_store, 
            vector_store=vector_store,
            processor_args=processor_args,
            processors=processors,
            filter_config=filter_config,
            **kwargs
        )

    def _graph_search(self, node_id):

        if self.vector_type == 'topic':
            cypher = f'''// topic-based entity network search                                  
            MATCH (l)-[:`__BELONGS_TO__`]->(t:`__Topic__`)
            WHERE {self.graph_store.node_id("t.topicId")} = $nodeId
            RETURN DISTINCT {self.graph_store.node_id("l.statementId")} AS l LIMIT $statementLimit
            '''
        else:
            cypher = f'''// chunk-based entity network search                                  
            MATCH (l)-[:`__BELONGS_TO__`]->()-[:`__MENTIONED_IN__`]->(c:`__Chunk__`)
            WHERE {self.graph_store.node_id("c.chunkId")} = $nodeId
            RETURN DISTINCT {self.graph_store.node_id("l.statementId")} AS l LIMIT $statementLimit
            '''

        properties = {
            'nodeId': node_id,
            'statementLimit': self.args.intermediate_limit
        }

        results = self.graph_store.execute_query(cypher, properties)
        statement_ids = [r['l'] for r in results]

        return self.get_statements_by_topic_and_source(statement_ids)

    def _get_entity_context_strings(self) -> List[str]:

        context_strs = [
            ', '.join([entity.entity.value.lower() for entity in entity_context])
            for entity_context in self.entity_contexts
        ]
    
        logger.debug(f'context_strs: {context_strs}')

        return context_strs
    
    def _get_node_ids(self, query_bundle: QueryBundle) -> List[str]:

        index_name = self.vector_type
        id_name = f'{index_name}Id'

        top_k_results = self.vector_store.get_index(index_name).top_k(query_bundle)
        node_ids = [result[index_name][id_name] for result in top_k_results]
        
        return node_ids

    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:

        start = time.time()
            
        all_start_node_ids = self._get_node_ids(query_bundle)
        
        entity_context_strs = self._get_entity_context_strings()

        for entity_context_str in entity_context_strs:
            all_start_node_ids.extend(self._get_node_ids(QueryBundle(query_str=entity_context_str)))

        start_node_ids = list(set(all_start_node_ids))

        end = time.time()
        duration_ms = (end-start) * 1000

        logger.debug(f'start_node_ids: [{self.vector_type}] {start_node_ids} ({duration_ms:.2f}ms)')
        
        return start_node_ids

    def do_graph_search(self, query_bundle:QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:
        
        logger.debug(f'Running {self.vector_type}-based entity-network search...')

        start = time.time()
        
        search_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.num_workers) as executor:

            futures = [
                executor.submit(self._graph_search, node_id)
                for node_id in start_node_ids
            ]
            
            executor.shutdown()

            for future in futures:
                for result in future.result():
                    search_results.append(result)

        end = time.time()
        duration_ms = (end-start) * 1000

        logger.debug(f'Retrieved {len(search_results)} search results for {len(start_node_ids)} {self.vector_type}s ({duration_ms:.2f}ms)')
                    
        search_results_collection = self._to_search_results_collection(search_results) 

        retriever_name = type(self).__name__
        if retriever_name in self.args.debug_results and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'''Entity-network results: {search_results_collection.model_dump_json(
                    indent=2, 
                    exclude_unset=True, 
                    exclude_defaults=True, 
                    exclude_none=True, 
                    warnings=False)
                }''')
                   
        
        return search_results_collection