# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List, Optional, Type, Union

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_search import ChunkBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.topic_based_search import TopicBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import TraversalBasedBaseRetriever

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

SubRetrieverType = Union[ChunkBasedSearch, TopicBasedSearch, Type[ChunkBasedSearch], Type[TopicBasedSearch]]

class EntityContextSearch(TraversalBasedBaseRetriever):
    """
    A retriever implementation designed to perform entity-context-based search within a graph database.
    It retrieves relevant nodes and their contexts based on the input query and employs advanced filtering
    and scoring mechanisms to refine results.

    The EntityContextSearch class inherits from TraversalBasedBaseRetriever and combines graph traversal
    and vector search for efficient entity context retrieval. It is optimized for use in systems
    requiring complex entity relational data processing.

    Attributes:
        sub_retriever (Optional[SubRetrieverType]): An optional sub-retriever instance or class used for
            deeper result retrieval during the search process.
    """
    def __init__(self,
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 processor_args:Optional[ProcessorArgs]=None,
                 processors:Optional[List[Type[ProcessorBase]]]=None,
                 sub_retriever:Optional[SubRetrieverType]=None,
                 filter_config:Optional[FilterConfig]=None,
                 **kwargs):
        """
        Initializes an instance of the class with specified parameters. This constructor
        sets up graph and vector stores, optional processing arguments, a list of
        processors, a sub-retriever, and filter configuration to create a specific
        retrieval or searching mechanism.

        Args:
            graph_store: The graph store to be used for managing graph-based data
                structures within the entity.
            vector_store: The vector store to be used for managing vector-based
                representations of data.
            processor_args: Optional arguments for processors, specifying additional
                configuration or parameters required by the processors.
            processors: Optional list of processor classes implementing the
                ProcessorBase type, used to transform or handle data within the entity.
            sub_retriever: Optional specific retriever type to be used for
                sub-retrieval operations. Defaults to ChunkBasedSearch.
            filter_config: Optional configuration object containing filter
                criteria or rules for data refinement.
            **kwargs: Additional keyword arguments that might be required for further
                customization or parameterization.
        """
        self.sub_retriever = sub_retriever or ChunkBasedSearch
        
        super().__init__(
            graph_store=graph_store, 
            vector_store=vector_store,
            processor_args=processor_args,
            processors=processors,
            filter_config=filter_config,
            **kwargs
        )

    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:
        return []
    
    
    def _get_sub_retriever(self):
        """
        Retrieves or constructs a sub-retriever based on the type of the existing sub-retriever
        or initializes a new one with the provided configuration.

        This private method either returns an already initialized sub-retriever instance
        if it adheres to a specific type or initializes a new sub-retriever object with
        parameters derived from configuration attributes. The initialized or retrieved
        sub-retriever is logged for debugging purposes.

        Returns:
            TraversalBasedBaseRetriever: An instance of a sub-retriever ready for use.
        """
        sub_retriever = (self.sub_retriever if isinstance(self.sub_retriever, TraversalBasedBaseRetriever)
                         else self.sub_retriever(
                            self.graph_store, 
                            self.vector_store, 
                            entity_contexts=self.entity_contexts,
                            vss_top_k=2,
                            max_search_results=2,
                            vss_diversity_factor=self.args.vss_diversity_factor,
                            include_facts=self.args.include_facts,
                            filter_config=self.filter_config,
                            ecs_max_contexts=self.args.ec_max_contexts
                        ))
        logger.debug(f'sub_retriever: {type(sub_retriever).__name__}')
        return sub_retriever
    
    def do_graph_search(self, query_bundle:QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:
        """
        Executes a graph-based search query starting from provided node IDs and processes the results.

        This method performs an entity-context-based search using given starting node IDs, constructs
        entity contexts, retrieves associated search results, and aggregates those results into a
        collection. It also provides debug logs if debugging is enabled for the specific retriever.

        Args:
            query_bundle (QueryBundle): The query bundle containing the search query information.
            start_node_ids (List[str]): A list of starting node IDs to base the search on.

        Returns:
            SearchResultCollection: A collection of search results aggregated from the entity-context-based search.
        """
        logger.debug('Running entity-context-based search...')

        sub_retriever = self._get_sub_retriever()
        
        entity_contexts = self.entity_contexts.all_context_strs

        search_results = []

        if self.args.ec_max_contexts:
            for entity_context in entity_contexts[:self.args.ec_max_contexts]:
                if entity_context:
                    results = sub_retriever.retrieve(QueryBundle(query_str=entity_context))
                    for result in results:
                        search_results.append(SearchResult.model_validate(result.metadata['result']))
                    
                
        search_results_collection = self._to_search_results_collection(search_results) 
        
        retriever_name = type(self).__name__
        
        if retriever_name in self.args.debug_results and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'''Entity context results: {search_results_collection.model_dump_json(
                    indent=2, 
                    exclude_unset=True, 
                    exclude_defaults=True, 
                    exclude_none=True, 
                    warnings=False)
                }''')
        
        return search_results_collection