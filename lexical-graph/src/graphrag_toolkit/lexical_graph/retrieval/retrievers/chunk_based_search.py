# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import concurrent.futures
from typing import List, Optional, Type

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorBase, ProcessorArgs
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import TraversalBasedBaseRetriever
from graphrag_toolkit.lexical_graph.retrieval.utils.vector_utils import get_diverse_vss_elements

from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

class ChunkBasedSearch(TraversalBasedBaseRetriever):
    """
    Class for performing chunk-based search in a graph store using traversal methods.

    This class extends the TraversalBasedBaseRetriever to implement functionality for
    retrieving data from a graph database. The retrieval is based on chunk-oriented
    principles, employing parallelized search techniques to enable efficient data
    retrieval for complex queries. It processes queries through graph traversal,
    facilitates use of external vector stores, and supports custom filtering.

    Attributes:
        graph_store (GraphStore): The graph store instance used for retrieving data.
        vector_store (VectorStore): The vector store instance used for vector-based searches.
        processor_args (Optional[ProcessorArgs]): Configuration arguments for data processors.
        processors (Optional[List[Type[ProcessorBase]]]): List of data processors applied
            during the search process.
        filter_config (Optional[FilterConfig]): Configuration for filtering search results.
    """
    def __init__(self,
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 processor_args:Optional[ProcessorArgs]=None,
                 processors:Optional[List[Type[ProcessorBase]]]=None,
                 filter_config:Optional[FilterConfig]=None,
                 **kwargs):
        """
        Initializes an instance of a class by setting up graph and vector stores, optional
        processing configurations, and other keyword arguments. This constructor is used
        to setup necessary parameters and configurations to enable the functionality
        of the class instance. It manages dependencies, configurations, and extensions
        that enable enhanced data handling and processing.

        Args:
            graph_store: The storage mechanism for graph data.
            vector_store: The storage mechanism for vector data.
            processor_args: Optional set of arguments that configure how
                processors handle incoming operations.
            processors: Optional list of processor types to handle specific operations
                or processing tasks.
            filter_config: Optional configuration data for filtering certain types of
                content or operations.
            **kwargs: Additional keyword arguments to pass into the parent
                initialization.
        """
        super().__init__(
            graph_store=graph_store, 
            vector_store=vector_store,
            processor_args=processor_args,
            processors=processors,
            filter_config=filter_config,
            **kwargs
        )
    
    def chunk_based_graph_search(self, chunk_id):
        """
        Performs a graph search query based on a specific chunk ID. The function constructs
        a Cypher query to search the graph database, matching relationships and nodes linked
        to a given chunk ID. The query focuses on retrieving related statements, topics,
        and associated chunks within defined query and statement limits.

        Args:
            chunk_id: The unique identifier of the chunk to search for in the graph database.

        Returns:
            List[dict]: A list of results retrieved from the graph database based on the
            provided chunk ID and query parameters.
        """

        cypher = f'''// chunk-based graph search                                  
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
        """
        Gets the starting node IDs based on the given query bundle for chunk-based search.

        This function retrieves diverse data elements classified as "chunks" that match
        the criteria specified in the query bundle, utilizing a vector store for searching
        and additional filtering configurations. It then extracts and returns the unique
        chunk IDs for further processing.

        Args:
            query_bundle (QueryBundle): The query object containing the search parameters
                and configurations.

        Returns:
            List[str]: A list of starting node IDs corresponding to the retrieved data chunks.

        """
        logger.debug('Getting start node ids for chunk-based search...')

        # CompositeTraversalBasedRetriever may attach a pre-kicked-off future
        # for this call (see its _retrieve override). Pop to consume so a reused
        # instance doesn't pick up a stale future on the next call.
        prefetched = self.__dict__.pop('_prefetched_chunks', None)
        if prefetched is not None:
            chunks = prefetched.result()
        else:
            chunks = get_diverse_vss_elements(
                'chunk',
                query_bundle,
                self.vector_store,
                self.args.vss_diversity_factor,
                self.args.vss_top_k,
                self.filter_config
            )

        return [chunk['chunk']['chunkId'] for chunk in chunks]
        
    def do_graph_search(self, query_bundle: QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:
        """
        Performs graph search using chunk-based retrieval strategy starting from given node IDs
        and executing concurrent search with thread pooling.

        This method conducts a search operation on a graph where starting points (chunks)
        are defined, and concurrent retrieval is used to collect results from multiple
        chunks in parallel. The search is based on the `chunk_based_graph_search` method,
        and the retrieval results are aggregated into a `SearchResultCollection`. It also
        logs detailed debug information when debugging is enabled.

        Args:
            query_bundle (QueryBundle): An object containing the query parameters and
                metadata required for performing the search.
            start_node_ids (List[str]): A list of starting node identifiers that mark
                the initial points for the graph search.

        Returns:
            SearchResultCollection: An aggregated collection of search results obtained
            from chunk-based graph search.
        """
        chunk_ids = start_node_ids

        logger.debug('Running chunk-based search...')
        
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
            logger.debug(f'''Chunk-based results: {search_results_collection.model_dump_json(
                    indent=2, 
                    exclude_unset=True, 
                    exclude_defaults=True, 
                    exclude_none=True, 
                    warnings=False)
                }''')
                   
        
        return search_results_collection
    
