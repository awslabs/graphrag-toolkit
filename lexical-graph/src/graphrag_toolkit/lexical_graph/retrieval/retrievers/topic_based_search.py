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

from graphrag_toolkit.core.types import QueryBundle

logger = logging.getLogger(__name__)

class TopicBasedSearch(TraversalBasedBaseRetriever):
    """
    A retriever class implementing topic-based search within a knowledge graph.

    The `TopicBasedSearch` class specializes in retrieving information from a
    graph database using a topic-based approach. It extends the
    `TraversalBasedBaseRetriever` to provide functionality specific to exploring
    the graph based on topic relationships. The retriever employs both a graph
    store for hierarchical relationships and a vector store for semantic queries.
    This is particularly useful in scenarios where topic-centric information
    organization and retrieval are required.

    Attributes:
        graph_store (GraphStore): An instance of the backing graph database used to
            execute cypher queries.
        vector_store (VectorStore): A vector store used for semantic representation
            and querying.
        processor_args (Optional[ProcessorArgs]): Optional arguments for
            configuring preprocessing workflows.
        processors (Optional[List[Type[ProcessorBase]]]): A list of processor
            classes for custom data preprocessing.
        filter_config (FilterConfig): Configuration for how filtering should be
            applied to retrieve results.
    """
    def __init__(self,
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 processor_args:Optional[ProcessorArgs]=None,
                 processors:Optional[List[Type[ProcessorBase]]]=None,
                 filter_config:FilterConfig=None,
                 **kwargs):
        """
        Initializes an instance of the class with specified configurations for graph storage, vector
        storage, processing arguments, processors, and filter configuration. This constructor
        also accepts additional keyword arguments to support customization or extension.

        Args:
            graph_store: The object responsible for storing and managing graph data.
            vector_store: The storage system designed to handle vectorized data.
            processor_args: Optional settings or configurations relevant to data processing,
                provided as `ProcessorArgs`. Defaults to None.
            processors: Optional list of `ProcessorBase` types used for performing specific
                processing tasks. Defaults to None.
            filter_config: Configuration object that defines filtering rules or settings
                as `FilterConfig`. Defaults to None.
            **kwargs: Additional keyword arguments for further customization or
                extension of functionality.
        """
        super().__init__(
            graph_store=graph_store, 
            vector_store=vector_store,
            processor_args=processor_args,
            processors=processors,
            filter_config=filter_config,
            **kwargs
        )
    
    def topic_based_graph_search(self, topic_id):
        """
        Performs a graph search based on a specific topic ID. The method uses a Cypher
        query to traverse a graph database, retrieving relevant `__Fact__` and associated
        `__Statement__` nodes connected to a specific topic.

        The query traverses relationships such as `__SUPPORTS__`, and
        `__BELONGS_TO__` to ensure that all relevant nodes and their connections are
        retrieved based on the provided topic ID and query limits.

        Args:
            topic_id (str): The ID of the topic for which the graph search will be performed.

        Returns:
            Any: Results from the graph database query represented in the format returned
            by the `execute_query` method of the `graph_store`.

        Raises:
            Any error or exception raised by the `self.graph_store.execute_query` method.
        """

        cypher = f'''// topic-based graph search                                  
        MATCH (f)-[:`__SUPPORTS__`]->()-[:`__BELONGS_TO__`]->(tt:`__Topic__`)
        WHERE {self.graph_store.node_id("tt.topicId")} = $topicId
        WITH f LIMIT $statementLimit
        MATCH (f)-[:`__SUPPORTS__`]->(l)
        RETURN DISTINCT {self.graph_store.node_id("l.statementId")} AS l LIMIT $statementLimit
        '''
                            
        properties = {
            'topicId': topic_id,
            'statementLimit': self.args.intermediate_limit
        }

        results = self.graph_store.execute_query(cypher, properties)
        statement_ids = [r['l'] for r in results]

        return statement_ids
        

    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:
        """
        Retrieves the start node IDs based on a topic-based search.

        This method extracts a list of start node IDs by performing
        a topic-based search using the provided query bundle and various
        configuration parameters. It utilizes a diverse set of topic elements
        from the vector store combined with filtering conditions.

        Args:
            query_bundle (QueryBundle): The query bundle containing the query
                details to use for searching topics.

        Returns:
            List[str]: A list of start node IDs extracted from the topics.
        """
        logger.debug('Getting start node ids for topic-based search...')

        topics = get_diverse_vss_elements(
            'topic', 
            query_bundle, 
            self.vector_store, 
            self.args.vss_diversity_factor, 
            self.args.vss_top_k, 
            self.filter_config
        )
        
        return [topic['topic']['topicId'] for topic in topics]
    
    def do_graph_search(self, query_bundle: QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:
        """
        Executes a graph-based search leveraging a multi-threaded approach to perform
        topic-based searches starting from a given set of node IDs and consolidates
        the results into a unified collection.

        Args:
            query_bundle: Encapsulates information about the search query, including
                query text and contextual metadata required for graph traversal and
                search execution.
            start_node_ids: A list of initial node identifiers serving as starting
                points for the graph search.

        Returns:
            A SearchResultCollection object containing accumulated search results from
            the topic-based graph search.
        """
            
        topic_ids = start_node_ids

        logger.debug('Running topic-based search...')
        
        statement_ids = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.num_workers) as executor:

            futures = [
                executor.submit(self.topic_based_graph_search, topic_id)
                for topic_id in topic_ids
            ]
            
            executor.shutdown()

            for future in futures:
                for result in future.result():
                    statement_ids.append(result)

        search_results = self.get_statements_by_topic_and_source(list(set(statement_ids)))           
        search_results_collection = self._to_search_results_collection(search_results) 
        
        retriever_name = type(self).__name__
        if retriever_name in self.args.debug_results and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'''Topic-based results: {search_results_collection.model_dump_json(
                    indent=2, 
                    exclude_unset=True, 
                    exclude_defaults=True, 
                    exclude_none=True, 
                    warnings=False)
                }''')
                   
        
        return search_results_collection
    
