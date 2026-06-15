# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import math
import concurrent.futures
from dataclasses import dataclass
from itertools import repeat
from typing import List, Type, Optional, Union, Iterator, cast

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.retrievers.traversal_based_base_retriever import TraversalBasedBaseRetriever
from graphrag_toolkit.lexical_graph.retrieval.utils.query_decomposition import QueryDecomposition
from graphrag_toolkit.lexical_graph.retrieval.retrievers.entity_network_search import EntityNetworkSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers.chunk_based_search import ChunkBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult

from graphrag_toolkit.core.types import QueryBundle, NodeWithScore

logger = logging.getLogger(__name__)

TraversalBasedRetrieverType = Union[TraversalBasedBaseRetriever, Type[TraversalBasedBaseRetriever]]

@dataclass
class WeightedTraversalBasedRetriever:
    """
    A retriever class that assigns a weight to a traversal-based retriever.

    This class is used to assign a specific weight to a `TraversalBasedRetrieverType` instance.
    It is helpful when combining multiple retrievers with varied importance levels.
    The `weight` attribute enables flexible scoring adjustments for a retriever's results.

    Attributes:
        retriever (TraversalBasedRetrieverType): An instance of the retrieval mechanism
            that operates based on traversal logic.
        weight (float): A multiplier to adjust the significance of the retriever's
            results. Defaults to 1.0.
    """
    retriever:TraversalBasedRetrieverType
    weight:float=1.0

DEFAULT_TRAVERSAL_BASED_RETRIEVERS = [
    WeightedTraversalBasedRetriever(retriever=ChunkBasedSearch, weight=1.0), 
    WeightedTraversalBasedRetriever(retriever=EntityNetworkSearch, weight=1.0)
]

WeightedTraversalBasedRetrieverType = Union[WeightedTraversalBasedRetriever, TraversalBasedBaseRetriever, Type[TraversalBasedBaseRetriever]]

class CompositeTraversalBasedRetriever(TraversalBasedBaseRetriever):
    """
    Handles composite traversal-based retrieval with decomposition and weighting of sub-retrievers.

    This class is a specialized retriever that integrates multiple weighted traversal-based retrievers.
    It allows for query decomposition into subqueries and combines the results from different retrieval
    strategies, providing a unified search result. It supports advanced configurations such as
    graph-based and vector-based retrieval integrations. The retrieval process leverages the
    functionality of `TraversalBasedBaseRetriever` while enabling dynamic interaction with sub-retrievers
    and query decomposition mechanisms.

    Attributes:
        query_decomposition (QueryDecomposition): Mechanism for decomposing queries into subqueries.
        weighted_retrievers (List[WeightedTraversalBasedRetrieverType]): A list of retrievers with
            associated weights for weighted retrievals.
    """
    def __init__(self, 
                 graph_store:GraphStore, 
                 vector_store:VectorStore,
                 retrievers:Optional[List[WeightedTraversalBasedRetrieverType]]=None,
                 query_decomposition:Optional[QueryDecomposition]=None,
                 filter_config:FilterConfig=None,
                 **kwargs):
        """
        Initializes an instance of a class that manages graph and vector stores, combines them
        with optional retrievers, a query decomposition mechanism, and a filter configuration.
        This class is designed to perform tasks involving traversal across a graph-like structure
        with weighted retrievers as well as facilitate query decomposition for enhanced search
        capabilities. Additional configurations can be set via keyword arguments.

        Args:
            graph_store: The storage mechanism for managing a graph-like data structure.
            vector_store: The storage mechanism for managing vectorized representations of the data.
            retrievers: A list of retrievers that work based on traversal in the graph, optionally
                weighted to prioritize certain paths or retrievers. Defaults to a predefined list
                of retrievers if not provided.
            query_decomposition: An optional query decomposition tool for breaking down complex
                queries into subqueries. Defaults to a new instance of `QueryDecomposition`
                initialized with `max_subqueries` value from `args` if not provided.
            filter_config: Configuration object for filtering results based on specific criteria.
            **kwargs: Additional keyword arguments to configure other settings or behavior.
        """
        super().__init__(
            graph_store=graph_store, 
            vector_store=vector_store,
            filter_config=filter_config,
            **kwargs
        )

        self.query_decomposition = query_decomposition or QueryDecomposition(self.args, max_subqueries=self.args.max_subqueries)

        retrievers = retrievers or DEFAULT_TRAVERSAL_BASED_RETRIEVERS

        self.weighted_retrievers:List[WeightedTraversalBasedRetrieverType] = [
            r if isinstance(r, WeightedTraversalBasedRetriever) else WeightedTraversalBasedRetriever(retriever=r, weight=1.0)
            for r in retrievers
        ]

    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:
        """
        Gets the starting node IDs for a given query.

        This function computes and returns the initial set of node
        identifiers that are relevant for the input query. The
        computation is based on the query details encapsulated
        within the `QueryBundle` object.

        Args:
            query_bundle: Contains the details of the query against
                which the starting node IDs are to be determined.

        Returns:
            List[str]: A list of node IDs representing the starting
            points for the query.
        """
        return []
    
    def _get_search_results_for_query(self, query_bundle: QueryBundle) -> SearchResultCollection:
        """
        Generates a collection of search results based on a given query bundle, utilizing
        various retrievers and employing weighted retriever configurations to process the
        query. This method coordinates multiple stages, including entity extraction,
        retriever configuration, and parallel query execution, to aggregate and return
        relevant search results and associated entities.

        Args:
            query_bundle (QueryBundle): The input query bundle containing the query to
                process, along with any additional contextual parameters required during
                retrieval.

        Returns:
            SearchResultCollection: A collection of search results along with the entities
            extracted from the query input. The results include relevant search matches
            processed by the configured retrievers.
        """
        def weighted_arg(v, weight, factor):
            """
            Represents a retrieval mechanism that relies on traversals across various components
            within a query or document-based system. This class extends the functionality
            provided by TraversalBasedBaseRetriever and customizes the search result retrieval
            process by implementing a specific query evaluation mechanism.

            Methods:
                _get_search_results_for_query: Retrieves search results for a given query
                                               bundle based on a customized scoring and
                                               weighted approach.

            """
            multiplier = min(1, weight * factor)
            return  math.ceil(v * multiplier)

        retrievers = []

        for wr in self.weighted_retrievers:
            
            if not isinstance(wr, WeightedTraversalBasedRetriever):
                wr = WeightedTraversalBasedRetriever(retriever=wr, weight=1.0)
            
            sub_args = self.args.to_dict()

            #sub_args['intermediate_limit'] = weighted_arg(self.args.intermediate_limit, wr.weight, 2)
            #sub_args['limit_per_query'] = weighted_arg(self.args.query_limit, wr.weight, 1)
            sub_args['max_search_results'] = math.ceil(self.args.max_search_results * wr.weight)
            sub_args['reranker'] = 'tfidf'
            sub_args['enrich_query'] = False

            retriever = (wr.retriever if isinstance(wr.retriever, TraversalBasedBaseRetriever) 
                         else wr.retriever(
                            self.graph_store, 
                            self.vector_store,
                            # processors=[
                            #     # No processing - just raw results
                            # ],
                            formatting_processors=[
                                # No processing - just raw results
                            ],
                            entity_contexts=self.entity_contexts,
                            filter_config=self.filter_config,
                            **sub_args
                        ))

            retrievers.append(retriever)

        search_results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.num_workers) as executor:
            scored_node_batches: Iterator[List[NodeWithScore]] = executor.map(
                lambda r, query: r.retrieve(query),
                retrievers,
                repeat(query_bundle)
            )
            scored_nodes = sum(scored_node_batches, start=cast(List[NodeWithScore], []))
            search_results = [SearchResult.model_validate_json(scored_node.node.text) for scored_node in scored_nodes]
        
        return SearchResultCollection(results=search_results, entity_contexts=self.entity_contexts)
            
    
    def do_graph_search(self, query_bundle: QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:
        """
        Performs a graph search based on the provided query and starting nodes. The function involves
        decomposing queries into subqueries if required and parallel processing those subqueries. The
        search results and entities are aggregated and returned as a single collection.

        Args:
            query_bundle (QueryBundle): The query to execute in the graph search.
            start_node_ids (List[str]): A list of IDs representing the starting nodes for the search.

        Returns:
            SearchResultCollection: An object containing the aggregated search results and entities.
        """
        

        subqueries = (self.query_decomposition.decompose_query(query_bundle) 
            if self.args.derive_subqueries 
            else [query_bundle]
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(subqueries)) as p:
            task_results = list(p.map(self._get_search_results_for_query, subqueries))

        search_results = SearchResultCollection(entity_contexts=self.entity_contexts)

        for task_result in task_results:
            for search_result in task_result.results:
                search_results.add_search_result(search_result) 
            
        
        return search_results
        