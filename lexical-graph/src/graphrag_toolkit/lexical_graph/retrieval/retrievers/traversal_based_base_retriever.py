# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import abc
import time
from typing import List, Any, Type, Optional
from importlib.metadata import version, PackageNotFoundError

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.versioning import VALID_FROM, VALID_TO, EXTRACT_TIMESTAMP, BUILD_TIMESTAMP, VERSION_INDEPENDENT_ID_FIELDS, TIMESTAMP_LOWER_BOUND, TIMESTAMP_UPPER_BOUND
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector.vector_store import VectorStore
from graphrag_toolkit.lexical_graph.retrieval.query_context import KeywordProvider, KeywordVSSProvider, KeywordNLPProvider, KeywordProviderMode, PassThruKeywordProvider
from graphrag_toolkit.lexical_graph.retrieval.query_context import EntityProvider, EntityVSSProvider, EntityContextProvider
from graphrag_toolkit.lexical_graph.retrieval.model import SearchResultCollection, SearchResult, EntityContexts
from graphrag_toolkit.lexical_graph.retrieval.processors import *

from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode

logger = logging.getLogger(__name__)

DEFAULT_PROCESSORS = [
    DedupResults,
    DisaggregateResults, 
    RemoveVersioningMetadata,
    FilterByMetadata,               
    PopulateStatementStrs,
    RerankStatements,
    PruneStatements,
    RescoreResults,
    SortResults,
    TruncateStatements,
    UpdateChunkMetadata,
    ClearScores
]

DEFAULT_FORMATTING_PROCESSORS = [
    StatementsToStrings,
    SimplifySingleTopicResults,
    FormatSources,
    ClearChunks,
    ClearTopicIds,
    TruncateResults
]

class TraversalBasedBaseRetriever(BaseRetriever):
    """
    Base class for retrieval using traversal-based methods combining a graph store and a
    vector store for querying and search.

    The TraversalBasedBaseRetriever class provides foundational utilities and
    interfaces for performing data retrieval by leveraging both a graph store for
    structural information and a vector store for semantic similarity search.
    It supports customization of processing and formatting logic through processor
    classes and handles retrieval-specific configurations like filtering. This
    class is abstract and requires subclasses to implement specific retrieval
    logic for start node determination and graph search.

    Attributes:
        args (ProcessorArgs): Configuration arguments used during processor
            initialization.
        graph_store (GraphStore): The graph-based data store used for traversal
            and query execution.
        vector_store (VectorStore): The vector-based storage used for semantic
            similarity search.
        processors (List[Type[ProcessorBase]]): List of processors for retrieval
            customization. Defaults to a predefined set of processors if not provided.
        formatting_processors (List[Type[ProcessorBase]]): List of processors for
            formatting retrieved results. Defaults to a predefined set if not
            provided.
        entities (List[ScoredEntity]): List of entities for which search results
            might be associated.
        filter_config (FilterConfig): Configuration settings for applying filters
            during retrieval.
    """
    def __init__(self, 
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 processor_args:Optional[ProcessorArgs]=None,
                 processors:Optional[List[Type[ProcessorBase]]]=None,
                 formatting_processors:Optional[List[Type[ProcessorBase]]]=None,
                 entity_contexts:Optional[EntityContexts]=None,
                 filter_config:FilterConfig=None,
                 **kwargs):
        """
        Initializes a class for managing and processing entities, their relationships,
        and vectors within a given graph and vector store. This also includes the
        initialization of necessary processors and configurations for handling
        filtering and formatting tasks.

        Args:
            graph_store (GraphStore):
                A storage interface for managing and interacting with graphs
                of interconnected entities.
            vector_store (VectorStore):
                A store for managing vector representations of entities or data.
            processor_args (Optional[ProcessorArgs]):
                Optional arguments for configuring processors. Defaults to None,
                in which case it is initialized using any additional keyword
                arguments provided.
            processors (Optional[List[Type[ProcessorBase]]]):
                A list of processor classes for handling data processing tasks.
                Defaults to a predefined set of processors if None.
            formatting_processors (Optional[List[Type[ProcessorBase]]]):
                A list of formatting processor classes for structuring and formatting
                processed outputs. Defaults to a predefined list if None.
            entities (Optional[List[ScoredEntity]]):
                A list of pre-scored entities for initial processing. Defaults to
                an empty list if None.
            filter_config (FilterConfig):
                Configurations for applying filters to data or entities.
                Defaults to a new FilterConfig if not given.
            **kwargs:
                Additional keyword arguments that can be used to initialize
                processor arguments or passed as optional configurations.
        """
        self.args = processor_args or ProcessorArgs(**kwargs)
        
        self.graph_store = graph_store
        self.vector_store = vector_store
        self.processors = processors if processors is not None else DEFAULT_PROCESSORS
        self.formatting_processors = formatting_processors if formatting_processors is not None else DEFAULT_FORMATTING_PROCESSORS
        self.entity_contexts:EntityContexts = entity_contexts or EntityContexts()
        self.filter_config = filter_config or FilterConfig()
        
    def get_statements_by_topic_and_source(self, statement_ids):

        statements_params = {
            'statementLimit': self.args.intermediate_limit,
            'limit': self.args.query_limit,
            'statementIds': statement_ids
        }

        chunk_metadata = 'properties(c)' if self.args.include_chunk_details else '{}'

        statements_cypher = f'''
        // get statements grouped by topic and source
        MATCH (t)<-[:`__BELONGS_TO__`]-(l:`__Statement__`)   
              -[:`__MENTIONED_IN__`]->(c)
              -[:`__EXTRACTED_FROM__`]->(s)
        WHERE {self.graph_store.node_id("l.statementId")} in $statementIds
        WITH {{ 
                sourceId: {self.graph_store.node_id("s.sourceId")}, 
                metadata: properties(s), 
                versioning: {{
                    valid_from: coalesce(s.{VALID_FROM}, {TIMESTAMP_LOWER_BOUND}), 
                    valid_to: coalesce(s.{VALID_TO}, {TIMESTAMP_UPPER_BOUND}),
                    extract_timestamp: coalesce(s.{EXTRACT_TIMESTAMP}, {TIMESTAMP_LOWER_BOUND}),
                    build_timestamp: coalesce(s.{BUILD_TIMESTAMP}, {TIMESTAMP_LOWER_BOUND}),
                    id_fields: split(coalesce(s.{VERSION_INDEPENDENT_ID_FIELDS}, ""), ";")
                }}  
            }} AS source,
            t, l, c,
            {{ chunkId: {self.graph_store.node_id("c.chunkId")}, value: NULL, metadata: {chunk_metadata} }} AS cc, 
            {{ statementId: {self.graph_store.node_id("l.statementId")}, statement: l.value, facts: [], details: l.details, chunkId: {self.graph_store.node_id("c.chunkId")}, score: 0 }} as ll
        WITH source, 
            t, 
            collect(distinct cc) as chunks, 
            collect(ll) as statements
        WITH source,
            {{ 
                topic: t.value, 
                topicId: {self.graph_store.node_id("t.topicId")},
                chunks: chunks,
                statements: statements
            }} as topic
        WITH sum(size(topic.statements)/size(topic.chunks)) AS score, source, collect(topic) AS topics
        RETURN {{
            score: score, 
            source: source,
            topics: topics
        }} as result ORDER BY result.score DESC LIMIT $limit'''
        
        statements_results =  self.graph_store.execute_query(statements_cypher, statements_params)
    
        statement_facts_cypher = f'''// get facts for statements
        MATCH (f)-[:`__SUPPORTS__`]->(l:`__Statement__`)
        WHERE {self.graph_store.node_id("l.statementId")} in $statementIds
        RETURN {self.graph_store.node_id("l.statementId")} AS statementId, collect(distinct f.value) AS facts'''

        statement_facts_params = {
            'statementIds': statement_ids
        }

        statement_facts_results = self.graph_store.execute_query(statement_facts_cypher, statement_facts_params)

        statement_facts = {
            r['statementId']:r['facts'] for r in statement_facts_results
        }

        for statements_result in statements_results:
            result = statements_result['result']
            for topic in result['topics']:
                for statement in topic['statements']:
                    facts = statement_facts.get(statement['statementId'], [])
                    if facts:
                        statement['facts'] = facts
                        statement['score'] = len(facts)

        return statements_results
    
    def _init(self, query_bundle: QueryBundle) -> List[str]:

        if not self.entity_contexts.keywords:

            start = time.time()

            if self.args.ec_keyword_provider == 'vss':
                keyword_provider = KeywordVSSProvider(self.graph_store, self.vector_store, self.args, self.filter_config)
            elif self.args.ec_keyword_provider == 'llm':
                keyword_provider = KeywordProvider(self.args, mode=KeywordProviderMode.SIMPLE)
            elif self.args.ec_keyword_provider == 'nlp':
                keyword_provider = KeywordNLPProvider(self.args)
            elif self.args.ec_keyword_provider == 'passthru':
                keyword_provider = PassThruKeywordProvider(self.args)
            else:
                raise ValueError(f'Invalid ec_keyword_provider arg. Expected one of: llm, vss, nlp, passthru')
            
            if self.args.ec_entity_provider == 'graph':
                entity_provider = EntityProvider(self.graph_store, self.args, self.filter_config)
            elif self.args.ec_entity_provider == 'vss':
                entity_provider = EntityVSSProvider(self.graph_store, self.vector_store, self.args, self.filter_config)
            else:
                raise ValueError(f'Invalid ec_entity_provider arg. Expected one of: graph, vss')
            
            logger.debug(f'Entity context strategy: {type(keyword_provider).__name__} + {type(entity_provider).__name__}')
            
            entity_context_provider = EntityContextProvider(self.graph_store, self.args)

            keywords = keyword_provider.get_keywords(query_bundle)
            entities = entity_provider.get_entities(keywords, query_bundle)
            entity_contexts = entity_context_provider.get_entity_contexts(entities, keywords, query_bundle)

            end = time.time()
            duration_ms = (end-start) * 1000

            logger.debug(f'Retrieved {len(entity_contexts.contexts)} entity contexts ({duration_ms:.2f}ms)')

            self.entity_contexts.contexts.extend(entity_contexts.contexts)
            self.entity_contexts.keywords.extend(keywords)

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """
        Retrieves nodes with associated scores by performing a graph search and applying processing routines.

        This function performs a search operation starting from the relevant node IDs determined by
        the provided query. It then applies a series of processing steps to refine the search results and
        format them accordingly. The retrieval and processing durations are logged for performance analysis.

        Args:
            query_bundle (QueryBundle): The query input containing necessary parameters for performing the graph search.

        Returns:
            List[NodeWithScore]: A list of nodes with their associated scores, ready for further processing or display.

        """
        logger.debug(f'[{type(self).__name__}] Begin retrieve [query: {query_bundle.query_str}, args: {self.args.to_dict()}]')
        
        start_retrieve = time.time()

        self._init(query_bundle)
        
        start_node_ids = self.get_start_node_ids(query_bundle)
        search_results:SearchResultCollection = self.do_graph_search(query_bundle, start_node_ids)

        end_retrieve = time.time()

        for processor in self.processors:
            search_results = processor(self.args, self.filter_config).process_results(search_results, query_bundle, type(self).__name__)

        formatted_search_results = search_results.model_copy(deep=True)
        
        for processor in self.formatting_processors:
            formatted_search_results = processor(self.args, self.filter_config).process_results(formatted_search_results, query_bundle, type(self).__name__)
        
        end_processing = time.time()

        retrieval_ms = (end_retrieve-start_retrieve) * 1000
        processing_ms = (end_processing-end_retrieve) * 1000

        logger.debug(f'[{type(self).__name__}] Retrieval: {retrieval_ms:.2f}ms')
        logger.debug(f'[{type(self).__name__}] Processing: {processing_ms:.2f}ms')

        entity_contexts = formatted_search_results.entity_contexts.model_dump()

        return [
            NodeWithScore(
                node=TextNode(
                    text=formatted_search_result.model_dump_json(exclude_none=True, exclude_defaults=True, indent=2),
                    metadata={
                        'result': search_result.model_dump(exclude_none=True, exclude_unset=True, exclude_defaults=True),
                        'entity_contexts': entity_contexts
                    }
                ), 
                score=search_result.score
            ) 
            for (search_result, formatted_search_result) in zip(search_results.results, formatted_search_results.results)
        ]
    
    def _to_search_results_collection(self, results:List[Any]) -> SearchResultCollection:
        """
        Transforms a list of results into a SearchResultCollection object by validating
        and filtering the provided data.

        This method processes a list of raw results, validates each result's data using
        the SearchResult model, and filters out entries that do not have a 'source' key
        present in their 'result' field. The valid and filtered results are then
        packaged into a SearchResultCollection object.

        Args:
            results (List[Any]): A list of raw result objects where each object is
                expected to have a 'result' key containing a dictionary.

        Returns:
            SearchResultCollection: A collection object containing the validated and
            filtered search results.
        """
        
        search_results = []
        
        for result in results:
            if isinstance(result, SearchResult):
                search_results.append(result)
            elif result['result'].get('source', None):
                search_results.append(SearchResult.model_validate(result['result']))

        try:
            toolkit_version = f" ({version('graphrag-toolkit-lexical-graph')})"
        except PackageNotFoundError:
            toolkit_version = ''

        retriever_name = f'{type(self).__name__}{toolkit_version}'

        for result in search_results:
            for topic in result.topics:
                for statement in topic.statements:
                    statement.retrievers.append(retriever_name)

        return SearchResultCollection(results=search_results, entity_contexts=self.entity_contexts)

    @abc.abstractmethod
    def get_start_node_ids(self, query_bundle: QueryBundle) -> List[str]:
        """
        Abstract method to retrieve the starting node IDs based on the provided query bundle.

        This method should be implemented by subclasses to determine which nodes
        to start the traversal or processing from, according to the given query.

        Args:
            query_bundle (QueryBundle): An object encapsulating the query parameters
                                        or context necessary to determine the start
                                        node IDs.

        Returns:
            List[str]: A list of node IDs representing the starting points
                       for processing or traversal.
        """
        pass
    
    @abc.abstractmethod
    def do_graph_search(self, query_bundle: QueryBundle, start_node_ids:List[str]) -> SearchResultCollection:
        """
        Performs a graph search starting from the specified nodes and utilizing the given
        query to determine the traversal or filtering logic, ultimately constructing a
        result collection based on the search output.

        Args:
            query_bundle: A bundle object containing the query details utilized for
                guiding the search process within the graph.
            start_node_ids: A list of string identifiers representing the starting nodes
                in the graph from where the search will commence.

        Returns:
            SearchResultCollection: An object encapsulating the collection of results
            obtained from the graph search process.

        Raises:
            NotImplementedError: This method must be implemented by any subclass and
                cannot be invoked directly from the abstract base class.
        """
        pass