# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import yaml
import logging
import time
from json2xml import json2xml
from typing import Optional, List, Type, Union

from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.versioning import VersioningConfig, to_versioning_config
from graphrag_toolkit.lexical_graph.tenant_id import TenantIdType, to_tenant_id
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.post_processors.bedrock_context_format import BedrockContextFormat
from graphrag_toolkit.lexical_graph.retrieval.retrievers import CompositeTraversalBasedRetriever, SemanticGuidedRetriever, QueryModeRetriever
from graphrag_toolkit.lexical_graph.retrieval.retrievers import StatementCosineSimilaritySearch, KeywordRankingSearch, SemanticBeamGraphSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers import WeightedTraversalBasedRetrieverType, SemanticGuidedRetrieverType
from graphrag_toolkit.lexical_graph.retrieval.model import EntityContexts
from graphrag_toolkit.lexical_graph.retrieval.retrievers import CompositeTraversalBasedRetriever, QueryModeRetriever
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory, GraphStoreType
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory, VectorStoreType
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore, ReadOnlyVectorStore
from graphrag_toolkit.lexical_graph.storage.vector import to_embedded_query
from graphrag_toolkit.lexical_graph.prompts.prompt_provider_factory import PromptProviderFactory

from llama_index.core import ChatPromptTemplate
from llama_index.core.llms import ChatMessage, MessageRole
from llama_index.core.schema import QueryBundle, NodeWithScore
from llama_index.core.base.base_query_engine import BaseQueryEngine
from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.postprocessor.types import BaseNodePostprocessor
from llama_index.core.callbacks.base import CallbackManager
from llama_index.core.base.response.schema import RESPONSE_TYPE
from llama_index.core.base.response.schema import Response, StreamingResponse
from llama_index.core.prompts.mixin import PromptDictType, PromptMixinType
from llama_index.core.types import TokenGen

logger = logging.getLogger(__name__)

RetrieverType = Union[BaseRetriever, Type[BaseRetriever]]
PostProcessorsType = Union[BaseNodePostprocessor, List[BaseNodePostprocessor]]
VersioningType = Union[bool, VersioningConfig]


class LexicalGraphQueryEngine(BaseQueryEngine):
       

    """
    Defines the LexicalGraphQueryEngine class, which serves as a query engine for retrieving and generating responses
    from graph and vector stores using different retrieval strategies.

    This class provides methods for constructing instances for traversal-based and semantic-guided search, configuring
    retrievers, post-processors, and managing prompts for response generation. The engine processes queries, retrieves
    relevant information from the underlying stores, formats the context, and generates responses using an LLM.

    Attributes:
        context_format (str): Format in which the retrieved context data is processed (e.g., 'json', 'text', 'bedrock_xml').
        llm (LLMCacheType): Language model used for generating responses based on retrieved context and query.
        chat_template (ChatPromptTemplate): Template used for constructing conversation prompts for the LLM.
        retriever (BaseRetriever): Retriever instance used for fetching relevant data based on queries.
        post_processors (list): List of post-processor objects for processing retrieved nodes before generating responses.
    """
    @staticmethod
    def for_traversal_based_search(graph_store: GraphStoreType,
                                   vector_store: VectorStoreType,
                                   tenant_id: Optional[TenantIdType] = None,
                                   retrievers: Optional[List[WeightedTraversalBasedRetrieverType]] = None,
                                   post_processors: Optional[PostProcessorsType] = None,
                                   filter_config: Optional[FilterConfig] = None,
                                   versioning: Optional[VersioningType] = None,
                                   **kwargs):
        """
        Constructs an instance of LexicalGraphQueryEngine configured for traversal-based search.

        This method initializes and configures the required components such as graph store, vector store,
        retriever, and other related configurations, ensuring compatibility with multi-tenant setups.

        Args:
            graph_store: The graph storage backend used to retrieve and store graph data. It is
            wrapped with multi-tenant support and configured for usage within the engine.
            vector_store: The vector storage backend used for handling vector-based search and retrieval.
            It is wrapped with read-only and multi-tenant capability encapsulations.
            tenant_id: Optional tenant identifier to distinguish data in a multi-tenant environment.
            When not supplied, defaults to the current tenant context or None.
            retrievers: Optional list of weighted traversal-based retriever instances. These are used
            to define specific retrieval strategies and weighting within the composite retriever.
            post_processors: Optional post-processing components applied to the results of the query
            engine. These can be used to modify or augment the output of retrieval operations.
            filter_config: Configurations for filtering the retrieval results, allowing for enhanced
            query precision and customization. Defaults to a new FilterConfig instance if not provided.
            **kwargs: Additional keyword arguments to customize or extend retrieval and processing
                behaviors within the query engine.

        Returns:
            LexicalGraphQueryEngine: A configured instance of the query engine for traversal-based
                search, encapsulating all specified stores, retrievers, and configurations.
        """
        tenant_id = to_tenant_id(tenant_id)

        if versioning is not None:
            versioning_config = to_versioning_config(versioning) if isinstance(versioning, bool) else versioning
        else:
            versioning_config = to_versioning_config(GraphRAGConfig.enable_versioning)

        filter_config = filter_config or FilterConfig()
        filter_config = versioning_config.apply(filter_config)
        
        graph_store =  MultiTenantGraphStore.wrap(
            GraphStoreFactory.for_graph_store(graph_store), 
            tenant_id
        ) 

        vector_store = ReadOnlyVectorStore.wrap(
            MultiTenantVectorStore.wrap(
                VectorStoreFactory.for_vector_store(vector_store),
                tenant_id
            )
        )

        def create_retriever(**retriever_kwargs):
            return CompositeTraversalBasedRetriever(
                graph_store,
                vector_store,
                retrievers=retrievers,
                filter_config=filter_config,
                **retriever_kwargs
            )
        
        context_format = kwargs.pop('context_format', 'text')
        
        retriever = QueryModeRetriever(create_retriever, **kwargs)

        return LexicalGraphQueryEngine(
            graph_store,
            vector_store,
            tenant_id=tenant_id,
            retriever=retriever,
            post_processors=post_processors,
            context_format=context_format,
            filter_config=filter_config,
            **kwargs
        )

    @staticmethod
    def for_semantic_guided_search(graph_store: GraphStoreType,
                                   vector_store: VectorStoreType,
                                   tenant_id: Optional[TenantIdType] = None,
                                   retrievers: Optional[List[SemanticGuidedRetrieverType]] = None,
                                   post_processors: Optional[PostProcessorsType] = None,
                                   filter_config: FilterConfig = None,
                                   enable_versioning: Optional[VersioningType] = None,
                                   **kwargs):
        """
        Creates and configures an instance of `LexicalGraphQueryEngine` for semantic-guided
        search. This method facilitates the setup of a multi-tenant graph store and vector
        store, along with specified or default retrievers, filter configurations, and
        optional post-processors for the search process. This is useful for performing
        semantic-guided search using a combination of retrievers and post processors
        tailored for a specific tenant or configuration.

        Args:
            graph_store: The base graph store instance to be wrapped for multi-tenant usage.
            vector_store: The base vector store instance to be wrapped for multi-tenant usage.
            tenant_id: An optional unique identifier for the tenant. If not provided, a
            default tenant ID is derived.
            retrievers: An optional list of retrievers for semantic-guided search. If not
            supplied, a default set of retrievers is created.
            post_processors: An optional collection of post-processors to apply after
            retrieving search results.
            filter_config: The filtering configuration options to apply within the search
            process. A default configuration is used if none is provided.
            **kwargs: Additional optional keyword arguments to pass to `SemanticGuidedRetriever`
            and `LexicalGraphQueryEngine`.

        Returns:
            LexicalGraphQueryEngine: A configured instance for performing semantic-guided search
            on the provided graph and vector store.
        """
        tenant_id = to_tenant_id(tenant_id)
        
        if enable_versioning is not None:
            versioning_config = to_versioning_config(enable_versioning) if isinstance(enable_versioning, bool) else enable_versioning
        else:
            versioning_config = to_versioning_config(GraphRAGConfig.enable_versioning)

        filter_config = filter_config or FilterConfig()
        filter_config = versioning_config.apply(filter_config) 

        graph_store = MultiTenantGraphStore.wrap(
            GraphStoreFactory.for_graph_store(graph_store),
            tenant_id
        )

        vector_store = ReadOnlyVectorStore.wrap(
            MultiTenantVectorStore.wrap(
                VectorStoreFactory.for_vector_store(vector_store),
                tenant_id
            )
        )

        retrievers = retrievers or [
            StatementCosineSimilaritySearch(
                vector_store=vector_store,
                graph_store=graph_store,
                top_k=50,
                filter_config=filter_config
            ),
            KeywordRankingSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                max_keywords=10,
                filter_config=filter_config
            ),
            SemanticBeamGraphSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                max_depth=8,
                beam_width=100,
                filter_config=filter_config
            )
        ]

        retriever = SemanticGuidedRetriever(
            vector_store=vector_store,
            graph_store=graph_store,
            retrievers=retrievers,
            share_results=True,
            filter_config=filter_config,
            **kwargs
        )

        if 'context_format' in kwargs:
            kwargs.pop('context_format')

        return LexicalGraphQueryEngine(
            graph_store,
            vector_store,
            tenant_id=tenant_id,
            retriever=retriever,
            post_processors=post_processors,
            context_format='bedrock_xml',
            filter_config=filter_config,
            **kwargs
        )

    def __init__(self,
                 graph_store: GraphStoreType,
                 vector_store: VectorStoreType,
                 tenant_id: Optional[TenantIdType] = None,
                 llm: LLMCacheType = None,
                 system_prompt: Optional[str] = None,
                 user_prompt: Optional[str] = None,
                 retriever: Optional[RetrieverType] = None,
                 post_processors: Optional[PostProcessorsType] = None,
                 callback_manager: Optional[CallbackManager] = None,
                 filter_config: FilterConfig = None,
                 streaming: bool = False,
                 **kwargs):
        """
                Initializes a LexicalGraphQueryEngine instance for querying and generating responses from graph and vector stores.

                This constructor sets up the engine with the provided stores, LLM, prompts, retriever, post-processors, and other configuration options.
                If system or user prompts are not provided, it attempts to retrieve them from a prompt provider.

                Args:
                    graph_store: The graph store implementation used for storing and retrieving graph data.
                    vector_store: The vector store implementation used for storing and retrieving vector data.
                    tenant_id: Optional tenant identifier used to enable multi-tenant functionality. Defaults to None.
                    llm: The language model to be used. If not provided, it uses a default LLM configuration. Defaults to None.
                    system_prompt: Optional system-level prompt to be used in the chat template.
                    user_prompt: Optional user-level prompt to be used in the chat template.
                    retriever: Optional custom retriever implementation. If not provided, a default CompositeTraversalBasedRetriever instance is created.
                    post_processors: Optional list of post-processing components or a single post-processor.
                    callback_manager: Optional callback manager for managing event callbacks in the processing workflow.
                    filter_config: An optional configuration object specifying filter criteria for retrieving data during processing.
                    **kwargs: Additional arguments for extended functionality, including custom context formatting or retriever behavior.
        """

        tenant_id = to_tenant_id(tenant_id)

        graph_store = MultiTenantGraphStore.wrap(GraphStoreFactory.for_graph_store(graph_store), tenant_id)
        vector_store = ReadOnlyVectorStore.wrap(
            MultiTenantVectorStore.wrap(VectorStoreFactory.for_vector_store(vector_store), tenant_id)
        )

        self.context_format = kwargs.get('context_format', 'json')
        self.verbose = kwargs.pop('verbose', True)
        
        no_cache = kwargs.pop('no_cache', False)
        enable_cache = False if no_cache else GraphRAGConfig.enable_cache

        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=enable_cache
        )
        self.streaming = streaming

        prompt_provider = kwargs.pop('prompt_provider', None)
        
        if prompt_provider is None:
            prompt_provider = PromptProviderFactory.get_provider()

        self.chat_template = ChatPromptTemplate(message_templates=[
            ChatMessage(role=MessageRole.SYSTEM, content=system_prompt or prompt_provider.get_system_prompt()),
            ChatMessage(role=MessageRole.USER, content=user_prompt or prompt_provider.get_user_prompt()),
        ])

        if retriever:
            if isinstance(retriever, BaseRetriever):
                self.retriever = retriever
            else:
                self.retriever = retriever(graph_store, vector_store, filter_config=filter_config, **kwargs)
        else:
            self.retriever = CompositeTraversalBasedRetriever(graph_store, vector_store, filter_config=filter_config, **kwargs)

        if post_processors:
            self.post_processors = post_processors if isinstance(post_processors, list) else [post_processors]
        else:
            self.post_processors = []

        if self.context_format == 'bedrock_xml':
            self.post_processors.append(BedrockContextFormat())

        if callback_manager:
            for post_processor in self.post_processors:
                post_processor.callback_manager = callback_manager

        super().__init__(callback_manager)

    def _generate_response(
            self,
            query_bundle: QueryBundle,
            search_results: str,
            additional_context: List[str] = []
    ) -> str:
        """
        Generates a response to a query by utilizing a language model with the provided query
        and context. The method applies a pre-defined chat template in conjunction with the
        query and search results, returning the language model's prediction as a response.

        Args:
            query_bundle: An object containing structured query information, including the query
                string needed for response generation.
            search_results: A string representing context or search results that augment the query
                information during response generation.

        Returns:
            str: The generated response provided by the language model based on the input query
            and context.
        """
        try:
            response = self.llm.predict(
                prompt=self.chat_template,
                query=query_bundle.query_str,
                search_results=search_results,
                additionalContext='\n'.join(additional_context),
                answer_mode='fully' if self.verbose else 'concisely'
            )
            return response
        except Exception:
            logger.exception(f'Error answering query [query: {query_bundle.query_str}, search_results: {search_results}, additional_context: {additional_context}]')
            raise

    def _generate_streaming_response(
            self,
            query_bundle: QueryBundle,
            search_results: str,
            additional_context: List[str] = []
    ) -> TokenGen:
       
        try:
            response = self.llm.stream(
                prompt=self.chat_template,
                query=query_bundle.query_str,
                search_results=search_results,
                additionalContext='\n'.join(additional_context)
            )
            return response
        except Exception:
            logger.exception(f'Error answering query [query: {query_bundle.query_str}, search_results: {search_results}, additional_context: {additional_context}]')
            raise

    def _format_as_text(self, json_results):

        def topic_as_text(topic, source):
            topic_lines = []
            topic_lines.append(f"""## {topic['topic']}""")
            topic_lines.append(' '.join([s for s in topic['statements']]))
            topic_lines.append(f"""[Source: {source}]""")
            topic_lines.append('\n')
            return topic_lines

        lines = []

        for json_result in json_results:
            source = json_result['source']
            if 'topics' in json_result:
                for topic in json_result['topics']:
                    lines.extend(topic_as_text(topic, source))
            else:
                lines.extend(topic_as_text(json_result, source))

        return '\n'.join(lines)

    def _format_context(self, search_results: List[NodeWithScore], context_format: str = 'json'):
        """
        Formats the provided search results into the specified context format.

        This method processes a list of search results in a specified format
        (e.g., JSON, YAML, XML, text). It converts the input into the required
        output format, enabling easier consumption of the processed data.

        Args:
            search_results (List[NodeWithScore]): A list of search result
                objects, each containing a `text` attribute with the result
                content.
            context_format (str): The desired format for the output data.
                Supported formats include 'json', 'yaml', 'xml', 'text',
                and 'bedrock_xml'. Defaults to 'json'.

        Returns:
            str: The search results formatted as per the specified
                `context_format`.
        """
        if context_format == 'bedrock_xml':
            return '\n'.join([result.text for result in search_results])

        json_results = [json.loads(result.text) for result in search_results]

        data = None

        if context_format == 'yaml':
            data = yaml.dump(json_results, sort_keys=False)
        elif context_format == 'xml':
            data = json2xml.Json2xml(json_results, attr_type=False).to_xml()
        elif context_format == 'text':
            data = self._format_as_text(json_results)
        else:
            data = json.dumps(json_results, indent=2)

        return data
    
    def _get_entity_contexts(self, search_results: List[NodeWithScore]) -> List[str]:
        if not search_results:
            return []
        
        contexts_dump = search_results[0].metadata.get('entity_contexts', []) 

        if not contexts_dump:
            return []
 
        return EntityContexts.model_validate(contexts_dump).all_context_strs


    def retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        """
        Retrieve nodes relevant to a given query bundle and process them using a series of
        post-processing steps. This method facilitates retrieval and post-processing of query
        results in order to return a set of relevant nodes with their associated scores.

        Args:
            query_bundle: A `QueryBundle` object containing details of the query to be
                executed. If a string is provided, it is converted into a `QueryBundle`.

        Returns:
            List[NodeWithScore]: A list of nodes with their corresponding scores, sorted by
                relevance to the provided query.
        """
        query_bundle = QueryBundle(query_bundle) if isinstance(query_bundle, str) else query_bundle

        query_bundle = to_embedded_query(query_bundle, GraphRAGConfig.embed_model)

        results = self.retriever.retrieve(query_bundle)

        for post_processor in self.post_processors:
            results = post_processor.postprocess_nodes(results, query_bundle)

        return results
     
    def _query(self, query_bundle: QueryBundle) -> RESPONSE_TYPE:
        """
        Executes a query against the system and processes the results to generate a
        final response. The method applies embedding on the query, retrieves relevant
        data, processes the data through registered post-processors, formats the
        context, and generates a response.

        Args:
            query_bundle: An instance of QueryBundle containing the query string and
                additional data required for the query.

        Returns:
            Response: An instance of the Response class. It contains the generated
                response, the source nodes used for building the response, and
                metadata such as timing details and applied configurations.

        Raises:
            Exception: If any error occurs during query processing, it is logged and
                re-raised.
        """
        try:

            start = time.time()

            query_bundle = to_embedded_query(query_bundle, GraphRAGConfig.embed_model)

            results = self.retriever.retrieve(query_bundle)

            end_retrieve = time.time()

            for post_processor in self.post_processors:
                results = post_processor.postprocess_nodes(results, query_bundle)

            end_postprocessing = time.time()

            context = self._format_context(results, self.context_format)
            entity_contexts = self._get_entity_contexts(results)
            
            if self.streaming:
                answer = self._generate_streaming_response(query_bundle, context, entity_contexts)
            else:
                answer = self._generate_response(query_bundle, context, entity_contexts)

            end = time.time()

            retrieve_ms = (end_retrieve - start) * 1000
            postprocess_ms = (end_postprocessing - end_retrieve) * 1000
            answer_ms = (end - end_retrieve) * 1000
            total_ms = (end - start) * 1000

            metadata = {
                'retrieve_ms': retrieve_ms,
                'postprocessing_ms': postprocess_ms,
                'answer_ms': answer_ms,
                'total_ms': total_ms,
                'context_format': self.context_format,
                'retriever': f'{type(self.retriever).__name__}: {self.retriever.__dict__}',
                'query': query_bundle.query_str,
                'postprocessors': [type(p).__name__ for p in self.post_processors],
                'context': context,
                'entity_contexts': entity_contexts,
                'num_source_nodes': len(results)
            }

            if self.streaming:
                return StreamingResponse(
                    response_gen=answer,
                    source_nodes=results,
                    metadata=metadata
                )
            else:
                return Response(
                    response=answer,
                    source_nodes=results,
                    metadata=metadata
                )
        except Exception as e:
            logger.exception('Error in query processing')
            raise

    async def _aquery(self, query_bundle: QueryBundle) -> RESPONSE_TYPE:
        pass

    def _get_prompts(self) -> PromptDictType:
        pass

    def _get_prompt_modules(self) -> PromptMixinType:
        pass

    def _update_prompts(self, prompts_dict: PromptDictType) -> None:
        pass
