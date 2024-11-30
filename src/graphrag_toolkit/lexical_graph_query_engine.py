# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
        
import json
import yaml
import logging
import time
from json2xml import json2xml
from typing import Optional, List, Type, Union

from graphrag_toolkit.config import GraphRAGConfig
from graphrag_toolkit.utils import LLMCache
from graphrag_toolkit.retrieval.prompts import ANSWER_QUESTION_SYSTEM_PROMPT, ANSWER_QUESTION_USER_PROMPT
from graphrag_toolkit.retrieval.post_processors.enrich_source_details import EnrichSourceDetails, SourceInfoAccessorType
from graphrag_toolkit.retrieval.post_processors.bedrock_context_format import BedrockContextFormat
from graphrag_toolkit.retrieval.retrievers import TraversalBasedRetriever, SemanticGuidedRetriever
from graphrag_toolkit.retrieval.retrievers import StatementCosineSimilaritySearch, KeywordRankingSearch, SemanticBeamGraphSearch
from graphrag_toolkit.retrieval.retrievers import WeightedTraversalBasedRetrieverType, SemanticGuidedRetrieverType
from graphrag_toolkit.storage import GraphStoreFactory, GraphStoreType
from graphrag_toolkit.storage import VectorStoreFactory, VectorStoreType
from graphrag_toolkit.storage.vector_index import to_embedded_query

from llama_index.core import ChatPromptTemplate
from llama_index.core.llms import ChatMessage, MessageRole
from llama_index.core.schema import QueryBundle, NodeWithScore
from llama_index.core.base.base_query_engine import BaseQueryEngine
from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.postprocessor.types import BaseNodePostprocessor
from llama_index.core.callbacks.base import CallbackManager
from llama_index.core.base.response.schema import RESPONSE_TYPE
from llama_index.core.base.response.schema import Response
from llama_index.core.prompts.mixin import PromptDictType, PromptMixinType

logger = logging.getLogger(__name__)

RetrieverType = Union[BaseRetriever, Type[BaseRetriever]]
PostProcessorsType = Union[BaseNodePostprocessor, List[BaseNodePostprocessor]]

def format_source(source_info_accessor:SourceInfoAccessorType='source'):
    return EnrichSourceDetails(
        source_info_accessor=source_info_accessor
    )

class LexicalGraphQueryEngine(BaseQueryEngine):

    @staticmethod
    def for_traversal_based_search(graph_store:GraphStoreType, 
                                   vector_store:VectorStoreType, 
                                   retrievers:Optional[List[WeightedTraversalBasedRetrieverType]]=None,
                                   post_processors:Optional[PostProcessorsType]=None, 
                                   **kwargs):
        
        retriever = TraversalBasedRetriever(
            graph_store, 
            vector_store, 
            retrievers=retrievers,
            **kwargs
        )
        
        return LexicalGraphQueryEngine(
            graph_store, 
            vector_store,
            retriever=retriever,
            post_processors=post_processors,
            **kwargs
        )
    
    @staticmethod
    def for_semantic_guided_search(graph_store:GraphStoreType, 
                                   vector_store:VectorStoreType, 
                                   retrievers:Optional[List[SemanticGuidedRetrieverType]]=None,
                                   post_processors:Optional[PostProcessorsType]=None, 
                                   **kwargs):
        
        retrievers = retrievers or [
            StatementCosineSimilaritySearch(
                vector_store=vector_store,
                graph_store=graph_store,
                top_k=50
            ),
            KeywordRankingSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                max_keywords=10
            ),
            SemanticBeamGraphSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                max_depth=8,
                beam_width=100
            )
        ]
        
        retriever = SemanticGuidedRetriever(
            vector_store=vector_store,
            graph_store=graph_store,
            retrievers=retrievers,
            share_results=True,
            **kwargs
        ) 
        
        return LexicalGraphQueryEngine(
            graph_store, 
            vector_store,
            retriever=retriever,
            post_processors=post_processors,
            context_format='bedrock_xml',
            **kwargs
        )


    def __init__(self, 
                 graph_store:GraphStoreType,
                 vector_store:VectorStoreType,
                 llm=None,
                 system_prompt:Optional[str]=ANSWER_QUESTION_SYSTEM_PROMPT,
                 user_prompt:Optional[str]=ANSWER_QUESTION_USER_PROMPT,
                 retriever:Optional[RetrieverType]=None,
                 post_processors:Optional[PostProcessorsType]=None,
                 callback_manager: Optional[CallbackManager]=None, 
                 **kwargs):
        
        graph_store = GraphStoreFactory.for_graph_store(graph_store)
        vector_store = VectorStoreFactory.for_vector_store(vector_store)
        
        self.context_format = kwargs.get('context_format', 'json')

        self.llm = LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache
        )
        self.chat_template = ChatPromptTemplate(message_templates=[
            ChatMessage(role=MessageRole.SYSTEM, content=system_prompt),
            ChatMessage(role=MessageRole.USER, content=user_prompt),
        ])

        if retriever:
            if isinstance(retriever, BaseRetriever):
                self.retriever = retriever
            else:
                self.retriever = retriever(graph_store, vector_store, **kwargs)
        else:
            self.retriever = TraversalBasedRetriever(graph_store, vector_store, **kwargs)

        if post_processors:
            self.post_processors = []
            post_processor_list = post_processors if isinstance(post_processors, list) else [post_processors]
            
            for post_processor in post_processor_list:
                if isinstance(post_processor, BaseNodePostprocessor):
                    self.post_processors.append(post_processor)
                elif isinstance(post_processor, type) and issubclass(post_processor, BaseNodePostprocessor):
                    try:
                        processor_instance = post_processor(**kwargs)
                        self.post_processors.append(processor_instance)
                    except TypeError:
                        processor_instance = post_processor() 
                        self.post_processors.append(processor_instance)
                else:
                    raise TypeError(f"Invalid post_processor type: {type(post_processor)}. "
                                f"Must be either a BaseNodePostprocessor instance or class.")
        else:
            self.post_processors = []

        if self.context_format == 'bedrock_xml':
            if not any(
                isinstance(p, BedrockContextFormat) or
                (isinstance(p, type) and issubclass(p, BedrockContextFormat))
                for p in self.post_processors
            ):
                self.post_processors.append(BedrockContextFormat())

        if callback_manager:
            for post_processor in self.post_processors:
                post_processor.callback_manager = callback_manager
        

        super().__init__(callback_manager)

    def _generate_response(
        self, 
        query_bundle: QueryBundle, 
        context: str
    ) -> str:
        try:
            response = self.llm.predict(
                prompt=self.chat_template,
                query=query_bundle.query_str,
                search_results=context
            )
            return response
        except Exception:
            logger.exception(f'Error answering query [query: {query_bundle.query_str}, context: {context}]')
            raise
            
    def _format_context(self, search_results:List[NodeWithScore], context_format:str='json'):

        if context_format == 'bedrock_xml':
            return '\n'.join([result.text for result in search_results])
        
        json_results = [json.loads(result.text) for result in search_results]
        
        data = None
        
        if context_format == 'yaml':
            data = yaml.dump(json_results, sort_keys=False)
        elif context_format == 'xml':
            data = json2xml.Json2xml(json_results, attr_type=False).to_xml()
        else:
            data = json.dumps(json_results, indent=2)
            
        logger.debug(f'data: {data}')
        
        return data
 
    def _query(self, query_bundle: QueryBundle) -> RESPONSE_TYPE:

        try:
        
            start = time.time()

            query_bundle = to_embedded_query(query_bundle, GraphRAGConfig.embed_model)
                
            results = self.retriever.retrieve(query_bundle)

            end_retrieve = time.time()

            for post_processor in self.post_processors:
                results = post_processor.postprocess_nodes(results, query_bundle)

            end_postprocessing = time.time()

            context = self._format_context(results, self.context_format)
            answer = self._generate_response(query_bundle, context)
            
            end = time.time()

            retrieve_ms = (end_retrieve-start) * 1000
            postprocess_ms = (end_postprocessing - end_retrieve) * 1000
            answer_ms = (end-end_retrieve) * 1000
            total_ms = (end-start) * 1000

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
                'num_source_nodes': len(results)
            }

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