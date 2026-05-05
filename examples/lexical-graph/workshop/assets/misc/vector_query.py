import logging
import time
import json
from typing import List

from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.tenant_id import TenantIdType, to_tenant_id
from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.retrieval.prompts import ANSWER_QUESTION_SYSTEM_PROMPT, ANSWER_QUESTION_USER_PROMPT
from graphrag_toolkit.lexical_graph.storage.vector.vector_index import to_embedded_query
from graphrag_toolkit.lexical_graph.storage.graph import MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore, ReadOnlyVectorStore

from llama_index.core import ChatPromptTemplate
from llama_index.core.base.base_retriever import BaseRetriever
from llama_index.core.base.base_query_engine import BaseQueryEngine
from llama_index.core.llms import ChatMessage, MessageRole
from llama_index.core.schema import NodeWithScore, QueryBundle, TextNode
from llama_index.core.base.response.schema import RESPONSE_TYPE
from llama_index.core.base.response.schema import Response, StreamingResponse
from llama_index.core.prompts.mixin import PromptDictType, PromptMixinType
from llama_index.core.types import TokenGen

logger = logging.getLogger(__name__)


class VectorRetriever(BaseRetriever):

    def __init__(self, 
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 top_k:int=5):
        
        self.graph_store = graph_store
        self.vector_store = vector_store
        self.top_k = top_k
        
    

    def _retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:
        
        top_k_results = self.vector_store.get_index('chunk').top_k(query_bundle, self.top_k)
        top_k_map = {r['chunk']['chunkId']:r for r in top_k_results}
        chunk_ids = list(top_k_map.keys())
        
        cypher = '''
        MATCH (c) WHERE id(c) IN $chunkIds RETURN id(c) AS chunkId, c.value AS chunk
        '''
        
        properties = {
            'chunkIds': chunk_ids
        }
        
        results = self.graph_store.execute_query(cypher, properties)
        
        for r in results:
            chunk_id = r['chunkId']
            chunk = r['chunk']
            top_k_map[chunk_id]['chunk']['value'] = chunk
        

        return [
            NodeWithScore(
                node=TextNode(
                    text=top_k_result['chunk']['value'],
                    metadata={
                        'source': top_k_result['source'],
                        'chunkId': top_k_result['chunk']['chunkId']
                    }
                ), 
                score=top_k_result['score']
            ) 
            for top_k_result in list(top_k_map.values())
        ]

class VectorQueryEngine(BaseQueryEngine):

    

    def __init__(self, 
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 top_k:int=5,
                 tenant_id:str=None,
                 streaming:bool=False
                 ):
                 
        tenant_id = to_tenant_id(tenant_id)

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
        
        self.retriever = VectorRetriever(graph_store, vector_store, top_k)
        
        self.llm = LLMCache(
            llm=GraphRAGConfig.response_llm,
            enable_cache=False
        )
        self.chat_template = ChatPromptTemplate(message_templates=[
            ChatMessage(role=MessageRole.SYSTEM, content=ANSWER_QUESTION_SYSTEM_PROMPT),
            ChatMessage(role=MessageRole.USER, content=ANSWER_QUESTION_USER_PROMPT),
        ])
        
        self.streaming = streaming

        super().__init__(None)

    def _generate_response(
        self, 
        query_bundle: QueryBundle, 
        context: str
    ) -> str:
        try:
            response = self.llm.predict(
                prompt=self.chat_template,
                query=query_bundle.query_str,
                search_results=context,
                answer_mode='fully'
            )
            return response
        except Exception:
            logger.exception(f'Error answering query [query: {query_bundle.query_str}, context: {context}]')
            raise
            
    def _generate_streaming_response(
            self,
            query_bundle: QueryBundle,
            context: str
    ) -> TokenGen:
       
        try:
            response = self.llm.stream(
                prompt=self.chat_template,
                query=query_bundle.query_str,
                search_results=context,
                answer_mode='fully'
            )
            return response
        except Exception:
            logger.exception(f'Error answering query [query: {query_bundle.query_str}, context: {context}]')
            raise

            
    def _format_context(self, search_results:List[NodeWithScore]):

        
        def format_result(result):     
            return {
                'source': result.metadata['source'].get('metadata', ''),
                'chunkId': result.metadata['chunkId'],
                'text': result.text
            }
            
        formatted_results = [format_result(result) for result in search_results]
        
        data = json.dumps(formatted_results, indent=2)
        
        return data
    
    def retrieve(self, query_bundle: QueryBundle) -> List[NodeWithScore]:

        query_bundle = QueryBundle(query_bundle) if isinstance(query_bundle, str) else query_bundle

        query_bundle = to_embedded_query(query_bundle, GraphRAGConfig.embed_model)
                
        results = self.retriever.retrieve(query_bundle)

        return results

 
    def _query(self, query_bundle: QueryBundle) -> RESPONSE_TYPE:

        try:
        
            start = time.time()

            query_bundle = to_embedded_query(query_bundle, GraphRAGConfig.embed_model)  
            results = self.retriever.retrieve(query_bundle)

            end_retrieve = time.time()

            context = self._format_context(results)
            
            if self.streaming:
                answer = self._generate_streaming_response(query_bundle, context)
            else:
                answer = self._generate_response(query_bundle, context)

            end = time.time()

            retrieve_ms = (end_retrieve-start) * 1000
            answer_ms = (end-end_retrieve) * 1000
            total_ms = (end-start) * 1000

            metadata = {
                'retrieve_ms': retrieve_ms,
                'answer_ms': answer_ms,
                'total_ms': total_ms,
                'query': query_bundle.query_str,
                'context': context,
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
    
import os

from graphrag_toolkit.lexical_graph import set_logging_config
from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.retrieval.retrievers import *

def vector_query(query, num_chunks=5, tenant_id=None, streaming=False):

    set_logging_config('INFO', [
        'graphrag_toolkit.lexical_graph.retrieval.query',
    ])
    
    with (
        GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE']) as graph_store,
        VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
    ):
    
        query_engine = VectorQueryEngine(
            graph_store, 
            vector_store,
            tenant_id=tenant_id,
            top_k=num_chunks,
            streaming=streaming
        )
    
        return query_engine.query(query)
    
def retrieve(query, num_chunks=5, tenant_id=None):

    set_logging_config('INFO', [
        'graphrag_toolkit.lexical_graph.retrieval.query',
    ])
    
    with (
        GraphStoreFactory.for_graph_store(os.environ['GRAPH_STORE']) as graph_store,
        VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
    ):
    
        query_engine = VectorQueryEngine(
            graph_store, 
            vector_store,
            tenant_id=tenant_id,
            top_k=num_chunks
        )
    
        return query_engine.retrieve(query)