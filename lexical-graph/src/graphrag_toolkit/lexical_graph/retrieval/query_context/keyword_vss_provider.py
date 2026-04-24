# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import concurrent.futures
from typing import List, Optional

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage.graph import GraphStore
from graphrag_toolkit.lexical_graph.storage.vector import VectorStore
from graphrag_toolkit.lexical_graph.storage.vector import DummyVectorIndex
from graphrag_toolkit.lexical_graph.storage.graph.graph_utils import node_result
from graphrag_toolkit.lexical_graph.retrieval.model import ScoredEntity
from graphrag_toolkit.lexical_graph.retrieval.utils.vector_utils import get_diverse_vss_elements
from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_provider_base import KeywordProviderBase
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)

IDENTIFY_RELEVANT_ENTITIES_PROMPT = '''
You are an expert AI assistant specialising in knowledge graphs. Given a user-supplied question and a piece of context, your task is to identify up to {num_keywords} of the most relevant named entities from the question and keywords from the context. Return them, most relevant first. You do not have to return the maximum number of items; you can return fewer. 

<question>
{question}
</question>

<context>
{context}
</context>

Put the relevant items on separate lines. Do not provide any other explanatory text. Do not surround the output with tags. Do not exceed {num_keywords} items in your response.
'''

class KeywordVSSProvider(KeywordProviderBase):
    
    def __init__(self,
                 graph_store:GraphStore,
                 vector_store:VectorStore,
                 args:ProcessorArgs,
                 filter_config:Optional[FilterConfig]=None,
                 llm:LLMCacheType=None
                ):
        
        super().__init__(args)

        self.graph_store = graph_store
        self.vector_store = vector_store
        self.filter_config = filter_config

        self.index_name = 'topic' if not isinstance(vector_store.get_index('topic'), DummyVectorIndex) else 'chunk'

        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache if not args.no_cache else not args.no_cache
        )

    def _get_node_ids(self, query_bundle:QueryBundle) -> List[str]:

        index_name = self.index_name
        id_name = f'{index_name}Id'

        vss_results = get_diverse_vss_elements(index_name, query_bundle, self.vector_store, 5, 3, self.filter_config)
        
        node_ids = [result[index_name][id_name] for result in vss_results]

        if type(self).__name__ in self.args.debug_results:
            logger.debug(f'node_ids: [index: {index_name}, ids: {node_ids}]')

        return node_ids
    
    def _get_chunk_content(self, node_ids:List[str]) -> List[str]:
        
        cypher = f"""
        // get chunk content
        MATCH (c:`__Chunk__`)
        WHERE {self.graph_store.node_id("c.chunkId")} in $nodeIds
        RETURN c.value AS content
        """

        parameters = {
            'nodeIds': node_ids
        }

        results = self.graph_store.execute_query(cypher, parameters)

        content = [result['content'] for result in results]

        return content
    
    def _get_topic_content(self, node_ids:List[str]) -> List[str]:

        content = []

        def format_statement(result):
            statement_str = result['statement']
            details = result['details'].split('/n')
            details_str = '' if not details else f" ({', '.join(details)})"
            return f'{statement_str}{details_str}'

        def get_statements_for_topic(topic_id):
            
            cypher = f"""
            // get topic content
            MATCH (t:`__Topic__`)<-[:`__BELONGS_TO__`]-(s)<-[r:`__SUPPORTS__`]-()
            WHERE {self.graph_store.node_id("t.topicId")} = $topicId
            WITH s, count(r) AS score ORDER BY score DESC
            RETURN s.value AS statement, s.details AS details LIMIT $statementLimit
            """

            parameters = {
                'topicId': topic_id,
                'statementLimit': self.args.intermediate_limit
            }

            results = self.graph_store.execute_query(cypher, parameters)

            return '\n'.join(format_statement(r) for r in results)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.num_workers) as executor:

            futures = [
                executor.submit(get_statements_for_topic, node_id)
                for node_id in node_ids
            ]
            
            executor.shutdown()

            for future in futures:
                for result in future.result():
                    content.append(result)
        
        return content
    
    def _get_content(self, node_ids:List[str]) -> List[str]:

        if self.index_name == 'topic':
            return self._get_topic_content(node_ids)
        else:
            return self._get_chunk_content(node_ids)
    
 
    def _get_keywords_from_content(self, query:str, content:List[str]) -> List[str]:

        response = self.llm.predict(
            PromptTemplate(template=IDENTIFY_RELEVANT_ENTITIES_PROMPT),
            question=query,
            context='\n\n'.join(content),
            num_keywords=self.args.max_keywords
        )
        
        if type(self).__name__ in self.args.debug_results:
            logger.debug(f'response: {response}')

        keywords = [k for k in response.split('\n') if k]

        return keywords

    def _get_keywords(self, query_bundle:QueryBundle) -> List[str]:
        
        node_ids =self._get_node_ids(query_bundle)
        content = self._get_content(node_ids)
        keywords = self._get_keywords_from_content(query_bundle.query_str, content)
        
        return keywords