# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
import time
from enum import Enum

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.prompt import PromptTemplate

logger = logging.getLogger(__name__)

GET_QUERY_MODE_PROMPT = '''
Is the following user query best described as a single or multipart query? A multipart query is one that can be decomposed into a list whose parts are completely independent of one another. A singlepart query is one whose parts are related to one another. 

Answer 'single' or 'multipart'. Do not provide any other explanation.

<query>
{query}
</query>
'''

class QueryMode(Enum):
    
    SIMPLE = 1
    COMPLEX = 2

class QueryModeProvider():
    
    def __init__(self, args:ProcessorArgs, llm:LLMCacheType=None):
        
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache if not args.no_cache else not args.no_cache
        )
 
    def _get_query_mode(self, query:str):
        response = self.llm.predict(
            PromptTemplate(template=GET_QUERY_MODE_PROMPT),
            query=query
        )

        return QueryMode.SIMPLE if 'single' in response.strip().lower() else QueryMode.COMPLEX

    def get_query_mode(self, query:str) -> QueryMode:

        start = time.time()
        query_mode = self._get_query_mode(query)
        end = time.time()
        duration_ms = (end-start) * 1000
        
        logger.debug(f'query_mode: [{query_mode}] {query} ({duration_ms:.2f} ms)')
        
        return query_mode