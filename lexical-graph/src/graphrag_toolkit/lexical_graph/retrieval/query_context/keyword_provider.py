# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import logging
from enum import Enum
from typing import List

from graphrag_toolkit.lexical_graph.config import GraphRAGConfig
from graphrag_toolkit.lexical_graph.utils import LLMCache, LLMCacheType
from graphrag_toolkit.lexical_graph.retrieval.query_context.keyword_provider_base import KeywordProviderBase
from graphrag_toolkit.lexical_graph.retrieval.prompts import SIMPLE_EXTRACT_KEYWORDS_PROMPT, EXTENDED_EXTRACT_KEYWORDS_PROMPT
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from llama_index.core.prompts import PromptTemplate
from llama_index.core.schema import QueryBundle

logger = logging.getLogger(__name__)



class KeywordProviderMode(Enum):
    
    SIMPLE = 1
    ALL = 2

class KeywordProvider(KeywordProviderBase):
    
    def __init__(self,
                 args:ProcessorArgs,
                 llm:LLMCacheType=None, 
                 simple_extract_keywords_template=SIMPLE_EXTRACT_KEYWORDS_PROMPT,
                 extended_extract_keywords_template=EXTENDED_EXTRACT_KEYWORDS_PROMPT,
                 mode=KeywordProviderMode.ALL
                ):
        
        super().__init__(args)
       
        self.llm = llm if llm and isinstance(llm, LLMCache) else LLMCache(
            llm=llm or GraphRAGConfig.response_llm,
            enable_cache=GraphRAGConfig.enable_cache if not args.no_cache else not args.no_cache
        )
        self.simple_extract_keywords_template=simple_extract_keywords_template
        self.extended_extract_keywords_template=extended_extract_keywords_template
        self.mode = mode
 
    def _extract_keywords(self, s:str, num_keywords:int, prompt_template:str):
        results = self.llm.predict(
            PromptTemplate(template=prompt_template),
            text=s,
            max_keywords=num_keywords
        )

        keywords = results.split('^')
        return keywords

    def _get_simple_keywords(self, query, num_keywords):
        simple_keywords = self._extract_keywords(query, num_keywords, self.simple_extract_keywords_template)
        logger.debug(f'Simple keywords: {simple_keywords}')
        return simple_keywords
    
    def _get_enriched_keywords(self, query, num_keywords):
        enriched_keywords = self._extract_keywords(query, num_keywords, self.extended_extract_keywords_template)
        logger.debug(f'Enriched keywords: {enriched_keywords}')
        return enriched_keywords

    def _get_keywords(self, query_bundle:QueryBundle) -> List[str]:
        
        query = query_bundle.query_str

        num_keywords = max(int(self.args.max_keywords/2), 1)

        logger.debug(f'query: {query}')
        logger.debug(f'mode: {self.mode}')

        keywords = self._get_simple_keywords(query, num_keywords)

        if self.mode == KeywordProviderMode.ALL:
            keywords.extend(self._get_enriched_keywords(query, num_keywords))
       
        unique_keywords = list({ k.lower(): None for k in keywords}.keys())
        
        return unique_keywords