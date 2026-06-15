# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import concurrent.futures
from itertools import repeat
from typing import List, Callable, cast, Iterator, Any

from graphrag_toolkit.lexical_graph.retrieval.processors import *
from graphrag_toolkit.lexical_graph.retrieval.query_context import QueryMode, QueryModeProvider
from graphrag_toolkit.lexical_graph.retrieval.query_context import KeywordProvider, KeywordProviderMode
from graphrag_toolkit.lexical_graph.retrieval.processors import ProcessorArgs

from graphrag_toolkit.core.retriever import Retriever
from graphrag_toolkit.core.types import NodeWithScore, QueryBundle

logger = logging.getLogger(__name__)

class QueryModeRetriever(Retriever):
    
    def __init__(self, retriever_fn:Callable[[Any], Retriever], **kwargs):
        self.retriever_fn = retriever_fn
        self.args = ProcessorArgs(**kwargs)

        logger.debug(f'args: {self.args.to_dict()}')
        
    def retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:

        if self.args.enable_multipart_queries:
            query_mode_provider = QueryModeProvider(self.args)
            query_mode = query_mode_provider.get_query_mode(query_bundle.query_str)
        else:
            query_mode = QueryMode.SIMPLE

        if query_mode == QueryMode.SIMPLE:
            
            logger.debug(f'Simple query, so running single retriever [enable_multipart_queries: {self.args.enable_multipart_queries}]')
            
            sub_args = self.args.to_dict()
            retriever = self.retriever_fn(**sub_args)

            return retriever.retrieve(query_bundle)
        
        else:

            keyword_provider = KeywordProvider(ProcessorArgs(), mode=KeywordProviderMode.SIMPLE)
            keywords = keyword_provider.get_keywords(query_bundle)

            sub_args = self.args.to_dict()
            max_search_results = int(sub_args['max_search_results']/len(keywords)) + 1
            sub_args['max_search_results'] = max_search_results
            sub_args['ec_keyword_provider'] = 'passthru'

            logger.debug(f'Complex query, so running multiple retrievers in parallel [num_retrievers: {len(keywords)}, search_results_per_retriever: {max_search_results}]')

            def retrieve(s):
                retriever:Retriever = self.retriever_fn(**sub_args)
                return retriever.retrieve(s)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(keywords)) as executor:
                intermediate_results: Iterator[List[NodeWithScore]] = executor.map(
                    lambda k, f: f(k),
                    keywords,
                    repeat(retrieve)
                )
                results = sum(intermediate_results, start=cast(List[NodeWithScore], []))

            return results
