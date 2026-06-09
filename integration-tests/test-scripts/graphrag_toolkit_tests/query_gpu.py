# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.retrieval.retrievers import RerankingBeamGraphSearch, StatementCosineSimilaritySearch, KeywordRankingSearch

from llama_index.core.schema import QueryBundle

        
class RerankingBeamGraphSearchGPU(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run reranking beam graph search on GPU'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-6')
        
        if os.environ.get('USE_GPU', 'False') == 'False':
            print('Non-GPU instance, so skipping test')
            handler.skip()
            return
            
        from graphrag_toolkit.lexical_graph.retrieval.post_processors.bge_reranker import BGEReranker
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            cosine_retriever = StatementCosineSimilaritySearch(
                vector_store=vector_store,
                graph_store=graph_store,
                top_k=50
            )
            
            keyword_retriever = KeywordRankingSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                max_keywords=10
            )
            
            reranker = BGEReranker(
                gpu_id=0,
                batch_size=128
            )
            
            beam_retriever = RerankingBeamGraphSearch(
                vector_store=vector_store,
                graph_store=graph_store,
                reranker=reranker,
                initial_retrievers=[cosine_retriever, keyword_retriever],
                max_depth=8,
                beam_width=100
            )
            
            query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
                graph_store, 
                vector_store,
                retrievers=[
                    cosine_retriever,
                    keyword_retriever,
                    beam_retriever
                ]
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.text for n in response.source_nodes])
            
            class RerankingBeamGraphSearchGPUAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(RerankingBeamGraphSearchGPUAssertions)
        

    
    
    