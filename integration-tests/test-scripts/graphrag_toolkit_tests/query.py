# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphQueryEngine, TenantId, GraphRAGConfig
from graphrag_toolkit.lexical_graph.metadata import FilterConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore
from graphrag_toolkit.lexical_graph.retrieval.retrievers import ChunkBasedSearch
from graphrag_toolkit.lexical_graph.retrieval.retrievers import ChunkBasedSemanticSearch, StatementCosineSimilaritySearch, KeywordRankingSearch, SemanticBeamGraphSearch, RerankingBeamGraphSearch
from graphrag_toolkit.lexical_graph.retrieval.post_processors import SentenceReranker, StatementDiversityPostProcessor, StatementEnhancementPostProcessor

from llama_index.core.vector_stores.types import FilterOperator, MetadataFilter, MetadataFilters, FilterCondition
from llama_index.core.schema import QueryBundle

TENANT_ID = TenantId('multihop')

class TraversalBasedQuery(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run traversal-based query'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class TraversalBasedQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(TraversalBasedQueryAssertions)
        
class TraversalBasedQueryWithModelReranker(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run traversal-based query using model reranker'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store,
                reranker='model'
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class TraversalBasedQueryWithModelRerankerAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(TraversalBasedQueryWithModelRerankerAssertions)
            
class TraversalBasedQueryWithBedrockReranker(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run traversal-based query using bedrock reranker'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store,
                reranker='bedrock'
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class TraversalBasedQueryWithBedrockRerankerAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(TraversalBasedQueryWithBedrockRerankerAssertions)
        
class ChunkBasedTraversalQuery(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run traversal-based query using ChunkBasedSearch retriever'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store,
                retrievers=[ChunkBasedSearch]
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class ChunkBasedTraversalQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(ChunkBasedTraversalQueryAssertions)
        
class MetadataFilteringQuery(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run query with metadata filter'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store,
                filter_config = FilterConfig(
                    MetadataFilters(
                        filters=[
                            MetadataFilter(
                                key='pub_date',
                                value='2023-06-15',
                                operator=FilterOperator.EQ
                            ),
                            MetadataFilter(
                                key='url',
                                value='https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
                                operator=FilterOperator.EQ
                            )
                        ],
                        condition=FilterCondition.AND
                    )
                    
                )
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class MetadataFilteringQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._source_nodes = response.source_nodes
            
                def test_source_nodes_match_metadata_filter(self):
                    """Source nodes contain sources that match metadata filter"""
                    for source_node in self._source_nodes:
                        if 'result' in source_node.metadata:
                            self.assertEqual(source_node.metadata['result']['source']['metadata']['url'], 'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html')
                        else:
                            self.assertEqual(source_node.metadata['source']['metadata']['url'], 'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html')
            handler.run_assertions(MetadataFilteringQueryAssertions)        
        
class SemanticGuidedQuery(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run semantic-guided query'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
                graph_store, 
                vector_store
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.text for n in response.source_nodes])
            
            class SemanticGuidedQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(SemanticGuidedQueryAssertions)
        
class SemanticGuidedQueryWithSubRetrievers(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run semantic-guided query with subretrievers'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
                graph_store, 
                vector_store,
                retrievers=[
                    StatementCosineSimilaritySearch, 
                    KeywordRankingSearch, 
                    SemanticBeamGraphSearch
                ]
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.text for n in response.source_nodes])
            
            class SemanticGuidedQueryWithSubRetrieversAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(SemanticGuidedQueryWithSubRetrieversAssertions)
        
class SemanticGuidedRerankingBeamSearchQuery(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run reranking beam search semantic-guided query'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
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
            
            reranker = SentenceReranker(
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
            
            class SemanticGuidedRerankingBeamSearchQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(SemanticGuidedRerankingBeamSearchQueryAssertions)
        
class SemanticGuidedQueryWithPostProcessors(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Run semantic-guided query'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='Neptune'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_semantic_guided_search(
                graph_store, 
                vector_store,
                post_processors=[
                    SentenceReranker(), 
                    StatementDiversityPostProcessor(), 
                    StatementEnhancementPostProcessor()
                ]
            )
            
            response = query_engine.query("What are the differences between Neptune Database and Neptune Analytics?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.text for n in response.source_nodes])
            
            class SemanticGuidedQueryWithPostProcessorsAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(SemanticGuidedQueryWithPostProcessorsAssertions)
        
class MultiHopQuery(IntegrationTestBase):

    @property
    def description(self):
        return 'Run query against multihop news dataset'
    
    def wait(self) -> bool:
        with MultiTenantVectorStore.wrap(
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']),
            tenant_id=TENANT_ID
        ) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='sport'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store,
                tenant_id=TENANT_ID
            )
            
            response = query_engine.query("What are the sales prospects for BlueBell Toys in the UK?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class MultiHopQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(MultiHopQueryAssertions)
            
class ChunkBasedSemanticSearchMultiHopQuery(IntegrationTestBase):

    @property
    def description(self):
        return 'Run query against multihop news dataset using ChunkBasedSemanticSearch'
    
    def wait(self) -> bool:
        with MultiTenantVectorStore.wrap(
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']),
            tenant_id=TENANT_ID
        ) as vector_store:
            return len(vector_store.get_index('chunk').top_k(QueryBundle(query_str='sport'), top_k=1)) == 0
   
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            query_engine = LexicalGraphQueryEngine.for_traversal_based_search(
                graph_store, 
                vector_store,
                tenant_id=TENANT_ID,
                retrievers=[ChunkBasedSemanticSearch]
            )
            
            response = query_engine.query("What are the sales prospects for BlueBell Toys in the UK?")
          
            handler.add_output('response', response.response)
            handler.add_output('context', [n.metadata for n in response.source_nodes])
            
            class ChunkBasedSemanticSearchMultiHopQueryAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_source_nodes = len(response.source_nodes)
            
                def test_has_at_least_one_result(self):
                    """Response contains at least one search result"""
                    self.assertGreater(self._num_source_nodes, 0)
                    
            handler.run_assertions(ChunkBasedSemanticSearchMultiHopQueryAssertions)
    
    
    