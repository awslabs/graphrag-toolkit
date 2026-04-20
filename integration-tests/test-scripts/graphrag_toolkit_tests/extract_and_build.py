# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, TenantId, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting, MultiTenantGraphStore

from llama_index.readers.web import SimpleWebPageReader


class ExtractAndBuild(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Extract propositions and topics from OpenSearch documentation, and build lexical graph'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
       
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                tenant_id='aoss'
            )
            
            doc_urls = [
                'https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-overview.html',
                'https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-comparison.html'
            ]
            
            docs = SimpleWebPageReader(
                html_to_text=True,
                metadata_fn=lambda url:{'url': url, 'mycollection': ['a', 'b']}
            ).load_data(doc_urls)
            
            graph_index.extract_and_build(docs, show_progress=True)
            
            
            class ExtractAndBuildAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._graph_store = MultiTenantGraphStore.wrap(graph_store, TenantId('aoss'))
                    cls._expected_num_docs = len(doc_urls)
            
                def test_one_source_node_for_each_doc(self):
                    """Graph contains one source node per doc"""
                    
                    results = self._graph_store.execute_query('MATCH (n:`__Source__`) RETURN count(n) AS count')
                    source_node_count = results[0]['count']
                    
                    self.assertEqual(source_node_count, self._expected_num_docs)
                    
                def test_collection_based_metadata_item_is_ignored(self):
                    """Collection-based metadata items are not added to source nodes"""
                    
                    results = self._graph_store.execute_query('MATCH (n:`__Source__`) RETURN properties(n) AS metadata')
                    for result in results:
                        self.assertTrue('mycollection' not in result['metadata'])
                    
            handler.run_assertions(ExtractAndBuildAssertions)
    
    