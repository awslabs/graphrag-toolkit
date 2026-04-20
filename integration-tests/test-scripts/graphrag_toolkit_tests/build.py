# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

class BuildFromFileSystem(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Build graph and vector stores from files on local filesystem'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        collection_id = params['collection_id']
        
        docs = FileBasedDocs(
            docs_directory='extracted',
            collection_id=collection_id
        )
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store
            )
            
            graph_index.build(docs, show_progress=True)
            
            class BuildAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._graph_store = graph_store
                    cls._expected_num_docs = params['expected_num_docs']
            
                def test_one_source_node_for_each_doc(self):
                    """Graph contains one source node per doc"""
                    
                    results = self._graph_store.execute_query('MATCH (n:`__Source__`) RETURN count(n) AS count')
                    source_node_count = results[0]['count']
                    
                    self.assertEqual(source_node_count, self._expected_num_docs)
                    
            handler.run_assertions(BuildAssertions)