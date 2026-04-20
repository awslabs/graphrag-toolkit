# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any
import uuid

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, TenantId
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

FACTS_TENANT_ID = TenantId(f'facts.{uuid.uuid4().hex[:9]}')
print(f'TENANT_ID: {FACTS_TENANT_ID}')

class BuildFacts(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Build graph and vector stores with next relationships between facts'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        docs = FileBasedDocs(
            docs_directory='source-data',
            collection_id='collection-2'
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
                vector_store,
                tenant_id=FACTS_TENANT_ID
            )
            
            graph_index.build(docs, show_progress=True)
            
            class BuildFactsAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._graph_store = MultiTenantGraphStore.wrap(graph_store, FACTS_TENANT_ID)
                    
            
                def test_contains_correct_number_of_facts(self):
                    """Graph contains correct number of facts"""
                    
                    results = self._graph_store.execute_query('MATCH (n:`__Fact__`) RETURN count(n) AS count')
                    fact_node_count = results[0]['count']
                    
                    self.assertEqual(4, fact_node_count)
                    
                def test_contains_correct_number_of_next_relationships(self):
                    """Graph contains correct number of next relationships"""
                    
                    results = self._graph_store.execute_query('MATCH (:`__Fact__`)-[r:`__NEXT__`]->() RETURN count(r) AS count')
                    next_relationship_count = results[0]['count']
                    
                    self.assertEqual(3, next_relationship_count)
                    
            handler.run_assertions(BuildFactsAssertions)