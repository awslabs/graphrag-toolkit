# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
import uuid
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, LexicalGraphQueryEngine
from graphrag_toolkit.lexical_graph import TenantId
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

DELETE_SOURCES_TENANT_ID = f't{uuid.uuid4().hex[:5]}'

class DeleteSourceDocs(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Build graph and vector stores, including local entities'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        docs = FileBasedDocs(
            docs_directory='source-data',
            collection_id='collection-1'
        )
        
        graph_store = GraphStoreFactory.for_graph_store(
            os.environ['GRAPH_STORE'],
            log_formatting=NonRedactedGraphQueryLogFormatting()
        )
        
        vector_store = VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE'])
        
        
        graph_index = LexicalGraphIndex(
            graph_store, 
            vector_store,
            tenant_id=DELETE_SOURCES_TENANT_ID
        )
        
        graph_index.build(docs, show_progress=True)
        
        before_stats = graph_index.get_stats()
        
        class BeforeDeleteSourceDocs(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._stats = before_stats
                
            def test_contains_sources_chunks_topics_statements_facts_and_entities(self):
                """Graph contains some source document data"""
                
                labels = ['source', 'chunk', 'topic', 'statement', 'fact', 'entity']
                
                for label in labels:
                    self.assertTrue(self._stats[label] > 0)
                    
        handler.run_assertions(BeforeDeleteSourceDocs)
        
        graph_index.delete_sources()
        
        after_stats = graph_index.get_stats()
        
        class AfterDeleteSourceDocs(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._stats = after_stats
                
            def test_is_empty_of_document_data(self):
                """Graph is empty"""
                
                labels = ['source', 'chunk', 'topic', 'statement', 'fact', 'entity']
                
                for label in labels:
                    self.assertTrue(self._stats[label] == 0)
                    
        handler.run_assertions(AfterDeleteSourceDocs)
            