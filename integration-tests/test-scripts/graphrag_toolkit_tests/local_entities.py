# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import TenantId, BuildConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

LOCAL_TENANT_ID = TenantId('local')
WITHOUT_LOCAL_TENANT_ID = TenantId('wolocal')

NUM_LOCAL_AND_GLOBAL_ENTITIES = [38, 33]
NUM_GLOBAL_ENTITIES = 11

class BuildWithLocalEntities(IntegrationTestBase):
    
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
        
        config = BuildConfig(
            include_local_entities=True
        )
        
        graph_index = LexicalGraphIndex(
            graph_store, 
            vector_store,
            tenant_id=LOCAL_TENANT_ID,
            indexing_config=config
        )
        
        graph_index.build(docs, show_progress=True)
    
        class BuildWithLocalEntitiesAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._graph_store = MultiTenantGraphStore.wrap(graph_store, LOCAL_TENANT_ID)
                cls._expected_num_nodes = NUM_LOCAL_AND_GLOBAL_ENTITIES

            def test_includes_local_entities(self):
                """Graph contains one entity per entity in source docs, including local entities"""
                
                results = self._graph_store.execute_query('MATCH (n:`__Entity__`) RETURN count(n) AS count')
                entity_node_count = results[0]['count']
                
                self.assertTrue(entity_node_count in self._expected_num_nodes)
                
                #self.assertEqual(entity_node_count, self._expected_num_nodes)
                
            def test_extracted_from_relationships(self):
                """Graph contains expected EXTRACTED_FROM relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Source__`)<-[r:`__EXTRACTED_FROM__`]-(:`__Chunk__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 1)
                
            def test_topic_mentioned_in_chunk_relationships(self):
                """Graph contains expected topic MENTIONED_IN chunk relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Chunk__`)<-[r:`__MENTIONED_IN__`]-(:`__Topic__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 1)
                
            def test_statement_mentioned_in_chunk_relationships(self):
                """Graph contains expected statement MENTIONED_IN chunk relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Chunk__`)<-[r:`__MENTIONED_IN__`]-(:`__Statement__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 26)
                
            def test_statement_belongs_to_topic_relationships(self):
                """Graph contains expected statement BELONGS_TO topic relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Topic__`)<-[r:`__BELONGS_TO__`]-(:`__Statement__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 26)
                
            def test_statement_previous_statement_relationships(self):
                """Graph contains expected statement PREVIOUS statment relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Statement__`)<-[r:`__PREVIOUS__`]-(:`__Statement__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 25)
                
            def test_fact_supports_statement_relationships(self):
                """Graph contains expected fact SUPPORTS statment relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Statement__`)<-[r:`__SUPPORTS__`]-(:`__Fact__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 43)
                
            def test_fact_next_fact_relationships(self):
                """Graph contains expected fact NEXT fact relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Fact__`)<-[r:`__NEXT__`]-(:`__Fact__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 27)
                
            def test_entity_subject_fact_relationships(self):
                """Graph contains expected entity SUBJECT fact relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Fact__`)<-[r:`__SUBJECT__`]-(:`__Entity__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 38)
                
            def test_entity_object_fact_relationships(self):
                """Graph contains expected entity OBJECT fact relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Fact__`)<-[r:`__OBJECT__`]-(:`__Entity__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertTrue(relationship_count in [38, 31])
                #self.assertEqual(relationship_count, 31)
                
            def test_entity_relation_entity_relationships(self):
                """Graph contains expected entity RELATION entity relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Entity__`)<-[r:`__RELATION__`]-(:`__Entity__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertTrue(relationship_count in [38, 31])
                #self.assertEqual(relationship_count, 31)
                
        handler.run_assertions(BuildWithLocalEntitiesAssertions)
        
class BuildWithoutLocalEntities(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Build graph and vector stores, without local entities'
        
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
        
        config = BuildConfig(
            include_local_entities=False
        )
        
        graph_index = LexicalGraphIndex(
            graph_store, 
            vector_store,
            tenant_id=WITHOUT_LOCAL_TENANT_ID,
            indexing_config=config
        )
        
        graph_index.build(docs, show_progress=True)
    
        class BuildWithoutLocalEntitiesAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._graph_store = MultiTenantGraphStore.wrap(graph_store, WITHOUT_LOCAL_TENANT_ID)
                cls._expected_num_nodes = NUM_GLOBAL_ENTITIES

            def test_does_not_include_local_entities(self):
                """Graph contains one entity per entity in source docs, but does not include local entities"""
                
                results = self._graph_store.execute_query('MATCH (n:`__Entity__`) RETURN count(n) AS count')
                entity_node_count = results[0]['count']
                
                self.assertEqual(entity_node_count, self._expected_num_nodes)
                
            def test_extracted_from_relationships(self):
                """Graph contains expected EXTRACTED_FROM relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Source__`)<-[r:`__EXTRACTED_FROM__`]-(:`__Chunk__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 1)
                
            def test_topic_mentioned_in_chunk_relationships(self):
                """Graph contains expected topic MENTIONED_IN chunk relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Chunk__`)<-[r:`__MENTIONED_IN__`]-(:`__Topic__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 1)
                
            def test_statement_mentioned_in_chunk_relationships(self):
                """Graph contains expected statement MENTIONED_IN chunk relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Chunk__`)<-[r:`__MENTIONED_IN__`]-(:`__Statement__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 26)
                
            def test_statement_belongs_to_topic_relationships(self):
                """Graph contains expected statement BELONGS_TO topic relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Topic__`)<-[r:`__BELONGS_TO__`]-(:`__Statement__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 26)
                
            def test_statement_previous_statement_relationships(self):
                """Graph contains expected statement PREVIOUS statment relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Statement__`)<-[r:`__PREVIOUS__`]-(:`__Statement__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 25)
                
            def test_fact_supports_statement_relationships(self):
                """Graph contains expected fact SUPPORTS statment relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Statement__`)<-[r:`__SUPPORTS__`]-(:`__Fact__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 43)
                
            def test_fact_next_fact_relationships(self):
                """Graph contains expected fact NEXT fact relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Fact__`)<-[r:`__NEXT__`]-(:`__Fact__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 15)
                
            def test_entity_subject_fact_relationships(self):
                """Graph contains expected entity SUBJECT fact relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Fact__`)<-[r:`__SUBJECT__`]-(:`__Entity__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 21)
                
            def test_entity_object_fact_relationships(self):
                """Graph contains expected entity OBJECT fact relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Fact__`)<-[r:`__OBJECT__`]-(:`__Entity__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 11)
                
            def test_entity_relation_entity_relationships(self):
                """Graph contains expected entity RELATION entity relationships"""
                
                results = self._graph_store.execute_query('MATCH (:`__Entity__`)<-[r:`__RELATION__`]-(:`__Entity__`) RETURN count(r) AS count')
                relationship_count = results[0]['count']
                
                self.assertEqual(relationship_count, 11)
                
        handler.run_assertions(BuildWithoutLocalEntitiesAssertions)