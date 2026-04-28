# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
import uuid
import time
import logging
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import to_tenant_id
from graphrag_toolkit.lexical_graph import LexicalGraphIndex, LexicalGraphQueryEngine, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting

logger = logging.getLogger(__name__)

from llama_index.core import SimpleDirectoryReader
from llama_index.core.readers.file.base import default_file_metadata_func

try:
    from graphrag_toolkit.lexical_graph import add_versioning_info, VersioningConfig
    from graphrag_toolkit.lexical_graph import to_metadata_filter
except ImportError as e:
    logger.warning(f'Unable to import metadata and versioning support')

VERSIONING_TENANT_ID = f'version.{uuid.uuid4().hex[:9]}' 
print(f'TENANT_ID: {VERSIONING_TENANT_ID}')

def file_metadata_fn(i):
    
    timestamp = 1761899970000 + (int(i) * 1000)
    
    def get_file_metadata(file_path):
        metadata = default_file_metadata_func(file_path)
        metadata['version'] = f'v{i}'
        metadata['deletionProtection'] = True
        return add_versioning_info(metadata, 'file_name', timestamp)
        
    return get_file_metadata


class CreateVersionedData(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Create versioned data from 4 versioned files'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        GraphRAGConfig.enable_versioning = True
        
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
                tenant_id=VERSIONING_TENANT_ID
            )
        
            for i in range(1, 5):
                reader = SimpleDirectoryReader(input_dir=f'./source-data/versioning/v{i}', file_metadata=file_metadata_fn(i))
                docs = reader.load_data()
                graph_index.extract_and_build(docs)
                
        class NullAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                return None
        
            def test_do_nothing(self):
                self.assertTrue(True)
                
        
        handler.run_assertions(NullAssertions)       
            
            
from graphrag_toolkit.lexical_graph.storage.vector import MultiTenantVectorStore
from llama_index.core.schema import QueryBundle

class QueryVersionedData(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Query versioned data'
    
    def wait(self) -> bool:
        with VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store:
            multi_tenant_vector_store = MultiTenantVectorStore.wrap(vector_store, tenant_id=to_tenant_id(VERSIONING_TENANT_ID))
            chunks = multi_tenant_vector_store.get_index('chunk').top_k(
                QueryBundle(query_str='boggle'), 
                top_k=1,
                filter_config=to_metadata_filter({'version':'v4'})
            )
            return len(chunks) == 0
    
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.response_llm = os.environ.get('TEST_RESPONSE_LLM', 'anthropic.claude-sonnet-4-6')
           
        class VersioningAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                return None
        
            def test_current_version(self):
                """Query against current version"""
                
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
                        versioning=True,
                        tenant_id=VERSIONING_TENANT_ID
                    )
                    
                    response = query_engine.query('What colour are boggles?')
                    
                    handler.add_output('response_test_current', response.response)
                
                    self.assertTrue('yellow' in response.response.lower())
                    self.assertEqual(1, len(response.source_nodes))
                    self.assertEqual('v4', response.source_nodes[0].metadata['result']['source']['metadata']['version'])
                    
            def test_v1(self):
                """Query against v1"""
                
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
                        versioning=VersioningConfig(at_timestamp=1761899971500),
                        tenant_id=VERSIONING_TENANT_ID
                    )
                    
                    response = query_engine.query('What colour are boggles?')
                    
                    handler.add_output('response_test_v1', response.response)
                
                    self.assertTrue('blue' in response.response.lower())
                    self.assertEqual(1, len(response.source_nodes))
                    self.assertEqual('v1', response.source_nodes[0].metadata['result']['source']['metadata']['version'])
                    
            def test_v2(self):
                """Query against v2"""
                
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
                        versioning=VersioningConfig(at_timestamp=1761899972500),
                        tenant_id=VERSIONING_TENANT_ID
                    )
                    
                    response = query_engine.query('What colour are boggles?')
                    
                    handler.add_output('response_test_v2', response.response)
                
                    self.assertTrue('orange' in response.response.lower())
                    self.assertEqual(1, len(response.source_nodes))
                    self.assertEqual('v2', response.source_nodes[0].metadata['result']['source']['metadata']['version'])
                    
            def test_v3(self):
                """Query against v3"""
                
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
                        versioning=VersioningConfig(at_timestamp=1761899973500),
                        tenant_id=VERSIONING_TENANT_ID
                    )
                    
                    for i in range(0, 3):
                        response = query_engine.query('What colour are boggles?')
                        if len(response.source_nodes) > 0:
                            break
                        else:
                            time.sleep(10)
                
                    handler.add_output('response_test_v3', response.response)
                    
                    self.assertTrue('green' in response.response.lower())
                    self.assertEqual(1, len(response.source_nodes))
                    self.assertEqual('v3', response.source_nodes[0].metadata['result']['source']['metadata']['version'])
                    
            def test_v4(self):
                """Query against v4"""
                
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
                        versioning=VersioningConfig(at_timestamp=1761899974500),
                        tenant_id=VERSIONING_TENANT_ID
                    )
                    
                    response = query_engine.query('What colour are boggles?')
                    
                    handler.add_output('response_test_v4', response.response)
                
                    self.assertTrue('yellow' in response.response.lower())
                    self.assertEqual(1, len(response.source_nodes))
                    self.assertEqual('v4', response.source_nodes[0].metadata['result']['source']['metadata']['version'])
                    
            def test_pre_v1(self):
                """Query against state of graph pre-v1"""
                
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
                        versioning=VersioningConfig(at_timestamp=1761899970500),
                        tenant_id=VERSIONING_TENANT_ID
                    )
                    
                    response = query_engine.query('What colour are boggles?')
                    
                    handler.add_output('response_test_pre_v1', response.response)
                
                    self.assertEqual(0, len(response.source_nodes))
                            
        handler.run_assertions(VersioningAssertions)
        
from graphrag_toolkit.lexical_graph.versioning import VersioningConfig, VersioningMode
        
class DeleteVersionedData(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Delete versioned data'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        GraphRAGConfig.enable_versioning = True
        
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
                tenant_id=VERSIONING_TENANT_ID
            )
            
            original_sources = graph_index.get_sources()
            
            versioning_config = VersioningConfig(versioning_mode=VersioningMode.PREVIOUS)
            
            deleted = graph_index.delete_sources(
                filter={
                    'version': 'v2'
                },
                versioning_config=versioning_config
            )
            
            surviving_sources = graph_index.get_sources()

        class DeleteVersionedDataAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._original_sources = original_sources
                cls._deleted = deleted
                cls._surviving_sources = surviving_sources
        
            
            def test_num_original_doc_versions(self):
                """Originally 4 versions of document"""
                self.assertEqual(len(self._original_sources), 4)
                
            def test_num_surviving_doc_versions(self):
                """3 versions of document surviving"""
                self.assertEqual(len(self._surviving_sources), 3)
                
            def test_num_deleted_docs(self):
                """1 version of document deleted"""
                self.assertEqual(len(self._deleted), 1)
                
            def test_deleted_correct_doc(self):
                """v2 deleted"""
                
                v2_source_id = None
                for s in self._original_sources:
                    if s['metadata']['version'] == 'v2':
                        v2_source_id = s['sourceId']
                        
                self.assertTrue(v2_source_id is not None)
                self.assertEqual(self._deleted[0]['sourceId'], v2_source_id)
                
                is_deleted = True
                for s in self._surviving_sources:
                    if s['metadata']['version'] == 'v2':
                        is_deleted = false
                        
                self.assertTrue(is_deleted)
                
                
        
        handler.run_assertions(DeleteVersionedDataAssertions)  
        
from graphrag_toolkit.lexical_graph.indexing.build import DeletePrevVersions
        
class AutoDeleteAllPrevVersionsData(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Auto-delete prev versions when ingesting new versions'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        GraphRAGConfig.enable_versioning = True
        
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
                tenant_id=f'v{uuid.uuid4().hex[:9]}'
            )
        
            for i in range(1, 5):
                reader = SimpleDirectoryReader(input_dir=f'./source-data/versioning/v{i}', file_metadata=file_metadata_fn(i))
                docs = reader.load_data()
                graph_index.extract_and_build(docs, handler=DeletePrevVersions(lexical_graph=graph_index))
                
            sources = graph_index.get_sources()
                
        class AutoDeletePrevVersionsAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._sources = sources
                return None
        
            def test_only_current_doc(self):
                """1 version of document remains"""
                self.assertEqual(len(self._sources), 1)
                
                is_current = False
                for s in self._sources:
                    if s['metadata']['version'] == 'v4':
                        is_current = True
                        
                self.assertTrue(is_current)
            
                
        
        handler.run_assertions(AutoDeletePrevVersionsAssertions)    
        
class DoNotAutoDeleteProtectedPrevVersionsData(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Do not auto-delete prev versions based on custom filter_fn when ingesting new versions'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        def deletion_protection_filter_fn(metadata):
            deletion_protection = metadata.get('deletionProtection', False)
            return not deletion_protection
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        GraphRAGConfig.enable_versioning = True
        
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
                tenant_id=f'v{uuid.uuid4().hex[:9]}'
            )
        
            for i in range(1, 5):
                reader = SimpleDirectoryReader(input_dir=f'./source-data/versioning/v{i}', file_metadata=file_metadata_fn(i))
                docs = reader.load_data()
                graph_index.extract_and_build(docs, handler=DeletePrevVersions(lexical_graph=graph_index, filter_fn=deletion_protection_filter_fn))
                
            sources = graph_index.get_sources()
                
        class DoNotAutoDeleteProtectedPrevVersionsAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._sources = sources
                return None
        
            def test_no_docs_deleted(self):
                """All 4 versions of document remain"""
                self.assertEqual(len(self._sources), 4)

        handler.run_assertions(DoNotAutoDeleteProtectedPrevVersionsAssertions)    
    
    