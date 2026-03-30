# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, BuildConfig
from graphrag_toolkit.lexical_graph import GraphRAGConfig, TenantId
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting, MultiTenantGraphStore
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs

TENANT_ID = TenantId('multihop')

class BuildFromS3(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Build graph and vector stores from files on S3'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        notebook_instance_type = os.environ.get('NOTEBOOK_INSTANCE_TYPE', 'ml.m5.xlarge')
        
        if '2x' in notebook_instance_type:
            num_workers = 4
        elif '4x' in notebook_instance_type:
            num_workers = 8
        else:
            num_workers = 2
        
        GraphRAGConfig.build_num_workers = num_workers
        GraphRAGConfig.build_batch_size = 32
        GraphRAGConfig.build_batch_write_size = 100
        
        collection_id = params['batch_collection_id'] 
        
        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        extracted_prefix = f'{s3_results_prefix}/extracted'
         
        docs = S3BasedDocs(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix,
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
                vector_store,
                tenant_id=TENANT_ID,
                indexing_config=BuildConfig(
                    include_domain_labels=True
                )
            )
            
            graph_index.build(docs, show_progress=True)
            
            class BuildFromS3Assertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._graph_store = MultiTenantGraphStore.wrap(graph_store, TENANT_ID)
                    cls._expected_num_docs = params['multihop_expected_num_batch_docs']
            
                def test_one_sourec_node_for_each_doc(self):
                    """Graph contains one source node per doc"""
                    
                    results = self._graph_store.execute_query('MATCH (n:`__Source__`) RETURN count(n) AS count')
                    source_node_count = results[0]['count']
                    
                    self.assertEqual(source_node_count, self._expected_num_docs)
                    
            handler.run_assertions(BuildFromS3Assertions)