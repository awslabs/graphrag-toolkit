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

CUAD_NUM_DOCS = 510

class BenchmarkBuild(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Build graph and vector stores from CUAD pre-extracted chunks for benchmarking'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        data_dir = os.environ.get('BENCHMARK_DATA_DIR', 'benchmark-tests/data')
        dataset = os.environ.get('BENCHMARK_DATASET', 'cuad')
        
        docs_directory = os.path.join(data_dir, dataset, 'extracted', '2026-02-17')
        
        docs = FileBasedDocs(
            docs_directory=docs_directory,
            collection_id=dataset
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
            
            # Store graph_store in params for downstream tests (query stage)
            params['benchmark_graph_store'] = os.environ['GRAPH_STORE']
            params['benchmark_vector_store'] = os.environ['VECTOR_STORE']
            params['benchmark_dataset'] = dataset
            params['benchmark_data_dir'] = data_dir
            
            class BenchmarkBuildAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._graph_store = graph_store
                    cls._expected_num_docs = CUAD_NUM_DOCS
            
                def test_one_source_node_for_each_doc(self):
                    """Graph contains one source node per CUAD document"""
                    
                    results = self._graph_store.execute_query('MATCH (n:`__Source__`) RETURN count(n) AS count')
                    source_node_count = results[0]['count']
                    
                    self.assertEqual(source_node_count, self._expected_num_docs)
                    
            handler.run_assertions(BenchmarkBuildAssertions)
