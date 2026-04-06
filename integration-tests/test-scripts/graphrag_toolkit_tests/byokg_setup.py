# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
import json
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.byokg_rag.graphstore import NeptuneAnalyticsGraphStore


class LoadBYOKGGraph(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Load BYOKG graph from S3'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        region = os.environ['AWS_REGION_NAME']
        graph_store_id = os.environ['GRAPH_STORE']
        
        if not graph_store_id.startswith('neptune-graph://'):
            raise ValueError(f"Invalid graph store id. Expected Neptune graph beginning 'neptune-graph://', but received {graph_store_id}.")
            
        graph_identifier = graph_store_id[16:]
        
        graph_store = NeptuneAnalyticsGraphStore(graph_identifier=graph_identifier, region=region)
        graph_store.read_from_csv(s3_path=f"s3://aws-neptune-customer-samples-{region}/sample-datasets/gremlin/KG/")
        
        number_of_nodes = len(graph_store.nodes())
        number_of_edges = len(graph_store.edges())
        
        print(f"The graph has {number_of_nodes} nodes and {number_of_edges} edges.")
        
        schema = graph_store.get_schema()
        print(json.dumps(schema, indent=4))
        
            
        class LoadBYOKGGraphAssertions(unittest.TestCase):
            
            @classmethod
            def setUpClass(cls):
                cls._number_of_nodes = number_of_nodes
                cls._number_of_edges = number_of_edges
        
            def test_graph_has_nodes(self):
                """Graph has nodes"""
                
                self.assertGreater(self._number_of_nodes, 0)
                
            def test_graph_has_edges(self):
                """Graph has edges"""
                
                self.assertGreater(self._number_of_edges, 0)
                
        handler.run_assertions(LoadBYOKGGraphAssertions)
    
    