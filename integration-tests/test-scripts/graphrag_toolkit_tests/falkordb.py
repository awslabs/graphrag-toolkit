# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler


from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit_contrib.lexical_graph.storage.graph.falkordb import FalkorDBGraphStoreFactory

class TestFalkorDBContrib(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Test install of FalkorDB install'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphStoreFactory.register(FalkorDBGraphStoreFactory)
        
        falkordb_connection_info = 'falkordb://myendpoint'

        graph_store = GraphStoreFactory.for_graph_store(falkordb_connection_info)
        
        msg = None
        
        try:
            graph_store.execute_query('MATCH n RETURN n LIMIT 1')
        except Exception as e:
            msg = str(e)
    
        class FalkorDBAssertions(unittest.TestCase):

            def test_error_message_indicates_library_was_installed(self):
                """Error message raised by FalkorDB graph store"""
                
                expected_message = "Error parsing endpoint url: Invalid endpoint URL format. Expected format: 'falkordb://host:port' or for local use 'falkordb://"
                
                self.assertTrue(expected_message in msg)
                
        handler.run_assertions(FalkorDBAssertions)