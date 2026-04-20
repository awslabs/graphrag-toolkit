# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import abc
from typing import Dict, Any
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

class IntegrationTestBase():
    
    @property
    @abc.abstractmethod
    def description(self):
        pass
        
    def wait(self) -> bool:
        return False
    
    @abc.abstractmethod
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        pass
        
    def init_test_details(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        handler.init_with_test_details(self.__class__.__name__, self.description, params)
    
    def run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        handler.start_test()
        self._run_test(handler, params)