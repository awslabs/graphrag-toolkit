# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import abc
from typing import Dict, Any
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler
from graphrag_toolkit.lexical_graph import GraphRAGConfig

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


MAX_KEYS_PER_DELETE = 1000


def delete_prefix(bucket_name:str, prefix:str):
    """Remove everything a test wrote under a prefix, in the sizes S3 accepts."""
    s3_client = GraphRAGConfig.s3

    pages = s3_client.get_paginator('list_objects_v2').paginate(Bucket=bucket_name, Prefix=prefix)
    keys = [{'Key': obj['Key']} for page in pages for obj in page.get('Contents', [])]

    for start in range(0, len(keys), MAX_KEYS_PER_DELETE):
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': keys[start:start + MAX_KEYS_PER_DELETE]}
        )
