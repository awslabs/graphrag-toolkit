# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import abc
from typing import Dict, Any
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler
from graphrag_toolkit.lexical_graph import GraphRAGConfig
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import MAX_KEYS_PER_DELETE
from graphrag_toolkit.lexical_graph.indexing.load.s3_based_docs import to_batches

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


def delete_prefix(bucket_name:str, prefix:str):
    """
    Remove everything a test wrote under a prefix, in the sizes S3 accepts.

    The bucket is the caller's, not one the suite created, so an empty prefix
    would list and delete all of it.
    """
    if not prefix or not prefix.strip():
        raise ValueError('delete_prefix needs a prefix; an empty one covers the whole bucket')

    s3_client = GraphRAGConfig.s3

    pages = s3_client.get_paginator('list_objects_v2').paginate(Bucket=bucket_name, Prefix=prefix)
    keys = [{'Key': obj['Key']} for page in pages for obj in page.get('Contents', [])]

    for batch in to_batches(keys, MAX_KEYS_PER_DELETE):
        refused = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': batch}).get('Errors', [])
        if refused:
            raise RuntimeError(f'Could not clean up under {prefix}: {refused}')
