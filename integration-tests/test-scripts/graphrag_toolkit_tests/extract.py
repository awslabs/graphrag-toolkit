# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, GraphRAGConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

from llama_index.readers.web import SimpleWebPageReader


class ExtractToFileSystem(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Extract propositions and topics from Neptune documentation, and write to the local filesystem'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        
        extracted_docs = FileBasedDocs(
            docs_directory='extracted'
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
            
            doc_urls = [
                'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
            ]
            
            pub_dates = [
                '2023-06-15',
                '2024-01-05',
                '2024-12-23',
                '2025-02-12'
            ]
            
            docs = SimpleWebPageReader(
                html_to_text=True,
                metadata_fn=lambda url:{'url': url, 'pub_date': pub_dates.pop()}
            ).load_data(doc_urls)
            
            graph_index.extract(docs, handler=extracted_docs, show_progress=True)
            
            collection_id = extracted_docs.collection_id
            
            params['collection_id'] = collection_id
            params['expected_num_docs'] = len(doc_urls)
            
            class ExtractAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(doc_urls)
            
                def test_extracted_one_doc_for_each_url(self):
                    """Extracted directory contains one source doc per source URL"""
                    
                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)
                    
            handler.run_assertions(ExtractAssertions)
    
    