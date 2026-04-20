# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import GraphRAGConfig, IndexingConfig, ExtractionConfig, BuildConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig

from llama_index.core.node_parser import SentenceSplitter
from llama_index.readers.web import SimpleWebPageReader

class BatchExtractToS3Fallback(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Use batch extract, but with small number of chunks, so fallback to chunk-by-chunk extract'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        GraphRAGConfig.extraction_batch_size = 100
        GraphRAGConfig.extraction_num_workers = 2

        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference'
        extracted_prefix = f'{s3_results_prefix}/extracted'
         
        extracted_docs = S3BasedDocs(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix
        )

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250
        )
        
        splitter = SentenceSplitter(chunk_size=6000, chunk_overlap=600)

        indexing_config = IndexingConfig(
            batch_config=batch_config,
            chunking=[splitter]
        )
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        

            doc_urls = [
                'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
            ]
            
            docs = SimpleWebPageReader(
                html_to_text=True,
                metadata_fn=lambda url:{'url': url}
            ).load_data(doc_urls)
            
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                indexing_config=indexing_config
            )
            
            graph_index.extract(docs, handler=extracted_docs, show_progress=True)
            
            collection_id = extracted_docs.collection_id
            
            params['fallback_batch_collection_id'] = collection_id
            params['fallback_expected_num_batch_docs'] = len(docs)
            
            class BatchExtractFallbackAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)
            
                def test_extracted_one_doc_for_each_url(self):
                    """Extracted directory in S3 contains one source doc per source URL"""
                    
                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)
                    
            handler.run_assertions(BatchExtractFallbackAssertions)
        

        extracted_docs = S3BasedDocs(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix
        )

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250
        )
        
        splitter = SentenceSplitter(chunk_size=6000, chunk_overlap=600)

        indexing_config = IndexingConfig(
            batch_config=batch_config,
            chunking=[splitter]
        )
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        

            doc_urls = [
                'https://docs.aws.amazon.com/neptune/latest/userguide/intro.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/what-is-neptune-analytics.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-features.html',
                'https://docs.aws.amazon.com/neptune-analytics/latest/userguide/neptune-analytics-vs-neptune-database.html'
            ]
            
            docs = SimpleWebPageReader(
                html_to_text=True,
                metadata_fn=lambda url:{'url': url}
            ).load_data(doc_urls)
            
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                indexing_config=indexing_config
            )
            
            graph_index.extract(docs, handler=extracted_docs, show_progress=True)
            
            collection_id = extracted_docs.collection_id
            
            params['fallback_batch_collection_id'] = collection_id
            params['fallback_expected_num_batch_docs'] = len(docs)
            
            class BatchExtractFallbackAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)
            
                def test_extracted_one_doc_for_each_url(self):
                    """Extracted directory in S3 contains one source doc per source URL"""
                    
                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)
                    
            handler.run_assertions(BatchExtractFallbackAssertions)
        
