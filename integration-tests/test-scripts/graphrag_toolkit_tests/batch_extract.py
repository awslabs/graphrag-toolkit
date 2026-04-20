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
from graphrag_toolkit.lexical_graph.indexing.load import S3BasedDocs, JSONArrayReader
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig, InferClassificationsConfig

def get_text(data):
    return f"Title: {data.get('title', '')}\nCategory: {data.get('category', '')}\nAuthor: {data.get('author', '')}\nSource: {data.get('source', '')}\nPublished At: {data.get('published_at', '')}\nURL: {data.get('url', '')}\n\n{data.get('body', '')}"

def get_metadata(data):
    metadata = {}
    metadata['title'] = data.get('title', None)
    metadata['author'] = data.get('author', None)
    metadata['source'] = data.get('source', None)
    metadata['published_at'] = data.get('published_at', None)
    metadata['url'] = data.get('url', None)
    metadata['category'] = data.get('category', None)
    return metadata

class BatchExtractToS3(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Extract propositions and topics from Neptune documentation using Bedrock batch inference, and save to S3'
        
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
        
        infer_config = InferClassificationsConfig(
            num_samples=5,
            num_iterations=10
        )

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250,
            max_num_concurrent_batches=2
        )
    
        indexing_config = IndexingConfig(
            extraction=ExtractionConfig(
                infer_entity_classifications=infer_config,
            ),   
            build=BuildConfig(
                include_local_entities=True
            ),
            batch_config=batch_config
        )
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        

            reader = JSONArrayReader(text_fn=get_text, metadata_fn=get_metadata)
            docs = reader.load_data('./source-data/corpus-modified.json')
            
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                indexing_config=indexing_config
            )
            
            graph_index.extract(docs, handler=extracted_docs, show_progress=True)
            
            collection_id = extracted_docs.collection_id
            
            params['batch_collection_id'] = collection_id
            params['multihop_expected_num_batch_docs'] = len(docs)
            
            class BatchExtractAssertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)
            
                def test_extracted_one_doc_for_each_url(self):
                    """Extracted directory in S3 contains one source doc per source URL"""
                    
                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)
                    
            handler.run_assertions(BatchExtractAssertions)
        
class BatchExtractFromS3ToS3(IntegrationTestBase):
    
    @property
    def description(self):
        return 'Extract propositions and topics from documents in S3 using Bedrock batch inference, and save to S3'
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        from llama_index.readers.s3 import S3Reader
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0')
        GraphRAGConfig.extraction_batch_size = 600
        GraphRAGConfig.extraction_num_workers = 4

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
        
        infer_config = InferClassificationsConfig(
            num_samples=5,
            num_iterations=10
        )

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=10000,
            max_num_concurrent_batches=4
        )
    
        indexing_config = IndexingConfig(
            extraction=ExtractionConfig(
                preferred_entity_classifications=None,
                infer_entity_classifications=infer_config,
            ),      
            batch_config=batch_config
        )
        
        with(
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
        

            reader = S3Reader(
                bucket=s3_results_bucket,
                prefix='ntsb'
            )
            docs = reader.load_data()
            
            graph_index = LexicalGraphIndex(
                graph_store, 
                vector_store,
                indexing_config=indexing_config
            )
            
            graph_index.extract(docs, handler=extracted_docs, show_progress=True)
            
            collection_id = extracted_docs.collection_id
            
            params['ntsb_batch_collection_id'] = collection_id
            
            class BatchExtractFromS3Assertions(unittest.TestCase):
                
                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)
            
                def test_extracted_one_doc_for_each_source_doc(self):
                    """Extracted directory in S3 contains one doc per source doc"""
                    
                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)
                    
            handler.run_assertions(BatchExtractFromS3Assertions)