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

def apply_extraction_doc_limit(docs):
    """Optionally cap the number of source documents used for extraction.

    Controlled by the BENCHMARK_EXTRACT_DOC_LIMIT environment variable (set
    directly, via .env / .env.testing, or through the build-tests.sh
    --benchmark-extract-doc-limit flag). When it is a positive integer, only
    the first N documents are extracted and the rest are skipped; when it is
    unset, empty, or non-positive, all documents are extracted as normal.

    Capping the input list here — before extract() is called — is worker-count
    agnostic: extraction concurrency (num_workers) operates on whatever
    documents remain in the list, so this behaves identically for single- and
    multi-threaded runs. Downstream assertions and the batch_build.BuildFromS3
    step read len(docs) after capping, so expected counts stay consistent.
    """
    raw_limit = os.environ.get('BENCHMARK_EXTRACT_DOC_LIMIT', '').strip()
    if not raw_limit:
        return docs

    try:
        limit = int(raw_limit)
    except ValueError:
        raise ValueError(
            f"BENCHMARK_EXTRACT_DOC_LIMIT must be an integer, but got '{raw_limit}'"
        )

    if limit <= 0 or limit >= len(docs):
        return docs

    print(f'[benchmark] BENCHMARK_EXTRACT_DOC_LIMIT set: extracting first {limit} of {len(docs)} documents')
    return docs[:limit]

class BatchExtractToS3(IntegrationTestBase):
    
    @property
    def description(self):
        return ('Baseline batch extraction: extract propositions and topics from the local Neptune docs '
                'corpus (source-data/corpus-modified.json) using Bedrock batch inference with a fixed '
                'batch_size (100), save to S3; output feeds batch_build.BuildFromS3')
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
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
            docs = apply_extraction_doc_limit(docs)

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
        
class BatchExtractAutoTuneToS3(IntegrationTestBase):

    @property
    def description(self):
        return ('Like BatchExtractToS3 over the same local corpus, but auto-tunes batch_size '
                '(auto_tune=True; user sets only num_workers + max_batch_size, no fixed batch_size or '
                'max_num_concurrent_batches); writes to the same extracted/ prefix so output feeds '
                'batch_build.BuildFromS3')

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        # Under auto-tuning, batch_size is a derived value; the user only needs
        # to specify num_workers (here) and max_batch_size (on BatchConfig).
        GraphRAGConfig.extraction_num_workers = 2

        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference-auto-tune'
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

        # Under auto-tuning the user specifies only num_workers (above) and
        # max_batch_size; batch_size is derived. max_num_concurrent_batches is
        # not used in auto-tuning mode (num_workers is the unit of concurrency).
        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=250,
            auto_tune=True
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
            docs = apply_extraction_doc_limit(docs)

            graph_index = LexicalGraphIndex(
                graph_store,
                vector_store,
                indexing_config=indexing_config
            )

            graph_index.extract(docs, handler=extracted_docs, show_progress=True)

            collection_id = extracted_docs.collection_id

            # Use the same param keys as BatchExtractToS3 so that batch_build.BuildFromS3
            # picks up this collection and its expected doc count as the next step.
            params['batch_collection_id'] = collection_id
            params['multihop_expected_num_batch_docs'] = len(docs)

            class BatchExtractAutoTuneAssertions(unittest.TestCase):

                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)

                def test_extracted_one_doc_for_each_url(self):
                    """Auto-tuned extraction produces one source doc per source URL,
                    equivalent to the fixed-batch_size run over the same corpus."""

                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)

            handler.run_assertions(BatchExtractAutoTuneAssertions)

class BatchExtractSmallBatchToS3(IntegrationTestBase):
    """Fixed-batch_size baseline with a deliberately small (mis-chosen) batch_size,
    to demonstrate the under-filled jobs that auto-tuning avoids. Compare this
    run's batch-inference-small-batch/ job count and fill against the auto-tune
    run's batch-inference-auto-tune/ over the same corpus."""

    @property
    def description(self):
        return ('Like BatchExtractToS3 over the same local corpus, but with a deliberately small fixed '
                'batch_size (10) that under-fills batch jobs; the mis-chosen baseline for comparing job '
                'count/fill against the auto-tune run (correctness is unaffected)')

    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):

        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
        # A small batch_size under-fills batch jobs: 10 docs per outer batch,
        # split across 2 workers, is far below num_workers x max_batch_size.
        extraction_batch_size = 10

        # This test's only unique value is as the under-fill baseline for the
        # auto-tune comparison, which needs the small batch_size to produce more
        # than one (under-filled) batch job. A benchmark cap at or below the
        # batch_size yields a single partial job — indistinguishable from what
        # auto-tuning would produce — so there is no comparison signal. Skip
        # (rather than report a misleading pass); correctness of the small-batch
        # path is already covered by BatchExtractToS3 over the same corpus.
        doc_limit = os.environ.get('BENCHMARK_EXTRACT_DOC_LIMIT', '').strip()
        if doc_limit.isdigit() and 0 < int(doc_limit) <= extraction_batch_size:
            print(f'Skipping BatchExtractSmallBatchToS3: BENCHMARK_EXTRACT_DOC_LIMIT={doc_limit} '
                  f'is <= batch_size ({extraction_batch_size}); the under-fill comparison has no signal')
            handler.skip()
            return

        GraphRAGConfig.extraction_batch_size = extraction_batch_size
        GraphRAGConfig.extraction_num_workers = 2

        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference-small-batch'
        extracted_prefix = f'{s3_results_prefix}/extracted-small-batch'

        extracted_docs = S3BasedDocs(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=extracted_prefix
        )

        infer_config = InferClassificationsConfig(
            num_samples=5,
            num_iterations=10
        )

        # Same max_batch_size and num_workers as the auto-tune run (auto_tune off),
        # so the only difference is the small fixed batch_size.
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
            # A benchmark cap reduces the corpus for a fast run. The extreme case
            # (cap <= batch_size, a single under-filled job with no comparison
            # signal) is skipped above; between that and the full corpus the fill
            # comparison is progressively less pronounced but still present.
            docs = apply_extraction_doc_limit(docs)

            graph_index = LexicalGraphIndex(
                graph_store,
                vector_store,
                indexing_config=indexing_config
            )

            graph_index.extract(docs, handler=extracted_docs, show_progress=True)

            collection_id = extracted_docs.collection_id

            params['small_batch_collection_id'] = collection_id

            class BatchExtractSmallBatchAssertions(unittest.TestCase):

                @classmethod
                def setUpClass(cls):
                    cls._num_extracted_docs = len([d for d in extracted_docs])
                    cls._expected_num_docs = len(docs)

                def test_extracted_one_doc_for_each_url(self):
                    """Small-batch_size extraction still produces one source doc per URL;
                    the difference vs auto-tuning is job count/fill, not correctness."""

                    self.assertEqual(self._num_extracted_docs, self._expected_num_docs)

            handler.run_assertions(BatchExtractSmallBatchAssertions)

class BatchExtractFromS3ToS3(IntegrationTestBase):

    @property
    def description(self):
        return ('Like BatchExtractToS3 but reads a different, larger source dataset (NTSB docs) from S3 via '
                'S3Reader instead of the local JSON corpus, at larger scale (batch_size 600, num_workers 4, '
                'max_batch_size 10000); saves to S3')
        
    def _run_test(self, handler:IntegrationTestHandler, params:Dict[str, Any]):
        
        from llama_index.readers.s3 import S3Reader
        
        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')
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
            docs = apply_extraction_doc_limit(docs)

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