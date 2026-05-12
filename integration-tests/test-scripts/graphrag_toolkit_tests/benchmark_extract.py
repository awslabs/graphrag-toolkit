# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
import unittest
from typing import Dict, Any, Optional

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, GraphRAGConfig, IndexingConfig, ExtractionConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig

from llama_index.core import SimpleDirectoryReader

import logging

logger = logging.getLogger(__name__)

BENCHMARK_DATA_DIR = 'source-data'


def sync_benchmark_data_from_s3(dataset: str, data_dir: str):
    """
    If BENCHMARK_DATA_S3_URI is set and the local dataset directory doesn't exist,
    sync the dataset from S3.
    """
    s3_uri = os.environ.get('BENCHMARK_DATA_S3_URI')
    if not s3_uri:
        return

    local_dataset_dir = os.path.join(data_dir, dataset)
    if os.path.exists(local_dataset_dir):
        logger.info(f'Dataset directory already exists: {local_dataset_dir}')
        return

    s3_dataset_uri = s3_uri.rstrip('/') + '/' + dataset + '/'
    logger.info(f'Syncing benchmark data from {s3_dataset_uri} to {local_dataset_dir}')
    os.makedirs(local_dataset_dir, exist_ok=True)
    subprocess.run(
        ['aws', 's3', 'sync', s3_dataset_uri, local_dataset_dir],
        check=True
    )
    logger.info(f'Sync complete: {local_dataset_dir}')


def run_benchmark_extract(handler: IntegrationTestHandler,
                          params: Dict[str, Any],
                          dataset: str,
                          data_dir: str,
                          graph_store_conn: Optional[str] = None,
                          vector_store_conn: Optional[str] = None,
                          expected_num_docs: Optional[int] = None,
                          use_batch_inference: bool = False):
    """
    Extracts propositions and topics from benchmark dataset documents and writes
    extracted chunks to the filesystem for subsequent build steps.

    Reads raw documents from <data_dir>/<dataset>/documents/, runs LLM-based
    extraction (propositions + topics), and writes results to
    <data_dir>/<dataset>/extracted/.

    When use_batch_inference=True, uses Bedrock batch inference jobs for dramatically
    faster extraction on large datasets. Requires BATCH_INFERENCE_ROLE, S3_RESULTS_BUCKET,
    S3_RESULTS_PREFIX, and AWS_REGION_NAME environment variables.

    Args:
        handler: Integration test handler for recording assertions and output.
        params: Shared params dict passed between pipeline stages.
        dataset: Dataset key (e.g. 'concurrentqa-prototype').
        data_dir: Root path to the benchmark data directory.
        graph_store_conn: Optional graph store connection string.
        vector_store_conn: Optional vector store connection string.
        expected_num_docs: Expected number of source documents (for assertion).
        use_batch_inference: Whether to use Bedrock batch inference (default: False).
    """
    GraphRAGConfig.extraction_llm = os.environ.get(
        'TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-20250514-v1:0'
    )

    sync_benchmark_data_from_s3(dataset, data_dir)

    docs_directory = os.path.join(data_dir, dataset, 'documents')
    extracted_directory = os.path.join(data_dir, dataset, 'extracted')

    extracted_docs = FileBasedDocs(
        docs_directory=extracted_directory,
        collection_id=dataset
    )

    from contextlib import nullcontext

    # Configure batch inference if requested
    indexing_config = None
    if use_batch_inference:
        s3_results_bucket = os.environ['S3_RESULTS_BUCKET']
        s3_results_prefix = os.environ['S3_RESULTS_PREFIX']
        aws_region_name = os.environ['AWS_REGION_NAME']
        batch_inference_role = os.environ['BATCH_INFERENCE_ROLE']
        batch_inference_prefix = f'{s3_results_prefix}/batch-inference/{dataset}'

        batch_config = BatchConfig(
            region=aws_region_name,
            bucket_name=s3_results_bucket,
            key_prefix=batch_inference_prefix,
            role_arn=batch_inference_role,
            max_batch_size=40000,
            max_num_concurrent_batches=1
        )

        indexing_config = IndexingConfig(
            extraction=ExtractionConfig(),
            batch_config=batch_config
        )

        GraphRAGConfig.extraction_num_workers = 4
        GraphRAGConfig.extraction_batch_size = 15000

        logger.info(f'Using batch inference for {dataset} extraction')
    else:
        logger.info(f'Using real-time inference for {dataset} extraction')

    graph_ctx = GraphStoreFactory.for_graph_store(
        graph_store_conn, log_formatting=NonRedactedGraphQueryLogFormatting()
    ) if graph_store_conn else nullcontext()

    vector_ctx = VectorStoreFactory.for_vector_store(
        vector_store_conn
    ) if vector_store_conn else nullcontext()

    with graph_ctx as graph_store, vector_ctx as vector_store:
        if indexing_config:
            graph_index = LexicalGraphIndex(graph_store, vector_store, indexing_config=indexing_config)
        else:
            graph_index = LexicalGraphIndex(graph_store, vector_store)

        docs = SimpleDirectoryReader(input_dir=docs_directory).load_data()

        graph_index.extract(docs, handler=extracted_docs, show_progress=True)

        num_extracted = len([d for d in extracted_docs])

        handler.add_output('num_extracted_docs', num_extracted)
        handler.add_output('use_batch_inference', use_batch_inference)
        params['benchmark_extracted_dir'] = extracted_directory

        class BenchmarkExtractAssertions(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls._num_extracted = num_extracted
                cls._expected_num_docs = expected_num_docs

            def test_extracted_docs_exist(self):
                """At least one document was extracted"""
                self.assertGreater(self._num_extracted, 0)

            def test_expected_doc_count(self):
                """Extracted the expected number of documents"""
                if self._expected_num_docs is not None:
                    self.assertEqual(self._num_extracted, self._expected_num_docs)

        handler.run_assertions(BenchmarkExtractAssertions)


class ConcurrentQaBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from ConcurrentQA documents using batch inference'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'concurrentqa-prototype' if is_prototype == 'true' else 'concurrentqa'

        expected_docs = 2 if is_prototype == 'true' else 13501

        # Use batch inference for full dataset, real-time for prototype
        use_batch = is_prototype != 'true'

        run_benchmark_extract(
            handler,
            params,
            dataset=dataset_name,
            data_dir=BENCHMARK_DATA_DIR,
            graph_store_conn=os.environ.get('GRAPH_STORE'),
            vector_store_conn=os.environ.get('VECTOR_STORE'),
            expected_num_docs=expected_docs,
            use_batch_inference=use_batch,
        )
