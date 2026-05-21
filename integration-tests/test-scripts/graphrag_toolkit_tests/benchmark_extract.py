# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
import logging
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import GraphRAGConfig, IndexingConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig

from llama_index.core import SimpleDirectoryReader

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


class ConcurrentQaBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from ConcurrentQA documents using batch inference'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'concurrentqa-prototype' if is_prototype == 'true' else 'concurrentqa'
        expected_docs = 2 if is_prototype == 'true' else 13501
        use_batch = is_prototype != 'true'

        input_path = os.path.join(BENCHMARK_DATA_DIR, dataset_name, 'documents')

        sync_benchmark_data_from_s3(dataset_name, BENCHMARK_DATA_DIR)

        # Set batch size and workers before creating any config
        GraphRAGConfig.extraction_llm = os.environ.get(
            'TEST_EXTRACTION_LLM', 'us.anthropic.claude-sonnet-4-6'
        )
        GraphRAGConfig.extraction_batch_size = 15000
        GraphRAGConfig.extraction_num_workers = 2

        indexing_config = None
        if use_batch:
            batch_config = BatchConfig(
                region=os.environ['AWS_REGION_NAME'],
                bucket_name=os.environ['S3_RESULTS_BUCKET'],
                key_prefix=f'{os.environ["S3_RESULTS_PREFIX"]}/batch-extract/{dataset_name}',
                role_arn=os.environ['BATCH_INFERENCE_ROLE'],
                max_batch_size=40000,
                max_num_concurrent_batches=1
            )
            indexing_config = IndexingConfig(batch_config=batch_config)

        extracted_docs = FileBasedDocs(
            docs_directory=os.path.join(BENCHMARK_DATA_DIR, dataset_name, 'extracted'),
            collection_id=dataset_name
        )

        with (
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
            if indexing_config:
                graph_index = LexicalGraphIndex(graph_store, vector_store, indexing_config=indexing_config)
            else:
                graph_index = LexicalGraphIndex(graph_store, vector_store)

            docs = SimpleDirectoryReader(input_dir=input_path).load_data()
            logger.info(f'Starting extraction for {len(docs)} documents')

            graph_index.extract(docs, handler=extracted_docs, show_progress=True)

        num_extracted = len([d for d in extracted_docs])
        handler.add_output('num_extracted_docs', num_extracted)
        handler.add_output('collection_id', extracted_docs.collection_id)

        class BenchmarkExtractAssertions(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls._num_extracted = num_extracted
                cls._expected_num_docs = expected_docs

            def test_extracted_docs_exist(self):
                """At least one document was extracted"""
                self.assertGreater(self._num_extracted, 0)

            def test_expected_doc_count(self):
                """Extracted the expected number of documents"""
                if self._expected_num_docs is not None:
                    self.assertEqual(self._num_extracted, self._expected_num_docs)

        handler.run_assertions(BenchmarkExtractAssertions)


class WikihowBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from WikiHow documents using batch inference'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        dataset_name = 'wikihow'
        expected_docs = 5000

        input_path = os.path.join(BENCHMARK_DATA_DIR, dataset_name, 'documents')

        sync_benchmark_data_from_s3(dataset_name, BENCHMARK_DATA_DIR)

        GraphRAGConfig.extraction_llm = os.environ.get(
            'TEST_EXTRACTION_LLM', 'us.anthropic.claude-sonnet-4-6'
        )
        GraphRAGConfig.extraction_batch_size = 15000
        GraphRAGConfig.extraction_num_workers = 2

        batch_config = BatchConfig(
            region=os.environ['AWS_REGION_NAME'],
            bucket_name=os.environ['S3_RESULTS_BUCKET'],
            key_prefix=f'{os.environ["S3_RESULTS_PREFIX"]}/batch-extract/{dataset_name}',
            role_arn=os.environ['BATCH_INFERENCE_ROLE'],
            max_batch_size=40000,
            max_num_concurrent_batches=1
        )
        indexing_config = IndexingConfig(batch_config=batch_config)

        extracted_docs = FileBasedDocs(
            docs_directory=os.path.join(BENCHMARK_DATA_DIR, dataset_name, 'extracted'),
            collection_id=dataset_name
        )

        with (
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store
        ):
            graph_index = LexicalGraphIndex(graph_store, vector_store, indexing_config=indexing_config)

            docs = SimpleDirectoryReader(input_dir=input_path).load_data()
            logger.info(f'Starting extraction for {len(docs)} documents')

            graph_index.extract(docs, handler=extracted_docs, show_progress=True)

        num_extracted = len([d for d in extracted_docs])
        handler.add_output('num_extracted_docs', num_extracted)
        handler.add_output('collection_id', extracted_docs.collection_id)

        class BenchmarkExtractAssertions(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls._num_extracted = num_extracted
                cls._expected_num_docs = expected_docs

            def test_extracted_docs_exist(self):
                """At least one document was extracted"""
                self.assertGreater(self._num_extracted, 0)

            def test_expected_doc_count(self):
                """Extracted the expected number of documents"""
                if self._expected_num_docs is not None:
                    self.assertEqual(self._num_extracted, self._expected_num_docs)

        handler.run_assertions(BenchmarkExtractAssertions)
