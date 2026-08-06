# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import logging
import unittest
from typing import Dict, Any, Optional

from benchmarks.scripts.integration_test_base import IntegrationTestBase
from benchmarks.scripts.integration_test_handler import IntegrationTestHandler
from benchmarks.utils.s3_utils import sync_benchmark_data_from_s3

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


def run_benchmark_extract(handler: IntegrationTestHandler,
                          dataset_name: str,
                          data_dir: str,
                          expected_docs: int,
                          use_batch: bool = True):
    """
    Extracts propositions and topics from benchmark dataset documents.

    Reads raw documents from <data_dir>/<dataset_name>/documents/, runs LLM-based
    extraction (propositions + topics), and writes results to
    <data_dir>/<dataset_name>/extracted/.

    When use_batch=True, uses Bedrock batch inference for faster extraction on
    large datasets. Requires BATCH_INFERENCE_ROLE, S3_RESULTS_BUCKET,
    S3_RESULTS_PREFIX, and AWS_REGION_NAME environment variables.

    Args:
        handler: Integration test handler for recording assertions and output.
        dataset_name: Dataset key (e.g. 'concurrentqa', 'wikihow', 'pga').
        data_dir: Root path to the benchmark data directory.
        expected_docs: Expected number of source documents (for assertion).
        use_batch: Whether to use Bedrock batch inference (default: True).
    """
    input_path = os.path.join(data_dir, dataset_name, 'documents')

    sync_benchmark_data_from_s3(dataset_name, data_dir)

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
        docs_directory=os.path.join(data_dir, dataset_name, 'extracted'),
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

    num_extracted = sum(1 for _ in extracted_docs)
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


class ConcurrentQaBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from ConcurrentQA documents using batch inference'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'concurrentqa-prototype' if is_prototype == 'true' else 'concurrentqa'
        expected_docs = 2 if is_prototype == 'true' else 13501
        use_batch = is_prototype != 'true'

        run_benchmark_extract(handler, dataset_name, BENCHMARK_DATA_DIR, expected_docs, use_batch)


class WikihowBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from WikiHow documents using batch inference'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        run_benchmark_extract(handler, 'wikihow', BENCHMARK_DATA_DIR, expected_docs=5000)


class PgaBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from PGA documents using batch inference'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        run_benchmark_extract(handler, 'pga', BENCHMARK_DATA_DIR, expected_docs=507)
