# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import logging
import unittest
from typing import Dict, Any, Optional

from benchmarks.scripts.integration_test_base import IntegrationTestBase
from benchmarks.scripts.integration_test_handler import IntegrationTestHandler
from benchmarks.utils.benchmark_env import env_bool, env_int
from benchmarks.utils.s3_utils import sync_benchmark_data_from_s3

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph import GraphRAGConfig, IndexingConfig
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs, S3BasedDocs
from graphrag_toolkit.lexical_graph.indexing.extract import BatchConfig

from llama_index.core import SimpleDirectoryReader

logger = logging.getLogger(__name__)

BENCHMARK_DATA_DIR = 'source-data'


def _count_source_docs(extracted_docs) -> int:
    """
    Count extracted source documents without pulling their contents back.

    Iterating S3BasedDocs downloads every object it lists - ~16.5k GETs on the
    WikiHow run purely to produce a number. Source documents are one key prefix
    each in both layouts, so a delimiter listing counts them without bodies.
    """
    if not isinstance(extracted_docs, S3BasedDocs):
        return sum(1 for _ in extracted_docs)

    collection_path = os.path.join(
        extracted_docs.key_prefix, extracted_docs.collection_id, ''
    )
    pages = GraphRAGConfig.s3.get_paginator('list_objects_v2').paginate(
        Bucket=extracted_docs.bucket_name, Prefix=collection_path, Delimiter='/'
    )

    return sum(len(page.get('CommonPrefixes', [])) for page in pages)


def apply_extraction_config():
    """
    Apply the benchmark's extraction settings to GraphRAGConfig from the
    environment.

    extraction_batch_size and extraction_num_workers used to be hardcoded here,
    which overrode the environment and made a worker sweep report the same
    number at every point on the curve. GraphRAGConfig reads both itself, but
    that is not enough: its own defaults differ from the benchmark's
    (extraction_batch_size defaults to 4, not 15000), and its
    int(os.environ.get(...)) raises on the empty string the harness exports for
    an unset allowlisted variable.
    """
    # Must precede extraction_llm, whose assignment builds a BedrockConverse
    # and fails outright if no region is configured yet.
    aws_region = os.environ.get('AWS_REGION_NAME')
    if aws_region:
        GraphRAGConfig.aws_region = aws_region

    GraphRAGConfig.extraction_llm = os.environ.get(
        'TEST_EXTRACTION_LLM', 'us.anthropic.claude-sonnet-4-6'
    )
    GraphRAGConfig.extraction_batch_size = env_int('EXTRACTION_BATCH_SIZE', 15000)
    GraphRAGConfig.extraction_num_workers = env_int('EXTRACTION_NUM_WORKERS', 2)


def build_indexing_config(use_batch: bool, dataset_name: str) -> Optional[IndexingConfig]:
    """
    Build the indexing config for the chosen inference mode.

    Returns None for on-demand inference, which is what makes use_batch=False
    actually change what runs rather than only what gets logged.
    """
    if not use_batch:
        return None

    batch_config = BatchConfig(
        region=os.environ['AWS_REGION_NAME'],
        bucket_name=os.environ['S3_RESULTS_BUCKET'],
        key_prefix=f'{os.environ["S3_RESULTS_PREFIX"]}/batch-extract/{dataset_name}',
        role_arn=os.environ['BATCH_INFERENCE_ROLE'],
        max_batch_size=40000,
        max_num_concurrent_batches=1
    )
    return IndexingConfig(batch_config=batch_config)


def run_benchmark_extract(handler: IntegrationTestHandler,
                          dataset_name: str,
                          data_dir: str,
                          expected_docs: int,
                          use_batch: bool):
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
        use_batch: Whether to use Bedrock batch inference. No default -
            every caller states its mode, because batch wall time measures
            queue wait rather than pipeline throughput.
    """
    input_path = os.path.join(data_dir, dataset_name, 'documents')

    sync_benchmark_data_from_s3(dataset_name, data_dir)

    doc_store = os.environ.get('BENCHMARK_DOC_STORE', 'file').lower()

    # Check both paths' variables before anything dereferences os.environ with
    # bracket access, or the batch block's bare KeyError fires first.
    required_vars = set()
    if use_batch:
        required_vars.update(
            ('AWS_REGION_NAME', 'S3_RESULTS_BUCKET', 'S3_RESULTS_PREFIX', 'BATCH_INFERENCE_ROLE')
        )
    if doc_store == 's3':
        required_vars.update(('AWS_REGION_NAME', 'S3_RESULTS_BUCKET', 'S3_RESULTS_PREFIX'))

    missing = sorted(var for var in required_vars if not os.environ.get(var))
    if missing:
        raise ValueError(
            f'use_batch={use_batch}, BENCHMARK_DOC_STORE={doc_store} '
            f'requires {", ".join(missing)} to be set'
        )

    apply_extraction_config()
    indexing_config = build_indexing_config(use_batch, dataset_name)

    if doc_store == 's3':
        extracted_docs = S3BasedDocs(
            region=os.environ['AWS_REGION_NAME'],
            bucket_name=os.environ['S3_RESULTS_BUCKET'],
            key_prefix=f'{os.environ["S3_RESULTS_PREFIX"]}/doc-store/{dataset_name}',
            collection_id=None,
            for_jsonl=os.environ.get('BENCHMARK_S3_JSONL', 'false').lower() == 'true'
        )
    else:
        extracted_docs = FileBasedDocs(
            docs_directory=os.path.join(data_dir, dataset_name, 'extracted'),
            collection_id=dataset_name
        )

    logger.info(
        f'Doc store: {type(extracted_docs).__name__} '
        f'(for_jsonl={getattr(extracted_docs, "for_jsonl", "n/a")}) '
        f'collection_id={extracted_docs.collection_id}'
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

    num_extracted = _count_source_docs(extracted_docs)
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
        return 'Extract propositions and topics from ConcurrentQA documents'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        dataset_name = 'concurrentqa-prototype' if is_prototype == 'true' else 'concurrentqa'
        expected_docs = 2 if is_prototype == 'true' else 13501
        use_batch = is_prototype != 'true'

        run_benchmark_extract(handler, dataset_name, BENCHMARK_DATA_DIR,
                              expected_docs=expected_docs, use_batch=use_batch)


class WikihowBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from WikiHow documents'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        # Required, not defaulted: batch wall time is queue wait, not
        # throughput, so the mode has to be a stated choice.
        run_benchmark_extract(handler, 'wikihow', BENCHMARK_DATA_DIR,
                              expected_docs=5000,
                              use_batch=env_bool('BENCHMARK_USE_BATCH'))


class PgaBenchmarkExtract(IntegrationTestBase):

    @property
    def description(self):
        return 'Extract propositions and topics from PGA documents'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        run_benchmark_extract(handler, 'pga', BENCHMARK_DATA_DIR,
                              expected_docs=507,
                              use_batch=env_bool('BENCHMARK_USE_BATCH'))
