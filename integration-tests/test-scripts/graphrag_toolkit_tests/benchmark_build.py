# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from contextlib import nullcontext
from typing import Dict, Any, Optional

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting
from graphrag_toolkit.lexical_graph.indexing.load import FileBasedDocs

DATASET_CONFIG = {
    'cuad-prototype': {
        'num_docs': 2,
        'extracted_dir': os.path.join('extracted', '2026-04-16'),
    },
    'cuad': {
        'num_docs': 510,
        'extracted_dir': os.path.join('extracted', '2026-02-17'),
    },
    'pga': {
        'num_docs': 507,
    },
    'concurrentqa': {
        'num_docs': 13501,
    },
}

BENCHMARK_DATA_DIR = 'source-data'

def run_benchmark_build(handler: IntegrationTestHandler, dataset: str, data_dir: str,
                        graph_store_conn: Optional[str] = None, vector_store_conn: Optional[str] = None):
    """
    Builds graph and vector stores from pre-extracted document chunks for a benchmark dataset.

    Loads extracted chunks via FileBasedDocs, builds the graph and vector indexes, and
    asserts that the expected number of source nodes were created. Either store connection
    can be omitted — the build will proceed with whichever stores are provided, and the
    source node assertion will be skipped if no graph store is configured.

    Args:
        handler: Integration test handler for recording assertions and output.
        dataset: Dataset key (e.g. 'cuad', 'pga', 'concurrentqa'). Must have a
            corresponding entry in DATASET_CONFIG.
        data_dir: Root path to the benchmark data directory containing dataset subdirectories.
        graph_store_conn: Optional graph store connection string (e.g. 'neptune-db://<hostname>' or 
            'neptune-graph://<graph-id>').
        vector_store_conn: Optional vector store connection string (e.g. 'aoss://...').
    """
    config = DATASET_CONFIG.get(dataset, {})

    extracted_subdir = config.get('extracted_dir', 'extracted')
    docs_directory = os.path.join(data_dir, dataset, extracted_subdir)

    docs = FileBasedDocs(
        docs_directory=docs_directory,
        collection_id=dataset
    )

    graph_ctx = GraphStoreFactory.for_graph_store(
        graph_store_conn, log_formatting=NonRedactedGraphQueryLogFormatting()
    ) if graph_store_conn else nullcontext()

    vector_ctx = VectorStoreFactory.for_vector_store(
        vector_store_conn
    ) if vector_store_conn else nullcontext()

    with graph_ctx as graph_store, vector_ctx as vector_store:
        graph_index = LexicalGraphIndex(graph_store, vector_store)
        graph_index.build(docs, show_progress=True)

        expected_num_docs = config.get('num_docs')

        class BenchmarkBuildAssertions(unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                cls._graph_store = graph_store
                cls._expected_num_docs = expected_num_docs

            def test_one_source_node_for_each_doc(self):
                """Graph contains one source node per document"""
                if self._graph_store is None:
                    self.skipTest('No graph store configured')
                results = self._graph_store.execute_query('MATCH (n:`__Source__`) RETURN count(n) AS count')
                source_node_count = results[0]['count']
                if self._expected_num_docs is not None:
                    self.assertEqual(source_node_count, self._expected_num_docs)
                else:
                    self.assertGreater(source_node_count, 0)

        handler.run_assertions(BenchmarkBuildAssertions)


class CuadBenchmarkBuild(IntegrationTestBase):

    @property
    def description(self):
        return 'Build graph and vector stores from CUAD pre-extracted chunks for benchmarking'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):
        graph_store_conn = os.environ.get('GRAPH_STORE')
        vector_store_conn = os.environ.get('VECTOR_STORE')
        is_prototype = os.environ.get('BENCHMARK_IS_PROTOTYPE')
        if is_prototype == 'true':
            dataset_name = 'cuad-prototype' 
        else:
            dataset_name = 'cuad'

        run_benchmark_build(handler, dataset_name, BENCHMARK_DATA_DIR, graph_store_conn, vector_store_conn)
