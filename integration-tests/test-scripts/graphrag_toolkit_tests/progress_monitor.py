# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import os
import unittest
from typing import Dict, Any

from graphrag_toolkit_tests.integration_test_base import IntegrationTestBase
from graphrag_toolkit_tests.integration_test_handler import IntegrationTestHandler

from graphrag_toolkit.lexical_graph import LexicalGraphIndex, GraphRAGConfig, NoOpProgressMonitor
from graphrag_toolkit.lexical_graph.storage import GraphStoreFactory
from graphrag_toolkit.lexical_graph.storage import VectorStoreFactory
from graphrag_toolkit.lexical_graph.storage.graph import NonRedactedGraphQueryLogFormatting

from llama_index.readers.web import SimpleWebPageReader


class TrackingProgressMonitor(NoOpProgressMonitor):

    def __init__(self):
        self.llm_docs = 0
        self.llm_chunks = 0
        self.graph_docs = 0
        self.graph_chunks = 0
        self.vector_docs = 0
        self.vector_chunks = 0

    def increment_llm_processed_documents(self, count=1):
        self.llm_docs += count

    def increment_llm_processed_chunks(self, count=1):
        self.llm_chunks += count

    def increment_graph_processed_documents(self, count=1):
        self.graph_docs += count

    def increment_graph_processed_chunks(self, count=1):
        self.graph_chunks += count

    def increment_vector_processed_documents(self, count=1):
        self.vector_docs += count

    def increment_vector_processed_chunks(self, count=1):
        self.vector_chunks += count


class ProgressMonitorExtractAndBuild(IntegrationTestBase):

    @property
    def description(self):
        return 'ProgressMonitor receives correct document and chunk counts during extract_and_build'

    def _run_test(self, handler: IntegrationTestHandler, params: Dict[str, Any]):

        GraphRAGConfig.extraction_llm = os.environ.get('TEST_EXTRACTION_LLM', 'anthropic.claude-sonnet-4-6')

        monitor = TrackingProgressMonitor()

        doc_urls = [
            'https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-overview.html',
            'https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-comparison.html'
        ]

        docs = SimpleWebPageReader(html_to_text=True).load_data(doc_urls)

        with (
            GraphStoreFactory.for_graph_store(
                os.environ['GRAPH_STORE'],
                log_formatting=NonRedactedGraphQueryLogFormatting()
            ) as graph_store,
            VectorStoreFactory.for_vector_store(os.environ['VECTOR_STORE']) as vector_store,
        ):
            graph_index = LexicalGraphIndex(graph_store, vector_store)
            graph_index.extract_and_build(docs, show_progress=True, progress_monitor=monitor)

        expected_docs = len(doc_urls)

        class ProgressMonitorAssertions(unittest.TestCase):

            @classmethod
            def setUpClass(cls):
                cls.monitor = monitor
                cls.expected_docs = expected_docs

            def test_llm_processed_documents_matches_input(self):
                """LLM document count equals number of input documents"""
                self.assertEqual(self.monitor.llm_docs, self.expected_docs)

            def test_llm_processed_chunks_is_positive(self):
                """LLM chunk count is positive (documents were chunked)"""
                self.assertGreater(self.monitor.llm_chunks, 0)

            def test_graph_processed_documents_matches_llm(self):
                """Graph document count equals LLM document count"""
                self.assertEqual(self.monitor.graph_docs, self.monitor.llm_docs)

            def test_vector_processed_documents_matches_graph(self):
                """Vector document count equals graph document count"""
                self.assertEqual(self.monitor.vector_docs, self.monitor.graph_docs)

            def test_graph_and_vector_chunk_counts_are_consistent(self):
                """Graph and vector chunk counts match (reported from same source)"""
                self.assertEqual(self.monitor.graph_chunks, self.monitor.vector_chunks)

            def test_graph_chunks_matches_llm_chunks(self):
                """Graph chunk count matches LLM chunk count"""
                self.assertEqual(self.monitor.graph_chunks, self.monitor.llm_chunks)

        handler.run_assertions(ProgressMonitorAssertions)
