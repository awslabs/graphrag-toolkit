# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import logging

from graphrag_toolkit.lexical_graph.indexing.progress_monitor import ProgressMonitor, NoOpProgressMonitor


class TestProgressMonitorABC:

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            ProgressMonitor()

    def test_must_implement_all_methods(self):
        class IncompleteMonitor(ProgressMonitor):
            def increment_llm_processed_documents(self, count=1):
                pass

        with pytest.raises(TypeError):
            IncompleteMonitor()


class TestNoOpProgressMonitor:

    def test_can_instantiate(self):
        monitor = NoOpProgressMonitor()
        assert monitor is not None

    def test_all_methods_callable(self):
        monitor = NoOpProgressMonitor()
        monitor.increment_llm_processed_documents()
        monitor.increment_llm_processed_documents(5)
        monitor.increment_llm_processed_chunks()
        monitor.increment_llm_processed_chunks(10)
        monitor.increment_graph_processed_documents()
        monitor.increment_graph_processed_documents(3)
        monitor.increment_graph_processed_chunks()
        monitor.increment_graph_processed_chunks(7)
        monitor.increment_vector_processed_documents()
        monitor.increment_vector_processed_documents(2)
        monitor.increment_vector_processed_chunks()
        monitor.increment_vector_processed_chunks(8)

    def test_subclass_override_subset(self):
        class DocCountMonitor(NoOpProgressMonitor):
            def __init__(self):
                self.doc_count = 0

            def increment_llm_processed_documents(self, count=1):
                self.doc_count += count

        monitor = DocCountMonitor()
        monitor.increment_llm_processed_documents()
        monitor.increment_llm_processed_documents(4)
        monitor.increment_llm_processed_chunks(10)
        monitor.increment_graph_processed_documents(2)

        assert monitor.doc_count == 5


class TestExtractionMonitorPipe:

    def test_extraction_monitor_fires_correctly(self):
        from unittest.mock import MagicMock
        from pipe import Pipe
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode, NodeRelationship, RelatedNodeInfo

        monitor = MagicMock(spec=NoOpProgressMonitor)

        node1 = TextNode(text="chunk1", id_="c1")
        node1.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="src1")
        node2 = TextNode(text="chunk2", id_="c2")
        node2.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="src1")
        node3 = TextNode(text="chunk3", id_="c3")
        node3.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="src1")

        doc = SourceDocument(nodes=[node1, node2, node3])

        extraction_monitor = LexicalGraphIndex._create_extraction_monitor_pipe(monitor)

        result = list([doc] | extraction_monitor)

        assert len(result) == 1
        assert result[0] is doc
        monitor.increment_llm_processed_documents.assert_called_once_with(1)
        monitor.increment_llm_processed_chunks.assert_called_once_with(3)

    def test_extraction_monitor_multiple_documents(self):
        from unittest.mock import MagicMock
        from pipe import Pipe
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode, NodeRelationship, RelatedNodeInfo

        monitor = MagicMock(spec=NoOpProgressMonitor)

        doc1 = SourceDocument(nodes=[
            TextNode(text="a", id_="a1"),
            TextNode(text="b", id_="a2"),
        ])
        doc2 = SourceDocument(nodes=[
            TextNode(text="c", id_="b1"),
        ])

        extraction_monitor = LexicalGraphIndex._create_extraction_monitor_pipe(monitor)

        result = list([doc1, doc2] | extraction_monitor)

        assert len(result) == 2
        assert monitor.increment_llm_processed_documents.call_count == 2
        calls = monitor.increment_llm_processed_chunks.call_args_list
        assert calls[0][0][0] == 2
        assert calls[1][0][0] == 1


class TestBuildPipelineMonitor:

    def _make_build_pipeline(self, monitor):
        from unittest.mock import MagicMock, patch
        from graphrag_toolkit.lexical_graph.indexing.build.build_pipeline import BuildPipeline
        from llama_index.core.ingestion import IngestionPipeline

        pipeline = BuildPipeline.__new__(BuildPipeline)
        pipeline.inner_pipeline = MagicMock(spec=IngestionPipeline)
        pipeline.num_workers = 1
        pipeline.batch_size = 100
        pipeline.batch_writes_enabled = False
        pipeline.batch_write_size = 10
        pipeline.include_domain_labels = False
        pipeline.include_local_entities = False
        pipeline.node_builders = MagicMock()
        pipeline.node_builders.return_value = []
        pipeline.node_filter = MagicMock(return_value=[])
        pipeline.pipeline_kwargs = {}
        pipeline.progress_monitor = monitor
        return pipeline

    def test_build_monitor_fires_for_single_batch(self):
        from unittest.mock import MagicMock, patch
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode

        monitor = MagicMock(spec=NoOpProgressMonitor)
        pipeline = self._make_build_pipeline(monitor)

        doc1 = SourceDocument(nodes=[TextNode(text="a", id_="a1"), TextNode(text="b", id_="a2")])
        doc2 = SourceDocument(nodes=[TextNode(text="c", id_="c1")])

        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.run_pipeline', return_value=[]):
            list(pipeline.build([doc1, doc2]))

        monitor.increment_graph_processed_documents.assert_called_once_with(2)
        monitor.increment_graph_processed_chunks.assert_called_once_with(3)
        monitor.increment_vector_processed_documents.assert_called_once_with(2)
        monitor.increment_vector_processed_chunks.assert_called_once_with(3)

    def test_build_monitor_fires_per_batch(self):
        from unittest.mock import MagicMock, patch
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode

        monitor = MagicMock(spec=NoOpProgressMonitor)
        pipeline = self._make_build_pipeline(monitor)
        pipeline.batch_size = 1  # force one doc per batch

        doc1 = SourceDocument(nodes=[TextNode(text="a", id_="a1"), TextNode(text="b", id_="a2")])
        doc2 = SourceDocument(nodes=[TextNode(text="c", id_="c1")])

        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.run_pipeline', return_value=[]):
            list(pipeline.build([doc1, doc2]))

        assert monitor.increment_graph_processed_documents.call_count == 2
        assert monitor.increment_vector_processed_documents.call_count == 2
        chunk_calls = [c[0][0] for c in monitor.increment_graph_processed_chunks.call_args_list]
        assert chunk_calls == [2, 1]

    def test_build_monitor_exception_does_not_crash(self, caplog):
        from unittest.mock import MagicMock, patch
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode

        monitor = MagicMock(spec=NoOpProgressMonitor)
        monitor.increment_graph_processed_documents.side_effect = RuntimeError("monitor error")
        pipeline = self._make_build_pipeline(monitor)

        doc = SourceDocument(nodes=[TextNode(text="a", id_="a1")])

        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.run_pipeline', return_value=[]):
            with caplog.at_level(logging.WARNING):
                list(pipeline.build([doc]))

        assert "ProgressMonitor raised an exception" in caplog.text

    def test_build_monitor_not_called_when_none(self):
        from unittest.mock import MagicMock, patch
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode

        pipeline = self._make_build_pipeline(monitor=None)
        doc = SourceDocument(nodes=[TextNode(text="a", id_="a1")])

        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.run_pipeline', return_value=[]):
            list(pipeline.build([doc]))  # should not raise


class TestExtractAndBuildMonitor:

    def test_same_monitor_receives_llm_and_build_increments(self):
        from unittest.mock import MagicMock, patch
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from graphrag_toolkit.lexical_graph.indexing.progress_monitor import ProgressMonitor

        call_log = []

        class TrackingMonitor(NoOpProgressMonitor):
            def increment_llm_processed_documents(self, count=1):
                call_log.append(('llm_docs', count))

            def increment_llm_processed_chunks(self, count=1):
                call_log.append(('llm_chunks', count))

            def increment_graph_processed_documents(self, count=1):
                call_log.append(('graph_docs', count))

            def increment_graph_processed_chunks(self, count=1):
                call_log.append(('graph_chunks', count))

            def increment_vector_processed_documents(self, count=1):
                call_log.append(('vector_docs', count))

            def increment_vector_processed_chunks(self, count=1):
                call_log.append(('vector_chunks', count))

        monitor = TrackingMonitor()

        # Simulate the extraction monitor pipe firing (LLM stage)
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex
        from llama_index.core.schema import TextNode

        doc = SourceDocument(nodes=[TextNode(text="a", id_="a1"), TextNode(text="b", id_="a2")])

        extraction_monitor = LexicalGraphIndex._create_extraction_monitor_pipe(monitor)
        list([doc] | extraction_monitor)

        # Simulate the build monitor firing (build stage)
        monitor.increment_graph_processed_documents(1)
        monitor.increment_graph_processed_chunks(2)
        monitor.increment_vector_processed_documents(1)
        monitor.increment_vector_processed_chunks(2)

        assert ('llm_docs', 1) in call_log
        assert ('llm_chunks', 2) in call_log
        assert ('graph_docs', 1) in call_log
        assert ('graph_chunks', 2) in call_log
        assert ('vector_docs', 1) in call_log
        assert ('vector_chunks', 2) in call_log

        # LLM events should appear before graph/vector events
        llm_idx = next(i for i, e in enumerate(call_log) if e[0] == 'llm_docs')
        graph_idx = next(i for i, e in enumerate(call_log) if e[0] == 'graph_docs')
        assert llm_idx < graph_idx


class TestMonitorExceptionHandling:

    def test_extraction_monitor_exception_does_not_crash(self, caplog):
        from unittest.mock import MagicMock
        from graphrag_toolkit.lexical_graph.lexical_graph_index import LexicalGraphIndex
        from graphrag_toolkit.lexical_graph.indexing.model import SourceDocument
        from llama_index.core.schema import TextNode

        monitor = MagicMock(spec=NoOpProgressMonitor)
        monitor.increment_llm_processed_documents.side_effect = RuntimeError("monitor error")

        doc = SourceDocument(nodes=[TextNode(text="chunk", id_="c1")])

        extraction_monitor = LexicalGraphIndex._create_extraction_monitor_pipe(monitor)

        with caplog.at_level(logging.WARNING):
            result = list([doc] | extraction_monitor)

        assert len(result) == 1
        assert result[0] is doc
        assert "ProgressMonitor raised an exception" in caplog.text
