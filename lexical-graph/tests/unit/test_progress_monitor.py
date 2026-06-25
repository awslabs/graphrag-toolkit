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


class TestMonitorExceptionHandling:

    def test_extraction_monitor_exception_does_not_crash(self, caplog):
        from unittest.mock import MagicMock
        from pipe import Pipe
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
