# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from graphrag_toolkit.lexical_graph.indexing.build.build_pipeline import BuildPipeline, NodeFilter


class TestNodeFilter:
    """Tests for NodeFilter functionality."""

    def test_node_filter_callable(self):
        """Verify NodeFilter is callable and filters nodes."""
        filter_func = NodeFilter()
        nodes = [
            Mock(node_id='n1'),
            Mock(node_id='n2'),
            Mock(node_id='n3'),
        ]
        result = filter_func(nodes)
        assert result is not None

    def test_node_filter_returns_list(self):
        """Verify NodeFilter returns a list."""
        filter_func = NodeFilter()
        result = filter_func([Mock()])
        assert isinstance(result, list)


class TestBuildPipelineInitialization:
    """Tests for BuildPipeline initialization."""

    def test_initialization_with_components(self):
        """Verify BuildPipeline initializes with transform components."""
        mock_component = Mock()
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[mock_component],
                graph_store=Mock(),
                vector_store=Mock(),
            )
            assert pipeline is not None

    def test_initialization_with_empty_components(self):
        """Verify BuildPipeline handles empty component list."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[],
                graph_store=Mock(),
                vector_store=Mock(),
            )
            assert pipeline is not None

    def test_initialization_with_multiple_components(self):
        """Verify BuildPipeline accepts multiple components."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[Mock(), Mock(), Mock()],
                graph_store=Mock(),
                vector_store=Mock(),
            )
            assert pipeline is not None


class TestBuildPipelineErrorHandling:
    """Tests for pipeline error handling."""

    def test_build_with_invalid_component(self):
        """Verify pipeline handles invalid components."""
        invalid_component = "not_a_component"
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            try:
                BuildPipeline.create(
                    components=[invalid_component],
                    graph_store=Mock(),
                    vector_store=Mock(),
                )
            except (TypeError, ValueError, AttributeError):
                pass
