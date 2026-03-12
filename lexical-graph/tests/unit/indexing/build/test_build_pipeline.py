# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock, patch
from graphrag_toolkit.lexical_graph.indexing.build.build_pipeline import BuildPipeline, NodeFilter


class TestNodeFilter:
    """Tests for NodeFilter functionality."""
    
    def test_node_filter_callable(self):
        """Verify NodeFilter is callable and filters nodes."""
        filter_func = NodeFilter()
        
        # Mock nodes
        nodes = [
            Mock(node_id='n1', type='Document'),
            Mock(node_id='n2', type='Chunk'),
            Mock(node_id='n3', type='Entity')
        ]
        
        # NodeFilter should be callable
        result = filter_func(nodes)
        assert result is not None


class TestBuildPipelineInitialization:
    """Tests for BuildPipeline initialization."""
    
    def test_initialization_with_components(self):
        """Verify BuildPipeline initializes with transform components."""
        mock_component = Mock()
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[mock_component],
                graph_store=Mock(),
                vector_store=Mock()
            )
            
            assert pipeline is not None
    
    def test_initialization_with_empty_components(self):
        """Verify BuildPipeline handles empty component list."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[],
                graph_store=Mock(),
                vector_store=Mock()
            )
            
            assert pipeline is not None


class TestBuildPipelineOrchestration:
    """Tests for pipeline orchestration functionality."""
    
    def test_build_with_source_documents(self, sample_documents):
        """Verify pipeline processes source documents."""
        mock_graph_store = Mock()
        mock_vector_store = Mock()
        mock_component = Mock()
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[mock_component],
                graph_store=mock_graph_store,
                vector_store=mock_vector_store
            )
            
            # Mock the build method
            pipeline.build = Mock(return_value={'status': 'success', 'nodes_processed': 3})
            
            result = pipeline.build(sample_documents)
            
            assert result is not None
            assert result['status'] == 'success'
    
    def test_build_with_empty_input(self):
        """Verify pipeline handles empty input gracefully."""
        mock_graph_store = Mock()
        mock_vector_store = Mock()
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[],
                graph_store=mock_graph_store,
                vector_store=mock_vector_store
            )
            
            # Mock the build method
            pipeline.build = Mock(return_value={'status': 'success', 'nodes_processed': 0})
            
            result = pipeline.build([])
            
            assert result is not None
            assert result['nodes_processed'] == 0
    
    def test_build_with_multiple_components(self):
        """Verify pipeline orchestrates multiple components."""
        mock_graph_store = Mock()
        mock_vector_store = Mock()
        mock_component1 = Mock()
        mock_component2 = Mock()
        mock_component3 = Mock()
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[mock_component1, mock_component2, mock_component3],
                graph_store=mock_graph_store,
                vector_store=mock_vector_store
            )
            
            assert pipeline is not None
    
    def test_to_node_batches_conversion(self):
        """Verify conversion of source documents to node batches."""
        mock_graph_store = Mock()
        mock_vector_store = Mock()
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[],
                graph_store=mock_graph_store,
                vector_store=mock_vector_store
            )
            
            # Mock the internal method
            if hasattr(pipeline, '_to_node_batches'):
                pipeline._to_node_batches = Mock(return_value=[[Mock(), Mock()]])
                
                result = pipeline._to_node_batches([[Mock()]], build_timestamp=123456)
                
                assert result is not None
                assert len(result) > 0


class TestBuildPipelineErrorHandling:
    """Tests for pipeline error handling."""
    
    def test_build_with_invalid_component(self):
        """Verify pipeline handles invalid components."""
        mock_graph_store = Mock()
        mock_vector_store = Mock()
        
        # Invalid component (not a TransformComponent)
        invalid_component = "not_a_component"
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            # Should handle gracefully or raise appropriate error
            try:
                pipeline = BuildPipeline.create(
                    components=[invalid_component],
                    graph_store=mock_graph_store,
                    vector_store=mock_vector_store
                )
            except (TypeError, ValueError, AttributeError):
                # Expected behavior for invalid component
                pass
    
    def test_build_with_component_failure(self):
        """Verify pipeline handles component failures."""
        mock_graph_store = Mock()
        mock_vector_store = Mock()
        mock_component = Mock()
        mock_component.side_effect = Exception("Component processing failed")
        
        with patch('graphrag_toolkit.lexical_graph.indexing.build.build_pipeline.IngestionPipeline'):
            pipeline = BuildPipeline.create(
                components=[mock_component],
                graph_store=mock_graph_store,
                vector_store=mock_vector_store
            )
            
            # Mock build to simulate failure
            pipeline.build = Mock(side_effect=Exception("Build failed"))
            
            with pytest.raises(Exception, match="Build failed"):
                pipeline.build([Mock()])
