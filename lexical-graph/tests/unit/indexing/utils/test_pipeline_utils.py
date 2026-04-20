# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, MagicMock
from llama_index.core.schema import TextNode, Document
from llama_index.core.ingestion import IngestionPipeline

from graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils import (
    sink,
    run_pipeline,
    node_batcher
)


class TestSink:
    """Tests for sink utility."""
    
    def test_sink_consumes_generator(self):
        """Verify sink consumes all items from generator."""
        items = [1, 2, 3, 4, 5]
        
        def generator():
            for item in items:
                yield item
        
        # Sink should consume all items without returning anything
        result = generator() | sink
        
        # Result should be None (sink returns nothing)
        assert result is None
    
    def test_sink_with_empty_generator(self):
        """Verify sink handles empty generator."""
        def empty_generator():
            return
            yield  # Never reached
        
        result = empty_generator() | sink
        assert result is None


class TestRunPipeline:
    """Tests for run_pipeline function."""
    
    def test_run_pipeline_processes_batches(self):
        """Verify run_pipeline processes node batches correctly."""
        # Create mock pipeline
        mock_pipeline = Mock(spec=IngestionPipeline)
        mock_pipeline.transformations = []
        mock_pipeline.cache = None
        mock_pipeline.disable_cache = True
        
        # Create test nodes
        batch1 = [TextNode(text="Node 1", id_="1"), TextNode(text="Node 2", id_="2")]
        batch2 = [TextNode(text="Node 3", id_="3"), TextNode(text="Node 4", id_="4")]
        node_batches = [batch1, batch2]
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.run_transformations') as mock_transform:
            # Mock transformation to return the same nodes
            mock_transform.side_effect = lambda transformations, **kwargs: kwargs.get('nodes', [])
            
            with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.ProcessPoolExecutor') as mock_executor:
                # Mock executor to process batches
                mock_pool = MagicMock()
                mock_pool.__enter__.return_value = mock_pool
                mock_pool.map.return_value = [batch1, batch2]
                mock_executor.return_value = mock_pool
                
                results = list(run_pipeline(mock_pipeline, node_batches, num_workers=2))
                
                assert len(results) == 4
                assert results[0].text == "Node 1"
                assert results[3].text == "Node 4"
    
    def test_run_pipeline_with_single_worker(self):
        """Verify run_pipeline works with single worker."""
        mock_pipeline = Mock(spec=IngestionPipeline)
        mock_pipeline.transformations = []
        mock_pipeline.cache = None
        mock_pipeline.disable_cache = True
        
        batch = [TextNode(text="Node 1", id_="1")]
        node_batches = [batch]
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.run_transformations') as mock_transform:
            mock_transform.return_value = batch
            
            with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.ProcessPoolExecutor') as mock_executor:
                mock_pool = MagicMock()
                mock_pool.__enter__.return_value = mock_pool
                mock_pool.map.return_value = [batch]
                mock_executor.return_value = mock_pool
                
                results = list(run_pipeline(mock_pipeline, node_batches, num_workers=1))
                
                assert len(results) == 1
                mock_executor.assert_called_once_with(max_workers=1)
    
    def test_run_pipeline_with_cache(self):
        """Verify run_pipeline uses cache when not disabled."""
        mock_cache = Mock()
        mock_pipeline = Mock(spec=IngestionPipeline)
        mock_pipeline.transformations = []
        mock_pipeline.cache = mock_cache
        mock_pipeline.disable_cache = False
        
        batch = [TextNode(text="Node 1", id_="1")]
        node_batches = [batch]
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.run_transformations') as mock_transform:
            mock_transform.return_value = batch
            
            with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.ProcessPoolExecutor') as mock_executor:
                mock_pool = MagicMock()
                mock_pool.__enter__.return_value = mock_pool
                mock_pool.map.return_value = [batch]
                mock_executor.return_value = mock_pool
                
                results = list(run_pipeline(mock_pipeline, node_batches, cache_collection="test_cache"))
                
                assert len(results) == 1
    
    def test_run_pipeline_empty_batches(self):
        """Verify run_pipeline handles empty batches."""
        mock_pipeline = Mock(spec=IngestionPipeline)
        mock_pipeline.transformations = []
        mock_pipeline.cache = None
        mock_pipeline.disable_cache = True
        
        node_batches = []
        
        with patch('graphrag_toolkit.lexical_graph.indexing.utils.pipeline_utils.ProcessPoolExecutor') as mock_executor:
            mock_pool = MagicMock()
            mock_pool.__enter__.return_value = mock_pool
            mock_pool.map.return_value = []
            mock_executor.return_value = mock_pool
            
            results = list(run_pipeline(mock_pipeline, node_batches))
            
            assert len(results) == 0


class TestNodeBatcher:
    """Tests for node_batcher function."""
    
    def test_node_batcher_divides_evenly(self):
        """Verify node_batcher divides nodes evenly into batches."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(10)]
        num_batches = 2
        
        batches = list(node_batcher(num_batches, nodes))
        
        assert len(batches) == 2
        assert len(batches[0]) == 5
        assert len(batches[1]) == 5
    
    def test_node_batcher_handles_uneven_division(self):
        """Verify node_batcher handles uneven division."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(10)]
        num_batches = 3
        
        batches = list(node_batcher(num_batches, nodes))
        
        # 10 nodes / 3 batches = batch_size 4 (rounded up)
        # Should create batches of size 4, 4, 2
        assert len(batches) == 3
        assert len(batches[0]) == 4
        assert len(batches[1]) == 4
        assert len(batches[2]) == 2
    
    def test_node_batcher_single_batch(self):
        """Verify node_batcher with single batch returns all nodes."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(5)]
        num_batches = 1
        
        batches = list(node_batcher(num_batches, nodes))
        
        assert len(batches) == 1
        assert len(batches[0]) == 5
    
    def test_node_batcher_more_batches_than_nodes(self):
        """Verify node_batcher when num_batches > num_nodes."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(3)]
        num_batches = 5
        
        batches = list(node_batcher(num_batches, nodes))
        
        # batch_size = max(1, 3/5) = 1
        # Should create 3 batches of size 1
        assert len(batches) == 3
        for batch in batches:
            assert len(batch) == 1
    
    def test_node_batcher_with_documents(self):
        """Verify node_batcher works with Document objects."""
        docs = [Document(text=f"Doc {i}") for i in range(8)]
        num_batches = 2
        
        batches = list(node_batcher(num_batches, docs))
        
        assert len(batches) == 2
        assert len(batches[0]) == 4
        assert len(batches[1]) == 4
        assert all(isinstance(doc, Document) for batch in batches for doc in batch)
    
    def test_node_batcher_empty_nodes(self):
        """Verify node_batcher handles empty node list."""
        nodes = []
        num_batches = 2
        
        batches = list(node_batcher(num_batches, nodes))
        
        assert len(batches) == 0
    
    def test_node_batcher_preserves_order(self):
        """Verify node_batcher preserves node order."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(6)]
        num_batches = 2
        
        batches = list(node_batcher(num_batches, nodes))
        
        # Flatten batches and check order
        flattened = [node for batch in batches for node in batch]
        assert [node.id_ for node in flattened] == ['0', '1', '2', '3', '4', '5']
    
    def test_node_batcher_large_number_of_nodes(self):
        """Verify node_batcher handles large number of nodes."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(1000)]
        num_batches = 10
        
        batches = list(node_batcher(num_batches, nodes))
        
        # Should create 10 batches
        assert len(batches) == 10
        # Total nodes should be preserved
        total_nodes = sum(len(batch) for batch in batches)
        assert total_nodes == 1000
    
    def test_node_batcher_batch_size_calculation(self):
        """Verify node_batcher calculates batch size correctly."""
        nodes = [TextNode(text=f"Node {i}", id_=str(i)) for i in range(15)]
        num_batches = 4
        
        batches = list(node_batcher(num_batches, nodes))
        
        # 15 nodes / 4 batches = 3.75, rounds to 4
        # But 4 * 4 = 16 > 15, so batch_size becomes 4
        # Creates batches: 4, 4, 4, 3
        assert len(batches) == 4
        assert len(batches[0]) == 4
        assert len(batches[1]) == 4
        assert len(batches[2]) == 4
        assert len(batches[3]) == 3
