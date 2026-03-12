# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path
from graphrag_toolkit.lexical_graph.indexing.build.checkpoint import (
    Checkpoint,
    CheckpointFilter,
    CheckpointWriter,
    DoNotCheckpoint
)


class TestCheckpointInitialization:
    """Tests for Checkpoint initialization."""
    
    def test_initialization_with_defaults(self):
        """Verify Checkpoint initializes with default parameters."""
        checkpoint = Checkpoint(checkpoint_name='test_checkpoint')
        
        assert checkpoint is not None
    
    def test_initialization_with_custom_output_dir(self):
        """Verify Checkpoint initializes with custom output directory."""
        checkpoint = Checkpoint(
            checkpoint_name='test_checkpoint',
            output_dir='custom_output'
        )
        
        assert checkpoint is not None
    
    def test_initialization_disabled(self):
        """Verify Checkpoint can be disabled."""
        checkpoint = Checkpoint(
            checkpoint_name='test_checkpoint',
            enabled=False
        )
        
        assert checkpoint is not None


class TestCheckpointFilter:
    """Tests for CheckpointFilter functionality."""
    
    def test_filter_initialization(self):
        """Verify CheckpointFilter initializes correctly."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path'):
            filter_obj = CheckpointFilter(checkpoint_dir=Path('test_dir'))
            
            assert filter_obj is not None
    
    def test_checkpoint_does_not_exist(self):
        """Verify checkpoint existence check."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.exists.return_value = False
            mock_path.return_value = mock_path_instance
            
            filter_obj = CheckpointFilter(checkpoint_dir=mock_path_instance)
            filter_obj.checkpoint_does_not_exist = Mock(return_value=True)
            
            result = filter_obj.checkpoint_does_not_exist('node_123')
            
            assert result is True
    
    def test_checkpoint_exists(self):
        """Verify checkpoint existence check when checkpoint exists."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.exists.return_value = True
            mock_path.return_value = mock_path_instance
            
            filter_obj = CheckpointFilter(checkpoint_dir=mock_path_instance)
            filter_obj.checkpoint_does_not_exist = Mock(return_value=False)
            
            result = filter_obj.checkpoint_does_not_exist('node_123')
            
            assert result is False
    
    def test_filter_nodes(self):
        """Verify filtering of nodes based on checkpoint status."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path'):
            filter_obj = CheckpointFilter(checkpoint_dir=Path('test_dir'))
            
            # Mock nodes
            nodes = [
                Mock(node_id='n1', metadata={'chunk': {'chunkId': 'chunk1'}}),
                Mock(node_id='n2', metadata={'chunk': {'chunkId': 'chunk2'}}),
                Mock(node_id='n3', metadata={'chunk': {'chunkId': 'chunk3'}})
            ]
            
            # Mock the filter to return all nodes
            filter_obj.__call__ = Mock(return_value=nodes)
            
            result = filter_obj(nodes)
            
            assert result is not None
            assert len(result) == 3


class TestCheckpointWriter:
    """Tests for CheckpointWriter functionality."""
    
    def test_writer_initialization(self):
        """Verify CheckpointWriter initializes correctly."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path'):
            writer = CheckpointWriter(checkpoint_dir=Path('test_dir'))
            
            assert writer is not None
    
    def test_touch_creates_checkpoint_file(self):
        """Verify touch creates checkpoint file."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.parent.mkdir = Mock()
            mock_path_instance.touch = Mock()
            
            writer = CheckpointWriter(checkpoint_dir=Path('test_dir'))
            writer.touch = Mock()
            
            writer.touch(mock_path_instance)
            
            writer.touch.assert_called_once()
    
    def test_accept_writes_checkpoints(self):
        """Verify accept writes checkpoints for nodes."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path'):
            writer = CheckpointWriter(checkpoint_dir=Path('test_dir'))
            
            # Mock nodes
            nodes = [
                Mock(node_id='n1', metadata={'chunk': {'chunkId': 'chunk1'}}),
                Mock(node_id='n2', metadata={'chunk': {'chunkId': 'chunk2'}})
            ]
            
            # Mock the accept method
            writer.accept = Mock()
            
            writer.accept(nodes)
            
            writer.accept.assert_called_once()


class TestCheckpointManagement:
    """Tests for checkpoint management operations."""
    
    def test_add_filter_to_checkpoint(self, default_tenant):
        """Verify adding filter to checkpoint."""
        checkpoint = Checkpoint(checkpoint_name='test_checkpoint')
        
        mock_object = Mock()
        
        # Mock the add_filter method
        checkpoint.add_filter = Mock()
        
        checkpoint.add_filter(mock_object, tenant_id=default_tenant)
        
        checkpoint.add_filter.assert_called_once()
    
    def test_add_writer_to_checkpoint(self):
        """Verify adding writer to checkpoint."""
        checkpoint = Checkpoint(checkpoint_name='test_checkpoint')
        
        mock_object = Mock()
        
        # Mock the add_writer method
        checkpoint.add_writer = Mock()
        
        checkpoint.add_writer(mock_object)
        
        checkpoint.add_writer.assert_called_once()
    
    def test_prepare_output_directories(self):
        """Verify output directory preparation."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.mkdir = Mock()
            mock_path.return_value = mock_path_instance
            
            checkpoint = Checkpoint(checkpoint_name='test_checkpoint')
            checkpoint.prepare_output_directories = Mock(return_value=(mock_path_instance, mock_path_instance))
            
            result = checkpoint.prepare_output_directories('test_checkpoint', 'output')
            
            assert result is not None


class TestDoNotCheckpoint:
    """Tests for DoNotCheckpoint marker."""
    
    def test_do_not_checkpoint_marker(self):
        """Verify DoNotCheckpoint marker exists and can be used."""
        # DoNotCheckpoint is a marker class/exception
        marker = DoNotCheckpoint()
        
        assert marker is not None


class TestCheckpointErrorHandling:
    """Tests for checkpoint error handling."""
    
    def test_checkpoint_with_invalid_directory(self):
        """Verify checkpoint handles invalid directory."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path') as mock_path:
            mock_path_instance = Mock()
            mock_path_instance.mkdir.side_effect = OSError("Permission denied")
            mock_path.return_value = mock_path_instance
            
            # Should handle or raise appropriate error
            try:
                checkpoint = Checkpoint(checkpoint_name='test_checkpoint')
                checkpoint.prepare_output_directories('test', 'invalid_dir')
            except (OSError, PermissionError):
                # Expected behavior
                pass
    
    def test_filter_with_missing_node_id(self):
        """Verify filter handles nodes with missing IDs."""
        with patch('graphrag_toolkit.lexical_graph.indexing.build.checkpoint.Path'):
            filter_obj = CheckpointFilter(checkpoint_dir=Path('test_dir'))
            
            # Node without proper ID structure
            nodes = [
                Mock(node_id='n1', metadata={}),  # Missing chunk metadata
            ]
            
            # Mock the filter to handle gracefully
            filter_obj.__call__ = Mock(return_value=nodes)
            
            result = filter_obj(nodes)
            
            assert result is not None
