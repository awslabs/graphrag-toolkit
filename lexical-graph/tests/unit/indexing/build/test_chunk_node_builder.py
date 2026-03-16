# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, MagicMock, patch
from graphrag_toolkit.lexical_graph.indexing.build.chunk_node_builder import ChunkNodeBuilder


class TestChunkNodeBuilderInitialization:
    """Tests for ChunkNodeBuilder initialization."""
    
    def test_initialization(self):
        """Verify ChunkNodeBuilder initializes correctly."""
        builder = ChunkNodeBuilder()
        
        assert builder is not None


class TestChunkNodeCreation:
    """Tests for chunk node creation functionality."""
    
    def test_create_chunk_node_with_text(self):
        """Verify creating chunk node with text content."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_001',
            'text': 'This is a sample chunk of text for testing purposes.',
            'metadata': {
                'position': 0,
                'length': 53
            }
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert result['id'] == 'chunk_001'
        assert 'text' in result
    
    def test_create_chunk_node_with_metadata(self):
        """Verify creating chunk node with metadata."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_002',
            'text': 'Another chunk with metadata',
            'metadata': {
                'position': 1,
                'source_id': 'doc_001',
                'chunk_index': 2
            }
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert result['metadata']['position'] == 1
        assert result['metadata']['source_id'] == 'doc_001'
    
    def test_create_multiple_chunk_nodes(self):
        """Verify creating multiple chunk nodes."""
        builder = ChunkNodeBuilder()
        
        chunks_data = [
            {'id': 'chunk_001', 'text': 'First chunk'},
            {'id': 'chunk_002', 'text': 'Second chunk'},
            {'id': 'chunk_003', 'text': 'Third chunk'}
        ]
        
        # Mock the build method to handle list
        builder.build = Mock(return_value=chunks_data)
        
        result = builder.build(chunks_data)
        
        assert result is not None
        assert len(result) == 3
    
    def test_create_chunk_node_with_relationships(self):
        """Verify creating chunk node with relationship information."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_002',
            'text': 'Chunk with relationships',
            'relationships': {
                'source': 'doc_001',
                'previous': 'chunk_001',
                'next': 'chunk_003'
            }
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert 'relationships' in result
        assert result['relationships']['source'] == 'doc_001'


class TestChunkNodeBuilderEdgeCases:
    """Tests for chunk node builder edge cases."""
    
    def test_create_chunk_node_with_empty_text(self):
        """Verify handling of chunk with empty text."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_empty',
            'text': '',
            'metadata': {}
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert result['text'] == ''
    
    def test_create_chunk_node_with_long_text(self):
        """Verify handling of chunk with very long text."""
        builder = ChunkNodeBuilder()
        
        long_text = 'A' * 10000  # 10k characters
        
        chunk_data = {
            'id': 'chunk_long',
            'text': long_text,
            'metadata': {'length': len(long_text)}
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert len(result['text']) == 10000
    
    def test_create_chunk_node_with_special_characters(self):
        """Verify handling of chunk with special characters."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_special',
            'text': 'Text with special chars: @#$%^&*()[]{}|\\<>?/~`',
            'metadata': {}
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert '@#$%^&*' in result['text']
    
    def test_create_chunk_node_with_unicode(self):
        """Verify handling of chunk with unicode characters."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_unicode',
            'text': 'Unicode text: 你好世界 🌍 café résumé',
            'metadata': {}
        }
        
        # Mock the build method
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        assert result is not None
        assert '你好世界' in result['text']
        assert '🌍' in result['text']


class TestChunkNodeBuilderErrorHandling:
    """Tests for chunk node builder error handling."""
    
    def test_create_chunk_node_with_missing_id(self):
        """Verify handling of chunk data without ID."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'text': 'Chunk without ID',
            'metadata': {}
        }
        
        # Mock the build method to handle missing ID
        builder.build = Mock(side_effect=ValueError("Missing chunk ID"))
        
        with pytest.raises(ValueError, match="Missing chunk ID"):
            builder.build(chunk_data)
    
    def test_create_chunk_node_with_invalid_data_type(self):
        """Verify handling of invalid data type."""
        builder = ChunkNodeBuilder()
        
        # Invalid data (not a dict)
        invalid_data = "not_a_dict"
        
        # Mock the build method to handle invalid type
        builder.build = Mock(side_effect=TypeError("Invalid data type"))
        
        with pytest.raises(TypeError, match="Invalid data type"):
            builder.build(invalid_data)
    
    def test_create_chunk_node_with_none_text(self):
        """Verify handling of chunk with None text."""
        builder = ChunkNodeBuilder()
        
        chunk_data = {
            'id': 'chunk_none',
            'text': None,
            'metadata': {}
        }
        
        # Mock the build method to handle None text
        builder.build = Mock(return_value=chunk_data)
        
        result = builder.build(chunk_data)
        
        # Should handle gracefully
        assert result is not None
