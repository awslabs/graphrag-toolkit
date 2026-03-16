# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock
from graphrag_toolkit.lexical_graph.indexing.build.source_node_builder import SourceNodeBuilder


class TestSourceNodeBuilderInitialization:
    """Tests for SourceNodeBuilder initialization."""
    
    def test_initialization(self):
        """Verify SourceNodeBuilder initializes correctly."""
        builder = SourceNodeBuilder()
        assert builder is not None


class TestSourceNodeCreation:
    """Tests for source node creation functionality."""
    
    def test_create_source_node_with_metadata(self):
        """Verify creating source node with metadata."""
        builder = SourceNodeBuilder()
        
        source_data = {
            'id': 'source_001',
            'title': 'Sample Document',
            'content': 'Document content here',
            'metadata': {
                'author': 'Test Author',
                'date': '2024-01-01',
                'source_type': 'pdf'
            }
        }
        
        builder.build = Mock(return_value=source_data)
        result = builder.build(source_data)
        
        assert result is not None
        assert result['id'] == 'source_001'
        assert result['metadata']['author'] == 'Test Author'
    
    def test_create_source_node_with_file_path(self):
        """Verify creating source node with file path."""
        builder = SourceNodeBuilder()
        
        source_data = {
            'id': 'source_002',
            'file_path': '/path/to/document.pdf',
            'metadata': {'size': 1024}
        }
        
        builder.build = Mock(return_value=source_data)
        result = builder.build(source_data)
        
        assert result is not None
        assert 'file_path' in result
    
    def test_create_multiple_source_nodes(self):
        """Verify creating multiple source nodes."""
        builder = SourceNodeBuilder()
        
        sources = [
            {'id': 's1', 'title': 'Doc 1'},
            {'id': 's2', 'title': 'Doc 2'},
            {'id': 's3', 'title': 'Doc 3'}
        ]
        
        builder.build = Mock(return_value=sources)
        result = builder.build(sources)
        
        assert result is not None
        assert len(result) == 3


class TestSourceNodeBuilderErrorHandling:
    """Tests for source node builder error handling."""
    
    def test_create_source_node_with_missing_id(self):
        """Verify handling of source without ID."""
        builder = SourceNodeBuilder()
        
        source_data = {'title': 'Document without ID'}
        
        builder.build = Mock(side_effect=ValueError("Missing source ID"))
        
        with pytest.raises(ValueError, match="Missing source ID"):
            builder.build(source_data)
    
    def test_create_source_node_with_invalid_metadata(self):
        """Verify handling of invalid metadata."""
        builder = SourceNodeBuilder()
        
        source_data = {
            'id': 'source_001',
            'metadata': "invalid_metadata_type"
        }
        
        builder.build = Mock(return_value=source_data)
        result = builder.build(source_data)
        
        assert result is not None
