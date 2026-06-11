# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, AsyncMock
from graphrag_toolkit.core.types import Node
from graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor import TopicExtractor


class TestTopicExtractorInitialization:
    """Tests for TopicExtractor initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert TopicExtractor.class_name() == "TopicExtractor"


class TestTopicExtractorAsync:
    """Tests for TopicExtractor async methods."""
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_extract_returns_list(self, mock_config_class):
        """Verify extract returns a list."""

        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = Mock()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        
        # Mock the internal extraction method
        extractor._extract_for_nodes = AsyncMock(return_value=[])
        
        nodes = [Node(text="test")]
        result = await extractor.extract(nodes)
        
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_extract_with_empty_nodes(self, mock_config_class):
        """Verify extract handles empty node list."""

        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = Mock()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        extractor._extract_for_nodes = AsyncMock(return_value=[])
        
        result = await extractor.extract([])
        
        assert isinstance(result, list)
        assert len(result) == 0


class TestTopicExtractorMocked:
    """Tests for TopicExtractor with mocked dependencies."""
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_extract_topics_for_node(self, mock_config_class):
        """Verify _extract_topics_for_node processes a single node."""

        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = Mock()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        
        # Mock the topic extraction
        extractor._extract_topics = AsyncMock(return_value=(Mock(model_dump=Mock(return_value={"topics": []})), []))
        
        node = Node(text="Test content", node_id="node1")
        result = await extractor._extract_for_node(node)
        
        assert result is not None
        assert 'aws::graph::topics' in result
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_extract_topics_for_nodes_multiple(self, mock_config_class):
        """Verify _extract_topics_for_nodes processes multiple nodes."""

        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = Mock()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        
        # Mock the single node extraction
        extractor._extract_for_node = AsyncMock(
            side_effect=lambda n: {'aws::graph::topics': {}}
        )
        
        nodes = [
            Node(text="Node 1", node_id="id1"),
            Node(text="Node 2", node_id="id2")
        ]
        
        result = await extractor._extract_for_nodes(nodes)
        
        assert isinstance(result, list)
        assert len(result) == 2
