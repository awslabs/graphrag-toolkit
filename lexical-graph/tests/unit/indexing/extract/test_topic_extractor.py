# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, AsyncMock
from llama_index.core.schema import TextNode
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
    async def test_aextract_returns_list(self, mock_config_class):
        """Verify aextract returns a list."""
        from llama_index.core.llms import MockLLM
        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = MockLLM()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        
        # Mock the internal extraction method
        extractor._extract_for_nodes = AsyncMock(return_value=[])
        
        nodes = [TextNode(text="test")]
        result = await extractor.aextract(nodes)
        
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_aextract_with_empty_nodes(self, mock_config_class):
        """Verify aextract handles empty node list."""
        from llama_index.core.llms import MockLLM
        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = MockLLM()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        extractor._extract_for_nodes = AsyncMock(return_value=[])
        
        result = await extractor.aextract([])
        
        assert isinstance(result, list)
        assert len(result) == 0


class TestTopicExtractorMocked:
    """Tests for TopicExtractor with mocked dependencies."""
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_extract_topics_for_node(self, mock_config_class):
        """Verify _extract_topics_for_node processes a single node."""
        from llama_index.core.llms import MockLLM
        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = MockLLM()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        
        # Mock the topic extraction
        extractor._extract_topics = AsyncMock(return_value=(Mock(model_dump=Mock(return_value={"topics": []})), []))
        
        node = TextNode(text="Test content", id_="node1")
        result = await extractor._extract_for_node(node)
        
        assert result is not None
        assert 'aws::graph::topics' in result
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.topic_extractor.GraphRAGConfig')
    async def test_extract_topics_for_nodes_multiple(self, mock_config_class):
        """Verify _extract_topics_for_nodes processes multiple nodes."""
        from llama_index.core.llms import MockLLM
        
        # Configure the mock class attributes
        mock_config_class.extraction_llm = MockLLM()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = TopicExtractor()
        
        # Mock the single node extraction
        extractor._extract_for_node = AsyncMock(
            side_effect=lambda n: {'aws::graph::topics': {}}
        )
        
        nodes = [
            TextNode(text="Node 1", id_="id1"),
            TextNode(text="Node 2", id_="id2")
        ]
        
        result = await extractor._extract_for_nodes(nodes)
        
        assert isinstance(result, list)
        assert len(result) == 2
