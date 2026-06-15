# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, AsyncMock
from graphrag_toolkit.core.types import Node
from graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor import LLMPropositionExtractor


class TestLLMPropositionExtractorInitialization:
    """Tests for LLMPropositionExtractor initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert LLMPropositionExtractor.class_name() == "LLMPropositionExtractor"
    
class TestLLMPropositionExtractorMocked:
    """Tests for LLMPropositionExtractor with mocked LLM."""
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor.GraphRAGConfig')
    async def test_extract_with_mock_llm(self, mock_config_class):
        """Verify extract works with mocked LLM."""
        # Configure the mock class attributes
        mock_config_class.extraction_llm = Mock()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = LLMPropositionExtractor()
        
        # Mock the extraction to return empty results - use AsyncMock for async methods
        extractor._extract_propositions_for_nodes = AsyncMock(return_value=[])
        
        nodes = [Node(text="test", node_id="node1")]
        result = await extractor.extract(nodes)
        
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    @patch('graphrag_toolkit.lexical_graph.indexing.extract.llm_proposition_extractor.GraphRAGConfig')
    async def test_extract_empty_nodes(self, mock_config_class):
        """Verify extract handles empty node list."""
        # Configure the mock class attributes
        mock_config_class.extraction_llm = Mock()
        mock_config_class.enable_cache = False
        mock_config_class.extraction_num_threads_per_worker = 1
        
        extractor = LLMPropositionExtractor()
        extractor._extract_propositions_for_nodes = AsyncMock(return_value=[])
        
        result = await extractor.extract([])
        
        assert isinstance(result, list)
        assert len(result) == 0
