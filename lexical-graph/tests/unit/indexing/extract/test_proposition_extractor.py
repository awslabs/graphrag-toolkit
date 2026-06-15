# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch, AsyncMock
from graphrag_toolkit.core.types import Node
from graphrag_toolkit.lexical_graph.indexing.extract.proposition_extractor import PropositionExtractor


class TestPropositionExtractorInitialization:
    """Tests for PropositionExtractor initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert PropositionExtractor.class_name() == "PropositionExtractor"


class TestPropositionExtractorProperties:
    """Tests for PropositionExtractor properties."""
    
class TestPropositionExtractorAsync:
    """Tests for PropositionExtractor async methods."""
    
    @pytest.mark.asyncio
    async def test_extract_returns_list(self):
        """Verify extract returns a list."""
        extractor = PropositionExtractor(source_metadata_field=None)
        
        # Mock the internal extraction method
        extractor._extract_propositions_for_nodes = AsyncMock(return_value=[])
        
        nodes = [Node(text="test")]
        result = await extractor.extract(nodes)
        
        assert isinstance(result, list)
    
    @pytest.mark.asyncio
    async def test_extract_with_empty_nodes(self):
        """Verify extract handles empty node list."""
        extractor = PropositionExtractor(source_metadata_field=None)
        extractor._extract_propositions_for_nodes = AsyncMock(return_value=[])
        
        result = await extractor.extract([])
        
        assert isinstance(result, list)
        assert len(result) == 0


class TestPropositionExtractorMocked:
    """Tests for PropositionExtractor with mocked dependencies."""
    
    @pytest.mark.asyncio
    async def test_extract_propositions_for_node(self):
        """Verify _extract_propositions_for_node processes a single node."""
        extractor = PropositionExtractor(source_metadata_field=None)
        
        # Mock the proposition extraction
        extractor._extract_propositions = AsyncMock(return_value=Mock(model_dump=Mock(return_value={'propositions': ["prop1", "prop2"]})))
        
        node = Node(text="Test content", node_id="node1")
        result = await extractor._extract_propositions_for_node(node)
        
        assert result is not None
        assert 'aws::graph::propositions' in result
    
    @pytest.mark.asyncio
    async def test_extract_propositions_for_nodes_multiple(self):
        """Verify _extract_propositions_for_nodes processes multiple nodes."""
        extractor = PropositionExtractor(source_metadata_field=None)
        
        # Mock the single node extraction
        extractor._extract_propositions_for_node = AsyncMock(
            side_effect=lambda n: {'aws::graph::propositions': []}
        )
        
        nodes = [
            Node(text="Node 1", node_id="id1"),
            Node(text="Node 2", node_id="id2")
        ]
        
        result = await extractor._extract_propositions_for_nodes(nodes)
        
        assert isinstance(result, list)
        assert len(result) == 2
