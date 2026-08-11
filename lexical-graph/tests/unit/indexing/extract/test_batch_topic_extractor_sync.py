# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync import BatchTopicExtractorSync
from graphrag_toolkit.lexical_graph.indexing.constants import TOPICS_KEY


class TestBatchTopicExtractorSyncInitialization:
    """Tests for BatchTopicExtractorSync initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert BatchTopicExtractorSync.class_name() == "BatchTopicExtractorSync"
    
class TestBatchTopicExtractorSyncCall:
    """Tests for __call__ method."""
    
class TestBatchTopicExtractorSyncBatchConfig:
    """Tests for batch configuration."""


class TestBatchTopicExtractorSyncUpdateNode:
    """Tests for _update_node method."""

    def _make_extractor(self):
        """Create a BatchTopicExtractorSync with minimal config for unit testing."""
        with patch('graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync.GraphRAGConfig') as mock_config:
            mock_config.extraction_llm = Mock()
            mock_config.enable_cache = False
            mock_config.local_output_dir = '/tmp'
            batch_config = Mock()
            batch_config.batch_size = 10
            batch_config.batch_inference_config = None
            extractor = BatchTopicExtractorSync.__new__(BatchTopicExtractorSync)
            # Manually set needed attributes without full __init__
            return extractor

    def test_update_node_with_none_topic_data(self):
        """Verify _update_node handles None topic_data (e.g. Nova 2 Lite returning null)."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-1")
        node_metadata_map = {"node-1": None}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[TOPICS_KEY] == {'topics': []}

    def test_update_node_with_dict_topic_data(self):
        """Verify _update_node passes through dict topic_data unchanged."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-1")
        topic_dict = {'topics': [{'topic': 'AI', 'entities': []}]}
        node_metadata_map = {"node-1": topic_dict}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[TOPICS_KEY] == topic_dict

    def test_update_node_with_missing_node_id(self):
        """Verify _update_node defaults to {'topics': []} when node_id not in map."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-missing")
        node_metadata_map = {"node-other": {'topics': [{'topic': 'X', 'entities': []}]}}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[TOPICS_KEY] == {'topics': []}
