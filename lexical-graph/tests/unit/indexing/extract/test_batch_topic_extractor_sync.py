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


class TestBatchTopicExtractorSyncRunNonBatchExtractor:
    """Tests for _run_non_batch_extractor method.

    Regression tests: when a node set falls below Bedrock's minimum batch
    size, BatchTopicExtractorSync falls back to the non-batch
    TopicExtractor. That fallback must reuse the configured LLM (self.llm)
    rather than silently defaulting to GraphRAGConfig.extraction_llm, which
    is a us.* inference profile and is invalid in any other Bedrock region.
    """

    def _make_extractor(self, llm, prompt_template="prompt", source_metadata_field=None,
                         entity_classification_provider=None, topic_provider=None):
        """Create a BatchTopicExtractorSync instance with fields populated
        via model_construct, bypassing validation/__init__ so no real
        BatchConfig or AWS setup is needed for this unit test."""
        return BatchTopicExtractorSync.model_construct(
            llm=llm,
            prompt_template=prompt_template,
            source_metadata_field=source_metadata_field,
            entity_classification_provider=entity_classification_provider or Mock(),
            topic_provider=topic_provider or Mock(),
        )

    @patch("graphrag_toolkit.lexical_graph.indexing.extract.batch_topic_extractor_sync.TopicExtractor")
    def test_run_non_batch_extractor_passes_configured_llm(self, mock_extractor_cls):
        """Verify the non-batch fallback is constructed with the extractor's
        own configured llm, not left to default to GraphRAGConfig.extraction_llm."""
        configured_llm = Mock(name="configured-llm")
        entity_classification_provider = Mock(name="entity-classification-provider")
        topic_provider = Mock(name="topic-provider")
        extractor = self._make_extractor(
            llm=configured_llm,
            entity_classification_provider=entity_classification_provider,
            topic_provider=topic_provider,
        )

        mock_instance = mock_extractor_cls.return_value
        mock_instance.extract.return_value = [{TOPICS_KEY: {'topics': []}}]

        nodes = [TextNode(text="test", id_="node-1")]
        extractor._run_non_batch_extractor(nodes)

        mock_extractor_cls.assert_called_once_with(
            llm=configured_llm,
            prompt_template="prompt",
            source_metadata_field=None,
            entity_classification_provider=entity_classification_provider,
            topic_provider=topic_provider,
        )
