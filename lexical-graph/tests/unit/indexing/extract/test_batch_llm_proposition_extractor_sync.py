# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.indexing.extract.batch_llm_proposition_extractor_sync import BatchLLMPropositionExtractorSync
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY


class TestBatchLLMPropositionExtractorSyncInitialization:
    """Tests for BatchLLMPropositionExtractorSync initialization."""
    
    def test_class_name(self):
        """Verify class_name returns correct name."""
        assert BatchLLMPropositionExtractorSync.class_name() == "BatchLLMPropositionExtractorSync"
    
class TestBatchLLMPropositionExtractorSyncCall:
    """Tests for __call__ method."""
    
class TestBatchLLMPropositionExtractorSyncBatchConfig:
    """Tests for batch configuration."""


class TestBatchLLMPropositionExtractorSyncUpdateNode:
    """Tests for _update_node method."""

    def _make_extractor(self):
        """Create a BatchLLMPropositionExtractorSync instance for unit testing."""
        extractor = BatchLLMPropositionExtractorSync.__new__(BatchLLMPropositionExtractorSync)
        return extractor

    def test_update_node_with_none_proposition_data(self):
        """Verify _update_node handles None proposition_data (e.g. Nova 2 Lite returning null)."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-1")
        node_metadata_map = {"node-1": None}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[PROPOSITIONS_KEY] == []

    def test_update_node_with_list_proposition_data(self):
        """Verify _update_node passes through list proposition_data unchanged."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-1")
        propositions = ["The sky is blue.", "Water is wet."]
        node_metadata_map = {"node-1": propositions}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[PROPOSITIONS_KEY] == propositions

    def test_update_node_with_string_proposition_data(self):
        """Verify _update_node parses newline-separated string into propositions list."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-1")
        node_metadata_map = {"node-1": "The sky is blue.\nWater is wet."}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[PROPOSITIONS_KEY] == ["The sky is blue.", "Water is wet."]

    def test_update_node_with_missing_node_id(self):
        """Verify _update_node defaults to [] when node_id not in map."""
        extractor = self._make_extractor()
        node = TextNode(text="test", id_="node-missing")
        node_metadata_map = {"node-other": ["some proposition"]}

        result = extractor._update_node(node, node_metadata_map)

        assert result.metadata[PROPOSITIONS_KEY] == []


class TestBatchLLMPropositionExtractorSyncRunNonBatchExtractor:
    """Tests for _run_non_batch_extractor method.

    Regression tests: when a node set falls below Bedrock's minimum batch
    size, BatchLLMPropositionExtractorSync falls back to the non-batch
    LLMPropositionExtractor. That fallback must reuse the configured LLM
    (self.llm) rather than silently defaulting to
    GraphRAGConfig.extraction_llm, which is a us.* inference profile and
    is invalid in any other Bedrock region.
    """

    def _make_extractor(self, llm, prompt_template="prompt", source_metadata_field=None):
        """Create a BatchLLMPropositionExtractorSync instance with fields
        populated via model_construct, bypassing validation/__init__ so no
        real BatchConfig or AWS setup is needed for this unit test."""
        return BatchLLMPropositionExtractorSync.model_construct(
            llm=llm,
            prompt_template=prompt_template,
            source_metadata_field=source_metadata_field,
        )

    @patch("graphrag_toolkit.lexical_graph.indexing.extract.batch_llm_proposition_extractor_sync.LLMPropositionExtractor")
    def test_run_non_batch_extractor_passes_configured_llm(self, mock_extractor_cls):
        """Verify the non-batch fallback is constructed with the extractor's
        own configured llm, not left to default to GraphRAGConfig.extraction_llm."""
        configured_llm = Mock(name="configured-llm")
        extractor = self._make_extractor(llm=configured_llm)

        mock_instance = mock_extractor_cls.return_value
        mock_instance.extract.return_value = [{PROPOSITIONS_KEY: []}]

        nodes = [TextNode(text="test", id_="node-1")]
        extractor._run_non_batch_extractor(nodes)

        mock_extractor_cls.assert_called_once_with(
            llm=configured_llm,
            prompt_template="prompt",
            source_metadata_field=None,
        )
