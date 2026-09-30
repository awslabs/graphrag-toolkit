# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import patch, MagicMock
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages import (
    LLMPropositionStage,
    LocalPropositionStage,
    LLMTopicExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY


class TestLLMPropositionStage:
    """Tests for LLMPropositionStage."""

    def test_is_extraction_stage(self):
        """Verify LLMPropositionStage is an ExtractionStage."""
        stage = LLMPropositionStage()
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_empty(self):
        """Verify input_keys returns empty list (reads raw text)."""
        stage = LLMPropositionStage()
        assert stage.input_keys() == []

    def test_output_keys(self):
        """Verify output_keys returns PROPOSITIONS_KEY."""
        stage = LLMPropositionStage()
        assert stage.output_keys() == [PROPOSITIONS_KEY]

    def test_stage_type(self):
        """Verify stage_type is 'llm'."""
        stage = LLMPropositionStage()
        assert stage.stage_type == 'llm'

    @patch('graphrag_toolkit.lexical_graph.indexing.extract.stages.llm_proposition_stage.LLMPropositionExtractor')
    def test_as_transform_creates_extractor(self, mock_cls):
        """Verify as_transform creates LLMPropositionExtractor."""
        stage = LLMPropositionStage(prompt_template='test prompt')
        stage.as_transform()
        mock_cls.assert_called_once_with(prompt_template='test prompt', llm=None)


class TestLocalPropositionStage:
    """Tests for LocalPropositionStage."""

    def test_is_extraction_stage(self):
        """Verify LocalPropositionStage is an ExtractionStage."""
        stage = LocalPropositionStage()
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_empty(self):
        """Verify input_keys returns empty list."""
        stage = LocalPropositionStage()
        assert stage.input_keys() == []

    def test_output_keys(self):
        """Verify output_keys returns PROPOSITIONS_KEY."""
        stage = LocalPropositionStage()
        assert stage.output_keys() == [PROPOSITIONS_KEY]

    def test_stage_type(self):
        """Verify stage_type is 'local'."""
        stage = LocalPropositionStage()
        assert stage.stage_type == 'local'

    @patch('graphrag_toolkit.lexical_graph.indexing.extract.stages.local_proposition_stage.PropositionExtractor')
    def test_as_transform_creates_extractor(self, mock_cls):
        """Verify as_transform creates PropositionExtractor."""
        stage = LocalPropositionStage(model_name='test-model', device='cpu')
        stage.as_transform()
        mock_cls.assert_called_once_with(proposition_model_name='test-model', device='cpu')

    @patch('graphrag_toolkit.lexical_graph.indexing.extract.stages.local_proposition_stage.PropositionExtractor')
    def test_as_transform_default_params(self, mock_cls):
        """Verify as_transform with no params passes no kwargs."""
        stage = LocalPropositionStage()
        stage.as_transform()
        mock_cls.assert_called_once_with()


class TestLLMTopicExtractionStage:
    """Tests for LLMTopicExtractionStage."""

    def test_is_extraction_stage(self):
        """Verify LLMTopicExtractionStage is an ExtractionStage."""
        stage = LLMTopicExtractionStage()
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_with_propositions(self):
        """Verify input_keys includes PROPOSITIONS_KEY when use_propositions=True."""
        stage = LLMTopicExtractionStage(use_propositions=True)
        assert stage.input_keys() == [PROPOSITIONS_KEY]

    def test_input_keys_without_propositions(self):
        """Verify input_keys is empty when use_propositions=False."""
        stage = LLMTopicExtractionStage(use_propositions=False)
        assert stage.input_keys() == []

    def test_output_keys(self):
        """Verify output_keys returns TOPICS_KEY."""
        stage = LLMTopicExtractionStage()
        assert stage.output_keys() == [TOPICS_KEY]

    def test_stage_type(self):
        """Verify stage_type is 'llm'."""
        stage = LLMTopicExtractionStage()
        assert stage.stage_type == 'llm'

    @patch('graphrag_toolkit.lexical_graph.indexing.extract.stages.llm_topic_extraction_stage.TopicExtractor')
    def test_as_transform_with_propositions(self, mock_cls):
        """Verify as_transform passes correct source_metadata_field."""
        stage = LLMTopicExtractionStage(use_propositions=True)
        stage.as_transform()
        mock_cls.assert_called_once_with(
            source_metadata_field=PROPOSITIONS_KEY,
            prompt_template=None,
            llm=None,
            entity_classification_provider=None,
            topic_provider=None,
            schema_constraints='',
        )

    @patch('graphrag_toolkit.lexical_graph.indexing.extract.stages.llm_topic_extraction_stage.TopicExtractor')
    def test_as_transform_without_propositions(self, mock_cls):
        """Verify as_transform passes None for source_metadata_field."""
        stage = LLMTopicExtractionStage(use_propositions=False)
        stage.as_transform()
        mock_cls.assert_called_once_with(
            source_metadata_field=None,
            prompt_template=None,
            llm=None,
            entity_classification_provider=None,
            topic_provider=None,
            schema_constraints='',
        )
