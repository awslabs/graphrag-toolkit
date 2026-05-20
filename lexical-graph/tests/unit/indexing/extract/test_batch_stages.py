# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import MagicMock
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages.batch_stages import (
    BatchLLMPropositionStage,
    BatchTopicExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.constants import PROPOSITIONS_KEY, TOPICS_KEY


class TestBatchLLMPropositionStage:
    """Tests for BatchLLMPropositionStage."""

    def test_is_extraction_stage(self):
        """Verify BatchLLMPropositionStage is an ExtractionStage."""
        stage = BatchLLMPropositionStage(batch_config=MagicMock())
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_empty(self):
        """Verify input_keys is empty."""
        stage = BatchLLMPropositionStage(batch_config=MagicMock())
        assert stage.input_keys() == []

    def test_output_keys(self):
        """Verify output_keys returns PROPOSITIONS_KEY."""
        stage = BatchLLMPropositionStage(batch_config=MagicMock())
        assert stage.output_keys() == [PROPOSITIONS_KEY]

    def test_stage_type(self):
        """Verify stage_type is 'llm'."""
        stage = BatchLLMPropositionStage(batch_config=MagicMock())
        assert stage.stage_type == 'llm'


class TestBatchTopicExtractionStage:
    """Tests for BatchTopicExtractionStage."""

    def test_is_extraction_stage(self):
        """Verify BatchTopicExtractionStage is an ExtractionStage."""
        stage = BatchTopicExtractionStage(batch_config=MagicMock())
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_with_propositions(self):
        """Verify input_keys includes PROPOSITIONS_KEY by default."""
        stage = BatchTopicExtractionStage(batch_config=MagicMock())
        assert stage.input_keys() == [PROPOSITIONS_KEY]

    def test_input_keys_without_propositions(self):
        """Verify input_keys is empty when use_propositions=False."""
        stage = BatchTopicExtractionStage(batch_config=MagicMock(), use_propositions=False)
        assert stage.input_keys() == []

    def test_output_keys(self):
        """Verify output_keys returns TOPICS_KEY."""
        stage = BatchTopicExtractionStage(batch_config=MagicMock())
        assert stage.output_keys() == [TOPICS_KEY]

    def test_stage_type(self):
        """Verify stage_type is 'llm'."""
        stage = BatchTopicExtractionStage(batch_config=MagicMock())
        assert stage.stage_type == 'llm'
