# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import MagicMock
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage


class ConcreteStage(ExtractionStage):
    """Concrete implementation for testing."""

    def input_keys(self):
        return ['input_key']

    def output_keys(self):
        return ['output_key']

    def as_transform(self):
        return MagicMock()


class TestExtractionStage:
    """Tests for ExtractionStage ABC."""

    def test_cannot_instantiate_abstract_class(self):
        """Verify ExtractionStage cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ExtractionStage()

    def test_concrete_stage_instantiation(self):
        """Verify concrete implementation can be instantiated."""
        stage = ConcreteStage()
        assert stage is not None

    def test_input_keys(self):
        """Verify input_keys returns expected keys."""
        stage = ConcreteStage()
        assert stage.input_keys() == ['input_key']

    def test_output_keys(self):
        """Verify output_keys returns expected keys."""
        stage = ConcreteStage()
        assert stage.output_keys() == ['output_key']

    def test_as_transform_returns_component(self):
        """Verify as_transform returns a TransformComponent."""
        stage = ConcreteStage()
        result = stage.as_transform()
        assert result is not None

    def test_default_stage_type(self):
        """Verify default stage_type is 'transform'."""
        stage = ConcreteStage()
        assert stage.stage_type == 'transform'

    def test_custom_stage_type(self):
        """Verify stage_type can be overridden."""
        class LLMStage(ConcreteStage):
            @property
            def stage_type(self):
                return 'llm'

        stage = LLMStage()
        assert stage.stage_type == 'llm'
