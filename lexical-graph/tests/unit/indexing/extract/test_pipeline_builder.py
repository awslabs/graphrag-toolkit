# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import MagicMock
from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_builder import PipelineBuilder


def make_stage(input_keys, output_keys):
    """Helper to create a concrete ExtractionStage."""
    class TestStage(ExtractionStage):
        def __init__(self, ik, ok):
            self._ik = ik
            self._ok = ok
        def input_keys(self):
            return self._ik
        def output_keys(self):
            return self._ok
        def as_transform(self):
            return MagicMock()
    return TestStage(input_keys, output_keys)


class TestPipelineBuilder:
    """Tests for PipelineBuilder."""

    def test_build_empty_raises(self):
        """Verify building an empty pipeline raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match='no stages'):
            builder.build()

    def test_add_stage_no_inputs(self):
        """Verify a stage with no input requirements can be added."""
        builder = PipelineBuilder()
        stage = make_stage([], ['key_a'])
        builder.add(stage)
        assert len(builder.stages) == 1

    def test_add_stage_with_satisfied_inputs(self):
        """Verify a stage with satisfied inputs can be added."""
        builder = PipelineBuilder(initial_keys=['key_a'])
        stage = make_stage(['key_a'], ['key_b'])
        builder.add(stage)
        assert len(builder.stages) == 1

    def test_add_stage_with_missing_inputs_raises(self):
        """Verify adding a stage with unsatisfied inputs raises ValueError."""
        builder = PipelineBuilder()
        stage = make_stage(['missing_key'], ['key_b'])
        with pytest.raises(ValueError, match='missing_key'):
            builder.add(stage)

    def test_chained_stages(self):
        """Verify stages can be chained where output feeds input."""
        builder = PipelineBuilder()
        stage1 = make_stage([], ['key_a'])
        stage2 = make_stage(['key_a'], ['key_b'])
        stage3 = make_stage(['key_a', 'key_b'], ['key_c'])
        builder.add(stage1).add(stage2).add(stage3)
        assert len(builder.stages) == 3

    def test_build_returns_transforms(self):
        """Verify build returns list of TransformComponents."""
        builder = PipelineBuilder()
        stage = make_stage([], ['key_a'])
        builder.add(stage)
        result = builder.build()
        assert len(result) == 1

    def test_available_keys_tracks_outputs(self):
        """Verify available_keys accumulates output keys."""
        builder = PipelineBuilder(initial_keys=['init'])
        stage = make_stage([], ['key_a', 'key_b'])
        builder.add(stage)
        assert 'init' in builder.available_keys
        assert 'key_a' in builder.available_keys
        assert 'key_b' in builder.available_keys

    def test_method_chaining(self):
        """Verify add() returns self for chaining."""
        builder = PipelineBuilder()
        result = builder.add(make_stage([], ['a']))
        assert result is builder

    def test_initial_keys_satisfy_first_stage(self):
        """Verify initial_keys are available for the first stage."""
        builder = PipelineBuilder(initial_keys=['text', 'source'])
        stage = make_stage(['text', 'source'], ['propositions'])
        builder.add(stage)
        assert len(builder.stages) == 1

    def test_partial_missing_keys_raises(self):
        """Verify error when only some required keys are available."""
        builder = PipelineBuilder(initial_keys=['key_a'])
        stage = make_stage(['key_a', 'key_b'], ['key_c'])
        with pytest.raises(ValueError, match='key_b'):
            builder.add(stage)
