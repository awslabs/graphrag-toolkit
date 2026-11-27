# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages.custom_extraction_stage import (
    CustomExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.constants import (
    EXTRACTION_STATUS_KEY,
    EXTRACTION_ERROR_KEY,
    EXTRACTION_STATUS_FAILED,
)


def node(text='some text', **metadata):
    return TextNode(text=text, metadata=metadata)


class TestCustomExtractionStageContract:
    """CustomExtractionStage is an ExtractionStage and reports its own keys."""

    def test_is_extraction_stage(self):
        stage = CustomExtractionStage(extractor_fn=lambda n: {}, output_keys=['out'])
        assert isinstance(stage, ExtractionStage)

    def test_input_keys_default_empty(self):
        stage = CustomExtractionStage(extractor_fn=lambda n: {})
        assert stage.input_keys() == []

    def test_input_keys_reflects_what_was_passed(self):
        stage = CustomExtractionStage(extractor_fn=lambda n: {}, input_keys=['in1', 'in2'])
        assert stage.input_keys() == ['in1', 'in2']

    def test_output_keys_reflects_what_was_passed(self):
        stage = CustomExtractionStage(extractor_fn=lambda n: {}, output_keys=['custom_out'])
        assert stage.output_keys() == ['custom_out']

    def test_stage_type_is_custom(self):
        stage = CustomExtractionStage(extractor_fn=lambda n: {})
        assert stage.stage_type == 'custom'


class TestCustomExtractionStageSuccess:
    """A successful extractor_fn result is merged onto the node's metadata."""

    def test_extractor_result_merged_onto_node_metadata(self):
        stage = CustomExtractionStage(
            extractor_fn=lambda n: {'custom_out': f'extracted:{n.text}'},
            output_keys=['custom_out'],
        )
        transform = stage.as_transform()

        [result] = transform([node('hello')])

        assert result.metadata['custom_out'] == 'extracted:hello'

    def test_successful_node_is_not_marked_failed(self):
        stage = CustomExtractionStage(extractor_fn=lambda n: {'out': 1}, output_keys=['out'])
        transform = stage.as_transform()

        [result] = transform([node()])

        assert result.metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_FAILED

    def test_extractor_fn_receives_the_node(self):
        seen = []
        stage = CustomExtractionStage(extractor_fn=lambda n: seen.append(n) or {}, output_keys=[])
        transform = stage.as_transform()
        n = node('check me')

        transform([n])

        assert seen == [n]


class TestCustomExtractionStageFailureIsolation:
    """A raising extractor_fn is caught per-node: it never aborts the batch.

    This is the guarantee that makes the same stage usable for both a
    single-node "real-time" call and a many-node "batch" call: failure
    isolation is per node regardless of how many nodes are in the list.
    """

    def test_raising_extractor_does_not_propagate(self):
        def boom(n):
            raise ValueError('extractor exploded')

        stage = CustomExtractionStage(extractor_fn=boom, output_keys=['out'])
        transform = stage.as_transform()

        # Must not raise.
        [result] = transform([node()])

        assert result.metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_FAILED

    def test_failure_is_recorded_with_error_message(self):
        def boom(n):
            raise ValueError('boom: bad input')

        stage = CustomExtractionStage(extractor_fn=boom, output_keys=['out'])
        transform = stage.as_transform()

        [result] = transform([node()])

        assert 'boom: bad input' in result.metadata[EXTRACTION_ERROR_KEY]

    def test_one_failing_document_does_not_affect_siblings_in_the_same_batch(self):
        """Per-document isolation (R13): one bad document in a batch of many
        must not block or corrupt extraction for the rest."""

        def flaky(n):
            if n.text == 'bad':
                raise RuntimeError('cannot extract this one')
            return {'out': f'ok:{n.text}'}

        stage = CustomExtractionStage(extractor_fn=flaky, output_keys=['out'])
        transform = stage.as_transform()

        nodes = [node('good-1'), node('bad'), node('good-2')]
        results = transform(nodes)

        by_text = {n.text: n for n in results}
        assert by_text['good-1'].metadata['out'] == 'ok:good-1'
        assert by_text['good-2'].metadata['out'] == 'ok:good-2'
        assert by_text['bad'].metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_FAILED
        assert 'out' not in by_text['bad'].metadata


class TestCustomExtractionStageComposesWithPipelineBuilder:
    """CustomExtractionStage must slot into the composable pipeline seam from PR #241."""

    def test_builds_through_pipeline_builder(self):
        from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_builder import PipelineBuilder

        stage = CustomExtractionStage(extractor_fn=lambda n: {'out': 1}, output_keys=['out'])
        builder = PipelineBuilder()
        builder.add(stage)

        transforms = builder.build()

        assert len(transforms) == 1

    def test_from_stages_accepts_custom_extraction_stage(self):
        from graphrag_toolkit.lexical_graph.lexical_graph_index import ExtractionConfig

        stage = CustomExtractionStage(extractor_fn=lambda n: {'out': 1}, output_keys=['out'])
        config = ExtractionConfig.from_stages([stage])

        assert config.stages == [stage]
