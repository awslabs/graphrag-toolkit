# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Spec item 4: a custom/fallback pipeline must run end-to-end through
ExtractionPipeline itself (not just the stage's TransformComponent in
isolation), under both the real-time (synchronous, in-process) path and the
Bedrock batch/auto-tuned path, with the same per-document isolation holding
in both."""

import pytest
from llama_index.core.schema import Document

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_pipeline import (
    ExtractionPipeline,
    PassThroughDecorator,
)
from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_builder import PipelineBuilder
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig
from graphrag_toolkit.lexical_graph.indexing.extract.stages.custom_extraction_stage import (
    CustomExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.fallback_extraction_stage import (
    FallbackExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.batch_stages import (
    BatchLLMPropositionStage,
)
from graphrag_toolkit.lexical_graph.indexing.constants import (
    EXTRACTION_STATUS_KEY,
    EXTRACTION_STATUS_QUARANTINED,
)


def in_house_ner(n):
    """Stands in for a customer's own NER/classification service."""
    if 'unsupported' in n.get_content():
        raise ValueError('document format not supported by in-house NER')
    return {'entities': [f'entity-from:{n.get_content()}']}


def default_fallback(n):
    """Stands in for the toolkit's default extraction stage."""
    return {'entities': [f'default-entity-from:{n.get_content()}']}


def build_fallback_pipeline_components():
    custom = CustomExtractionStage(extractor_fn=in_house_ner, output_keys=['entities'])
    default = CustomExtractionStage(extractor_fn=default_fallback, output_keys=['entities'])
    fallback = FallbackExtractionStage(chain=[custom, default])

    builder = PipelineBuilder()
    builder.add(fallback)
    return builder.build()


class TestCustomExtractionPipelineRealTime:
    """A from_stages()-style pipeline (built through PipelineBuilder) runs
    through ExtractionPipeline's real-time (_extract_fixed_batch) path."""

    def test_real_time_path_runs_custom_and_fallback_stages(self):
        pipeline = ExtractionPipeline(
            components=build_fallback_pipeline_components(),
            decorator=PassThroughDecorator(),
            num_workers=1,
        )

        docs = [
            Document(text='a normal document'),
            Document(text='an unsupported document'),
        ]
        results = list(pipeline.extract(docs))
        nodes = [n for sd in results for n in sd.nodes]
        by_text = {n.get_content(): n for n in nodes}

        assert by_text['a normal document'].metadata['entities'] == [
            'entity-from:a normal document'
        ]
        assert by_text['an unsupported document'].metadata['entities'] == [
            'default-entity-from:an unsupported document'
        ]

    def test_real_time_path_isolates_a_document_that_exhausts_the_chain(self):
        def selective(n):
            if 'bad' in n.get_content():
                raise RuntimeError('cannot handle')
            return {'entities': [f'ok:{n.get_content()}']}

        custom = CustomExtractionStage(extractor_fn=selective, output_keys=['entities'])
        other = CustomExtractionStage(extractor_fn=selective, output_keys=['entities'])
        fallback = FallbackExtractionStage(chain=[custom, other])

        builder = PipelineBuilder()
        builder.add(fallback)

        pipeline = ExtractionPipeline(
            components=builder.build(),
            decorator=PassThroughDecorator(),
            num_workers=1,
        )

        docs = [Document(text='good document'), Document(text='bad document')]
        results = list(pipeline.extract(docs))
        nodes = [n for sd in results for n in sd.nodes]
        by_text = {n.get_content(): n for n in nodes}

        assert by_text['good document'].metadata['entities'] == ['ok:good document']
        assert by_text['bad document'].metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_QUARANTINED


class TestCustomExtractionPipelineBatchRouting:
    """A from_stages()-style pipeline that includes a Bedrock batch stage is
    picked up by ExtractionPipeline exactly as the non-custom
    (ExtractionConfig(stages=None)) path already is."""

    def _batch_config(self):
        return BatchConfig(
            role_arn='arn:aws:iam::123456789012:role/test-role',
            region='us-east-1',
            bucket_name='test-bucket',
        )

    def test_batch_stage_from_a_custom_pipeline_is_detected_by_extraction_pipeline(self):
        batch_stage = BatchLLMPropositionStage(batch_config=self._batch_config())

        builder = PipelineBuilder()
        builder.add(batch_stage)
        components = builder.build()

        pipeline = ExtractionPipeline(components=components, decorator=PassThroughDecorator())

        assert pipeline._batch_config is not None
        assert pipeline._batch_config.bucket_name == 'test-bucket'

    @pytest.mark.skip(
        reason='Pending a decision on spec CUSTOM_EXTRACTION_SPEC.md item 4, '
               'open question 1: reject vs. support mixing a Batch*Stage with a '
               'synchronous CustomExtractionStage/FallbackExtractionStage in one '
               'from_stages([...]) list. Left red-but-skipped rather than decided '
               'unilaterally; un-skip once the behavior is chosen.'
    )
    def test_mixing_a_batch_stage_and_a_sync_custom_stage_is_rejected_at_build_time(self):
        """Decision (spec item 4, open question 1): mixing a Batch*Stage and
        a synchronous CustomExtractionStage/FallbackExtractionStage in one
        from_stages([...]) list is not a supported combination today -
        PipelineBuilder has no concept of batch-compatible vs. sync-only
        stages, so a custom pipeline that wires both must not silently
        pretend to work. Reject it explicitly at build time instead.
        """
        batch_stage = BatchLLMPropositionStage(batch_config=self._batch_config())
        custom_stage = CustomExtractionStage(
            extractor_fn=in_house_ner, input_keys=batch_stage.output_keys(), output_keys=['entities']
        )

        builder = PipelineBuilder()
        builder.add(batch_stage)

        with pytest.raises(ValueError, match='(?i)batch'):
            builder.add(custom_stage)
