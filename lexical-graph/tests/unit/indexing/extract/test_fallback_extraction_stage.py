# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from llama_index.core.schema import TextNode

from graphrag_toolkit.lexical_graph.indexing.extract.extraction_stage import ExtractionStage
from graphrag_toolkit.lexical_graph.indexing.extract.stages.custom_extraction_stage import (
    CustomExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.extract.stages.fallback_extraction_stage import (
    FallbackExtractionStage,
)
from graphrag_toolkit.lexical_graph.indexing.constants import (
    EXTRACTION_STATUS_KEY,
    EXTRACTION_STATUS_FAILED,
    EXTRACTION_STATUS_QUARANTINED,
)


def node(text='some text'):
    return TextNode(text=text)


def always_fails(n):
    raise RuntimeError(f'cannot extract {n.text}')


def always_succeeds(tag):
    def extractor_fn(n):
        return {'out': f'{tag}:{n.text}'}
    return extractor_fn


def fails_for(bad_texts, tag):
    def extractor_fn(n):
        if n.text in bad_texts:
            raise RuntimeError(f'{tag} cannot handle {n.text}')
        return {'out': f'{tag}:{n.text}'}
    return extractor_fn


class TestFallbackExtractionStageContract:

    def test_is_extraction_stage(self):
        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary])
        assert isinstance(stage, ExtractionStage)

    def test_requires_at_least_one_stage(self):
        with pytest.raises(ValueError):
            FallbackExtractionStage(chain=[])

    def test_input_keys_come_from_first_stage_in_chain(self):
        primary = CustomExtractionStage(
            extractor_fn=always_fails, input_keys=['primary_in'], output_keys=['out'],
        )
        fallback = CustomExtractionStage(extractor_fn=always_succeeds('fb'), output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, fallback])

        assert stage.input_keys() == ['primary_in']

    def test_output_keys_union_across_chain_without_duplicates(self):
        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out', 'shared'])
        fallback = CustomExtractionStage(extractor_fn=always_succeeds('fb'), output_keys=['shared', 'extra'])
        stage = FallbackExtractionStage(chain=[primary, fallback])

        assert stage.output_keys() == ['out', 'shared', 'extra']

    def test_stage_type_is_fallback(self):
        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary])
        assert stage.stage_type == 'fallback'


class TestFallbackExtractionStageRecovery:

    def test_all_succeed_at_first_stage_never_touches_the_rest_of_the_chain(self):
        calls = []

        def tracking_fallback(n):
            calls.append(n.text)
            return {'out': f'fallback:{n.text}'}

        primary = CustomExtractionStage(extractor_fn=always_succeeds('primary'), output_keys=['out'])
        fallback = CustomExtractionStage(extractor_fn=tracking_fallback, output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, fallback])
        transform = stage.as_transform()

        [result] = transform([node('doc-1')])

        assert result.metadata['out'] == 'primary:doc-1'
        assert calls == []

    def test_node_failing_first_stage_is_recovered_by_second_stage(self):
        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        fallback = CustomExtractionStage(extractor_fn=always_succeeds('default'), output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, fallback])
        transform = stage.as_transform()

        [result] = transform([node('doc-1')])

        assert result.metadata['out'] == 'default:doc-1'
        assert result.metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_FAILED

    def test_node_failing_every_stage_is_quarantined_without_raising(self):
        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        fallback = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, fallback])
        transform = stage.as_transform()

        # Must not raise.
        [result] = transform([node('doc-1')])

        assert result.metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_QUARANTINED

    def test_a_stage_with_no_status_opinion_is_treated_as_success(self):
        """A plain stage that knows nothing about the status contract (e.g. a
        toolkit-default stage) must still work as a fallback target."""

        class NoStatusTransform:
            def __call__(self, nodes, **kwargs):
                for n in nodes:
                    n.metadata['out'] = f'no-status:{n.text}'
                return nodes

        class NoStatusStage(ExtractionStage):
            def input_keys(self):
                return []

            def output_keys(self):
                return ['out']

            def as_transform(self):
                return NoStatusTransform()

        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, NoStatusStage()])
        transform = stage.as_transform()

        [result] = transform([node('doc-1')])

        assert result.metadata['out'] == 'no-status:doc-1'
        assert result.metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_FAILED


class TestFallbackExtractionStagePerDocumentIsolationInABatch:
    """R13/R17: within one batch call, each document's outcome is independent."""

    def test_mixed_outcomes_resolve_independently_in_a_single_batch_call(self):
        primary = CustomExtractionStage(
            extractor_fn=fails_for({'needs-fallback', 'unrecoverable'}, tag='primary'),
            output_keys=['out'],
        )
        fallback = CustomExtractionStage(
            extractor_fn=fails_for({'unrecoverable'}, tag='fallback'),
            output_keys=['out'],
        )
        stage = FallbackExtractionStage(chain=[primary, fallback])
        transform = stage.as_transform()

        nodes = [node('succeeds-immediately'), node('needs-fallback'), node('unrecoverable')]
        results = transform(nodes)
        by_text = {n.text: n for n in results}

        assert by_text['succeeds-immediately'].metadata['out'] == 'primary:succeeds-immediately'
        assert by_text['needs-fallback'].metadata['out'] == 'fallback:needs-fallback'
        assert by_text['unrecoverable'].metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_QUARANTINED

        # The quarantined document must not have corrupted or blocked the others.
        assert by_text['succeeds-immediately'].metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_QUARANTINED
        assert by_text['needs-fallback'].metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_QUARANTINED


class TestFallbackExtractionStageComposesWithPipelineBuilder:

    def test_builds_through_pipeline_builder(self):
        from graphrag_toolkit.lexical_graph.indexing.extract.pipeline_builder import PipelineBuilder

        primary = CustomExtractionStage(extractor_fn=always_fails, output_keys=['out'])
        fallback = CustomExtractionStage(extractor_fn=always_succeeds('default'), output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, fallback])

        builder = PipelineBuilder()
        builder.add(stage)
        transforms = builder.build()

        assert len(transforms) == 1


class RaisingTransform:
    """A plain TransformComponent-shaped callable that raises like a
    built-in stage (e.g. NERTransform) does — it has no opinion about
    EXTRACTION_STATUS_KEY and does not catch anything itself."""

    def __init__(self, bad_texts, tag):
        self.bad_texts = set(bad_texts)
        self.tag = tag
        self.calls = []

    def __call__(self, nodes, **kwargs):
        self.calls.append([n.text for n in nodes])
        for n in self.bad_texts:
            if any(node.text == n for node in nodes):
                raise RuntimeError(f'{self.tag} cannot handle {n}')
        for n in nodes:
            n.metadata['out'] = f'{self.tag}:{n.text}'
        return nodes


class RaisingStage(ExtractionStage):
    """Wraps RaisingTransform as an ExtractionStage, the way a built-in
    stage (NERExtractionStage, LLMTopicExtractionStage, ...) wraps its own
    TransformComponent: no per-node try/except of its own."""

    def __init__(self, bad_texts, tag):
        self._transform = RaisingTransform(bad_texts, tag)

    def input_keys(self):
        return []

    def output_keys(self):
        return ['out']

    def as_transform(self):
        return self._transform


class TestFallbackExtractionStageIsolatesBuiltinStyleStages:
    """A stage that raises outright (no status-tag convention of its own,
    like every shipped built-in stage) must still be isolated per-node by
    FallbackExtractionStage itself, not just by CustomExtractionStage's
    internal try/except."""

    def test_node_that_crashes_a_builtin_style_stage_is_recovered_by_next_stage(self):
        primary = RaisingStage(bad_texts={'doc-1'}, tag='primary')
        fallback = CustomExtractionStage(extractor_fn=always_succeeds('default'), output_keys=['out'])
        stage = FallbackExtractionStage(chain=[primary, fallback])
        transform = stage.as_transform()

        [result] = transform([node('doc-1')])

        assert result.metadata['out'] == 'default:doc-1'
        assert result.metadata.get(EXTRACTION_STATUS_KEY) != EXTRACTION_STATUS_FAILED

    def test_node_that_crashes_every_builtin_style_stage_is_quarantined_without_raising(self):
        primary = RaisingStage(bad_texts={'doc-1'}, tag='primary')
        fallback = RaisingStage(bad_texts={'doc-1'}, tag='fallback')
        stage = FallbackExtractionStage(chain=[primary, fallback])
        transform = stage.as_transform()

        # Must not raise, even though neither stage catches anything itself.
        [result] = transform([node('doc-1')])

        assert result.metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_QUARANTINED

    def test_one_crashing_node_does_not_affect_siblings_in_the_same_batch_call(self):
        primary = RaisingStage(bad_texts={'bad'}, tag='primary')
        stage = FallbackExtractionStage(chain=[primary])
        transform = stage.as_transform()

        results = transform([node('good-1'), node('bad'), node('good-2')])
        by_text = {n.text: n for n in results}

        assert by_text['good-1'].metadata['out'] == 'primary:good-1'
        assert by_text['good-2'].metadata['out'] == 'primary:good-2'
        assert by_text['bad'].metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_QUARANTINED

    def test_happy_path_still_calls_the_stage_once_for_the_whole_batch(self):
        """The per-node isolation must not cost batching efficiency when
        nothing fails: a stage that succeeds for every node in a batch is
        still called once with the full batch, not once per node."""
        primary = RaisingStage(bad_texts=set(), tag='primary')
        stage = FallbackExtractionStage(chain=[primary])
        transform = stage.as_transform()

        nodes = [node('a'), node('b'), node('c')]
        transform(nodes)

        assert primary._transform.calls == [['a', 'b', 'c']]

    def test_a_raising_batch_call_retries_node_by_node_to_isolate_the_culprit(self):
        """When the batched call raises, only the actual offending node(s)
        should end up failed/quarantined; nodes that would have succeeded
        in that same batch call must still get their output."""
        primary = RaisingStage(bad_texts={'bad'}, tag='primary')
        stage = FallbackExtractionStage(chain=[primary])
        transform = stage.as_transform()

        results = transform([node('good-1'), node('bad'), node('good-2')])
        by_text = {n.text: n for n in results}

        # First call was the full batch and raised; the retry is per-node.
        assert primary._transform.calls[0] == ['good-1', 'bad', 'good-2']
        assert by_text['good-1'].metadata['out'] == 'primary:good-1'
        assert by_text['good-2'].metadata['out'] == 'primary:good-2'
        assert by_text['bad'].metadata[EXTRACTION_STATUS_KEY] == EXTRACTION_STATUS_QUARANTINED
