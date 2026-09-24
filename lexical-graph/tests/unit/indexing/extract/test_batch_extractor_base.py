# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph.utils import LLMCache
from graphrag_toolkit.lexical_graph.indexing.extract.run_manifest import (
    COMPLETE,
    SUBMITTED,
    PartitionRecord,
    partition_id,
)
from graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base import BatchExtractorBase
from graphrag_toolkit.lexical_graph.indexing.extract.batch_config import BatchConfig


class ConcreteBatchExtractor(BatchExtractorBase):
    """Concrete implementation for testing."""
    
    @classmethod
    def class_name(cls) -> str:
        return "ConcreteBatchExtractor"
    
    def _get_json(self, node, llm, inference_parameters):
        """Mock implementation of _get_json."""
        return {}
    
    def _run_non_batch_extractor(self, nodes):
        """Mock implementation of _run_non_batch_extractor."""
        return []
    
    def _update_node(self, node: TextNode, node_metadata_map):
        """Mock implementation of _update_node."""
        return node


class TestBatchExtractorBaseInitialization:
    """Tests for BatchExtractorBase initialization."""
    
    def test_class_name_abstract(self):
        """Verify class_name is implemented in concrete class."""
        assert ConcreteBatchExtractor.class_name() == "ConcreteBatchExtractor"
    
class TestBatchExtractorBaseHelperMethods:
    """Tests for BatchExtractorBase helper methods."""
    
class TestBatchExtractorBaseUpdateNode:
    """Tests for _update_node method."""
    
    pass


# --- What a restart does with a partition an earlier run submitted -----------
#
# The decision is the expensive one in the feature: invoking the model again
# costs the job a second time, and skipping a job that produced nothing loses
# the chunks it was meant to extract.

MODULE = 'graphrag_toolkit.lexical_graph.indexing.extract.batch_extractor_base'


class _Extractor(BatchExtractorBase):
    """The smallest extractor that can run a batch."""

    @classmethod
    def class_name(cls) -> str:
        return '_Extractor'

    def _get_json(self, node, llm, inference_parameters):
        return {'recordId': node.node_id, 'modelInput': {}}

    def _run_non_batch_extractor(self, nodes):
        return []

    def _update_node(self, node, node_metadata_map):
        return node


def _extractor(manifest_store=None, tmp_path=None):
    return _Extractor(
        batch_config=BatchConfig(role_arn='arn:role', region='us-east-1', bucket_name='b'),
        llm=_llm(),
        prompt_template='{text}',
        batch_inference_dir=str(tmp_path),
        description='topic',
        manifest_store=manifest_store,
    )


def _llm():
    """An LLM cache that answers what the batch path asks of it, and nothing more."""
    llm = Mock(spec=LLMCache)
    llm.llm = Mock(_get_all_kwargs=lambda: {})
    llm.model = 'anthropic.claude-x'
    return llm


def _nodes():
    return [TextNode(text=f'text {i}', id_=f'c{i}') for i in range(3)]


def _store(record=None):
    store = Mock()
    store.run_id = 'run-1'
    store.read.return_value = record
    store.written = []
    store.write.side_effect = lambda r, s3_client: store.written.append(r)
    return store


def _record(state, attempt=1, **kwargs):
    return PartitionRecord(
        partition_id=partition_id([n.node_id for n in _nodes()], stage='topic'),
        attempt=attempt, state=state,
        job_arn='arn:job', output_path='out/', input_filename='in.jsonl',
        **kwargs
    )


def _run(extractor, bedrock_client=None):
    with (
        patch(f'{MODULE}.create_and_run_batch_job') as submit,
        patch(f'{MODULE}.download_output_files'),
        patch(f'{MODULE}.process_batch_output_sync', return_value=[('c0', 'extracted')]),
    ):
        results = list(extractor._process_single_batch(
            0, _nodes(), Mock(), bedrock_client or Mock()
        ))

    return results, submit


class TestARunThatKeepsNoRecord:

    def test_it_submits_without_asking(self, tmp_path):
        results, submit = _run(_extractor(tmp_path=tmp_path))

        assert submit.called
        assert results == [('c0', 'extracted')]

    def test_the_job_keeps_its_timestamped_name(self, tmp_path):
        _, submit = _run(_extractor(tmp_path=tmp_path))

        assert submit.call_args.kwargs['job_name'] is None
        assert submit.call_args.kwargs['on_submitted'] is None


class TestAPartitionNoRunHasTouched:

    def test_it_is_submitted_as_the_first_attempt(self, tmp_path):
        store = _store(record=None)

        _, submit = _run(_extractor(manifest_store=store, tmp_path=tmp_path))

        assert submit.called
        assert '-a1-' in submit.call_args.kwargs['job_name']

    def test_it_is_recorded_as_complete_once_its_output_is_read(self, tmp_path):
        store = _store(record=None)

        _run(_extractor(manifest_store=store, tmp_path=tmp_path))

        assert [r.state for r in store.written] == [COMPLETE]


class TestAJobAnEarlierRunCompleted:

    def test_its_output_is_downloaded_rather_than_invoked_again(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': 'Completed'}

        results, submit = _run(_extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client)

        assert not submit.called, 'the job was paid for once already'
        assert results == [('c0', 'extracted')]

    def test_the_partition_is_recorded_as_complete(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': 'Completed'}

        _run(_extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client)

        assert [r.state for r in store.written] == [COMPLETE]
        assert store.written[0].attempt == 1, 'recovering is not another attempt'


class TestAJobThatDidNotComplete:

    @pytest.mark.parametrize('status', ['Failed', 'Stopped', 'Expired', 'PartiallyCompleted', 'InProgress'])
    def test_the_partition_is_resubmitted_as_its_next_attempt(self, status, tmp_path):
        store = _store(record=_record(SUBMITTED, attempt=1))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': status}

        _, submit = _run(_extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client)

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']

    def test_a_job_that_cannot_be_read_is_resubmitted(self, tmp_path):
        # An account that lost sight of the job, rather than a job that failed.
        # Resubmitting costs a job; trusting it costs the chunks.
        store = _store(record=_record(SUBMITTED))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.side_effect = RuntimeError('denied')

        _, submit = _run(_extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client)

        assert submit.called

    def test_a_record_with_no_job_is_resubmitted(self, tmp_path):
        # The run died between writing the record and creating the job.
        store = _store(record=PartitionRecord(
            partition_id='abc', attempt=1, state=SUBMITTED
        ))

        _, submit = _run(_extractor(manifest_store=store, tmp_path=tmp_path))

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']


class TestTheRecordGoesDownBeforeTheWait:

    def test_the_job_is_recorded_as_soon_as_it_exists(self, tmp_path):
        # A job nobody recorded is a job a restart pays for a second time, so
        # the record is written from the submission callback rather than after
        # the wait, which is the part that can outlive the process.
        store = _store(record=None)
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)

        with (
            patch(f'{MODULE}.create_and_run_batch_job') as submit,
            patch(f'{MODULE}.download_output_files'),
            patch(f'{MODULE}.process_batch_output_sync', return_value=[]),
        ):
            submit.side_effect = lambda *a, **kw: kw['on_submitted']('arn:job', kw['job_name'])
            list(extractor._process_single_batch(0, _nodes(), Mock(), Mock()))

        assert [r.state for r in store.written] == [SUBMITTED, COMPLETE]
        assert store.written[0].job_arn == 'arn:job'
        assert store.written[1].job_arn == 'arn:job', 'the completed record keeps the job'


class TestAnOutputThatCannotBeUsed:
    """
    A completed job whose output cannot be read describes none of the
    partition's nodes. Recording it complete would drop those chunks and report
    success, which is the failure the whole feature exists to prevent.
    """

    def _completed_job(self):
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': 'Completed'}
        return bedrock_client

    def _run_with_output(self, extractor, bedrock_client, results=None, download=None):
        with (
            patch(f'{MODULE}.create_and_run_batch_job') as submit,
            patch(f'{MODULE}.download_output_files', side_effect=download),
            patch(f'{MODULE}.process_batch_output_sync', return_value=results or []),
        ):
            list(extractor._process_single_batch(0, _nodes(), Mock(), bedrock_client))

        return submit

    def test_an_output_that_has_aged_out_is_resubmitted(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)

        # Only the recovery download fails. The one after the resubmission
        # reads a folder the new job just wrote.
        downloads = iter([RuntimeError('no such folder'), None])

        def download(*args, **kwargs):
            outcome = next(downloads, None)
            if isinstance(outcome, Exception):
                raise outcome

        submit = self._run_with_output(
            extractor, self._completed_job(), results=[('c0', 'x')], download=download,
        )

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']

    def test_an_output_holding_no_results_is_resubmitted(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)

        submit = self._run_with_output(extractor, self._completed_job(), results=[])

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']

    def test_neither_is_recorded_as_complete_before_the_work_is_redone(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)

        self._run_with_output(extractor, self._completed_job(), results=[])

        assert [r.state for r in store.written] == [COMPLETE], 'recorded once, after the redo'
        assert store.written[0].attempt == 2


class TestThePartitionAnExtractorAsksAbout:
    """
    An extractor holding a batch has to name the partition that batch is, or
    it reads and writes some other partition's record.
    """

    def test_it_is_named_from_the_nodes_in_hand(self, tmp_path):
        store = _store(record=None)

        _run(_extractor(manifest_store=store, tmp_path=tmp_path))

        expected = partition_id([n.node_id for n in _nodes()], stage='topic')
        assert store.read.call_args.args[0] == expected
        assert store.written[0].partition_id == expected

    def test_two_stages_dividing_the_same_nodes_name_different_partitions(self, tmp_path):
        topics = _store(record=None)
        propositions = _store(record=None)

        _run(_extractor(manifest_store=topics, tmp_path=tmp_path))
        extractor = _extractor(manifest_store=propositions, tmp_path=tmp_path)
        extractor.description = 'proposition'
        _run(extractor)

        assert topics.read.call_args.args[0] != propositions.read.call_args.args[0]
