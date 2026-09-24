# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import Mock, patch
from llama_index.core.schema import TextNode
from graphrag_toolkit.lexical_graph import BatchJobError
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


def _extracted(count=3):
    """An output describing the first count nodes of the batch."""
    return [(f'c{i}', 'extracted') for i in range(count)]


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
        patch(f'{MODULE}.process_batch_output_sync', return_value=_extracted()),
    ):
        results = list(extractor._process_single_batch(
            0, _nodes(), Mock(), bedrock_client or Mock()
        ))

    return results, submit


class TestARunThatKeepsNoRecord:

    def test_it_submits_without_asking(self, tmp_path):
        results, submit = _run(_extractor(tmp_path=tmp_path))

        assert submit.called
        assert results == _extracted()

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
        assert results == _extracted()

    def test_the_partition_is_recorded_as_complete(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': 'Completed'}

        _run(_extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client)

        assert [r.state for r in store.written] == [COMPLETE]
        assert store.written[0].attempt == 1, 'recovering is not another attempt'


class TestAJobThatDidNotComplete:

    @pytest.mark.parametrize('status', ['Failed', 'Stopped', 'Stopping', 'Expired', 'PartiallyCompleted'])
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
            patch(f'{MODULE}.process_batch_output_sync', return_value=_extracted()),
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

    def _run_with_output(self, extractor, bedrock_client, results=None, download=None, outputs=None):
        # results: one output for every read. outputs: one per read in order,
        # for a recovery that must fail and a redo that must succeed.
        with (
            patch(f'{MODULE}.create_and_run_batch_job') as submit,
            patch(f'{MODULE}.download_output_files', side_effect=download),
            patch(f'{MODULE}.process_batch_output_sync', **({'side_effect': outputs} if outputs is not None else {'return_value': results or []})),
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
            extractor, self._completed_job(), results=_extracted(), download=download,
        )

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']

    @pytest.mark.parametrize('described', [0, 1, 2])
    def test_an_output_short_of_a_node_is_resubmitted(self, described, tmp_path):
        # A node the output does not describe would be extracted as nothing,
        # and a record marked complete means no restart comes back for it.
        store = _store(record=_record(SUBMITTED))
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)

        submit = self._run_with_output(extractor, self._completed_job(), results=_extracted(described))

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']

    def test_neither_is_recorded_as_complete_before_the_work_is_redone(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)

        self._run_with_output(extractor, self._completed_job(), outputs=[[], _extracted()])

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


class TestAJobAnEarlierRunLeftRunning:
    """
    The state a restart is most likely to find right after a crash. The job
    bills to its end whether or not it is resubmitted, so waiting is cheaper,
    and a second submission would also count against the concurrent-job quota.
    """

    def _run_waiting(self, extractor, bedrock_client, wait_outcome=None, results=None):
        with (
            patch(f'{MODULE}.create_and_run_batch_job') as submit,
            patch(f'{MODULE}.wait_for_job_completion', side_effect=wait_outcome) as wait,
            patch(f'{MODULE}.download_output_files'),
            patch(f'{MODULE}.process_batch_output_sync', return_value=results if results is not None else _extracted()),
        ):
            out = list(extractor._process_single_batch(0, _nodes(), Mock(), bedrock_client))
        return out, submit, wait

    @pytest.mark.parametrize('status', ['Submitted', 'Validating', 'Scheduled', 'InProgress'])
    def test_it_is_waited_on_and_its_output_used(self, status, tmp_path):
        store = _store(record=_record(SUBMITTED))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': status}

        out, submit, wait = self._run_waiting(_extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client)

        assert wait.call_args.args[1] == 'arn:job', 'waited on the job the record names'
        assert not submit.called, 'the running job is the one paid for'
        assert out == _extracted()
        assert [r.state for r in store.written] == [COMPLETE]

    def test_one_that_ends_badly_is_resubmitted(self, tmp_path):
        store = _store(record=_record(SUBMITTED, attempt=1))
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': 'InProgress'}

        _, submit, _ = self._run_waiting(
            _extractor(manifest_store=store, tmp_path=tmp_path), bedrock_client,
            wait_outcome=BatchJobError('ended Failed'),
        )

        assert submit.called
        assert '-a2-' in submit.call_args.kwargs['job_name']


class TestARecoveryDirectoryIsItsOwn:

    def test_a_second_recovery_does_not_read_the_first_ones_files(self, tmp_path):
        store = _store(record=_record(SUBMITTED))
        extractor = _extractor(manifest_store=store, tmp_path=tmp_path)
        bedrock_client = Mock()
        bedrock_client.get_model_invocation_job.return_value = {'status': 'Completed'}
        stale = tmp_path / 'recovered' / store.read.return_value.partition_id / 'stale.jsonl.out'
        stale.parent.mkdir(parents=True)
        stale.write_text('left by an earlier recovery')

        _run(extractor, bedrock_client)

        assert not stale.exists(), 'the directory is cleaned before the download lands'


class TestAFreshJobThatDescribesFewerNodesThanItWasGiven:
    """
    The rule the recovery path applies holds for a job this run submitted: a
    record marked complete means no restart comes back for the nodes the
    output left out, so the record stays as submitted and a restart redoes it.
    """

    def _run_short(self, extractor, described):
        with (
            patch(f'{MODULE}.create_and_run_batch_job'),
            patch(f'{MODULE}.download_output_files'),
            patch(f'{MODULE}.process_batch_output_sync', return_value=_extracted(described)),
        ):
            return list(extractor._process_single_batch(0, _nodes(), Mock(), Mock()))

    def test_the_partition_is_not_recorded_complete(self, tmp_path):
        store = _store(record=None)

        self._run_short(_extractor(manifest_store=store, tmp_path=tmp_path), described=2)

        assert COMPLETE not in [r.state for r in store.written]

    def test_what_the_job_did_describe_is_still_used(self, tmp_path):
        # Extraction carries on as it always has; only the record withholds
        # the word complete.
        store = _store(record=None)

        out = self._run_short(_extractor(manifest_store=store, tmp_path=tmp_path), described=2)

        assert out == _extracted(2)

    def test_a_full_output_is_recorded_complete(self, tmp_path):
        store = _store(record=None)

        self._run_short(_extractor(manifest_store=store, tmp_path=tmp_path), described=3)

        assert [r.state for r in store.written][-1] == COMPLETE
